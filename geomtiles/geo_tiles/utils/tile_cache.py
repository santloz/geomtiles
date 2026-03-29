"""Caché de tiles MVT por sistema de ficheros con TTL.

English:
MVT tile filesystem cache with TTL.

Los tiles se guardan como archivos .pbf con la estructura:
    {cache_dir}/{namespace}/{schema}/{table}/{geom_col}/z={z}/x={x}/y={y}/tile.pbf

Cada tile incluye un manifest JSON junto al PBF para que el microservicio
pueda inspeccionar qué capa contiene, cuándo se generó y cuál es su TTL.

Each tile includes a JSON manifest alongside the PBF so the microservice
can inspect which layer it contains, when it was generated and its TTL.

El TTL se comprueba con la fecha de modificación del archivo (mtime),
sin necesidad de base de datos ni metadatos externos.

The TTL is checked using the file modification time (mtime), requiring no
database or external metadata.

Las operaciones de disco se ejecutan en un thread pool para no bloquear
el event loop de asyncio.

Disk operations run in a thread pool so they do not block the asyncio event loop.
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
import re
import time
import uuid
from pathlib import Path
from typing import Any, Optional, Union

logger = logging.getLogger("geo_tiles.tile_cache")

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PROJECTS_DIR = "proyectos"


def _make_cache_key(z: int, x: int, y: int, layers_str: str) -> str:
    """
    Genera un nombre de archivo único para la combinación z/x/y + capa.

    Usa un hash SHA-1 corto (8 hex chars) de las capas para manejar
    múltiples capas en la misma celda sin nombres de archivo demasiado largos.
    """
    layers_hash = hashlib.sha1(layers_str.encode()).hexdigest()[:8]
    return f"{y}_{layers_hash}.pbf"


def _split_layer_key(layer_key: str) -> tuple[str, str, str]:
    """Valida y separa una capa en schema.table.geom_col."""
    parts = layer_key.strip().split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Formato de capa inválido: {layer_key!r}. Se esperaba 'schema.table.geom_col'."
        )
    schema, table, geom_col = parts
    for part in (schema, table, geom_col):
        if not _IDENTIFIER_RE.match(part):
            raise ValueError(f"Identificador SQL inválido en capa: {part!r}")
    return schema, table, geom_col


def _sanitize_scope_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._=-]+", "-", value).strip("-") or "default"


class FilesystemTileCache:
    """
    Caché de tiles MVT sobre sistema de ficheros.

    English:
    MVT tile filesystem cache.

    Estructura en disco / Disk layout:
        {cache_dir}/{namespace}/{schema}/{table}/{geom_col}/z={z}/x={x}/y={y}/tile.pbf

    Uso / Usage:
        cache = FilesystemTileCache(cache_dir="/tmp/geo_tiles_cache", ttl=3600)
        svc = TileService(session_factory, tile_cache=cache)

    El caché es apto para compartir entre múltiples workers (p. ej. varios
    procesos uvicorn) ya que escribe archivos atómicamente y comprueba el
    mtime para validar el TTL.

    The cache is safe to share among multiple workers (e.g. several uvicorn
    processes) because it writes files atomically and validates TTL via mtime.
    """

    def __init__(
        self, cache_dir: Union[str, Path], ttl: int = 3600, namespace: str = "mvt"
    ):
        """
        Args:
            cache_dir: Directorio raíz donde se almacenan los tiles.
            ttl: Tiempo de vida de los tiles en segundos (default: 1 hora).
            namespace: Valor por defecto si no se especifica project_id.
        """
        # Usar directamente el directorio configurado por `TILE_CACHE_DIR`.
        # Antes se añadía una subcarpeta fija (`proyectos`), lo que provocaba
        # rutas duplicadas cuando el volumen ya montaba la carpeta de proyectos
        # (p.ej. Docker compose monta `./prueba/proyectos:/proyectos`).
        # Alineamos con la documentación: base == TILE_CACHE_DIR
        self._base = Path(cache_dir)
        self._namespace = namespace.strip() or "mvt"
        self._ttl = ttl
        self._base.mkdir(parents=True, exist_ok=True)
        # raíz histórica compatible con implementaciones anteriores
        # (`_root` se usa en varias utilidades). Apunta al directorio
        # base donde se guardan los proyectos.
        self._root = self._base

    def _layer_root(
        self,
        schema: str,
        table: str,
        geom_col: str,
        cache_scope: str = "",
        project_id: str = None,
    ) -> Path:
        scope_dir = _sanitize_scope_token(cache_scope)
        # Si project_id está presente, usarlo como subcarpeta; si no, usar namespace
        project_folder = str(project_id) if project_id is not None else self._namespace
        return self._base / project_folder / schema / table / geom_col / scope_dir

    def _tile_dir(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        cache_scope: str = "",
        project_id: str = None,
    ) -> Path:
        return (
            self._layer_root(schema, table, geom_col, cache_scope, project_id)
            / f"z={z}"
            / f"x={x}"
            / f"y={y}"
        )

    def _tile_path(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        cache_scope: str = "",
        project_id: str = None,
    ) -> Path:
        return (
            self._tile_dir(z, x, y, schema, table, geom_col, cache_scope, project_id)
            / "tile.pbf"
        )

    def _manifest_path(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        cache_scope: str = "",
        project_id: str = None,
    ) -> Path:
        return (
            self._tile_dir(z, x, y, schema, table, geom_col, cache_scope, project_id)
            / "manifest.json"
        )

    def _build_manifest(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        cache_scope: str,
        data_size: int,
        project_id: str = None,
    ) -> dict[str, Any]:
        scope_dir = _sanitize_scope_token(cache_scope)
        # Determinar el nombre de la carpeta de proyecto
        project_folder = str(project_id) if project_id is not None else self._namespace
        return {
            "project_folder": project_folder,
            "schema": schema,
            "table": table,
            "geom_col": geom_col,
            "cache_scope": scope_dir,
            "layer": f"{schema}.{table}.{geom_col}",
            "zoom": z,
            "x": x,
            "y": y,
            "tile_file": "tile.pbf",
            "size_bytes": data_size,
            "ttl_seconds": self._ttl,
            "created_at": time.time(),
        }

    # ──────────────────────────── API pública (async) ─────────────────────────

    async def get_layer(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        cache_scope: str = "",
        project_id: str = None,
    ) -> Optional[bytes]:
        """
        Devuelve los bytes del tile desde disco o None si no existe / expiró.

        La lectura de disco se realiza en un thread pool para no bloquear
        el event loop.

        English:
            Returns the bytes of the tile from disk or None if it does not
            exist or has expired. Disk reads are performed in a thread pool
            so as not to block the asyncio event loop.
        """
        path = self._tile_path(
            z, x, y, schema, table, geom_col, cache_scope, project_id
        )
        logger.debug(
            "get_layer: path=%s schema=%s table=%s geom=%s z=%s x=%s y=%s",
            path,
            schema,
            table,
            geom_col,
            z,
            x,
            y,
        )
        result = await asyncio.to_thread(self._read_if_valid, path)
        if result is not None:
            logger.info(
                "cache hit layer=%s.%s.%s z=%s x=%s y=%s size=%d",
                schema,
                table,
                geom_col,
                z,
                x,
                y,
                len(result),
            )
        else:
            logger.debug("cache miss for path=%s", path)
        return result

    async def set_layer(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        data: bytes,
        cache_scope: str = "",
        project_id: str = None,
    ) -> None:
        """
        Escribe el tile en disco de forma atómica (write-to-temp + rename).

        La escritura se hace en un thread pool para no bloquear el event loop.
        Escribe primero en un archivo temporal y luego hace rename, que es
        atómico en Linux/macOS y casi atómico en Windows (NTFS).

        English:
            Writes the tile to disk atomically (write-to-temp + rename).
            The write runs in a thread pool to avoid blocking the event loop.
            First writes to a temporary file and then renames it; this is
            atomic on Unix-like systems and effectively atomic on NTFS.
        """
        if not data:
            return
        tile_path = self._tile_path(
            z, x, y, schema, table, geom_col, cache_scope, project_id
        )
        manifest_path = self._manifest_path(
            z, x, y, schema, table, geom_col, cache_scope, project_id
        )
        manifest = self._build_manifest(
            z, x, y, schema, table, geom_col, cache_scope, len(data), project_id
        )
        await asyncio.to_thread(
            self._write_atomic_bundle, tile_path, manifest_path, data, manifest
        )

    # -------------------- Aggregated / multi-layer API --------------------
    def _aggregated_root(
        self, layers_str: str, cache_scope: str = "", project_id: str = None
    ) -> Path:
        layers_hash = hashlib.sha1(layers_str.encode()).hexdigest()[:8]
        project_folder = str(project_id) if project_id is not None else self._namespace
        scope_dir = _sanitize_scope_token(cache_scope)
        return self._base / project_folder / "_aggregated" / layers_hash / scope_dir

    def _aggregated_tile_dir(
        self,
        z: int,
        x: int,
        y: int,
        layers_str: str,
        cache_scope: str = "",
        project_id: str = None,
    ) -> Path:
        return (
            self._aggregated_root(layers_str, cache_scope, project_id)
            / f"z={z}"
            / f"x={x}"
            / f"y={y}"
        )

    def _aggregated_tile_path(
        self,
        z: int,
        x: int,
        y: int,
        layers_str: str,
        cache_scope: str = "",
        project_id: str = None,
    ) -> Path:
        return (
            self._aggregated_tile_dir(z, x, y, layers_str, cache_scope, project_id)
            / "tile.pbf"
        )

    def _aggregated_gz_path(
        self,
        z: int,
        x: int,
        y: int,
        layers_str: str,
        cache_scope: str = "",
        project_id: str = None,
    ) -> Path:
        return (
            self._aggregated_tile_dir(z, x, y, layers_str, cache_scope, project_id)
            / "tile.pbf.gz"
        )

    def _aggregated_manifest_path(
        self,
        z: int,
        x: int,
        y: int,
        layers_str: str,
        cache_scope: str = "",
        project_id: str = None,
    ) -> Path:
        return (
            self._aggregated_tile_dir(z, x, y, layers_str, cache_scope, project_id)
            / "manifest.json"
        )

    async def get_aggregated(
        self,
        z: int,
        x: int,
        y: int,
        layers_str: str,
        cache_scope: str = "",
        project_id: str = None,
    ) -> Optional[bytes]:
        """
        Devuelve el tile agregado para una combinación de capas o None si no existe/expiró.

        English:
            Returns the aggregated tile bytes for a given `layers_str` or `None`
            if the aggregated file is missing or has expired. Aggregated tiles
            are stored under the `_aggregated` subtree using a hash of the
            layers string.
        """
        path = self._aggregated_tile_path(z, x, y, layers_str, cache_scope, project_id)
        logger.debug(
            "get_aggregated: path=%s layers=%s cache_scope=%s project_id=%s",
            path,
            layers_str,
            cache_scope,
            project_id,
        )
        result = await asyncio.to_thread(self._read_if_valid, path)
        if result is not None:
            logger.info(
                "aggregated cache hit z=%s x=%s y=%s layers=%s size=%d",
                z,
                x,
                y,
                layers_str,
                len(result),
            )
        else:
            logger.debug("aggregated cache miss path=%s", path)
        return result

    async def get_aggregated_gz(
        self,
        z: int,
        x: int,
        y: int,
        layers_str: str,
        cache_scope: str = "",
        project_id: str = None,
    ) -> Optional[bytes]:
        """
        Devuelve la versión gzip del tile agregado si existe.

        English:
            Returns the gzipped aggregated tile if present; otherwise None.
            Useful to serve compressed responses directly when available.
        """
        path = self._aggregated_gz_path(z, x, y, layers_str, cache_scope, project_id)
        logger.debug(
            "get_aggregated_gz: path=%s layers=%s cache_scope=%s project_id=%s",
            path,
            layers_str,
            cache_scope,
            project_id,
        )
        result = await asyncio.to_thread(self._read_if_valid, path)
        if result is not None:
            logger.info(
                "aggregated.gz cache hit z=%s x=%s y=%s layers=%s size=%d",
                z,
                x,
                y,
                layers_str,
                len(result),
            )
        else:
            logger.debug("aggregated.gz cache miss path=%s", path)
        return result

    async def set_aggregated(
        self,
        z: int,
        x: int,
        y: int,
        layers_str: str,
        data: bytes,
        cache_scope: str = "",
        project_id: str = None,
    ) -> None:
        """
        Guarda el tile agregado en disco junto con su manifest.

        English:
            Stores the aggregated tile bytes on disk together with a manifest
            JSON. The aggregated key is derived from a hash of `layers_str`.
        """
        if not data:
            return
        tile_path = self._aggregated_tile_path(
            z, x, y, layers_str, cache_scope, project_id
        )
        manifest_path = self._aggregated_manifest_path(
            z, x, y, layers_str, cache_scope, project_id
        )
        manifest = {
            "project_folder": (
                str(project_id) if project_id is not None else self._namespace
            ),
            "layers": layers_str,
            "cache_scope": _sanitize_scope_token(cache_scope),
            "tile_file": "tile.pbf",
            "zoom": z,
            "x": x,
            "y": y,
            "size_bytes": len(data),
            "ttl_seconds": self._ttl,
            "created_at": time.time(),
            "aggregated": True,
        }
        await asyncio.to_thread(
            self._write_atomic_bundle, tile_path, manifest_path, data, manifest
        )

    async def set_aggregated_gz(
        self,
        z: int,
        x: int,
        y: int,
        layers_str: str,
        data: bytes,
        cache_scope: str = "",
        project_id: str = None,
    ) -> None:
        """
        Genera y escribe la versión gzip del tile agregado de forma atómica.
        Esta operación puede ejecutarse en background sin bloquear la respuesta.
        """
        if not data:
            return
        try:
            # Comprimir en thread pool para evitar bloquear el event loop
            compressed = await asyncio.to_thread(gzip.compress, data)
            gz_path = self._aggregated_gz_path(
                z, x, y, layers_str, cache_scope, project_id
            )
            await asyncio.to_thread(self._write_atomic, gz_path, compressed)
        except Exception:
            # No propagamos errores de escritura de compresión
            pass

    async def invalidate_layer(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        cache_scope: str = "",
    ) -> None:
        """Elimina un tile específico de la caché."""
        tile_dir = self._tile_dir(z, x, y, schema, table, geom_col, cache_scope)
        await asyncio.to_thread(self._delete_dir, tile_dir)

    async def invalidate_zoom(self, z: int) -> None:
        """Elimina todos los tiles de un nivel de zoom."""
        await asyncio.to_thread(self._delete_zoom, z)

    async def clear(self) -> None:
        """Vacía toda la caché eliminando el directorio raíz y recreándolo."""
        await asyncio.to_thread(self._delete_dir, self._root)
        await asyncio.to_thread(self._root.mkdir, parents=True, exist_ok=True)

    async def describe_layer(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        cache_scope: str = "",
    ) -> Optional[dict[str, Any]]:
        """Devuelve el manifest del tile si existe y no ha expirado."""
        path = self._manifest_path(z, x, y, schema, table, geom_col, cache_scope)
        return await asyncio.to_thread(self._read_manifest_if_valid, path)

    async def get(self, z: int, x: int, y: int, layers_str: str) -> Optional[bytes]:
        """Compatibilidad hacia atrás para una sola capa schema.table.geom_col."""
        schema, table, geom_col = self._parse_single_layer_key(layers_str)
        return await self.get_layer(z, x, y, schema, table, geom_col)

    async def set(self, z: int, x: int, y: int, layers_str: str, data: bytes) -> None:
        """Compatibilidad hacia atrás para una sola capa schema.table.geom_col."""
        schema, table, geom_col = self._parse_single_layer_key(layers_str)
        await self.set_layer(z, x, y, schema, table, geom_col, data, "")

    async def invalidate(self, z: int, x: int, y: int, layers_str: str) -> None:
        """Compatibilidad hacia atrás para una sola capa schema.table.geom_col."""
        schema, table, geom_col = self._parse_single_layer_key(layers_str)
        await self.invalidate_layer(z, x, y, schema, table, geom_col)

    async def describe(
        self, z: int, x: int, y: int, layers_str: str
    ) -> Optional[dict[str, Any]]:
        """Compatibilidad hacia atrás para una sola capa schema.table.geom_col."""
        schema, table, geom_col = self._parse_single_layer_key(layers_str)
        return await self.describe_layer(z, x, y, schema, table, geom_col)

    def _parse_single_layer_key(self, layer_key: str) -> tuple[str, str, str]:
        if "," in layer_key:
            raise ValueError(
                "FilesystemTileCache ahora almacena una sola capa por archivo. "
                "Usa get_layer/set_layer con una clave schema.table.geom_col."
            )
        return _split_layer_key(layer_key)

    # ──────────────────────────── Operaciones sync (thread pool) ──────────────

    def _read_if_valid(self, path: Path) -> Optional[bytes]:
        try:
            stat = path.stat()
        except FileNotFoundError:
            logger.debug("_read_if_valid: missing %s", path)
            return None
        # TTL check based on mtime
        if self._ttl > 0 and (time.time() - stat.st_mtime) > self._ttl:
            # Expirado: borrar para no acumular archivos viejos
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            logger.info(
                "_read_if_valid: expired %s (age=%.1fs ttl=%s)",
                path,
                time.time() - stat.st_mtime,
                self._ttl,
            )
            return None

        try:
            data = path.read_bytes()
            logger.debug("_read_if_valid: read %s size=%d", path, len(data))
            return data
        except OSError:
            logger.exception("_read_if_valid: error reading %s", path)
            return None

    def _read_manifest_if_valid(self, path: Path) -> Optional[dict[str, Any]]:
        try:
            stat = path.stat()
        except FileNotFoundError:
            return None

        if self._ttl > 0 and (time.time() - stat.st_mtime) > self._ttl:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            return None

        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def _delete_zoom(self, z: int) -> None:
        import shutil

        for path in list(self._root.rglob(f"z={z}")):
            if path.is_dir():
                try:
                    shutil.rmtree(path, ignore_errors=True)
                except OSError:
                    pass

    def _write_atomic(self, path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(".tmp")
        try:
            tmp_path.write_bytes(data)
            tmp_path.rename(path)
        except OSError:
            # No es crítico: el tile se regenerará en el siguiente request
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _write_atomic_bundle(
        self,
        tile_path: Path,
        manifest_path: Path,
        data: bytes,
        manifest: dict[str, Any],
    ) -> None:
        tile_path.parent.mkdir(parents=True, exist_ok=True)
        tile_tmp = tile_path.with_suffix(".tmp")
        manifest_tmp = manifest_path.with_suffix(".tmp")
        try:
            tile_tmp.write_bytes(data)
            manifest_tmp.write_text(
                json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            tile_tmp.rename(tile_path)
            manifest_tmp.rename(manifest_path)
        except OSError:
            try:
                tile_tmp.unlink(missing_ok=True)
            except OSError:
                pass
            try:
                manifest_tmp.unlink(missing_ok=True)
            except OSError:
                pass

    def _delete_file(self, path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass

    def _delete_dir(self, path: Path) -> None:
        import shutil

        try:
            shutil.rmtree(path, ignore_errors=True)
        except OSError:
            pass

    # ──────────────────────────── Utilidades ──────────────────────────────────

    def cache_size_bytes(self) -> int:
        """Devuelve el tamaño total en bytes de los archivos en caché (operación sync)."""
        total = 0
        for f in self._root.rglob("*.pbf"):
            try:
                total += f.stat().st_size
            except OSError:
                pass
        return total

    def __repr__(self) -> str:
        return f"FilesystemTileCache(cache_dir={str(self._root)!r}, ttl={self._ttl}s)"


class RedisTileCache:
    """
    Caché de tiles en Redis (async). Guarda tanto el blob PBF como
    un manifest JSON separado. Está pensado para usarse junto a
    `FilesystemTileCache` en un `HybridTileCache`.
    """

    def __init__(self, redis_url: str, ttl: int = 3600, namespace: str = "mvt"):
        try:
            import redis.asyncio as aioredis  # tipo: ignore
        except Exception as exc:  # pragma: no cover - entorno sin deps
            raise RuntimeError(
                "Para usar RedisTileCache instale el paquete 'redis' (redis-py >= 4.x)"
            ) from exc

        self._client = aioredis.from_url(redis_url)
        # ttl <= 0 disables expiry: filesystem already treats ttl<=0 as
        # non-expiring (checks `if self._ttl > 0`). For Redis we must
        # avoid passing `ex=0` (which would expire immediately), so
        # we keep the int but only pass `ex` when > 0.
        self._ttl = ttl
        self._namespace = namespace.strip() or "mvt"

    def _prefix(
        self, schema: str, table: str, geom_col: str, project_id: Optional[str]
    ) -> str:
        project_folder = str(project_id) if project_id is not None else self._namespace
        return f"geo_tiles:{project_folder}:{schema}:{table}:{geom_col}"

    def _tile_key(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        project_id: Optional[str],
        cache_scope: str = "",
    ) -> str:
        prefix = self._prefix(schema, table, geom_col, project_id)
        scope = _sanitize_scope_token(cache_scope)
        return f"{prefix}:{scope}:z={z}:x={x}:y={y}:tile"

    def _manifest_key(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        project_id: Optional[str],
        cache_scope: str = "",
    ) -> str:
        prefix = self._prefix(schema, table, geom_col, project_id)
        scope = _sanitize_scope_token(cache_scope)
        return f"{prefix}:{scope}:z={z}:x={x}:y={y}:manifest"

    async def get_layer(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        cache_scope: str = "",
        project_id: Optional[str] = None,
    ) -> Optional[bytes]:
        key = self._tile_key(z, x, y, schema, table, geom_col, project_id, cache_scope)
        try:
            data = await self._client.get(key)
            return data if data is not None else None
        except Exception:
            return None

    async def set_layer(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        data: bytes,
        cache_scope: str = "",
        project_id: Optional[str] = None,
    ) -> None:
        if not data:
            return
        tile_key = self._tile_key(
            z, x, y, schema, table, geom_col, project_id, cache_scope
        )
        manifest_key = self._manifest_key(
            z, x, y, schema, table, geom_col, project_id, cache_scope
        )
        manifest = {
            "project_folder": (
                str(project_id) if project_id is not None else self._namespace
            ),
            "schema": schema,
            "table": table,
            "geom_col": geom_col,
            "cache_scope": _sanitize_scope_token(cache_scope),
            "layer": f"{schema}.{table}.{geom_col}",
            "zoom": z,
            "x": x,
            "y": y,
            "tile_file": "tile.pbf",
            "size_bytes": len(data),
            "ttl_seconds": self._ttl,
            "created_at": time.time(),
        }
        try:
            pipe = self._client.pipeline()
            if self._ttl and self._ttl > 0:
                pipe.set(tile_key, data, ex=self._ttl)
                pipe.set(
                    manifest_key,
                    json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),
                    ex=self._ttl,
                )
            else:
                # ttl <= 0 -> do not set expiration on Redis keys
                pipe.set(tile_key, data)
                pipe.set(
                    manifest_key,
                    json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),
                )
            await pipe.execute()
        except Exception:
            # No propagamos la excepción; la caché queda eventualmente coherente
            pass

    # ─────────────────── Lock / coordination utilities (Redis) ──────────────
    def _lock_key(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        project_id: Optional[str],
        cache_scope: str = "",
    ) -> str:
        base = self._tile_key(z, x, y, schema, table, geom_col, project_id, cache_scope)
        return f"lock:{base}"

    async def acquire_generation_lock(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        cache_scope: str = "",
        project_id: Optional[str] = None,
        ttl: int = 30,
    ) -> Optional[str]:
        """Try to acquire a lock for generating this tile. Returns a token if acquired, else None."""
        key = self._lock_key(z, x, y, schema, table, geom_col, project_id, cache_scope)
        token = uuid.uuid4().hex
        try:
            ok = await self._client.set(key, token, nx=True, ex=ttl)
            return token if ok else None
        except Exception:
            return None

    async def release_generation_lock(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        token: str,
        cache_scope: str = "",
        project_id: Optional[str] = None,
    ) -> None:
        """Release lock only if token matches (atomic)."""
        key = self._lock_key(z, x, y, schema, table, geom_col, project_id, cache_scope)
        try:
            # Lua script to compare and del
            script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
            await self._client.eval(script, 1, key, token)
        except Exception:
            pass

    async def wait_for_tile(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        cache_scope: str = "",
        project_id: Optional[str] = None,
        timeout: int = 15,
        poll_interval: float = 0.25,
    ) -> Optional[bytes]:
        """Polls for the tile to appear in Redis until timeout."""
        end = time.time() + float(timeout)
        while time.time() < end:
            try:
                v = await self.get_layer(
                    z, x, y, schema, table, geom_col, cache_scope, project_id
                )
                if v is not None:
                    return v
            except Exception:
                pass
            await asyncio.sleep(poll_interval)
        return None

    async def invalidate_layer(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        cache_scope: str = "",
        project_id: Optional[str] = None,
    ) -> None:
        tile_key = self._tile_key(
            z, x, y, schema, table, geom_col, project_id, cache_scope
        )
        manifest_key = self._manifest_key(
            z, x, y, schema, table, geom_col, project_id, cache_scope
        )
        try:
            await self._client.delete(tile_key, manifest_key)
        except Exception:
            pass

    async def invalidate_zoom(self, z: int) -> None:
        # Eliminar claves que contengan `z={z}`. Scan puede ser costoso en grandes despliegues.
        pattern = f"geo_tiles:*:z={z}:*"
        try:
            async for k in self._client.scan_iter(match=pattern):
                await self._client.delete(k)
        except Exception:
            pass

    async def clear(self) -> None:
        pattern = "geo_tiles:*"
        try:
            async for k in self._client.scan_iter(match=pattern):
                await self._client.delete(k)
        except Exception:
            pass

    async def describe_layer(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        cache_scope: str = "",
        project_id: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        manifest_key = self._manifest_key(
            z, x, y, schema, table, geom_col, project_id, cache_scope
        )
        try:
            txt = await self._client.get(manifest_key)
            if not txt:
                return None
            if isinstance(txt, (bytes, bytearray)):
                txt = txt.decode("utf-8")
            return json.loads(txt)
        except Exception:
            return None

    async def get(self, z: int, x: int, y: int, layers_str: str) -> Optional[bytes]:
        schema, table, geom_col = _split_layer_key(layers_str)
        return await self.get_layer(z, x, y, schema, table, geom_col)

    async def set(self, z: int, x: int, y: int, layers_str: str, data: bytes) -> None:
        schema, table, geom_col = _split_layer_key(layers_str)
        await self.set_layer(z, x, y, schema, table, geom_col, data, "")

    async def invalidate(self, z: int, x: int, y: int, layers_str: str) -> None:
        schema, table, geom_col = _split_layer_key(layers_str)
        await self.invalidate_layer(z, x, y, schema, table, geom_col, "")

    def cache_size_bytes(self) -> int:
        # Operación costosa y aproximada: no implementada con precisión
        raise NotImplementedError("cache_size_bytes no soportado para RedisTileCache")


class HybridTileCache:
    """
    Combina un `FilesystemTileCache` con un backend remoto (por ejemplo Redis).

    Estrategia por defecto:
    - Lectura: intentar backend remoto, luego disco; si disco tiene valor, repoblar remoto.
    - Escritura: escribir en ambos (escritura-síncrona por simplicidad).
    - Invalidación: eliminar en ambos.
    """

    def __init__(
        self,
        filesystem_cache: FilesystemTileCache,
        remote_cache: Optional[RedisTileCache] = None,
    ):
        self._fs = filesystem_cache
        self._remote = remote_cache

    async def get_layer(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        cache_scope: str = "",
        project_id: Optional[str] = None,
    ) -> Optional[bytes]:
        if self._remote is not None:
            try:
                v = await self._remote.get_layer(
                    z, x, y, schema, table, geom_col, cache_scope, project_id
                )
                if v is not None:
                    return v
            except Exception:
                pass

        # Fallback a disco
        v = await self._fs.get_layer(
            z, x, y, schema, table, geom_col, cache_scope, project_id
        )
        if v is not None and self._remote is not None:
            # Repoblar remoto (fire-and-forget-ish)
            try:
                await self._remote.set_layer(
                    z, x, y, schema, table, geom_col, v, cache_scope, project_id
                )
            except Exception:
                pass
        return v

    async def set_layer(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        data: bytes,
        cache_scope: str = "",
        project_id: Optional[str] = None,
    ) -> None:
        tasks = [
            self._fs.set_layer(
                z, x, y, schema, table, geom_col, data, cache_scope, project_id
            )
        ]
        if self._remote is not None:
            tasks.append(
                self._remote.set_layer(
                    z, x, y, schema, table, geom_col, data, cache_scope, project_id
                )
            )
        await asyncio.gather(*tasks)

    async def invalidate_layer(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        cache_scope: str = "",
        project_id: Optional[str] = None,
    ) -> None:
        tasks = [
            self._fs.invalidate_layer(z, x, y, schema, table, geom_col, cache_scope)
        ]
        if self._remote is not None:
            tasks.append(
                self._remote.invalidate_layer(
                    z, x, y, schema, table, geom_col, cache_scope, project_id
                )
            )
        await asyncio.gather(*tasks)

    async def invalidate_zoom(self, z: int) -> None:
        tasks = [self._fs.invalidate_zoom(z)]
        if self._remote is not None:
            tasks.append(self._remote.invalidate_zoom(z))
        await asyncio.gather(*tasks)

    async def clear(self) -> None:
        tasks = [self._fs.clear()]
        if self._remote is not None:
            tasks.append(self._remote.clear())
        await asyncio.gather(*tasks)

    async def describe_layer(
        self,
        z: int,
        x: int,
        y: int,
        schema: str,
        table: str,
        geom_col: str,
        cache_scope: str = "",
        project_id: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        if self._remote is not None:
            try:
                m = await self._remote.describe_layer(
                    z, x, y, schema, table, geom_col, cache_scope, project_id
                )
                if m is not None:
                    return m
            except Exception:
                pass
        return await self._fs.describe_layer(
            z, x, y, schema, table, geom_col, cache_scope
        )

    async def get(self, z: int, x: int, y: int, layers_str: str) -> Optional[bytes]:
        schema, table, geom_col = self._fs._parse_single_layer_key(layers_str)
        return await self.get_layer(z, x, y, schema, table, geom_col)

    async def set(self, z: int, x: int, y: int, layers_str: str, data: bytes) -> None:
        schema, table, geom_col = self._fs._parse_single_layer_key(layers_str)
        await self.set_layer(z, x, y, schema, table, geom_col, data)

    async def invalidate(self, z: int, x: int, y: int, layers_str: str) -> None:
        schema, table, geom_col = self._fs._parse_single_layer_key(layers_str)
        await self.invalidate_layer(z, x, y, schema, table, geom_col)

    def cache_size_bytes(self) -> int:
        try:
            return self._fs.cache_size_bytes()
        except Exception:
            raise

    # ------------------ Aggregated / multi-layer passthrough -----------------
    async def get_aggregated(
        self,
        z: int,
        x: int,
        y: int,
        layers_str: str,
        cache_scope: str = "",
        project_id: Optional[str] = None,
    ) -> Optional[bytes]:
        return await self._fs.get_aggregated(
            z, x, y, layers_str, cache_scope, project_id
        )

    async def get_aggregated_gz(
        self,
        z: int,
        x: int,
        y: int,
        layers_str: str,
        cache_scope: str = "",
        project_id: Optional[str] = None,
    ) -> Optional[bytes]:
        return await self._fs.get_aggregated_gz(
            z, x, y, layers_str, cache_scope, project_id
        )

    async def set_aggregated(
        self,
        z: int,
        x: int,
        y: int,
        layers_str: str,
        data: bytes,
        cache_scope: str = "",
        project_id: Optional[str] = None,
    ) -> None:
        await self._fs.set_aggregated(
            z, x, y, layers_str, data, cache_scope, project_id
        )

    async def set_aggregated_gz(
        self,
        z: int,
        x: int,
        y: int,
        layers_str: str,
        data: bytes,
        cache_scope: str = "",
        project_id: Optional[str] = None,
    ) -> None:
        await self._fs.set_aggregated_gz(
            z, x, y, layers_str, data, cache_scope, project_id
        )
