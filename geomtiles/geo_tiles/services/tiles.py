"""
TileService: orquesta la generación de tiles MVT multicapa con concurrencia controlada.

English:
Orchestrates multi-layer MVT tile generation with controlled concurrency.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

from ..domain.exceptions import InvalidTileCoordinateError
from ..domain.models import LayerConfig, PolygonTileRequest, TileRequest
from ..repositories.tiles import TileRepository
from ..services.metadata import MetadataService
from ..sql.registry import get_generator, default_generator_name
from ..utils.geometry import is_valid_wkt
from ..utils.tiles import grid_size_for_zoom, tile_xyz_to_bbox

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_DEFAULT_MAX_CONCURRENT = 50
_POLYGON_TILE_EXTENT = 16384
_POLYGON_TILE_BUFFER = 256


def _parse_layer_str(layer_str: str) -> "tuple[str, str, str]":
    """
    Parsea 'schema.table.geom_col' y valida que los tres segmentos sean identificadores SQL seguros.

    Raises:
        ValueError: si el formato es incorrecto o contiene caracteres no permitidos.
    """
    parts = layer_str.strip().split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Formato de capa inválido: {layer_str!r}. Se esperaba 'schema.table.geom_col'."
        )
    schema, table, geom_col = parts
    for part in (schema, table, geom_col):
        if not _IDENTIFIER_RE.match(part):
            raise ValueError(f"Identificador SQL inválido en capa: {part!r}")
    return schema, table, geom_col


def _cache_scope_for_tile(force_zero: bool) -> str:
    payload = json.dumps(
        {"force_zero": bool(force_zero)}, sort_keys=True, separators=(",", ":")
    )
    return f"fz={int(force_zero)}-{hashlib.sha1(payload.encode()).hexdigest()[:8]}"


class TileService:
    """
    Servicio principal para generación de tiles MVT/PBF desde PostGIS.

    Soporta:
      - Tiles XYZ estándar (get_mvt_tile)
      - Tiles recortados a polígono WKT (get_mvt_polygon)
      - Registro de capas con LayerConfig
      - Caché de tiles por sistema de ficheros (opcional)

    La concurrencia se controla con un asyncio.Semaphore para evitar saturar
    el pool de conexiones cuando hay muchas capas por tile.

        English:
                Primary service for generating MVT/PBF tiles from PostGIS.

        Supports:
            - Standard XYZ tiles (get_mvt_tile)
            - Polygon-clipped tiles (get_mvt_polygon)
            - Layer registration via LayerConfig
            - Optional filesystem tile cache

        Concurrency is controlled by an asyncio.Semaphore to avoid exhausting
        database connection pools when many layers are processed concurrently.
    """

    def __init__(
        self,
        session_factory,
        layer_configs: Optional[Dict[str, LayerConfig]] = None,
        max_concurrent_layers: int = _DEFAULT_MAX_CONCURRENT,
        tile_cache=None,
        sql_generator: Optional[object] = None,
    ):
        """
        Args:
            session_factory: Factory de sesiones SQLAlchemy async.
            layer_configs: Capas pre-registradas (también se pueden añadir con register_layer).
            max_concurrent_layers: Máximo de capas procesadas en paralelo por tile.
            tile_cache: Instancia de FilesystemTileCache u otro objeto compatible.
                        Si es None, no se cachea nada en disco.
        """
        self._repo = TileRepository(session_factory)
        self._metadata = MetadataService(session_factory)
        self._layers: Dict[str, LayerConfig] = layer_configs or {}
        # Also index layers by canonical key schema.table.geom_col for quick lookup
        self._layers_by_key: Dict[str, LayerConfig] = {
            f"{c.schema}.{c.table}.{c.geom_column}": c
            for c in (layer_configs or {}).values()
        }
        self._semaphore = asyncio.Semaphore(max_concurrent_layers)
        self._tile_cache = tile_cache
        # sql_generator may be a callable or a string ('cow') to choose mvt_cow
        self._sql_generator = sql_generator

    @classmethod
    def from_dsn(cls, dsn: str, **kwargs) -> "TileService":
        """Crea un TileService a partir de un DSN de conexión."""
        from ..db import create_session_factory

        return cls(create_session_factory(dsn), **kwargs)

    def register_layer(self, config: LayerConfig) -> None:
        """Registra una capa con su configuración para uso posterior."""
        self._layers[config.name] = config
        try:
            key = f"{config.schema}.{config.table}.{config.geom_column}"
            self._layers_by_key[key] = config
        except Exception:
            pass

    async def get_raw_columns(
        self, schema: str, table: str, exclude_columns: Optional[List[str]] = None
    ) -> list[str]:
        """Proxy público a MetadataService.get_raw_columns."""
        return await self._metadata.get_raw_columns(
            schema, table, exclude_columns=exclude_columns
        )

    async def describe_layer_metadata(
        self,
        schema: str,
        table: str,
        geom_column: str,
        exclude_extra: Optional[List[str]] = None,
        resolve_base_table: bool = True,
    ):
        """Proxy público a MetadataService.describe_layer."""
        return await self._metadata.describe_layer(
            schema,
            table,
            geom_column,
            exclude_extra=exclude_extra,
            resolve_base_table=resolve_base_table,
        )

    async def discover_tables(self, schema: str) -> list[str]:
        """Proxy público a MetadataService.discover_tables."""
        return await self._metadata.discover_tables(schema)

    async def table_exists(self, schema: str, table: str) -> bool:
        """Proxy público a MetadataService.table_exists."""
        return await self._metadata.table_exists(schema, table)

    async def find_geom_col(self, schema: str, table: str) -> Optional[str]:
        """Proxy público a MetadataService.find_geom_col."""
        return await self._metadata.find_geom_col(schema, table)

    async def get_columns_sql(
        self,
        schema: str,
        table: str,
        geom_column: str,
        exclude_extra: Optional[List[str]] = None,
    ) -> tuple[str, str]:
        """Proxy público a MetadataService.get_columns."""
        return await self._metadata.get_columns(
            schema,
            table,
            geom_column,
            exclude_extra=exclude_extra,
        )

    @asynccontextmanager
    async def layer_slot(self):
        """Reserva un slot de concurrencia para trabajar con una capa."""
        async with self._semaphore:
            yield

    async def execute_tile_sql(self, sql: str) -> bytes:
        """Ejecuta SQL de tile y devuelve bytes PBF o b'' si no hay contenido."""
        tile = await self._repo.get_tile_bytes(sql)
        return bytes(tile) if tile else b""

    async def _get_layer_tile(
        self,
        schema: str,
        table: str,
        geom_col: str,
        envelope_sql: str,
        z: int,
        priority: int,
        force_zero: bool,
        minx: float | None = None,
        miny: float | None = None,
        maxx: float | None = None,
        maxy: float | None = None,
    ) -> bytes:
        """Obtiene los bytes del tile para una sola capa, respetando el semáforo."""
        async with self.layer_slot():
            layer_meta = await self.describe_layer_metadata(schema, table, geom_col)
            cols_str, clustered_cols_str = await self.get_columns_sql(
                schema,
                layer_meta.resolved_table,
                geom_col,
                exclude_extra=[geom_col],
            )
            # Decide which SQL generator to use: per-layer config overrides global
            key = f"{schema}.{layer_meta.resolved_table}.{geom_col}"
            layer_conf = self._layers_by_key.get(key)

            # Resolve generator name from layer config or global preference
            gen_callable = None
            # Per-layer explicit sql_mode (preferred)
            if layer_conf and getattr(layer_conf, "sql_mode", None):
                try:
                    gen_callable = get_generator(layer_conf.sql_mode)
                except KeyError:
                    gen_callable = get_generator(default_generator_name())
            elif layer_conf and getattr(layer_conf, "use_cow", False):
                try:
                    gen_callable = get_generator("cow")
                except KeyError:
                    gen_callable = get_generator(default_generator_name())
            elif callable(self._sql_generator):
                gen_callable = self._sql_generator
            elif isinstance(self._sql_generator, str):
                try:
                    gen_callable = get_generator(self._sql_generator)
                except KeyError:
                    gen_callable = get_generator(default_generator_name())
            else:
                gen_callable = get_generator(default_generator_name())

            # Call the selected generator. If it expects envelope_sql instead of bbox,
            # fall back to the legacy signature by detecting argument names.
            try:
                # If generator accepts bbox-style args, prefer those when available
                if gen_callable.__name__.lower().endswith("cow") and None not in (minx, miny, maxx, maxy):
                    # generator likely expects bbox numeric args
                    sql = gen_callable(
                        schema=layer_meta.resolved_table.split('.')[0] if '.' in layer_meta.resolved_table else schema,
                        table=layer_meta.resolved_table.split('.', 1)[-1] if '.' in layer_meta.resolved_table else layer_meta.resolved_table,
                        geom_col=geom_col,
                        minx=minx,
                        miny=miny,
                        maxx=maxx,
                        maxy=maxy,
                        z=z,
                        grid_size=grid_size_for_zoom(z),
                        columns_str=cols_str,
                        clustered_columns_str=clustered_cols_str,
                        priority=priority,
                        force_zero=force_zero,
                        project_ids=None,
                        exclude_project_ids=None,
                        id_gis=None,
                        has_is_deleted=layer_meta.has_is_deleted,
                    )
                else:
                    # Legacy generator signature: envelope_sql
                    sql = gen_callable(
                        schema=schema,
                        table=layer_meta.resolved_table,
                        geom_col=geom_col,
                        envelope_sql=envelope_sql,
                        z=z,
                        grid_size=grid_size_for_zoom(z),
                        columns_str=cols_str,
                        clustered_columns_str=clustered_cols_str,
                        priority=priority,
                        force_zero=force_zero,
                    )
            except TypeError:
                # Fallback: try calling with legacy signature
                sql = gen_callable(
                    schema=schema,
                    table=layer_meta.resolved_table,
                    geom_col=geom_col,
                    envelope_sql=envelope_sql,
                    z=z,
                    grid_size=grid_size_for_zoom(z),
                    columns_str=cols_str,
                    clustered_columns_str=clustered_cols_str,
                    priority=priority,
                    force_zero=force_zero,
                )
            return await self.execute_tile_sql(sql)

    async def _get_layer_tile_cached(
        self,
        schema: str,
        table: str,
        geom_col: str,
        envelope_sql: str,
        z: int,
        x: int,
        y: int,
        priority: int,
        force_zero: bool,
        minx: float | None = None,
        miny: float | None = None,
        maxx: float | None = None,
        maxy: float | None = None,
    ) -> bytes:
        """Obtiene una capa desde caché o la genera y la guarda por separado."""
        if self._tile_cache is not None:
            cache_scope = _cache_scope_for_tile(force_zero)
            cached = await self._tile_cache.get_layer(
                z, x, y, schema, table, geom_col, cache_scope
            )
            if cached is not None:
                return cached

        tile = await self._get_layer_tile(
            schema=schema,
            table=table,
            geom_col=geom_col,
            envelope_sql=envelope_sql,
            z=z,
            priority=priority,
            force_zero=force_zero,
            minx=minx,
            miny=miny,
            maxx=maxx,
            maxy=maxy,
        )

        if self._tile_cache is not None and tile:
            cache_scope = _cache_scope_for_tile(force_zero)
            await self._tile_cache.set_layer(
                z, x, y, schema, table, geom_col, tile, cache_scope
            )

        return tile

    async def get_mvt_tile(self, req: TileRequest) -> bytes:
        """
        Genera y concatena tiles MVT para las capas solicitadas en coordenadas XYZ.

        Si hay un tile_cache configurado, comprueba primero en disco antes de
        consultar PostGIS. Los tiles generados se almacenan automáticamente en caché.

        Args:
            req: TileRequest con z/x/y y lista de layers en formato "schema.table.geom_col".

        Returns:
            bytes PBF con todas las capas concatenadas.

        Raises:
            InvalidTileCoordinateError: si el zoom está fuera de rango (0-30).
            ValueError: si alguna capa tiene formato o identificadores inválidos.
        """
        """
        English:
            Generate and concatenate MVT tiles for requested layers at given XYZ coordinates.

        Raises:
            InvalidTileCoordinateError: if zoom is out of range (0-30).
            ValueError: if any layer has invalid format or identifiers.
        """
        if not (0 <= req.z <= 30):
            raise InvalidTileCoordinateError(
                f"Zoom inválido: {req.z}. Rango permitido: 0-30."
            )

        # ── Generación desde PostGIS ───────────────────────────────────────────
        minx, miny, maxx, maxy = tile_xyz_to_bbox(req.x, req.y, req.z)
        envelope_sql = f"ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}, 3857)"

        parsed_layers = [_parse_layer_str(ls) for ls in req.layers]

        tasks = [
            self._get_layer_tile_cached(
                schema=schema,
                table=table,
                geom_col=geom_col,
                envelope_sql=envelope_sql,
                z=req.z,
                x=req.x,
                y=req.y,
                priority=idx + 1,
                force_zero=req.force_point_count_zero,
                minx=minx,
                miny=miny,
                maxx=maxx,
                maxy=maxy,
            )
            for idx, (schema, table, geom_col) in enumerate(parsed_layers)
        ]

        results = await asyncio.gather(*tasks)
        tile_bytes = b"".join(r for r in results if r)

        return tile_bytes

    async def get_mvt_polygon(self, req: PolygonTileRequest) -> bytes:
        """
        Genera tiles MVT recortados al polígono WKT indicado.

        El WKT es validado estrictamente antes de incluirlo en el SQL para
        evitar inyección SQL. Solo se permiten caracteres numericos, de tipo
        geométrico, espacios, comas y paréntesis.

        Args:
            req: PolygonTileRequest con el WKT del polígono y las tablas a consultar.

        Returns:
            bytes PBF con todas las capas concatenadas.

        Raises:
            ValueError: si el WKT no es válido o algún nombre de tabla es inválido.
        """
        """
        English:
            Generate MVT tiles clipped to the provided WKT polygon.

        The WKT is strictly validated before embedding in SQL to avoid SQL
        injection. Only numeric, geometry-type characters, spaces, commas and
        parentheses are allowed.

        Raises:
            ValueError: if the WKT is invalid or a table name is invalid.
        """
        if not is_valid_wkt(req.polygon_wkt):
            raise ValueError(
                "WKT de polígono inválido o con caracteres no permitidos. "
                "Solo se aceptan tipos geometry estándar con coordenadas numéricas."
            )

        # El WKT ya fue validado: solo contiene chars seguros, sin comillas ni punto y coma.
        envelope_sql = f"ST_GeomFromText('{req.polygon_wkt}', 3857)"

        tables = req.tables or []
        tasks = [
            self._get_layer_tile(
                schema=req.schema,
                table=table,
                geom_col="geom",
                envelope_sql=envelope_sql,
                z=18,  # Zoom alto → sin clustering para polígonos completos
                priority=idx + 1,
                force_zero=req.force_point_count_zero,
            )
            for idx, table in enumerate(tables)
            if _IDENTIFIER_RE.match(table)  # Valida identificador antes de usar
        ]

        results = await asyncio.gather(*tasks)
        return b"".join(r for r in results if r)

    async def get_mvt_polygon_tile(
        self,
        schema: str,
        polygon_wkt: str,
        tables: Optional[List[str]] = None,
        force_zero: bool = False,
    ) -> bytes:
        """
        Genera un tile MVT recortado por polígono usando la API pública.

        Resuelve tablas, vistas y geometrías con los helpers públicos del servicio
        y concatena los PBF de cada tabla válida.
        """
        if not is_valid_wkt(polygon_wkt):
            raise ValueError("WKT de polígono inválido.")

        schema = schema.strip()
        if not _IDENTIFIER_RE.match(schema):
            raise ValueError(f"Identificador SQL inválido: {schema!r}")

        if tables:
            base_tables = [table.strip() for table in tables if table and table.strip()]
        else:
            base_tables = await self.discover_tables(schema)

        async def resolve_table(base_table: str) -> Optional[tuple[str, str]]:
            if not _IDENTIFIER_RE.match(base_table):
                return None
            view_name = f"{base_table}_view"
            table_name = (
                view_name if await self.table_exists(schema, view_name) else base_table
            )
            geom_col = await self.find_geom_col(schema, table_name)
            if not geom_col:
                return None
            return table_name, geom_col

        resolved = await asyncio.gather(
            *(resolve_table(table) for table in base_tables)
        )
        valid_layers = [
            (table_name, geom_col)
            for item in resolved
            if item
            for table_name, geom_col in [item]
        ]
        if not valid_layers:
            return b""

        poly_envelope = (
            f"ST_Envelope(ST_Transform(ST_GeomFromText('{polygon_wkt}', 4326), 3857))"
        )
        poly_intersect = f"ST_Transform(ST_GeomFromText('{polygon_wkt}', 4326), 3857)"
        point_count_expr = "0" if force_zero else "1"

        async def build_layer(table_name: str, geom_col: str, priority: int) -> bytes:
            async with self.layer_slot():
                layer_meta = await self.describe_layer_metadata(
                    schema,
                    table_name,
                    geom_col,
                    exclude_extra=[geom_col],
                )
                columns_str, _ = await self.get_columns_sql(
                    schema,
                    layer_meta.resolved_table,
                    geom_col,
                    exclude_extra=[geom_col],
                )
                cols_select = (
                    f", {columns_str}"
                    if columns_str and "NULL" not in columns_str
                    else ""
                )
                sql = f"""
                SELECT ST_AsMVT(mvtgeom, '{layer_meta.resolved_table}', {_POLYGON_TILE_EXTENT}, 'geom')
                FROM (
                    SELECT
                        ST_AsMVTGeom({geom_col}, {poly_envelope}, {_POLYGON_TILE_EXTENT}, {_POLYGON_TILE_BUFFER}, true) AS geom,
                        {point_count_expr} AS point_count,
                        id_gis{cols_select},
                        {priority} AS priority
                    FROM {schema}.{layer_meta.resolved_table}
                    WHERE ST_Intersects({geom_col}, {poly_intersect})
                ) AS mvtgeom
                """
                return await self.execute_tile_sql(sql)

        parts = await asyncio.gather(
            *(
                build_layer(table_name, geom_col, idx + 1)
                for idx, (table_name, geom_col) in enumerate(valid_layers)
            )
        )
        return b"".join(part for part in parts if part)
