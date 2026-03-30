"""
MetadataService: resuelve tablas base, columnas y cachea los resultados con TTL.

English:
MetadataService resolves base tables, column lists and caches results with a TTL.
"""

import re
from dataclasses import dataclass
from typing import List, Optional, Tuple

from ..repositories.metadata import MetadataRepository
from ..utils.cache import TTLCache
from ..utils.metrics import metrics

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class LayerMetadata:
    resolved_table: str
    columns: list[str]
    has_is_deleted: bool
    has_project_id: bool
    has_id_auto: bool


def _safe_id(name: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Identificador SQL inválido: {name!r}")
    return name


class MetadataService:
    """
    Servicio de metadatos con caché TTL en memoria.

    Cachea la resolución de tablas base y la lista de columnas para evitar
    consultas repetidas a information_schema en cada tile.
    """

    """
    English:
        Metadata service with an in-memory TTL cache. It centralizes base-table
        resolution and column detection so services do not need to access the
        repository directly on every request.
    """

    def __init__(self, session_factory, cache_ttl: int = 300):
        self._repo = MetadataRepository(session_factory)
        self._cache = TTLCache(ttl=cache_ttl)

    async def get_raw_columns(
        self,
        schema: str,
        table: str,
        exclude_columns: Optional[List[str]] = None,
    ) -> list[str]:
        """Devuelve la lista cruda de columnas excluyendo las indicadas."""
        _safe_id(schema)
        _safe_id(table)
        excluded = set(exclude_columns or [])
        return await self._repo.get_columns(
            schema, table, exclude_columns=list(excluded)
        )

    async def discover_tables(self, schema: str) -> list[str]:
        _safe_id(schema)
        return await self._repo.discover_tables(schema)

    async def table_exists(self, schema: str, table: str) -> bool:
        _safe_id(schema)
        _safe_id(table)
        return await self._repo.table_exists(schema, table)

    async def find_geom_col(self, schema: str, table: str) -> Optional[str]:
        _safe_id(schema)
        _safe_id(table)
        return await self._repo.find_geom_col(schema, table)

    async def has_column(self, schema: str, table: str, column: str) -> bool:
        """Comprueba si una columna existe en la tabla (proxy al repositorio)."""
        _safe_id(schema)
        _safe_id(table)
        _safe_id(column)
        return await self._repo.has_column(schema, table, column)

    async def describe_layer(
        self,
        schema: str,
        table: str,
        geom_column: str,
        exclude_extra: Optional[List[str]] = None,
        resolve_base_table: bool = True,
    ) -> LayerMetadata:
        """
        Devuelve metadatos de capa listos para generar SQL de tiles.

        Centraliza la resolución de la tabla base y la detección de columnas
        para que los servicios no tengan que acceder a _repo directamente.
        """
        """
        Devuelve metadatos de capa listos para generar SQL de tiles.

        English:
            Return layer metadata ready to build MVT SQL. Centralizes base table
            resolution and column detection so higher-level services do not
            query the repository directly.
        """
        _safe_id(schema)
        _safe_id(table)
        _safe_id(geom_column)
        metrics.increment("metadata.describe_layer")
        
        resolved_table = (
            await self.resolve_table_with_fallback(schema, table)
            if resolve_base_table
            else table
        )
        excluded = [geom_column, "geom", *(exclude_extra or [])]
        columns = await self._repo.get_columns(
            schema, resolved_table, exclude_columns=excluded
        )

        return LayerMetadata(
            resolved_table=resolved_table,
            columns=columns,
            has_is_deleted="is_deleted" in columns,
            has_project_id="project_id" in columns,
            has_id_auto="id_auto" in columns,
        )

    async def get_columns(
        self,
        schema: str,
        table: str,
        geom_column: str,
        exclude_extra: Optional[List[str]] = None,
    ) -> Tuple[str, str]:
        """
        Devuelve (columns_str, clustered_columns_str) para incrustar en SQL MVT.

        - columns_str: columnas no-geométricas separadas por coma.
        - clustered_columns_str: mismas columnas envueltas en MIN() para GROUP BY en clustering.

        El resultado se cachea con el TTL configurado.
        """
        """
        Devuelve (columns_str, clustered_columns_str) para incrustar en SQL MVT.

        English:
            Return (columns_str, clustered_columns_str) to be embedded in MVT SQL.
            - columns_str: non-geometry columns separated by commas.
            - clustered_columns_str: same columns wrapped in MIN() for GROUP BY.

        The result is cached using the configured TTL.
        """
        _safe_id(schema)
        _safe_id(table)
        cache_key = f"cols:{schema}.{table}.{geom_column}"
        cached = await self._cache.get(cache_key)
        if cached is not None:
            return cached

        excluded = {geom_column, "geom", *(exclude_extra or [])}
        metrics.increment("metadata.get_columns")
        cols = await self._repo.get_columns(
            schema, table, exclude_columns=list(excluded)
        )

        if cols:
            columns_str = ", ".join(cols)
            clustered_columns_str = ", ".join(f"MIN({c}) AS {c}" for c in cols)
        else:
            columns_str = "NULL::text AS _empty"
            clustered_columns_str = "NULL::text AS _empty"

        result = (columns_str, clustered_columns_str)
        await self._cache.set(cache_key, result)
        return result

    async def resolve_table_with_fallback(self, schema: str, table: str) -> str:
        """
        Resuelve la tabla base de una vista; devuelve 'table' si no hay resolución.

        El resultado se cachea para evitar consultas repetidas a pg_depend.
        """
        """
        Resuelve la tabla base de una vista; devuelve 'table' si no hay resolución.

        English:
            Resolve the base table name for a view; return the original table
            name if no base table is found. Result is cached to avoid repeated
            pg_depend queries.
        """
        _safe_id(schema)
        _safe_id(table)
        cache_key = f"base:{schema}.{table}"
        cached = await self._cache.get(cache_key)
        if cached is not None:
            return cached

        base = await self._repo.resolve_base_table(schema, table)
        resolved = base if base else table
        await self._cache.set(cache_key, resolved)
        return resolved

    async def invalidate(self, schema: str, table: str) -> None:
        """Invalida manualmente la caché para una tabla concreta.

        English:
            Manually invalidate the cache for a specific table.
        """
        await self._cache.delete(f"cols:{schema}.{table}.geom")
        await self._cache.delete(f"base:{schema}.{table}")
