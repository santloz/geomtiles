"""MetadataRepository: consultas a information_schema y pg_catalog.

English:
MetadataRepository: queries against information_schema and pg_catalog.
"""

import re
from typing import List, Optional

from sqlalchemy import text

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_id(name: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Identificador SQL inválido: {name!r}")
    return name


class MetadataRepository:
    """Acceso a metadatos de tablas, vistas y columnas en PostgreSQL.

    English:
    Access to table, view and column metadata in PostgreSQL.
    """

    def __init__(self, session_factory):
        self._session_factory = session_factory

    async def get_columns(
        self,
        schema: str,
        table: str,
        exclude_columns: Optional[List[str]] = None,
    ) -> List[str]:
        """
        Devuelve la lista de columnas de una tabla/vista desde information_schema.

        English:
            Returns the list of column names for a table/view from
            information_schema.

        Args:
            schema: Nombre del esquema PostgreSQL.
            table: Nombre de la tabla o vista.
            exclude_columns: Columnas a excluir del resultado.

        Returns:
            Lista de nombres de columna en orden de posición.
        """
        _safe_id(schema)
        _safe_id(table)
        exclude = set(exclude_columns or [])

        sql = text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name   = :table
            ORDER BY ordinal_position
        """
        )
        async with self._session_factory() as session:  # type: AsyncSession
            result = await session.execute(sql, {"schema": schema, "table": table})
            # Validamos que los nombres de columna sean identificadores seguros
            return [
                row[0]
                for row in result.fetchall()
                if row[0] not in exclude and _IDENTIFIER_RE.match(row[0])
            ]

    async def table_exists(self, schema: str, table: str) -> bool:
        """Verifica si una tabla o vista existe en el esquema dado.

        English:
            Checks whether a table or view exists in the given schema.
        """
        _safe_id(schema)
        _safe_id(table)
        sql = text(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_name   = :table
            LIMIT 1
        """
        )
        async with self._session_factory() as session:
            result = await session.execute(sql, {"schema": schema, "table": table})
            return result.scalar() is not None

    async def resolve_base_table(self, schema: str, view_name: str) -> Optional[str]:
        """
        Intenta resolver la tabla base de una vista consultando pg_depend / pg_rewrite.

        English:
            Attempts to resolve a view's base table by querying pg_depend and
            pg_rewrite. Returns the base table name or None if it cannot be
            resolved (for example, when the object is a table, not a view).
        """
        _safe_id(schema)
        _safe_id(view_name)
        sql = text(
            """
            SELECT c.relname AS base_table
            FROM pg_depend    d
            JOIN pg_rewrite   r ON r.oid       = d.objid
            JOIN pg_class     v ON v.oid       = r.ev_class
            JOIN pg_class     c ON c.oid       = d.refobjid
            JOIN pg_namespace n ON n.oid       = v.relnamespace
            WHERE v.relname  = :view_name
              AND n.nspname  = :schema
              AND c.relkind  = 'r'
            LIMIT 1
        """
        )
        async with self._session_factory() as session:
            result = await session.execute(
                sql, {"view_name": view_name, "schema": schema}
            )
            return result.scalar()

    async def discover_tables(self, schema: str) -> List[str]:
        """
        Lista todas las tablas y vistas materializadas de un esquema.

        English:
            Lists all tables and materialized views in a schema.

        Returns:
            Lista de nombres de tabla en orden alfabético.
        """
        _safe_id(schema)
        sql = text(
            """
            SELECT c.relname FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = :schema AND c.relkind IN ('r', 'm')
            ORDER BY c.relname
        """
        )
        async with self._session_factory() as session:
            result = await session.execute(sql, {"schema": schema})
            return [row[0] for row in result.fetchall() if _IDENTIFIER_RE.match(row[0])]

    async def find_geom_col(
        self,
        schema: str,
        table: str,
        candidates: Optional[List[str]] = None,
    ) -> Optional[str]:
        """
        Devuelve el nombre de la primera columna de geometría encontrada en la tabla.

        English:
            Returns the name of the first geometry column found in a table.

        Args:
            schema: Nombre del esquema.
            table: Nombre de la tabla o vista.
            candidates: Columnas a buscar en orden de preferencia.
                        Por defecto ['geom', 'layout_geom'].

        Returns:
            Nombre de la columna o None si no se encuentra ninguna.
        """
        _safe_id(schema)
        _safe_id(table)
        cols = candidates or ["geom", "layout_geom"]
        sql = text(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
              AND udt_name = 'geometry'
              AND column_name = ANY(:cols)
            ORDER BY ordinal_position
            LIMIT 1
        """
        )
        async with self._session_factory() as session:
            result = await session.execute(
                sql, {"schema": schema, "table": table, "cols": cols}
            )
            row = result.fetchone()
            return row[0] if row else None
