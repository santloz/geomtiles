"""FeatureRepository: consultas geoespaciales que devuelven GeoJSON.

English:
FeatureRepository: geospatial queries that return GeoJSON.
"""

import re
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from ..sql.filters import build_where_clause

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_id(name: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Identificador SQL inválido: {name!r}")
    return name


class FeatureRepository:
    """
    Acceso a datos geoespaciales tipo WFS.

    Devuelve filas GeoJSON usando ST_AsGeoJSON de PostGIS.
    Los filtros de usuario se pasan siempre como parámetros de binding (sin interpolación),
    previniendo inyección SQL.
    """

    """
    English:
        Access to WFS-like geospatial data.

    Returns GeoJSON rows using PostGIS ST_AsGeoJSON. User-provided filters are
    always passed as bound parameters (no string interpolation), preventing
    SQL injection.
    """

    def __init__(self, session_factory):
        self._session_factory = session_factory

    async def get_features_by_bbox(
        self,
        schema: str,
        table: str,
        geom_column: str,
        minx: float,
        miny: float,
        maxx: float,
        maxy: float,
        srid_input: int = 4326,
        limit: int = 1000,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict]:
        """Devuelve features GeoJSON que intersectan el BBOX indicado.

        English:
            Returns GeoJSON features intersecting the provided BBOX.
        """
        schema = _safe_id(schema)
        table = _safe_id(table)
        geom_column = _safe_id(geom_column)

        where_extra, params = build_where_clause(filters)
        params.update(
            {
                "minx": minx,
                "miny": miny,
                "maxx": maxx,
                "maxy": maxy,
                "srid": srid_input,
                "limit": limit,
                "offset": offset,
            }
        )

        sql_str = f"""
            SELECT ST_AsGeoJSON(t.*)::json AS feature
            FROM {schema}.{table} AS t
            WHERE {geom_column} && ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, :srid)
              AND ST_Intersects(
                    {geom_column},
                    ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, :srid)
                  )
              {where_extra}
            LIMIT :limit OFFSET :offset
        """
        async with self._session_factory() as session:  # type: AsyncSession
            result = await session.execute(text(sql_str), params)
            return [row[0] for row in result.fetchall()]

    async def get_features_by_polygon(
        self,
        schema: str,
        table: str,
        geom_column: str,
        polygon_wkt: str,
        srid_input: int = 4326,
        limit: int = 1000,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict]:
        """
        Devuelve features GeoJSON que intersectan el polígono WKT indicado.

        English:
            Returns GeoJSON features intersecting the provided WKT polygon.

        The WKT is passed as a bound parameter and never interpolated into SQL.
        """
        schema = _safe_id(schema)
        table = _safe_id(table)
        geom_column = _safe_id(geom_column)

        where_extra, params = build_where_clause(filters)
        params.update(
            {
                "polygon_wkt": polygon_wkt,
                "srid": srid_input,
                "limit": limit,
                "offset": offset,
            }
        )

        sql_str = f"""
            SELECT ST_AsGeoJSON(t.*)::json AS feature
            FROM {schema}.{table} AS t
            WHERE ST_Intersects(
                    {geom_column},
                    ST_GeomFromText(:polygon_wkt, :srid)
                  )
              {where_extra}
            LIMIT :limit OFFSET :offset
        """
        async with self._session_factory() as session:
            result = await session.execute(text(sql_str), params)
            return [row[0] for row in result.fetchall()]
