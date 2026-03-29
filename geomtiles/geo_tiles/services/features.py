"""
FeatureService: API de alto nivel para consultas geoespaciales tipo WFS.

English:
High-level API for WFS-like geospatial queries returning GeoJSON features.
"""

from typing import Any, Dict, List, Optional

from ..domain.exceptions import InvalidGeometryError
from ..domain.models import FeatureRequest
from ..repositories.features import FeatureRepository
from ..utils.geometry import is_valid_wkt


class FeatureService:
    """
    Servicio para consultas WFS-like que devuelven GeoJSON.

    Soporta filtrado por BBOX (EPSG:4326) o por polígono WKT (EPSG:4326),
    con filtros adicionales de atributos.
    """

    """
    English:
        Service for WFS-like queries returning GeoJSON.

    Supports filtering by BBOX (EPSG:4326) or WKT polygon (EPSG:4326) with
    additional attribute filters.
    """

    def __init__(self, session_factory):
        self._repo = FeatureRepository(session_factory)

    async def get_features(self, req: FeatureRequest) -> List[Dict]:
        """
        Devuelve una lista de features GeoJSON.

        English:
            Returns a list of GeoJSON features.

        Args:
            req: FeatureRequest con schema/table, bbox o polygon_wkt, y filtros opcionales.

        Returns:
            Lista de objetos GeoJSON (dicts).

        Raises:
            ValueError: si ni bbox ni polygon_wkt están provistos, o el WKT es inválido.
            InvalidGeometryError: si el WKT no pasa la validación de seguridad.
        """
        if req.bbox is not None:
            minx, miny, maxx, maxy = req.bbox
            return await self._repo.get_features_by_bbox(
                schema=req.schema,
                table=req.table,
                geom_column=req.geom_column,
                minx=minx,
                miny=miny,
                maxx=maxx,
                maxy=maxy,
                limit=req.limit,
                offset=req.offset,
                filters=req.filters,
            )

        if req.polygon_wkt is not None:
            # El WKT va como parámetro de binding en FeatureRepository (seguro).
            # English: the WKT is passed as a bound parameter in FeatureRepository (safe).
            # Still we validate the format to return a clear error to the user.
            if not is_valid_wkt(req.polygon_wkt):
                raise InvalidGeometryError(
                    "WKT de polígono inválido. Se esperan tipos geometry estándar."
                )
            return await self._repo.get_features_by_polygon(
                schema=req.schema,
                table=req.table,
                geom_column=req.geom_column,
                polygon_wkt=req.polygon_wkt,
                limit=req.limit,
                offset=req.offset,
                filters=req.filters,
            )

        raise ValueError("FeatureRequest debe incluir 'bbox' o 'polygon_wkt'.")

    async def get_features_by_bbox(
        self,
        schema: str,
        table: str,
        bbox: tuple,
        geom_column: str = "geom",
        limit: int = 1000,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict]:
        """Acceso directo sin construir un FeatureRequest."""
        minx, miny, maxx, maxy = bbox
        return await self._repo.get_features_by_bbox(
            schema=schema,
            table=table,
            geom_column=geom_column,
            minx=minx,
            miny=miny,
            maxx=maxx,
            maxy=maxy,
            limit=limit,
            filters=filters,
        )
