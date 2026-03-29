"""
Adaptador FastAPI para geo_tiles.

English:
FastAPI adapter for geo_tiles. Exposes plug-and-play routers for MVT tiles,
polygon clipping and GeoJSON features.

Ejemplo de uso / Example:
    from fastapi import FastAPI
    from geo_tiles import create_session_factory, TileService, FeatureService
    from geo_tiles.integrations.fastapi import create_tile_router, create_feature_router

    session_factory = create_session_factory("postgresql+asyncpg://...")
    tile_svc = TileService(session_factory)
    feature_svc = FeatureService(session_factory)

    app = FastAPI()
    app.include_router(create_tile_router(tile_svc), prefix="/geo")
    app.include_router(create_feature_router(feature_svc), prefix="/geo")
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel

from ..domain.exceptions import (
    GeoTilesError,
    InvalidGeometryError,
    InvalidTileCoordinateError,
)
from ..domain.models import FeatureRequest, PolygonTileRequest, TileRequest

if TYPE_CHECKING:
    from ..services.features import FeatureService
    from ..services.tiles import TileService

_MVT_CONTENT_TYPE = "application/x-protobuf"
_MVT_CACHE_HEADER = "public, max-age=60"


def create_tile_router(tile_service: "TileService") -> APIRouter:
    """
    Crea un APIRouter de FastAPI con los endpoints de tiles XYZ y polígono.

    Endpoints:
        GET /tiles/{layer_names}/{z}/{x}/{y}.pbf
        POST /tiles/polygon

    English:
        Create a FastAPI APIRouter exposing XYZ tile and polygon endpoints.

    Endpoints:
        GET /tiles/{layer_names}/{z}/{x}/{y}.pbf
        POST /tiles/polygon
    """
    router = APIRouter(tags=["tiles"])

    @router.get(
        "/tiles/{layer_names}/{z}/{x}/{y}.pbf",
        response_class=Response,
        summary="Tile MVT por coordenadas XYZ",
    )
    async def get_tile(
        layer_names: str,
        z: int,
        x: int,
        y: int,
        force_point_count_zero: bool = Query(False, alias="forceZero"),
    ) -> Response:
        """
        Devuelve un tile MVT/PBF para las capas indicadas.

        English:
            Returns an MVT/PBF tile for the requested layers.

        `layer_names` es una lista separada por coma en formato `schema.table.geom_col`.
        Ejemplo: `public.buildings.geom,public.roads.geom`
        """
        req = TileRequest(
            z=z,
            x=x,
            y=y,
            layers=layer_names.split(","),
            force_point_count_zero=force_point_count_zero,
        )
        try:
            tile_bytes = await tile_service.get_mvt_tile(req)
        except (InvalidTileCoordinateError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except GeoTilesError as exc:
            raise HTTPException(status_code=500, detail=str(exc))

        return Response(
            content=tile_bytes,
            media_type=_MVT_CONTENT_TYPE,
            headers={"Cache-Control": _MVT_CACHE_HEADER},
        )

    class PolygonTileBody(BaseModel):
        polygon_wkt: str
        schema_name: str
        tables: List[str] = []
        force_point_count_zero: bool = False

    @router.post(
        "/tiles/polygon",
        response_class=Response,
        summary="Tile MVT recortado a polígono WKT",
    )
    async def get_polygon_tile(body: PolygonTileBody) -> Response:
        """
        Devuelve un tile MVT recortado al polígono WKT indicado (EPSG:3857).

        English:
            Returns an MVT tile clipped to the provided WKT polygon (EPSG:3857).
        """
        req = PolygonTileRequest(
            polygon_wkt=body.polygon_wkt,
            schema=body.schema_name,
            tables=body.tables,
            force_point_count_zero=body.force_point_count_zero,
        )
        try:
            tile_bytes = await tile_service.get_mvt_polygon(req)
        except (ValueError, InvalidGeometryError) as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except GeoTilesError as exc:
            raise HTTPException(status_code=500, detail=str(exc))

        return Response(
            content=tile_bytes,
            media_type=_MVT_CONTENT_TYPE,
        )

    return router


def create_feature_router(feature_service: "FeatureService") -> APIRouter:
    """
    Crea un APIRouter de FastAPI con el endpoint WFS-like de features GeoJSON.

    Endpoints:
        GET /features/{schema}/{table}?bbox=minx,miny,maxx,maxy
        GET /features/{schema}/{table}?polygon_wkt=...

    English:
        Create a FastAPI APIRouter providing a WFS-like endpoint for GeoJSON
        features.

    Endpoints:
        GET /features/{schema}/{table}?bbox=minx,miny,maxx,maxy
        GET /features/{schema}/{table}?polygon_wkt=...
    """
    router = APIRouter(tags=["features"])

    @router.get(
        "/features/{schema}/{table}",
        summary="Features GeoJSON por BBOX o polígono",
    )
    async def get_features(
        schema: str,
        table: str,
        geom_column: str = Query("geom"),
        bbox: Optional[str] = Query(
            None, description="minx,miny,maxx,maxy en EPSG:4326"
        ),
        polygon_wkt: Optional[str] = Query(None),
        limit: int = Query(1000, ge=1, le=10000),
        offset: int = Query(0, ge=0),
    ):
        parsed_bbox = None
        if bbox:
            try:
                parts = [float(v) for v in bbox.split(",")]
                if len(parts) != 4:
                    raise ValueError
                parsed_bbox = tuple(parts)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Parámetro 'bbox' inválido. Formato esperado: minx,miny,maxx,maxy",
                )

        req = FeatureRequest(
            schema=schema,
            table=table,
            geom_column=geom_column,
            bbox=parsed_bbox,
            polygon_wkt=polygon_wkt,
            limit=limit,
            offset=offset,
        )
        try:
            features = await feature_service.get_features(req)
        except (ValueError, InvalidGeometryError) as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except GeoTilesError as exc:
            raise HTTPException(status_code=500, detail=str(exc))

        return {
            "type": "FeatureCollection",
            "features": features,
            "count": len(features),
        }

    return router
