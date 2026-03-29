"""geo_tiles — Librería geoespacial para tiles MVT/PBF, WFS-like queries y PostGIS.

English:
geo_tiles — Geospatial library for MVT/PBF tiles, WFS-like queries and PostGIS.

Ejemplo de uso / Example:
    from geo_tiles import TileService, LayerConfig, create_session_factory

    session_factory = create_session_factory("postgresql+asyncpg://user:pass@host/db")
    svc = TileService(session_factory)
    svc.register_layer(LayerConfig(
        name="buildings",
        schema="public",
        table="buildings_view",
        geom_column="geom",
        minzoom=10,
        maxzoom=20,
    ))
    tile_bytes = await svc.get_mvt_tile(
        TileRequest(z=14, x=8345, y=6000, layers=["public.buildings_view.geom"])
    )
"""

__version__ = "0.1.1"
__author__ = "Tu Nombre"

from .db import create_session_factory
from .domain.exceptions import (
    DatabaseError,
    GeoTilesError,
    InvalidTileCoordinateError,
    LayerNotFoundError,
)
from .domain.models import FeatureRequest, LayerConfig, PolygonTileRequest, TileRequest
from .services.features import FeatureService
from .services.tiles import TileService
from .utils.tile_cache import FilesystemTileCache

__all__ = [
    "create_session_factory",
    "LayerConfig",
    "TileRequest",
    "PolygonTileRequest",
    "FeatureRequest",
    "GeoTilesError",
    "LayerNotFoundError",
    "InvalidTileCoordinateError",
    "DatabaseError",
    "TileService",
    "FeatureService",
    "FilesystemTileCache",
]
