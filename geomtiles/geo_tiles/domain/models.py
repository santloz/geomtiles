"""Modelos de dominio de geo_tiles.

English:
Domain models used by geo_tiles.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from typing import Literal


@dataclass
class LayerConfig:
    """Configuración de una capa geoespacial registrada en TileService.

    English:
        Configuration for a geospatial layer registered in TileService.
    """

    name: str  # Nombre lógico interno
    schema: str
    table: str
    geom_column: str
    minzoom: int = 0
    maxzoom: int = 30
    priority: int = 1
    clustering: bool = True
    extent: int = 4096
    buffer: int = 64
    srid: int = 3857
    use_cow: bool = False
    sql_mode: Literal['default', 'cow'] = 'default'


@dataclass
class TileRequest:
    """Solicitud de tile MVT por coordenadas XYZ.

    English:
        Request model for an MVT tile by XYZ coordinates.
    """

    z: int
    x: int
    y: int
    layers: List[str]  # Formato: "schema.table.geom_col"
    force_point_count_zero: bool = False
    theme: Optional[str] = None


@dataclass
class PolygonTileRequest:
    """Solicitud de tile MVT recortado a un polígono WKT.

    English:
        Request model for an MVT tile clipped to a WKT polygon.
    """

    polygon_wkt: str  # WKT en EPSG:3857
    schema: str
    tables: Optional[List[str]] = None
    force_point_count_zero: bool = False
    theme: Optional[str] = None


@dataclass
class FeatureRequest:
    """Solicitud de features GeoJSON por BBOX o polígono (WFS-like).

    English:
        Request model for GeoJSON features by BBOX or polygon (WFS-like).
    """

    schema: str
    table: str
    geom_column: str = "geom"
    bbox: Optional[Tuple[float, float, float, float]] = (
        None  # (minx,miny,maxx,maxy) EPSG:4326
    )
    polygon_wkt: Optional[str] = None  # WKT EPSG:4326
    limit: int = 1000
    offset: int = 0
    filters: Optional[Dict[str, Any]] = None
