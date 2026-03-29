"""Utilidades para conversión y cálculo de tiles XYZ.

English:
Utilities for conversion and calculation of XYZ tiles (Web Mercator).
"""

EARTH_HALF: float = 20037508.342789244


def tile_xyz_to_bbox(x: int, y: int, z: int) -> tuple[float, float, float, float]:
    """
    Convierte coordenadas de tile XYZ a BBOX en EPSG:3857.

    English:
        Convert XYZ tile coordinates to a BBOX in EPSG:3857 (Web Mercator).

    Returns:
        (minx, miny, maxx, maxy) en metros (Web Mercator).
    """
    n = 2.0**z
    tile_size = EARTH_HALF * 2 / n
    minx = -EARTH_HALF + x * tile_size
    maxx = -EARTH_HALF + (x + 1) * tile_size
    maxy = EARTH_HALF - y * tile_size
    miny = EARTH_HALF - (y + 1) * tile_size
    return minx, miny, maxx, maxy


def get_cluster_factor(
    z: int,
    zoom_low: float = 17.5,
    zoom_high: float = 18.5,
    factor_max: float = 0.15,
    factor_min: float = 0.01,
) -> float:
    """
    Devuelve el factor de clustering de puntos para un nivel de zoom dado.

    English:
        Return the point clustering factor for a given zoom level. Lower
        zooms produce more clustering (higher factor), while higher zooms
        use a smaller (minimum) factor.
    """
    if z <= zoom_low:
        return factor_max
    if z >= zoom_high:
        return factor_min
    val = factor_max * (zoom_high - z) / (zoom_high - zoom_low)
    return max(val, factor_min)


def grid_size_for_zoom(z: int) -> float:
    """
    Calcula el tamaño de grid en metros para el clustering por zoom.

    English:
        Calculate grid size in meters for clustering by zoom. Used with
        ST_SnapToGrid in MVT point queries.
    """
    cluster_factor = get_cluster_factor(z)
    return EARTH_HALF * 2 / (2**z) * cluster_factor
