"""Excepciones de dominio de geo_tiles.

English:
Domain exceptions used by geo_tiles.
"""


class GeoTilesError(Exception):
    """Error base de la librería.

    English:
        Base library error.
    """


class LayerNotFoundError(GeoTilesError):
    """La capa solicitada no existe o no se pudo resolver.

    English:
        The requested layer does not exist or could not be resolved.
    """


class InvalidTileCoordinateError(GeoTilesError):
    """Coordenadas de tile fuera de rango o inválidas.

    English:
        Tile coordinates are out of range or invalid.
    """


class DatabaseError(GeoTilesError):
    """Error de acceso o ejecución en la base de datos.

    English:
        Error accessing or executing database operations.
    """


class InvalidGeometryError(GeoTilesError):
    """Geometría WKT/WKB inválida o no segura.

    English:
        Invalid or unsafe WKT/WKB geometry.
    """
