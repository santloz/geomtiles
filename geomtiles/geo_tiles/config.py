"""Configuración centralizada de la librería.

English:
Central configuration types for the library.
"""

from dataclasses import dataclass


@dataclass
class PoolConfig:
    """Configuración del pool de conexiones SQLAlchemy.

    English:
        SQLAlchemy connection pool configuration.
    """

    pool_size: int = 50
    max_overflow: int = 25
    pool_timeout: int = 10
    pool_recycle: int = 900


@dataclass
class TileConfig:
    """Configuración de generación de tiles MVT.

    English:
        Configuration for MVT tile generation.
    """

    max_concurrent_layers: int = 10

    # Extent y buffer para zoom bajo (<18) y zoom alto (>=18)
    # English: extent and buffer for low zoom (<18) and high zoom (>=18)
    extent_low: int = 8192
    extent_high: int = 16384
    buffer_low: int = 256
    buffer_high: int = 128

    # Parámetros de clustering por zoom
    # English: clustering parameters by zoom
    cluster_zoom_low: float = 17.5  # zoom <= este valor → factor máximo
    cluster_zoom_high: float = 18.5  # zoom >= este valor → factor mínimo
    cluster_factor_max: float = 0.15
    cluster_factor_min: float = 0.01
