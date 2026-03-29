"""Gestión de conexión asíncrona a PostgreSQL mediante SQLAlchemy + asyncpg.

English:
Asynchronous PostgreSQL connection management using SQLAlchemy + asyncpg.
"""

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from .config import PoolConfig


def create_session_factory(
    dsn: str,
    pool_config: "PoolConfig | None" = None,
    connect_args: "dict | None" = None,
):
    """
    Crea y devuelve una session factory async para usar con TileService / FeatureService.

    English:
        Create and return an async session factory to be used with
        TileService / FeatureService.

    Args:
        dsn: DSN de conexión. Ej.: "postgresql+asyncpg://user:pass@host/db"
        pool_config: Parámetros del pool de conexiones. Usa los defaults si no se indica.
        connect_args: Argumentos extra para asyncpg (p. ej. pgbouncer statement_cache_size=0).

    Returns:
        sessionmaker configurado con AsyncSession.
    """
    cfg = pool_config or PoolConfig()
    engine = create_async_engine(
        dsn,
        pool_size=cfg.pool_size,
        max_overflow=cfg.max_overflow,
        pool_timeout=cfg.pool_timeout,
        pool_recycle=cfg.pool_recycle,
        pool_pre_ping=True,
        echo=False,
        **({"connect_args": connect_args} if connect_args else {}),
    )
    return sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
