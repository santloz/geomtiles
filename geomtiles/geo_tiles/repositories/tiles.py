"""TileRepository: ejecuta queries ST_AsMVT y devuelve bytes PBF.

English:
TileRepository: executes ST_AsMVT queries and returns PBF bytes.
"""

from typing import Optional

from sqlalchemy import text


class TileRepository:
    """
    Acceso a datos de bajo nivel para tiles MVT.

    Recibe SQL pre-construido y devuelve los bytes del tile.
    La responsabilidad de construir el SQL seguro recae en TileService y sql/mvt.py.
    """

    """
    English:
        Low-level data access for MVT tiles.

    Accepts pre-built SQL and returns tile bytes. The responsibility for
    building safe SQL lies with TileService and sql/mvt.py.
    """

    def __init__(self, session_factory):
        self._session_factory = session_factory

    async def get_tile_bytes(self, sql: str) -> Optional[bytes]:
        """
        Ejecuta la query ST_AsMVT y devuelve los bytes del tile o None si está vacío.

        English:
            Execute the ST_AsMVT query and return the tile bytes or None if
            empty.

        Args:
            sql: Query SQL completa generada por mvt_sql_for_layer.

        Returns:
            bytes del tile PBF, o None si PostGIS devuelve NULL.
        """
        async with self._session_factory() as session:  # type: AsyncSession
            result = await session.execute(text(sql))
            return result.scalar()
