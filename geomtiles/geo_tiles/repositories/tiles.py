"""TileRepository: ejecuta queries ST_AsMVT y devuelve bytes PBF.

English:
TileRepository: executes ST_AsMVT queries and returns PBF bytes.
"""

from typing import Optional
import re
from loguru import logger

from sqlalchemy import text

from ..utils.metrics import metrics


class TileRepository:
    """
    Acceso a datos de bajo nivel para tiles MVT.

    Recibe SQL pre-construido y devuelve los bytes del tile.
    La responsabilidad de construir el SQL seguro recae en TileService y sql/mvt.py.
    """

    def __init__(self, session_factory):
        self._session_factory = session_factory

    async def get_tile_bytes(self, sql: str) -> Optional[bytes]:
        """
        Ejecuta la query ST_AsMVT y devuelve los bytes del tile o None si está vacío.

        Esta implementación intenta detectar errores por columnas inexistentes
        y aplica una sanitización conservadora del SELECT para reintentar la
        ejecución sin las expresiones que referencian la columna faltante.
        """
        async with self._session_factory() as session:  # type: AsyncSession
            try:
                metrics.increment("tile_repo.requests")
                with metrics.time("tile_repo.query_ms"):
                    result = await session.execute(text(sql))
                metrics.increment("tile_repo.success")
                return result.scalar()
            except Exception as e:
                metrics.increment("tile_repo.failures")
                msg = str(e) if e is not None else ""
                # Pattern: column "<col>" does not exist
                m = re.search(r"column \"(?P<col>[A-Za-z0-9_]+)\" does not exist", msg)
                if m:
                    col = m.group("col")

                    def remove_column_from_select(sql_text: str, column: str) -> str:
                        pat1 = re.compile(r"\(array_agg\(\(?\"?%s\"?\)::text\)\)\[1\]\s+AS\s+\"?%s\"?\s*,?" % (column, column), re.IGNORECASE)
                        pat2 = re.compile(r"\(\"?%s\"?\)::text\s+AS\s+\"?%s\"?\s*,?" % (column, column), re.IGNORECASE)
                        pat3 = re.compile(r",\s*\"?%s\"?\s*(?:AS\s+\"?%s\"?)?\s*,?" % (column, column), re.IGNORECASE)

                        new = pat1.sub("", sql_text)
                        new = pat2.sub("", new)
                        new = pat3.sub("", new)
                        new = re.sub(r",\s*,", ",", new)
                        new = re.sub(r"SELECT\s+\,", "SELECT", new, flags=re.IGNORECASE)
                        return new

                    try:
                        sanitized_sql = remove_column_from_select(sql, col)
                        if sanitized_sql and sanitized_sql != sql:
                            # Use a fresh session for retry
                            metrics.increment("tile_repo.retries")
                            async with self._session_factory() as session2:
                                try:
                                    with metrics.time("tile_repo.query_ms"):
                                        result2 = await session2.execute(text(sanitized_sql))
                                    logger.warning("Retried SQL after removing missing column {}", col)
                                    metrics.increment("tile_repo.success")
                                    return result2.scalar()
                                except Exception:
                                    metrics.increment("tile_repo.failures")
                                    logger.exception("Retry after sanitizing SQL failed for column {}", col)
                    except Exception:
                        logger.exception("Error during SQL sanitization")

                # If we couldn't recover, log and re-raise
                logger.exception("Tile SQL execution failed: %s", msg)
                raise
