"""Caché en memoria con TTL (Time To Live) para metadatos geoespaciales.

English:
In-memory TTL cache for geospatial metadata.
"""

import asyncio
import time
from typing import Any, Dict, Optional, Tuple


class TTLCache:
    """
    Caché in-memory async-safe con TTL configurable.

    Ideal para cachear metadatos de columnas y resolución de tablas/vistas,
    evitando consultas repetidas a information_schema.
    """

    """
    English:
        Async-safe in-memory TTL cache used primarily for metadata such as
        column lists or resolved base tables to avoid repeated queries.
    """

    def __init__(self, ttl: int = 300):
        """
        Args:
            ttl: Tiempo de vida de las entradas en segundos (default: 300 s).
        """
        self._ttl = ttl
        self._store: Dict[str, Tuple[Any, float]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        """Devuelve el valor asociado a la clave o None si expiró o no existe.

        English:
            Return the value for the key or None if it expired or does not exist.
        """
        async with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            value, expiry = entry
            if time.monotonic() > expiry:
                del self._store[key]
                return None
            return value

    async def set(self, key: str, value: Any) -> None:
        """Almacena un valor con TTL a partir del momento actual.

        English:
            Store a value with a TTL relative to the current time.
        """
        async with self._lock:
            self._store[key] = (value, time.monotonic() + self._ttl)

    async def delete(self, key: str) -> None:
        """Elimina una entrada por clave.

        English:
            Delete an entry by key.
        """
        async with self._lock:
            self._store.pop(key, None)

    async def clear(self) -> None:
        """Vacía la caché por completo.

        English:
            Clear the entire cache.
        """
        async with self._lock:
            self._store.clear()

    @property
    def size(self) -> int:
        """Número de entradas actualmente en la caché (incluyendo expiradas no purgadas).

        English:
            Number of entries currently in the cache (may include expired items).
        """
        return len(self._store)
