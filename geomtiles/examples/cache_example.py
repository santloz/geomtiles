"""Ejemplo: uso directo de FilesystemTileCache.

English:
Direct usage example of FilesystemTileCache to store and read a tile.

Instrucciones:
 - Ejecuta: `python geomtiles/examples/cache_example.py`
"""

import asyncio

from geo_tiles.utils.tile_cache import FilesystemTileCache


async def main() -> None:
    cache = FilesystemTileCache(cache_dir="./.cache_geomtiles", ttl=0)

    z, x, y = 14, 8345, 6000
    schema, table, geom = "public", "buildings_view", "geom"

    # Datos de ejemplo (en la práctica serían bytes PBF reales)
    sample = b"EXAMPLE_PBF_BYTES"

    # Escribir en caché
    await cache.set_layer(z, x, y, schema, table, geom, sample)

    # Leer desde caché
    got = await cache.get_layer(z, x, y, schema, table, geom)
    if got:
        print(f"Read {len(got)} bytes from cache (preview: {got[:20]!r})")
    else:
        print("No cached tile found")


if __name__ == "__main__":
    asyncio.run(main())
