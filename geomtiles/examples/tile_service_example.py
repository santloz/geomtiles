"""Ejemplo completo: generar un tile XYZ con TileService.

English:
Full example: generate an XYZ tile using TileService.

Instrucciones:
 - Ajusta la variable `DSN` con tu conexión a PostGIS.
 - Ejecuta: `python geomtiles/examples/tile_service_example.py`
"""

import asyncio

from geo_tiles import LayerConfig, TileRequest, TileService, create_session_factory
from geo_tiles.utils.tile_cache import FilesystemTileCache


async def main() -> None:
    # Cambia este DSN por tu base de datos PostGIS
    DSN = "postgresql+asyncpg://user:pass@localhost/db"

    # Crear session factory
    session_factory = create_session_factory(DSN)

    # Opcional: usar caché en disco sin expiración (ttl=0)
    cache = FilesystemTileCache(cache_dir="./.cache_geomtiles", ttl=0)

    # Instanciar servicio de tiles
    svc = TileService(session_factory, tile_cache=cache)

    # Registrar una capa (configurar según tu BD)
    svc.register_layer(
        LayerConfig(
            name="buildings",
            schema="public",
            table="buildings_view",
            geom_column="geom",
            minzoom=10,
            maxzoom=20,
        )
    )

    # Solicitud de ejemplo (XYZ)
    req = TileRequest(z=14, x=8345, y=6000, layers=["public.buildings.geom"])

    # Generar/recuperar tile
    tile_bytes = await svc.get_mvt_tile(req)

    if tile_bytes:
        out = "tile.pbf"
        with open(out, "wb") as fh:
            fh.write(tile_bytes)
        print(f"Tile written to {out} ({len(tile_bytes)} bytes)")
    else:
        print("Empty tile (no features returned)")


if __name__ == "__main__":
    asyncio.run(main())
