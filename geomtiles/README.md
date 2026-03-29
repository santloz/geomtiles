# geomtiles

Utilidades para generación y caché de tiles MVT (multi-capa, CoW, filesystem/Redis).

English:
Utilities for generating and caching MVT tiles (multi-layer, copy-on-write,
filesystem/Redis backends).

## Instalación / Installation

```bash
pip install geomtiles
```

## Uso rápido / Quickstart

Español:

```python
from geo_tiles import create_session_factory, TileService, LayerConfig

# Crear session factory (asyncpg)
sf = create_session_factory("postgresql+asyncpg://user:pass@host/db")

# Instanciar TileService
svc = TileService(sf)

# Registrar una capa
svc.register_layer(LayerConfig(
	name="buildings",
	schema="public",
	table="buildings_view",
	geom_column="geom",
	minzoom=10,
	maxzoom=20,
))

# Obtener un tile MVT (ejemplo, en contexto async)
# tile_bytes = await svc.get_mvt_tile(TileRequest(z=14, x=8345, y=6000, layers=["public.buildings.geom"]))
```

English:

```python
from geo_tiles import create_session_factory, TileService, LayerConfig

# Create session factory (asyncpg)
sf = create_session_factory("postgresql+asyncpg://user:pass@host/db")

# Instantiate TileService
svc = TileService(sf)

# Register a layer
svc.register_layer(LayerConfig(
	name="buildings",
	schema="public",
	table="buildings_view",
	geom_column="geom",
	minzoom=10,
	maxzoom=20,
))

# Get an MVT tile (example, in async context)
# tile_bytes = await svc.get_mvt_tile(TileRequest(z=14, x=8345, y=6000, layers=['public.buildings.geom']))
```

## Casos de uso / Use cases

- Servir tiles XYZ MVT a partir de vistas PostGIS: use `TileService.get_mvt_tile()` con capas registradas.
- Tiles multilayer (composición de varias capas) y caché en disco: configure `FilesystemTileCache` y páselo como `tile_cache` a `TileService`.
- Consultas WFS-like (GeoJSON): use `FeatureService` y `FeatureRequest` para filtrar por `bbox` o `polygon_wkt`.

English:

- Serve XYZ MVT tiles from PostGIS views: use `TileService.get_mvt_tile()` with registered layers.
- Multilayer tiles (compose several layers) and disk cache: configure `FilesystemTileCache` and pass it as `tile_cache` to `TileService`.
- WFS-like GeoJSON queries: use `FeatureService` and `FeatureRequest` to filter by `bbox` or `polygon_wkt`.

## Ejemplos / Examples

Hay ejemplos ejecutables en la carpeta `examples/` con instrucciones y scripts:

- Ver [examples/README.md](examples/README.md) para ejemplos completos bilingües (TileService, FeatureService, Cache).

## Ejemplo: usar caché en disco sin expiración / Example: disk cache with no expiration

```python
from geo_tiles.utils.tile_cache import FilesystemTileCache

# ttl=0 → no expira en disco
cache = FilesystemTileCache(cache_dir="/var/cache/geomtiles", ttl=0)
svc = TileService(sf, tile_cache=cache)
```

## Más documentación / More documentation

Consulta los docstrings en los módulos `geo_tiles.*` para detalles de API, tipos y ejemplos adicionales.

English:

See the `geo_tiles.*` module docstrings for API details, types and more examples.

## Licencia / License

Este proyecto se publica bajo la licencia MIT — ver el archivo `LICENSE`.

English:

This project is released under the MIT license — see the `LICENSE` file.
