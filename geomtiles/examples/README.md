# Ejemplos / Examples

Esta carpeta contiene ejemplos ejecutables y bilingües de uso de la librería `geomtiles`.

Requisitos / Requirements
- Python 3.9+
- Una base de datos PostGIS accesible si se desea ejecutar los ejemplos que consultan PostGIS.

Archivos
- `tile_service_example.py`: Genera un tile XYZ usando `TileService`.
- `feature_service_example.py`: Consulta features GeoJSON vía `FeatureService`.
- `cache_example.py`: Ejemplo de uso directo de `FilesystemTileCache`.

Cómo ejecutar / How to run

1. Ajusta el DSN dentro del script a tu base de datos PostGIS.
2. Ejecuta el script desde la raíz del repo:

```bash
python geomtiles/examples/tile_service_example.py
python geomtiles/examples/feature_service_example.py
python geomtiles/examples/cache_example.py
```

Notas: los ejemplos son didácticos; algunos realizarán llamadas reales a PostGIS si se configura el DSN.

---

# Examples (English)

This folder contains runnable, bilingual examples for the `geomtiles` library.

Files
- `tile_service_example.py`: Generate an XYZ tile using `TileService`.
- `feature_service_example.py`: Query GeoJSON features via `FeatureService`.
- `cache_example.py`: Direct usage of `FilesystemTileCache`.

How to run

1. Set a valid DSN inside the script to point to your PostGIS database.
2. Run the script from the repository root:

```bash
python geomtiles/examples/tile_service_example.py
```

These examples are illustrative; some will perform real PostGIS queries if a DSN is provided.
