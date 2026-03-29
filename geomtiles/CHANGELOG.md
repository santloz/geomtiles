# CHANGELOG

Todos los cambios notables en este repositorio estarán documentados en este archivo.

## [0.1.0] - 2026-03-29

### Added
- Implementación inicial de la librería `geo_tiles` (servicios, repositorios y utilidades).
- `FilesystemTileCache` con TTL, soporte para tiles agregados y versión gzip de tiles agregados.
- `RedisTileCache` y diseño pensado para uso híbrido (filesystem + redis).
- Integración con FastAPI: routers para servir tiles y features.
- Ejemplos ejecutables en `examples/` (`tile_service_example.py`, `feature_service_example.py`, `cache_example.py`).
- Tests unitarios para utilidades y caché en `tests/`.
- Docstrings bilingües (español / english) en módulos principales.
- Empaquetado y metadata listos: `setup.cfg`, `pyproject.toml`; artefactos `wheel`/`sdist` generados.
- Licencia MIT incluida (con traducción al español).

### Changed
- Ajustes en metadata del paquete (URL, project_urls, keywords, classifiers, dependencias adicionales).

### Notes
- CI y publicación automática a TestPyPI/PyPI pendientes.
- Próximos pasos: detallar el changelog por releases futuras, publicar en TestPyPI y añadir flujo de CI.
