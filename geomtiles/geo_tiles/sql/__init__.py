"""Utilities and SQL generation helpers for MVT and PostGIS queries.

This package exposes SQL generator registration for pluggable generators.
Default generators registered at import time:
- `default` → `mvt_sql_for_layer` (legacy)
- `cow` → `mvt_sql_cow` (CoW quadrant generator)
"""

from .mvt import mvt_sql_for_layer

try:
	from .mvt_cow import mvt_sql_cow
except Exception:
	mvt_sql_cow = None

from .registry import register_generator, get_generator, list_generators

# Register built-in generators
register_generator("default", mvt_sql_for_layer)
if mvt_sql_cow is not None:
	register_generator("cow", mvt_sql_cow)

__all__ = ["register_generator", "get_generator", "list_generators"]
