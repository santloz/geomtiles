"""
Registry para generadores SQL (plugins).

Permite registrar y recuperar generadores por nombre. Un generador
debe ser una callable que acepte parámetros relacionados con la
generación de SQL para MVT. No forzamos una firma estricta, pero la
convención es la siguiente (ejemplo):

  def mvt_sql_xxx(schema, table, geom_col, minx, miny, maxx, maxy, z,
                  grid_size, columns_str, clustered_columns_str, priority,
                  force_zero=False, project_ids=None, exclude_project_ids=None,
                  id_gis=None, has_is_deleted=True, simplify_px=0.5, simplify_method='auto') -> str

El registry expone `register_generator(name, func)` y `get_generator(name)`.
"""

from __future__ import annotations

from typing import Callable, Dict, Iterable


_REGISTRY: Dict[str, Callable] = {}


def register_generator(name: str, func: Callable) -> None:
    """Registra un generador bajo `name`. Sobrescribe si ya existe."""
    _REGISTRY[name] = func


def get_generator(name: str):
    """Devuelve el generador registrado por `name` o lanza KeyError."""
    try:
        return _REGISTRY[name]
    except KeyError:
        raise KeyError(f"SQL generator not found: {name}")


def list_generators() -> Iterable[str]:
    return list(_REGISTRY.keys())


def default_generator_name() -> str:
    return "default"
