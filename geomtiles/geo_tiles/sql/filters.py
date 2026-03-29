"""
Helpers para construir cláusulas WHERE seguras usando parámetros nombrados de SQLAlchemy.

English:
Helpers to build safe WHERE clauses using SQLAlchemy named parameters.

No filter value is interpolated directly into SQL: all values are passed as
bound parameters via sqlalchemy.text(), preventing SQL injection.
"""

import re
from typing import Any, Dict, Optional, Tuple

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_identifier(name: str) -> str:
    """Valida que el nombre sea un identificador SQL seguro.

    English:
        Validate that the name is a safe SQL identifier.
    """
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Identificador SQL inválido: {name!r}")
    return name


def build_where_clause(
    filters: Optional[Dict[str, Any]],
) -> Tuple[str, Dict[str, Any]]:
    """
    Construye una cláusula WHERE adicional con parámetros nombrados de SQLAlchemy.

    English:
        Build an additional WHERE clause with SQLAlchemy named parameters.

    Args:
        filters: Diccionario {nombre_columna: valor} con los filtros a aplicar.
                 Los nombres de columna deben ser identificadores válidos.

    Returns:
        Tupla (sql_fragment, params_dict).
        - sql_fragment: cadena tipo "AND col1 = :filter_col1 AND col2 = :filter_col2"
        - params_dict: dict de parámetros para pasar a session.execute(text(sql), params)

    Example:
        sql_extra, params = build_where_clause({"tipo": "edificio", "activo": True})
        # sql_extra  → "AND tipo = :filter_tipo AND activo = :filter_activo"
        # params     → {"filter_tipo": "edificio", "filter_activo": True}
    """
    if not filters:
        return "", {}

    clauses = []
    params: Dict[str, Any] = {}
    for col, val in filters.items():
        safe_col = _safe_identifier(col)
        param_name = f"filter_{safe_col}"
        clauses.append(f"{safe_col} = :{param_name}")
        params[param_name] = val

    return "AND " + " AND ".join(clauses), params
