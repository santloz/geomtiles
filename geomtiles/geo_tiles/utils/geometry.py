"""Helpers de geometría: validación WKT, conversión BBOX y expresiones SQL.

English:
Geometry helpers: WKT validation, BBOX conversion and SQL expressions.
"""

import re

# Valida el tipo de geometría al inicio del WKT
_WKT_TYPE_RE = re.compile(
    r"^(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)"
    r"\s*[ZM ]*\s*\(",
    re.IGNORECASE,
)

# Solo caracteres numéricos, de tipo y de estructura — sin comillas ni punto y coma
_WKT_SAFE_CHARS_RE = re.compile(r"^[A-Za-z\s\d.,()+-]+$")


def is_valid_wkt(wkt: str) -> bool:
    """
    Valida que una cadena sea WKT bien formado y que no contenga caracteres peligrosos.

    Verifica que:
      - Comience con un tipo de geometría válido.
      - Solo contenga caracteres seguros (sin comillas, punto y coma, etc.).

    Esto es suficiente para prevenir inyección SQL cuando el WKT
    se embebe en expresiones ST_GeomFromText.

    English:
        Validates that a string is well-formed WKT and does not contain
        unsafe characters. It checks that the WKT starts with a valid
        geometry type and that only safe characters are present. This
        validation is sufficient to avoid SQL injection when embedding WKT
        in ST_GeomFromText expressions.
    """
    wkt = wkt.strip()
    return bool(_WKT_TYPE_RE.match(wkt)) and bool(_WKT_SAFE_CHARS_RE.match(wkt))


def bbox_to_wkt(minx: float, miny: float, maxx: float, maxy: float) -> str:
    """Convierte un BBOX a WKT POLYGON.

    English:
        Convert a BBOX to a WKT POLYGON string.
    """
    return (
        f"POLYGON(({minx} {miny}, {maxx} {miny}, "
        f"{maxx} {maxy}, {minx} {maxy}, {minx} {miny}))"
    )


def make_envelope_sql(
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    srid: int = 3857,
) -> str:
    """Genera una expresión ST_MakeEnvelope lista para incrustar en SQL.

    English:
        Generate an ST_MakeEnvelope expression ready to embed in SQL.
    """
    return f"ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}, {srid})"
