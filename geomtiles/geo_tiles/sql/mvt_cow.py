"""
Generador MVT CoW (Copy-on-Write) con cuadrantes y DISTINCT ON por id_gis.

Este módulo expone `mvt_sql_cow(...)` que genera SQL optimizado que:
- Usa 4 cuadrantes para paralelizar y limitar escaneo
- Usa DISTINCT ON (id_gis) y ORDER BY project_id DESC para resolver CoW
- Soporta filtros por project_ids / exclude_project_ids
- Incluye simplificación/subdivide hooks (parámetros) para geometrías grandes

La implementación mantiene el SQL como texto para ser ejecutado por
`sqlalchemy.text(...)` desde TileRepository.
"""

from __future__ import annotations

from typing import Optional, Tuple
import re


def _cast_cols_to_text(columns_str: str) -> str:
    if not columns_str or not columns_str.strip():
        return ""
    cols = [c.strip() for c in columns_str.split(",") if c.strip()]
    casted = []
    for c in cols:
        m = re.search(r"\s+AS\s+(.+)$", c, re.IGNORECASE)
        if m:
            alias = m.group(1).strip()
            base = c[: m.start()].strip()
            casted.append(f"{base}::text AS {alias}")
        else:
            casted.append(f"{c}::text")
    return ", ".join(casted)


def mvt_sql_cow(
    schema: str,
    table: str,
    geom_col: str,
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    z: int,
    grid_size: float,
    columns_str: str,
    clustered_columns_str: str,
    priority: int,
    force_zero: bool = False,
    project_ids: Optional[Tuple[int, ...]] = None,
    exclude_project_ids: Optional[Tuple[int, ...]] = None,
    id_gis: Optional[int] = None,
    has_is_deleted: bool = True,
    simplify_px: float = 0.5,
    simplify_method: str = "auto",
    max_subdivide_vertices: int = 0,
) -> str:
    """Genera SQL MVT con estrategia CoW.

    Firma minimalista pensada para ser llamada desde TileService.
    """

    # Qualified table names
    qualified_table = f"{schema}.{table}"

    envelope = f"ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}, 3857)"
    midx = (minx + maxx) / 2.0
    midy = (miny + maxy) / 2.0

    tile_extent = 8192 if z < 18 else 16384
    tile_buffer = 256 if z < 18 else 128

    # Cast columns for UNION compatibility
    casted_columns_str = _cast_cols_to_text(columns_str)

    # Filters: project_ids
    unique_project_ids = sorted(set(project_ids)) if project_ids else []
    branch_ids = [p for p in unique_project_ids if p != 0]

    # Exclude
    points_exclude = ""
    if exclude_project_ids:
        ex_list = ",".join(str(int(p)) for p in exclude_project_ids)
        points_exclude = (
            f" AND t.id_gis NOT IN (SELECT id_gis FROM {qualified_table} WHERE project_id IN ({ex_list}) AND is_deleted = FALSE)"
        )

    # id_gis filter
    if id_gis is not None:
        if isinstance(id_gis, int):
            id_filter = f" AND t.id_gis = {id_gis}"
        else:
            id_filter = f" AND t.id_gis = '{id_gis}'"
    else:
        id_filter = ""

    # Common filters appended to quadrant queries
    v_common_filters = f"{points_exclude}{id_filter}"

    def quadrant_sql(x1, y1, x2, y2):
        # Master (project_id = 0) quadrant subselect
        deleted_pred = " AND t.is_deleted = FALSE" if has_is_deleted else ""
        return (
            f"(SELECT DISTINCT ON (t.id_gis) t.*\n"
            f" FROM {qualified_table} t\n"
            f" WHERE t.project_id = 0{deleted_pred}\n"
            f"   AND t.{geom_col} && ST_MakeEnvelope({x1}, {y1}, {x2}, {y2}, 3857)\n"
            f"   {v_common_filters}\n"
            f" ORDER BY t.id_gis, t.id_auto DESC)"
        )

    master_quadrants = (
        f"{quadrant_sql(minx, miny, midx, midy)}\n"
        f"UNION ALL\n"
        f"{quadrant_sql(midx, miny, maxx, midy)}\n"
        f"UNION ALL\n"
        f"{quadrant_sql(minx, midy, midx, maxy)}\n"
        f"UNION ALL\n"
        f"{quadrant_sql(midx, midy, maxx, maxy)}"
    )

    # Branch data: include branch projects when provided
    branch_cte = ""
    if branch_ids:
        if len(branch_ids) == 1:
            branch_filter = f"t.project_id = {branch_ids[0]}"
        else:
            branch_filter = "t.project_id IN (" + ",".join(str(int(p)) for p in branch_ids) + ")"

        deleted_pred = " AND t.is_deleted = FALSE" if has_is_deleted else ""
        branch_cte = (
            f"branch_data AS (\n"
            f"  SELECT DISTINCT ON (t.id_gis) t.*\n"
            f"  FROM {qualified_table} t\n"
            f"  WHERE {branch_filter}{deleted_pred}\n"
            f"    AND t.{geom_col} && {envelope}\n"
            f"    {points_exclude}{id_filter}\n"
            f"  ORDER BY t.id_gis, t.id_auto DESC\n"
            f"),"
        )

    # Build SELECT list
    cols_select = f", {casted_columns_str}" if casted_columns_str else ""
    point_count_expr = "0" if force_zero else "1"

    union_branch = f"UNION ALL SELECT * FROM branch_data" if branch_ids else ""

    # Simplification / subdivide heuristics
    # compute a simplify tolerance in map units (approx): simplify_px * (bbox_width / tile_extent)
    bbox_width = maxx - minx
    simplify_tol = simplify_px * (bbox_width / float(tile_extent))

    # choose base simplification/snap expression
    snap_expr = f"ST_SnapToGrid(t.{geom_col}, GREATEST({grid_size}, 0.00001))"
    simplify_expr = f"ST_SimplifyPreserveTopology(t.{geom_col}, {simplify_tol})"

    if simplify_method == "snap":
        mvt_pre_geom = snap_expr
    elif simplify_method == "simplify":
        mvt_pre_geom = simplify_expr
    else:
        # auto: snap for points, simplify for others
        mvt_pre_geom = (
            f"CASE WHEN GeometryType(t.{geom_col}) ILIKE '%POINT' THEN {snap_expr} ELSE {simplify_expr} END"
        )

    # subdivide branch: when geometry has many vertices, apply ST_Subdivide and re-collect
    if max_subdivide_vertices and max_subdivide_vertices > 0:
        subdivide_expr = (
            "(SELECT ST_Collect(p.geom) FROM ("
            f"SELECT (ST_Dump(ST_Subdivide({mvt_pre_geom}, {int(max_subdivide_vertices)}))).geom as geom"
            ") p)"
        )
        mvt_geom_expr = (
            f"CASE WHEN ST_NPoints(t.{geom_col}) > {int(max_subdivide_vertices)} THEN {subdivide_expr} ELSE {mvt_pre_geom} END"
        )
    else:
        mvt_geom_expr = mvt_pre_geom

    # Build final SQL
    if branch_cte:
        with_prefix = f"WITH {branch_cte} master_data AS (\n"
    else:
        with_prefix = "WITH master_data AS (\n"

    sql = (
        f"{with_prefix}"
        f"  SELECT DISTINCT ON (u.id_gis) u.*\n"
        f"  FROM (\n"
        f"{master_quadrants}\n"
        f"  ) u\n"
        f"  ORDER BY u.id_gis, u.id_auto DESC\n"
        f")\n"
        f"SELECT ST_AsMVT(tile, '{table}', {tile_extent}, '{geom_col}') FROM (\n"
        f"  SELECT\n"
        f"    ST_AsMVTGeom({mvt_geom_expr}, {envelope}, {tile_extent}, {tile_buffer}, TRUE) AS {geom_col},\n"
        f"    {point_count_expr} AS point_count{cols_select}, {priority} AS priority\n"
        f"  FROM (\n"
        f"    SELECT DISTINCT ON (fin.id_gis) fin.*\n"
        f"    FROM (\n"
        f"      SELECT * FROM master_data\n"
        f"      {union_branch}\n"
        f"    ) fin\n"
        f"    ORDER BY fin.id_gis, fin.project_id DESC, fin.id_auto DESC\n"
        f"  ) t\n"
        f"  WHERE t.is_deleted = FALSE\n"
        f") AS tile;"
    )

    return sql
