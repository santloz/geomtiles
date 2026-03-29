"""
Generadores de SQL para tiles MVT usando ST_AsMVT / ST_AsMVTGeom de PostGIS.

English:
SQL generators for MVT tiles using PostGIS ST_AsMVT / ST_AsMVTGeom.

All tile generation is delegated to PostGIS for maximum performance. Zoom
level determines whether point clustering is applied.
"""


def mvt_sql_for_layer(
    schema: str,
    table: str,
    geom_col: str,
    envelope_sql: str,
    z: int,
    grid_size: float,
    columns_str: str,
    clustered_columns_str: str,
    priority: int,
    force_zero: bool = False,
    extent_low: int = 8192,
    extent_high: int = 16384,
    buffer_low: int = 256,
    buffer_high: int = 128,
) -> str:
    """
    Genera la consulta SQL para obtener un tile MVT de una capa.

    English:
      Generate the SQL query to obtain an MVT tile for a layer.

    For zoom < 18 points are clustered via ST_SnapToGrid to reduce volume.
    For zoom >= 18 geometries are returned unclustered at higher resolution.

    Args:
      schema: Esquema PostgreSQL de la tabla.
      table: Nombre de la tabla o vista.
      geom_col: Nombre de la columna de geometría.
      envelope_sql: Expresión SQL del envelope del tile (ST_MakeEnvelope o ST_GeomFromText).
      z: Nivel de zoom del tile.
      grid_size: Tamaño de grid en metros para el clustering de puntos.
      columns_str: Columnas no-geométricas separadas por coma.
      clustered_columns_str: Mismo que columns_str pero envueltas con MIN() para GROUP BY.
      priority: Número de prioridad de la capa (se incluye como atributo del tile).
      force_zero: Si True, fuerza point_count = 0 (usado para temas/reportes).
      extent_low: Extent MVT para zoom < 18.
      extent_high: Extent MVT para zoom >= 18.
      buffer_low: Buffer MVT para zoom < 18.
      buffer_high: Buffer MVT para zoom >= 18.

    Returns:
      Cadena SQL lista para ejecutar con sqlalchemy text().
    """
    tile_extent = extent_low if z < 18 else extent_high
    tile_buffer = buffer_low if z < 18 else buffer_high
    table_qualified = f"{schema}.{table}"

    if z < 18:
        non_point_count = "0" if force_zero else "1"
        sql = f"""
        SELECT ST_AsMVT(tile, '{table}', {tile_extent}, '{geom_col}')
        FROM (
          -- Puntos: agrupados por grid para clustering
          SELECT
            ST_AsMVTGeom(
              ST_Centroid(ST_Collect({geom_col})),
              {envelope_sql},
              {tile_extent},
              {tile_buffer},
              true
            ) AS {geom_col},
            {'0' if force_zero else 'COUNT(*)'} AS point_count,
            MIN(id_gis) AS id_gis,
            {clustered_columns_str},
            {priority} AS priority
          FROM {table_qualified}
          WHERE {geom_col} && {envelope_sql}
            AND ST_Intersects({geom_col}, {envelope_sql})
            AND GeometryType({geom_col}) ILIKE '%POINT'
          GROUP BY ST_SnapToGrid({geom_col}, GREATEST({grid_size}, 0.00001))

          UNION ALL

          -- No puntos (líneas, polígonos): sin agrupamiento
          SELECT
            ST_AsMVTGeom(
              {geom_col},
              {envelope_sql},
              {tile_extent},
              {tile_buffer},
              true
            ) AS {geom_col},
            {non_point_count} AS point_count,
            id_gis,
            {columns_str},
            {priority} AS priority
          FROM {table_qualified}
          WHERE {geom_col} && {envelope_sql}
            AND ST_Intersects({geom_col}, {envelope_sql})
            AND GeometryType({geom_col}) NOT ILIKE '%POINT'
        ) AS tile;
        """
    else:
        point_count = "0" if force_zero else "1"
        sql = f"""
        SELECT ST_AsMVT(tile, '{table}', {tile_extent}, '{geom_col}')
        FROM (
          SELECT
            ST_AsMVTGeom(
              {geom_col},
              {envelope_sql},
              {tile_extent},
              {tile_buffer},
              true
            ) AS {geom_col},
            {point_count} AS point_count,
            id_gis,
            {columns_str},
            {priority} AS priority
          FROM {table_qualified}
          WHERE {geom_col} && {envelope_sql}
            AND ST_Intersects({geom_col}, {envelope_sql})
        ) AS tile;
        """
    return sql
