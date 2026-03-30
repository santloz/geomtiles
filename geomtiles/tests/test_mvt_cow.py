import re

from geo_tiles.sql.mvt_cow import mvt_sql_cow


def test_mvt_cow_basic_contains_distinct_and_order():
    sql = mvt_sql_cow(
        schema="public",
        table="test_table",
        geom_col="geom",
        minx=0,
        miny=0,
        maxx=256,
        maxy=256,
        z=10,
        grid_size=10,
        columns_str='"id", "name"',
        clustered_columns_str='MIN("id") AS "id", MIN("name") AS "name"',
        priority=1,
    )

    assert re.search(r"DISTINCT ON \([^\)]*id_gis\)", sql)
    assert "ORDER BY fin.id_gis, fin.project_id DESC" in sql
    # quadrants implemented as 4 ST_MakeEnvelope occurrences in master union
    matches = re.findall(r"ST_MakeEnvelope\(", sql)
    assert len(matches) >= 4


def test_mvt_cow_high_zoom_still_has_distinct():
    sql = mvt_sql_cow(
        schema="public",
        table="test_table",
        geom_col="geom",
        minx=-1000,
        miny=-1000,
        maxx=1000,
        maxy=1000,
        z=18,
        grid_size=10,
        columns_str='"id", "name"',
        clustered_columns_str='MIN("id") AS "id", MIN("name") AS "name"',
        priority=2,
    )

    assert re.search(r"DISTINCT ON \([^\)]*id_gis\)", sql)
    assert "ORDER BY fin.id_gis, fin.project_id DESC" in sql
