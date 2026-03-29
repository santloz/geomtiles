from geo_tiles.utils.tiles import (
    tile_xyz_to_bbox,
    get_cluster_factor,
    grid_size_for_zoom,
    EARTH_HALF,
)


def test_tile_xyz_to_bbox_z0():
    minx, miny, maxx, maxy = tile_xyz_to_bbox(0, 0, 0)
    assert minx == -EARTH_HALF
    assert maxx == EARTH_HALF
    assert miny == -EARTH_HALF
    assert maxy == EARTH_HALF


def test_cluster_factor_bounds():
    assert get_cluster_factor(0) == 0.15
    assert get_cluster_factor(100) == 0.01
    mid = get_cluster_factor(18)
    assert 0.01 <= mid <= 0.15


def test_grid_size_monotonic():
    z1 = grid_size_for_zoom(10)
    z2 = grid_size_for_zoom(11)
    assert z2 < z1
