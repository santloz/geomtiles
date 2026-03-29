from geo_tiles.utils.geometry import is_valid_wkt, bbox_to_wkt, make_envelope_sql


def test_valid_wkt_and_injection():
    assert is_valid_wkt("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")
    # Semicolon and SQL keywords should be rejected by the safe-chars check
    assert not is_valid_wkt("POLYGON((0 0)); DROP TABLE users;")


def test_bbox_to_wkt():
    wkt = bbox_to_wkt(0, 0, 1, 1)
    assert wkt.startswith("POLYGON((")
    assert "0 0" in wkt and "1 1" in wkt


def test_make_envelope_sql():
    sql = make_envelope_sql(0, 0, 1, 1, 4326)
    assert "ST_MakeEnvelope" in sql and "4326" in sql
