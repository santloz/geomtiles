import asyncio
import gzip
import os
import time

from geo_tiles.utils.tile_cache import FilesystemTileCache


def test_set_get_and_describe_layer(tmp_path):
    cache = FilesystemTileCache(tmp_path, ttl=3600)
    data = b"xyz123"
    asyncio.run(cache.set_layer(0, 0, 0, "public", "roads", "geom", data))
    got = asyncio.run(cache.get_layer(0, 0, 0, "public", "roads", "geom"))
    assert got == data
    manifest = asyncio.run(cache.describe_layer(0, 0, 0, "public", "roads", "geom"))
    assert manifest is not None
    assert manifest["size_bytes"] == len(data)
    assert manifest["ttl_seconds"] == cache._ttl


def test_ttl_expiry(tmp_path):
    cache = FilesystemTileCache(tmp_path, ttl=1)
    data = b"hello"
    asyncio.run(cache.set_layer(0, 0, 0, "s", "t", "g", data))
    tile_path = cache._tile_path(0, 0, 0, "s", "t", "g")
    # Simular archivo antiguo forzando mtime
    old = time.time() - 3600
    os.utime(tile_path, (old, old))
    got = asyncio.run(cache.get_layer(0, 0, 0, "s", "t", "g"))
    assert got is None


def test_aggregated_set_get_and_gz(tmp_path):
    cache = FilesystemTileCache(tmp_path, ttl=3600)
    data = b"aggdata"
    layers = "public.a.geom,public.b.geom"
    asyncio.run(cache.set_aggregated(0, 0, 0, layers, data))
    got = asyncio.run(cache.get_aggregated(0, 0, 0, layers))
    assert got == data
    asyncio.run(cache.set_aggregated_gz(0, 0, 0, layers, data))
    gz = asyncio.run(cache.get_aggregated_gz(0, 0, 0, layers))
    assert gz is not None
    assert gzip.decompress(gz) == data
