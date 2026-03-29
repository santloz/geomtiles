import asyncio

from geo_tiles.services.metadata import MetadataService
from geo_tiles.utils.cache import TTLCache


class DummyRepo:
    def __init__(self):
        self.get_columns_calls = 0
        self.resolve_calls = 0

    async def get_columns(self, schema, table, exclude_columns=None):
        self.get_columns_calls += 1
        # devolver columnas típicas
        return ["id", "name", "project_id", "is_deleted", "id_auto"]

    async def resolve_base_table(self, schema, table):
        self.resolve_calls += 1
        return "base_table"

    async def discover_tables(self, schema):
        return ["t1", "t2"]

    async def table_exists(self, schema, table):
        return True

    async def find_geom_col(self, schema, table, candidates=None):
        return "geom"


def test_describe_layer_and_cache_behavior():
    svc = MetadataService(session_factory=None, cache_ttl=60)
    dummy = DummyRepo()
    svc._repo = dummy
    svc._cache = TTLCache(ttl=60)

    lm = asyncio.run(svc.describe_layer("public", "view1", "geom"))
    assert lm.resolved_table == "base_table"
    assert lm.has_project_id is True

    # Comprobamos que get_columns se cachea
    before = dummy.get_columns_calls
    cols1 = asyncio.run(svc.get_columns("public", "view1", "geom"))
    after = dummy.get_columns_calls
    cols2 = asyncio.run(svc.get_columns("public", "view1", "geom"))
    assert dummy.get_columns_calls == after
    assert cols1 == cols2
