"""Ejemplo: consultar features GeoJSON con FeatureService.

English:
Example: query GeoJSON features using FeatureService.

Instrucciones:
 - Ajusta `DSN` y los nombres de `schema`/`table` según tu base de datos.
 - Ejecuta: `python geomtiles/examples/feature_service_example.py`
"""

import asyncio
import json

from geo_tiles import FeatureRequest, FeatureService, create_session_factory


async def main() -> None:
    DSN = "postgresql+asyncpg://user:pass@localhost/db"
    session_factory = create_session_factory(DSN)

    svc = FeatureService(session_factory)

    # Example: query by bbox (minx,miny,maxx,maxy) in EPSG:4326
    req = FeatureRequest(
        schema="public",
        table="buildings_view",
        bbox=(-74.10, 4.60, -73.90, 4.80),
        limit=100,
        offset=0,
    )

    features = await svc.get_features(req)

    print(f"Returned {len(features)} features")
    if features:
        print(json.dumps(features[0], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
