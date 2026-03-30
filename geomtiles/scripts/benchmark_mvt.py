"""Minimal benchmark script for MVT SQL generation and execution.

Usage (example):
    python scripts/benchmark_mvt.py --dsn postgresql+asyncpg://user:pass@host/db --layer public.test.geom --z 14 --x 0 --y 0 --iters 10

This script is a lightweight helper to measure SQL generation + execution latency
against a PostGIS instance. It is intentionally minimal and meant as a starting
point for reproducible local benchmarks.
"""

import argparse
import asyncio
import time

from geo_tiles.db import create_session_factory
from geo_tiles.services.tiles import TileService
from geo_tiles.domain.models import TileRequest


async def run(args):
    session_factory = create_session_factory(args.dsn)
    svc = TileService(session_factory)

    layers = [args.layer]
    req = TileRequest(z=args.z, x=args.x, y=args.y, layers=layers, force_point_count_zero=False)

    # Warmup
    await svc.get_mvt_tile(req)

    times = []
    for i in range(args.iters):
        t0 = time.perf_counter()
        await svc.get_mvt_tile(req)
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000.0)
        print(f"iter {i+1}/{args.iters}: {times[-1]:.2f} ms")

    print(f"avg: {sum(times)/len(times):.2f} ms, min: {min(times):.2f} ms, max: {max(times):.2f} ms")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--layer", required=True, help="schema.table.geom_col")
    parser.add_argument("--z", type=int, required=True)
    parser.add_argument("--x", type=int, required=True)
    parser.add_argument("--y", type=int, required=True)
    parser.add_argument("--iters", type=int, default=5)
    args = parser.parse_args()
    asyncio.run(run(args))
