"""Métricas simples en memoria para instrumentación ligera.

No añadimos dependencias externas; este módulo guarda contadores
y temporizadores agregados en memoria. Es suficiente para pruebas y
exposición interna. Para monitorización real, exponer estos datos a
Prometheus o StatsD es recomendado.
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Dict


class Metrics:
    def __init__(self):
        self._counters: Dict[str, int] = {}
        self._timers: Dict[str, Dict[str, float]] = {}
        self._lock = threading.Lock()

    def increment(self, key: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + int(amount)

    @contextmanager
    def time(self, key: str):
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed = (time.perf_counter() - start) * 1000.0
            with self._lock:
                entry = self._timers.get(key) or {"count": 0.0, "total_ms": 0.0}
                entry["count"] = entry.get("count", 0) + 1
                entry["total_ms"] = entry.get("total_ms", 0.0) + elapsed
                self._timers[key] = entry

    def snapshot(self) -> Dict[str, object]:
        with self._lock:
            timers = {k: {**v, "avg_ms": (v["total_ms"] / v["count"] if v["count"] else 0.0)} for k, v in self._timers.items()}
            return {"counters": dict(self._counters), "timers": timers}


metrics = Metrics()
