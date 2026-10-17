"""
research/bars.py — 1-minute bars with volume, VWAP and trade count.

For every research symbol (the ladder equities plus SPY/QQQ/VIXY), every
`interval_s`, fetch from max(last stored − overlap, now − initial_lookback)
to now and upsert. The overlap re-reads the most recent bars each pass so
late prints (Polygon revises a closed minute for a while) are absorbed —
the upsert replaces the row. Idempotent and self-healing: a missed pass is
covered by the next one, bounded by max_lookback.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Awaitable, Callable, Iterable, List, Optional

from research.polygon_research import Bar1m, PolygonResearchClient

log = logging.getLogger("research.bars")

MINUTE_MS = 60_000


class BarCollector:
    def __init__(
        self, client: PolygonResearchClient, symbols: List[str],
        upsert: Callable[[Iterable[Bar1m]], Awaitable[int]],
        latest_ms: Callable[[str], Awaitable[Optional[int]]],
        *, overlap_min: int = 60, initial_lookback_min: int = 7 * 24 * 60,
        max_lookback_min: int = 30 * 24 * 60, now_ms: Callable[[], int] = lambda: int(time.time() * 1000),
    ):
        self._client = client
        self.symbols = list(symbols)
        self._upsert = upsert
        self._latest = latest_ms
        self._overlap = overlap_min * MINUTE_MS
        self._initial = initial_lookback_min * MINUTE_MS
        self._max = max_lookback_min * MINUTE_MS
        self._now_ms = now_ms
        self.stats = {"passes": 0, "bars": 0, "errors": 0}

    async def run_once(self) -> int:
        total = 0
        now = self._now_ms()
        for sym in self.symbols:
            try:
                last = await self._latest(sym)
                lo = (last - self._overlap) if last is not None else (now - self._initial)
                lo = max(lo, now - self._max)
                bars = await self._client.bars_1m(sym, lo, now)
                n = await self._upsert(bars)
                total += n
                log.debug("bars %s: %d rows from %d", sym, n, lo)
            except Exception:
                self.stats["errors"] += 1
                log.exception("bars pass failed for %s (continuing)", sym)
        self.stats["passes"] += 1
        self.stats["bars"] += total
        log.info("bars pass: %d rows across %d symbols", total, len(self.symbols))
        return total

    async def run_forever(self, interval_s: float, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.run_once()
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval_s)
            except asyncio.TimeoutError:
                pass
