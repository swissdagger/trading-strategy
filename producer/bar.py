"""
producer/bar.py — The local OHLC bar type.

Defined here so the trading system has its OWN bar type and imports NOTHING from
CausaView (its models.py carried OHLCPoint alongside replay/frontend types this
headless system never uses). Kept named `OHLCPoint` with the same field shape so
the rest of the codebase imports it from here with no other change.

timestamp is epoch MILLISECONDS (int) — same convention as the feeds produce and
as service2_machine's _epoch_ms_to_iso expects. asset_class is the canonical
class string ('crypto' | 'equity' | 'fx'), resolved from ASSET_CONFIGS per symbol.
"""
from __future__ import annotations

from dataclasses import dataclass
import asyncio

@dataclass
class OHLCPoint:
    symbol: str
    timestamp: int          # epoch milliseconds
    open: float
    high: float
    low: float
    close: float
    timeframe: str
    asset_class: str


class RateLimiter:
    """Caps both concurrent requests AND requests-per-second.

    Used by service_1's seeder AND the live poller. They share ONE instance
    so they don't accidentally double up on Polygon and trigger 429s.

    Tuning:

      Polygon free tier (5 req/min):
          requests_per_second=0.08, max_concurrent=1
          (You will not be able to keep 195 pairs current. This tier is
          fine for prototyping a few pairs only.)

      Polygon paid (Starter/Developer/etc., "stay under 100 req/s"):
          requests_per_second=5-20, max_concurrent=5
          Default below is conservative; raise once you confirm no 429s.
    """

    def __init__(self, requests_per_second: float, max_concurrent: int = 5):
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be > 0")

        self.min_interval = 1.0 / requests_per_second
        self.max_concurrent = max_concurrent
        self.sem = asyncio.Semaphore(max_concurrent)
        self._last_start = 0.0
        self._gate = asyncio.Lock()

        logger.info(
            f"RateLimiter: {requests_per_second} req/s "
            f"(min spacing {self.min_interval * 1000:.0f}ms), "
            f"max_concurrent={max_concurrent}"
        )

    async def __aenter__(self):
        await self.sem.acquire()
        async with self._gate:
            now = time.monotonic()
            wait = self.min_interval - (now - self._last_start)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_start = time.monotonic()
        return self

    async def __aexit__(self, *exc):
        self.sem.release()