"""
feeds/binance_futures.py — Binance USDⓈ-M Futures via REST klines polling.

Live data is polled from the /fapi/v1/klines REST endpoint (start_live runs one
poll loop per timeframe), NOT the futures websocket: Binance silences fstream to
cloud/datacenter egress IPs, so a ws live path receives no frames from such
hosts, whereas the REST endpoint is reachable. fetch_history uses the same REST
endpoint for warmup/backfill. Native rungs (1m+) are served directly; sub-minute
rungs would require the (blocked) ws and are not used on crypto from such a host.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Set

import httpx

from feeds.base import AssetFeed, LiveDataCallback, is_market_open
from producer.bar import OHLCPoint
from producer.bar import RateLimiter
from feeds.resample import _parse_timeframe, _resample_to_timeframe

logger = logging.getLogger(__name__)

# Binance USDⓈ-M futures NATIVE kline intervals. These are served directly by
# the /fapi/v1/klines endpoint, so a native rung is fetched as-is (no
# resampling). This set is the feed's contract; it MUST mirror
# feeds/binance_ws._NATIVE_INTERVALS so the REST backfill sibling and the WS
# live feed agree on what "native" means.
#
# Sub-minute rungs (5s/10s/30s) are NOT Binance-native — klines bottom out at
# 1m — so they are DERIVED by aggregating the 1s kline. _get_base_tf routes a
# sub-minute target to the 1s base; every other (native) target is its own base.
_BINANCE_INTERVALS: Set[str] = {
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M",
}

# Sub-minute rungs we can DERIVE from the 1s kline. Mirrors
# feeds/binance_ws._DERIVED_INTERVALS.
_DERIVED_INTERVALS: Set[str] = {"1s", "5s", "10s", "15s", "30s"}

# The full set this feed can serve = native ∪ derived.
_SUPPORTED: Set[str] = _BINANCE_INTERVALS | _DERIVED_INTERVALS


# Base timeframe a target rung is built from. A native rung is its OWN base
# (fetched directly, no resample); a sub-minute rung is derived from 1s.
def _get_base_tf(tf: str) -> str:
    if tf in _DERIVED_INTERVALS and tf != "1s":
        return "1s"
    return tf


_KLINES_LIMIT = 1500


class BinanceFuturesFeed(AssetFeed):
    asset_class = "crypto"
    name = "binance_futures"
    BASE_URL = "https://fapi.binance.com/fapi/v1/klines"

    def __init__(
        self,
        symbols: List[str],
        timeframes: List[str],
        requests_per_second: float = 5.0,
        max_concurrent: int = 3,
    ):
        unsupported = [tf for tf in timeframes if tf not in _SUPPORTED]
        if unsupported:
            raise ValueError(
                f"BinanceFuturesFeed does not support timeframe(s): {unsupported}. "
                f"Supported: {sorted(_SUPPORTED)}"
            )

        self.symbols = symbols
        self.timeframes = timeframes
        self.rate_limiter = RateLimiter(requests_per_second=requests_per_second, max_concurrent=max_concurrent)
        self._client: httpx.AsyncClient | None = None
        self._running = False

    def supported_timeframes(self) -> Set[str]:
        return _SUPPORTED

    def to_native_symbol(self, symbol: str) -> str:
        return symbol

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient()
        return self._client

    async def aclose(self) -> None:
        self._running = False
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _request_klines(self, symbol: str, interval: str, end_time: int) -> List[List[Any]] | None:
        client = await self._get_client()
        params = {"symbol": symbol, "interval": interval, "limit": _KLINES_LIMIT, "endTime": end_time}

        for attempt in range(4):
            try:
                async with self.rate_limiter:
                    resp = await client.get(self.BASE_URL, params=params, timeout=30.0)

                if resp.status_code in (429, 418):
                    await asyncio.sleep(2 ** attempt)
                    continue
                if resp.status_code == 403:
                    logger.error(f"403 Forbidden from Binance for {symbol}.")
                    return None

                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError:
                await asyncio.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------------
    # HISTORY (warmup phase) - Fixed to guarantee points_needed
    # ---------------------------------------------------------------
    async def fetch_history(self, symbol: str, timeframe: str, points_needed: int) -> List[OHLCPoint]:
        base_tf = _get_base_tf(timeframe)
        # A native rung is fetched at its own interval and needs no resampling;
        # only a derived (sub-minute) rung is built from a finer base.
        is_native = base_tf == timeframe

        collected_ticks: List[List[Any]] = []
        resampled: List[Dict[str, Any]] = []

        cursor = int(time.time() * 1000)

        # Loop until the RESAMPLED array has enough bars, not the raw array.
        while len(resampled) < points_needed:
            ticks = await self._request_klines(symbol, base_tf, end_time=cursor)
            if not ticks:
                break

            # Binance returns ascending. Prepend to our list.
            collected_ticks = ticks + collected_ticks

            # Step backward from the oldest bar returned
            cursor = ticks[0][0] - 1

            # Format to dicts so _resample_to_timeframe can ingest them
            raw_dicts = [
                {"t": t[0], "o": float(t[1]), "h": float(t[2]), "l": float(t[3]), "c": float(t[4])}
                for t in collected_ticks
            ]
            if is_native:
                # Base == target: the raw klines ARE the target bars.
                resampled = raw_dicts
            else:
                resampled = _resample_to_timeframe(raw_dicts, timeframe)

            # If Binance gives us fewer than limit, we've hit the beginning of
            # the market's available history — stop paginating.
            if len(ticks) < _KLINES_LIMIT:
                break

        # Trim exact amount
        resampled = resampled[-points_needed:]

        return [
            OHLCPoint(
                symbol=symbol, timestamp=r["t"], open=r["o"], high=r["h"],
                low=r["l"], close=r["c"], timeframe=timeframe, asset_class=self.asset_class
            )
            for r in resampled
        ]

    # ---------------------------------------------------------------
    # LIVE STREAMING VIA WEBSOCKETS
    # ---------------------------------------------------------------
    async def start_live(
        self, symbols: List[str], timeframes: List[str],
        initial_cursors: Dict[str, Dict[str, int]], callback: LiveDataCallback
    ) -> None:
        """One REST poll loop per timeframe — NOT a websocket.

        WHY REST, NOT WS: Binance silences its futures WEBSOCKET (fstream) to
        cloud/datacenter egress IPs, so a ws live path connects but receives zero
        frames from such hosts (see the crypto transport note in config). The REST
        klines endpoint (fapi) IS reachable, so the live path polls it. This
        mirrors the Polygon REST feed's start_live: per-timeframe poll tasks, a
        tf/2 cadence (8s floor, 600s ceiling), closed-window-only emission, and a
        per-(symbol, tf) high-water cursor seeded from warmup's feed_cursors.

        Only NATIVE rungs (1m+) are servable by REST klines; sub-minute rungs
        would need the ws (blocked) and must not be configured on crypto for a
        host in this position — the config scopes them off. fetch_history already
        rejects unsupported tfs, so a stray sub-minute rung fails loudly rather
        than silently polling nothing.
        """
        self._running = True

        async def poll_one_tf(tf: str) -> None:
            multiplier, _, candle_ms = _parse_timeframe(tf)
            tf_ms = multiplier * candle_ms
            tf_s = tf_ms // 1000

            # Half-candle cadence, 8s floor, 600s ceiling — same as Polygon REST.
            interval = min(max(tf_s // 2, 8), 600)

            # Per-(symbol) high-water cursor for this tf. Seed from warmup's
            # feed_cursors so live resumes exactly where warmup ended; fall back
            # to "one candle ago" for a cold rung.
            cursors: Dict[str, int] = {}
            for sym in symbols:
                cursors[sym] = (
                    initial_cursors.get(sym, {}).get(tf)
                    or (int(time.time() * 1000) - tf_ms)
                )

            logger.info(f"[{self.name}:{tf}] REST polling every {interval}s")

            while self._running:
                try:
                    now = datetime.now(timezone.utc)
                    # Crypto is 24/7 (is_market_open returns True), but keep the
                    # gate so this loop is correct if ever reused for a class with
                    # sessions.
                    if not is_market_open(self.asset_class, now):
                        await asyncio.sleep(interval)
                        continue

                    now_ms = int(now.timestamp() * 1000)
                    batch: List[OHLCPoint] = []

                    for sym in symbols:
                        # Enough bars to cover the poll gap plus slack. A handful
                        # of candles per interval is plenty; fetch_history returns
                        # ascending closed bars already resampled to tf.
                        points = await self.fetch_history(sym, tf, points_needed=5)
                        for p in points:
                            # Closed-window-only: fetch_history returns closed
                            # klines, but guard the still-forming window defensively.
                            if p.timestamp + tf_ms > now_ms:
                                continue
                            if p.timestamp >= cursors[sym]:
                                batch.append(p)
                                cursors[sym] = p.timestamp + tf_ms

                    if batch:
                        await callback(batch)

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"[{self.name}:{tf}] REST poll error: {e}")

                await asyncio.sleep(interval)

        tasks = [asyncio.create_task(poll_one_tf(tf)) for tf in timeframes]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()