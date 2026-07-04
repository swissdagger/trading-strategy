"""
feeds/binance_futures.py — Binance USDⓈ-M Futures with 1s aggregation and Websockets.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Set

import httpx
import websockets

from feeds.base import AssetFeed, LiveDataCallback
from producer.bar import OHLCPoint
from producer.bar import RateLimiter
from feeds.polygon_options import _parse_timeframe, _resample_to_timeframe

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
#
# NOTE: this set is ONLY the sub-minute (1s-sourced) derivations. Coarse
# derivations like 2m and 10m — which Binance also lacks natively but which
# resample cleanly from a coarser native base (1m, 5m) — are NOT listed here;
# they are resolved by _get_base_tf's general "largest native divisor" rule
# below and reported by supported_timeframes(). Keeping this set 1s-only
# preserves the sub-minute vs coarse distinction that matters for backfill:
# 1s-derived rungs can't be REST-backfilled (Binance REST bottoms out at 1m),
# whereas coarse derived rungs CAN (their base kline is REST-available).
_DERIVED_INTERVALS: Set[str] = {"1s", "5s", "10s", "15s", "30s"}


def _tf_ms(tf: str) -> int:
    """Milliseconds in one bar of `tf`. Local, dependency-free (mirrors
    _parse_timeframe's arithmetic) so the base-resolver can run at import
    time without constructing anything."""
    multiplier, _, base_ms = _parse_timeframe(tf)
    return multiplier * base_ms


def _largest_native_divisor(tf: str) -> str | None:
    """The coarsest NATIVE interval that divides `tf` evenly and is strictly
    finer than it — the ideal resample base (fewest source bars, exact
    windows). Returns None if no native interval divides `tf` (then `tf`
    can't be derived and isn't supported).

    Example: 10m → 5m (10m / 5m = 2, exact) beats 1m (also exact) because it
    means 2 source bars per window instead of 10. 2m → 1m. 7m → None (no
    native interval divides 7 minutes evenly).
    """
    tf_ms = _tf_ms(tf)
    best: str | None = None
    best_ms = 0
    for native in _BINANCE_INTERVALS:
        n_ms = _tf_ms(native)
        if n_ms < tf_ms and tf_ms % n_ms == 0 and n_ms > best_ms:
            best, best_ms = native, n_ms
    return best


# Base timeframe a target rung is built from. Three cases:
#   - native rung  → itself (fetched directly, no resample)
#   - sub-minute   → "1s" (Binance's finest kline)
#   - coarse derived (e.g. 2m, 10m) → largest native interval that divides it
def _get_base_tf(tf: str) -> str | None:
    if tf in _BINANCE_INTERVALS:
        return tf
    if tf in _DERIVED_INTERVALS:  # sub-minute (incl. the 1s passthrough)
        return "1s"
    # Coarse non-native rung: derive from the best native base, if one exists.
    return _largest_native_divisor(tf)


# The coarse (non-native, non-sub-minute) rungs this feed derives. Kept
# EXPLICIT rather than enumerated: the ladder uses exactly 2m and 10m, and
# listing them is clearer than generating every native multiple and inviting
# exotic rungs no strategy asks for. Add a rung here when a strategy needs it;
# _get_base_tf already knows how to resolve any native-divisible label, so
# this list is the only edit a new coarse rung requires.
_COARSE_DERIVED_INTERVALS: Set[str] = {"2m", "10m"}

# The full set this feed can serve = native ∪ sub-minute-derived ∪ coarse-derived.
_SUPPORTED: Set[str] = _BINANCE_INTERVALS | _DERIVED_INTERVALS | _COARSE_DERIVED_INTERVALS


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
        self._running = True

        base_tfs = set(_get_base_tf(tf) for tf in timeframes)
        streams = [f"{sym.lower()}@kline_{btf}" for sym in symbols for btf in base_tfs]
        url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"

        # Buffer to keep the recent raw bars for resampling
        raw_buffers = {sym: {btf: [] for btf in base_tfs} for sym in symbols}

        cursors: Dict[str, Dict[str, int]] = {}
        for sym in symbols:
            cursors[sym] = {}
            for tf in timeframes:
                multiplier, _, base_ms = _parse_timeframe(tf)
                default_cursor = int(time.time() * 1000) - (multiplier * base_ms)
                cursors[sym][tf] = initial_cursors.get(sym, {}).get(tf) or default_cursor

        while self._running:
            try:
                async for websocket in websockets.connect(url):
                    if not self._running:
                        break
                    logger.info(f"Binance WS connected for {len(symbols)} symbols")

                    async for msg in websocket:
                        if not self._running:
                            break

                        data = json.loads(msg)
                        if 'data' not in data:
                            continue

                        k = data['data']['k']
                        if not k['x']:  # Only process finalized base bars
                            continue

                        sym = data['data']['s']
                        base_tf = k['i']

                        tick = {"t": k['t'], "o": float(k['o']), "h": float(k['h']), "l": float(k['l']), "c": float(k['c'])}
                        raw_buffers[sym][base_tf].append(tick)

                        # Prevent memory leak, keep only what's needed for the largest aggregate
                        raw_buffers[sym][base_tf] = raw_buffers[sym][base_tf][-2000:]

                        batch = []
                        now_ms = int(time.time() * 1000)

                        for tf in timeframes:
                            if _get_base_tf(tf) != base_tf:
                                continue

                            if _get_base_tf(tf) == tf:
                                # Native rung: the closed base kline IS the bar.
                                resampled = [tick]
                            else:
                                resampled = _resample_to_timeframe(raw_buffers[sym][base_tf], tf)

                            multiplier, _, base_ms = _parse_timeframe(tf)
                            tf_ms_local = multiplier * base_ms

                            for r in resampled:
                                window_start = r["t"]
                                if window_start + tf_ms_local > now_ms:
                                    continue  # Window still open

                                if window_start >= cursors[sym][tf]:
                                    batch.append(OHLCPoint(
                                        symbol=sym, timestamp=window_start, open=r["o"],
                                        high=r["h"], low=r["l"], close=r["c"],
                                        timeframe=tf, asset_class=self.asset_class
                                    ))
                                    cursors[sym][tf] = window_start + tf_ms_local

                        if batch:
                            await callback(batch)

            except Exception as e:
                logger.error(f"Binance WS error: {e}.")
                if self._running:
                    await asyncio.sleep(5)