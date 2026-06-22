"""
feeds/polygon_options.py — Polygon options aggregates as an AssetFeed.

Mirrors PolygonForexFeed's structure (same /v2/aggs/ticker URL shape,
same backward-paginated history fetch, same 429-with-Retry-After
handling). Three things differ from forex:

1. **Canonical symbol form is the native form.** Options tickers
   already include the `O:` prefix (e.g. `O:AAPL250620C00200000`)
   in the canonical name we store. to_native_symbol is identity.
   Compare PolygonForexFeed where canonical is `EURUSD` and native
   is `C:EURUSD`.

2. **Session-hour gating on start_live.** Options trade 13:30-20:00
   UTC weekdays. The poll loop sleeps through closed periods rather
   than burning rate-limit budget on calls Polygon would return
   empty for anyway.

3. **Short-lifetime contracts are normal.** The top-1-by-OI rule
   (contract_discovery.py) often picks near-expiry contracts.
   fetch_history returns whatever Polygon has for that contract's
   listed window — possibly fewer than `points_needed`. The seeder
   that consumes this (Phase 5's wiring) is responsible for marking
   `option_contracts.insufficient_history` when the result is too
   short for Sumtyme to chain on.

Phase 2 builds this feed in isolation. The standalone validator in
scripts/test_polygon_options.py exercises it. Wiring into Service 1
is Phase 5.
"""
from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Dict, List, Set

import httpx

from feeds.base import AssetFeed, LiveDataCallback, is_market_open
from producer.bar import OHLCPoint
from producer.bar import RateLimiter

logger = logging.getLogger(__name__)


# Same v3-plan canonical list as the rest of the system. 10m omitted
# because Binance Futures doesn't support it — kept symmetric across
# crypto and options so a unified TIMEFRAMES_COMMON drives both feeds.
_SUPPORTED_TIMEFRAMES: Set[str] = {
    # Active ladder: [30s, 45s, 1m, 2m, 3m, 5m]
    "30s", "45s", "1m", "2m", "3m", "5m",
    # Sub-minute (compat; finer than active baseline)
    "5s", "10s", "15s",
    # Coarser (compat)
    "10m", "15m", "30m", "1h", "2h", "4h", "8h", "12h",
}


# Polygon API mapping for timeframe strings — identical to forex.
_UNIT_MAP = {"s": "second", "m": "minute", "h": "hour", "d": "day"}
_MS_MAP   = {"s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}


def _parse_timeframe(timeframe: str):
    """Return (multiplier, polygon_timespan, candle_ms)."""
    multiplier = int(timeframe[:-1])
    unit = timeframe[-1].lower()
    return multiplier, _UNIT_MAP.get(unit, "minute"), _MS_MAP.get(unit, 60_000)


def _resample_to_timeframe(ticks, timeframe: str) -> list:
    """Re-aggregate Polygon's raw bars into uniform timeframe-aligned
    windows. Returns a list of {t, o, h, l, c} dicts matching Polygon's
    aggregate shape, sorted ascending by timestamp.

    Why this exists: Polygon's options aggregates do NOT honor the
    requested `multiplier` for sparse contracts. Asked for `5/minute`
    on a low-volume strike, Polygon may return back-to-back 1-minute
    bars where trades occurred. Sumtyme's wall-clock-anchored mode
    rejects non-uniform spacing (and is the wrong tool for this
    problem anyway — see Service 2's step-mode rationale). To
    satisfy the "bars labelled '5m' must be 5 minutes apart" contract
    that Sumtyme's interval/interval_unit imposes, we re-aggregate
    here before returning anything to the seeder.

    Aggregation rule (standard OHLC bucketing):
      - window_start = floor(t / tf_ms) * tf_ms
      - open  = first bar's open  (lowest t in the window)
      - high  = max of all highs in the window
      - low   = min of all lows in the window
      - close = last bar's close  (highest t in the window)
      - t     = window_start

    Windows with no source bars (no trades during that 5-minute
    period) produce NO output row. This creates gaps in the returned
    series — exactly the gaps that Service 2's step-mode call is
    designed to handle. The alternative (forward-filling with
    synthetic flat bars) would corrupt Sumtyme's input with bars
    that don't reflect real trading; gaps preserve the "same input
    → same output" guarantee.
    """
    if not ticks:
        return []
    multiplier, _, base_ms = _parse_timeframe(timeframe)
    # _parse_timeframe returns base ms per UNIT, not per timeframe.
    # `5m` → (5, 'minute', 60_000). Multiply to get the actual window size.
    tf_ms = multiplier * base_ms

    # Bucket by window-start. Use a dict so we don't need to pre-sort.
    by_window: dict = {}
    for tick in ticks:
        t = tick["t"]
        window_start = (t // tf_ms) * tf_ms
        bucket = by_window.get(window_start)
        if bucket is None:
            by_window[window_start] = {
                "t":         window_start,
                "o":         tick["o"],
                "h":         tick["h"],
                "l":         tick["l"],
                "c":         tick["c"],
                "_first_t":  t,
                "_last_t":   t,
            }
        else:
            bucket["h"] = max(bucket["h"], tick["h"])
            bucket["l"] = min(bucket["l"], tick["l"])
            if t < bucket["_first_t"]:
                bucket["o"] = tick["o"]
                bucket["_first_t"] = t
            if t > bucket["_last_t"]:
                bucket["c"] = tick["c"]
                bucket["_last_t"] = t

    # Strip the bookkeeping fields and return sorted ascending.
    out = []
    for ws in sorted(by_window):
        b = by_window[ws]
        out.append({"t": b["t"], "o": b["o"], "h": b["h"], "l": b["l"], "c": b["c"]})
    return out


class PolygonOptionsFeed(AssetFeed):
    """Polygon options aggregates.

    `symbols` here are full OCC tickers (e.g. `O:AAPL250620C00200000`),
    NOT underlyings. The contract_discovery module resolves "I care
    about AAPL options" → "track this specific OCC ticker" by writing
    to the `option_contracts` table. Service 1 reads from there at
    seed time and passes the resolved tickers to this feed's
    `fetch_history` / `start_live` (Phase 5).

    Each feed instance owns its own RateLimiter — same isolation rule
    as the rest of the multi-feed architecture. Note that during the
    fx + options transition, PolygonForexFeed and PolygonOptionsFeed
    will both compete for the same Polygon API key's HTTP weight
    budget. The plan accepts this as a transient risk through Phase 5;
    fx sunsets in Phase 6.
    """

    asset_class = "option"
    name = "polygon_options"

    MAX_CANDLES_PER_CALL = 5000  # Polygon options aggregates cap

    def __init__(
        self,
        api_key: str,
        symbols: List[str],
        timeframes: List[str],
        requests_per_second: float = 5.0,
        max_concurrent: int = 5,
    ):
        # Validate timeframes early. supported_timeframes() is the
        # contract; mismatches die loudly at construction rather than
        # silently producing empty fetches.
        unsupported = [tf for tf in timeframes if tf not in _SUPPORTED_TIMEFRAMES]
        if unsupported:
            raise ValueError(
                f"PolygonOptionsFeed does not support timeframe(s): {unsupported}. "
                f"Supported: {sorted(_SUPPORTED_TIMEFRAMES)}"
            )

        self.api_key = api_key
        self.symbols = symbols
        self.timeframes = timeframes
        self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            max_concurrent=max_concurrent,
        )
        self._client: httpx.AsyncClient | None = None
        self._running = False

    def supported_timeframes(self) -> Set[str]:
        return _SUPPORTED_TIMEFRAMES

    def to_native_symbol(self, symbol: str) -> str:
        # Canonical form already includes the O: prefix. Identity.
        # If a caller passes a raw OCC string without O:, this would
        # produce a 400 from Polygon; we defensively prefix.
        return symbol if symbol.startswith("O:") else f"O:{symbol}"

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient()
        return self._client

    async def aclose(self) -> None:
        """Idempotent shutdown."""
        self._running = False
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ---------------------------------------------------------------
    # HISTORY
    # ---------------------------------------------------------------
    async def fetch_history(
        self,
        symbol: str,
        timeframe: str,
        points_needed: int,
    ) -> List[OHLCPoint]:
        """Backward-paginate aggregates until we have `points_needed`
        candles or three empty chunks in a row, then resample to
        strict uniform spacing at the requested timeframe.

        Returns whatever's available; for short-lifetime contracts
        this may be fewer than `points_needed`. The caller (Phase 5's
        seeder) is responsible for marking `insufficient_history`
        when the result is too short for Sumtyme.

        Identical pagination algorithm to PolygonForexFeed.fetch_history
        — the only differences are the native symbol (`O:` prefix) and
        the resampling step. Kept as a separate file rather than
        refactored to a shared helper because Phase 6 deletes the
        forex file entirely.

        Resampling is required because Polygon's options aggregates
        do not honor the requested `multiplier` for sparse contracts.
        Asked for `5/minute` on a low-volume strike, Polygon may
        return back-to-back 1-minute bars where trades occurred.
        Sumtyme's interval/interval_unit interface requires uniform
        spacing at the labelled timeframe, so we re-aggregate via
        _resample_to_timeframe before returning. Session gaps
        (overnight, weekend, holiday) are preserved in the output —
        they're handled at the Sumtyme call boundary by step-mode
        indexing (timeframe_dependent=False), not at the feed level.
        """
        if timeframe not in _SUPPORTED_TIMEFRAMES:
            logger.error(f"Unsupported options timeframe: {timeframe}")
            return []

        multiplier, timespan, base_ms = _parse_timeframe(timeframe)
        native_symbol = self.to_native_symbol(symbol)
        client = await self._get_client()

        collected_points: List[dict] = []
        resampled: List[dict] = []
        chunk_ms = int(points_needed * multiplier * base_ms * 1.5)
        current_to = int(datetime.now(timezone.utc).timestamp() * 1000)
        empty_chunks = 0

        while len(resampled) < points_needed and empty_chunks < 3:
            current_from = current_to - chunk_ms
            limit_needed = min(points_needed - len(collected_points), self.MAX_CANDLES_PER_CALL)

            url = (
                f"https://api.polygon.io/v2/aggs/ticker/{native_symbol}"
                f"/range/{multiplier}/{timespan}/{current_from}/{current_to}"
            )
            params = {
                "adjusted": "true",
                "sort":     "desc",
                "limit":    limit_needed,
                "apiKey":   self.api_key,
            }

            success = False
            for attempt in range(3):
                try:
                    async with self.rate_limiter:
                        resp = await client.get(url, params=params, timeout=30.0)

                    if resp.status_code == 429:
                        retry_after = resp.headers.get("Retry-After")
                        try:
                            backoff = float(retry_after) if retry_after else 2 ** attempt
                        except (ValueError, TypeError):
                            backoff = 2 ** attempt
                        logger.warning(
                            f"429 seeding {symbol} {timeframe}; backing off {backoff:.1f}s "
                            f"(attempt {attempt + 1}/3)"
                        )
                        await asyncio.sleep(backoff)
                        continue

                    if resp.status_code in (401, 403):
                        # Auth failure or tier limitation — not transient.
                        # Stop trying, surface the cause cleanly.
                        logger.error(
                            f"{resp.status_code} from Polygon options for {symbol}. "
                            f"Check that POLYGON_API_KEY has options access. NOT retrying."
                        )
                        return []

                    resp.raise_for_status()
                    results = resp.json().get("results", [])

                    if results:
                        collected_points.extend(results)
                        current_to = results[-1]["t"] - 1
                        empty_chunks = 0
                    else:
                        current_to = current_from - 1
                        empty_chunks += 1

                    success = True
                    break

                except Exception as e:
                    logger.warning(
                        f"Options fetch error {symbol} {timeframe} "
                        f"(attempt {attempt + 1}/3): {e}"
                    )
                    await asyncio.sleep(2 ** attempt)

            if not success:
                logger.error(f"Failed to fetch chunk for {symbol} {timeframe}")
                break
            resampled = _resample_to_timeframe(collected_points, timeframe)
        resampled = resampled[-points_needed:]
        # Re-aggregate Polygon's raw bars to the requested timeframe.
        # Polygon may have returned mixed-resolution data (1m where
        # trades occurred, 5m elsewhere) on sparse contracts; the
        # resampler enforces strict uniform-spaced windows. See
        # _resample_to_timeframe's docstring for the full rationale.
        #
        # The output may have gaps (no-trade windows) within sessions
        # and across sessions. Service 2 forward-fills intra-session
        # gaps with synthetic flat bars at buffer-append time, before
        # data reaches Sumtyme. Inter-session gaps remain and are
        # handled at the Sumtyme call boundary by step-mode indexing.
        # The fill lives in Service 2 rather than the feed so that
        # both fetch_history (warmup) and start_live (live) data
        # flow through the same fill point.
        resampled = _resample_to_timeframe(collected_points, timeframe)

        return [
            OHLCPoint(
                symbol=symbol,
                timestamp=tick["t"],
                open=tick["o"],
                high=tick["h"],
                low=tick["l"],
                close=tick["c"],
                timeframe=timeframe,
                asset_class=self.asset_class,
            )
            for tick in resampled
        ]

    # ---------------------------------------------------------------
    # LIVE
    # ---------------------------------------------------------------
    async def start_live(
        self,
        symbols: List[str],
        timeframes: List[str],
        initial_cursors: Dict[str, Dict[str, int]],
        callback: LiveDataCallback,
    ) -> None:
        """One poll loop per timeframe. Sleeps through closed sessions.

        Polling cadence is tf_seconds / 2, floored at 10s and ceilinged
        at 600s. 10s floor for the sub-minute baseline (30s candle polls
        every 15s). The session check is per-cycle — if the market just
        closed, abort the whole cycle and sleep to the next interval.
        """
        self._running = True

        async def poll_one_tf(tf: str) -> None:
            # candle duration in seconds. _parse_timeframe returns
            # (multiplier, polygon_timespan, candle_ms) and candle_ms
            # is correct for ANY unit — including 's'. The previous
            # code hand-rolled unit branching that only handled 'm'/'h'
            # and silently treated '10s' as 10 minutes (fell through
            # the else to *60). Use the already-correct candle_ms.
            multiplier, _, candle_ms = _parse_timeframe(tf)
            tf_s = (multiplier * candle_ms) // 1000

            # Half-candle polling, 10s floor, 600s ceiling.
            # 30s baseline → 15s cadence; 5m top → 150s cadence.
            # The shared RateLimiter serializes all calls at ≤5 req/s.
            interval = min(max(tf_s // 2, 10), 600)

            # Per-(symbol, tf) cursor — same shape as forex/Binance.
            cursors: Dict[str, int] = {}
            for sym in symbols:
                cursors[sym] = (
                    initial_cursors.get(sym, {}).get(tf)
                    or (int(datetime.now(timezone.utc).timestamp() * 1000) - tf_s * 1000)
                )

            logger.info(f"[polygon_options:{tf}] polling every {interval}s")

            was_closed = True  # tracks market-open transition for jitter
            while self._running:
                try:
                    now = datetime.now(timezone.utc)
                    if not is_market_open("option", now):
                        # Market closed. Sleep one polling interval and
                        # check again.
                        was_closed = True
                        await asyncio.sleep(interval)
                        continue

                    # Market-open jitter: on the first cycle after a
                    # closed period, sleep a random fraction of the
                    # interval before fetching. Without this, all 6
                    # timeframe pollers × ~30 symbols fire simultaneously
                    # at 13:30:00 UTC, creating a spike that causes the
                    # ReadTimeouts seen in the logs.
                    if was_closed:
                        jitter = random.uniform(0, interval)
                        logger.debug(f"[polygon_options:{tf}] market open — jitter {jitter:.1f}s before first fetch")
                        await asyncio.sleep(jitter)
                        was_closed = False
                        now = datetime.now(timezone.utc)

                    batch: List[OHLCPoint] = []
                    for sym in symbols:
                        native = self.to_native_symbol(sym)
                        multiplier, timespan, _ = _parse_timeframe(tf)
                        url = (
                            f"https://api.polygon.io/v2/aggs/ticker/{native}"
                            f"/range/{multiplier}/{timespan}/{cursors[sym]}/"
                            f"{int(now.timestamp() * 1000)}"
                        )
                        params = {
                            "adjusted": "true",
                            "sort":     "asc",
                            "limit":    self.MAX_CANDLES_PER_CALL,
                            "apiKey":   self.api_key,
                        }

                        try:
                            client = await self._get_client()
                            async with self.rate_limiter:
                                # 20s timeout. Originally 10s with a
                                # "fail fast" intent, but in practice
                                # Polygon's 30s/45s options aggregations
                                # consistently exceeded 10s while 1m+
                                # stayed well under — meaning the live
                                # poller succeeded on long timeframes
                                # and ReadTimeout'd 100% of the time on
                                # short ones. 20s gives short-TF calls
                                # the headroom they need without paying
                                # warmup's full 30s budget per symbol.
                                # A truly stuck request still bails
                                # within the polling interval (30s+),
                                # so the original "don't block the
                                # cycle" intent is preserved.
                                resp = await client.get(url, params=params, timeout=20.0)
                            if resp.status_code == 429:
                                # Don't aggressively retry inside the live loop;
                                # the next polling cycle will retry naturally.
                                # This avoids amplifying the rate-limit storm.
                                logger.warning(f"[polygon_options:{tf}] 429 on {sym}; will retry next cycle")
                                continue
                            if resp.status_code in (401, 403):
                                logger.error(
                                    f"{resp.status_code} from Polygon options for {sym}. "
                                    f"Stopping live poll for this symbol."
                                )
                                continue
                            resp.raise_for_status()
                            results = resp.json().get("results", [])
                        except Exception as e:
                            # str(e) is empty for several httpx network
                            # exceptions (ReadTimeout, ConnectTimeout,
                            # etc.) — the class name is the only signal.
                            # Always log the type; append str(e) only
                            # when it's non-empty so we don't get a
                            # dangling colon. exc_info gives a full
                            # traceback for the genuinely unexpected
                            # (non-network) cases without bloating the
                            # common timeout-line.
                            detail = f": {e}" if str(e) else ""
                            logger.error(
                                f"[polygon_options:{tf}] {sym} fetch error "
                                f"({type(e).__name__}){detail}",
                                exc_info=not isinstance(e, httpx.TransportError),
                            )
                            continue

                        # Resample raw bars to timeframe-aligned windows
                        # (same logic as fetch_history) and filter to only
                        # CLOSED windows — those where window_start + tf_ms
                        # is in the past. Open windows (still accumulating
                        # trades) are deliberately held back; they'll be
                        # emitted on a later cycle once enough wall-clock
                        # time has passed for the window to close.
                        #
                        # This is the live counterpart to the resampling
                        # done in fetch_history. Without it, sparse
                        # contracts would feed mixed-resolution bars to
                        # Service 2's buffer, violating Sumtyme's
                        # uniform-spacing requirement.
                        if not results:
                            continue
                        _, _, base_ms = _parse_timeframe(tf)
                        tf_ms_local = multiplier * base_ms
                        now_ms = int(now.timestamp() * 1000)
                        resampled = _resample_to_timeframe(results, tf)
                        emitted_for_sym = 0
                        for tick in resampled:
                            window_start = tick["t"]
                            if window_start + tf_ms_local > now_ms:
                                # Window still in progress — skip until
                                # next cycle. Don't advance the cursor
                                # past it either; we need to re-fetch
                                # the in-progress data next cycle.
                                continue
                            batch.append(OHLCPoint(
                                symbol=sym,
                                timestamp=window_start,
                                open=tick["o"],
                                high=tick["h"],
                                low=tick["l"],
                                close=tick["c"],
                                timeframe=tf,
                                asset_class=self.asset_class,
                            ))
                            # Advance cursor to the start of the NEXT
                            # window (= end of THIS window). Using
                            # window_start + tf_ms_local means the next
                            # fetch picks up raw bars whose timestamps
                            # are >= the next window's start, which is
                            # exactly what we want. A +1 here would skip
                            # the first raw bar of the next window —
                            # subtle, but breaks aggregation when that
                            # window eventually closes (its open value
                            # would be wrong and one bar of high/low
                            # would be lost).
                            cursors[sym] = max(
                                cursors[sym], window_start + tf_ms_local,
                            )
                            emitted_for_sym += 1
                        if emitted_for_sym:
                            logger.debug(
                                f"[polygon_options:{tf}] {sym}: emitted "
                                f"{emitted_for_sym} closed windows"
                            )

                    if batch:
                        await callback(batch)
                except Exception as e:
                    logger.error(f"[polygon_options:{tf}] cycle error: {e}")

                await asyncio.sleep(interval)

        tasks = [asyncio.create_task(poll_one_tf(tf)) for tf in timeframes]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for t in tasks:
                t.cancel()
            raise