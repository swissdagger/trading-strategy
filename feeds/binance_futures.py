"""
feeds/binance_futures.py — Binance USDⓈ-M Futures klines as an AssetFeed.

Confirmed binance.com Futures (not binance.us) per the plan. Endpoint:
  https://fapi.binance.com/fapi/v1/klines

Phase 3 — feed exists but is not wired into Service 1 yet. ASSET_CONFIGS
still has only the (disabled) fx entry. Phase 4 adds:

    "crypto": {
        "source": "binance_futures",
        "symbols": ["BTCUSDT"],
        "timeframes": TIMEFRAMES_COMMON,
    }

and Service 1's `_instantiate_feed` registry gains a `binance_futures`
branch. Until that happens, this file is exercised only by
scripts/test_binance_futures.py.

A few specifics worth surfacing:

- **Weight budget**, not request count. Binance Futures gives ~2400
  weight/minute. Klines `limit=1500` costs 10 weight per request, so
  the absolute ceiling is ~240 req/min. We start much slower
  (requests_per_second=5, max_concurrent=3) and monitor
  `X-MBX-USED-WEIGHT-1M` in case headers tell us we've underestimated.

- **HTTP 418 is a ban warning**, not just throttling. Binance's
  documented behaviour: keep sending after a 429 and you get a 418
  (and an IP ban that escalates if ignored). We honor `Retry-After`
  strictly on both, and back off aggressively on 418.

- **Closed candles only.** Klines responses include the in-progress
  current candle as the last element. Its `close_time` is in the
  future. Including it in fetch_history would put a non-final
  OHLC into the Sumtyme 5000-point window, corrupting forecasts
  on every refresh. The filter on `close_time < now_ms` runs in
  one place — at the OHLCPoint construction site — so live polling
  and history fetches both inherit it.

- **No auth required.** /fapi/v1/klines is a public market data
  endpoint. We send no API key, no signature. If Binance ever moves
  this behind auth, the env var pattern from Polygon (API_KEY in
  .env, read in _instantiate_feed) is the place to extend.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Set

import httpx

from feeds.base import AssetFeed, LiveDataCallback
from producer.bar import OHLCPoint
from producer.bar import RateLimiter

logger = logging.getLogger(__name__)


# Binance klines `interval` values. The names match our canonical
# timeframe strings — there's no translation needed for the supported
# entries. The `10m` is intentionally absent (Binance Futures doesn't
# offer it); supported_timeframes() returns this set so a config
# that asks for 10m gets rejected at startup rather than at first call.
_BINANCE_INTERVALS: Set[str] = {
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M",
}


# Per-request limit. Binance accepts up to 1500 for klines on
# Futures; weight per request scales with this:
#   limit <= 100:    weight 1
#   limit <= 500:    weight 2
#   limit <= 1000:   weight 5
#   limit <= 1500:   weight 10
# We always ask for the max to minimize round-trips during the
# 5000-point seed (4 requests of 1500 instead of 50 of 100).
_KLINES_LIMIT = 1500


def _tf_to_seconds(tf: str) -> int:
    """Convert a canonical timeframe string to seconds. Used for
    paginating fetch_history's forward walk and computing live-poll
    cadence. Mirrors the same helper in service_1_ingestion.py;
    duplicated here to keep the feed self-contained.
    """
    multiplier = int(tf[:-1])
    unit = tf[-1].lower()
    return multiplier * {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}.get(unit, 60)


class BinanceFuturesFeed(AssetFeed):
    """USDⓈ-M Futures perpetuals from binance.com.

    Crypto spot, when added later, lives in a separate file
    (feeds/binance_spot.py) wrapping api.binance.com/api/v3/klines.
    Symbols don't collide — but if a future scenario does name 'BTCUSDT'
    in both, the asset_class prefix in our Kafka key (Seam 1) keeps
    them on distinct partitions.
    """

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
        # Validate timeframes up front. The contract is:
        # supported_timeframes() is the source of truth; any request
        # for an unsupported timeframe should fail loudly here, not
        # silently produce empty results.
        unsupported = [tf for tf in timeframes if tf not in _BINANCE_INTERVALS]
        if unsupported:
            raise ValueError(
                f"BinanceFuturesFeed does not support timeframe(s): {unsupported}. "
                f"Supported: {sorted(_BINANCE_INTERVALS)}"
            )

        self.symbols = symbols
        self.timeframes = timeframes
        # Per-feed RateLimiter (user direction from Phase 1 design —
        # each feed owns its own budget). Conservative starting values
        # from the plan; raise after watching X-MBX-USED-WEIGHT-1M for
        # a few hours under load.
        self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            max_concurrent=max_concurrent,
        )
        self._client: httpx.AsyncClient | None = None
        self._running = False

    def supported_timeframes(self) -> Set[str]:
        return _BINANCE_INTERVALS

    def to_native_symbol(self, symbol: str) -> str:
        # Binance symbols are pass-through (BTCUSDT, ETHUSDT, etc.)
        # Polygon needs `C:` for forex and `O:` for options; Binance
        # uses the bare ticker.
        return symbol

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient()
        return self._client

    async def aclose(self) -> None:
        """Idempotent shutdown — Service 1 calls this in its finally
        block. After aclose, the feed instance is unusable.
        """
        self._running = False
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ---------------------------------------------------------------
    # Common request path
    # ---------------------------------------------------------------
    async def _request_klines(
        self,
        symbol: str,
        interval: str,
        *,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int = _KLINES_LIMIT,
    ) -> List[List[Any]] | None:
        """One klines call with retry/backoff. Returns the raw klines
        array, or None on persistent failure.

        Honors `Retry-After` on 429 and 418. 418 is a ban warning;
        we treat it as 429-but-louder and let the retry loop handle
        spacing — escalating further is the caller's problem.
        """
        client = await self._get_client()
        params: Dict[str, Any] = {
            "symbol":   symbol,
            "interval": interval,
            "limit":    limit,
        }
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time

        for attempt in range(4):
            try:
                async with self.rate_limiter:
                    resp = await client.get(self.BASE_URL, params=params, timeout=30.0)

                # Monitor used-weight so an operator running the test
                # script with --verbose sees how close we are to the
                # 2400/min ceiling. Not load-bearing for the feed
                # itself, just diagnostic.
                used = resp.headers.get("X-MBX-USED-WEIGHT-1M")
                if used:
                    logger.debug(f"[binance] X-MBX-USED-WEIGHT-1M={used}")

                if resp.status_code in (429, 418):
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        backoff = float(retry_after) if retry_after else 2 ** attempt
                    except (ValueError, TypeError):
                        backoff = 2 ** attempt
                    severity = "418 BAN WARNING" if resp.status_code == 418 else "429"
                    logger.warning(
                        f"{severity} on {symbol} {interval}; backing off {backoff:.1f}s "
                        f"(attempt {attempt + 1}/4)"
                    )
                    await asyncio.sleep(backoff)
                    continue

                if resp.status_code == 403:
                    # Binance returns 403 for geo-blocked requests (e.g.
                    # most cloud data-center IP ranges, US-based hosts
                    # depending on Binance's current policy). This is
                    # not transient — retrying just wastes the rate
                    # budget and produces misleading 4-attempt logs.
                    # Abort the request and let the caller decide what
                    # to do (Phase 4 may want to disable the feed
                    # entirely; Phase 3 just surfaces the error).
                    logger.error(
                        f"403 Forbidden from Binance for {symbol} {interval}. "
                        "Likely geographic / IP-range block — your host is not "
                        "permitted to access fapi.binance.com. Possible fixes: "
                        "run from a different network, use a proxy, or switch "
                        "to a permitted region. NOT retrying."
                    )
                    return None

                resp.raise_for_status()
                return resp.json()

            except httpx.HTTPError as e:
                logger.warning(
                    f"Binance fetch error {symbol} {interval} "
                    f"(attempt {attempt + 1}/4): {e}"
                )
                await asyncio.sleep(2 ** attempt)

        logger.error(f"Binance: persistent failure for {symbol} {interval}")
        return None

    def _ticks_to_points(
        self,
        symbol: str,
        timeframe: str,
        ticks: List[List[Any]],
    ) -> List[OHLCPoint]:
        """Convert raw klines arrays to OHLCPoints, filtering out the
        in-progress candle.

        Closed-candle filter: kline[6] is close_time (ms). If it's
        in the future, the candle is still open and its OHLC values
        will change. Including it would feed a moving target into
        Sumtyme.
        """
        now_ms = int(time.time() * 1000)
        points: List[OHLCPoint] = []
        for tick in ticks:
            # tick = [open_time, open, high, low, close, volume,
            #         close_time, quote_volume, trades, taker_base,
            #         taker_quote, ignore]
            close_time = tick[6]
            if close_time > now_ms:
                # In-progress candle. Skip.
                continue
            points.append(OHLCPoint(
                symbol=symbol,
                timestamp=tick[0],
                open=float(tick[1]),
                high=float(tick[2]),
                low=float(tick[3]),
                close=float(tick[4]),
                timeframe=timeframe,
                asset_class=self.asset_class,
            ))
        return points

    # ---------------------------------------------------------------
    # HISTORY (warmup phase)
    # ---------------------------------------------------------------
    async def fetch_history(
        self,
        symbol: str,
        timeframe: str,
        points_needed: int,
    ) -> List[OHLCPoint]:
        """Forward-paginate klines until we have `points_needed`
        candles.

        Polygon's forex feed paginates *backwards* (from now toward
        the past) because the `sort=desc` parameter makes that cheaper
        — the API returns the newest first and we trim. Binance's
        klines API has no sort parameter; it returns ascending by
        `open_time` within the requested window. Paginating forward
        from `now - points_needed * tf_seconds` is the natural fit.

        On a fresh DB the first call for BTCUSDT 5m fetches 1500
        candles in one request — well within the 2400 weight budget.
        Even a full 5000-point seed is 4 sequential requests, ~40
        weight, ~8 seconds at our 5 req/s default. The rate-limit
        ceiling is nowhere close to relevant at v1 scale.
        """
        if timeframe not in _BINANCE_INTERVALS:
            # Caller bug — constructor should have caught this. Defensive.
            logger.error(f"Unsupported Binance timeframe: {timeframe}")
            return []

        tf_seconds = _tf_to_seconds(timeframe)
        now_ms = int(time.time() * 1000)
        # Start far enough back to cover the requested point count plus
        # a small fudge factor (closed-candle filter may drop the most
        # recent candle).
        start_ms = now_ms - (points_needed + 5) * tf_seconds * 1000

        collected: List[OHLCPoint] = []
        cursor = start_ms

        while len(collected) < points_needed:
            ticks = await self._request_klines(
                symbol=symbol,
                interval=timeframe,
                start_time=cursor,
                limit=_KLINES_LIMIT,
            )
            if not ticks:
                break  # persistent failure already logged

            new_points = self._ticks_to_points(symbol, timeframe, ticks)
            if not new_points:
                # Either empty window or every candle filtered as open.
                # Advance cursor past this batch to avoid infinite loop;
                # if Binance returned nothing, advance by the request
                # window size; if it returned only in-progress candles,
                # break since there's nothing newer to fetch.
                if not ticks:
                    break
                cursor = ticks[-1][0] + 1
                continue

            collected.extend(new_points)
            # Advance past the last returned bar; +1 to avoid re-fetching
            # the same bar on the next call.
            cursor = new_points[-1].timestamp + 1

            # Defensive bail: if Binance returned fewer than the limit
            # and we haven't reached now, there's nothing more available
            # in this window. Stop rather than spin.
            if len(ticks) < _KLINES_LIMIT:
                break

        # Trim to exactly points_needed (may have over-fetched due to
        # the +5 fudge or the last batch being a full 1500).
        return collected[-points_needed:] if len(collected) > points_needed else collected

    # ---------------------------------------------------------------
    # LIVE (post-warmup, runs forever)
    # ---------------------------------------------------------------
    async def start_live(
        self,
        symbols: List[str],
        timeframes: List[str],
        initial_cursors: Dict[str, Dict[str, int]],
        callback: LiveDataCallback,
    ) -> None:
        """One asyncio task per timeframe. Each task polls every
        symbol once per cycle, batches results, invokes callback once.

        Cadence: `tf_seconds / 2`, floored at 30s, ceiling at 600s.
        - 1m tf polls every 30s (catches both candles in the minute)
        - 5m polls every 150s
        - 12h polls every 600s
        The plan says `tf/2` with no floor/ceiling; the floor exists
        because polling 1m at 30s gives us redundancy for missed bars
        without exhausting the weight budget, and the ceiling because
        a 12h-tf bar's close is what matters and waiting 6h between
        polls is plenty.

        24/7 — no session-hours check, unlike options.
        """
        self._running = True

        async def poll_one_tf(tf: str) -> None:
            tf_s = _tf_to_seconds(tf)
            interval = min(max(tf_s // 2, 30), 600)
            # Per-symbol cursors live across polling cycles. Seeded from
            # initial_cursors (latest already-persisted timestamp), so the
            # first poll only fetches what warmup missed.
            cursors: Dict[str, int] = {}
            for sym in symbols:
                cursors[sym] = (
                    initial_cursors.get(sym, {}).get(tf)
                    or (int(time.time() * 1000) - tf_s * 1000)
                )

            # Staggered start so all timeframes don't pummel Binance
            # at second zero. Smaller tfs go first.
            stagger = min(tf_s / 60, 60)
            await asyncio.sleep(stagger)
            logger.info(f"[binance:{tf}] polling every {interval}s")

            while self._running:
                try:
                    batch: List[OHLCPoint] = []
                    for sym in symbols:
                        # `endTime` omitted — let Binance return up to
                        # now. Limit kept small here (100) because each
                        # polling cycle for live data is incremental,
                        # not bulk.
                        ticks = await self._request_klines(
                            symbol=sym,
                            interval=tf,
                            start_time=cursors[sym],
                            limit=100,
                        )
                        if not ticks:
                            continue
                        points = self._ticks_to_points(sym, tf, ticks)
                        if not points:
                            continue
                        cursors[sym] = points[-1].timestamp + 1
                        batch.extend(points)

                    if batch:
                        await callback(batch)
                except Exception as e:
                    logger.error(f"[binance:{tf}] cycle error: {e}")

                await asyncio.sleep(interval)

        tasks = [asyncio.create_task(poll_one_tf(tf)) for tf in timeframes]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for t in tasks:
                t.cancel()
            raise