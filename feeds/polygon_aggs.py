"""
feeds/polygon_aggs.py — shared Polygon aggregates machinery for the
Phase 7 spot feeds (fx + equity).

PolygonForexFeed (feeds/polygon_forex.py) and PolygonStocksFeed
(feeds/polygon_stocks.py) are thin subclasses of _PolygonAggsFeedBase.
The fetch/poll mechanics are byte-for-byte the algorithm that
polygon_options.py runs in production — backward-paginated history,
429-with-Retry-After handling, resample-to-uniform-windows, closed-
window-only live emission, market-open jitter — parameterised by:

  - `asset_class`        — 'fx' | 'equity' (class attribute)
  - `to_native_symbol()` — canonical → Polygon ticker mapping
                           (fx: EURUSD → C:EURUSD; equity: identity)
  - session gating       — is_market_open(self.asset_class, ...) so fx
                           polls 24/5 and equities poll the cash session

Why a shared base instead of two more copies of polygon_options.py:
the system's standing rule is one canonical implementation per
concern. polygon_options.py itself is NOT refactored onto this base —
it's battle-tested production code and consolidation would risk the
exact subtle behaviours (cursor advance, partial-window holdback) its
inline comments exist to protect. Instead this module imports the two
pure helpers it shares (_parse_timeframe, _resample_to_timeframe)
straight from polygon_options so the timeframe arithmetic and the
bucketing rule have exactly one definition.

TODO(consolidation): when polygon_options next needs surgery anyway,
move the helpers here and re-parent it onto this base.
"""
from __future__ import annotations

import asyncio
import logging
import math
import random
from datetime import datetime, timezone
from typing import Dict, List, Set

import httpx

from feeds.base import AssetFeed, LiveDataCallback, is_market_open
# Single shared definition of the timeframe arithmetic and the OHLC
# bucketing rule — see module docstring. Importing the underscore
# names is deliberate: a private copy here WOULD drift.
from feeds.polygon_options import _parse_timeframe, _resample_to_timeframe
from producer.bar import OHLCPoint
from producer.bar import RateLimiter

logger = logging.getLogger(__name__)


class _PolygonAggsFeedBase(AssetFeed):
    """Shared Polygon /v2/aggs feed. Subclasses set `asset_class`,
    `name`, `SUPPORTED_TIMEFRAMES`, and override `to_native_symbol`.

    Each concrete feed instance owns its own RateLimiter — the same
    per-feed isolation rule as the rest of the multi-feed
    architecture. Within one feed, Service 1's seeder and live poller
    share THIS instance's limiter (they call through the same feed
    object), preserving the "one limiter per upstream budget"
    invariant. Note the Polygon Currencies / Stocks / Options plans
    share one API key but are separately metered server-side, so
    separate client-side limiters per feed is the correct shape.
    """

    # Subclasses MUST set: asset_class, name, SUPPORTED_TIMEFRAMES.
    SUPPORTED_TIMEFRAMES: Set[str] = set()

    MAX_CANDLES_PER_CALL = 5000  # Polygon aggregates per-call cap
    # Lever 1 (coarse-TF timeout fix). A single request for thousands of
    # coarse bars (e.g. 12h × 5000 ≈ 10 years) makes Polygon assemble an
    # enormous aggregate in one HTTP call, which ReadTimeouts even at a
    # 60s read budget. Capping bars-per-CHUNK turns one giant request
    # into several small fast ones. chunk_ms is derived from THIS cap (not
    # points_needed) so the time window shrinks in lockstep with the bar
    # count — otherwise Polygon would still scan a multi-year window and
    # the cap would buy nothing.
    MAX_BARS_PER_CHUNK = 500

    def __init__(
        self,
        api_key: str,
        symbols: List[str],
        timeframes: List[str],
        requests_per_second: float = 25.0,
        max_concurrent: int = 10,
        inception_ms: dict | None = None,
    ):
        # inception_ms: {canonical_symbol: epoch_ms}. Hard floor on
        # history — the feed will never request or return a bar older
        # than a symbol's inception. Guards against ticker recycling
        # (a reassigned symbol whose Polygon history belongs to a
        # different, delisted security). Absent symbol → no floor.
        # Source: config.EQUITY_INCEPTION_DATES via inception_ms_for.
        self._inception_ms = inception_ms or {}
        # Validate timeframes early — mismatches die loudly at
        # construction rather than silently producing empty fetches.
        # (Same contract as PolygonOptionsFeed.)
        unsupported = [tf for tf in timeframes if tf not in self.SUPPORTED_TIMEFRAMES]
        if unsupported:
            raise ValueError(
                f"{type(self).__name__} does not support timeframe(s): {unsupported}. "
                f"Supported: {sorted(self.SUPPORTED_TIMEFRAMES)}"
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
        return self.SUPPORTED_TIMEFRAMES

    def to_native_symbol(self, symbol: str) -> str:  # pragma: no cover
        raise NotImplementedError

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            # Explicit pool + split timeout. Rationale:
            #   - max_connections bounds concurrency at the transport
            #     so a warmup burst can't over-commit the box's
            #     sockets (the all-equity re-seed saturation we hit).
            #     Sized just above the rate limiter's max_concurrent.
            #   - Split timeout: heavy coarse-TF reads (a 12h backfill
            #     spans years and Polygon is slow to assemble it) get a
            #     generous READ budget, while a stalled CONNECT or a
            #     starved POOL fails FAST (cheap retry) instead of
            #     burning the full read budget. Previously one flat 45s
            #     timeout meant pool contention and slow reads looked
            #     identical and both cost 45s.
            self._client = httpx.AsyncClient(
                limits=httpx.Limits(
                    max_connections=self.rate_limiter.max_concurrent + 4,
                    max_keepalive_connections=self.rate_limiter.max_concurrent,
                ),
                timeout=httpx.Timeout(
                    connect=10.0,
                    read=60.0,
                    write=10.0,
                    pool=5.0,
                ),
            )
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

        Returns whatever's available; possibly fewer than
        `points_needed`. That is NOT fatal for these classes:

          - fx: a pair Polygon doesn't carry in the requested
            direction (e.g. an inverse quote like USDGBP) returns
            zero rows everywhere. The seeder's "got 0 points" log
            line is the operator's signal to swap the pair for its
            market-convention direction in ASSET_CONFIGS. The pair's
            tile simply never appears (SymbolsCache only surfaces
            symbols with recent `prices` rows), so a dead pair is
            inert, not corrupting.
          - equity: recently listed names (SPCX) and coarse
            timeframes simply have less history than WINDOW_SIZE.
            Those streams accumulate live bars and start forecasting
            whenever the buffer eventually fills — by design.

        Resampling: Polygon's aggregates don't reliably honor the
        requested multiplier for sparse series (thin ADRs at 15s,
        exotic pairs). Sumtyme's interval contract requires uniform
        spacing, so we re-bucket via _resample_to_timeframe — the
        same rule, same function, as the options feed.
        """
        if timeframe not in self.SUPPORTED_TIMEFRAMES:
            logger.error(f"Unsupported {self.asset_class} timeframe: {timeframe}")
            return []

        multiplier, timespan, base_ms = _parse_timeframe(timeframe)
        native_symbol = self.to_native_symbol(symbol)
        client = await self._get_client()

        collected_points: List[dict] = []
        # Window spans ~MAX_BARS_PER_CHUNK bars (not the full request)
        # × 1.5 slack so each window comfortably contains its target bar
        # count even across weekend/holiday gaps. Smaller windows = fast
        # Polygon responses; we just take more of them.
        bars_per_chunk = min(points_needed, self.MAX_BARS_PER_CHUNK)
        chunk_ms = int(bars_per_chunk * multiplier * base_ms * 1.5)
        current_to = int(datetime.now(timezone.utc).timestamp() * 1000)
        empty_chunks = 0
        # Empty-chunk tolerance must SCALE with chunk count. With small
        # windows a 5000-bar seed walks ~points_needed/bars_per_chunk
        # chunks, and a sparse coarse series (e.g. an illiquid pair at
        # 12h, or weekend/holiday gaps) will legitimately return several
        # empty windows in a row. The old flat `< 3` would abort such a
        # series before it reached its target. Allow empties proportional
        # to the expected walk length (capped), so genuine sparsity is
        # tolerated while a real outage (every chunk failing) still
        # terminates within a bounded number of attempts.
        expected_chunks = max(1, math.ceil(points_needed / bars_per_chunk))
        max_empty_chunks = min(expected_chunks * 2, 12)

        # Ticker-recycling guard. If this symbol has an inception
        # floor, never page back past it — Polygon serves a recycled
        # ticker's pre-listing history as one continuous series, and
        # those bars belong to a different security. When the backward
        # walk reaches the floor we stop; sub-floor bars that slip in
        # on a straddling chunk are filtered out before return.
        floor_ms = self._inception_ms.get(symbol)
        if floor_ms is not None and current_to <= floor_ms:
            # Now is before inception (clock skew / misconfig) — nothing
            # legitimate to fetch.
            logger.warning(
                f"{symbol} {timeframe}: current time precedes inception "
                f"floor; no history fetched"
            )
            return []

        while len(collected_points) < points_needed and empty_chunks < max_empty_chunks:
            # Floor reached: once the cursor has descended to/through
            # inception there is nothing legitimate left to fetch.
            # Break cleanly rather than burn the 3-empty-chunk budget.
            if floor_ms is not None and current_to <= floor_ms:
                break
            current_from = current_to - chunk_ms
            if floor_ms is not None and current_from < floor_ms:
                # Clamp this chunk's lower bound to inception so we ask
                # Polygon only for the [inception, current_to] slice.
                current_from = floor_ms
            limit_needed = min(
                points_needed - len(collected_points),
                self.MAX_BARS_PER_CHUNK,
            )

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
                        # 45s warmup timeout (was 30s). Warmup is
                        # latency-insensitive (it runs once at startup),
                        # and the heavy requests are coarse-timeframe
                        # backfills — a 5000-bar 4h/6h/12h chunk spans
                        # YEARS of history and Polygon's response is
                        # large and occasionally slow. The blank-message
                        # "fetch error ... (attempt 1/3):" warnings were
                        # ReadTimeouts on exactly these heavy queries,
                        # not 5xx. A longer timeout lets them complete;
                        # the extra wall-clock cost only applies to the
                        # rare slow request, not the common fast one.
                        # Uses the client's default split timeout
                        # (read=60s) — right for heavy warmup backfills.
                        resp = await client.get(url, params=params)

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
                        logger.error(
                            f"{resp.status_code} from Polygon for {native_symbol}. "
                            f"Check that POLYGON_API_KEY has {self.asset_class} "
                            f"({self.name}) access. NOT retrying."
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
                    # str(e) is EMPTY for several httpx transport
                    # exceptions (ReadTimeout, ConnectTimeout,
                    # ConnectError) — only the class name carries the
                    # signal. Logging bare {e} (the previous behaviour)
                    # produced a dangling-colon line with no cause,
                    # making timeout loops indistinguishable from 5xx
                    # in the logs. Mirror the live loop: always log the
                    # type, append str(e) only when non-empty.
                    detail = f": {e}" if str(e) else ""
                    logger.warning(
                        f"{self.name} fetch error {symbol} {timeframe} "
                        f"({type(e).__name__}){detail} "
                        f"(attempt {attempt + 1}/3)"
                    )
                    # Jittered backoff. A flat 2**attempt schedule makes
                    # every concurrently-warming stream retry in lockstep,
                    # so during a degraded-gateway stretch they re-hit
                    # Polygon in synchronized waves (thundering herd) and
                    # prolong the degradation. Spread them out.
                    backoff = (2 ** attempt) * random.uniform(0.5, 1.5)
                    await asyncio.sleep(backoff)

            if not success:
                # This chunk exhausted its retries (timeout / 5xx /
                # transport). The OLD behaviour was to `break` the whole
                # pagination loop and return whatever was collected so
                # far — which meant a failure on the FIRST chunk
                # discarded the entire fetch and left the stream EMPTY
                # (count=0). The seeder then re-queues that stream every
                # warmup, producing the observed non-converging retry
                # loop on MSFT/NVDA coarse timeframes.
                #
                # New behaviour: don't let one bad chunk zero the stream.
                #   - If we've already collected bars, keep them — a
                #     short buffer beats an empty one, and the next
                #     warmup tops it up. Stop here (further chunks are
                #     older history we can fill later).
                #   - If we've collected NOTHING yet, advance the cursor
                #     past this window and try the NEXT (older) chunk
                #     instead of giving up. A single transient failure on
                #     the most-recent window shouldn't doom the stream;
                #     the empty_chunks counter still bounds total effort
                #     so a sustained outage terminates cleanly.
                if collected_points:
                    logger.error(
                        f"Chunk failed for {symbol} {timeframe} after "
                        f"collecting {len(collected_points)} pts; "
                        f"returning partial history."
                    )
                    break
                logger.warning(
                    f"First chunk failed for {symbol} {timeframe}; "
                    f"advancing past it and retrying older window "
                    f"(empty_chunks={empty_chunks + 1}/3)."
                )
                current_to = current_from - 1
                empty_chunks += 1
                if empty_chunks >= max_empty_chunks:
                    logger.error(
                        f"Failed to fetch any data for {symbol} "
                        f"{timeframe} after {max_empty_chunks} "
                        f"failed/empty chunks."
                    )
                    break
                continue

        # Belt-and-suspenders: drop any collected bar at or before the
        # inception floor. The current_from clamp above prevents asking
        # for them, but a chunk that straddles the boundary can still
        # return a few; this guarantees none reach the resampler/DB.
        if floor_ms is not None:
            before = len(collected_points)
            collected_points = [p for p in collected_points if p["t"] >= floor_ms]
            dropped = before - len(collected_points)
            if dropped:
                logger.info(
                    f"{symbol} {timeframe}: dropped {dropped} pre-inception "
                    f"bar(s) below {floor_ms} (ticker-recycling guard)"
                )

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
        """One poll loop per timeframe. Sleeps through closed sessions
        (fx: weekend gap; equity: overnight/weekend — both driven by
        is_market_open(self.asset_class, ...)).

        Polling cadence is tf_seconds / 2, floored at 8s, ceilinged at
        600s. The floor is 8s rather than the options feed's 10s
        because the Phase 7 baseline is 15s (15s candle → ~8s cadence
        keeps emission latency under one window). Closed-window-only
        emission and the cursor-advance rule are identical to the
        options feed — see its inline comments for the rationale; the
        invariants are the same.
        """
        self._running = True

        async def poll_one_tf(tf: str) -> None:
            multiplier, _, candle_ms = _parse_timeframe(tf)
            tf_s = (multiplier * candle_ms) // 1000

            # Half-candle polling, 8s floor, 600s ceiling.
            interval = min(max(tf_s // 2, 8), 600)

            # Per-(symbol, tf) cursor — same shape as the other feeds.
            cursors: Dict[str, int] = {}
            for sym in symbols:
                cursors[sym] = (
                    initial_cursors.get(sym, {}).get(tf)
                    or (int(datetime.now(timezone.utc).timestamp() * 1000) - tf_s * 1000)
                )
                # Ticker-recycling guard, live path: never let a stale
                # DB cursor (e.g. one seeded from pre-inception rows
                # that haven't been purged yet) drag the live fetch
                # back below inception. Floor it.
                floor_ms = self._inception_ms.get(sym)
                if floor_ms is not None and cursors[sym] < floor_ms:
                    cursors[sym] = floor_ms

            logger.info(f"[{self.name}:{tf}] polling every {interval}s")

            was_closed = True  # tracks market-open transition for jitter
            while self._running:
                try:
                    now = datetime.now(timezone.utc)
                    if not is_market_open(self.asset_class, now):
                        was_closed = True
                        await asyncio.sleep(interval)
                        continue

                    # Market-open jitter: on the first cycle after a
                    # closed period, sleep a random fraction of the
                    # interval before fetching, so all timeframe
                    # pollers × symbols don't fire simultaneously at
                    # the session open. Same spike-avoidance the
                    # options feed needed.
                    if was_closed:
                        jitter = random.uniform(0, interval)
                        logger.debug(
                            f"[{self.name}:{tf}] market open — jitter "
                            f"{jitter:.1f}s before first fetch"
                        )
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
                                # 20s timeout — inherited from the
                                # options feed's hard-won finding that
                                # Polygon's sub-minute aggregations
                                # routinely exceed 10s. See the long
                                # comment in polygon_options.py.
                                resp = await client.get(url, params=params, timeout=20.0)
                            if resp.status_code == 429:
                                # Don't retry inside the live loop; the
                                # next polling cycle retries naturally.
                                logger.warning(f"[{self.name}:{tf}] 429 on {sym}; will retry next cycle")
                                continue
                            if resp.status_code in (401, 403):
                                logger.error(
                                    f"{resp.status_code} from Polygon for {native}. "
                                    f"Stopping live poll for this symbol."
                                )
                                continue
                            resp.raise_for_status()
                            results = resp.json().get("results", [])
                        except Exception as e:
                            # str(e) is empty for several httpx network
                            # exceptions — log the type, append str(e)
                            # only when non-empty (same convention as
                            # the options feed).
                            detail = f": {e}" if str(e) else ""
                            logger.error(
                                f"[{self.name}:{tf}] {sym} fetch error "
                                f"({type(e).__name__}){detail}",
                                exc_info=not isinstance(e, httpx.TransportError),
                            )
                            continue

                        # Resample to timeframe-aligned windows and emit
                        # only CLOSED windows. Open windows are held back
                        # and re-fetched next cycle — and the cursor is
                        # NOT advanced past them. The cursor advances to
                        # window_start + tf_ms (no +1 — a +1 skips the
                        # first raw bar of the next window and corrupts
                        # its open/high/low). Identical invariants to
                        # the options live loop.
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
                            cursors[sym] = max(
                                cursors[sym], window_start + tf_ms_local,
                            )
                            emitted_for_sym += 1
                        if emitted_for_sym:
                            logger.debug(
                                f"[{self.name}:{tf}] {sym}: emitted "
                                f"{emitted_for_sym} closed windows"
                            )

                    if batch:
                        await callback(batch)
                except Exception as e:
                    logger.error(f"[{self.name}:{tf}] cycle error: {e}")

                await asyncio.sleep(interval)

        tasks = [asyncio.create_task(poll_one_tf(tf)) for tf in timeframes]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for t in tasks:
                t.cancel()