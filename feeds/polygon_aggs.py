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
from feeds.base import session_bounds_for, timeframe_to_ms
# Single shared definition of the timeframe arithmetic and the OHLC
# bucketing rule — see module docstring. Importing the underscore
# names is deliberate: a private copy here WOULD drift.
from feeds.polygon_options import _parse_timeframe, _resample_to_timeframe
from producer.bar import OHLCPoint
from producer.bar import RateLimiter

logger = logging.getLogger(__name__)


def _trades_to_ohlcpoints(
    ticks: List[dict],
    timeframe: str,
    symbol: str,
    asset_class: str,
) -> List[OHLCPoint]:
    """Bucket degenerate trade-ticks ({t,o,h,l,c} with o=h=l=c=price) into
    uniform timeframe-aligned OHLC windows via the SAME _resample_to_timeframe
    rule the aggregate path uses, then lift them into OHLCPoints — the exact
    type fetch_history returns, so downstream persist/cursor code is identical.

    Keeping the trade→bar bucketing on the shared resampler (rather than a
    private copy here) preserves the one-canonical-definition rule: a trade is
    just a bar whose open/high/low/close all equal its price, so no new
    bucketing logic is introduced."""
    windows = _resample_to_timeframe(ticks, timeframe)  # -> [{t,o,h,l,c}] asc
    return [
        OHLCPoint(
            symbol=symbol,
            timestamp=w["t"],
            open=w["o"],
            high=w["h"],
            low=w["l"],
            close=w["c"],
            timeframe=timeframe,
            asset_class=asset_class,
        )
        for w in windows
    ]


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
        resampled: List[dict] = []
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
        # to the expected walk length, so genuine sparsity is tolerated
        # while a real outage (every chunk failing) still terminates
        # within a bounded number of attempts.
        expected_chunks = max(1, math.ceil(points_needed / bars_per_chunk))

        # ...but a flat ceiling on that budget (the old `min(..., 12)`) is
        # WRONG for fine timeframes. Each empty chunk only rewinds the
        # cursor by chunk_ms of WALL-CLOCK time, and chunk_ms shrinks
        # linearly with the timeframe (5s chunk ≈ 1h; 30s chunk ≈ 6.25h).
        # When warmup runs into a market-closed stretch (overnight, a
        # weekend, a holiday) the most-recent wall-clock windows contain
        # NO trades and come back empty. A flat budget of 12 lets 30s
        # tunnel ~75h (clears a weekend) but lets 5s tunnel only ~12.5h —
        # so 5s/10s exhaust the budget inside the closed gap and return
        # ZERO bars, while 30s/1m+ punch through to live data. (This is
        # exactly the observed AAPL hydration: 30s=4739 but 5s/10s=0.)
        #
        # Fix: size the empty-chunk budget from a fixed WALL-CLOCK gap we
        # must always be able to clear, not from a fixed chunk count. The
        # gap below covers a 3-day weekend plus an adjacent market holiday
        # (e.g. Fri close → Tue open across a Monday holiday ≈ 4 days of
        # closed wall-clock) with margin. Coarse timeframes already clear
        # this with few chunks; fine timeframes get however many chunks
        # that span requires. A genuine outage still terminates because
        # the budget, while larger, remains bounded.
        MAX_CLOSED_GAP_MS = 4 * 24 * 60 * 60 * 1000  # 4 days
        gap_chunks = math.ceil(MAX_CLOSED_GAP_MS / chunk_ms)
        # Hard ceiling so a pathological tiny chunk_ms can't make the
        # budget unbounded; 600 chunks × MAX_BARS_PER_CHUNK is far more
        # walk than any real seed needs and still terminates a true outage
        # in finite time.
        max_empty_chunks = min(max(expected_chunks * 2, gap_chunks), 600)

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

        while len(resampled) < points_needed and empty_chunks < max_empty_chunks:
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
            # `resampled` is recomputed below from the running
            # `collected_points`, so this reflects the actual number of
            # OUTPUT bars we have so far — not raw bars. Resampling can
            # be lossy (a chunk boundary can split a window so one side
            # never accumulates a full bucket, sparse/holiday gaps drop
            # windows entirely), so raw-bar count alone is NOT a safe
            # proxy for "have we collected enough." Sizing limit_needed
            # off the real shortfall, and re-deriving that shortfall
            # every iteration, is what makes the loop keep paging back
            # until 5000 OUTPUT bars actually exist (previously it
            # silently undershot to ~4900 whenever resampling dropped a
            # few windows, because the loop's exit check never saw the
            # resampled count at all).
            limit_needed = min(
                points_needed - len(resampled),
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
                        # Re-resample on every successful chunk (not
                        # just once at the end) so the loop's own exit
                        # check — len(resampled) < points_needed — is
                        # measuring real OUTPUT bars instead of a stale
                        # value that's always 0 until the loop exits.
                        # _resample_to_timeframe buckets by window_start
                        # and tracks _first_t/_last_t per bucket itself,
                        # so it's order-independent — no need to sort
                        # collected_points first (confirmed from the
                        # source in polygon_options.py).
                        resampled = _resample_to_timeframe(
                            collected_points, timeframe,
                        )
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
                    f"(empty_chunks={empty_chunks + 1}/{max_empty_chunks})."
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
        resampled = resampled[-points_needed:]

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
    # SUB-MINUTE WARMUP (from raw ticks: trades for equity, quotes for fx)
    # ---------------------------------------------------------------
    TRADES_MAX_PER_CALL = 50000  # Polygon /v3/{trades,quotes} per-page cap

    # --- point-source hooks (overridden per asset class) ------------
    # The pagination/retry/resample machinery is shared; only two things differ
    # between a trades source (equity) and a quotes source (fx): the endpoint URL
    # and how one raw record becomes a (t_ms, price) pair. Subclasses override
    # these two hooks; the loop below stays single-sourced.

    def _subminute_source_url(self, native: str) -> str:
        """Endpoint for the raw tick source. Equity default: /v3/trades."""
        return f"https://api.polygon.io/v3/trades/{native}"

    def _tick_from_record(self, rec: dict):
        """Map one raw record -> (t_ms, price), or None to skip.

        Equity default: a TRADE. sip_timestamp (ns, SIP-consolidated) is the
        canonical time; fall back to participant_timestamp. price is the trade
        price. A trade is a degenerate bar (o=h=l=c=price)."""
        t_ns = rec.get("sip_timestamp") or rec.get("participant_timestamp")
        p = rec.get("price")
        if t_ns is None or p is None:
            return None
        return t_ns // 1_000_000, p

    async def _fetch_ticks_window(
        self,
        base_url: str,
        native: str,
        lo_ms: int,
        hi_ms: int,
    ) -> "tuple[List[dict], bool]":
        """Page one time window [lo_ms, hi_ms) NEWEST-FIRST, returning
        (ticks, ok). Each tick is a degenerate bar dict {t,o,h,l,c}. `ok` is
        False if a page failed after retries (partial result) or an auth error
        stopped the walk — the caller stops walking further back in that case.

        Paging order='desc' (most-recent-first) is deliberate and load-bearing:
        we want the NEWEST bars (for a fresh cursor) and want to stop as soon as
        the buffer is full, WITHOUT paging through the multi-million-trade tail
        of a liquid name. An ascending walk from far back spends the whole page
        budget on the oldest trades and never reaches recent ones (the bug this
        replaced). Descending, we take what we need off the top and stop."""
        gte_ns = lo_ms * 1_000_000
        lt_ns = hi_ms * 1_000_000
        params = {
            "timestamp.gte": gte_ns,
            "timestamp.lt":  lt_ns,
            "order":         "desc",
            "limit":         self.TRADES_MAX_PER_CALL,
            "apiKey":        self.api_key,
        }

        ticks: List[dict] = []
        next_url: str | None = None
        pages = 0
        # Ceiling on pages PER WINDOW. A window is one session (~6.5h RTH), so at
        # 50k trades/page this bounds even a very liquid name's single session
        # while still terminating a pathological loop. The session-walk caller
        # bounds the number of windows separately (max_sessions).
        MAX_PAGES_PER_WINDOW = 200

        while pages < MAX_PAGES_PER_WINDOW:
            success = False
            body: dict = {}
            for attempt in range(3):
                try:
                    async with self.rate_limiter:
                        if next_url:
                            # next_url carries all query state EXCEPT apiKey
                            # (Polygon strips it from the cursor link).
                            resp = await self._client.get(
                                next_url, params={"apiKey": self.api_key},
                            )
                        else:
                            resp = await self._client.get(base_url, params=params)

                    if resp.status_code == 429:
                        retry_after = resp.headers.get("Retry-After")
                        try:
                            backoff = float(retry_after) if retry_after else 2 ** attempt
                        except (ValueError, TypeError):
                            backoff = 2 ** attempt
                        logger.warning(
                            f"429 subminute-seeding {native}; backing off "
                            f"{backoff:.1f}s (attempt {attempt + 1}/3)"
                        )
                        await asyncio.sleep(backoff)
                        continue

                    if resp.status_code in (401, 403):
                        logger.error(
                            f"{resp.status_code} from Polygon {base_url} for "
                            f"{native}. Check the plan includes this tick source. "
                            f"Falling back to cold-start for this rung."
                        )
                        return ticks, False

                    resp.raise_for_status()
                    body = resp.json()
                    success = True
                    break

                except Exception as e:
                    detail = f": {e}" if str(e) else ""
                    logger.warning(
                        f"{self.name} subminute fetch error {native} "
                        f"({type(e).__name__}){detail} (attempt {attempt + 1}/3)"
                    )
                    backoff = (2 ** attempt) * random.uniform(0.5, 1.5)
                    await asyncio.sleep(backoff)

            if not success:
                logger.error(
                    f"subminute page failed for {native} after collecting "
                    f"{len(ticks)} ticks in-window; returning partial."
                )
                return ticks, False

            for rec in body.get("results", []):
                mapped = self._tick_from_record(rec)
                if mapped is None:
                    continue
                t_ms, price = mapped
                ticks.append({"t": t_ms, "o": price, "h": price, "l": price, "c": price})

            next_url = body.get("next_url")
            pages += 1
            if not next_url:
                break
        else:
            logger.warning(
                f"subminute-seed for {native} hit MAX_PAGES_PER_WINDOW="
                f"{MAX_PAGES_PER_WINDOW} in one session window; partial window."
            )

        return ticks, True

    async def fetch_subminute_history(
        self,
        symbol: str,
        timeframe: str,
        target_bars: int,
    ) -> List[OHLCPoint]:
        """Build up to `target_bars` sub-minute OHLC bars from raw ticks, walking
        BACK across trading sessions (newest first) until the buffer is full or a
        session/page cap is hit.

        WHY THIS EXISTS (and why it's separate from fetch_history):
        The /v2/aggs SECOND aggregates are T+1 stale — a second-bar isn't final
        until an end-of-day reconciliation folds in late dark-pool prints, so
        seeding sub-minute buffers from /v2/aggs injects a ~24h-old series AND
        (worse) writes a ~24h-stale feed_cursors high-water mark that rewinds the
        live poller. Raw ticks (trades for equity, quotes for fx) carry only
        reporting latency, so bars built from them end at ~now; the cursor written
        from them is CORRECT and the live poller resumes at the true recent edge.

        SESSION WALK: we take the session containing (or most recently before)
        now, page its ticks newest-first, resample, and if we still need more
        bars, jump to the PRIOR session's close and repeat. This is the Option-B
        "fill across sessions" behaviour — crossing weekend/overnight gaps is fine
        because the bars carry real timestamps and the gaps are simply absent from
        the series (the chain machine's step-mode handles gaps). Bounded by
        MAX_SESSIONS so a thin symbol can't walk back forever.

        Returns ascending OHLCPoints; may be FEWER than target_bars (young listing,
        illiquid name, cap reached) — the rest fills live, the documented
        partial-ladder case. The still-FORMING newest window is dropped (tail).
        """
        if timeframe not in self.SUPPORTED_TIMEFRAMES:
            logger.error(f"Unsupported {self.asset_class} timeframe: {timeframe}")
            return []

        native = self.to_native_symbol(symbol)
        await self._get_client()  # ensure self._client is initialised

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        floor_ms = self._inception_ms.get(symbol)  # ticker-recycling guard
        tf_ms = timeframe_to_ms(timeframe)

        # Cap on how many sessions back we'll walk. WINDOW_SIZE (5000) bars of a
        # sub-minute rung is a fraction of one session (5s: 5000×5s ≈ 7h ≈ one
        # RTH day), so a healthy liquid name fills in 1-2 sessions. The cap is
        # generous enough to fill a fine rung on a thinner name across a normal
        # week, but bounded so an illiquid symbol degrades to a partial buffer
        # (fills live) instead of walking back indefinitely.
        MAX_SESSIONS = 7

        all_ticks: List[dict] = []
        resampled_count = 0
        # Start the walk from the session containing now, or if now is in a gap
        # (overnight/weekend), from the most recent prior session. Probe backward
        # a bounded number of steps to find that session's bounds.
        cursor_ms = now_ms
        sessions_walked = 0

        while sessions_walked < MAX_SESSIONS and resampled_count < target_bars:
            session = session_bounds_for(self.asset_class, cursor_ms)
            if session is None:
                # cursor_ms is in a gap (overnight/weekend/holiday). Step back in
                # SMALL increments to find the nearest prior session. The step
                # must be smaller than the shortest possible session or we could
                # leap over one — an equity RTH session is ~6.5h, so a 1h step
                # never skips a session. The horizon must cover the longest gap
                # (a 3-day weekend plus an adjacent holiday ≈ 4 days), hence the
                # generous iteration bound. fx (weekly bounds) resolves on the
                # first in-session step.
                GAP_PROBE_STEP_MS = 3_600_000        # 1 hour
                GAP_PROBE_MAX_STEPS = 120            # up to ~5 days back
                probe = cursor_ms
                found = None
                for _ in range(GAP_PROBE_MAX_STEPS):
                    probe -= GAP_PROBE_STEP_MS
                    if floor_ms is not None and probe < floor_ms:
                        break
                    found = session_bounds_for(self.asset_class, probe)
                    if found is not None:
                        break
                if found is None:
                    break  # no session within probe horizon — stop the walk
                session = found

            sess_start, sess_end = session
            if floor_ms is not None:
                sess_start = max(sess_start, floor_ms)
            # Window upper bound is min(session end, now): never ask for ticks
            # in the future, and on the current live session cap at now.
            hi = min(sess_end, now_ms)
            lo = sess_start
            if lo >= hi:
                # Degenerate/empty window (e.g. floor above session end). Skip to
                # the prior session.
                cursor_ms = sess_start - 1
                sessions_walked += 1
                continue

            window_ticks, ok = await self._fetch_ticks_window(
                self._subminute_source_url(native), native, lo, hi,
            )
            all_ticks.extend(window_ticks)
            # Re-resample the running set so the loop's exit check counts real
            # OUTPUT bars (same posture as fetch_history) — resampling is lossy
            # across gaps, so raw tick count is not a safe proxy.
            resampled_count = len(_resample_to_timeframe(all_ticks, timeframe))

            sessions_walked += 1
            # Next iteration: the session strictly before this one.
            cursor_ms = sess_start - 1

            if floor_ms is not None and cursor_ms <= floor_ms:
                break  # reached inception — nothing older is legitimately ours
            if not ok:
                break  # a page failed / auth error — stop; return what we have

        # Drop the still-FORMING window. _resample_to_timeframe buckets purely by
        # window_start and — unlike the live poller — has NO closed-window check,
        # so the newest window it emits is the one currently forming at boot time.
        # Seeding that truncated bar would (a) hand the buffer a half-built bar and
        # (b) collide with the live poller emitting the SAME window closed on its
        # first cycle. Drop any window whose close-time is in the future. Using the
        # now_ms captured at fetch START is conservative: wall-clock only advanced
        # since, so this can drop a window that was still forming then but never
        # keeps one still open now. Mirrors the live loop's hold-back.
        points = _trades_to_ohlcpoints(all_ticks, timeframe, symbol, self.asset_class)
        points = [p for p in points if p.timestamp + tf_ms <= now_ms]
        # Walking newest-session-first accumulates across sessions in arbitrary
        # window order; _trades_to_ohlcpoints already sorts ascending by window,
        # so points is ascending and points[-1] is the true newest bar (the fresh
        # cursor the caller needs). Cap to the newest target_bars.
        if len(points) > target_bars:
            points = points[-target_bars:]
        return points

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