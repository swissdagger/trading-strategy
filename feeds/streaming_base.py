"""
feeds/streaming_base.py — the resilient websocket supervisor.

This is the shared machinery a websocket feed sits on (today feeds/polygon_ws,
the equity stream). All the resilience lives here, once.

## What "resilient" means concretely

A poller is accidentally resilient: a dropped cycle just retries next
interval (the existing feeds lean on exactly this). A websocket has no
such safety net — a dropped connection means a silent hole in the bar
stream until something notices. So this supervisor owns four jobs the
poller never had to:

  1. RECONNECT with exponential backoff + jitter. A connection drop is
     expected, not exceptional; we reconnect indefinitely (the feed
     process above us also relaunches start_live, so we're resilient at
     two levels).

  2. BACKFILL the outage. On every reconnect after the first, REST-fetch
     the bars that closed while we were disconnected and emit them
     BEFORE resuming live — otherwise the aggregator's downstream buffers
     get holes. The REST feed we compose (see __init__) is the backfill
     engine: its battle-tested pagination/429 logic, reused, not
     reimplemented.

  3. STALENESS WATCHDOG. A websocket can die silently — no close frame,
     just no more messages. During market hours, prolonged silence means
     a dead socket; we force a reconnect. Outside market hours silence is
     normal, so the watchdog stands down (uses feeds.base.is_market_open).

  4. TRAILING FLUSH. The aggregator emits a window when the NEXT window's
     first source bar arrives. A stream that goes quiet leaves its last
     window un-emitted; a low-frequency timer calls the aggregator's
     flush_elapsed so those close on wall-clock time.

## Composition, not inheritance, for backfill

A ws feed HOLDS a REST feed instance (its `backfill_feed`) purely to call
fetch_history / fetch_range on it. The ws feed is NOT a subclass of the
REST feed. Rationale: the two share nothing behaviourally — one holds a
socket, the other paginates HTTP — and the only thing the ws path wants
from the REST path is "give me the closed bars for this window," which is
a clean call boundary. This keeps the pagination/429/resample logic with
exactly one owner (the REST feed) and lets the ws feed stay focused on
connection lifecycle.

## The live/backfill split (important, subtle)

- LIVE bars arrive per-source-granularity (Polygon: per second) and go
  THROUGH the BarAggregator, which resamples them to the target
  timeframes and emits on window close.
- BACKFILL bars come from the REST feed ALREADY resampled to the target
  timeframes (fetch_history resamples via the same _resample_to_timeframe
  rule). They must NOT go through the aggregator again — that would
  double-resample. Backfill calls the callback directly.

Because both the aggregator and the REST resampler apply the identical
bucketing rule, a bar produced by backfill for window W is byte-identical
to the bar the aggregator would have produced live for W. The handoff is
therefore seamless: whichever path produces W, downstream sees the same W.
"""
from __future__ import annotations

import asyncio
import logging
import random
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional, Dict, List, Protocol, Sequence, runtime_checkable

from feeds.base import AssetFeed, LiveDataCallback, is_market_open, timeframe_to_ms
from feeds.bar_aggregator import BarAggregator
from feeds.close_order import order_key
from producer.bar import OHLCPoint

logger = logging.getLogger(__name__)


@runtime_checkable
class BackfillFeed(Protocol):
    """The slice of AssetFeed the supervisor needs for outage backfill.

    Any REST feed already satisfies fetch_history. fetch_range is
    optional (see fetch_range_or_recent) — a feed that doesn't implement
    it degrades to most-recent-N, which covers short outages.
    """

    async def fetch_history(
        self, symbol: str, timeframe: str, points_needed: int,
    ) -> List[OHLCPoint]: ...


async def fetch_range_or_recent(
    backfill_feed: BackfillFeed,
    symbol: str,
    timeframe: str,
    since_ms: int,
    now_ms: int,
) -> List[OHLCPoint]:
    """Fetch closed bars for [since_ms, now_ms] for one (symbol, timeframe).

    Prefers a time-windowed fetch (fetch_range) when the feed provides one,
    since it asks Polygon for exactly the outage slice. Falls back to
    fetch_history(points_needed) sized to cover the outage when it doesn't —
    the REST feed's most-recent-N walk, filtered to the window. The fallback
    over-fetches slightly (whole chunks) but the aggregator/consumer dedupe
    on timestamp, so extra bars are harmless.

    Kept as a module function, not a method, so it's unit-testable against a
    fake feed with no supervisor or socket in the picture.
    """
    fetch_range = getattr(backfill_feed, "fetch_range", None)
    if fetch_range is not None:
        bars = await fetch_range(symbol, timeframe, since_ms, now_ms)
    else:
        # Size the most-recent-N request to comfortably span the outage.
        tf_ms = timeframe_to_ms(timeframe)
        span_ms = max(now_ms - since_ms, tf_ms)
        # +2 slack windows so boundary bars aren't clipped.
        points_needed = span_ms // tf_ms + 2
        bars = await backfill_feed.fetch_history(symbol, timeframe, int(points_needed))
    # Return only bars whose window closed within the outage, ascending.
    windowed = [b for b in bars if since_ms <= b.timestamp <= now_ms]
    windowed.sort(key=lambda b: (b.timestamp, b.timeframe))
    return windowed


class StreamingUnavailable(RuntimeError):
    """Raised by start_live when the stream could not be established on the
    FIRST connect after FIRST_CONNECT_MAX_ATTEMPTS tries. Distinct from a
    mid-run disconnect (which reconnects forever): on a cold start a repeated
    auth/entitlement failure means this transport is not going to work, and
    the caller (feed_process) can fall back to the REST sibling rather than
    retrying a dead socket for the process lifetime. Carries the last error."""


class StreamingFeedBase(AssetFeed, ABC):
    """Shared websocket supervisor. Subclasses implement the source-specific
    connect/subscribe/parse hooks; this base owns the resilience loop.

    Subclasses MUST set (as AssetFeed requires) `asset_class` and `name`,
    and implement `supported_timeframes`, `to_native_symbol`, and the
    abstract hooks below. They inherit `fetch_history` by delegating to the
    composed backfill feed (see __init__), so the ws feed satisfies the
    full AssetFeed contract without reimplementing history fetching.
    """

    # Reconnect backoff: exponential from BASE to MAX, with full jitter.
    RECONNECT_BASE_S = 1.0
    RECONNECT_MAX_S = 30.0
    # Cold-start budget before start_live raises StreamingUnavailable.
    FIRST_CONNECT_MAX_ATTEMPTS = 3
    # Staleness: if no message for STALENESS_FACTOR × the finest configured
    # timeframe while the market is open, treat the socket as dead.
    STALENESS_FACTOR = 4.0
    # Minimum staleness ceiling so a very fine finest-tf (5s) doesn't cause
    # false trips on brief legitimate quiet periods.
    STALENESS_FLOOR_S = 30.0
    # Trailing-flush cadence — how often we ask the aggregator to close
    # windows that have elapsed on wall-clock time with no later bar.
    FLUSH_INTERVAL_S = 1.0

    def __init__(self, backfill_feed: BackfillFeed, fill_gaps: bool = True):
        # The REST sibling, held purely as the outage-backfill engine.
        # fetch_history is delegated to it (see below), so the ws feed
        # doesn't reimplement history fetching.
        self._backfill_feed = backfill_feed
        self._fill_gaps = fill_gaps
        self._running = False
        # Set on first successful connect; drives "is this a reconnect
        # (backfill needed) or the first connect (warmup already seeded)?"
        self._last_message_ms: int | None = None
        self._connected_once = False
        # Per-(symbol, timeframe) high-water cursors, the backfill dedupe
        # boundary. Initialised empty here (not only in start_live) so
        # methods that touch it — _advance_cursors, _backfill_outage — never
        # depend on start_live having run first. start_live seeds it from
        # initial_cursors on top of this.
        self._backfill_cursors: Dict[str, Dict[str, int]] = {}
        # Serialises the live, flush and backfill publishes (see _emit).
        self._emit_lock = asyncio.Lock()
        # Finest configured timeframe in ms; set in start_live. Kept as an
        # attribute (not a start_live local) so _staleness_window_s can read
        # it, with a floor fallback when unset.
        self._finest_tf_ms: int | None = None

    # ---- AssetFeed contract: delegate history to the backfill sibling ----

    async def fetch_history(
        self, symbol: str, timeframe: str, points_needed: int,
    ) -> List[OHLCPoint]:
        """Delegated to the composed REST feed. The ws feed and its REST
        sibling fetch identical history — there is one implementation."""
        return await self._backfill_feed.fetch_history(symbol, timeframe, points_needed)

    # ---- Source-specific hooks (subclasses implement) ----

    @abstractmethod
    async def _connect(self):
        """Open and authenticate the websocket. Return the live connection
        object (whatever the subclass's ws library yields). Raise on
        failure — the supervisor catches and backs off."""

    @abstractmethod
    async def _subscribe(self, conn, symbols: List[str]) -> None:
        """Subscribe the connection to the per-source-granularity stream
        for every symbol (Polygon: A.<sym> per-second aggregates for the
        cluster; one connection, all symbols)."""

    @abstractmethod
    def _parse_message(self, raw) -> List[tuple]:
        """Parse one raw ws message into a list of source bars, each a
        tuple (symbol, src_start_ms, open, high, low, close). Returns []
        for non-bar messages (status, heartbeat). A single message may
        carry several bars (Polygon batches)."""

    @abstractmethod
    async def _read(self, conn):
        """Async-iterate raw messages off the connection. Yields raw
        messages until the connection drops (then returns/raises, which
        the supervisor treats as a disconnect)."""

    @abstractmethod
    async def _close(self, conn) -> None:
        """Close the connection cleanly on shutdown. Best-effort; must not
        raise on an already-dead socket."""

    # ---- The supervisor loop ----

    async def start_live(
        self,
        symbols: List[str],
        timeframes: List[str],
        initial_cursors: Dict[str, Dict[str, int]],
        callback: LiveDataCallback,
    ) -> None:
        """Run the resilient stream until cancelled. Never returns normally.

        initial_cursors: {symbol: {timeframe: last_seen_ms}} — the high-water
        mark warmup left. On the FIRST connect we do NOT backfill (warmup
        already seeded up to these cursors); on every RECONNECT we backfill
        from the last message we saw to now.
        """
        self._running = True
        self._backfill_cursors: Dict[str, Dict[str, int]] = {
            s: dict(initial_cursors.get(s, {})) for s in symbols
        }
        aggregator = BarAggregator(
            asset_class=self.asset_class,
            timeframes=list(timeframes),
            fill_gaps=self._fill_gaps,
        )
        self._finest_tf_ms = min(timeframe_to_ms(tf) for tf in timeframes)

        attempt = 0
        last_error: Optional[BaseException] = None
        flush_task = asyncio.create_task(
            self._flush_loop(aggregator, callback), name=f"{self.name}-flush",
        )
        try:
            while self._running:
                try:
                    conn = await self._connect()
                    attempt = 0  # reset backoff on a successful connect
                    await self._subscribe(conn, symbols)

                    # Reconnect (not first connect): backfill the outage
                    # before resuming live, so no window is silently lost.
                    if self._connected_once and self._last_message_ms is not None:
                        await self._backfill_outage(
                            symbols, timeframes, self._last_message_ms, callback,
                        )
                    self._connected_once = True
                    logger.info("[%s] connected; streaming %d symbols", self.name, len(symbols))

                    await self._consume(conn, aggregator, callback)
                    # _consume returns only on disconnect.
                    logger.warning("[%s] stream ended; will reconnect", self.name)
                    await self._close_quietly(conn)

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    last_error = e
                    logger.exception("[%s] connection error", self.name)

                if not self._running:
                    break
                # Exponential backoff with full jitter before reconnecting.
                attempt += 1
                if (not self._connected_once
                        and attempt >= self.FIRST_CONNECT_MAX_ATTEMPTS):
                    # Never got a socket up. Stop retrying here and let the
                    # caller decide (feed_process falls back to REST).
                    raise StreamingUnavailable(
                        f"{self.name}: no successful connect after {attempt} "
                        f"attempts (last error: {last_error!r})"
                    ) from last_error
                delay = min(
                    self.RECONNECT_MAX_S,
                    self.RECONNECT_BASE_S * (2 ** (attempt - 1)),
                )
                delay = random.uniform(0, delay)
                logger.info("[%s] reconnecting in %.1fs (attempt %d)", self.name, delay, attempt)
                await asyncio.sleep(delay)
        finally:
            flush_task.cancel()
            try:
                await flush_task
            except asyncio.CancelledError:
                pass

    async def _consume(self, conn, aggregator: BarAggregator, callback: LiveDataCallback) -> None:
        """Read messages, feed source bars through the aggregator, emit
        closed bars. Returns when the connection drops OR the staleness
        check declares the socket dead.

        Staleness is enforced with a per-message READ TIMEOUT rather than a
        separate racing task: each read waits at most `staleness_window`
        seconds. A timeout means either a genuinely dead socket or a
        legitimately quiet market — we distinguish with is_market_open. This
        avoids cancelling a suspended async generator (which corrupts it),
        the bug an earlier race-based version had.
        """
        self._last_message_ms = self._now_ms()
        window_s = self._staleness_window_s()
        read_iter = self._read(conn).__aiter__()
        while True:
            try:
                raw = await asyncio.wait_for(read_iter.__anext__(), timeout=window_s)
            except asyncio.TimeoutError:
                # No message within the window. If the market is open, the
                # socket is silently dead — force a reconnect. If it's
                # closed, silence is expected; keep waiting.
                if is_market_open(self.asset_class, datetime.now(timezone.utc)):
                    logger.warning(
                        "[%s] no data for %.0fs during open market — socket stale, reconnecting",
                        self.name, window_s,
                    )
                    return
                continue
            except StopAsyncIteration:
                return  # clean end of stream → disconnect
            self._last_message_ms = self._now_ms()

            bars = self._parse_message(raw)
            if not bars:
                continue
            emitted: List[OHLCPoint] = []
            for (symbol, src_t, o, h, l, c) in bars:
                emitted.extend(aggregator.add_source_bar(symbol, src_t, o, h, l, c))
            if emitted:
                await self._emit(emitted, callback)

    def _staleness_window_s(self) -> float:
        """Seconds of silence (during an open market) that we treat as a
        dead socket. Scales with the finest configured timeframe but never
        drops below the floor, so a 5s-tf feed doesn't false-trip on a brief
        legitimate quiet spell.

        `_finest_tf_ms` is set in start_live; if _consume is exercised
        without it (e.g. in isolation), fall back to the floor rather than
        raising — the read loop should never crash on a missing cadence hint.
        """
        finest_ms = getattr(self, "_finest_tf_ms", None)
        if not finest_ms:
            return self.STALENESS_FLOOR_S
        return max(
            self.STALENESS_FLOOR_S,
            self.STALENESS_FACTOR * (finest_ms / 1000.0),
        )

    async def _flush_loop(self, aggregator: BarAggregator, callback: LiveDataCallback) -> None:
        """Low-frequency trailing-edge flush: close windows that elapsed on
        wall-clock time with no later source bar to trigger them. Runs for
        the whole start_live lifetime, across reconnects (the aggregator
        persists; only the connection cycles)."""
        while True:
            await asyncio.sleep(self.FLUSH_INTERVAL_S)
            emitted = aggregator.flush_elapsed(self._now_ms())
            if emitted:
                await self._emit(emitted, callback)

    def _can_backfill_tf(self, tf: str) -> bool:
        """Whether the composed REST sibling can actually backfill this timeframe.

        Default True: the Polygon REST feeds backfill every target timeframe
        (their fetch_history resamples from per-second/minute aggregates the REST
        API serves). A feed whose REST sibling CANNOT serve some timeframe — e.g.
        Binance, whose futures REST klines bottom out at 1m so sub-minute rungs
        would (incorrectly) request interval=1s and 400 — overrides this to skip
        those rungs. Skipped rungs simply resume live after reconnect (for
        sub-minute crypto they self-heal from fresh 1s klines); attempting REST
        backfill on them only produces a 400-storm that delays live resumption.
        """
        return True

    async def _backfill_outage(
        self,
        symbols: List[str],
        timeframes: Sequence[str],
        since_ms: int,
        callback: LiveDataCallback,
    ) -> None:
        """REST-fetch and emit the bars that closed during a disconnect,
        for every (symbol, timeframe), before live resumes.

        Backfill bars are already resampled to the target timeframes by the
        REST feed, so they bypass the aggregator and go straight to the
        callback (see module docstring: the live/backfill split). Bars at or
        before a symbol/tf's existing cursor are skipped — the cursor is the
        dedupe boundary, same discipline as the REST feeds' live path.

        Timeframes the REST sibling can't serve (_can_backfill_tf) are skipped
        entirely — they resume live rather than hammering REST with requests
        that will fail.
        """
        now_ms = self._now_ms()
        backfillable = [tf for tf in timeframes if self._can_backfill_tf(tf)]
        skipped = [tf for tf in timeframes if tf not in backfillable]
        if skipped:
            logger.info(
                "[%s] backfill skipping non-REST-servable tfs %s (resume live)",
                self.name, skipped,
            )
        logger.info(
            "[%s] backfilling outage: %.1fs across %d symbols (%d tfs)",
            self.name, (now_ms - since_ms) / 1000.0, len(symbols), len(backfillable),
        )
        for symbol in symbols:
            # All of a symbol's rungs first, then ONE close-time-ordered emit:
            # rung by rung would publish every 30s bar of the outage before
            # any 45s bar.
            got: List[OHLCPoint] = []
            for tf in backfillable:
                try:
                    got.extend(await fetch_range_or_recent(
                        self._backfill_feed, symbol, tf, since_ms, now_ms,
                    ))
                except Exception:
                    logger.exception(
                        "[%s] backfill failed for %s %s; live will resume "
                        "(gap remains until next bar)", self.name, symbol, tf,
                    )
            if got:
                await self._emit(got, callback)

    async def _emit(self, bars: List[OHLCPoint], callback: LiveDataCallback) -> None:
        """The one publish point for the live, flush and backfill paths.
        Drops any bar not newer than what was already emitted for its
        (symbol, timeframe) — after a reconnect the aggregator can still hold
        the window that was open at the disconnect, which the backfill has
        since emitted whole — sorts the rest into close-time order, and holds
        a lock so the three paths' batches never interleave on the wire."""
        async with self._emit_lock:
            fresh = [b for b in bars
                     if b.timestamp > self._backfill_cursors.get(b.symbol, {})
                     .get(b.timeframe, -1)]
            if not fresh:
                return
            fresh.sort(key=order_key)
            self._advance_cursors(fresh)
            await callback(fresh)

    def _advance_cursors(self, bars: List[OHLCPoint]) -> None:
        """Move each (symbol, timeframe) high-water cursor to the latest
        emitted bar. Shared by the live, flush, and backfill paths so the
        backfill dedupe boundary reflects everything already emitted,
        whatever produced it."""
        for b in bars:
            sym = self._backfill_cursors.setdefault(b.symbol, {})
            prev = sym.get(b.timeframe)
            if prev is None or b.timestamp > prev:
                sym[b.timeframe] = b.timestamp

    async def _close_quietly(self, conn) -> None:
        try:
            await self._close(conn)
        except Exception:
            logger.debug("[%s] error closing connection (ignored)", self.name, exc_info=True)

    @staticmethod
    def _now_ms() -> int:
        return int(datetime.now(timezone.utc).timestamp() * 1000)