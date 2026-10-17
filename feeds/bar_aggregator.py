"""
feeds/bar_aggregator.py — incremental (streaming) OHLC re-aggregation.

The live-path counterpart to feeds.resample._resample_to_timeframe.
That helper is a BATCH re-bucketer: it takes a full list of fine-grained
bars and returns every window at once. On a websocket we don't have the
full list — bars arrive one at a time, forever — so we need the SAME
bucketing arithmetic expressed as a state machine that holds the single
currently-open window per (symbol, timeframe) and emits it the moment it
can prove the window has closed.

The bucketing rule is identical to _resample_to_timeframe's, by design —
this module is the streaming expression of that one rule, not a second
rule that could drift:

    window_start = floor(t / tf_ms) * tf_ms
    open  = first source bar's open   (lowest source-t in the window)
    high  = max of source highs
    low   = min of source lows
    close = last source bar's close   (highest source-t in the window)

## The close-detection rule (the one genuinely new thing)

A batch resampler knows a window is done because the input list ended.
A stream never ends, so we need a trigger. The rule:

    A window W is closed when we first observe a source bar whose
    timestamp belongs to a LATER window (source_window_start > W_start).

That arriving later bar is the PROOF that no more trades can land in W:
Polygon/Binance emit source bars in non-decreasing time order per symbol,
so once a bar for a later window shows up, W can receive nothing further.
This is the event-driven form of the "closed-window-only emission" the
REST feeds already do by only returning fully-elapsed windows.

Two source bars in the SAME window just fold into the open window (update
high/low/close). A source bar in a later window flushes the open window
(emit) and starts a fresh one.

## No-trade windows

Polygon emits no source bar for a second with no qualifying trades; the
same is true minute-wise for crypto. So between two real source bars there
may be empty windows. Whether those empty windows should be emitted as
synthetic flat bars is asset-class dependent, and that decision already
has ONE home: feeds.base.count_synthetic_for_gap (the arithmetic Service 2
and every REST feed share). We reuse it here so the live path fills gaps
exactly as the historical path does — see `_emit_gap_fill`. A caller that
wants raw gaps (no synthetic fill) passes fill_gaps=False.

## Why hold only ONE open window per stream

We emit a window as soon as the next window's first bar arrives, so at any
instant only the most-recent window is unclosed. There is never a need to
keep a map of many open windows — that would be unbounded memory for no
benefit. One open window per (symbol, timeframe) is O(streams) memory,
independent of how long the process runs.

## Trailing-edge flush

If a stream goes quiet (no later bar arrives to trigger the flush), the
open window sits un-emitted. That's correct while the window might still
receive trades, but once wall-clock time has moved past the window's end
the window is genuinely closed and should be emitted even though no later
source bar has arrived. The supervisor calls `flush_elapsed(now_ms)`
on a low-frequency timer to force those out. This is the ONLY place
wall-clock time enters the aggregation; the primary path is purely
event-ordered off source-bar timestamps, which is what keeps it
deterministic and replay-safe.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

from feeds.base import count_synthetic_for_gap, timeframe_to_ms
from feeds.close_order import order_key
from producer.bar import OHLCPoint


@dataclass
class _OpenWindow:
    """The single in-progress window for one (symbol, timeframe) stream."""
    window_start: int   # floor(source_t / tf_ms) * tf_ms
    open: float
    high: float
    low: float
    close: float
    first_src_t: int    # lowest source timestamp folded in (fixes `open`)
    last_src_t: int     # highest source timestamp folded in (fixes `close`)


class _StreamAggregator:
    """Aggregates one (symbol, timeframe) stream of fine-grained source
    bars into closed target-timeframe OHLCPoints.

    Not used directly by callers — BarAggregator owns a grid of these,
    one per (symbol, timeframe). Kept as its own class so the per-stream
    state machine is testable in isolation and has a single responsibility:
    turn an ordered stream of source bars into closed windows.
    """

    def __init__(
        self,
        symbol: str,
        timeframe: str,
        asset_class: str,
        fill_gaps: bool = True,
    ):
        self.symbol = symbol
        self.timeframe = timeframe
        self.asset_class = asset_class
        self.tf_ms = timeframe_to_ms(timeframe)
        self.fill_gaps = fill_gaps
        self._open: _OpenWindow | None = None

    def _window_start_of(self, src_t: int) -> int:
        return (src_t // self.tf_ms) * self.tf_ms

    def add(self, src_t: int, o: float, h: float, l: float, c: float) -> List[OHLCPoint]:
        """Fold one source bar into the aggregation.

        Returns the list of newly-CLOSED OHLCPoints this bar triggered
        (usually empty or one; more than one only when synthetic gap-fill
        emits several flat no-trade windows between two real bars).

        Source bars are expected in non-decreasing `src_t` order per
        stream, which is what the venues guarantee per symbol. An
        out-of-order bar (src_t belonging to an already-closed window)
        is dropped rather than corrupting a window we've already emitted —
        see the guard below. This mirrors the REST feeds' cursor discipline
        (never re-emit a window already past the cursor).
        """
        ws = self._window_start_of(src_t)
        cur = self._open

        if cur is None:
            # First bar for this stream — open the window, emit nothing yet.
            self._open = _OpenWindow(ws, o, h, l, c, src_t, src_t)
            return []

        if ws == cur.window_start:
            # Same window: fold in. Update high/low always; open/close
            # only when this bar extends the window's time span, matching
            # _resample_to_timeframe's first-t/last-t rule exactly.
            cur.high = max(cur.high, h)
            cur.low = min(cur.low, l)
            if src_t < cur.first_src_t:
                cur.open = o
                cur.first_src_t = src_t
            if src_t > cur.last_src_t:
                cur.close = c
                cur.last_src_t = src_t
            return []

        if ws < cur.window_start:
            # Out-of-order: a bar for an EARLIER window than the one we
            # already have open. The earlier window is either open (can't
            # be — we'd have it as cur) or already emitted. Drop it: acting
            # on it would either corrupt cur or re-emit a closed window.
            return []

        # ws > cur.window_start: the open window is now provably closed
        # (a later window's bar has arrived). Emit it, fill any no-trade
        # gap between it and the new window, then open the new window.
        emitted: List[OHLCPoint] = [self._point_from(cur)]
        if self.fill_gaps:
            emitted.extend(self._gap_fill(cur, ws))
        self._open = _OpenWindow(ws, o, h, l, c, src_t, src_t)
        return emitted

    def flush_elapsed(self, now_ms: int) -> List[OHLCPoint]:
        """Force-emit the open window if wall-clock time has moved past
        its end and no later source bar has arrived to trigger the normal
        flush. Idempotent: once flushed, the window is cleared so a
        repeat call at the same now_ms emits nothing.

        Returns the emitted point (as a one-element list) or [].
        """
        cur = self._open
        if cur is None:
            return []
        window_end = cur.window_start + self.tf_ms
        if now_ms < window_end:
            return []  # window may still receive trades — not closed yet
        self._open = None
        return [self._point_from(cur)]

    def _point_from(self, w: _OpenWindow) -> OHLCPoint:
        return OHLCPoint(
            symbol=self.symbol,
            timestamp=w.window_start,
            open=w.open,
            high=w.high,
            low=w.low,
            close=w.close,
            timeframe=self.timeframe,
            asset_class=self.asset_class,
        )

    def _gap_fill(self, closed: _OpenWindow, next_ws: int) -> List[OHLCPoint]:
        """Emit synthetic flat bars for no-trade windows strictly between
        the just-closed window and the newly-opened one.

        Count comes from the ONE shared definition (count_synthetic_for_gap),
        so the live path fabricates exactly the windows the historical path
        would — no drift between them. Each synthetic bar is flat at the
        closed window's close (a no-trade window has no price movement),
        which is the standard forward-fill for a gap where the market was
        open but no trade printed.

        For asset classes where count_synthetic_for_gap returns 0 (fx, and
        any gap that crosses a session boundary), no synthetic bars are
        emitted — the gap passes through, exactly as the batch path leaves
        it. That's the correct behaviour: fabricating bars across a genuine
        session gap would corrupt the manifold.
        """
        n = count_synthetic_for_gap(
            self.asset_class, self.timeframe, closed.window_start, next_ws,
        )
        if n <= 0:
            return []
        flat = closed.close
        out: List[OHLCPoint] = []
        for i in range(1, n + 1):
            ts = closed.window_start + i * self.tf_ms
            out.append(OHLCPoint(
                symbol=self.symbol,
                timestamp=ts,
                open=flat, high=flat, low=flat, close=flat,
                timeframe=self.timeframe,
                asset_class=self.asset_class,
            ))
        return out


class BarAggregator:
    """Fans a symbol's fine-grained source bars out to one target window
    per configured timeframe, emitting closed OHLCPoints.

    One BarAggregator instance serves an entire feed (all symbols, all
    timeframes for that asset class). It owns a grid of _StreamAggregators
    keyed by (symbol, timeframe), created lazily on first sight of a symbol
    so the feed doesn't need the symbol list up front (though it usually
    has it).

    Usage from a websocket feed's message handler:

        agg = BarAggregator(asset_class="equity",
                            timeframes=["5s","10s","30s","1m","2m","5m","10m"])
        ...
        # per inbound per-second Polygon aggregate:
        closed = agg.add_source_bar(symbol, sec_start_ms, o, h, l, c)
        if closed:
            await callback(closed)   # same LiveDataCallback as REST feeds

    and on a low-frequency timer:

        closed = agg.flush_elapsed(now_ms)
        if closed:
            await callback(closed)
    """

    def __init__(
        self,
        asset_class: str,
        timeframes: List[str],
        fill_gaps: bool = True,
    ):
        if not timeframes:
            raise ValueError("BarAggregator needs at least one target timeframe")
        self.asset_class = asset_class
        self.timeframes = list(timeframes)
        self.fill_gaps = fill_gaps
        self._streams: Dict[Tuple[str, str], _StreamAggregator] = {}

    def _stream_for(self, symbol: str, timeframe: str) -> _StreamAggregator:
        key = (symbol, timeframe)
        s = self._streams.get(key)
        if s is None:
            s = _StreamAggregator(
                symbol, timeframe, self.asset_class, fill_gaps=self.fill_gaps,
            )
            self._streams[key] = s
        return s

    def add_source_bar(
        self, symbol: str, src_t: int, o: float, h: float, l: float, c: float,
    ) -> List[OHLCPoint]:
        """Feed one fine-grained source bar (e.g. a Polygon per-second
        aggregate) for `symbol` into every configured target timeframe.
        Returns all OHLCPoints closed by this bar across all timeframes,
        sorted by (timestamp, timeframe) so downstream ordering is stable.
        """
        emitted: List[OHLCPoint] = []
        for tf in self.timeframes:
            emitted.extend(
                self._stream_for(symbol, tf).add(src_t, o, h, l, c)
            )
        # Close-time order, the shorter rung first on a tie: the chain
        # machines apply bars in arrival order. (This sorted by the window
        # START and the timeframe STRING, so a 5m and a 1m window closing
        # together came out 5m first — and '15m' < '1m' as strings.)
        emitted.sort(key=order_key)
        return emitted

    def flush_elapsed(self, now_ms: int) -> List[OHLCPoint]:
        """Force-emit any windows across all streams whose end is now in
        the past. Called on a low-frequency wall-clock timer so quiet
        streams still close their windows. Sorted like add_source_bar.
        """
        emitted: List[OHLCPoint] = []
        for stream in self._streams.values():
            emitted.extend(stream.flush_elapsed(now_ms))
        emitted.sort(key=order_key)
        return emitted