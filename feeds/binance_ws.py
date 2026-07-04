"""
feeds/binance_ws.py — Binance USD-M futures websocket feed.

The crypto streaming feed, sitting on the same StreamingFeedBase supervisor
as the Polygon ws feeds. It closes the sub-minute crypto gap that config.py
flags: Binance's REST klines (and this project's BinanceFuturesFeed) bottom
out at 1m, but Binance's WEBSOCKET offers a native 1s kline, so sub-minute
ladder rungs (5s/10s/30s) can be built by aggregating closed 1s klines.

## Two source paths, chosen per configured timeframe

Binance's native kline intervals are 1s, 1m, 3m, 5m, 15m, 30m, 1h, ... A
configured target timeframe is served one of two ways:

  NATIVE  (1m and coarser, matching Binance's grid): subscribe directly to
          <sym>@kline_<tf>, take only CLOSED klines (payload k.x == true),
          and emit them as-is. No aggregation — the kline already IS the
          target bar. Routing it through BarAggregator would be a wasteful
          no-op and risks re-bucketing an already-final bar.

  DERIVED: any target Binance doesn't serve natively, built by aggregating
          the closed klines of a finer NATIVE base:
            - sub-minute (5s/10s/30s, and a passthrough 1s) derive from 1s;
            - coarse rungs Binance lacks (2m, 10m) derive from the largest
              native base that divides them evenly (2m←1m, 10m←5m).
          Each base has one @kline_<base> subscription feeding one aggregator
          configured with only the rungs that source from that base, so a 1m
          kline is never mis-bucketed into a 5s window. This is the same
          OHLC bucketing Polygon's per-second bars get — see BarAggregator.

So a symbol configured for {5s, 10s, 1m, 2m, 5m, 10m} opens: @kline_1s
(feeding 5s + 10s), @kline_1m and @kline_5m (emitted directly as native 1m/5m
AND feeding the 2m and 10m aggregators respectively). All on ONE websocket
connection via combined streams. A base kline that is ALSO a native target
(1m, 5m here) is subscribed once and dispatched to both paths in _handle_frame.

## Backfill asymmetry (an honest constraint, not a bug)

StreamingFeedBase backfills a reconnect outage via the composed REST feed
(BinanceFuturesFeed). That feed can backfill 1m-and-coarser rungs — its
fetch_history covers them. It CANNOT backfill sub-minute rungs: Binance's
REST klines don't go below 1m and the REST feed doesn't request 1s. So:

  - NATIVE rungs (1m+): backfilled normally on reconnect.
  - COARSE DERIVED rungs (2m/10m): ALSO backfilled — the REST feed derives
    them from its own 1m/5m klines (see binance_futures._get_base_tf), so a
    reconnect gap in a 2m/10m rung fills exactly like a native rung.
  - DERIVED sub-minute rungs (5s/10s/30s): NOT backfilled — the outage
    leaves a small gap that self-heals as fresh 1s klines resume feeding
    the aggregator. For a strategy on 5s bars this is a few missing bars
    across a reconnect, which the ladder's synthetic-fill and the
    forecast-buffer warmup tolerate (the same tolerance the RUNBOOK notes
    for streams that start before their buffer is full).

The base's _backfill_outage already handles a REST feed returning nothing
for a (symbol, tf) gracefully (it just emits nothing for that rung), so the
sub-minute rungs simply come back empty from backfill and resume live. No
special-casing needed here beyond documenting it.

## Combined-stream wire format

Connecting to /stream?streams=a@kline_1s/b@kline_1m wraps each event as
{"stream":"<name>","data":<rawKlinePayload>}. The kline payload's k object
carries t (start ms), o/h/l/c (strings), i (interval), and x (is-closed).
We key on x: only closed klines are emitted, so a target bar is emitted
exactly once, on close — the same closed-window-only discipline as every
other feed.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Set

import websockets

from feeds.bar_aggregator import BarAggregator
from feeds.base import is_market_open
from feeds.binance_futures import (
    _COARSE_DERIVED_INTERVALS,
    _get_base_tf,
)
from feeds.streaming_base import StreamingFeedBase
from producer.bar import OHLCPoint

logger = logging.getLogger(__name__)


# Binance-native kline intervals we allow as NATIVE (direct) subscriptions.
# Mirrors BinanceFuturesFeed._BINANCE_INTERVALS (1m and coarser). Sub-minute
# rungs are handled via the DERIVED 1s path instead, so they're deliberately
# NOT here.
_NATIVE_INTERVALS: Set[str] = {
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M",
}

# Sub-minute rungs we can DERIVE by aggregating the native 1s kline. 1s
# itself is a passthrough (the 1s kline emitted directly). Extend if the
# ladder ever uses other sub-minute rungs.
_SUBMINUTE_DERIVED_INTERVALS: Set[str] = {"1s", "5s", "10s", "15s", "30s"}

# All derived rungs = sub-minute (1s-sourced) ∪ coarse (native-base-sourced,
# e.g. 2m from 1m, 10m from 5m). _COARSE_DERIVED_INTERVALS is imported from the
# REST sibling so the two feeds cannot disagree on which coarse rungs exist —
# the same single-source-of-truth discipline the module docstrings call out.
_DERIVED_INTERVALS: Set[str] = _SUBMINUTE_DERIVED_INTERVALS | _COARSE_DERIVED_INTERVALS

# The full set this feed can serve = native ∪ derived.
_SUPPORTED: Set[str] = _NATIVE_INTERVALS | _DERIVED_INTERVALS

_FSTREAM_BASE = "wss://fstream.binance.com/stream"


class BinanceFuturesWSFeed(StreamingFeedBase):
    """Binance USD-M futures per-kline websocket feed.

    Serves native intervals directly and sub-minute rungs by aggregating the
    native 1s kline. One connection, combined streams, all symbols.
    """

    asset_class = "crypto"
    name = "binance_futures_ws"

    OPEN_TIMEOUT_S = 15.0

    def __init__(self, backfill_feed, fill_gaps: bool = True):
        super().__init__(backfill_feed=backfill_feed, fill_gaps=fill_gaps)
        # Split configured timeframes into native vs derived at start_live.
        self._native_tfs: List[str] = []
        self._derived_tfs: List[str] = []
        # Derived rungs grouped by the native base kline they aggregate from:
        #   {"1s": ["5s","30s"], "1m": ["2m"], "5m": ["10m"]}
        # One aggregator per base, each configured with ONLY the rungs that
        # source from that base — so a 1m base kline never gets bucketed into
        # a 5s window. Native klines touch none of these. Built in start_live.
        self._derived_by_base: Dict[str, List[str]] = {}
        self._base_aggs: Dict[str, BarAggregator] = {}

    def supported_timeframes(self) -> Set[str]:
        return _SUPPORTED

    def to_native_symbol(self, symbol: str) -> str:
        return symbol  # Binance uses the bare ticker (BTCUSDT)

    # ---- timeframe partitioning ----

    def _partition_timeframes(self, timeframes: List[str]) -> None:
        """Split configured timeframes into native (direct-subscribe) and
        derived (1s-aggregated). Raises on an unsupported timeframe so a
        misconfig dies loudly at startup, matching the REST feed."""
        unsupported = [tf for tf in timeframes if tf not in _SUPPORTED]
        if unsupported:
            raise ValueError(
                f"{self.name}: unsupported timeframes {unsupported}; "
                f"supported: {sorted(_SUPPORTED)}"
            )
        self._native_tfs = [tf for tf in timeframes if tf in _NATIVE_INTERVALS]
        self._derived_tfs = [tf for tf in timeframes if tf in _DERIVED_INTERVALS]

        # Group each derived rung under the native base it aggregates from.
        # _get_base_tf resolves every derived rung to its base: sub-minute → 1s,
        # coarse → the largest native divisor (2m→1m, 10m→5m). Native rungs
        # aren't in _derived_tfs, so they never reach here. This is the
        # wire-level plan: one @kline_<base> subscription per base, each
        # feeding one aggregator (built in start_live).
        self._derived_by_base = {}
        for tf in self._derived_tfs:
            base = _get_base_tf(tf)
            self._derived_by_base.setdefault(base, []).append(tf)

    def _stream_names(self, symbols: List[str]) -> List[str]:
        """Every combined-stream name this feed needs: one @kline_<tf> per
        (symbol, native tf), plus one @kline_<base> per (symbol, distinct
        derived base) — 1s for sub-minute rungs, 1m/5m for coarse rungs.
        A base that's also a native target is subscribed once (dedup)."""
        names: List[str] = []
        for sym in symbols:
            lower = sym.lower()
            for tf in self._native_tfs:
                names.append(f"{lower}@kline_{tf}")
            # One base-kline subscription per distinct derived base (1s for
            # sub-minute, 1m/5m for coarse rungs). A base already subscribed
            # as a NATIVE target isn't re-subscribed — the same closed kline
            # frame serves both the native emit and the aggregator (dispatched
            # in _handle_frame). Dedup guards that overlap.
            for base in self._derived_by_base:
                stream = f"{lower}@kline_{base}"
                if stream not in names:
                    names.append(stream)
        return names

    # ---- StreamingFeedBase hooks ----

    async def _connect(self):
        """Open the combined-stream connection. Binance public market
        streams need no auth. The websockets lib auto-replies to Binance's
        ping frames, satisfying the keepalive requirement."""
        # Streams are set on the URL at connect time; _subscribe is where we
        # actually build the list, so connect first with a bare /stream and
        # subscribe via a control frame (Binance supports both URL streams
        # and SUBSCRIBE control frames; we use the control frame so connect
        # and subscribe stay cleanly separated like the Polygon feed).
        conn = await websockets.connect(
            f"{_FSTREAM_BASE}?streams=",
            open_timeout=self.OPEN_TIMEOUT_S,
            ping_interval=None,   # Binance drives ping; lib auto-pongs
            max_size=None,
        )
        return conn

    async def _subscribe(self, conn, symbols: List[str]) -> None:
        """Subscribe to all needed streams via a SUBSCRIBE control frame."""
        names = self._stream_names(symbols)
        await conn.send(json.dumps({
            "method": "SUBSCRIBE",
            "params": names,
            "id": 1,
        }))
        logger.info(
            "[%s] subscribed %d streams (%d native tfs, %d derived tfs, %d symbols)",
            self.name, len(names), len(self._native_tfs),
            len(self._derived_tfs), len(symbols),
        )

    def _parse_message(self, raw) -> List[tuple]:
        """Return DERIVED-path source bars (closed base klines) for one frame,
        as (base, sym, start_ms, o, h, l, c) tuples.

        This satisfies StreamingFeedBase's abstract contract and is kept
        consistent with _handle_frame, but note this feed overrides _consume
        (Binance mixes native klines + derived-base klines on one connection),
        so the live path uses _handle_frame directly. This method is the
        source-bar projection of _handle_frame, useful for testing and for any
        caller that wants just the aggregator-bound source bars.
        """
        _native, source_bars = self._handle_frame(raw)
        return source_bars

    async def _consume(self, conn, aggregator: BarAggregator, callback) -> None:
        """Custom consume: routes native closed klines straight to the
        callback and 1s klines into the DERIVED aggregator.

        We override rather than reuse the base loop because Binance mixes two
        kinds of bar on one connection — already-final native klines (emit
        directly) and 1s klines (aggregate) — whereas the base loop assumes
        every source bar should be aggregated. The base's staleness handling
        is reproduced via the same per-message read timeout.
        """
        self._last_message_ms = self._now_ms()
        window_s = self._staleness_window_s()
        read_iter = self._read(conn).__aiter__()
        while True:
            try:
                raw = await asyncio.wait_for(read_iter.__anext__(), timeout=window_s)
            except asyncio.TimeoutError:
                if is_market_open(self.asset_class, datetime.now(timezone.utc)):
                    logger.warning(
                        "[%s] no data for %.0fs (market open) — reconnecting",
                        self.name, window_s,
                    )
                    return
                continue
            except StopAsyncIteration:
                return
            self._last_message_ms = self._now_ms()

            native_bars, source_bars = self._handle_frame(raw)

            emitted: List[OHLCPoint] = []
            # Native closed klines: emit directly (already final target bars).
            emitted.extend(native_bars)
            # Base source klines: feed each into the aggregator for ITS base,
            # which buckets it only into the derived rungs that source from
            # that base (1s→sub-minute, 1m→2m, 5m→10m).
            for (base, symbol, src_t, o, h, l, c) in source_bars:
                agg = self._base_aggs.get(base)
                if agg is not None:
                    emitted.extend(agg.add_source_bar(symbol, src_t, o, h, l, c))

            if emitted:
                emitted.sort(key=lambda p: (p.timestamp, p.timeframe))
                self._advance_cursors(emitted)
                await callback(emitted)

    def _handle_frame(self, raw):
        """Decode one combined-stream frame into (native_bars, source_bars).

        native_bars: list[OHLCPoint] — closed klines at a NATIVE target tf,
                     ready to emit directly.
        source_bars: list[tuple] — closed base klines as (base, sym, start_ms,
                     o,h,l,c) for the per-base aggregator to bucket into derived
                     rungs. `base` names which aggregator to feed.

        A single closed kline can populate BOTH lists: when 1m is a native
        target AND the base for 2m, the 1m frame emits directly as a 1m bar and
        also feeds the 2m aggregator. So native emit and source routing are
        INDEPENDENT checks below, not an if/elif.

        Non-kline frames (subscription acks {"result":null,"id":1}, or
        not-yet-closed klines with x==false) yield nothing.
        """
        native_bars: List[OHLCPoint] = []
        source_bars: List[tuple] = []

        payload = self._unwrap(raw)
        if payload is None:
            return native_bars, source_bars

        k = payload.get("k")
        if not isinstance(k, dict):
            return native_bars, source_bars  # not a kline event
        if not k.get("x"):
            return native_bars, source_bars  # kline not closed yet — skip

        try:
            symbol = payload["s"] if "s" in payload else k["s"]
            interval = k["i"]
            start_ms = int(k["t"])
            o = float(k["o"]); h = float(k["h"]); l = float(k["l"]); c = float(k["c"])
        except (KeyError, TypeError, ValueError):
            logger.warning("[%s] skipping malformed kline: %r", self.name, k)
            return native_bars, source_bars

        # Native emit: this interval is a configured native target.
        if interval in _NATIVE_INTERVALS and interval in self._native_tfs:
            native_bars.append(OHLCPoint(
                symbol=symbol,
                timestamp=start_ms,
                open=o, high=h, low=l, close=c,
                timeframe=interval,
                asset_class=self.asset_class,
            ))
        # Base-source routing: this interval is the base for one or more
        # derived rungs. Independent of the native check above — the same
        # 1m kline can do both. `self._derived_by_base` was built at
        # partition time; membership means "some derived rung sources here".
        if interval in self._derived_by_base:
            source_bars.append((interval, symbol, start_ms, o, h, l, c))
        # A kline for an interval that's neither a native target nor a
        # derived base is one we didn't configure — ignore.
        return native_bars, source_bars

    async def _read(self, conn):
        async for raw in conn:
            yield raw

    async def _close(self, conn) -> None:
        try:
            await conn.close()
        except Exception:
            pass

    # ---- start_live override to partition timeframes first ----

    async def start_live(self, symbols, timeframes, initial_cursors, callback) -> None:
        """Partition timeframes into native/derived before running the base
        supervisor loop. The base builds an aggregator over ALL timeframes;
        we let it, but only DERIVED source bars are ever fed into it (native
        klines bypass it in _consume), so the native entries in that
        aggregator simply stay empty and inert."""
        self._partition_timeframes(list(timeframes))
        # One aggregator per derived base, each configured with ONLY the rungs
        # that source from it. Feeding a 1m bar to an aggregator that also held
        # 5s would bucket it into a 5s window — hence the per-base split rather
        # than one aggregator over all derived rungs. Native rungs never touch
        # these (they emit directly in _handle_frame).
        self._base_aggs = {
            base: BarAggregator(
                asset_class=self.asset_class,
                timeframes=rungs,
                fill_gaps=self._fill_gaps,
            )
            for base, rungs in self._derived_by_base.items()
        }
        logger.info(
            "[%s] native tfs=%s derived tfs=%s (bases: %s)",
            self.name, self._native_tfs, self._derived_tfs,
            {b: r for b, r in self._derived_by_base.items()},
        )
        await super().start_live(symbols, timeframes, initial_cursors, callback)

    async def _flush_loop(self, aggregator, callback) -> None:
        """Trailing-edge flush across ALL per-base aggregators.

        Overrides the base loop, which flushes the single aggregator it built.
        This feed keeps its derived rungs in per-base aggregators (built in
        start_live), so the base's aggregator is inert and we flush ours
        instead. Without this, a quiet coarse window (e.g. a 10m bar with no
        later source kline to trigger it) would never close on the trailing
        edge. The passed `aggregator` is the base's inert one — ignored.
        """
        while True:
            await asyncio.sleep(self.FLUSH_INTERVAL_S)
            now_ms = self._now_ms()
            emitted: List[OHLCPoint] = []
            for agg in self._base_aggs.values():
                emitted.extend(agg.flush_elapsed(now_ms))
            if emitted:
                emitted.sort(key=lambda p: (p.timestamp, p.timeframe))
                self._advance_cursors(emitted)
                await callback(emitted)

    @staticmethod
    def _unwrap(raw):
        """Unwrap a combined-stream frame to its inner kline payload, or
        return None for control frames / junk. Combined frames look like
        {"stream": "...", "data": {...}}; a raw (non-combined) frame is the
        payload itself."""
        try:
            msg = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(msg, dict):
            return None
        if "data" in msg and isinstance(msg["data"], dict):
            return msg["data"]
        # Subscription ack {"result": null, "id": 1} or a bare payload.
        if "k" in msg:
            return msg
        return None