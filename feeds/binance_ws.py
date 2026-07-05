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

  DERIVED (sub-minute: 5s/10s/30s, and a passthrough 1s): subscribe to
          <sym>@kline_1s, take closed 1s klines, and feed them as source
          bars into BarAggregator, which buckets them into the target rung
          exactly as Polygon's per-second bars are bucketed. One 1s
          subscription feeds ALL derived rungs for a symbol.

So a symbol configured for {5s, 10s, 1m, 5m} opens: one @kline_1s stream
(feeding 5s + 10s via the aggregator) plus @kline_1m and @kline_5m streams
(emitted directly). All on ONE websocket connection via combined streams.

## Backfill asymmetry (an honest constraint, not a bug)

StreamingFeedBase backfills a reconnect outage via the composed REST feed
(BinanceFuturesFeed). That feed can backfill 1m-and-coarser rungs — its
fetch_history covers them. It CANNOT backfill sub-minute rungs: Binance's
REST klines don't go below 1m and the REST feed doesn't request 1s. So:

  - NATIVE rungs (1m+): backfilled normally on reconnect.
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
_DERIVED_INTERVALS: Set[str] = {"1s", "5s", "10s", "15s", "30s"}

# The full set this feed can serve = native ∪ derived.
_SUPPORTED: Set[str] = _NATIVE_INTERVALS | _DERIVED_INTERVALS

# The source interval for all DERIVED rungs.
_DERIVE_SOURCE = "1s"

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
        # A dedicated aggregator for the DERIVED rungs only. Native klines
        # never touch it. Built in start_live once we know the timeframes.
        self._derive_agg: BarAggregator | None = None

    def supported_timeframes(self) -> Set[str]:
        return _SUPPORTED

    def _can_backfill_tf(self, tf: str) -> bool:
        """Binance futures REST klines bottom out at 1m. A DERIVED sub-minute
        rung (5s/10s/30s/1s) would make the REST sibling request interval=1s,
        which Binance futures REST rejects with 400. So only NATIVE (1m+) rungs
        are REST-backfillable; derived rungs are skipped and self-heal live from
        fresh 1s klines once the ws reconnects (see the backfill-asymmetry note
        in this module's docstring). This is the enforcement of that documented
        constraint the base loop needs."""
        return tf in _NATIVE_INTERVALS

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

    def _stream_names(self, symbols: List[str]) -> List[str]:
        """Every combined-stream name this feed needs: one @kline_<tf> per
        (symbol, native tf), plus one @kline_1s per symbol if any derived
        rung is configured (a single 1s stream feeds all derived rungs)."""
        names: List[str] = []
        for sym in symbols:
            lower = sym.lower()
            for tf in self._native_tfs:
                names.append(f"{lower}@kline_{tf}")
            if self._derived_tfs:
                names.append(f"{lower}@kline_{_DERIVE_SOURCE}")
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

    _SUBSCRIBE_ID = 1
    _SUBSCRIBE_ACK_TIMEOUT_S = 10.0

    async def _subscribe(self, conn, symbols: List[str]) -> None:
        """Subscribe to all needed streams via a SUBSCRIBE control frame, then
        CONFIRM Binance's ack before returning.

        Binance replies to a SUBSCRIBE with {"result": null, "id": <id>}. Without
        reading it, a silently-rejected subscription is indistinguishable from a
        working one until the staleness watchdog fires ~30s later — the failure
        mode that produced a permanent reconnect loop when the ws was unreachable.
        Reading the ack (with a bounded timeout) surfaces a bad subscription
        immediately as a ConnectionError, which the supervisor treats as a
        reconnect trigger — same posture as the Polygon feed confirming
        auth_success. A non-error ack frame that is actually market data (Binance
        can interleave) is tolerated: we scan a few frames for the matching id.
        """
        names = self._stream_names(symbols)
        await conn.send(json.dumps({
            "method": "SUBSCRIBE",
            "params": names,
            "id": self._SUBSCRIBE_ID,
        }))

        # Read frames until we see the ack for our id, or time out. Binance may
        # interleave a market frame before the ack; scan a bounded number.
        acked = False
        for _ in range(10):
            try:
                raw = await asyncio.wait_for(
                    conn.recv(), timeout=self._SUBSCRIBE_ACK_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                raise ConnectionError(
                    f"[{self.name}] no SUBSCRIBE ack within "
                    f"{self._SUBSCRIBE_ACK_TIMEOUT_S:.0f}s — subscription likely "
                    f"rejected or stream unreachable (streams={len(names)})"
                )
            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(msg, dict) and msg.get("id") == self._SUBSCRIBE_ID:
                # {"result": null, "id": 1} on success; an "error" object on
                # failure. Binance uses result=null for a clean subscribe.
                if msg.get("error") is not None:
                    raise ConnectionError(
                        f"[{self.name}] SUBSCRIBE rejected: {msg['error']}"
                    )
                acked = True
                break
            # Not the ack. In Binance's protocol the ack is the FIRST frame on a
            # fresh connection (it's the direct response to the control frame), so
            # this branch is not expected to hit. If a data frame does arrive
            # first, we drop it rather than plumb it into the aggregator from here
            # (that would spread consume-path wiring into subscribe for an edge
            # that essentially never occurs); log it so the drop is never silent.
            # One dropped source frame is within the reconnect tolerance the feed
            # already accepts.
            if isinstance(msg, dict) and "data" in msg:
                logger.warning(
                    "[%s] data frame arrived before SUBSCRIBE ack; dropping it "
                    "(within reconnect tolerance)", self.name,
                )
        if not acked:
            raise ConnectionError(
                f"[{self.name}] SUBSCRIBE ack not seen in first frames "
                f"(streams={len(names)})"
            )

        logger.info(
            "[%s] subscribed %d streams (%d native tfs, %d derived tfs, %d symbols)",
            self.name, len(names), len(self._native_tfs),
            len(self._derived_tfs), len(symbols),
        )

    def _parse_message(self, raw) -> List[tuple]:
        """Return DERIVED-path source bars (closed 1s klines) for one frame.

        This satisfies StreamingFeedBase's abstract contract and is kept
        consistent with _handle_frame, but note this feed overrides _consume
        (Binance mixes native + 1s bars on one connection), so the live path
        uses _handle_frame directly. This method is the 1s-only projection of
        _handle_frame, useful for testing and for any caller that wants just
        the aggregator-bound source bars.
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
            # 1s source bars: aggregate into derived rungs.
            for (symbol, src_t, o, h, l, c) in source_bars:
                emitted.extend(aggregator.add_source_bar(symbol, src_t, o, h, l, c))

            if emitted:
                emitted.sort(key=lambda p: (p.timestamp, p.timeframe))
                self._advance_cursors(emitted)
                await callback(emitted)

    def _handle_frame(self, raw):
        """Decode one combined-stream frame into (native_bars, source_bars).

        native_bars: list[OHLCPoint] — closed klines at a NATIVE target tf,
                     ready to emit directly.
        source_bars: list[tuple] — closed 1s klines as (sym, start_ms,
                     o,h,l,c) for the aggregator to bucket into derived rungs.

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

        if interval == _DERIVE_SOURCE and self._derived_tfs:
            # 1s kline → source bar for the aggregator.
            source_bars.append((symbol, start_ms, o, h, l, c))
        elif interval in _NATIVE_INTERVALS and interval in self._native_tfs:
            # Native closed kline → emit directly as its target bar.
            native_bars.append(OHLCPoint(
                symbol=symbol,
                timestamp=start_ms,
                open=o, high=h, low=l, close=c,
                timeframe=interval,
                asset_class=self.asset_class,
            ))
        # else: a kline for an interval we didn't configure — ignore.
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
        logger.info(
            "[%s] native tfs=%s derived tfs=%s",
            self.name, self._native_tfs, self._derived_tfs,
        )
        await super().start_live(symbols, timeframes, initial_cursors, callback)

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