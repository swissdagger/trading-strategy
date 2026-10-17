"""
feeds/polygon_ws.py — Polygon stocks websocket feed (per-second aggregates).

The streaming counterpart to feeds/polygon_aggs.py: _PolygonWSBase carries
the Polygon-cluster wire specifics (auth handshake, subscribe framing,
message parsing); PolygonStocksWSFeed is the thin stocks subclass. All the
resilience (reconnect, backfill, staleness) lives in StreamingFeedBase.
Restored for the equities dense ladder (PR 9); the forex subclass was not.

## Why per-second aggregates

The stream delivers ev="A" bars once per second per symbol; BarAggregator
(feeds/bar_aggregator.py) buckets them into EVERY configured rung — 30s, 45s,
90s as well as 1m and up — with the same epoch-aligned floor the REST path's
resampler uses. This is what makes the sub-minute rungs genuinely live; the
REST /v2/aggs second aggregates are T+1 stale and never used for them.

## One connection per cluster (a hard Polygon constraint)

Polygon permits ONE simultaneous connection per cluster. A second connection
to the same cluster silently drops the first. So a _PolygonWSBase instance
opens exactly one socket and multiplexes EVERY symbol for its asset class
over it (subscribe "A.AAPL,A.MSFT"). This is why the feed is constructed
per-asset-class (as FeedProcess already does) and never per-symbol.

## Backfill composition and REST fallback

The ws feed is constructed WITH its REST sibling (PolygonStocksFeed), passed
to StreamingFeedBase as the backfill engine: on reconnect the supervisor
fetches the outage window from it (minute-and-up rungs only; see
_can_backfill_tf). If the socket cannot be established at all on a cold
start — wrong host, no real-time entitlement — start_live raises
StreamingUnavailable and feed_process runs the REST sibling for the process
lifetime instead (`rest_feed` below exposes it).
"""
from __future__ import annotations

import json
import logging
import os
from typing import Dict, List, Set

import websockets

from feeds.streaming_base import StreamingFeedBase
from producer.bar import OHLCPoint

logger = logging.getLogger(__name__)


class _PolygonWSBase(StreamingFeedBase):
    """Shared Polygon per-second websocket feed. Subclasses set CLUSTER,
    EVENT_CODE, SUPPORTED_TIMEFRAMES, asset_class, name, and implement
    to_native_symbol / to_wire_symbol.
    """

    # Subclasses MUST set these.
    CLUSTER: str = ""                # 'stocks' | 'forex'
    EVENT_CODE: str = ""             # 'A' (stocks) | 'CA' (forex)
    SUPPORTED_TIMEFRAMES: Set[str] = set()

    # Cluster hostname. This account's real-time entitlement (both stocks and
    # forex) is served from Polygon/Massive's Business cluster host, confirmed
    # directly with a Massive employee. Hitting the generic socket.polygon.io
    # host returns real-time-access-denied despite the plan including real-time,
    # because the entitlement is bound to this host. Kept as a class attribute
    # (not a hardcoded constant) so a per-cluster override remains possible if
    # one cluster ever moves.
    WS_HOST: str = os.environ.get("POLYGON_WS_HOST", "business.polygon.io")

    # websockets recv timeout is driven by the supervisor's staleness window
    # (per-message wait_for in _consume). The connection-level open timeout
    # below is just for the initial handshake.
    OPEN_TIMEOUT_S = 15.0

    def __init__(self, api_key: str, backfill_feed, fill_gaps: bool = True):
        super().__init__(backfill_feed=backfill_feed, fill_gaps=fill_gaps)
        self._api_key = api_key

    @property
    def rest_feed(self):
        """The REST sibling this feed backfills from — and what feed_process
        falls back to when start_live raises StreamingUnavailable."""
        return self._backfill_feed

    def supported_timeframes(self) -> Set[str]:
        return self.SUPPORTED_TIMEFRAMES

    def _ws_url(self) -> str:
        return f"wss://{self.WS_HOST}/{self.CLUSTER}"

    def _can_backfill_tf(self, tf: str) -> bool:
        """Polygon REST cannot usefully backfill SUB-MINUTE rungs (5s/10s/15s/
        30s): its sub-minute aggregates lag badly and are effectively unusable
        in real time — the very reason these rungs are served by the websocket
        stream instead. So on a reconnect we must NOT REST-backfill them: the
        fetch returns stale-or-empty data and, worse, risks the 400/latency
        storm the base docstring warns of, delaying live resumption. Sub-minute
        rungs simply resume live from the ws stream after reconnect (they refill
        from fresh per-second aggregates). Minute-and-up rungs remain
        REST-backfillable and return True.

        A timeframe is sub-minute iff it is denominated in seconds ('...s' but
        not minutes/hours). This is why REST calls for e.g. 10-second bars must
        never be issued by the outage-backfill path.
        """
        return not tf.endswith("s")

    def to_wire_symbol(self, canonical: str) -> str:
        """Canonical symbol -> the form Polygon's ws subscribe expects.
        Default is identity (stocks). Forex overrides (EURUSD -> EUR/USD)."""
        return canonical

    def from_wire_symbol(self, wire: str) -> str:
        """Inverse of to_wire_symbol: the `sym` field on an inbound message
        -> canonical form stored/emitted. Default identity (stocks)."""
        return wire

    # ---- StreamingFeedBase hooks ----

    async def _connect(self):
        """Open the socket and authenticate. Returns the live connection.
        Raises on auth failure so the supervisor backs off and retries."""
        conn = await websockets.connect(
            self._ws_url(),
            open_timeout=self.OPEN_TIMEOUT_S,
            # No client-side ping: Polygon sends its own; we rely on the
            # supervisor's staleness window to detect a dead socket, which
            # is market-aware (won't false-trip on a closed session).
            ping_interval=None,
            max_size=None,   # aggregate batches can be large for * subs
        )
        # Auth handshake. Polygon replies with a status message; we read
        # until we see auth_success (or fail loudly).
        await conn.send(json.dumps({"action": "auth", "params": self._api_key}))
        await self._await_auth(conn)
        return conn

    async def _await_auth(self, conn) -> None:
        """Read status messages until auth is confirmed. Polygon sends
        {"ev":"status","status":"auth_success",...} on success or
        "auth_failed" on a bad key."""
        raw = await conn.recv()
        for msg in self._decode(raw):
            if msg.get("ev") == "status":
                status = msg.get("status")
                if status == "auth_success":
                    return
                if status in ("auth_failed", "auth_timeout"):
                    raise ConnectionError(
                        f"Polygon {self.CLUSTER} auth failed: {msg.get('message')}"
                    )
        # Some deployments send a "connected" status first, THEN auth_success
        # in a later frame. Read one more frame before giving up.
        raw = await conn.recv()
        for msg in self._decode(raw):
            if msg.get("ev") == "status" and msg.get("status") == "auth_success":
                return
        raise ConnectionError(f"Polygon {self.CLUSTER}: no auth_success in handshake")

    async def _subscribe(self, conn, symbols: List[str]) -> None:
        """Subscribe to per-second aggregates for every symbol on the single
        cluster connection: 'A.AAPL,A.MSFT' (stocks) / 'CA.EUR/USD' (forex)."""
        params = ",".join(
            f"{self.EVENT_CODE}.{self.to_wire_symbol(s)}" for s in symbols
        )
        await conn.send(json.dumps({"action": "subscribe", "params": params}))
        logger.info("[%s] subscribed %d symbols on %s cluster",
                    self.name, len(symbols), self.CLUSTER)

    def _parse_message(self, raw) -> List[tuple]:
        """Parse one raw ws frame into source-bar tuples
        (canonical_symbol, start_ms, o, h, l, c).

        A frame is a JSON array of events; we keep only our EVENT_CODE
        aggregates and skip status/other events. Each kept event uses `s`
        (window start, ms) as the source-bar timestamp — the aggregator
        buckets on it exactly as the REST resampler buckets on Polygon's
        aggregate `t`.
        """
        out: List[tuple] = []
        for msg in self._decode(raw):
            if msg.get("ev") != self.EVENT_CODE:
                continue  # status, or a different event type — skip
            try:
                symbol = self.from_wire_symbol(msg["sym"])
                out.append((
                    symbol,
                    int(msg["s"]),        # window start, Unix ms
                    float(msg["o"]),
                    float(msg["h"]),
                    float(msg["l"]),
                    float(msg["c"]),
                ))
            except (KeyError, TypeError, ValueError):
                # A malformed aggregate must not kill the stream. Log once
                # and drop it; the next bar for that window (or backfill on
                # a later reconnect) recovers the data.
                logger.warning("[%s] skipping malformed aggregate: %r", self.name, msg)
        return out

    async def _read(self, conn):
        """Async-iterate raw frames off the socket until it drops. The
        `async for` ends (raising ConnectionClosed) on disconnect, which
        propagates to the supervisor as a normal disconnect."""
        async for raw in conn:
            yield raw

    async def _close(self, conn) -> None:
        try:
            await conn.close()
        except Exception:
            pass  # already-dead socket — best effort, never raise

    @staticmethod
    def _decode(raw) -> List[dict]:
        """Polygon frames are JSON arrays of event objects (occasionally a
        bare object). Normalise to a list of dicts; tolerate junk."""
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return []
        if isinstance(data, list):
            return [m for m in data if isinstance(m, dict)]
        if isinstance(data, dict):
            return [data]
        return []


# The active-ladder timeframes plus nearby compat labels, matching the REST
# feeds' supported sets. Supporting a label and configuring it are separate
# decisions (config.py owns the latter).
_STOCKS_TIMEFRAMES: Set[str] = {
    # Active ladder (DL) — every rung is built from the 1-second stream.
    "30s", "45s", "1m", "90s", "2m", "3m", "15m", "30m", "1h", "2h", "4h", "6h",
    # Compat
    "5s", "10s", "15s", "5m", "10m",
}
_FOREX_TIMEFRAMES: Set[str] = set(_STOCKS_TIMEFRAMES)


class PolygonStocksWSFeed(_PolygonWSBase):
    """Polygon US cash-equity per-second aggregates over websocket.

    Canonical symbol == wire symbol (bare ticker). Session gating is handled
    by the supervisor's staleness check via is_market_open('equity'), which
    stands down outside the 13:30-20:00 UTC cash session so overnight silence
    doesn't trip a false reconnect.
    """

    asset_class = "equity"
    name = "polygon_stocks_ws"
    CLUSTER = "stocks"
    EVENT_CODE = "A"
    SUPPORTED_TIMEFRAMES = _STOCKS_TIMEFRAMES

    def to_native_symbol(self, symbol: str) -> str:
        return symbol  # REST-side identity too

