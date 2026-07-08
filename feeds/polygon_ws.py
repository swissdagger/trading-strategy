"""
feeds/polygon_ws.py — Polygon websocket feeds (stocks + forex).

The streaming counterpart to feeds/polygon_aggs.py. Where polygon_aggs.py is
one shared REST base with thin PolygonStocksFeed / PolygonForexFeed
subclasses, this is one shared websocket base (_PolygonWSBase) with thin
PolygonStocksWSFeed / PolygonForexWSFeed subclasses — the same shape, so the
two transports mirror each other and a reader who knows one knows the other.

All the resilience (reconnect, backfill, staleness) lives in
StreamingFeedBase. All the Polygon-cluster wire specifics live here. The
subclasses below carry only the handful of per-cluster differences.

## Per-cluster differences (why subclasses exist)

Polygon's per-second aggregate stream differs between clusters in exactly
three ways, so those three things are the class attributes / overrides:

  1. CLUSTER URL      stocks -> wss://business.polygon.io/stocks
                      forex  -> wss://business.polygon.io/forex
                      (this account's real-time entitlement host for both
                       clusters; see WS_HOST)
  2. EVENT CODE       stocks per-second aggregates arrive as ev="A"
                      forex  per-second aggregates arrive as ev="CA"
  3. WIRE SYMBOL      stocks: bare ticker "AAPL" (== canonical)
                      forex:  "EUR/USD" on the wire; canonical is "EURUSD"

Everything else — auth handshake, subscribe framing, message parsing,
batching, the (o,h,l,c,s) field names — is identical across clusters and
lives in the base.

## One connection per cluster (a hard Polygon constraint)

Polygon permits ONE simultaneous connection per cluster. A second connection
to the same cluster silently drops the first. So a _PolygonWSBase instance
opens exactly one socket and multiplexes EVERY symbol for its asset class
over it (subscribe "A.AAPL,A.MSFT,A.SPCX"). This is why the feed is
constructed per-asset-class (as FeedProcess already does) and never
per-symbol.

## Backfill composition

Each ws feed is constructed WITH its REST sibling (PolygonStocksFeed /
PolygonForexFeed), passed straight to StreamingFeedBase as the backfill
engine. On reconnect the supervisor calls fetch_history / fetch_range on
that sibling — the same battle-tested pagination the REST path uses. We add
a thin fetch_range here (see _PolygonRESTRange) so backfill asks Polygon for
exactly the outage window rather than most-recent-N.
"""
from __future__ import annotations

import json
import logging
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
    WS_HOST: str = "business.polygon.io"

    # websockets recv timeout is driven by the supervisor's staleness window
    # (per-message wait_for in _consume). The connection-level open timeout
    # below is just for the initial handshake.
    OPEN_TIMEOUT_S = 15.0

    def __init__(self, api_key: str, backfill_feed, fill_gaps: bool = True):
        super().__init__(backfill_feed=backfill_feed, fill_gaps=fill_gaps)
        self._api_key = api_key

    def supported_timeframes(self) -> Set[str]:
        return self.SUPPORTED_TIMEFRAMES

    def _ws_url(self) -> str:
        return f"wss://{self.WS_HOST}/{self.CLUSTER}"

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
    "5s", "10s", "30s", "1m", "2m", "5m", "10m",
    "15s", "3m", "15m", "30m", "1h",
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


class PolygonForexWSFeed(_PolygonWSBase):
    """Polygon forex per-second aggregates over websocket.

    Wire symbol is slash-separated ({from}/{to}); canonical is the bare
    concatenation (EURUSD) the rest of the system uses. Per-second forex
    events use ev="CA" (not "A"). Session gating via is_market_open('fx')
    (24/5, weekend gap) keeps the staleness watchdog from tripping across
    the Fri-Sun close.
    """

    asset_class = "fx"
    name = "polygon_forex_ws"
    CLUSTER = "forex"
    EVENT_CODE = "CA"
    SUPPORTED_TIMEFRAMES = _FOREX_TIMEFRAMES

    def to_native_symbol(self, symbol: str) -> str:
        # REST side uses the C: prefix (C:EURUSD); mirror polygon_forex.
        return f"C:{symbol}"

    def to_wire_symbol(self, canonical: str) -> str:
        # EURUSD -> EUR/USD for the ws subscribe. Assumes 3+3 major-pair
        # convention, which matches the tracked fx universe.
        if "/" in canonical:
            return canonical  # already wire form
        return f"{canonical[:3]}/{canonical[3:]}"

    def from_wire_symbol(self, wire: str) -> str:
        # "EUR/USD" -> "EURUSD"
        return wire.replace("/", "")