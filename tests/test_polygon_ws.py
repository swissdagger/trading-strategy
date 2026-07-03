"""
Tests for feeds/polygon_ws.py.

Covers the per-cluster wire contract without touching the network:

  - message parsing: keeps our EVENT_CODE, skips status/other events,
    survives malformed aggregates, handles batched arrays
  - symbol conversion: stocks identity; forex EURUSD<->EUR/USD and the
    C: REST-native prefix
  - subscribe framing: correct "A.SYM,A.SYM" / "CA.EUR/USD" params
  - auth handshake: auth_success proceeds; auth_failed raises

The message field shapes (ev, sym, o/h/l/c, s, e) are exactly those in
Polygon's per-second aggregate docs (stocks ev="A", forex ev="CA").
"""
import json

import pytest

from feeds.polygon_ws import PolygonStocksWSFeed, PolygonForexWSFeed


class FakeBackfill:
    async def fetch_history(self, symbol, timeframe, points_needed):
        return []


def stocks_feed():
    return PolygonStocksWSFeed("KEY", FakeBackfill())


def forex_feed():
    return PolygonForexWSFeed("KEY", FakeBackfill())


# --------------------------------------------------------------------
# Symbol conversion
# --------------------------------------------------------------------

def test_stocks_symbol_identity():
    f = stocks_feed()
    assert f.to_wire_symbol("AAPL") == "AAPL"
    assert f.from_wire_symbol("AAPL") == "AAPL"
    assert f.to_native_symbol("AAPL") == "AAPL"


def test_forex_symbol_conversion():
    f = forex_feed()
    # canonical -> wire (slash) for subscribe
    assert f.to_wire_symbol("EURUSD") == "EUR/USD"
    # wire -> canonical for inbound `sym`
    assert f.from_wire_symbol("EUR/USD") == "EURUSD"
    # REST-native uses the C: prefix, mirroring polygon_forex
    assert f.to_native_symbol("EURUSD") == "C:EURUSD"
    # idempotent if already wire form
    assert f.to_wire_symbol("EUR/USD") == "EUR/USD"


# --------------------------------------------------------------------
# Message parsing
# --------------------------------------------------------------------

def test_parse_stocks_aggregate():
    f = stocks_feed()
    frame = json.dumps([
        {"ev": "A", "sym": "AAPL", "o": 100.0, "h": 101.0, "l": 99.5,
         "c": 100.5, "s": 1610000000000, "e": 1610000001000, "v": 500},
    ])
    out = f._parse_message(frame)
    assert out == [("AAPL", 1610000000000, 100.0, 101.0, 99.5, 100.5)]


def test_parse_forex_aggregate_ca_event():
    f = forex_feed()
    frame = json.dumps([
        {"ev": "CA", "sym": "EUR/USD", "o": 1.10, "h": 1.11, "l": 1.09,
         "c": 1.105, "s": 1610000000000, "e": 1610000001000},
    ])
    out = f._parse_message(frame)
    # symbol normalised to canonical EURUSD; ts is the window START (s)
    assert out == [("EURUSD", 1610000000000, 1.10, 1.11, 1.09, 1.105)]


def test_parse_skips_wrong_event():
    f = stocks_feed()
    # forex CA event must be ignored by a stocks feed (wrong EVENT_CODE),
    # and status events are always skipped.
    frame = json.dumps([
        {"ev": "status", "status": "auth_success", "message": "authenticated"},
        {"ev": "CA", "sym": "EUR/USD", "o": 1, "h": 1, "l": 1, "c": 1, "s": 1},
        {"ev": "AM", "sym": "AAPL", "o": 1, "h": 1, "l": 1, "c": 1, "s": 1},  # per-minute, not per-second
    ])
    assert f._parse_message(frame) == []


def test_parse_batched_multiple_symbols():
    f = stocks_feed()
    frame = json.dumps([
        {"ev": "A", "sym": "AAPL", "o": 1, "h": 2, "l": 0.5, "c": 1.5, "s": 1000},
        {"ev": "A", "sym": "MSFT", "o": 3, "h": 4, "l": 2.5, "c": 3.5, "s": 1000},
    ])
    out = f._parse_message(frame)
    assert len(out) == 2
    assert {o[0] for o in out} == {"AAPL", "MSFT"}


def test_parse_malformed_aggregate_dropped_not_fatal():
    f = stocks_feed()
    frame = json.dumps([
        {"ev": "A", "sym": "AAPL", "o": 1, "h": 2, "l": 0.5, "c": 1.5, "s": 1000},
        {"ev": "A", "sym": "BADD"},  # missing OHLC/s — must be dropped
    ])
    out = f._parse_message(frame)
    # The good bar survives; the malformed one is dropped without raising.
    assert out == [("AAPL", 1000, 1.0, 2.0, 0.5, 1.5)]


def test_parse_junk_frame():
    f = stocks_feed()
    assert f._parse_message("not json") == []
    assert f._parse_message(json.dumps({"ev": "status", "status": "connected"})) == []


# --------------------------------------------------------------------
# Subscribe framing + auth handshake (fake socket, no network)
# --------------------------------------------------------------------

class FakeConn:
    """Scripts inbound frames (for auth) and records outbound sends."""

    def __init__(self, inbound):
        self._inbound = list(inbound)
        self.sent = []

    async def send(self, msg):
        self.sent.append(msg)

    async def recv(self):
        if not self._inbound:
            raise AssertionError("recv called with no scripted frames left")
        return self._inbound.pop(0)

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_subscribe_framing_stocks():
    f = stocks_feed()
    conn = FakeConn([])
    await f._subscribe(conn, ["AAPL", "MSFT"])
    sent = json.loads(conn.sent[0])
    assert sent["action"] == "subscribe"
    assert sent["params"] == "A.AAPL,A.MSFT"


@pytest.mark.asyncio
async def test_subscribe_framing_forex_slash_and_ca():
    f = forex_feed()
    conn = FakeConn([])
    await f._subscribe(conn, ["EURUSD", "GBPUSD"])
    sent = json.loads(conn.sent[0])
    assert sent["params"] == "CA.EUR/USD,CA.GBP/USD"


@pytest.mark.asyncio
async def test_auth_success_proceeds():
    f = stocks_feed()
    conn = FakeConn([
        json.dumps([{"ev": "status", "status": "auth_success", "message": "ok"}]),
    ])
    # Should return without raising.
    await f._await_auth(conn)


@pytest.mark.asyncio
async def test_auth_success_after_connected_frame():
    f = stocks_feed()
    conn = FakeConn([
        json.dumps([{"ev": "status", "status": "connected", "message": "Connected"}]),
        json.dumps([{"ev": "status", "status": "auth_success", "message": "ok"}]),
    ])
    await f._await_auth(conn)  # reads the second frame for auth_success


@pytest.mark.asyncio
async def test_auth_failed_raises():
    f = stocks_feed()
    conn = FakeConn([
        json.dumps([{"ev": "status", "status": "auth_failed", "message": "bad key"}]),
    ])
    with pytest.raises(ConnectionError):
        await f._await_auth(conn)
