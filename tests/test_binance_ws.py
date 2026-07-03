"""
Tests for feeds/binance_ws.py.

Covers the crypto ws feed's distinctive logic without touching the network:

  - timeframe partitioning into native (direct) vs derived (1s-aggregated),
    and rejection of unsupported timeframes
  - stream-name generation: one @kline_<tf> per native rung, one @kline_1s
    per symbol when any derived rung is configured
  - frame handling: only CLOSED klines (x==true) are used; native closed
    klines emit directly; 1s closed klines become aggregator source bars;
    control frames / unclosed klines yield nothing
  - combined-stream unwrapping ({"stream","data"} envelope)
  - end-to-end through the custom _consume: a native 1m kline emits a 1m
    bar; a sequence of 1s klines produces an aggregated 5s bar

The kline field shapes (k.t, k.o/h/l/c, k.i, k.x, payload.s) are exactly
those in Binance's USD-M futures kline docs.
"""
import json

import pytest

from feeds.binance_ws import (
    BinanceFuturesWSFeed,
    _NATIVE_INTERVALS,
    _DERIVED_INTERVALS,
)
from producer.bar import OHLCPoint


class FakeBackfill:
    async def fetch_history(self, symbol, timeframe, points_needed):
        return []


def feed():
    return BinanceFuturesWSFeed(FakeBackfill())


def kline_frame(symbol, interval, start_ms, o, h, l, c, closed, combined=True):
    """Build a Binance combined-stream kline frame."""
    payload = {
        "e": "kline", "E": start_ms + 500, "s": symbol,
        "k": {
            "t": start_ms, "T": start_ms + 999, "s": symbol, "i": interval,
            "o": str(o), "h": str(h), "l": str(l), "c": str(c),
            "v": "1", "n": 5, "x": closed,
        },
    }
    if combined:
        return json.dumps({"stream": f"{symbol.lower()}@kline_{interval}", "data": payload})
    return json.dumps(payload)


# --------------------------------------------------------------------
# Timeframe partitioning
# --------------------------------------------------------------------

def test_partition_native_and_derived():
    f = feed()
    f._partition_timeframes(["5s", "10s", "1m", "5m"])
    assert f._native_tfs == ["1m", "5m"]
    assert f._derived_tfs == ["5s", "10s"]


def test_partition_rejects_unsupported():
    f = feed()
    with pytest.raises(ValueError, match="unsupported timeframes"):
        f._partition_timeframes(["1m", "7s"])   # 7s is neither native nor derived


def test_supported_is_native_union_derived():
    f = feed()
    assert f.supported_timeframes() == _NATIVE_INTERVALS | _DERIVED_INTERVALS


# --------------------------------------------------------------------
# Stream-name generation
# --------------------------------------------------------------------

def test_stream_names_native_only():
    f = feed()
    f._partition_timeframes(["1m", "5m"])
    names = f._stream_names(["BTCUSDT", "ETHUSDT"])
    assert set(names) == {
        "btcusdt@kline_1m", "btcusdt@kline_5m",
        "ethusdt@kline_1m", "ethusdt@kline_5m",
    }


def test_stream_names_adds_single_1s_for_derived():
    f = feed()
    f._partition_timeframes(["5s", "10s", "1m"])
    names = f._stream_names(["BTCUSDT"])
    # One 1s stream feeds BOTH 5s and 10s; plus the native 1m stream.
    assert set(names) == {"btcusdt@kline_1s", "btcusdt@kline_1m"}


def test_stream_names_no_1s_when_no_derived():
    f = feed()
    f._partition_timeframes(["1m"])
    names = f._stream_names(["BTCUSDT"])
    assert "btcusdt@kline_1s" not in names


# --------------------------------------------------------------------
# Frame handling: closed-flag gating and routing
# --------------------------------------------------------------------

def test_unclosed_kline_ignored():
    f = feed()
    f._partition_timeframes(["1m"])
    native, source = f._handle_frame(kline_frame("BTCUSDT", "1m", 60000, 1, 2, 0.5, 1.5, closed=False))
    assert native == [] and source == []


def test_native_closed_kline_emits_direct():
    f = feed()
    f._partition_timeframes(["1m", "5m"])
    native, source = f._handle_frame(kline_frame("BTCUSDT", "1m", 60000, 1, 2, 0.5, 1.5, closed=True))
    assert source == []
    assert len(native) == 1
    bar = native[0]
    assert isinstance(bar, OHLCPoint)
    assert bar.symbol == "BTCUSDT" and bar.timeframe == "1m"
    assert bar.timestamp == 60000
    assert (bar.open, bar.high, bar.low, bar.close) == (1.0, 2.0, 0.5, 1.5)


def test_1s_closed_kline_becomes_source_bar():
    f = feed()
    f._partition_timeframes(["5s", "10s"])   # derived rungs configured
    native, source = f._handle_frame(kline_frame("BTCUSDT", "1s", 1000, 1, 2, 0.5, 1.5, closed=True))
    assert native == []
    assert source == [("BTCUSDT", 1000, 1.0, 2.0, 0.5, 1.5)]


def test_1s_ignored_when_no_derived_configured():
    f = feed()
    f._partition_timeframes(["1m"])   # no derived rungs
    native, source = f._handle_frame(kline_frame("BTCUSDT", "1s", 1000, 1, 2, 0.5, 1.5, closed=True))
    # A stray 1s kline with no derived rung configured is ignored.
    assert native == [] and source == []


def test_native_kline_for_unconfigured_interval_ignored():
    f = feed()
    f._partition_timeframes(["1m"])   # only 1m native
    # A 5m closed kline arrives but 5m wasn't configured -> ignore.
    native, source = f._handle_frame(kline_frame("BTCUSDT", "5m", 300000, 1, 2, 0.5, 1.5, closed=True))
    assert native == [] and source == []


def test_subscription_ack_ignored():
    f = feed()
    f._partition_timeframes(["1m"])
    native, source = f._handle_frame(json.dumps({"result": None, "id": 1}))
    assert native == [] and source == []


def test_raw_non_combined_frame_handled():
    f = feed()
    f._partition_timeframes(["1m"])
    # Non-combined (bare payload) frame should still parse.
    native, source = f._handle_frame(kline_frame("BTCUSDT", "1m", 60000, 1, 2, 0.5, 1.5, closed=True, combined=False))
    assert len(native) == 1 and native[0].timeframe == "1m"


def test_malformed_kline_dropped():
    f = feed()
    f._partition_timeframes(["1m"])
    bad = json.dumps({"stream": "x", "data": {"k": {"i": "1m", "x": True}}})  # missing OHLC/t
    native, source = f._handle_frame(bad)
    assert native == [] and source == []


def test_junk_frame():
    f = feed()
    f._partition_timeframes(["1m"])
    assert f._handle_frame("not json") == ([], [])


# --------------------------------------------------------------------
# _parse_message projection (abstract-contract satisfaction)
# --------------------------------------------------------------------

def test_parse_message_returns_only_source_bars():
    f = feed()
    f._partition_timeframes(["5s", "1m"])
    # native 1m closed kline -> _parse_message returns [] (no source bars)
    assert f._parse_message(kline_frame("BTCUSDT", "1m", 60000, 1, 2, 0.5, 1.5, closed=True)) == []
    # 1s closed kline -> one source bar
    assert f._parse_message(kline_frame("BTCUSDT", "1s", 1000, 1, 2, 0.5, 1.5, closed=True)) == [
        ("BTCUSDT", 1000, 1.0, 2.0, 0.5, 1.5)
    ]


# --------------------------------------------------------------------
# End-to-end through the custom _consume
# --------------------------------------------------------------------

class ScriptedConn:
    def __init__(self, frames):
        self._frames = list(frames)

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for fr in self._frames:
            yield fr

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_consume_emits_native_and_aggregated(monkeypatch):
    from feeds.bar_aggregator import BarAggregator

    f = feed()
    f._partition_timeframes(["5s", "1m"])

    # Frames: three 1s klines spanning a 5s window then one in the next
    # window (closes the 5s bar), plus a native 1m closed kline.
    frames = [
        kline_frame("BTCUSDT", "1s", 0, 100, 101, 99, 100, closed=True),
        kline_frame("BTCUSDT", "1s", 1000, 100, 102, 100, 101, closed=True),
        kline_frame("BTCUSDT", "1s", 5000, 101, 101, 101, 101, closed=True),  # opens next 5s window
        kline_frame("BTCUSDT", "1m", 60000, 100, 105, 95, 104, closed=True),
    ]
    conn = ScriptedConn(frames)
    agg = BarAggregator("crypto", ["5s", "1m"], fill_gaps=False)

    emitted = []
    async def cb(points):
        emitted.extend(points)

    await f._consume(conn, agg, cb)

    # The 5s window [0,5000) should have closed (high 102, low 99).
    fivesec = [b for b in emitted if b.timeframe == "5s" and b.timestamp == 0]
    assert len(fivesec) == 1
    assert fivesec[0].high == 102 and fivesec[0].low == 99
    # The native 1m kline should have emitted directly.
    onemin = [b for b in emitted if b.timeframe == "1m"]
    assert len(onemin) == 1
    assert onemin[0].close == 104
