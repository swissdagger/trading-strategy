"""
Bars are labelled by their OPEN time, and chain steps record that label; the
sweep COMPARES signals by when their bar closed (when the information
existed). A 1h bar 11:00-12:00 and a 30m bar 11:30-12:00 both close at 12:00,
so the 1h signal extends a chain whose top rung is that 30m signal. Each step
also carries bar_close and detected_at explicitly.
"""
from collections import deque

import pytest

from producer.bar import OHLCPoint
from producer.service2_machine import WINDOW_SIZE, ServiceTwoChainMachine

LADDER = ["1m", "30m", "1h"]
M = 60_000
H = 60 * M
STEP = {"1m": M, "30m": 30 * M, "1h": H}


def _returns(value):
    async def _coro():
        return value
    return _coro()


def _machine(signals):
    async def _noop(*a, **k):
        return None

    return ServiceTwoChainMachine(
        symbol="BTC-USD", asset_class="crypto", ladder=LADDER, ladder_name="T",
        forecast_fn=lambda **kw: {"5001": signals.pop(0)},
        prepare_df_fn=lambda *a, **kw: (None, None),
        to_interval_fn=lambda tf: (1, "minutes"),
        build_df_fn=lambda *a, **kw: None,
        persist_signal=_noop, persist_chain_init=_noop,
        persist_chain_propagate=_noop, persist_chain_terminate=_noop,
        load_active_chains=lambda: _returns([]),
        load_last_base_signal=lambda: _returns(None),
    )


def _fill(m, before_ms):
    for tf in LADDER:
        m._buffers[tf] = deque(
            ({"timestamp": before_ms - (WINDOW_SIZE - i) * STEP[tf]} for i in range(WINDOW_SIZE)),
            maxlen=WINDOW_SIZE)


def bar(ts, tf):
    return OHLCPoint(symbol="BTC-USD", timestamp=ts, open=1.0, high=1.0, low=1.0,
                     close=1.0, timeframe=tf, asset_class="crypto")


@pytest.mark.asyncio
async def test_1h_extends_a_30m_chain_forecasting_the_same_moment():
    eleven = 11 * H
    m = _machine([1, 1, 1])
    _fill(m, eleven)
    events = []
    for b in (bar(eleven + 29 * M, "1m"),     # 11:29-11:30: initiates
              bar(eleven + 30 * M, "30m"),    # 11:30-12:00: level 2
              bar(eleven, "1h")):             # 11:00-12:00: closes with it, level 3
        events += [(e.event_type, e.propagation_level, e.timestamp, e.bar_close)
                   for e in await m.on_bar(b)]
    assert [(t, lvl) for t, lvl, _, _ in events] == [
        ("chain_initiated", 1), ("chain_propagated", 2), ("chain_propagated", 3)]
    # Labels are the bars' opens; closes are explicit.
    assert [ts for _, _, ts, _ in events] == [
        "1970-01-01T11:29:00+00:00", "1970-01-01T11:30:00+00:00", "1970-01-01T11:00:00+00:00"]
    assert [c for _, _, _, c in events] == [
        "1970-01-01T11:30:00+00:00", "1970-01-01T12:00:00+00:00", "1970-01-01T12:00:00+00:00"]
    steps = next(iter(m._active_chains.values()))["history"]
    assert [st["timestamp"] for st in steps] == [ts for _, _, ts, _ in events]
    assert [st["bar_close"] for st in steps] == [c for _, _, _, c in events]
    assert all(st["detected_at"] for st in steps)


@pytest.mark.asyncio
async def test_a_signal_forecasting_an_earlier_moment_does_not_extend():
    eleven = 11 * H
    m = _machine([1, 1, 1])
    _fill(m, eleven)
    events = []
    for b in (bar(eleven + 59 * M, "1m"),     # 11:59-12:00
              bar(eleven + 30 * M, "30m"),    # 11:30-12:00: level 2
              bar(eleven - H, "1h")):         # 10:00-11:00: older information, refused
        events += [e.event_type for e in await m.on_bar(b)]
    assert events == ["chain_initiated", "chain_propagated"]


@pytest.mark.asyncio
async def test_a_chain_hydrated_without_bar_close_derives_it_from_its_label():
    from producer.service2_machine import _epoch_ms_to_iso
    eleven = 11 * H
    m = _machine([1])
    _fill(m, eleven)
    # A chain persisted before chain_steps.bar_close existed: only the open label.
    m._active_chains["c"] = {
        "chain_id": "c", "direction": 1, "current_level": 1, "next_expected_level": 2,
        "active_timeframes": ["1m", "30m"], "last_timestamp": _epoch_ms_to_iso(eleven + 30 * M),
        "history": [{"level": 1, "timeframe": "1m", "timestamp": _epoch_ms_to_iso(eleven + 29 * M)},
                    {"level": 2, "timeframe": "30m", "timestamp": _epoch_ms_to_iso(eleven + 30 * M)}],
    }
    m._last_base_signal = 1
    events = [e.event_type for e in await m.on_bar(bar(eleven, "1h"))]   # closes 12:00 = the 30m's close
    assert events == ["chain_propagated"]
