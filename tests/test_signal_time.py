"""
The chain sweep compares signals by the bar they FORECAST (window end), not by
the window start of the bar they were computed from. A 1h bar at 11:00 and a
30m bar at 11:30 both forecast 12:00, so the 1h signal extends a chain whose
top rung is that 30m signal.
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
    for b in (bar(eleven + 29 * M, "1m"),     # forecasts 11:30: initiates
              bar(eleven + 30 * M, "30m"),    # forecasts 12:00: level 2
              bar(eleven, "1h")):             # forecasts 12:00: level 3
        events += [(e.event_type, e.propagation_level, e.timestamp) for e in await m.on_bar(b)]
    assert [(t, lvl) for t, lvl, _ in events] == [
        ("chain_initiated", 1), ("chain_propagated", 2), ("chain_propagated", 3)]
    assert events[1][2] == events[2][2] == "1970-01-01T12:00:00+00:00"


@pytest.mark.asyncio
async def test_a_signal_forecasting_an_earlier_moment_does_not_extend():
    eleven = 11 * H
    m = _machine([1, 1, 1])
    _fill(m, eleven)
    events = []
    for b in (bar(eleven + 59 * M, "1m"),     # forecasts 12:00
              bar(eleven + 30 * M, "30m"),    # forecasts 12:00: level 2
              bar(eleven - H, "1h")):         # forecasts 11:00: stale for this chain
        events += [e.event_type for e in await m.on_bar(b)]
    assert events == ["chain_initiated", "chain_propagated"]
