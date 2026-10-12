"""
The chain machine drops a bar that is not newer than the newest bar already in
its buffer for that timeframe. In Sumtyme's step mode the buffer order IS the
series ("one step" = one row), so a duplicate or out-of-order bar would corrupt
the input — and a duplicate would also re-run the forecast and re-emit the same
chain events. Sources: Kafka at-least-once replay after a signal-process
restart, and any feed/warmup cursor overlap.
"""
from collections import deque

import pytest

from producer.bar import OHLCPoint
from producer.service2_machine import WINDOW_SIZE, ServiceTwoChainMachine

FC_LADDER = ["1m", "3m", "5m", "15m", "1h"]


def _returns(value):
    async def _coro():
        return value
    return _coro()


def _machine(forecast_calls):
    async def _noop(*a, **k):
        return None

    def forecast(**kw):
        forecast_calls.append(kw)
        return {"5001": 0}

    return ServiceTwoChainMachine(
        symbol="BTC-USD", asset_class="crypto", ladder=FC_LADDER, ladder_name="FC",
        forecast_fn=forecast,
        prepare_df_fn=lambda *a, **kw: (None, None),
        to_interval_fn=lambda tf: (1, "minutes"),
        build_df_fn=lambda *a, **kw: None,
        persist_signal=_noop, persist_chain_init=_noop,
        persist_chain_propagate=_noop, persist_chain_terminate=_noop,
        load_active_chains=lambda: _returns([]),
        load_last_base_signal=lambda: _returns(None),
    )


def bar(ts, tf="1m", close=100.0):
    return OHLCPoint(symbol="BTC-USD", timestamp=ts, open=close, high=close,
                     low=close, close=close, timeframe=tf, asset_class="crypto")


@pytest.mark.asyncio
async def test_duplicate_bar_is_not_appended():
    m = _machine([])
    await m.on_bar(bar(60_000))
    await m.on_bar(bar(60_000, close=101.0))   # replayed (even with other values)
    buf = m._buffers["1m"]
    assert [t["timestamp"] for t in buf] == [60_000]
    assert buf[-1]["close"] == 100.0           # the original is kept


@pytest.mark.asyncio
async def test_out_of_order_bar_is_not_appended():
    m = _machine([])
    await m.on_bar(bar(120_000))
    await m.on_bar(bar(60_000))
    assert [t["timestamp"] for t in m._buffers["1m"]] == [120_000]


@pytest.mark.asyncio
async def test_newer_bars_still_append_and_timeframes_are_independent():
    m = _machine([])
    await m.on_bar(bar(60_000))
    await m.on_bar(bar(180_000, tf="3m"))
    await m.on_bar(bar(120_000))               # newer for 1m; older than the 3m bar
    assert [t["timestamp"] for t in m._buffers["1m"]] == [60_000, 120_000]
    assert [t["timestamp"] for t in m._buffers["3m"]] == [180_000]


@pytest.mark.asyncio
async def test_duplicate_on_a_full_buffer_does_not_reforecast():
    calls = []
    m = _machine(calls)
    m._buffers["1m"] = deque(
        ({"timestamp": i * 60_000, "close": 100.0} for i in range(WINDOW_SIZE)),
        maxlen=WINDOW_SIZE)
    newest = (WINDOW_SIZE - 1) * 60_000
    events = await m.on_bar(bar(newest))       # replay of the newest buffered bar
    assert events == [] and calls == []
    assert len(m._buffers["1m"]) == WINDOW_SIZE
    assert m._buffers["1m"][-1]["timestamp"] == newest


@pytest.mark.asyncio
async def test_hydrated_buffer_guards_the_first_live_bar():
    # After a restart the buffer comes from warmup_bars; a replayed bar equal to
    # the newest hydrated one must be dropped too.
    m = _machine([])
    m._buffers["1m"] = deque([{"timestamp": 600_000, "close": 1.0}], maxlen=WINDOW_SIZE)
    await m.on_bar(bar(600_000))
    await m.on_bar(bar(660_000))
    assert [t["timestamp"] for t in m._buffers["1m"]] == [600_000, 660_000]
