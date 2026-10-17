"""
The bar store: every bar a machine APPENDS goes to warmup_bars once it is
fully applied, so a restarted signal process hydrates its buffers up to where
the last run got. Before this, buffers came back as warmup left them while
Kafka resumed after the last committed offset, and every bar in between was
spliced out of the series Sumtyme reads as consecutive steps.
"""
from __future__ import annotations

import os
from collections import deque

import pytest
import pytest_asyncio

from producer.bar import OHLCPoint
from producer.fanout_router_multi import MultiLadderRouter
from producer.service2_machine import WINDOW_SIZE, ServiceTwoChainMachine
from strategy.ladder_registry import LadderRegistry

LADDER = ["1m", "3m"]
SYM = "BTC-USD"
M = 60_000


def _returns(value):
    async def _coro():
        return value
    return _coro()


def bar(ts, tf="1m", close=100.0):
    return OHLCPoint(symbol=SYM, timestamp=ts, open=close, high=close, low=close,
                     close=close, timeframe=tf, asset_class="crypto")


def _machine(order, load_buffers=None):
    async def _noop(*a, **k):
        return None

    def forecast(**kw):
        order.append("forecast")
        return {"5001": 0}

    return ServiceTwoChainMachine(
        symbol=SYM, asset_class="crypto", ladder=LADDER, ladder_name="FC",
        forecast_fn=forecast,
        build_df_fn=lambda snap: None, prepare_df_fn=lambda df, tf: (None, None),
        to_interval_fn=lambda tf: (1, "minutes"),
        persist_signal=_noop, persist_chain_init=_noop,
        persist_chain_propagate=_noop, persist_chain_terminate=_noop,
        load_active_chains=lambda: _returns([]),
        load_last_base_signal=lambda: _returns(None),
        load_buffers=load_buffers,
    )


def _router(machine, persist_bar):
    reg = LadderRegistry()
    reg.register("FC", LADDER)

    async def _publish(raw):
        return None

    return MultiLadderRouter(registry=reg, make_machine=lambda s, ln, tfs: machine,
                             publish=_publish, persist_bar=persist_bar)


@pytest.mark.asyncio
async def test_appended_bars_are_stored_after_apply_and_duplicates_are_not():
    order, stored = [], []

    async def persist(b):
        order.append("store")
        stored.append((b.timeframe, b.timestamp))

    m = _machine(order)
    m._buffers["1m"] = deque(({"timestamp": i * M} for i in range(WINDOW_SIZE)),
                             maxlen=WINDOW_SIZE)
    router = _router(m, persist)
    newest = (WINDOW_SIZE - 1) * M
    await router.on_bar(bar(newest + M))             # warm rung: forecast, then store
    await router.on_bar(bar(newest + M))             # duplicate: not appended, not stored
    await router.on_bar(bar(newest - M))             # older: dropped, not stored
    await router.on_bar(bar(0, "3m"))                # cold rung: appended, stored
    assert stored == [("1m", newest + M), ("3m", 0)]
    assert order == ["forecast", "store", "store"]   # stored only after the forecast/apply


@pytest.mark.asyncio
async def test_a_store_failure_does_not_stop_the_bar():
    async def persist(b):
        raise RuntimeError("db down")

    m = _machine([])
    router = _router(m, persist)
    await router.on_bar(bar(0))
    assert [t["timestamp"] for t in m._buffers["1m"]] == [0]


DSN = os.environ.get("QISS_TEST_DSN")


@pytest_asyncio.fixture
async def pool():
    import asyncpg
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=2, statement_cache_size=0)
    await p.execute("DELETE FROM warmup_bars WHERE symbol = $1", SYM)
    yield p
    await p.execute("DELETE FROM warmup_bars WHERE symbol = $1", SYM)
    await p.close()


@pytest.mark.skipif(not DSN, reason="QISS_TEST_DSN not set; needs real Postgres")
@pytest.mark.asyncio
async def test_restart_hydrates_the_bars_the_last_run_appended(pool):
    from producer.persistence import (make_load_buffers, make_persist_live_bar,
                                      make_persist_warmup_bars, read_bar_store_cursors)

    # Warmup seeded bars 0..9; the first run then appended 10..14 live.
    await make_persist_warmup_bars(pool)(SYM, "crypto", "1m", [bar(i * M) for i in range(10)])
    first = _machine([], load_buffers=make_load_buffers(pool, SYM, LADDER))
    router = _router(first, make_persist_live_bar(pool))
    for i in range(10, 15):
        await router.on_bar(bar(i * M))
    # A replay with revised values must not overwrite what the buffer kept.
    await router.on_bar(bar(14 * M, close=999.0))

    second = _machine([], load_buffers=make_load_buffers(pool, SYM, LADDER))
    await second.hydrate()
    assert [t["timestamp"] for t in second._buffers["1m"]] == [i * M for i in range(15)]
    assert second._buffers["1m"][-1]["close"] == 100.0
    assert (await read_bar_store_cursors(pool))[SYM]["1m"] == 14 * M
