"""Chain timing against real Postgres (migration 027): a step records its bar's
open label, its close and when it was detected; a restart hydrates the close
the sweep compares on; trade_signals carries the deciding bar's close."""
import os
from collections import deque

import pytest
import pytest_asyncio

from producer.bar import OHLCPoint
from producer.service2_machine import WINDOW_SIZE, ServiceTwoChainMachine

DSN = os.environ.get("QISS_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="QISS_TEST_DSN not set; needs real Postgres")
SYM = "TIMING-USD"
LADDER = ["1m", "5m"]
M = 60_000
T = 1_760_000_100_000 // (5 * M) * (5 * M)        # a 5m-aligned epoch ms


@pytest_asyncio.fixture
async def pool():
    import asyncpg
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=2, statement_cache_size=0)
    await p.execute("DELETE FROM chains WHERE symbol = $1", SYM)
    await p.execute("DELETE FROM trade_signals WHERE symbol = $1", SYM)
    yield p
    await p.execute("DELETE FROM chains WHERE symbol = $1", SYM)
    await p.execute("DELETE FROM trade_signals WHERE symbol = $1", SYM)
    await p.close()


def _returns(value):
    async def _coro():
        return value
    return _coro()


def _machine(pool, signals):
    from producer.persistence import (make_load_active_chains, make_persist_chain_init,
                                      make_persist_chain_propagate,
                                      make_persist_chain_terminate)

    async def _noop(*a, **k):
        return None

    m = ServiceTwoChainMachine(
        symbol=SYM, asset_class="crypto", ladder=LADDER, ladder_name="FC",
        forecast_fn=lambda **kw: {"5001": signals.pop(0)},
        prepare_df_fn=lambda *a, **kw: (None, None), to_interval_fn=lambda tf: (1, "minutes"),
        build_df_fn=lambda *a, **kw: None, persist_signal=_noop,
        persist_chain_init=make_persist_chain_init(pool),
        persist_chain_propagate=make_persist_chain_propagate(pool),
        persist_chain_terminate=make_persist_chain_terminate(pool),
        load_active_chains=make_load_active_chains(pool, SYM, "crypto", "FC"),
        load_last_base_signal=lambda: _returns(None),
    )
    for tf, step in (("1m", M), ("5m", 5 * M)):
        m._buffers[tf] = deque(({"timestamp": T - (WINDOW_SIZE - i) * step}
                                for i in range(WINDOW_SIZE)), maxlen=WINDOW_SIZE)
    return m


def bar(ts, tf):
    return OHLCPoint(symbol=SYM, timestamp=ts, open=1.0, high=1.0, low=1.0, close=1.0,
                     timeframe=tf, asset_class="crypto")


@pytest.mark.asyncio
async def test_steps_record_open_label_close_and_detection_and_survive_a_restart(pool):
    m = _machine(pool, [1, 1])
    await m.hydrate()
    await m.on_bar(bar(T + 4 * M, "1m"))     # 1m bar T+4..T+5: initiates
    await m.on_bar(bar(T, "5m"))             # 5m bar T..T+5: closes with it, propagates
    rows = await pool.fetch(
        """SELECT s.step_index, s.step_time, s.bar_close, s.detected_at, s.recorded_at
           FROM chain_steps s JOIN chains c USING (chain_id)
           WHERE c.symbol = $1 ORDER BY s.step_index""", SYM)
    assert len(rows) == 2
    opens = [int(r["step_time"].timestamp() * 1000) for r in rows]
    closes = [int(r["bar_close"].timestamp() * 1000) for r in rows]
    assert opens == [T + 4 * M, T]                    # labels: the bars' opens
    assert closes == [T + 5 * M, T + 5 * M]           # both closed at T+5
    assert all(r["detected_at"] is not None for r in rows)

    # A restarted machine hydrates the stored close, which the sweep compares on.
    m2 = _machine(pool, [])
    await m2.hydrate()
    (chain,) = m2._active_chains.values()
    assert chain["history"][-1]["bar_close"] == rows[-1]["bar_close"].isoformat()


@pytest.mark.asyncio
async def test_trade_signals_store_the_deciding_bar_close(pool):
    from execution.signal_writer import SignalWriter, SignalWriterHooks
    from execution.types import TradeSide
    from producer.persistence import make_insert_signal

    w = SignalWriter(SignalWriterHooks(insert_signal=make_insert_signal(pool)), "rule3_v1")
    await w.emit_entry(chain_id="timing-c", side=TradeSide.LONG, symbol=SYM, asset_class="crypto",
                       decided_at="2026-10-16T12:03:00+00:00", decision_price=1.0, trigger_level=2,
                       decided_bar_close="2026-10-16T12:06:00+00:00")
    row = await pool.fetchrow("SELECT decided_at, decided_bar_close FROM trade_signals "
                              "WHERE symbol = $1", SYM)
    assert row["decided_at"].isoformat() == "2026-10-16T12:03:00+00:00"
    assert row["decided_bar_close"].isoformat() == "2026-10-16T12:06:00+00:00"
