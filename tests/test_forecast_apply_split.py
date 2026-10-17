"""
The forecast/apply split (ServiceTwoChainMachine.prepare / forecast / apply +
SymbolWorkerPool's stager/applier pipeline) lets one symbol's Sumtyme calls
overlap. These tests pin the invariants that make that safe:

  1. Buffer appends happen in arrival order, and each forecast's input is the
     buffer EXACTLY as it stood at that bar's append (the snapshot), even when
     later bars have been appended by the time the call runs.
  2. _latest_signals writes and the state-machine sweep happen in ARRIVAL
     order regardless of forecast COMPLETION order (the FIFO of futures).
  3. A skipped bar (SDK error / timeout / empty) retires its offset and leaves
     _latest_signals untouched.
  4. Two ladders sharing a timeframe make ONE API call per window (memo
     owner/join), and a failed owner call is NOT shared.
  5. The per-symbol in-flight cap bounds one symbol's concurrent forecasts.
"""
from __future__ import annotations

import asyncio
import threading
import time
from collections import deque

import pytest

from producer.bar import OHLCPoint
from producer.fanout_router_multi import MultiLadderRouter
from producer.forecast_gate import ForecastGate, ForecastMemo
from producer.service2_machine import WINDOW_SIZE, ServiceTwoChainMachine
from producer.symbol_workers import SymbolWorkerPool
from strategy.ladder_registry import LadderRegistry

LADDER = ["1m", "3m", "15m"]
SYM = "BTC-USD"


def _returns(value):
    async def _coro():
        return value
    return _coro()


def bar(ts, tf="1m", close=100.0, symbol=SYM):
    return OHLCPoint(symbol=symbol, timestamp=ts, open=close, high=close,
                     low=close, close=close, timeframe=tf, asset_class="crypto")


def _fill(m, tf, n=WINDOW_SIZE, step_ms=60_000):
    m._buffers[tf] = deque(
        ({"timestamp": i * step_ms, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0}
         for i in range(n)),
        maxlen=WINDOW_SIZE)
    return (n - 1) * step_ms


def _machine(forecast_fn, *, ladder_name="FC", memo=None, gate=None,
             persist_log=None, timeout_s=5.0):
    """A real ServiceTwoChainMachine with the frame helpers stubbed so that
    `data_input` handed to forecast_fn is (last_ts, n_rows) of the SNAPSHOT —
    which is exactly what the snapshot invariant needs to observe."""
    async def _noop(*a, **k):
        return None

    async def persist_signal(symbol, tf, placeholder_dt, sig, ac, price):
        if persist_log is not None:
            persist_log.append((tf, placeholder_dt, sig))

    return ServiceTwoChainMachine(
        symbol=SYM, asset_class="crypto", ladder=LADDER, ladder_name=ladder_name,
        forecast_fn=forecast_fn,
        build_df_fn=lambda snap: (snap[-1]["timestamp"], len(snap)),
        prepare_df_fn=lambda df, tf: (df, df[0]),   # api_df=(last_ts, n), placeholder_dt=last_ts
        to_interval_fn=lambda tf: (1, "minutes"),
        persist_signal=persist_signal, persist_chain_init=_noop,
        persist_chain_propagate=_noop, persist_chain_terminate=_noop,
        load_active_chains=lambda: _returns([]),
        load_last_base_signal=lambda: _returns(None),
        forecast_timeout_s=timeout_s,
        forecast_gate=gate, forecast_memo=memo,
    )


def _router(machines: dict, publish=None):
    """Router over a fixed set of (ladder_name -> machine) for SYM."""
    reg = LadderRegistry()
    for name in machines:
        reg.register(name, LADDER)
    published = []

    async def _publish(raw):
        published.append(raw)
        if publish:
            await publish(raw)

    router = MultiLadderRouter(
        registry=reg,
        make_machine=lambda s, ln, tfs: machines[ln],
        publish=_publish,
    )
    return router, published


async def _run_pool(router, bars, *, max_inflight=4):
    pool = SymbolWorkerPool(
        prepare=router.prepare, forecast=router.forecast, apply=router.apply,
        max_queue_per_symbol=100, max_inflight_per_symbol=max_inflight,
    )
    for off, b in enumerate(bars):
        await pool.submit(b.symbol, b, off)
    await pool.drain(timeout=10)
    done = pool.take_done_offsets()
    await pool.stop(drain_timeout=1)
    return done


# ---------------------------------------------------------------------------
# 1 + 2: snapshot correctness and apply order under scrambled completion
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_apply_order_is_arrival_order_when_forecasts_complete_out_of_order():
    # Forecast latency DECREASES with bar timestamp, so later bars finish FIRST.
    calls, completions, persisted = [], [], []
    lock = threading.Lock()

    def forecast(**kw):
        last_ts, n = kw["data_input"]
        with lock:
            calls.append((last_ts, n))
        k = len(calls)
        time.sleep(0.25 - 0.05 * k)      # 5 bars: 0.20, 0.15, 0.10, 0.05, 0.00
        with lock:
            completions.append(last_ts)
        return {"5001": 1}

    gate = ForecastGate(concurrency=8)
    m = _machine(forecast, gate=gate, persist_log=persisted)
    newest = _fill(m, "1m")
    router, _ = _router({"FC": m})

    bars = [bar(newest + 60_000 * i) for i in range(1, 6)]
    done = await _run_pool(router, bars)

    arrival = [b.timestamp for b in bars]
    # Forecasts genuinely finished out of order (the scenario under test).
    assert completions != arrival and sorted(completions) == arrival
    # Each forecast saw the window as of ITS append: last row == its own bar,
    # and the window was full-size — even though later bars were appended
    # while it waited.
    assert sorted(calls) == [(ts, WINDOW_SIZE) for ts in arrival]
    # Applied (persist_signal runs in apply) strictly in arrival order.
    assert [p[1] for p in persisted] == arrival
    # The cache ends on the newest bar, and the buffer is in arrival order.
    assert m._latest_signals[0]["timestamp"].startswith("1970")  # sanity: epoch-based ts
    assert [t["timestamp"] for t in m._buffers["1m"]][-5:] == arrival
    # Every bar retired exactly once.
    assert sorted(done) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_pipelined_events_equal_serial_events_under_scrambled_completion():
    """THE equivalence check. The same bars, (a) through the pipelined pool
    with forecast latencies arranged so completions are scrambled, and (b)
    through the serial on_bar path, must yield the IDENTICAL event sequence:
    same types, directions, chain ids, timestamps, in the same order."""
    def make_forecast(scramble: bool):
        lock = threading.Lock()
        n = [0]

        def forecast(**kw):
            last_ts, _ = kw["data_input"]
            with lock:
                n[0] += 1
                i = n[0]
            if scramble:
                time.sleep([0.15, 0.0, 0.1, 0.02, 0.07][i % 5])
            # Direction depends only on the bar, never on call order.
            k = last_ts // 60_000
            return {"5001": 1 if k % 3 else -1}
        return forecast

    def events_of(published):
        return [(e["event_type"], e["direction"], e["chain_id"], e["timestamp"],
                 e["propagation_level"]) for e in published]

    # (a) pipelined, scrambled
    m1 = _machine(make_forecast(True), gate=ForecastGate(concurrency=8))
    newest = _fill(m1, "1m")
    r1, pub1 = _router({"FC": m1})
    bars = [bar(newest + 60_000 * i) for i in range(1, 13)]
    await _run_pool(r1, bars, max_inflight=8)

    # (b) serial reference
    m2 = _machine(make_forecast(False))
    _fill(m2, "1m")
    r2, pub2 = _router({"FC": m2})
    for b in bars:
        await r2.on_bar(b)

    assert events_of(pub1) == events_of(pub2)
    assert len(pub2) > 0
    assert m1._latest_signals == m2._latest_signals
    assert m1._active_chains.keys() == m2._active_chains.keys()


# ---------------------------------------------------------------------------
# 3: a skipped bar retires its offset and leaves the cache alone
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_skipped_bar_retires_offset_and_does_not_touch_cache():
    def forecast(**kw):
        last_ts, _ = kw["data_input"]
        if last_ts % 120_000 == 0:
            raise RuntimeError("sdk boom")        # every even bar fails
        return {"5001": 1}

    m = _machine(forecast, gate=ForecastGate(concurrency=4))
    newest = _fill(m, "1m")
    router, _ = _router({"FC": m})

    bars = [bar(newest + 60_000 * i) for i in range(1, 5)]   # alternate ok / fail
    done = await _run_pool(router, bars)

    assert sorted(done) == [0, 1, 2, 3]                       # all retired, incl. failures
    # The cache holds the last SUCCESSFUL bar; the failed ones left it alone.
    from producer.service2_machine import _epoch_ms_to_iso
    ok = [b for b in bars if b.timestamp % 120_000 != 0]
    failed = [b for b in bars if b.timestamp % 120_000 == 0]
    assert ok and failed
    assert m._latest_signals[0]["timestamp"] == _epoch_ms_to_iso(ok[-1].timestamp)
    assert [t["timestamp"] for t in m._buffers["1m"]][-4:] == [b.timestamp for b in bars]


@pytest.mark.asyncio
async def test_timeout_is_a_skip_not_a_crash():
    def slow(**kw):
        time.sleep(0.3)
        return {"5001": 1}

    m = _machine(slow, gate=ForecastGate(concurrency=2), timeout_s=0.05)
    newest = _fill(m, "1m")
    p = m.prepare(bar(newest + 60_000))
    assert p is not None
    await m.forecast(p)
    assert p.forecast is None
    assert await m.apply(p) == []
    assert 0 not in m._latest_signals


# ---------------------------------------------------------------------------
# 4: memo — one call per window across concurrent ladders; failures not shared
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_two_ladders_sharing_a_timeframe_make_one_api_call():
    n_calls = 0

    def forecast(**kw):
        nonlocal n_calls
        n_calls += 1
        time.sleep(0.05)
        return {"5001": 1}

    memo = ForecastMemo()
    gate = ForecastGate(concurrency=4)
    a = _machine(forecast, ladder_name="FC", memo=memo, gate=gate)
    b = _machine(forecast, ladder_name="XX", memo=memo, gate=gate)
    newest = _fill(a, "1m"); _fill(b, "1m")
    router, _ = _router({"FC": a, "XX": b})

    routed = await router.prepare(bar(newest + 60_000))
    assert routed is not None and len(routed.items) == 2
    await router.forecast(routed)               # concurrent: owner + joiner
    assert n_calls == 1
    pendings = [p for (_, _, p) in routed.items]
    assert all(p.forecast == {"5001": 1} for p in pendings)
    assert [p.memo_hit for p in pendings] == [False, True]


@pytest.mark.asyncio
async def test_failed_owner_call_is_not_shared_with_joiner():
    attempts = []

    def forecast(**kw):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("first call fails")
        return {"5001": -1}

    memo = ForecastMemo()
    gate = ForecastGate(concurrency=4)
    a = _machine(forecast, ladder_name="FC", memo=memo, gate=gate)
    b = _machine(forecast, ladder_name="XX", memo=memo, gate=gate)
    newest = _fill(a, "1m"); _fill(b, "1m")
    router, _ = _router({"FC": a, "XX": b})

    routed = await router.prepare(bar(newest + 60_000))
    await router.forecast(routed)
    pa, pb = [p for (_, _, p) in routed.items]
    assert pa.forecast is None                  # owner failed, skipped
    assert pb.forecast == {"5001": -1}          # joiner retried on its own
    assert len(attempts) == 2


@pytest.mark.asyncio
async def test_memo_owner_settles_future_on_cancellation():
    memo = ForecastMemo()
    key = ("crypto", SYM, "1m", 0, 60_000, 5000)
    claim = memo.claim(key)
    assert claim.owner
    joiner = memo.claim(key)
    assert not joiner.owner and joiner.future is claim.future
    memo.fail(key)                              # what forecast()'s finally does
    assert joiner.future.done() and joiner.future.result() is None
    assert memo.claim(key).owner                # slot is clean again


# ---------------------------------------------------------------------------
# 5: per-symbol in-flight cap
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_per_symbol_inflight_cap_bounds_concurrency():
    inflight = 0
    peak = 0
    lock = threading.Lock()

    def forecast(**kw):
        nonlocal inflight, peak
        with lock:
            inflight += 1
            peak = max(peak, inflight)
        time.sleep(0.05)
        with lock:
            inflight -= 1
        return {"5001": 0}

    m = _machine(forecast, gate=ForecastGate(concurrency=16))
    newest = _fill(m, "1m")
    router, _ = _router({"FC": m})
    bars = [bar(newest + 60_000 * i) for i in range(1, 9)]
    await _run_pool(router, bars, max_inflight=2)
    assert peak == 2


# ---------------------------------------------------------------------------
# The serial on_bar path is unchanged in meaning
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_on_bar_composes_the_three_stages():
    seen = []

    def forecast(**kw):
        seen.append(kw["data_input"])
        return {"5001": 1}

    m = _machine(forecast)
    newest = _fill(m, "1m")
    events = await m.on_bar(bar(newest + 60_000))
    assert seen == [(newest + 60_000, WINDOW_SIZE)]
    assert [e.event_type for e in events] == ["chain_initiated"]
    assert m._latest_signals[0]["signal"] == 1
