"""
Regressions for the execution-side review fixes:

  1. stream fills after a restart advance the re-loaded trade
  2. the listener retries a failed signal and holds the persisted cursor
  3. a failed touch read during an EXIT retries instead of HALTing
  4. every escalation rung is more aggressive; the top rung gets its window
  5. HALT cancels the working exit and keeps the trade under management
  6. a cancelled rung's unstreamed fills are netted out of the next rung;
     rejected exits retry the same rung; gated (trading off) exits hold
  7. a trade-key conflict refuses the entry
  8. releasing the kill switch clears the persisted KILL_SWITCH reason
  9. shadow-only strategies fill at decision_price
 10. rule* strategies are shadow-only even when missing from the config set

Driven with the fakes from test_signal_executor / test_executor_hardening.
"""
import logging
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from test_close_requests import CancelAdapter
from test_executor_hardening import positions, shadow_executor
from test_signal_executor import _working, enter_row, exit_row, make_hooks

from execution import client_order_id as coid
from execution import signal_listener
from execution.gated_adapter import GatedAdapter
from execution.pricing import CapConfig, price_marketable_limit
from execution.signal_executor import EscalationRung, ExitCap, OpenTrade, SignalExecutor
from execution.signal_listener import SignalListener
from execution.types import (
    ExitReason, Fill, OrderIntent, OrderResult, OrderStatus, TradeSide,
)

KEY = ("two_ladder", "chainX:L")
TID = "11111111-2222-3333-4444-555555555555"


def _loaded(state="SUBMITTED", filled="0", exit_rung=0, exit_reason=None):
    return OpenTrade(
        trade_id=TID, strategy="two_ladder", position_key="chainX:L", symbol="AAPL",
        asset_class="equity", side=TradeSide.LONG, target_qty=Decimal("10"),
        filled_qty=Decimal(filled), state=state,
        entry_client_order_id=coid.build(TID, OrderIntent.ENTRY, 0),
        exit_rung=exit_rung, exit_reason=exit_reason,
        exit_deadline_ms=None if state != "CLOSING" else 2_000_000,
    )


def _exits(adapter):
    return [o for o in adapter.submitted if o.intent == OrderIntent.EXIT]


async def _booted(adapter=None, hooks=None, **kw):
    adapter = adapter or CancelAdapter()
    if hooks is None:
        hooks, _ = make_hooks()
    ex = SignalExecutor(adapter, hooks, **kw)
    await ex.start()
    return ex, adapter


# --------------------------------------------------------------------
# 1. Restart: loaded trades' order ids are attributable again
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_entry_fill_after_restart_opens_the_loaded_trade_and_exit_sells_it():
    hooks, _ = make_hooks(open_trades=[_loaded()])
    adapter = CancelAdapter()
    ex, _ = await _booted(adapter, hooks)
    entry_cid = coid.build(TID, OrderIntent.ENTRY, 0)
    await adapter._fill_cb(Fill(venue_exec_id="f1", qty=Decimal("10"), price=Decimal("100"),
                                intent=OrderIntent.ENTRY, filled_at_ms=1,
                                venue_order_id="v1", client_order_id=entry_cid))
    trade = ex._open[KEY]
    assert trade.filled_qty == Decimal("10") and trade.state == "OPEN"
    # The EXIT's entry cancel reports the venue-side fill with no fills; the
    # close must not wait for fills the stream already delivered.
    adapter.cancel_results = [OrderResult(client_order_id="x", status=OrderStatus.FILLED,
                                          filled_qty=Decimal("10"))]
    await ex.handle_signal(exit_row())
    assert [o.qty for o in _exits(adapter)] == [Decimal("10")]


@pytest.mark.asyncio
async def test_working_exit_of_a_loaded_closing_trade_is_registered():
    hooks, _ = make_hooks(open_trades=[_loaded(state="CLOSING", filled="10", exit_rung=1,
                                               exit_reason="TARGET")])
    adapter = CancelAdapter()
    adapter.reconcile_result = positions(AAPL=10)       # venue still holds it
    ex, _ = await _booted(adapter, hooks)
    assert ex._open[KEY].state == "CLOSING"
    exit_cid = coid.build(TID, OrderIntent.EXIT, 1)
    assert ex._coid_to_key[exit_cid] == KEY
    await adapter._fill_cb(Fill(venue_exec_id="x1", qty=Decimal("10"), price=Decimal("100"),
                                intent=OrderIntent.EXIT, filled_at_ms=1,
                                client_order_id=exit_cid))
    assert KEY not in ex._open                     # flat -> CLOSED


# --------------------------------------------------------------------
# 2. Listener: failed signals are retried; persisted cursor held
# --------------------------------------------------------------------

def _listener(handler, save):
    return SignalListener(dsn="unused", pool=None, handler=handler,
                          initial_cursor=datetime(2026, 1, 1, tzinfo=timezone.utc),
                          on_cursor_advance=save)


@pytest.mark.asyncio
async def test_failed_signal_is_retried_and_cursor_never_persists_past_it():
    t1 = datetime(2026, 1, 2, tzinfo=timezone.utc)
    t2 = t1 + timedelta(seconds=5)
    fail = {"s1"}
    handled, saved = [], []
    async def handler(row):
        if row["signal_id"] in fail:
            raise ConnectionError("db blip")
        handled.append(row["signal_id"])
    async def save(ts): saved.append(ts)
    lst = _listener(handler, save)
    await lst._dispatch_row({"signal_id": "s1", "created_at": t1})
    await lst._dispatch_row({"signal_id": "s2", "created_at": t2})
    assert handled == ["s2"]
    assert max(saved) <= t1                       # not past the failed s1
    fail.clear()
    lst._next_retry["s1"] = 0                     # its backoff has elapsed
    await lst._maybe_retry_failed()
    assert handled == ["s2", "s1"]
    assert saved[-1] == t2                        # released once s1 succeeds


@pytest.mark.asyncio
async def test_a_failing_signal_backs_off_and_is_reported_stuck_never_dropped():
    async def handler(row):
        raise ConnectionError("db down")
    lst = _listener(handler, None)
    row = {"signal_id": "s1", "created_at": datetime(2026, 1, 2, tzinfo=timezone.utc)}
    await lst._dispatch_row(row)
    delays = []
    for _ in range(signal_listener._STUCK_AFTER + 2):
        delays.append(lst._next_retry["s1"] - time.monotonic())
        lst._next_retry["s1"] = 0
        await lst._maybe_retry_failed()
    # 5 s, 10 s, 20 s ... capped at 5 min.
    assert 4 < delays[0] <= 5 and 9 < delays[1] <= 10 and 19 < delays[2] <= 20
    assert max(delays) <= signal_listener._RETRY_MAX_S
    assert lst.stuck_signals() == 1 and "s1" in lst._failed     # still retried
    # Not due yet: no retry.
    calls = []
    async def counting(row):
        calls.append(row["signal_id"])
        raise ConnectionError("db down")
    lst._handler = counting
    lst._next_retry["s1"] = time.monotonic() + 60
    await lst._maybe_retry_failed()
    assert calls == []


@pytest.mark.asyncio
async def test_catch_up_sweep_starts_a_lag_before_the_cursor():
    seen = []
    class Pool:
        async def fetch(self, sql, since):
            seen.append(since)
            return []
    cursor = datetime(2026, 1, 2, tzinfo=timezone.utc)
    lst = SignalListener(dsn="unused", pool=Pool(), handler=None, initial_cursor=cursor)
    await lst._catch_up()
    assert seen == [cursor - timedelta(seconds=signal_listener._SWEEP_LAG_S)]


# --------------------------------------------------------------------
# 3. A failed touch read on an EXIT retries the rung, never HALTs
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_touch_read_error_on_exit_retries_instead_of_halting():
    hooks, state = make_hooks()
    ex, adapter = await _booted(hooks=hooks)
    await ex.handle_signal(enter_row())
    real_touch = hooks.get_touch
    async def boom(sym, ac): raise ConnectionError("venue 5xx")
    hooks.get_touch = boom
    await ex.handle_signal(exit_row())             # must not raise
    trade = ex._open[KEY]
    assert trade.state == "CLOSING" and not trade.exit_order_submitted
    await ex.run_escalation_once()                 # within the retry window
    assert trade.state == "CLOSING"
    hooks.get_touch = real_touch
    state["now"][0] += 2_001
    await ex.run_escalation_once()
    assert trade.state != "HALTED"
    assert [o.client_order_id for o in _exits(adapter)] == [
        coid.build(trade.trade_id, OrderIntent.EXIT, 0)]


# --------------------------------------------------------------------
# 4. The ladder widens the price; the top rung gets its window
# --------------------------------------------------------------------

def test_each_rung_prices_more_aggressively_with_the_seeded_caps():
    # migration 015's EXIT row: cross 1.5, cap 3.0, 50 bps; ladder 4.0/10s, 6.0/15s.
    cap = ExitCap(base=CapConfig(1.5, 3.0, 50), ladder=(
        EscalationRung(Decimal("4.0"), 10), EscalationRung(Decimal("6.0"), 15)))
    limits = [price_marketable_limit(TradeSide.LONG, OrderIntent.EXIT, Decimal("100"),
                                     Decimal("100.10"), cap.cap_for_rung(r))[0]
              for r in range(3)]
    assert limits[0] > limits[1] > limits[2]       # sell limits step down
    assert limits[2] == Decimal("99.5")            # still bounded by cap_abs_bps
    assert [cap.window_seconds_for_rung(r) for r in range(4)] == [10, 10, 15, None]


def test_a_rung_never_narrows_below_the_base():
    cap = ExitCap(base=CapConfig(2.0, 3.0, None),
                  ladder=(EscalationRung(Decimal("1.0"), 10),))
    c = cap.cap_for_rung(1)
    assert c.cross_spread_mult == Decimal("2.0") and c.cap_spread_mult == Decimal("3.0")


# --------------------------------------------------------------------
# 5. HALT: cancel first, stay under management
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_halted_trade_keeps_its_slot_and_a_later_exit_is_not_acted_on(caplog):
    hooks, state = make_hooks(ladder=(EscalationRung(Decimal("3"), 30),))
    ex, adapter = await _booted(hooks=hooks, max_open_positions=1)
    await ex.handle_signal(enter_row())
    adapter.queue_results(_working("exit0"), _working("exit1"))
    await ex.handle_signal(exit_row())
    trade = ex._open[KEY]
    for _ in range(2):
        state["now"][0] += 31_000
        await ex.run_escalation_once()
    assert trade.state == "HALTED"
    assert adapter.cancels[-1] == coid.build(trade.trade_id, OrderIntent.EXIT, 1)
    n = len(adapter.submitted)
    with caplog.at_level(logging.WARNING, logger="executor.signal"):
        await ex.handle_signal({**exit_row(), "signal_id": "again", "exit_reason": "STOP"})
    assert len(adapter.submitted) == n
    assert any("HALTED" in r.getMessage() for r in caplog.records)
    await ex.handle_signal(enter_row(chain="chainY"))     # cap of 1 is held
    assert len(adapter.submitted) == n


@pytest.mark.asyncio
async def test_halt_waits_for_a_confirmed_cancel():
    hooks, state = make_hooks(ladder=())
    ex, adapter = await _booted(hooks=hooks)
    await ex.handle_signal(enter_row())
    adapter.queue_results(_working("exit0"))
    await ex.handle_signal(exit_row())
    trade = ex._open[KEY]
    adapter.cancel_raises = True
    await ex.run_escalation_once()
    assert trade.state == "CLOSING"               # order may still work: no HALT
    adapter.cancel_raises = False
    state["now"][0] += 2_001
    await ex.run_escalation_once()
    assert trade.state == "HALTED"


# --------------------------------------------------------------------
# 6. Partial fills, rejections, trading off
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_next_rung_is_sized_net_of_fills_the_cancel_reported():
    hooks, state = make_hooks()
    ex, adapter = await _booted(hooks=hooks)
    await ex.handle_signal(enter_row())                          # holds 10
    adapter.queue_results(_working("exit0"), _working("exit1"))
    await ex.handle_signal(exit_row())
    trade = ex._open[KEY]
    # Rung 0 sold 4 at the venue; the stream has not delivered it yet.
    adapter.cancel_results = [OrderResult(client_order_id="x", status=OrderStatus.CANCELLED,
                                          filled_qty=Decimal("4"))]
    state["now"][0] += 31_000
    await ex.run_escalation_once()
    assert [o.qty for o in _exits(adapter)] == [Decimal("10"), Decimal("6")]
    rung0 = coid.build(trade.trade_id, OrderIntent.EXIT, 0)
    rung1 = coid.build(trade.trade_id, OrderIntent.EXIT, 1)
    for eid, cid, q in (("s4", rung0, "4"), ("s6", rung1, "6")):
        await adapter._fill_cb(Fill(venue_exec_id=eid, qty=Decimal(q), price=Decimal("100"),
                                    intent=OrderIntent.EXIT, filled_at_ms=1,
                                    client_order_id=cid))
    assert KEY not in ex._open and trade.state == "CLOSED"


@pytest.mark.asyncio
async def test_rejected_exit_retries_the_same_rung():
    hooks, state = make_hooks()
    ex, adapter = await _booted(hooks=hooks)
    await ex.handle_signal(enter_row())
    adapter.queue_results(
        OrderResult(client_order_id="x", status=OrderStatus.REJECTED,
                    reject_reason="INSUFFICIENT_FUND"),
        _working("exit0"))
    await ex.handle_signal(exit_row())
    trade = ex._open[KEY]
    assert not trade.exit_order_submitted
    state["now"][0] += 2_001
    await ex.run_escalation_once()
    rung0 = coid.build(trade.trade_id, OrderIntent.EXIT, 0)
    assert [o.client_order_id for o in _exits(adapter)] == [rung0, rung0]
    assert trade.exit_order_submitted and trade.exit_rung == 0


@pytest.mark.asyncio
async def test_exit_rejected_past_the_retries_escalates_instead_of_retrying_forever():
    hooks, state = make_hooks()
    ex, adapter = await _booted(hooks=hooks)
    await ex.handle_signal(enter_row())
    rejected = lambda: OrderResult(client_order_id="x", status=OrderStatus.REJECTED,
                                   reject_reason="INSUFFICIENT_FUND")
    adapter.queue_results(*[rejected() for _ in range(4)], _working("exit1"))
    await ex.handle_signal(exit_row())                       # reject 1
    trade = ex._open[KEY]
    for _ in range(3):                                       # rejects 2, 3, 4
        state["now"][0] += 2_001
        await ex.run_escalation_once()
    rung0 = coid.build(trade.trade_id, OrderIntent.EXIT, 0)
    rung1 = coid.build(trade.trade_id, OrderIntent.EXIT, 1)
    assert [o.client_order_id for o in _exits(adapter)] == [rung0] * 4 + [rung1]
    assert trade.exit_rung == 1 and trade.exit_order_submitted


class _Mode:
    def __init__(self): self.on = True
    def is_enabled(self): return self.on


@pytest.mark.asyncio
async def test_exits_hold_while_trading_is_off_and_never_halt():
    mode = _Mode()
    inner = CancelAdapter()
    gated = GatedAdapter(inner, mode)
    hooks, state = make_hooks()
    ex, _ = await _booted(gated, hooks)
    await ex.handle_signal(enter_row())
    inner.queue_results(_working("exit0"))
    await ex.handle_signal(exit_row())                     # rung 0 working
    trade = ex._open[KEY]
    mode.on = False
    for _ in range(40):                                    # far past every window
        state["now"][0] += 5_000
        await ex.run_escalation_once()
    assert trade.state == "CLOSING" and trade.exit_rung == 0
    assert inner.cancels == []                             # nothing reached the venue
    mode.on = True
    inner.queue_results(_working("exit1"))
    state["now"][0] += 5_000
    await ex.run_escalation_once()
    assert trade.exit_rung == 1 and trade.exit_order_submitted


# --------------------------------------------------------------------
# 7. A trade-key conflict refuses the entry
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_entry_refused_when_the_trade_row_is_held_by_another_trade():
    hooks, _ = make_hooks()
    async def conflict(t): return False
    hooks.persist_trade = conflict
    ex, adapter = await _booted(hooks=hooks)
    await ex.handle_signal(enter_row())
    assert adapter.submitted == [] and KEY not in ex._open


# --------------------------------------------------------------------
# 8. Kill switch release clears the persisted reason
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_released_flatten_is_not_resumed_after_a_restart():
    hooks, state = make_hooks()
    switch = {"on": False}
    async def flatten(): return switch["on"]
    hooks.kill_switch_flatten = flatten
    ex, adapter = await _booted(hooks=hooks)
    adapter.set_next_result(_working("e0"))
    await ex.handle_signal(enter_row())                     # entry still working
    trade = ex._open[KEY]
    adapter.cancel_results = [OrderResult(client_order_id="x", status=OrderStatus.WORKING)]
    switch["on"] = True
    await ex.run_close_requests_once()                      # cancel unconfirmed: queued
    assert trade.exit_reason == ExitReason.KILL_SWITCH.value
    switch["on"] = False
    await ex.run_close_requests_once()
    assert trade.exit_reason is None
    assert state["trades"][trade.trade_id].exit_reason is None
    hooks2, _ = make_hooks(open_trades=[trade])
    ex2, _ = await _booted(hooks=hooks2)
    assert ex2._close_requests == {}


# --------------------------------------------------------------------
# 9 + 10. Shadow-only strategies
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_shadow_only_strategy_fills_at_decision_price():
    ex, adapter, store = await shadow_executor(shadow_only_strategies=frozenset({"two_ladder"}))
    await ex.handle_signal(enter_row(direction="LONG"))      # decision_price 100.0
    await ex.handle_signal(exit_row(direction="LONG"))       # decision_price 101.0
    row = store.rows[KEY]
    assert row["entry_px"] == Decimal("100.0") and row["exit_px"] == Decimal("101.0")
    assert adapter.submitted == []


@pytest.mark.asyncio
async def test_shadow_only_falls_back_to_the_touch_without_decision_price():
    ex, _, store = await shadow_executor(shadow_only_strategies=frozenset({"two_ladder"}))
    await ex.handle_signal({**enter_row(direction="LONG"), "decision_price": None})
    assert store.rows[KEY]["entry_px"] == Decimal("100.10")  # the ask


@pytest.mark.asyncio
async def test_short_on_a_venue_strategy_still_fills_at_the_touch():
    ex, _, store = await shadow_executor()
    await ex.handle_signal(enter_row(direction="SHORT"))
    assert store.rows[("two_ladder", "chainX:S")]["entry_px"] == Decimal("100")   # the bid


@pytest.mark.asyncio
async def test_rule_strategy_missing_from_the_set_is_still_shadow_only(caplog):
    ex, adapter, store = await shadow_executor(shadow_only_strategies=frozenset())
    with caplog.at_level(logging.ERROR, logger="executor.signal"):
        await ex.handle_signal(enter_row(strategy="rule9_v1", direction="LONG"))
        await ex.handle_signal(enter_row(strategy="rule9_v1", chain="chainY", direction="LONG"))
    assert adapter.submitted == []
    assert ("rule9_v1", "chainX:L") in store.rows
    assert sum("rule9_v1" in r.getMessage() for r in caplog.records
               if r.levelno == logging.ERROR) == 1


def test_launcher_refuses_a_config_without_shadow_only_strategies():
    import types
    import run_executor
    with pytest.raises(RuntimeError):
        run_executor.shadow_only_strategies(types.SimpleNamespace())
    assert run_executor.shadow_only_strategies(
        types.SimpleNamespace(SHADOW_ONLY_STRATEGIES={"rule3_v1"})) == frozenset({"rule3_v1"})
