"""
Final closes that must settle the entry first:
  * the kill switch's 'flatten' mode closes every open position (reason
    KILL_SWITCH);
  * a normal EXIT on a trade whose entry is still working cancels the entry
    before selling (before this, the trade was marked CLOSED while its buy
    kept working).
Both go through SignalExecutor._request_close and the per-tick retry in
run_close_requests_once. Driven with the fakes from test_signal_executor /
test_executor_hardening.
"""
import asyncio
from decimal import Decimal

import pytest

from test_executor_hardening import SpotAdapter, db_trade, positions
from test_signal_executor import enter_row, exit_row, make_hooks

from execution.gated_adapter import GATED_PREFIX
from execution.signal_executor import OpenTrade, SignalExecutor
from execution.types import ExitReason, Fill, OrderIntent, OrderResult, OrderStatus, TradeSide


class CancelAdapter(SpotAdapter):
    """SpotAdapter that records cancels and returns scripted cancel results."""
    def __init__(self, **kw):
        super().__init__(**kw)
        self.cancels = []
        self.cancel_results = []      # popped in order; default CANCELLED, 0 filled
        self.cancel_raises = False

    async def cancel_order(self, cid):
        self.cancels.append(cid)
        if self.cancel_raises:
            raise ConnectionError("venue down")
        if self.cancel_results:
            r = self.cancel_results.pop(0)
            return OrderResult(client_order_id=cid, status=r.status,
                               filled_qty=r.filled_qty, fills=r.fills,
                               reject_reason=r.reject_reason)
        return OrderResult(client_order_id=cid, status=OrderStatus.CANCELLED)


def _result(status, filled="0", fills=()):
    return OrderResult(client_order_id="x", status=status,
                       filled_qty=Decimal(filled), fills=list(fills))


def _fill(eid, qty, intent=OrderIntent.ENTRY, cid=None):
    return Fill(venue_exec_id=eid, qty=Decimal(qty), price=Decimal("100.05"),
                intent=intent, filled_at_ms=1, client_order_id=cid)


async def _executor(adapter=None, hooks=None, state=None, flatten=None):
    adapter = adapter or CancelAdapter()
    if hooks is None:
        hooks, state = make_hooks()
    switch = {"on": False} if flatten is None else flatten
    async def kill_switch_flatten():
        return switch["on"]
    hooks.kill_switch_flatten = kill_switch_flatten
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    return ex, adapter, state, switch


def _trade(ex):
    (trade,) = ex._open.values()
    return trade


def _exits(adapter):
    return [o for o in adapter.submitted if o.intent == OrderIntent.EXIT]


# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_does_nothing_while_switch_is_not_in_flatten_mode():
    ex, adapter, _, _ = await _executor()
    await ex.handle_signal(enter_row())
    await ex.run_close_requests_once()
    assert _trade(ex).state == "OPEN"
    assert _exits(adapter) == [] and adapter.cancels == []


@pytest.mark.asyncio
async def test_no_hook_means_no_flatten():
    hooks, _ = make_hooks()
    adapter = CancelAdapter()
    ex = SignalExecutor(adapter, hooks)       # kill_switch_flatten left None
    await ex.start()
    await ex.handle_signal(enter_row())
    await ex.run_close_requests_once()
    assert _trade(ex).state == "OPEN"


@pytest.mark.asyncio
async def test_filled_position_is_closed_with_kill_switch_reason():
    ex, adapter, state, switch = await _executor()
    await ex.handle_signal(enter_row())
    trade_id = _trade(ex).trade_id
    switch["on"] = True
    await ex.run_close_requests_once()
    (exit_order,) = _exits(adapter)
    assert exit_order.side.value == "LONG" and exit_order.qty == Decimal("10")
    assert adapter.cancels == []                  # entry was complete: nothing to cancel
    assert ex._open == {}                         # exit filled synchronously
    assert state["states"][trade_id][0] == "CLOSED"
    assert state["trades"][trade_id].exit_reason == ExitReason.KILL_SWITCH.value


@pytest.mark.asyncio
async def test_working_entry_with_no_fill_is_cancelled_and_closed_with_nothing_to_sell():
    ex, adapter, state, switch = await _executor()
    adapter.set_next_result(_result(OrderStatus.WORKING))
    await ex.handle_signal(enter_row())
    trade = _trade(ex)
    assert trade.state == "SUBMITTED"
    switch["on"] = True
    await ex.run_close_requests_once()
    assert adapter.cancels == [trade.entry_client_order_id]
    assert _exits(adapter) == []
    assert ex._open == {}
    assert state["states"][trade.trade_id][0] == "CLOSED"


@pytest.mark.asyncio
async def test_partial_entry_cancels_the_rest_then_sells_what_was_bought():
    ex, adapter, _, switch = await _executor()
    adapter.set_next_result(_result(OrderStatus.PARTIALLY_FILLED, "4",
                                    [_fill("e1", "4")]))
    await ex.handle_signal(enter_row())
    trade = _trade(ex)
    assert trade.state == "OPEN" and trade.filled_qty == Decimal("4")
    switch["on"] = True
    adapter.cancel_results = [_result(OrderStatus.CANCELLED, "4")]
    await ex.run_close_requests_once()
    assert adapter.cancels == [trade.entry_client_order_id]
    (exit_order,) = _exits(adapter)
    assert exit_order.qty == Decimal("4")
    assert ex._open == {}


@pytest.mark.asyncio
async def test_close_waits_for_entry_fills_the_cancel_reported_but_the_stream_has_not():
    # The venue says 6 of the entry filled before the cancel; only 4 are
    # recorded. Selling 4 now would leave 2 unmanaged.
    ex, adapter, _, switch = await _executor()
    adapter.set_next_result(_result(OrderStatus.PARTIALLY_FILLED, "4",
                                    [_fill("e1", "4")]))
    await ex.handle_signal(enter_row())
    trade = _trade(ex)
    switch["on"] = True
    adapter.cancel_results = [_result(OrderStatus.CANCELLED, "6")]
    await ex.run_close_requests_once()
    assert _exits(adapter) == []
    await ex.run_close_requests_once()                   # still waiting; no re-cancel
    assert _exits(adapter) == [] and len(adapter.cancels) == 1
    await ex._on_fill(_fill("e2", "2", cid=trade.entry_client_order_id))
    await ex.run_close_requests_once()
    (exit_order,) = _exits(adapter)
    assert exit_order.qty == Decimal("6")


@pytest.mark.asyncio
async def test_unconfirmed_cancel_does_not_sell_and_retries():
    ex, adapter, _, switch = await _executor()
    adapter.set_next_result(_result(OrderStatus.WORKING))
    await ex.handle_signal(enter_row())
    switch["on"] = True
    adapter.cancel_results = [_result(OrderStatus.WORKING)]
    await ex.run_close_requests_once()
    assert _trade(ex).state == "SUBMITTED" and _exits(adapter) == []
    await ex.run_close_requests_once()                   # default: cancel confirmed
    assert len(adapter.cancels) == 2 and ex._open == {}


@pytest.mark.asyncio
async def test_cancel_that_raises_is_retried_next_pass():
    ex, adapter, _, switch = await _executor()
    adapter.set_next_result(_result(OrderStatus.WORKING))
    await ex.handle_signal(enter_row())
    switch["on"] = True
    adapter.cancel_raises = True
    await ex.run_close_requests_once()
    assert _trade(ex).state == "SUBMITTED"
    adapter.cancel_raises = False
    await ex.run_close_requests_once()
    assert ex._open == {}


@pytest.mark.asyncio
async def test_gated_cancel_is_not_taken_as_cancelled():
    # Trading off: the cancel never reached the venue, so the entry may still
    # be working. The trade must not be finalized as flat.
    ex, adapter, _, switch = await _executor()
    adapter.set_next_result(_result(OrderStatus.WORKING))
    await ex.handle_signal(enter_row())
    switch["on"] = True
    adapter.cancel_results = [OrderResult(client_order_id="x", status=OrderStatus.CANCELLED,
                                          reject_reason=f"{GATED_PREFIX}trade_disabled")]
    await ex.run_close_requests_once()
    assert _trade(ex).state == "SUBMITTED"


@pytest.mark.asyncio
async def test_final_exit_already_working_is_left_to_the_escalation_driver():
    ex, adapter, _, switch = await _executor()
    await ex.handle_signal(enter_row())
    adapter.set_next_result(_result(OrderStatus.WORKING))
    await ex.handle_signal(exit_row(reason="STOP"))
    assert _trade(ex).state == "CLOSING"
    n = len(adapter.submitted)
    switch["on"] = True
    await ex.run_close_requests_once()
    assert len(adapter.submitted) == n and adapter.cancels == []
    assert _trade(ex).exit_reason == "STOP"


@pytest.mark.asyncio
async def test_halted_trade_is_left_for_a_human():
    adapter = CancelAdapter()
    adapter.reconcile_result = positions(AAPL=10)
    hooks, state = make_hooks(open_trades=[db_trade("t1", state="HALTED")])
    ex, adapter, _, switch = await _executor(adapter, hooks, state)
    switch["on"] = True
    await ex.run_close_requests_once()
    assert adapter.submitted == [] and adapter.cancels == []


@pytest.mark.asyncio
async def test_flattens_every_symbol():
    ex, adapter, _, switch = await _executor()
    await ex.handle_signal(enter_row(chain="c1", symbol="AAPL"))
    await ex.handle_signal(enter_row(chain="c2", symbol="MSFT"))
    switch["on"] = True
    await ex.run_close_requests_once()
    assert sorted(o.symbol for o in _exits(adapter)) == ["AAPL", "MSFT"]
    assert ex._open == {}


@pytest.mark.asyncio
async def test_unfilled_flatten_exit_escalates_like_any_exit():
    ex, adapter, state, switch = await _executor()
    await ex.handle_signal(enter_row())
    switch["on"] = True
    adapter.set_next_result(_result(OrderStatus.WORKING))
    await ex.run_close_requests_once()
    trade = _trade(ex)
    assert trade.state == "CLOSING" and trade.exit_rung == 0
    await ex.run_close_requests_once()                   # already closing for good: no-op
    assert len(_exits(adapter)) == 1
    state["now"][0] = trade.exit_deadline_ms + 1
    await ex.run_escalation_once()                # rung 0 expired -> re-issue wider
    assert len(_exits(adapter)) == 2 and ex._open == {}


@pytest.mark.asyncio
async def test_escalation_loop_drives_flatten():
    ex, adapter, _, switch = await _executor()
    await ex.handle_signal(enter_row())
    switch["on"] = True
    task = asyncio.create_task(ex.run_escalation_loop(tick_seconds=0.01))
    for _ in range(100):
        if not ex._open:
            break
        await asyncio.sleep(0.01)
    ex.stop()
    await task
    assert ex._open == {} and len(_exits(adapter)) == 1


@pytest.mark.asyncio
async def test_entries_are_still_blocked_while_flattening():
    hooks, _ = make_hooks(kill=True)
    ex, adapter, _, switch = await _executor(hooks=hooks)
    switch["on"] = True
    await ex.handle_signal(enter_row())
    assert adapter.submitted == []


# --------------------------------------------------------------------
# A normal EXIT also cancels a still-working entry first
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_exit_on_unfilled_working_entry_cancels_it():
    # The bug: the trade was marked CLOSED while its buy order kept working,
    # so a late fill would have been an unmanaged position.
    ex, adapter, state, _ = await _executor()
    adapter.set_next_result(_result(OrderStatus.WORKING))
    await ex.handle_signal(enter_row())
    trade = _trade(ex)
    await ex.handle_signal(exit_row(reason="STOP"))
    assert adapter.cancels == [trade.entry_client_order_id]
    assert _exits(adapter) == [] and ex._open == {}
    assert state["states"][trade.trade_id][0] == "CLOSED"
    assert state["trades"][trade.trade_id].exit_reason == "STOP"


@pytest.mark.asyncio
async def test_exit_on_partial_entry_cancels_the_rest_and_sells_what_was_bought():
    ex, adapter, _, _ = await _executor()
    adapter.set_next_result(_result(OrderStatus.PARTIALLY_FILLED, "4", [_fill("e1", "4")]))
    await ex.handle_signal(enter_row())
    adapter.cancel_results = [_result(OrderStatus.CANCELLED, "4")]
    await ex.handle_signal(exit_row(reason="TARGET"))
    assert len(adapter.cancels) == 1
    (exit_order,) = _exits(adapter)
    assert exit_order.qty == Decimal("4") and ex._open == {}


@pytest.mark.asyncio
async def test_exit_on_fully_filled_entry_makes_no_cancel_call():
    ex, adapter, _, _ = await _executor()
    await ex.handle_signal(enter_row())
    await ex.handle_signal(exit_row())
    assert adapter.cancels == [] and len(_exits(adapter)) == 1 and ex._open == {}


@pytest.mark.asyncio
async def test_exit_with_unconfirmed_entry_cancel_is_retried_by_the_loop():
    ex, adapter, _, _ = await _executor()
    adapter.set_next_result(_result(OrderStatus.WORKING))
    await ex.handle_signal(enter_row())
    adapter.cancel_results = [_result(OrderStatus.WORKING)]
    await ex.handle_signal(exit_row(reason="STOP"))
    assert _trade(ex).state == "SUBMITTED" and _exits(adapter) == []
    await ex.run_close_requests_once()            # default: cancel confirmed
    assert len(adapter.cancels) == 2 and ex._open == {}


@pytest.mark.asyncio
async def test_exit_waits_for_entry_fills_still_on_the_stream():
    ex, adapter, _, _ = await _executor()
    adapter.set_next_result(_result(OrderStatus.PARTIALLY_FILLED, "4", [_fill("e1", "4")]))
    await ex.handle_signal(enter_row())
    trade = _trade(ex)
    adapter.cancel_results = [_result(OrderStatus.CANCELLED, "6")]
    await ex.handle_signal(exit_row(reason="STOP"))
    assert _exits(adapter) == []
    await ex._on_fill(_fill("e2", "2", cid=trade.entry_client_order_id))
    await ex.run_close_requests_once()
    (exit_order,) = _exits(adapter)
    assert exit_order.qty == Decimal("6") and len(adapter.cancels) == 1


@pytest.mark.asyncio
async def test_duplicate_exit_while_waiting_does_not_cancel_twice():
    ex, adapter, _, _ = await _executor()
    adapter.set_next_result(_result(OrderStatus.PARTIALLY_FILLED, "4", [_fill("e1", "4")]))
    await ex.handle_signal(enter_row())
    adapter.cancel_results = [_result(OrderStatus.CANCELLED, "6")]
    await ex.handle_signal(exit_row(reason="STOP"))
    await ex.handle_signal(exit_row(reason="STOP"))
    assert len(adapter.cancels) == 1 and _exits(adapter) == []


@pytest.mark.asyncio
async def test_requested_close_survives_a_restart():
    # Crash after the EXIT was recorded but before the entry cancel settled:
    # the trade comes back SUBMITTED with exit_reason STOP.
    t = OpenTrade(trade_id="t1", strategy="s", position_key="c1:L", symbol="AAPL",
                  asset_class="equity", side=TradeSide.LONG, target_qty=Decimal("10"),
                  filled_qty=Decimal("0"), state="SUBMITTED",
                  entry_client_order_id="e-t1", exit_reason="STOP")
    hooks, state = make_hooks(open_trades=[t])
    ex, adapter, _, _ = await _executor(hooks=hooks, state=state)
    assert ex._close_requests == {("s", "c1:L"): "STOP"}
    await ex.run_close_requests_once()
    assert adapter.cancels == ["e-t1"] and ex._open == {}


@pytest.mark.asyncio
async def test_releasing_flatten_keeps_a_signal_requested_close():
    ex, adapter, _, switch = await _executor()
    adapter.set_next_result(_result(OrderStatus.WORKING))
    await ex.handle_signal(enter_row())
    adapter.cancel_results = [_result(OrderStatus.WORKING)]
    await ex.handle_signal(exit_row(reason="STOP"))
    switch["on"] = False
    await ex.run_close_requests_once()            # flatten off: STOP request kept
    assert ex._open == {}


@pytest.mark.asyncio
async def test_historical_scale_exit_row_is_ignored_never_a_close():
    # Partial exits were removed; a replayed SCALE row must not flatten.
    ex, adapter, _, _ = await _executor()
    await ex.handle_signal(enter_row())
    scale = {**exit_row(reason="SCALE"), "signal_id": "sig-scale", "trigger_level": 6,
             "fraction": "0.25"}
    await ex.handle_signal(scale)
    assert _trade(ex).state == "OPEN"
    assert _exits(adapter) == [] and adapter.cancels == [] and ex._close_requests == {}
    await ex.handle_signal(exit_row(reason="STOP"))       # a real close still works
    (final,) = _exits(adapter)
    assert final.qty == Decimal("10") and ex._open == {}


def _scaled_trade(state, exit_rung=0):
    # A trade whose last exit lifecycle was a (now removed) SCALE tranche.
    return OpenTrade(trade_id="0c8f2a6e-1d3b-4f5a-9e7c-2b4d6f8a0c1e", strategy="s",
                     position_key="c1:L", symbol="AAPL", asset_class="equity",
                     side=TradeSide.LONG, target_qty=Decimal("10"),
                     filled_qty=Decimal("10"), exit_filled_qty=Decimal("2.5"),
                     state=state, entry_client_order_id="e-t1",
                     exit_reason="SCALE", exit_rung=exit_rung)


@pytest.mark.asyncio
async def test_open_trade_with_historical_scale_reason_is_not_closed_at_boot():
    t = _scaled_trade("OPEN", exit_rung=300)
    hooks, state = make_hooks(open_trades=[t])
    adapter = CancelAdapter()
    adapter.reconcile_result = positions(AAPL="7.5")
    ex, adapter, _, _ = await _executor(adapter=adapter, hooks=hooks, state=state)
    assert ex._close_requests == {} and t.exit_reason is None
    await ex.run_close_requests_once()
    assert t.state == "OPEN" and _exits(adapter) == [] and adapter.cancels == []
    # A later final close sells the remainder at rung 0 (not the strided rung).
    await ex.handle_signal(exit_row(strategy="s", chain="c1", reason="STOP"))
    (final,) = _exits(adapter)
    assert final.qty == Decimal("7.5") and final.escalation_rung == 0
    assert ex._open == {}


@pytest.mark.asyncio
async def test_closing_scale_tranche_at_boot_is_cancelled_and_halted():
    t = _scaled_trade("CLOSING", exit_rung=600)
    hooks, state = make_hooks(open_trades=[t])
    adapter = CancelAdapter()
    adapter.reconcile_result = positions(AAPL="7.5")
    ex, adapter, _, _ = await _executor(adapter=adapter, hooks=hooks, state=state)
    assert adapter.cancels == [f"cv1.{t.trade_id.replace('-', '')}.X.600"]
    assert t.state == "HALTED" and state["states"][t.trade_id][0] == "HALTED"
    assert "SCALE" in state["states"][t.trade_id][1]
    assert ex._close_requests == {} and _exits(adapter) == []
    await ex.run_escalation_once()                        # never re-issued as a close
    assert _exits(adapter) == []
