"""
Kill switch 'flatten' mode: while engaged, the executor cancels working entries
and closes every open position (reason KILL_SWITCH) through the normal exit
path. Driven with the fakes from test_signal_executor / test_executor_hardening.
"""
import asyncio
from decimal import Decimal

import pytest

from test_executor_hardening import SpotAdapter, db_trade, positions
from test_signal_executor import enter_row, exit_row, make_hooks

from execution.gated_adapter import GATED_PREFIX
from execution.signal_executor import SignalExecutor
from execution.types import ExitReason, Fill, OrderIntent, OrderResult, OrderStatus


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
    await ex.run_flatten_once()
    assert _trade(ex).state == "OPEN"
    assert _exits(adapter) == [] and adapter.cancels == []


@pytest.mark.asyncio
async def test_no_hook_means_no_flatten():
    hooks, _ = make_hooks()
    adapter = CancelAdapter()
    ex = SignalExecutor(adapter, hooks)       # kill_switch_flatten left None
    await ex.start()
    await ex.handle_signal(enter_row())
    await ex.run_flatten_once()
    assert _trade(ex).state == "OPEN"


@pytest.mark.asyncio
async def test_filled_position_is_closed_with_kill_switch_reason():
    ex, adapter, state, switch = await _executor()
    await ex.handle_signal(enter_row())
    trade_id = _trade(ex).trade_id
    switch["on"] = True
    await ex.run_flatten_once()
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
    await ex.run_flatten_once()
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
    await ex.run_flatten_once()
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
    await ex.run_flatten_once()
    assert _exits(adapter) == []
    await ex.run_flatten_once()                   # still waiting; no re-cancel
    assert _exits(adapter) == [] and len(adapter.cancels) == 1
    await ex._on_fill(_fill("e2", "2", cid=trade.entry_client_order_id))
    await ex.run_flatten_once()
    (exit_order,) = _exits(adapter)
    assert exit_order.qty == Decimal("6")


@pytest.mark.asyncio
async def test_unconfirmed_cancel_does_not_sell_and_retries():
    ex, adapter, _, switch = await _executor()
    adapter.set_next_result(_result(OrderStatus.WORKING))
    await ex.handle_signal(enter_row())
    switch["on"] = True
    adapter.cancel_results = [_result(OrderStatus.WORKING)]
    await ex.run_flatten_once()
    assert _trade(ex).state == "SUBMITTED" and _exits(adapter) == []
    await ex.run_flatten_once()                   # default: cancel confirmed
    assert len(adapter.cancels) == 2 and ex._open == {}


@pytest.mark.asyncio
async def test_cancel_that_raises_is_retried_next_pass():
    ex, adapter, _, switch = await _executor()
    adapter.set_next_result(_result(OrderStatus.WORKING))
    await ex.handle_signal(enter_row())
    switch["on"] = True
    adapter.cancel_raises = True
    await ex.run_flatten_once()
    assert _trade(ex).state == "SUBMITTED"
    adapter.cancel_raises = False
    await ex.run_flatten_once()
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
    await ex.run_flatten_once()
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
    await ex.run_flatten_once()
    assert len(adapter.submitted) == n and adapter.cancels == []
    assert _trade(ex).exit_reason == "STOP"


@pytest.mark.asyncio
async def test_halted_trade_is_left_for_a_human():
    adapter = CancelAdapter()
    adapter.reconcile_result = positions(AAPL=10)
    hooks, state = make_hooks(open_trades=[db_trade("t1", state="HALTED")])
    ex, adapter, _, switch = await _executor(adapter, hooks, state)
    switch["on"] = True
    await ex.run_flatten_once()
    assert adapter.submitted == [] and adapter.cancels == []


@pytest.mark.asyncio
async def test_flattens_every_symbol():
    ex, adapter, _, switch = await _executor()
    await ex.handle_signal(enter_row(chain="c1", symbol="AAPL"))
    await ex.handle_signal(enter_row(chain="c2", symbol="MSFT"))
    switch["on"] = True
    await ex.run_flatten_once()
    assert sorted(o.symbol for o in _exits(adapter)) == ["AAPL", "MSFT"]
    assert ex._open == {}


@pytest.mark.asyncio
async def test_unfilled_flatten_exit_escalates_like_any_exit():
    ex, adapter, state, switch = await _executor()
    await ex.handle_signal(enter_row())
    switch["on"] = True
    adapter.set_next_result(_result(OrderStatus.WORKING))
    await ex.run_flatten_once()
    trade = _trade(ex)
    assert trade.state == "CLOSING" and trade.exit_rung == 0
    await ex.run_flatten_once()                   # already closing for good: no-op
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
