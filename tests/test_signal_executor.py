"""
Tests for execution/signal_executor.py — the signal-consuming executor.

Uses a fake ExecutionAdapter and fake ExecutorHooks so the executor's
orchestration (boot sequence, ENTER/EXIT flows, gating, reconcile-HALT,
idempotency) is tested without a venue or a DB. The pricing is the real
price_marketable_limit; the fakes supply touch and caps.
"""
import dataclasses
from decimal import Decimal

import pytest

from execution.signal_executor import (
    SignalExecutor, ExecutorHooks, OpenTrade, Touch,
    EscalationRung, ExitCap,
)
from execution.pricing import CapConfig
from execution.types import (
    Fill, OrderIntent, OrderResult, OrderStatus, ReconcileResult,
    TradeSide, VenuePosition,
)
from execution import client_order_id as coid


# --------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------

class FakeAdapter:
    venue = "fake"
    supported_classes = {"equity"}
    def __init__(self):
        self.connected = False
        self.reconciled = False
        self.fill_stream_started = False
        self.submitted = []
        self.reconcile_result = ReconcileResult(positions=[], open_orders=[], recent_fills=[])
        self._next_result = None
        self._queue = []
        self._fill_cb = None
    async def connect(self):
        self.connected = True
    async def reconcile(self):
        self.reconciled = True
        return self.reconcile_result
    async def start_fill_stream(self, cb):
        self.fill_stream_started = True
        self._fill_cb = cb
    async def get_positions(self):
        return self.reconcile_result.positions
    async def submit_order(self, order):
        self.submitted.append(order)
        # A real venue echoes the client_order_id it was sent. Scripted results
        # carry a placeholder id; stamp the order's real (cv1) id on them so the
        # executor's venue_order_id -> client_order_id bridge resolves.
        if self._queue:
            return self._echo(order, self._queue.pop(0))
        if self._next_result is not None:
            r, self._next_result = self._next_result, None
            return self._echo(order, r)
        # Default: filled in full synchronously.
        return OrderResult(
            client_order_id=order.client_order_id, status=OrderStatus.FILLED,
            venue_order_id="v1", filled_qty=order.qty,
            fills=[Fill(venue_exec_id=f"ex-{order.client_order_id}", qty=order.qty,
                        price=order.limit_price, intent=order.intent, filled_at_ms=1)],
        )
    async def cancel_order(self, cid):
        return OrderResult(client_order_id=cid, status=OrderStatus.CANCELLED)
    async def amend_order(self, cid, px):
        return OrderResult(client_order_id=cid, status=OrderStatus.WORKING)
    def to_venue_symbol(self, s):
        return s
    @staticmethod
    def _echo(order, result):
        return dataclasses.replace(result, client_order_id=order.client_order_id)
    def set_next_result(self, r):
        self._next_result = r

    def queue_results(self, *results):
        """Queue multiple results, returned in order across submit_order calls.
        For escalation scenarios: first exit working, re-issue fills, etc."""
        self._queue = list(results)


def make_hooks(*, kill=False, touch=Touch(Decimal("100"), Decimal("100.10")),
               size=Decimal("10"), open_trades=None, ladder=None, clock=None):
    state = {
        "trades": {},
        "orders": {},
        "fills": {},
        "states": {},
        "exit_states": {},
        "now": [1_000_000],   # mutable clock, ms
    }
    if clock is not None:
        state["now"][0] = clock
    async def persist_trade(t): state["trades"][t.trade_id] = t
    async def update_trade_state(tid, s, hr): state["states"][tid] = (s, hr)
    async def persist_order(row): state["orders"][row["client_order_id"]] = row
    async def persist_fill(row):
        eid = row["venue_exec_id"]
        if eid in state["fills"]:
            return False
        state["fills"][eid] = row
        return True
    async def advance_order_fill(cid, qty, status): pass
    async def load_open_trades(): return open_trades or []
    async def get_touch(sym, ac): return touch
    async def get_cap(ac, intent):
        return CapConfig(cross_spread_mult=1.0, cap_spread_mult=2.0, cap_abs_bps=50.0)
    async def size_position(sym, ac, side, dp): return size
    async def kill_switch_blocks(): return kill
    async def get_exit_cap(ac):
        base = CapConfig(cross_spread_mult=1.0, cap_spread_mult=2.0, cap_abs_bps=50.0)
        rungs = ladder if ladder is not None else (
            EscalationRung(widen_to_spread_mult=Decimal("3"), window_seconds=30),
            EscalationRung(widen_to_spread_mult=Decimal("5"), window_seconds=30),
        )
        return ExitCap(base=base, ladder=tuple(rungs))
    async def persist_exit_state(tid, rung, deadline_ms):
        state["exit_states"][tid] = (rung, deadline_ms)
    def now_ms(): return state["now"][0]
    hooks = ExecutorHooks(
        persist_trade=persist_trade, update_trade_state=update_trade_state,
        persist_order=persist_order, persist_fill=persist_fill,
        advance_order_fill=advance_order_fill, load_open_trades=load_open_trades,
        get_touch=get_touch, get_cap=get_cap, size_position=size_position,
        get_exit_cap=get_exit_cap, persist_exit_state=persist_exit_state,
        kill_switch_blocks=kill_switch_blocks, now_ms=now_ms,
    )
    return hooks, state


def enter_row(strategy="two_ladder", chain="chainX", direction="LONG", symbol="AAPL"):
    pkey = f"{chain}:{'L' if direction == 'LONG' else 'S'}"
    return {
        "signal_id": f"sig-{strategy}-{pkey}-ENTER", "position_key": pkey,
        "chain_id": chain, "direction": direction, "action": "ENTER",
        "symbol": symbol, "asset_class": "equity", "strategy": strategy,
        "trigger_level": 5, "decision_price": 100.0, "exit_reason": None,
    }


def exit_row(strategy="two_ladder", chain="chainX", direction="LONG", symbol="AAPL", reason="TARGET"):
    pkey = f"{chain}:{'L' if direction == 'LONG' else 'S'}"
    return {
        "signal_id": f"sig-{strategy}-{pkey}-EXIT", "position_key": pkey,
        "chain_id": chain, "direction": direction, "action": "EXIT",
        "symbol": symbol, "asset_class": "equity", "strategy": strategy,
        "trigger_level": 5, "decision_price": 101.0, "exit_reason": reason,
    }


# --------------------------------------------------------------------
# Boot sequence
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_boot_sequence_order():
    adapter = FakeAdapter()
    hooks, _ = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    # connect -> reconcile -> fill stream, all ran.
    assert adapter.connected
    assert adapter.reconciled
    assert adapter.fill_stream_started


@pytest.mark.asyncio
async def test_signal_ignored_before_boot():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    # Not booted yet.
    await ex.handle_signal(enter_row())
    assert adapter.submitted == []   # nothing acted on before boot


# --------------------------------------------------------------------
# ENTER
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enter_submits_and_opens():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    await ex.handle_signal(enter_row())
    # One entry order submitted, priced as a marketable buy limit.
    assert len(adapter.submitted) == 1
    o = adapter.submitted[0]
    assert o.intent == OrderIntent.ENTRY and o.side == TradeSide.LONG
    assert coid.is_ours(o.client_order_id)
    # Trade recorded and OPEN (filled synchronously).
    trade = list(state["trades"].values())[0]
    assert state["states"][trade.trade_id][0] == "OPEN"


@pytest.mark.asyncio
async def test_enter_blocked_by_kill_switch():
    adapter = FakeAdapter()
    hooks, state = make_hooks(kill=True)
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    await ex.handle_signal(enter_row())
    assert adapter.submitted == []      # kill switch prevented the order
    assert state["trades"] == {}


@pytest.mark.asyncio
async def test_enter_rejected_marks_trade_rejected():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    # Venue rejects the entry.
    adapter.set_next_result(OrderResult(
        client_order_id="x", status=OrderStatus.REJECTED, reject_reason="margin"))
    await ex.handle_signal(enter_row())
    trade = list(state["trades"].values())[0]
    assert state["states"][trade.trade_id][0] == "REJECTED"


@pytest.mark.asyncio
async def test_enter_idempotent_on_replay():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    await ex.handle_signal(enter_row())
    await ex.handle_signal(enter_row())   # replay same position
    assert len(adapter.submitted) == 1     # second is a no-op


@pytest.mark.asyncio
async def test_enter_zero_size_no_order():
    adapter = FakeAdapter()
    hooks, state = make_hooks(size=Decimal("0"))
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    await ex.handle_signal(enter_row())
    assert adapter.submitted == []


@pytest.mark.asyncio
async def test_two_strategies_same_chain_two_trades():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    await ex.handle_signal(enter_row(strategy="two_ladder"))
    await ex.handle_signal(enter_row(strategy="confirmed_15m"))
    # Same chain, different strategies -> two distinct trades.
    assert len(state["trades"]) == 2
    assert len(adapter.submitted) == 2


# --------------------------------------------------------------------
# EXIT
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_exit_closes_open_trade():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    await ex.handle_signal(enter_row())
    await ex.handle_signal(exit_row())
    # Two orders: entry then exit.
    assert len(adapter.submitted) == 2
    exit_order = adapter.submitted[1]
    assert exit_order.intent == OrderIntent.EXIT
    # Exiting a LONG is a SELL; the exit order carries the position side.
    assert exit_order.side == TradeSide.LONG
    trade = list(state["trades"].values())[0]
    assert state["states"][trade.trade_id][0] == "CLOSED"


@pytest.mark.asyncio
async def test_exit_unknown_position_no_order():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    await ex.handle_signal(exit_row())   # no matching open trade
    assert adapter.submitted == []


# --------------------------------------------------------------------
# Reconcile
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reconcile_db_open_venue_flat_halts():
    adapter = FakeAdapter()
    # DB says we hold an OPEN trade; venue reconcile returns NO positions.
    open_trade = OpenTrade(
        trade_id="t-1", strategy="two_ladder", position_key="chainX:L",
        symbol="AAPL", asset_class="equity", side=TradeSide.LONG,
        target_qty=Decimal("10"), filled_qty=Decimal("10"), state="OPEN",
        entry_client_order_id="cv1.x.E.0",
    )
    hooks, state = make_hooks(open_trades=[open_trade])
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    # The trade should be HALTED (DB open, venue flat).
    assert state["states"]["t-1"][0] == "HALTED"


@pytest.mark.asyncio
async def test_reconcile_db_open_venue_confirms_keeps_managing():
    adapter = FakeAdapter()
    open_trade = OpenTrade(
        trade_id="t-2", strategy="two_ladder", position_key="chainX:L",
        symbol="AAPL", asset_class="equity", side=TradeSide.LONG,
        target_qty=Decimal("10"), filled_qty=Decimal("10"), state="OPEN",
        entry_client_order_id="cv1.x.E.0",
    )
    hooks, state = make_hooks(open_trades=[open_trade])
    # Venue confirms the position.
    adapter.reconcile_result = ReconcileResult(
        positions=[VenuePosition(symbol="AAPL", qty=Decimal("10"))],
        open_orders=[], recent_fills=[],
    )
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    # Not halted; still under management.
    assert "t-2" not in state["states"] or state["states"]["t-2"][0] != "HALTED"
    assert ("two_ladder", "chainX:L") in ex._open


# --------------------------------------------------------------------
# Fills / dedup
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fill_dedup_by_venue_exec_id():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    adapter.set_next_result(OrderResult(
        client_order_id="placeholder", status=OrderStatus.WORKING,
        venue_order_id="VO-D", filled_qty=Decimal("0"), fills=[]))
    await ex.handle_signal(enter_row())
    trade = ex._open[("two_ladder", "chainX:L")]
    f = Fill(venue_exec_id="dup-1", venue_order_id="VO-D", qty=Decimal("5"),
             price=Decimal("100"), intent=OrderIntent.ENTRY, filled_at_ms=1)
    await ex._on_fill(f)
    await ex._on_fill(f)   # re-delivery
    assert len(state["fills"]) == 1   # deduped
    assert trade.filled_qty == Decimal("5")   # counted once


# --------------------------------------------------------------------
# Escalation-ladder driving
# --------------------------------------------------------------------

def _working(cid, intent=OrderIntent.EXIT):
    """An OrderResult that acked WORKING with no fills (exit didn't fill)."""
    return OrderResult(client_order_id=cid, status=OrderStatus.WORKING,
                       venue_order_id="w1", filled_qty=Decimal("0"), fills=[])


def _filled(cid, qty, intent=OrderIntent.EXIT, price="100"):
    return OrderResult(
        client_order_id=cid, status=OrderStatus.FILLED, venue_order_id="f1",
        filled_qty=qty,
        fills=[Fill(venue_exec_id=f"ex-{cid}", qty=qty, price=Decimal(price),
                    intent=intent, filled_at_ms=1)],
    )


@pytest.mark.asyncio
async def test_exit_sets_deadline_and_rung_zero():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    await ex.handle_signal(enter_row())
    # Make the exit NOT fill, so it stays CLOSING with a deadline.
    adapter.queue_results(_working("exit0"))
    await ex.handle_signal(exit_row())
    key = ("two_ladder", "chainX:L")
    trade = ex._open[key]
    assert trade.state == "CLOSING"
    assert trade.exit_rung == 0
    # Deadline = now (1_000_000) + first rung window (30s) = +30_000ms.
    assert trade.exit_deadline_ms == 1_000_000 + 30_000
    assert state["exit_states"][trade.trade_id] == (0, 1_000_000 + 30_000)


@pytest.mark.asyncio
async def test_escalation_advances_rung_when_window_expires():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    await ex.handle_signal(enter_row())
    # Exit at rung 0 doesn't fill.
    adapter.queue_results(_working("exit0"))
    await ex.handle_signal(exit_row())
    # Advance the clock past the rung-0 window; the re-issue at rung 1 fills.
    state["now"][0] = 1_000_000 + 31_000
    adapter.queue_results(_filled("exit1", Decimal("10")))
    await ex.run_escalation_once()
    key = ("two_ladder", "chainX:L")
    # Escalated to rung 1, filled, now CLOSED.
    assert key not in ex._open   # finalized
    trade = state["trades"][list(state["trades"])[0]]
    assert state["states"][trade.trade_id][0] == "CLOSED"
    # The re-issue priced at rung 1 (wider cap) — a second exit order was sent.
    exit_orders = [o for o in adapter.submitted if o.intent == OrderIntent.EXIT]
    assert len(exit_orders) == 2
    assert exit_orders[1].escalation_rung == 1


@pytest.mark.asyncio
async def test_escalation_no_action_before_deadline():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    await ex.handle_signal(enter_row())
    adapter.queue_results(_working("exit0"))
    await ex.handle_signal(exit_row())
    # Clock has NOT passed the deadline.
    state["now"][0] = 1_000_000 + 10_000
    await ex.run_escalation_once()
    exit_orders = [o for o in adapter.submitted if o.intent == OrderIntent.EXIT]
    assert len(exit_orders) == 1   # no escalation yet


@pytest.mark.asyncio
async def test_escalation_halts_at_top_of_ladder():
    adapter = FakeAdapter()
    cancels = []
    async def cancel_order(cid):
        cancels.append(cid)
        return OrderResult(client_order_id=cid, status=OrderStatus.CANCELLED)
    adapter.cancel_order = cancel_order
    # A one-rung ladder: rung 0 (base cap), then rung 1 = ladder[0]. The top
    # rung gets its OWN window before the HALT, and the HALT cancels its order.
    hooks, state = make_hooks(ladder=(
        EscalationRung(widen_to_spread_mult=Decimal("3"), window_seconds=30),
    ))
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    await ex.handle_signal(enter_row())
    adapter.queue_results(_working("exit0"))
    await ex.handle_signal(exit_row())
    key = ("two_ladder", "chainX:L")
    trade = ex._open[key]
    # rung 0's window is ladder[0]=30s. Expire it; escalate to rung 1.
    state["now"][0] = 1_000_000 + 31_000
    adapter.queue_results(_working("exit1"))
    await ex.run_escalation_once()
    assert trade.exit_rung == 1
    assert trade.exit_deadline_ms == 1_000_000 + 31_000 + 30_000   # top rung's window
    await ex.run_escalation_once()
    assert trade.state == "CLOSING"             # window still running: no HALT yet
    # Top rung's window expires -> cancel its order, then HALT.
    state["now"][0] = 1_000_000 + 62_000
    await ex.run_escalation_once()
    assert state["states"][trade.trade_id][0] == "HALTED"
    assert cancels[-1] == coid.build(trade.trade_id, OrderIntent.EXIT, 1)
    assert ex._open[key] is trade               # HALTED stays under management


@pytest.mark.asyncio
async def test_partial_exit_fill_tracked_then_completes():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    await ex.handle_signal(enter_row())   # holds 10
    # Exit rung 0 partially fills 4 (working remainder).
    adapter.queue_results(OrderResult(
        client_order_id="exit0", status=OrderStatus.PARTIALLY_FILLED,
        venue_order_id="p1", filled_qty=Decimal("4"),
        fills=[Fill(venue_exec_id="pex-0", qty=Decimal("4"), price=Decimal("100"),
                    intent=OrderIntent.EXIT, filled_at_ms=1)],
    ))
    await ex.handle_signal(exit_row())
    key = ("two_ladder", "chainX:L")
    trade = ex._open[key]
    assert trade.exit_filled_qty == Decimal("4")   # sync fill attributed
    assert trade.state == "CLOSING"                # 6 remain
    # Escalate: the re-issue closes the remaining 6.
    state["now"][0] = 1_000_000 + 31_000
    adapter.queue_results(_filled("exit1", Decimal("6")))
    await ex.run_escalation_once()
    assert key not in ex._open
    assert state["states"][trade.trade_id][0] == "CLOSED"
    # Re-issue quantity was the REMAINING 6, not the full 10.
    exit_orders = [o for o in adapter.submitted if o.intent == OrderIntent.EXIT]
    assert exit_orders[1].qty == Decimal("6")


@pytest.mark.asyncio
async def test_duplicate_exit_while_closing_ignored():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    await ex.handle_signal(enter_row())
    adapter.queue_results(_working("exit0"))
    await ex.handle_signal(exit_row())
    # A second EXIT while CLOSING must not stack another order.
    await ex.handle_signal(exit_row())
    exit_orders = [o for o in adapter.submitted if o.intent == OrderIntent.EXIT]
    assert len(exit_orders) == 1


@pytest.mark.asyncio
async def test_sync_entry_fill_attributed():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    await ex.handle_signal(enter_row())
    key = ("two_ladder", "chainX:L")
    trade = ex._open[key]
    # The default adapter fills the entry in full (qty 10) synchronously.
    assert trade.filled_qty == Decimal("10")
    # And that fill is persisted, attributed to the trade.
    assert len(state["fills"]) == 1
    fill_row = list(state["fills"].values())[0]
    assert fill_row["trade_id"] == trade.trade_id


# --------------------------------------------------------------------
# Async fill attribution (via venue_order_id)
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_entry_fill_attributed_and_opens():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    # Entry acks WORKING (no sync fill), with a venue_order_id.
    adapter.set_next_result(OrderResult(
        client_order_id="e0", status=OrderStatus.WORKING,
        venue_order_id="VO-1", filled_qty=Decimal("0"), fills=[]))
    await ex.handle_signal(enter_row())
    key = ("two_ladder", "chainX:L")
    trade = ex._open[key]
    assert trade.state == "SUBMITTED"
    # Async fill arrives on the stream, tagged with the same venue_order_id.
    await ex._on_fill(Fill(
        venue_exec_id="ex-async-1", venue_order_id="VO-1",
        qty=Decimal("10"), price=Decimal("100"),
        intent=OrderIntent.ENTRY, filled_at_ms=2))
    # Attributed: filled_qty advanced, trade now OPEN.
    assert trade.filled_qty == Decimal("10")
    assert state["states"][trade.trade_id][0] == "OPEN"
    # Fill persisted with the trade id (attributed).
    assert state["fills"]["ex-async-1"]["trade_id"] == trade.trade_id


@pytest.mark.asyncio
async def test_async_exit_fill_completes_close():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    await ex.handle_signal(enter_row())   # holds 10 (sync-filled by default)
    key = ("two_ladder", "chainX:L")
    trade = ex._open[key]
    # Exit acks WORKING with a venue_order_id, no sync fill.
    adapter.set_next_result(OrderResult(
        client_order_id="x0", status=OrderStatus.WORKING,
        venue_order_id="VO-EXIT", filled_qty=Decimal("0"), fills=[]))
    await ex.handle_signal(exit_row())
    assert trade.state == "CLOSING"
    # Async exit fill closes the whole position.
    await ex._on_fill(Fill(
        venue_exec_id="ex-async-x", venue_order_id="VO-EXIT",
        qty=Decimal("10"), price=Decimal("101"),
        intent=OrderIntent.EXIT, filled_at_ms=3))
    assert key not in ex._open   # finalized
    assert state["states"][trade.trade_id][0] == "CLOSED"


@pytest.mark.asyncio
async def test_async_partial_exit_fill_then_remainder():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    await ex.handle_signal(enter_row())   # holds 10
    key = ("two_ladder", "chainX:L")
    trade = ex._open[key]
    adapter.set_next_result(OrderResult(
        client_order_id="x0", status=OrderStatus.WORKING,
        venue_order_id="VO-X", filled_qty=Decimal("0"), fills=[]))
    await ex.handle_signal(exit_row())
    # Two async partial fills on the same exit order.
    await ex._on_fill(Fill(venue_exec_id="p1", venue_order_id="VO-X",
                           qty=Decimal("6"), price=Decimal("101"),
                           intent=OrderIntent.EXIT, filled_at_ms=3))
    assert trade.state == "CLOSING"          # 4 remain
    assert trade.exit_filled_qty == Decimal("6")
    await ex._on_fill(Fill(venue_exec_id="p2", venue_order_id="VO-X",
                           qty=Decimal("4"), price=Decimal("101"),
                           intent=OrderIntent.EXIT, filled_at_ms=4))
    assert key not in ex._open               # completed
    assert state["states"][trade.trade_id][0] == "CLOSED"


@pytest.mark.asyncio
async def test_async_fill_unattributable_counted_not_persisted():
    from execution.observability import Metrics
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    metrics = Metrics()
    ex = SignalExecutor(adapter, hooks, metrics=metrics)
    await ex.start()
    # A fill for an order we don't know (foreign/manual, or pre-start).
    await ex._on_fill(Fill(
        venue_exec_id="foreign-1", venue_order_id="UNKNOWN-ORDER",
        qty=Decimal("1"), price=Decimal("100"),
        intent=OrderIntent.ENTRY, filled_at_ms=5))
    # fills.trade_id is NOT NULL, so it cannot be stored; it is counted and left
    # for the reconcile sweep. Never guessed onto a trade.
    assert "foreign-1" not in state["fills"]
    assert metrics.snapshot()["counters"]["fills_unattributable"] == 1


@pytest.mark.asyncio
async def test_async_fill_dedup_with_sync():
    adapter = FakeAdapter()
    hooks, state = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    await ex.start()
    # Entry sync-fills (default adapter) qty 10 with venue_order_id "v1" and
    # exec id "ex-cv1...". If the SAME execution is also re-delivered on the
    # stream, it must dedupe by venue_exec_id and not double-count.
    key = ("two_ladder", "chainX:L")
    await ex.handle_signal(enter_row())
    trade = ex._open[key]
    assert trade.filled_qty == Decimal("10")
    sync_fill_exec_id = list(state["fills"])[0]
    # Re-deliver the same execution on the stream.
    await ex._on_fill(Fill(
        venue_exec_id=sync_fill_exec_id, venue_order_id="v1",
        qty=Decimal("10"), price=Decimal("100"),
        intent=OrderIntent.ENTRY, filled_at_ms=1))
    # No double count: filled_qty still 10.
    assert trade.filled_qty == Decimal("10")