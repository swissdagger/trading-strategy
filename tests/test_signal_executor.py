"""
Tests for execution/signal_executor.py — the signal-consuming executor.

Uses a fake ExecutionAdapter and fake ExecutorHooks so the executor's
orchestration (boot sequence, ENTER/EXIT flows, gating, reconcile-HALT,
idempotency) is tested without a venue or a DB. The pricing is the real
price_marketable_limit; the fakes supply touch and caps.
"""
from decimal import Decimal

import pytest

from execution.signal_executor import (
    SignalExecutor, ExecutorHooks, OpenTrade, Touch,
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
    venue = "ibkr"
    supported_classes = {"equity"}
    def __init__(self):
        self.connected = False
        self.reconciled = False
        self.fill_stream_started = False
        self.submitted = []
        self.reconcile_result = ReconcileResult(positions=[], open_orders=[], recent_fills=[])
        self._next_result = None
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
        if self._next_result is not None:
            r, self._next_result = self._next_result, None
            return r
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
    def set_next_result(self, r):
        self._next_result = r


def make_hooks(*, kill=False, touch=Touch(Decimal("100"), Decimal("100.10")),
               size=Decimal("10"), open_trades=None):
    state = {
        "trades": {},
        "orders": {},
        "fills": {},
        "states": {},
    }
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
    hooks = ExecutorHooks(
        persist_trade=persist_trade, update_trade_state=update_trade_state,
        persist_order=persist_order, persist_fill=persist_fill,
        advance_order_fill=advance_order_fill, load_open_trades=load_open_trades,
        get_touch=get_touch, get_cap=get_cap, size_position=size_position,
        kill_switch_blocks=kill_switch_blocks,
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
    await ex.handle_signal(enter_row(strategy="four_chain"))
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
    f = Fill(venue_exec_id="dup-1", qty=Decimal("5"), price=Decimal("100"),
             intent=OrderIntent.ENTRY, filled_at_ms=1)
    await ex._on_fill(f)
    await ex._on_fill(f)   # re-delivery
    assert len(state["fills"]) == 1   # deduped
