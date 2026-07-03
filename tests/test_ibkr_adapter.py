"""
Tests for the IB adapter stack:
  - GatedAdapter: order calls no-op when disabled, pass through when enabled;
    reads always pass through.
  - IBKRAdapter mapping: order direction (entry/exit x long/short), fill
    mapping, contract building, status mapping — the pure logic, without a live
    gateway (the IB client is mocked where a call is unavoidable).

TradeEnableState has its own integration test against real Postgres.
"""
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from execution.gated_adapter import GatedAdapter, GATED_PREFIX
from execution.ibkr_adapter import IBKRAdapter, _is_buy, _to_status
from execution.types import (
    Fill,
    OrderIntent,
    OrderRequest,
    OrderResult,
    OrderStatus,
    ReconcileResult,
    TradeSide,
    VenuePosition,
)
from execution import client_order_id as coid


# --------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------

class FakeEnable:
    def __init__(self, enabled):
        self._enabled = enabled
    def is_enabled(self):
        return self._enabled


class FakeInner:
    """Records order calls and returns canned results, to prove pass-through /
    suppression."""
    venue = "ibkr"
    supported_classes = {"equity", "fx"}
    def __init__(self):
        self.submitted = []
        self.cancelled = []
        self.amended = []
        self.reconciled = 0
        self.positions_called = 0
    async def connect(self): pass
    async def start_fill_stream(self, cb): self._cb = cb
    async def reconcile(self):
        self.reconciled += 1
        return ReconcileResult(positions=[], open_orders=[], recent_fills=[])
    async def get_positions(self):
        self.positions_called += 1
        return [VenuePosition(symbol="AAPL", qty=Decimal(10))]
    async def submit_order(self, order):
        self.submitted.append(order)
        return OrderResult(client_order_id=order.client_order_id, status=OrderStatus.FILLED)
    async def cancel_order(self, cid):
        self.cancelled.append(cid)
        return OrderResult(client_order_id=cid, status=OrderStatus.CANCELLED)
    async def amend_order(self, cid, px):
        self.amended.append((cid, px))
        return OrderResult(client_order_id=cid, status=OrderStatus.WORKING)
    def to_venue_symbol(self, s): return s


def _order(intent=OrderIntent.ENTRY, side=TradeSide.LONG):
    tid = "11111111111111111111111111111111"
    return OrderRequest(
        client_order_id=coid.build(tid, intent, 0),
        trade_id=tid, symbol="AAPL", asset_class="equity",
        intent=intent, side=side,
        qty=Decimal(10), limit_price=Decimal("100.50"),
    )


# --------------------------------------------------------------------
# GatedAdapter — disabled
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_suppressed_when_disabled():
    inner = FakeInner()
    g = GatedAdapter(inner, FakeEnable(False))
    res = await g.submit_order(_order())
    assert res.status == OrderStatus.REJECTED
    assert res.reject_reason.startswith(GATED_PREFIX)
    assert inner.submitted == []   # nothing reached the venue


@pytest.mark.asyncio
async def test_cancel_amend_noop_when_disabled():
    inner = FakeInner()
    g = GatedAdapter(inner, FakeEnable(False))
    c = await g.cancel_order("cv1.x.E.0")
    a = await g.amend_order("cv1.x.E.0", Decimal("101"))
    assert c.status == OrderStatus.CANCELLED and c.reject_reason.startswith(GATED_PREFIX)
    assert a.status == OrderStatus.CANCELLED and a.reject_reason.startswith(GATED_PREFIX)
    assert inner.cancelled == [] and inner.amended == []


@pytest.mark.asyncio
async def test_reads_pass_through_when_disabled():
    inner = FakeInner()
    g = GatedAdapter(inner, FakeEnable(False))
    rec = await g.reconcile()
    pos = await g.get_positions()
    # Reads must reach the venue even with trading off.
    assert inner.reconciled == 1
    assert inner.positions_called == 1
    assert isinstance(rec, ReconcileResult)
    assert pos[0].symbol == "AAPL"


# --------------------------------------------------------------------
# GatedAdapter — enabled
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_passes_through_when_enabled():
    inner = FakeInner()
    g = GatedAdapter(inner, FakeEnable(True))
    res = await g.submit_order(_order())
    assert res.status == OrderStatus.FILLED
    assert len(inner.submitted) == 1


@pytest.mark.asyncio
async def test_cancel_amend_pass_through_when_enabled():
    inner = FakeInner()
    g = GatedAdapter(inner, FakeEnable(True))
    await g.cancel_order("cv1.x.E.0")
    await g.amend_order("cv1.x.E.0", Decimal("101"))
    assert inner.cancelled == ["cv1.x.E.0"]
    assert inner.amended == [("cv1.x.E.0", Decimal("101"))]


def test_gated_mirrors_inner_identity():
    inner = FakeInner()
    g = GatedAdapter(inner, FakeEnable(True))
    assert g.venue == "ibkr"
    assert g.supported_classes == {"equity", "fx"}
    assert g.to_venue_symbol("AAPL") == "AAPL"


# --------------------------------------------------------------------
# IBKRAdapter — order direction
# --------------------------------------------------------------------

def test_order_direction_entry_long_is_buy():
    assert _is_buy(_order(OrderIntent.ENTRY, TradeSide.LONG)) is True

def test_order_direction_entry_short_is_sell():
    assert _is_buy(_order(OrderIntent.ENTRY, TradeSide.SHORT)) is False

def test_order_direction_exit_long_is_sell():
    # Exiting a LONG means selling to close.
    assert _is_buy(_order(OrderIntent.EXIT, TradeSide.LONG)) is False

def test_order_direction_exit_short_is_buy():
    # Exiting a SHORT means buying to close.
    assert _is_buy(_order(OrderIntent.EXIT, TradeSide.SHORT)) is True


# --------------------------------------------------------------------
# IBKRAdapter — status mapping
# --------------------------------------------------------------------

def test_status_mapping():
    assert _to_status("Filled") == OrderStatus.FILLED
    assert _to_status("Cancelled") == OrderStatus.CANCELLED
    assert _to_status("Submitted") == OrderStatus.WORKING
    assert _to_status("Inactive") == OrderStatus.REJECTED
    # Unknown -> WORKING (never guess terminal).
    assert _to_status("SomeNewIBStatus") == OrderStatus.WORKING


# --------------------------------------------------------------------
# IBKRAdapter — contract building + canonical round-trip
# --------------------------------------------------------------------

def test_build_contract_equity():
    a = IBKRAdapter()
    c = a._build_contract("AAPL", "equity")
    assert c.symbol == "AAPL"
    assert c.secType == "STK"


def test_build_contract_fx():
    a = IBKRAdapter()
    c = a._build_contract("EURUSD", "fx")
    assert c.secType == "CASH"
    # ib_async Forex splits the pair.
    assert c.symbol == "EUR"
    assert c.currency == "USD"


def test_build_contract_rejects_crypto():
    a = IBKRAdapter()
    with pytest.raises(ValueError):
        a._build_contract("BTCUSDT", "crypto")


def test_canonical_from_contract_equity():
    a = IBKRAdapter()
    c = a._build_contract("AAPL", "equity")
    assert a._canonical_from_contract(c) == "AAPL"


def test_canonical_from_contract_fx_roundtrip():
    a = IBKRAdapter()
    c = a._build_contract("EURUSD", "fx")
    # EUR + USD -> EURUSD canonical.
    assert a._canonical_from_contract(c) == "EURUSD"


def test_canonical_from_contract_unknown_type_ignored():
    a = IBKRAdapter()
    fake = MagicMock()
    fake.secType = "FUT"   # a class we don't track
    assert a._canonical_from_contract(fake) is None


# --------------------------------------------------------------------
# IBKRAdapter — fill mapping
# --------------------------------------------------------------------

def _ib_fill(exec_id, shares, price, order_ref, commission=None, currency=None, ts_ms=1000):
    from datetime import datetime, timezone
    ex = MagicMock()
    ex.execId = exec_id
    ex.shares = shares
    ex.price = price
    ex.orderRef = order_ref
    ex.time = datetime.fromtimestamp(ts_ms / 1000, timezone.utc)
    cr = MagicMock()
    cr.commission = commission
    cr.currency = currency
    fill = MagicMock()
    fill.execution = ex
    fill.commissionReport = cr
    return fill


def test_map_fill_ours_entry():
    a = IBKRAdapter()
    tid = "22222222222222222222222222222222"
    ref = coid.build(tid, OrderIntent.ENTRY, 0)
    ibf = _ib_fill("exec-1", 10, 100.25, ref, commission=1.5, currency="USD", ts_ms=5000)
    f = a._map_fill(ibf)
    assert f.venue_exec_id == "exec-1"
    assert f.qty == Decimal("10")
    assert f.price == Decimal("100.25")
    assert f.intent == OrderIntent.ENTRY
    assert f.fee == Decimal("1.5")
    assert f.fee_currency == "USD"
    assert f.filled_at_ms == 5000


def test_map_fill_exit_intent_from_ref():
    a = IBKRAdapter()
    tid = "33333333333333333333333333333333"
    ref = coid.build(tid, OrderIntent.EXIT, 2)
    ibf = _ib_fill("exec-2", 5, 99.0, ref)
    f = a._map_fill(ibf)
    assert f.intent == OrderIntent.EXIT
    assert f.fee == Decimal("0")   # no commission report yet


def test_map_fill_no_commission_defaults_zero():
    a = IBKRAdapter()
    tid = "44444444444444444444444444444444"
    ref = coid.build(tid, OrderIntent.ENTRY, 0)
    ibf = _ib_fill("exec-3", 1, 50.0, ref, commission=None)
    f = a._map_fill(ibf)
    assert f.fee == Decimal("0")
    assert f.fee_currency is None
