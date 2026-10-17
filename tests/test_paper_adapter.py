"""
Tests for execution/paper_adapter.py (ADR-001 Phase 5), the off/paper factory
mapping, the shadow book's live fee tier, and run_executor's mode-change
safety callback. The market is a fake standing in for the read-only real
Coinbase adapter: a mutable book, product rules and a fee tier.
"""
import asyncio
import uuid
from decimal import Decimal

import pytest

from execution import client_order_id as coid
from execution.coinbase_adapter import Product
from execution.paper_adapter import MemPaperLedger, PaperCoinbaseAdapter
from execution.read_only_adapter import READ_ONLY_REASON, ReadOnlyAdapter
from execution.trade_enable import TradeMode
from execution.types import (
    MinOrderSize, OrderIntent, OrderRequest, OrderStatus, Touch, TradeSide,
)

NOW_MS = 1_791_000_000_000


class FakeMarket:
    """Stands in for ReadOnlyAdapter(CoinbaseAdapter): reads only."""
    venue = "coinbase"
    supported_classes = {"crypto"}
    supports_short = False

    def __init__(self, fee_bps=Decimal("60")):
        self.books = {"BTC-USD": (Decimal("100000"), Decimal("100010"))}
        self.fee_bps = fee_bps
        self.connected = False
        self.tradable = {"BTC-USD": True}

    async def connect(self):
        self.connected = True

    async def get_touch(self, symbol, asset_class):
        b = self.books.get(symbol)
        return Touch(*b) if b else None

    async def get_step_size(self, symbol, asset_class):
        return Decimal("0.0001")

    async def get_price_increment(self, symbol, asset_class):
        return Decimal("0.01")

    async def get_min_order_size(self, symbol, asset_class):
        return MinOrderSize(base=Decimal("0.0001"), quote=Decimal("1"))

    async def product(self, symbol):
        return Product(symbol, Decimal("0.0001"), Decimal("0.01"), Decimal("0.0001"),
                       Decimal("1"), self.tradable.get(symbol, False))

    async def get_taker_fee_bps(self):
        if isinstance(self.fee_bps, Exception):
            raise self.fee_bps
        return self.fee_bps


async def paper(ledger=None, market=None, cash="10000", **kw):
    market = market or FakeMarket()
    ledger = ledger or MemPaperLedger()
    a = PaperCoinbaseAdapter(market, ledger.hooks(), quote_currency="USD",
                             starting_cash=Decimal(cash), now_ms=lambda: NOW_MS, **kw)
    await a.connect()
    return a, market, ledger


def order(qty="0.05", limit="100020", intent=OrderIntent.ENTRY, side=TradeSide.LONG,
          symbol="BTC-USD", cid=None):
    tid = str(uuid.uuid4())
    return OrderRequest(client_order_id=cid or coid.build(tid, intent, 0), trade_id=tid,
                        symbol=symbol, asset_class="crypto", intent=intent, side=side,
                        qty=Decimal(qty), limit_price=Decimal(limit))


# --------------------------------------------------------------------
# Account
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_first_connect_seeds_starting_cash_once():
    ledger = MemPaperLedger()
    a, market, _ = await paper(ledger)
    assert market.connected
    assert a.state.balances == {"USD": Decimal("10000")}
    ledger.state.balances["USD"] = Decimal("123")
    b, _, _ = await paper(ledger, cash="99999")          # restart: no reseed
    assert b.state.balances["USD"] == Decimal("123")


# --------------------------------------------------------------------
# Fill model
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_crossing_buy_fills_in_full_at_its_limit_with_the_real_fee():
    a, _, _ = await paper()
    r = await a.submit_order(order(qty="0.05", limit="100020"))
    assert r.status == OrderStatus.FILLED and r.filled_qty == Decimal("0.05")
    (f,) = r.fills
    assert f.price == Decimal("100020")               # the limit, not the 100010 ask
    notional = Decimal("0.05") * Decimal("100020")
    assert f.fee == notional * Decimal("0.006")       # 60 bps tier
    assert f.intent == OrderIntent.ENTRY and f.client_order_id == r.client_order_id
    assert a.state.balances["BTC"] == Decimal("0.05")
    assert a.state.balances["USD"] == Decimal("10000") - notional - f.fee


@pytest.mark.asyncio
async def test_crossing_sell_exit_credits_cash_net_of_fee():
    a, _, _ = await paper()
    await a.submit_order(order(qty="0.05", limit="100020"))
    cash = a.state.balances["USD"]
    r = await a.submit_order(order(qty="0.05", limit="99990", intent=OrderIntent.EXIT))
    assert r.status == OrderStatus.FILLED
    assert a.state.balances["BTC"] == 0
    proceeds = Decimal("0.05") * Decimal("99990")
    assert a.state.balances["USD"] == cash + proceeds - proceeds * Decimal("0.006")


@pytest.mark.asyncio
async def test_non_crossing_order_rests_then_fills_when_the_book_reaches_it():
    a, market, _ = await paper()
    r = await a.submit_order(order(qty="0.01", limit="99900"))   # below the 100010 ask
    assert r.status == OrderStatus.WORKING and not r.fills
    got = []
    async def cb(f): got.append(f)
    assert await a.check_resting_once(cb) == 0
    market.books["BTC-USD"] = (Decimal("99880"), Decimal("99895"))
    assert await a.check_resting_once(cb) == 1
    assert got[0].price == Decimal("99900") and got[0].venue_order_id == r.venue_order_id
    assert a.state.orders[r.client_order_id].status == "FILLED"


@pytest.mark.asyncio
async def test_resting_order_is_rechecked_not_filled_without_a_touch():
    a, market, _ = await paper()
    r = await a.submit_order(order(qty="0.01", limit="99900"))
    market.books.clear()
    async def cb(f): raise AssertionError("must not fill without a book")
    assert await a.check_resting_once(cb) == 0
    assert a.state.orders[r.client_order_id].status == "OPEN"


# --------------------------------------------------------------------
# Venue rules
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_insufficient_cash_is_rejected_including_reserved_cash():
    a, _, _ = await paper(cash="1000")
    r = await a.submit_order(order(qty="0.02", limit="100020"))   # ~2000 needed
    assert r.status == OrderStatus.REJECTED and "INSUFFICIENT_FUND" in r.reject_reason
    a2, _, _ = await paper(cash="1500")
    assert (await a2.submit_order(order(qty="0.01", limit="99900"))).status == OrderStatus.WORKING
    r = await a2.submit_order(order(qty="0.006", limit="100020"))  # 999 resting + 600 > 1500
    assert r.status == OrderStatus.REJECTED


@pytest.mark.asyncio
async def test_sell_beyond_holdings_or_reserved_base_is_rejected():
    a, _, _ = await paper()
    await a.submit_order(order(qty="0.05", limit="100020"))
    assert (await a.submit_order(order(qty="0.06", limit="99990",
                                       intent=OrderIntent.EXIT))).status == OrderStatus.REJECTED
    assert (await a.submit_order(order(qty="0.03", limit="100500",
                                       intent=OrderIntent.EXIT))).status == OrderStatus.WORKING
    r = await a.submit_order(order(qty="0.03", limit="99990", intent=OrderIntent.EXIT))
    assert r.status == OrderStatus.REJECTED   # only 0.02 unreserved


@pytest.mark.asyncio
async def test_size_is_floored_and_minimums_enforced():
    a, market, _ = await paper()
    r = await a.submit_order(order(qty="0.012399", limit="100020"))
    assert r.filled_qty == Decimal("0.0123")
    assert "rounds to 0" in (await a.submit_order(order(qty="0.00005"))).reject_reason
    r = await a.submit_order(order(qty="0.0001", limit="5000"))      # 0.50 < 1 USD
    assert r.status == OrderStatus.REJECTED and "quote_min_size" in r.reject_reason


@pytest.mark.asyncio
async def test_short_and_untradable_are_refused():
    a, market, _ = await paper()
    assert "short" in (await a.submit_order(order(side=TradeSide.SHORT))).reject_reason
    market.tradable["BTC-USD"] = False
    assert "not tradable" in (await a.submit_order(order())).reject_reason


@pytest.mark.asyncio
async def test_repeated_client_order_id_returns_the_existing_order():
    a, _, _ = await paper()
    o = order()
    r1, r2 = await a.submit_order(o), await a.submit_order(o)
    assert r1.venue_order_id == r2.venue_order_id
    assert a.state.balances["BTC"] == Decimal("0.05")   # filled once


@pytest.mark.asyncio
async def test_cancel_releases_reserved_cash_and_reports_terminal_states():
    a, _, _ = await paper(cash="1500")
    resting = order(qty="0.01", limit="99900")
    await a.submit_order(resting)
    c = await a.cancel_order(resting.client_order_id)
    assert c.status == OrderStatus.CANCELLED
    assert (await a.get_account_equity()).available == Decimal("1500")
    filled = order(qty="0.001", limit="100020")
    await a.submit_order(filled)
    assert (await a.cancel_order(filled.client_order_id)).status == OrderStatus.FILLED
    assert (await a.cancel_order("cv1." + "0" * 32 + ".E.0")).status == OrderStatus.CANCELLED


@pytest.mark.asyncio
async def test_amend_moves_a_resting_order_and_fills_if_it_now_crosses():
    a, _, _ = await paper()
    o = order(qty="0.01", limit="99900")
    await a.submit_order(o)
    r = await a.amend_order(o.client_order_id, Decimal("100015.019"))
    assert r.status == OrderStatus.FILLED and r.fills[0].price == Decimal("100015.01")


# --------------------------------------------------------------------
# Equity, positions, reconcile, restart
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_equity_and_positions():
    a, market, _ = await paper()
    await a.submit_order(order(qty="0.05", limit="100020"))
    await a.submit_order(order(qty="0.01", limit="99000"))          # rests
    eq = await a.get_account_equity()
    cash = a.state.balances["USD"]
    assert eq.total == cash + Decimal("0.05") * Decimal("100005")
    assert eq.available == cash - Decimal("0.01") * Decimal("99000") * Decimal("1.006")
    assert [(p.symbol, p.qty) for p in await a.get_positions()] == [("BTC-USD", Decimal("0.05"))]


@pytest.mark.asyncio
async def test_reconcile_and_restart_resume_the_same_account():
    ledger = MemPaperLedger()
    a, market, _ = await paper(ledger)
    filled = order(qty="0.05", limit="100020")
    resting = order(qty="0.01", limit="99000")
    await a.submit_order(filled)
    await a.submit_order(resting)
    snap = await a.reconcile()
    assert [p.symbol for p in snap.positions] == ["BTC-USD"]
    assert [o.client_order_id for o in snap.open_orders] == [resting.client_order_id]
    assert [f.client_order_id for f in snap.recent_fills] == [filled.client_order_id]

    b, _, _ = await paper(ledger, market=market)                    # restart
    assert b.state.balances == a.state.balances
    market.books["BTC-USD"] = (Decimal("98990"), Decimal("98995"))
    got = []
    async def cb(f): got.append(f)
    assert await b.check_resting_once(cb) == 1
    assert got[0].client_order_id == resting.client_order_id


@pytest.mark.asyncio
async def test_fee_falls_back_when_the_tier_cannot_be_read():
    market = FakeMarket(fee_bps=RuntimeError("no tier"))
    a, _, _ = await paper(market=market, fallback_fee_bps=Decimal("120"))
    r = await a.submit_order(order(qty="0.01", limit="100020"))
    assert r.fills[0].fee == Decimal("0.01") * Decimal("100020") * Decimal("0.012")


# --------------------------------------------------------------------
# End to end under the real executor
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_executor_paper_round_trip():
    from test_signal_executor import enter_row, exit_row, make_hooks
    from execution.signal_executor import SignalExecutor

    a, market, ledger = await paper()
    hooks, state = make_hooks(size=Decimal("0.05"))
    hooks.get_touch = a.get_touch
    ex = SignalExecutor(a, hooks)
    await ex.start()

    row = enter_row(symbol="BTC-USD")
    row["asset_class"] = "crypto"
    await ex.handle_signal(row)
    trade = next(iter(ex._open.values()))
    assert trade.state == "OPEN" and trade.filled_qty == Decimal("0.05")

    xrow = exit_row(symbol="BTC-USD")
    xrow["asset_class"] = "crypto"
    await ex.handle_signal(xrow)
    assert trade.state == "CLOSED"
    assert ledger.state.balances["BTC"] == 0
    assert ledger.state.balances["USD"] < Decimal("10000")          # paid the spread caps + fees
    assert len(ledger.state.fills) == 2


@pytest.mark.asyncio
async def test_executor_exit_that_cannot_fill_rests_then_fills_on_the_stream():
    from test_signal_executor import enter_row, exit_row, make_hooks
    from execution.signal_executor import SignalExecutor

    a, market, ledger = await paper()
    hooks, state = make_hooks(size=Decimal("0.05"))
    hooks.get_touch = a.get_touch
    ex = SignalExecutor(a, hooks)
    await ex.start()
    row = enter_row(symbol="BTC-USD"); row["asset_class"] = "crypto"
    await ex.handle_signal(row)
    trade = next(iter(ex._open.values()))
    assert trade.state == "OPEN"

    # The executor prices the exit off a touch above the real book, so its
    # SELL limit sits above the real bid and rests at the paper venue...
    async def stale_high(symbol, ac):
        return Touch(Decimal("101000"), Decimal("101010"))
    hooks.get_touch = stale_high
    xrow = exit_row(symbol="BTC-USD"); xrow["asset_class"] = "crypto"
    await ex.handle_signal(xrow)
    assert trade.state == "CLOSING"
    async def deliver(f): await ex._on_fill(f)
    assert await a.check_resting_once(deliver) == 0
    # ...until the real book rises to meet it; the fill arrives on the stream.
    market.books["BTC-USD"] = (Decimal("101000"), Decimal("101010"))
    assert await a.check_resting_once(deliver) == 1
    assert trade.state == "CLOSED"
    assert ledger.state.balances["BTC"] == 0


# --------------------------------------------------------------------
# Factory and mode-change safety
# --------------------------------------------------------------------

def test_factory_off_and_paper_build_the_paper_adapter_over_read_only_reads():
    from test_coinbase_adapter import KEY_NAME, PORTFOLIO, ec_pem
    from execution.adapter_factory import build_coinbase_adapter
    from execution.coinbase_adapter import CoinbaseAdapter

    _, pem = ec_pem()
    cfg = {"api_key_name": KEY_NAME, "api_private_key": pem, "portfolio_uuid": PORTFOLIO}
    ledger = MemPaperLedger().hooks()
    for mode in (TradeMode.OFF, TradeMode.PAPER):
        a, kind = build_coinbase_adapter(cfg, mode, paper_ledger=ledger,
                                         paper_starting_cash=500, fallback_fee_bps=99)
        assert isinstance(a, PaperCoinbaseAdapter) and kind == "paper"
        assert isinstance(a.market, ReadOnlyAdapter)
        assert a.market.inner.require_trade is False
        assert a.starting_cash == Decimal("500") and a.fallback_fee_bps == Decimal("99")
    live, kind = build_coinbase_adapter(cfg, TradeMode.LIVE, paper_ledger=ledger)
    assert isinstance(live, CoinbaseAdapter) and kind == "live"
    with pytest.raises(ValueError, match="paper ledger"):
        build_coinbase_adapter(cfg, TradeMode.PAPER)


@pytest.mark.asyncio
async def test_venue_change_blocks_writes_and_stops_for_a_restart():
    import run_executor
    from execution.gated_adapter import GatedAdapter

    class Mode:
        def is_enabled(self): return True
    a, _, _ = await paper()
    gated = GatedAdapter(a, Mode())
    current, stop, flag = {"adapter": a, "gated": gated}, asyncio.Event(), {"flag": False}
    cb = run_executor.make_on_mode_change(current, stop, flag)

    await cb(TradeMode.OFF, TradeMode.PAPER)             # same venue: nothing
    assert not stop.is_set() and gated.inner is a

    await cb(TradeMode.PAPER, TradeMode.LIVE)            # venue change
    assert stop.is_set() and flag["flag"]
    r = await gated.submit_order(order())
    assert r.status == OrderStatus.REJECTED and r.reject_reason == READ_ONLY_REASON
    await cb(TradeMode.LIVE, TradeMode.PAPER)            # idempotent: not double-wrapped
    assert not isinstance(gated.inner.inner, ReadOnlyAdapter)


# --------------------------------------------------------------------
# Shadow book fee tier
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_shadow_book_charges_the_live_fee_tier_with_fallback():
    from execution.shadow_book import ShadowBook
    from test_executor_hardening import MemShadowStore
    from test_signal_executor import enter_row

    async def tier(): return Decimal("40")
    store = MemShadowStore()
    book = ShadowBook(store.hooks(), taker_fee_bps=Decimal("120"), fee_bps_fn=tier)
    await book.start()
    await book.on_enter(enter_row(direction="SHORT"))
    assert store.rows[("two_ladder", "chainX:S")]["fee_bps"] == Decimal("40")

    async def broken(): raise RuntimeError("down")
    store2 = MemShadowStore()
    book2 = ShadowBook(store2.hooks(), taker_fee_bps=Decimal("120"), fee_bps_fn=broken)
    await book2.on_enter(enter_row(direction="SHORT"))
    assert store2.rows[("two_ladder", "chainX:S")]["fee_bps"] == Decimal("120")
