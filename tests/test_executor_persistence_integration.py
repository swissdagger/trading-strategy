"""
Integration tests for the Phase 3 persistence (migrations 019-021) against a
real Postgres with all migrations applied: shadow_trades, executor_cursor and
executor_alerts, through the same make_* factories run_executor wires in.
"""
import asyncio
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio

from execution.persistence import (
    make_alert, make_load_executor_cursor, make_save_executor_cursor,
    make_shadow_hooks,
)
from execution.shadow_book import ShadowBook
from execution.types import Touch, TradeSide

DSN = os.environ.get("QISS_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="QISS_TEST_DSN not set; needs real Postgres")


@pytest_asyncio.fixture
async def pool():
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=4, statement_cache_size=0)
    await p.execute("TRUNCATE shadow_trades, executor_cursor, executor_alerts")
    yield p
    await p.close()


def _row(direction="SHORT", action="ENTER", reason=None, sid=None):
    pkey = f"chainX:{'L' if direction == 'LONG' else 'S'}"
    return {"signal_id": sid or f"sig-{pkey}-{action}", "position_key": pkey,
            "direction": direction, "action": action, "symbol": "BTC-USD",
            "asset_class": "crypto", "strategy": "confirmed_15m",
            "decision_price": 100.0, "exit_reason": reason}


@pytest.mark.asyncio
async def test_shadow_round_trip_persists_and_reloads(pool):
    touch = {"t": Touch(Decimal("100"), Decimal("100.10"))}
    async def get_touch(sym, ac):
        return touch["t"]
    book = ShadowBook(make_shadow_hooks(pool, get_touch), taker_fee_bps=Decimal("120"))
    await book.start()
    await book.on_enter(_row())
    await book.on_enter(_row())                       # replay: still one row
    assert await pool.fetchval("SELECT count(*) FROM shadow_trades") == 1

    # A fresh book (restart) reloads the open position.
    book2 = ShadowBook(make_shadow_hooks(pool, get_touch), taker_fee_bps=Decimal("120"))
    await book2.start()
    assert book2.holds("confirmed_15m", "chainX:S")

    touch["t"] = Touch(Decimal("98"), Decimal("98.10"))
    await book2.on_exit(_row(action="EXIT", reason="TARGET"))
    r = await pool.fetchrow("SELECT * FROM shadow_trades")
    assert r["side"] == "SHORT"
    assert r["entry_px"] == Decimal("100")
    assert r["exit_px"] == Decimal("98.10")
    assert r["pnl_bps"] == Decimal("-50.0000")
    assert r["exit_reason"] == "TARGET"

    book3 = ShadowBook(make_shadow_hooks(pool, get_touch), taker_fee_bps=Decimal("120"))
    await book3.start()
    assert book3.open_count() == 0


@pytest.mark.asyncio
async def test_shadow_exit_columns_are_all_or_nothing(pool):
    await pool.execute(
        """INSERT INTO shadow_trades (strategy, position_key, symbol, asset_class,
               side, entry_signal_id, entry_px, entry_at, fee_bps)
           VALUES ('s', 'c:S', 'BTC-USD', 'crypto', 'SHORT', 'e1', 100, now(), 120)""")
    with pytest.raises(asyncpg.CheckViolationError):
        await pool.execute("UPDATE shadow_trades SET exit_px = 99")


@pytest.mark.asyncio
async def test_executor_cursor_load_save_monotonic(pool):
    load, save = make_load_executor_cursor(pool), make_save_executor_cursor(pool)
    assert await load() is None
    t = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)
    await save(t)
    assert await load() == t
    await save(t - timedelta(minutes=5))              # out-of-order write
    assert await load() == t
    await save(t + timedelta(minutes=1))
    assert await load() == t + timedelta(minutes=1)


@pytest.mark.asyncio
async def test_alert_row_and_notify(pool):
    got = asyncio.get_running_loop().create_future()
    conn = await asyncpg.connect(DSN, statement_cache_size=0)
    try:
        await conn.add_listener("executor_alert",
                                lambda *a: got.done() or got.set_result(a[3]))
        await make_alert(pool)("trade_halted", {
            "trade_id": "t-1", "symbol": "BTC-USD", "reason": "test", "qty": Decimal("1")})
        alert_id = await asyncio.wait_for(got, timeout=5)
    finally:
        await conn.close()
    r = await pool.fetchrow("SELECT * FROM executor_alerts")
    assert str(r["alert_id"]) == alert_id
    assert r["kind"] == "trade_halted"
    assert r["trade_id"] == "t-1" and r["symbol"] == "BTC-USD"
    assert r["delivered_at"] is None


# --------------------------------------------------------------------
# Paper ledger (migration 022)
# --------------------------------------------------------------------

@pytest_asyncio.fixture
async def paper_pool():
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=4, statement_cache_size=0)
    await p.execute("TRUNCATE paper_fills, paper_orders, paper_balances")
    yield p
    await p.close()


@pytest.mark.asyncio
async def test_paper_account_persists_and_resumes(paper_pool):
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from test_paper_adapter import FakeMarket, order
    from execution.paper_adapter import PaperCoinbaseAdapter
    from execution.persistence import make_paper_ledger_hooks

    market = FakeMarket()
    def adapter():
        return PaperCoinbaseAdapter(market, make_paper_ledger_hooks(paper_pool),
                                    starting_cash=Decimal("10000"))
    a = adapter()
    await a.connect()
    filled = order(qty="0.05", limit="100020")
    resting = order(qty="0.01", limit="99000")
    r = await a.submit_order(filled)
    await a.submit_order(resting)
    assert r.status.value == "FILLED"

    bal = {row["currency"]: row["qty"] for row in
           await paper_pool.fetch("SELECT currency, qty FROM paper_balances")}
    assert bal["BTC"] == Decimal("0.05")
    assert bal["USD"] == a.state.balances["USD"]
    assert await paper_pool.fetchval("SELECT count(*) FROM paper_fills") == 1

    b = adapter()                                     # restart
    await b.connect()
    assert b.state.balances == a.state.balances
    assert b.state.orders[resting.client_order_id].status == "OPEN"
    assert [f.client_order_id for f in b.state.fills] == [filled.client_order_id]
    market.books["BTC-USD"] = (Decimal("98990"), Decimal("98995"))
    got = []
    async def cb(f): got.append(f)
    assert await b.check_resting_once(cb) == 1
    status = await paper_pool.fetchval(
        "SELECT status FROM paper_orders WHERE client_order_id = $1", resting.client_order_id)
    assert status == "FILLED"


@pytest.mark.asyncio
async def test_paper_fill_writes_are_atomic(paper_pool):
    from execution.paper_adapter import PaperOrder
    from execution.persistence import make_paper_ledger_hooks
    from execution.types import Fill, OrderIntent

    h = make_paper_ledger_hooks(paper_pool)
    await h.seed({"USD": Decimal("1000")})
    await h.seed({"USD": Decimal("5")})               # second seed is a no-op
    o = PaperOrder("cid-1", "paper-1", "BTC-USD", "BUY", "ENTRY", Decimal("0.001"),
                   Decimal("100000"), "OPEN")
    await h.save_order(o)
    fill = Fill(venue_exec_id="f-1", qty=Decimal("0.001"), price=Decimal("100000"),
                intent=OrderIntent.ENTRY, filled_at_ms=1_791_000_000_000,
                fee=Decimal("0.6"), fee_currency="USD", venue_order_id="paper-1",
                client_order_id="cid-1")
    deltas = {"USD": Decimal("-100.6"), "BTC": Decimal("0.001")}
    filled = PaperOrder(**{**o.__dict__, "status": "FILLED", "filled_qty": Decimal("0.001")})
    await h.apply_fill(fill, "BTC-USD", filled, deltas)
    with pytest.raises(asyncpg.UniqueViolationError):          # same fill id again
        await h.apply_fill(fill, "BTC-USD", filled, deltas)
    bal = {r["currency"]: r["qty"] for r in
           await paper_pool.fetch("SELECT currency, qty FROM paper_balances")}
    assert bal == {"USD": Decimal("899.4"), "BTC": Decimal("0.001")}   # applied once
