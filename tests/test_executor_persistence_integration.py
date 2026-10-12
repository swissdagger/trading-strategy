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
            "asset_class": "crypto", "strategy": "four_chain",
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
    assert book2.holds("four_chain", "chainX:S")

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
