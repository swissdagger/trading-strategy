"""Each event the owner asked to be alerted on produces an alert-tagged log
record (ops.alerts forwards exactly those to Slack): a real or shadow trade
opening, a HALT, a burst of Sumtyme failures, processing lag, the kill switch
changing, and the daily summary."""
import asyncio
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from test_executor_hardening import booted, shadow_executor
from test_signal_executor import enter_row, make_hooks


def _alerts(caplog):
    return [(r.alert, r.getMessage()) for r in caplog.records if getattr(r, "alert", None)]


@pytest.mark.asyncio
async def test_a_real_trade_opening_alerts_with_mode_strategy_and_asset(caplog):
    caplog.set_level(logging.INFO)
    ex, _ = await booted(mode_label=lambda: "paper")
    await ex.handle_signal(enter_row(strategy="confirmed_15m", direction="LONG"))
    opened = [m for k, m in _alerts(caplog) if k.startswith("open:")]
    assert len(opened) == 1
    msg = opened[0]
    assert msg.startswith("PAPER trade OPENED: LONG AAPL equity")
    assert "confirmed_15m" in msg and "@ " in msg and "@ ?" not in msg


@pytest.mark.asyncio
async def test_a_shadow_trade_opening_alerts_as_shadow(caplog):
    caplog.set_level(logging.INFO)
    ex, adapter, _ = await shadow_executor()
    await ex.handle_signal(enter_row(direction="SHORT"))
    opened = [m for k, m in _alerts(caplog) if k.startswith("shadow-open:")]
    assert len(opened) == 1 and opened[0].startswith("SHADOW trade OPENED: SHORT AAPL")
    assert "two_ladder" in opened[0]
    # A replayed ENTER does not alert twice.
    await ex.handle_signal(enter_row(direction="SHORT"))
    assert len([k for k, _ in _alerts(caplog) if k.startswith("shadow-open:")]) == 1


@pytest.mark.asyncio
async def test_a_halt_alerts(caplog):
    caplog.set_level(logging.INFO)
    ex, _ = await booted()
    await ex.handle_signal(enter_row())
    (trade,) = ex._open.values()
    await ex._halt(trade, "escalation: exit unfilled through full ladder")
    halts = [m for k, m in _alerts(caplog) if k.startswith("halt:")]
    assert len(halts) == 1 and "HALTED" in halts[0] and "NOT being managed" in halts[0]


def test_a_burst_of_sumtyme_failures_alerts_once_one_failure_does_not(caplog, monkeypatch):
    from producer import service2_machine as sm
    caplog.set_level(logging.INFO)
    monkeypatch.setattr(sm, "_forecast_failures", sm.deque())
    sm._note_forecast_failure("timeout")
    assert not _alerts(caplog)
    for _ in range(sm.FORECAST_FAILURE_BURST - 1):
        sm._note_forecast_failure("timeout")
    assert [k for k, _ in _alerts(caplog)] == ["sumtyme-failing"]


def test_processing_lag_is_measured_from_the_bar_close(caplog):
    import time
    from producer.bar import OHLCPoint
    from signal_process import SignalProcess

    caplog.set_level(logging.INFO)
    proc = SignalProcess.__new__(SignalProcess)
    proc.cfg = type("Cfg", (), {"lag_warn_s": 300.0})()
    proc.workers = None
    now_ms = int(time.time() * 1000)
    # A 1h bar that closed 60 s ago is NOT behind (its open was 61 min ago).
    proc._observe_lag(OHLCPoint("BTC-USD", now_ms - 3_660_000, 1, 1, 1, 1, "1h", "crypto"))
    assert not _alerts(caplog)
    assert proc.bars_flowing()
    # A 1m bar that closed 10 min ago is.
    proc._observe_lag(OHLCPoint("BTC-USD", now_ms - 660_000, 1, 1, 1, 1, "1m", "crypto"))
    assert [k for k, _ in _alerts(caplog)] == ["lag:BTC-USD"]


# --------------------------------------------------------------------------
# Database-backed: kill switch monitor and daily summary
# --------------------------------------------------------------------------

DSN = os.environ.get("QISS_TEST_DSN")
needs_db = pytest.mark.skipif(not DSN, reason="QISS_TEST_DSN not set; needs real Postgres")


@pytest_asyncio.fixture
async def pool():
    import asyncpg
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=4, statement_cache_size=0)
    await p.execute("UPDATE kill_switch SET engaged = false, mode = 'halt_new', reason = NULL")
    yield p
    await p.execute("UPDATE kill_switch SET engaged = false, mode = 'halt_new', reason = NULL")
    await p.close()


@needs_db
@pytest.mark.asyncio
async def test_kill_switch_changes_alert(pool, caplog):
    from ops.summary import kill_switch_monitor
    caplog.set_level(logging.INFO)
    task = asyncio.create_task(kill_switch_monitor(pool, interval_s=0.01))
    await asyncio.sleep(0.05)
    assert not _alerts(caplog)                               # off at start: quiet
    await pool.execute("UPDATE kill_switch SET engaged = true, mode = 'flatten', "
                       "reason = 'test'")
    await asyncio.sleep(0.05)
    await pool.execute("UPDATE kill_switch SET engaged = false")
    await asyncio.sleep(0.05)
    task.cancel()
    msgs = [m for _, m in _alerts(caplog)]
    assert len(msgs) == 2
    assert "ENGAGED (flatten): test" in msgs[0] and "closing every position" in msgs[0]
    assert "released" in msgs[1]


@needs_db
@pytest.mark.asyncio
async def test_daily_summary_reports_real_and_shadow_trades(pool):
    from ops.summary import build_daily_summary
    since = datetime.now(timezone.utc) - timedelta(days=1)
    pk = f"sum-{uuid.uuid4()}:L"
    await pool.execute(
        """INSERT INTO shadow_trades (strategy, position_key, symbol, asset_class, side,
                                      entry_signal_id, entry_px, entry_at, exit_signal_id,
                                      exit_px, exit_at, exit_reason, fee_bps, pnl_bps)
           VALUES ('rule3_v1', $1, 'BTC-USD', 'crypto', 'LONG', $2, 100, now(), $3,
                   101, now(), 'STOP', 120, -140)""", pk, pk + "e", pk + "x")
    try:
        text = await build_daily_summary(pool, since, mode="paper")
    finally:
        await pool.execute("DELETE FROM shadow_trades WHERE position_key = $1", pk)
    assert "trade mode *paper*" in text and "kill switch off" in text
    assert "rule3_v1: 1 trade(s), mean -140.0" in text


@pytest.mark.asyncio
async def test_shadow_opens_batch_into_one_message(caplog):
    from decimal import Decimal
    from execution.shadow_book import ShadowBook
    from ops.alerts import AlertDigest
    from test_executor_hardening import MemShadowStore

    caplog.set_level(logging.INFO)
    digest = AlertDigest("SHADOW trades opened", "shadow-digest")
    book = ShadowBook(MemShadowStore().hooks(), taker_fee_bps=Decimal("120"),
                      opened_digest=digest)
    await book.start()
    ex, _ = await booted(shadow_book=book)
    await ex.handle_signal(enter_row(chain="c1", direction="SHORT"))
    await ex.handle_signal(enter_row(chain="c2", direction="SHORT", symbol="MSFT"))
    assert not _alerts(caplog)                            # nothing per open
    assert digest.flush() == 2
    (key, msg), = _alerts(caplog)
    assert key.startswith("shadow-digest:")
    assert msg.startswith("SHADOW trades opened (2):") and "AAPL" in msg and "MSFT" in msg
    assert digest.flush() == 0 and len(_alerts(caplog)) == 1   # empty interval: no post


@pytest.mark.asyncio
async def test_a_manual_trade_strategy_alerts_each_open_and_close_at_once(caplog):
    # rule1_v1's trades are placed by hand: open and close each post to Slack
    # immediately, naming the venue, and never join the batched digest.
    from decimal import Decimal
    from execution.shadow_book import ShadowBook
    from ops.alerts import AlertDigest
    from test_executor_hardening import MemShadowStore
    from test_signal_executor import exit_row

    caplog.set_level(logging.INFO)
    digest = AlertDigest("SHADOW trades opened", "shadow-digest")
    book = ShadowBook(MemShadowStore().hooks(), taker_fee_bps=Decimal("5"),
                      opened_digest=digest, manual_strategies={"rule1_v1": "Trading 212"})
    await book.start()
    ex, _ = await booted(shadow_book=book, shadow_only_strategies=frozenset({"rule1_v1"}))
    await ex.handle_signal(enter_row(strategy="rule1_v1", symbol="NVDA", direction="LONG"))
    (key, msg), = _alerts(caplog)
    assert key.startswith("manual-open:rule1_v1:")
    assert msg.startswith("MANUAL TRADE — OPEN LONG NVDA @ 100.0 on Trading 212")
    assert "rule1_v1" in msg and "signal " in msg
    assert digest.flush() == 0                       # not batched
    await ex.handle_signal(exit_row(strategy="rule1_v1", symbol="NVDA", direction="LONG"))
    (key, msg), = [(k, m) for k, m in _alerts(caplog) if k.startswith("manual-close:")]
    assert msg.startswith("MANUAL TRADE — CLOSE LONG NVDA @ 101.0 on Trading 212")
    assert "bps net" in msg
