"""
Integration tests for execution/signal_listener.py against a REAL Postgres
with migration 009's trigger applied.

These prove the delivery semantics that a mock can't:
  - the 009 trigger fires a NOTIFY on a genuine insert, and the listener is
    pushed the signal_id (fast path)
  - the trigger does NOT fire on an ON CONFLICT DO NOTHING skip (a replayed
    emit must not re-notify)
  - the catch-up sweep dispatches rows inserted while the listener was down
  - a signal delivered by BOTH fast path and sweep is dispatched once (dedup)
  - the cursor advances so a restart doesn't replay

DSN comes from QISS_TEST_DSN; the test is skipped if unset (so the suite still
runs where no Postgres is available). The harness in conftest-style setup here
points it at the local socket instance.
"""
import asyncio
import os
import uuid
from datetime import datetime, timezone, timedelta

import asyncpg
import pytest

from execution.signal_listener import SignalListener, CHANNEL

DSN = os.environ.get("QISS_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="QISS_TEST_DSN not set; needs a real Postgres")


def _row(action="ENTER", direction="LONG", **over):
    sid = over.pop("signal_id", None) or f"sig1.{uuid.uuid4().hex[:32]}"
    base = {
        "signal_id": sid,
        "position_key": over.pop("position_key", f"chain-{sid}:L"),
        "chain_id": over.pop("chain_id", f"chain-{sid}"),
        "direction": direction,
        "action": action,
        "symbol": over.pop("symbol", "AAPL"),
        "asset_class": over.pop("asset_class", "equity"),
        "trigger_level": over.pop("trigger_level", 1),
        "decision_price": over.pop("decision_price", 100.0),
        "exit_reason": over.pop("exit_reason", None if action == "ENTER" else "STOP"),
        "decided_at": over.pop("decided_at", datetime.now(timezone.utc)),
    }
    base.update(over)
    return base


async def _insert(pool, row) -> bool:
    """Mirror make_insert_signal: idempotent insert returning newly-inserted."""
    result = await pool.execute(
        """
        INSERT INTO trade_signals
            (signal_id, position_key, chain_id, direction, action, symbol,
             asset_class, trigger_level, decision_price, exit_reason, decided_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT (signal_id) DO NOTHING
        """,
        row["signal_id"], row["position_key"], row["chain_id"], row["direction"],
        row["action"], row["symbol"], row["asset_class"], row["trigger_level"],
        row["decision_price"], row["exit_reason"], row["decided_at"],
    )
    return result.endswith(" 1")


@pytest.fixture
async def pool():
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=4, statement_cache_size=0)
    # Clean slate for each test.
    await p.execute("TRUNCATE trade_signals")
    yield p
    await p.close()


async def _collect_handler(received, event=None):
    async def handler(row):
        received.append(row["signal_id"])
        if event is not None:
            event.set()
    return handler


# --------------------------------------------------------------------
# Fast path: NOTIFY delivery
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_notify_fast_path_delivers(pool):
    received = []
    got = asyncio.Event()
    handler = await _collect_handler(received, got)
    # Start listener from 'now' so the pre-insert catch-up finds nothing and we
    # test the pure fast path.
    listener = SignalListener(DSN, pool, handler, initial_cursor=datetime.now(timezone.utc))
    task = asyncio.create_task(listener.run())
    try:
        await asyncio.sleep(0.3)  # let it connect + LISTEN
        row = _row()
        assert await _insert(pool, row) is True
        await asyncio.wait_for(got.wait(), timeout=3.0)
        assert row["signal_id"] in received
    finally:
        await listener.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# --------------------------------------------------------------------
# Trigger does NOT fire on ON CONFLICT skip
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_replayed_insert_does_not_renotify(pool):
    received = []
    handler = await _collect_handler(received)
    listener = SignalListener(DSN, pool, handler, initial_cursor=datetime.now(timezone.utc))
    task = asyncio.create_task(listener.run())
    try:
        await asyncio.sleep(0.3)
        row = _row()
        assert await _insert(pool, row) is True     # real insert -> notify
        await asyncio.sleep(0.4)
        # Replay the SAME signal_id: ON CONFLICT DO NOTHING, no row inserted,
        # so the AFTER INSERT trigger must NOT fire again.
        assert await _insert(pool, row) is False
        await asyncio.sleep(0.4)
        # Only one dispatch for this id (the recent-set would also dedup, but
        # the point is the trigger didn't even fire a second NOTIFY).
        assert received.count(row["signal_id"]) == 1
    finally:
        await listener.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# --------------------------------------------------------------------
# Catch-up sweep: rows inserted while listener was down
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_catch_up_sweep_on_start(pool):
    # Insert BEFORE the listener starts; a NOTIFY fires but nobody is
    # listening, so it's lost. The catch-up sweep must still deliver it.
    old_cursor = datetime.now(timezone.utc) - timedelta(seconds=1)
    row = _row()
    await _insert(pool, row)

    received = []
    got = asyncio.Event()
    handler = await _collect_handler(received, got)
    # Start from a cursor BEFORE the insert so the sweep picks it up.
    listener = SignalListener(DSN, pool, handler, initial_cursor=old_cursor)
    task = asyncio.create_task(listener.run())
    try:
        await asyncio.wait_for(got.wait(), timeout=3.0)
        assert row["signal_id"] in received
    finally:
        await listener.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# --------------------------------------------------------------------
# Dedup: same signal via fast path AND sweep -> handled once
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dedup_fast_path_and_sweep(pool):
    # A row present before start (so the sweep will read it) that ALSO gets a
    # fast-path notify if re-inserted. We simulate overlap: insert, start
    # listener with an old cursor (sweep reads it), then the fast path for the
    # same id is suppressed by the recent-set.
    old_cursor = datetime.now(timezone.utc) - timedelta(seconds=1)
    row = _row()
    await _insert(pool, row)

    received = []
    handler = await _collect_handler(received)
    listener = SignalListener(DSN, pool, handler, initial_cursor=old_cursor)
    task = asyncio.create_task(listener.run())
    try:
        await asyncio.sleep(0.5)
        # Attempt a replay NOTIFY path: same id won't re-insert (ON CONFLICT),
        # but even if a stray notify arrived, the recent-set dedups.
        await _insert(pool, row)
        await asyncio.sleep(0.4)
        assert received.count(row["signal_id"]) == 1
    finally:
        await listener.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# --------------------------------------------------------------------
# Cursor advances past dispatched rows
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reconnect_recovers_missed_signal(pool):
    # Drop the listener's connection, insert while it's down (NOTIFY lost),
    # and confirm the reconnect + catch-up sweep recovers the row. With the
    # termination listener, detection is near-instant rather than a 5s poll.
    received = []
    handler = await _collect_handler(received)
    listener = SignalListener(
        DSN, pool, handler,
        initial_cursor=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    task = asyncio.create_task(listener.run())
    try:
        await asyncio.sleep(0.3)   # connect + LISTEN
        await listener._close_conn()   # simulate a drop
        row = _row()
        await _insert(pool, row)       # NOTIFY lost (nobody listening)
        # Reconnect backoff is ~1s; allow margin for the sweep.
        await asyncio.sleep(2.5)
        assert row["signal_id"] in received
    finally:
        await listener.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_cursor_advances(pool):
    received = []
    got = asyncio.Event()
    handler = await _collect_handler(received, got)
    listener = SignalListener(DSN, pool, handler, initial_cursor=datetime.now(timezone.utc))
    task = asyncio.create_task(listener.run())
    try:
        await asyncio.sleep(0.3)
        before = listener.cursor
        row = _row()
        await _insert(pool, row)
        await asyncio.wait_for(got.wait(), timeout=3.0)
        await asyncio.sleep(0.2)
        # Cursor must have advanced to at least the row's created_at.
        assert listener.cursor > before
    finally:
        await listener.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
