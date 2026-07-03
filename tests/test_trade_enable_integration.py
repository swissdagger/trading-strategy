"""
Integration tests for execution/trade_enable.py against real Postgres with
migration 010 applied.

Proves:
  - initial read reflects the DB row (disabled by default)
  - a live UPDATE to enabled=true is picked up (via NOTIFY fast path)
  - config_default is used only until the DB is read
  - fail-safe: unknown state returns config default (which defaults False)
"""
import asyncio
import os

import asyncpg
import pytest

from execution.trade_enable import TradeEnableState

DSN = os.environ.get("QISS_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="QISS_TEST_DSN not set; needs real Postgres")


@pytest.fixture
async def pool():
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=4, statement_cache_size=0)
    # Reset to the default disabled state before each test.
    await p.execute("UPDATE trade_enabled SET enabled = false WHERE id = true")
    yield p
    await p.close()


async def _set_enabled(pool, value: bool):
    await pool.execute("UPDATE trade_enabled SET enabled = $1 WHERE id = true", value)


@pytest.mark.asyncio
async def test_initial_read_reflects_db(pool):
    state = TradeEnableState(DSN, pool, config_default=True)  # default True...
    await state.start()
    try:
        # ...but the DB says disabled, and the DB wins once read.
        assert state.is_enabled() is False
    finally:
        await state.stop()


@pytest.mark.asyncio
async def test_live_enable_picked_up(pool):
    state = TradeEnableState(DSN, pool, config_default=False, poll_interval_s=0.5)
    await state.start()
    try:
        assert state.is_enabled() is False
        await _set_enabled(pool, True)      # flip live
        # NOTIFY should refresh the cache promptly.
        for _ in range(30):
            if state.is_enabled():
                break
            await asyncio.sleep(0.1)
        assert state.is_enabled() is True
    finally:
        await state.stop()


@pytest.mark.asyncio
async def test_live_disable_picked_up(pool):
    await _set_enabled(pool, True)
    state = TradeEnableState(DSN, pool, config_default=False, poll_interval_s=0.5)
    await state.start()
    try:
        assert state.is_enabled() is True
        await _set_enabled(pool, False)     # flip back off
        for _ in range(30):
            if not state.is_enabled():
                break
            await asyncio.sleep(0.1)
        assert state.is_enabled() is False
    finally:
        await state.stop()


@pytest.mark.asyncio
async def test_config_default_before_first_read():
    # Without start() (no read yet), is_enabled falls back to config_default.
    # Use a bogus pool that we never query — is_enabled must not touch it.
    state = TradeEnableState(DSN, pool=None, config_default=True)
    assert state.is_enabled() is True
    state2 = TradeEnableState(DSN, pool=None, config_default=False)
    assert state2.is_enabled() is False


@pytest.mark.asyncio
async def test_refresh_returns_current(pool):
    state = TradeEnableState(DSN, pool, config_default=False)
    await state.start()
    try:
        await _set_enabled(pool, True)
        # Force an immediate re-read rather than waiting for NOTIFY/poll.
        assert await state.refresh() is True
    finally:
        await state.stop()
