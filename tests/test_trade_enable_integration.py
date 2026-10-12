"""
Integration tests for execution/trade_enable.py (three-state mode) against real
Postgres with migrations 010 + 012 applied.

Proves:
  - initial read reflects the DB mode
  - is_enabled() is derived (paper OR live -> True, off -> False)
  - live mode flip is picked up (via NOTIFY)
  - the on_mode_change callback fires on a genuine mode change (the reconnect
    hook), with the right (old, new)
  - config default applies before the first read; fail-safe is OFF
"""
import asyncio
import os

import asyncpg
import pytest
import pytest_asyncio

from execution.trade_enable import TradeEnableState, TradeMode

DSN = os.environ.get("QISS_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="QISS_TEST_DSN not set; needs real Postgres")


@pytest_asyncio.fixture
async def pool():
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=4, statement_cache_size=0)
    await p.execute("UPDATE trade_enabled SET mode = 'off' WHERE id = true")
    yield p
    await p.close()


async def _set_mode(pool, mode: str):
    await pool.execute("UPDATE trade_enabled SET mode = $1 WHERE id = true", mode)


@pytest.mark.asyncio
async def test_initial_read_reflects_db_mode(pool):
    await _set_mode(pool, "paper")
    state = TradeEnableState(DSN, pool, config_default=TradeMode.LIVE)  # default live...
    await state.start()
    try:
        # ...but the DB says paper, and the DB wins once read.
        assert state.mode() == TradeMode.PAPER
        assert state.is_enabled() is True     # paper is "enabled"
        assert state.is_paper() is True
        assert state.is_live() is False
    finally:
        await state.stop()


@pytest.mark.asyncio
async def test_off_is_not_enabled(pool):
    await _set_mode(pool, "off")
    state = TradeEnableState(DSN, pool)
    await state.start()
    try:
        assert state.mode() == TradeMode.OFF
        assert state.is_enabled() is False
    finally:
        await state.stop()


@pytest.mark.asyncio
async def test_live_mode_enabled_and_live(pool):
    await _set_mode(pool, "live")
    state = TradeEnableState(DSN, pool)
    await state.start()
    try:
        assert state.mode() == TradeMode.LIVE
        assert state.is_enabled() is True
        assert state.is_live() is True
    finally:
        await state.stop()


@pytest.mark.asyncio
async def test_mode_flip_picked_up(pool):
    state = TradeEnableState(DSN, pool, config_default=TradeMode.OFF, poll_interval_s=0.5)
    await state.start()
    try:
        assert state.mode() == TradeMode.OFF
        await _set_mode(pool, "paper")
        for _ in range(30):
            if state.mode() == TradeMode.PAPER:
                break
            await asyncio.sleep(0.1)
        assert state.mode() == TradeMode.PAPER
    finally:
        await state.stop()


@pytest.mark.asyncio
async def test_on_mode_change_callback_fires(pool):
    changes = []
    async def on_change(old, new):
        changes.append((old, new))
    state = TradeEnableState(DSN, pool, config_default=TradeMode.OFF,
                            on_mode_change=on_change, poll_interval_s=0.5)
    await state.start()
    try:
        # off -> live: a venue-changing transition; callback must fire.
        await _set_mode(pool, "live")
        for _ in range(30):
            if changes:
                break
            await asyncio.sleep(0.1)
        assert changes, "on_mode_change should have fired"
        old, new = changes[-1]
        assert old == TradeMode.OFF and new == TradeMode.LIVE
    finally:
        await state.stop()


@pytest.mark.asyncio
async def test_no_callback_on_initial_read(pool):
    await _set_mode(pool, "paper")
    changes = []
    async def on_change(old, new):
        changes.append((old, new))
    state = TradeEnableState(DSN, pool, on_mode_change=on_change)
    await state.start()
    try:
        # The initial read established paper; no transition, so no callback.
        await asyncio.sleep(0.2)
        assert changes == []
    finally:
        await state.stop()


@pytest.mark.asyncio
async def test_config_default_before_first_read():
    state = TradeEnableState(DSN, pool=None, config_default=TradeMode.PAPER)
    assert state.mode() == TradeMode.PAPER
    assert state.is_enabled() is True
    off = TradeEnableState(DSN, pool=None, config_default=TradeMode.OFF)
    assert off.is_enabled() is False


@pytest.mark.asyncio
async def test_paper_and_live_both_enabled_but_distinct(pool):
    await _set_mode(pool, "paper")
    state = TradeEnableState(DSN, pool, poll_interval_s=0.5)
    await state.start()
    try:
        assert state.is_enabled() and state.is_paper() and not state.is_live()
        await _set_mode(pool, "live")
        for _ in range(30):
            if state.is_live():
                break
            await asyncio.sleep(0.1)
        assert state.is_enabled() and state.is_live() and not state.is_paper()
    finally:
        await state.stop()