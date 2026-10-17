"""The chain rules' restart reconcile queries against real Postgres: opposing
initiations on the rule's own ladder after the entry (by wall time), and
whether a chain reached the confirm level inside the stall window."""
import os
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

DSN = os.environ.get("QISS_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="QISS_TEST_DSN not set; needs real Postgres")
UTC = timezone.utc
T = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)


@pytest_asyncio.fixture
async def pool():
    import asyncpg
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=2, statement_cache_size=0)
    await p.execute("DELETE FROM chains WHERE chain_id LIKE 'rc-%'")
    yield p
    await p.execute("DELETE FROM chains WHERE chain_id LIKE 'rc-%'")
    await p.close()


async def _chain(pool, cid, direction, ladder, steps):
    """steps: [(level, recorded_at)]; step 0 is the initiation."""
    await pool.execute(
        """INSERT INTO chains (chain_id, symbol, asset_class, ladder, direction, origin_time,
                               active_timeframes) VALUES ($1, 'RC-USD', 'crypto', $2, $3, $4, '{1m}')""",
        cid, ladder, direction, T - timedelta(days=1))   # market time deliberately old
    for i, (level, at) in enumerate(steps):
        await pool.execute(
            """INSERT INTO chain_steps (chain_id, step_index, level, timeframe, step_time, recorded_at)
               VALUES ($1, $2, $3, '1m', $4, $5)""", cid, i, level, T, at)


@pytest.mark.asyncio
async def test_opposing_initiation_is_matched_on_ladder_and_wall_time(pool):
    from producer.persistence import make_load_opposing_chain_since
    load = make_load_opposing_chain_since(pool)
    await _chain(pool, "rc-other-ladder", -1, "CN", [(1, T + timedelta(minutes=5))])
    await _chain(pool, "rc-before", -1, "FC", [(1, T - timedelta(minutes=5))])
    assert not await load("RC-USD", -1, T, "FC")
    await _chain(pool, "rc-after", -1, "FC", [(1, T + timedelta(minutes=5))])
    assert await load("RC-USD", -1, T, "FC")


@pytest.mark.asyncio
async def test_confirmed_means_reached_the_level_by_the_deadline(pool):
    from producer.persistence import make_load_chain_confirmed
    load = make_load_chain_confirmed(pool)
    await _chain(pool, "rc-c", 1, "FC", [(1, T), (2, T + timedelta(minutes=1)),
                                          (3, T + timedelta(minutes=40))])
    assert await load("rc-c", 3, T + timedelta(minutes=60))
    assert not await load("rc-c", 3, T + timedelta(minutes=30))
    assert not await load("rc-c", 4, T + timedelta(minutes=60))
