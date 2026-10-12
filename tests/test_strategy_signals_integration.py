"""
Integration test: the strategy column + strategy-aware signal_id resolves the
cross-strategy collision in the durable trade_signals table.

Two strategies deciding on the SAME chain/direction/symbol must produce TWO
distinct rows (distinct signal_id, tagged with distinct strategy), not one
deduped row. Runs against real Postgres with migration 011 applied.
"""
import os
from datetime import datetime, timezone
import uuid

import asyncpg
import pytest

from execution.signal_writer import SignalWriter, SignalWriterHooks
from producer.persistence import make_insert_signal
from execution.types import TradeSide

DSN = os.environ.get("QISS_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="QISS_TEST_DSN not set; needs real Postgres")


@pytest.fixture
async def pool():
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=4, statement_cache_size=0)
    await p.execute("TRUNCATE trade_signals")
    yield p
    await p.close()


@pytest.mark.asyncio
async def test_two_strategies_same_chain_distinct_rows(pool):
    insert = make_insert_signal(pool)
    chain_id = f"chain-{uuid.uuid4().hex[:8]}"

    # Two writers, same chain, different strategies.
    w1 = SignalWriter(SignalWriterHooks(insert_signal=insert), strategy="two_ladder")
    w2 = SignalWriter(SignalWriterHooks(insert_signal=insert), strategy="mean_revert")

    k1 = await w1.emit_entry(
        chain_id=chain_id, side=TradeSide.LONG, symbol="AAPL",
        asset_class="equity", decided_at=datetime(2026,1,1,tzinfo=timezone.utc),
    )
    k2 = await w2.emit_entry(
        chain_id=chain_id, side=TradeSide.LONG, symbol="AAPL",
        asset_class="equity", decided_at=datetime(2026,1,1,tzinfo=timezone.utc),
    )

    # Base position_key is the same (chain:side) — that's fine; strategy
    # disambiguates.
    assert k1 == k2 == f"{chain_id}:L"

    rows = await pool.fetch(
        "SELECT signal_id, strategy, position_key FROM trade_signals WHERE chain_id = $1 ORDER BY strategy",
        chain_id,
    )
    # TWO distinct rows, one per strategy — NOT deduped into one.
    assert len(rows) == 2
    assert rows[0]["strategy"] == "mean_revert"
    assert rows[1]["strategy"] == "two_ladder"
    # Distinct signal_ids (the PK) — this is what stops the ON CONFLICT dedupe
    # from collapsing them.
    assert rows[0]["signal_id"] != rows[1]["signal_id"]


@pytest.mark.asyncio
async def test_same_strategy_replay_still_dedupes(pool):
    # Within ONE strategy, a replayed emit must STILL dedupe (unchanged
    # behaviour — the strategy-aware id is deterministic per strategy).
    insert = make_insert_signal(pool)
    chain_id = f"chain-{uuid.uuid4().hex[:8]}"
    w = SignalWriter(SignalWriterHooks(insert_signal=insert), strategy="two_ladder")

    await w.emit_entry(chain_id=chain_id, side=TradeSide.LONG, symbol="AAPL",
                       asset_class="equity", decided_at=datetime(2026,1,1,tzinfo=timezone.utc))
    # Replay the identical decision.
    await w.emit_entry(chain_id=chain_id, side=TradeSide.LONG, symbol="AAPL",
                       asset_class="equity", decided_at=datetime(2026,1,1,tzinfo=timezone.utc))

    rows = await pool.fetch("SELECT signal_id FROM trade_signals WHERE chain_id = $1", chain_id)
    assert len(rows) == 1   # deduped — one row despite two emits


@pytest.mark.asyncio
async def test_default_strategy_backfill_value(pool):
    # A writer with no explicit strategy uses 'two_ladder' (the migration
    # default), so single-strategy and legacy behaviour is unchanged.
    insert = make_insert_signal(pool)
    chain_id = f"chain-{uuid.uuid4().hex[:8]}"
    w = SignalWriter(SignalWriterHooks(insert_signal=insert))  # no strategy arg
    await w.emit_entry(chain_id=chain_id, side=TradeSide.SHORT, symbol="MSFT",
                       asset_class="equity", decided_at=datetime(2026,1,1,tzinfo=timezone.utc))
    row = await pool.fetchrow("SELECT strategy FROM trade_signals WHERE chain_id = $1", chain_id)
    assert row["strategy"] == "two_ladder"
