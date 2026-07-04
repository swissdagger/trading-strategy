-- ============================================================================
-- 014_trades_strategy.sql
--
-- Adds STRATEGY provenance to `trades`, closing the gap between the execution
-- side and the multi-strategy signal side.
--
-- WHY. SignalExecutor keys every live position on (strategy, position_key) —
-- that is the tuple handle_signal builds, and the tuple OpenTrade carries. But
-- the 001 `trades` table predates multi-strategy: it identifies a trade by
-- (chain_id, side) and has no strategy column. So on restart, load_open_trades
-- can reconstruct position_key from (chain_id, side) but has NOTHING to populate
-- OpenTrade.strategy from, and must guess a default.
--
-- The guess is correct for a single-strategy deployment and WRONG the moment two
-- enabled strategies (e.g. two_ladder and four_chain, both enabled in config.py)
-- hold a position on the same chain in the same direction: both reconstruct to
-- the same (default_strategy, position_key) in-memory key and collide — the
-- second silently overwrites the first in the executor's _open map. This column
-- removes the guess.
--
-- CONSISTENCY WITH 011. Migration 011 added the same `strategy text NOT NULL
-- DEFAULT 'two_ladder'` to trade_signals, with the same reasoning: the default
-- correctly labels pre-multi-strategy rows (they all came from two_ladder) and
-- lets a single-strategy deployment omit it. We mirror that here so a trade and
-- the signal that opened it agree on strategy by construction.
--
-- THE KEY. (chain_id, side) is STILL the idempotency backstop and stays UNIQUE:
-- one chain in one direction opens at most one position, regardless of strategy.
-- We do NOT widen the unique key to include strategy. Two strategies acting on
-- the SAME chain_id would be two signals with DISTINCT position_keys and DISTINCT
-- signal_ids (011 weaves strategy into both on the writer side), but if they ever
-- resolved to the same chain_id+side they must NOT both open a venue position on
-- one chain — the (chain_id, side) uniqueness is exactly the capital backstop
-- that prevents that. strategy here is PROVENANCE (which strategy's decision this
-- was), not part of the position's identity key. This matches 001's design note
-- that in-memory dedupe is not enough with real money; the DB key is the guard.
--
-- BACKFILL. Existing trades predate multi-strategy and all came from the original
-- two-ladder strategy, so the 'two_ladder' default labels them correctly. The
-- default stays so a single-strategy deployment need not set it on insert.
--
-- WRITER SIDE (must accompany this migration — see note at bottom). The value is
-- only as good as the insert that sets it: the executor's entry-insert path must
-- write the signal row's strategy into this column, or every new trade will take
-- the default and the collision returns for non-two_ladder strategies. This
-- migration is necessary but not sufficient on its own.
-- ============================================================================

ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS strategy text NOT NULL DEFAULT 'two_ladder';


-- Per-strategy scans (reconciliation, analysis, "all open trades for strategy X").
-- Cheap, and load_open_trades' hot scan already filters on the partial active
-- index; this is for the analytical/per-strategy reads.
CREATE INDEX IF NOT EXISTS ix_trades_strategy ON trades (strategy);


-- ----------------------------------------------------------------------------
-- ACCOMPANYING WRITER CHANGE (not DDL — done in the executor's insert path):
--
--   The pending-trade INSERT must include the signal's strategy. In
--   execution/persistence.py make_persist_trade, add `strategy` to the column
--   list and to the ON CONFLICT DO UPDATE (a replay of the same trade should not
--   silently change its strategy, so DO UPDATE should NOT overwrite strategy —
--   leave it out of the UPDATE SET, same as chain_id/side which are the key).
--   OpenTrade already carries `strategy`, so the value is in hand at insert time.
--
--   And load_open_trades must SELECT strategy and use it instead of the
--   DEFAULT_STRATEGY stamp:
--       SELECT ..., strategy FROM trades WHERE state NOT IN ('CLOSED','REJECTED')
--       ... strategy=r["strategy"] ...
--
-- Both edits are described inline in execution/persistence.py's "LEFT TO DO".
--
-- ROLLBACK (dev only; never drop a NOT NULL column with live trades mid-flight):
--   DROP INDEX IF EXISTS ix_trades_strategy;
--   ALTER TABLE trades DROP COLUMN IF EXISTS strategy;
-- ----------------------------------------------------------------------------