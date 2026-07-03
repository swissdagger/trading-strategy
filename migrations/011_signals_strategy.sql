-- ============================================================================
-- 011_signals_strategy.sql
--
-- Adds STRATEGY provenance to trade_signals, for the multi-strategy world.
--
-- QISS now runs multiple strategies concurrently, and two strategies can hold
-- positions on the SAME chain/direction/symbol at once. Without a strategy
-- discriminator, both would produce the same position_key (chain_id:L|S) and
-- the same deterministic signal_id — so the second strategy's signal would be
-- silently deduped away by the ON CONFLICT (signal_id) DO NOTHING insert, and
-- the two positions would be indistinguishable in the durable record.
--
-- This column carries which strategy produced each signal. Combined with the
-- writer weaving the strategy into position_key and signal_id (see the updated
-- signal_writer), the collision is resolved: the two strategies produce
-- distinct signal_ids, distinct position_keys, and distinct rows.
--
-- CROSS-TEAM CONTRACT NOTE (migration 004 discipline). trade_signals is a
-- cross-team interface; its shape changes in lockstep with the consuming team.
-- This change is ADDITIVE and NON-BREAKING:
--   * the column is nullable with a default, so existing INSERTs that omit it
--     still succeed;
--   * a consumer that ignores the column reads rows exactly as before.
-- Coordinate with the consuming team that the column EXISTS and what it means
-- (route/size per strategy, and treat (position_key) as unique only WITHIN a
-- strategy), but no consumer is forced to change to keep working.
-- ============================================================================

ALTER TABLE trade_signals
    ADD COLUMN IF NOT EXISTS strategy text NOT NULL DEFAULT 'two_ladder';

-- Backfill note: existing rows predate multi-strategy and all came from the
-- original two-ladder strategy, so the 'two_ladder' default correctly labels
-- them. The default stays so a single-strategy deployment need not set it.

-- Query all signals for one strategy (per-strategy reconciliation / analysis).
CREATE INDEX IF NOT EXISTS ix_trade_signals_strategy ON trade_signals (strategy);

-- The position_key uniqueness contract is now PER STRATEGY: (strategy,
-- position_key) names one position, not position_key alone. We do NOT add a
-- unique constraint here (signals are an append-only log — entry + exits share
-- a position_key), but consumers matching an EXIT to its ENTER must match on
-- (strategy, position_key) together. Documented rather than constrained,
-- consistent with 004's treatment of the position-key matching gap.


-- ----------------------------------------------------------------------------
-- ROLLBACK (manual):
--   DROP INDEX IF EXISTS ix_trade_signals_strategy;
--   ALTER TABLE trade_signals DROP COLUMN IF EXISTS strategy;
-- ----------------------------------------------------------------------------