-- ============================================================================
-- 026_chains_ladder.sql
--
-- chains.ladder: the ladder a chain belongs to. producer/persistence.py has
-- written and read this column since the multi-ladder work (its docstring
-- cites "migration 019" — the pre-renumbering name; 019 is now shadow_trades)
-- but no migration in the tree created it, so a FRESH database fails on the
-- first chain init. Existing databases that received the column by hand are
-- unaffected (IF NOT EXISTS throughout).
--
-- Mandatory on anything live: an active chain without a ladder cannot be
-- hydrated into the right machine. Historical inactive rows may be NULL.
-- ============================================================================

ALTER TABLE chains ADD COLUMN IF NOT EXISTS ladder text;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_chains_ladder_required') THEN
        ALTER TABLE chains ADD CONSTRAINT ck_chains_ladder_required
            CHECK (NOT active OR ladder IS NOT NULL);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_chains_symbol_ladder_active
    ON chains (symbol, asset_class, ladder) WHERE active = true;

-- ROLLBACK (manual): ALTER TABLE chains DROP CONSTRAINT ck_chains_ladder_required;
--                    DROP INDEX ix_chains_symbol_ladder_active;  (keep the column)
