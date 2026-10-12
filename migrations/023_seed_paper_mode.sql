-- ============================================================================
-- 023_seed_paper_mode.sql
--
-- ADR-001 Phase 6: a fresh deployment starts in PAPER mode, so the executor
-- service in docker-compose runs the paper account from its first start.
--
-- Deliberately narrow: it only moves the singleton trade_enabled row from
-- 'off' to 'paper' while that row is still in its untouched initial state
-- (migration 010's seed reason). A mode an operator has set — including a
-- deliberate 'off' — is never overridden, and re-applying this is a no-op.
-- 'live' is only ever set by hand.
-- ============================================================================

UPDATE trade_enabled
   SET mode = 'paper',
       reason = 'initial: paper trading (ADR-001 Phase 6 seed)',
       set_by = 'migration 023',
       updated_at = now()
 WHERE id = true
   AND mode = 'off'
   AND reason LIKE 'initial:%'
   AND reason <> 'initial: paper trading (ADR-001 Phase 6 seed)';
