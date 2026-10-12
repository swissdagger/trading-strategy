-- ============================================================================
-- 020_executor_cursor.sql
--
-- The executor's durable SignalListener cursor (ADR-001, Phase 3): the
-- created_at high-water of the last trade_signals row it dispatched. Without
-- it, every restart resumed from "now" and silently dropped any signal written
-- while the executor was down — including EXITs for positions it holds.
--
-- On boot run_executor loads this as the listener's initial_cursor; the
-- listener's catch-up sweep then replays everything since (>= cursor). The
-- handler deduplicates replays, and stale ENTERs are skipped by age
-- (config.ENTER_MAX_AGE_SECONDS) while EXITs are always honoured.
--
-- Single row. Writes are monotonic (GREATEST), so an out-of-order write can
-- never move the cursor backwards.
-- ============================================================================

CREATE TABLE IF NOT EXISTS executor_cursor (
    id          boolean      PRIMARY KEY DEFAULT true,   -- single-row guard
    cursor_at   timestamptz  NOT NULL,
    updated_at  timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_executor_cursor_singleton CHECK (id = true)
);

-- No seed row: absent means "never ran", and run_executor starts from now.

ALTER TABLE executor_cursor ENABLE ROW LEVEL SECURITY;

-- ROLLBACK (manual):  DROP TABLE IF EXISTS executor_cursor;
-- (The executor then resumes from "now" on its next start.)
