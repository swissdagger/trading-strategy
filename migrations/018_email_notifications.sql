-- ============================================================================
-- 018_email_notifications.sql
--
-- Dedupe + durable cursor for the EMAIL NOTIFIER — a THIRD consumer of
-- trade_signals (alongside the other team's consumer and this project's own
-- executor). It reads new rows via the same SignalListener the executor uses
-- and emails a human a plain-text instruction per signal.
--
-- WHY A TABLE (two jobs, one row):
--
--   1. DEDUPE. SignalListener guarantees AT-LEAST-ONCE (module docstring): a
--      signal_id can arrive twice — once by NOTIFY, again by a catch-up sweep
--      that overlaps it. Without a durable gate the human would get duplicate
--      emails on every reconnect. This table's PRIMARY KEY (signal_id) + an
--      INSERT ... ON CONFLICT DO NOTHING is that gate: the row inserts once,
--      per signal_id, forever. (Resend's Idempotency-Key header is a second,
--      shorter-lived line of defense for the crash-between-send-and-record
--      window; this table is the durable one.)
--
--   2. CURSOR. max(created_at) of the rows we've recorded is the notifier's
--      resume point. On restart it replays from there (per the user's choice:
--      replay recent history so the per-position tally re-seeds), and the
--      dedupe gate suppresses re-emailing anything already sent. No separate
--      cursor table — the sent-log IS the cursor.
--
-- This is the notifier's OWN state, not part of the cross-team trade_signals
-- contract. trade_signals stays append-only and untouched; nothing here writes
-- to it. So this migration is fully isolated — applying or rolling it back
-- affects only the notifier.
-- ============================================================================


CREATE TABLE IF NOT EXISTS email_notifications (
    -- The trade_signals row this email corresponds to. PK = the dedupe gate.
    -- Same id space as trade_signals.signal_id (deterministic TEXT), so a
    -- replayed signal maps to the same row here and ON CONFLICT DO NOTHING
    -- makes the second delivery a no-op.
    signal_id     text         PRIMARY KEY,

    -- Copied from the trade_signals row at send time. This is what makes the
    -- table a cursor: we resume from max(created_at) here. Copying it (rather
    -- than joining back to trade_signals) keeps the notifier's resume logic a
    -- single-table read and survives even if a signal row were ever archived.
    created_at    timestamptz  NOT NULL,

    -- When we actually sent (wall clock). NULL would mean "recorded but not yet
    -- sent"; we insert AFTER a successful send, so in practice this is set on
    -- every row. Kept explicit for auditing send latency vs decided_at.
    sent_at       timestamptz  NOT NULL DEFAULT now(),

    -- Resend's message id from the send response, for tracing a specific email
    -- back to the provider. Nullable: a send can succeed with the provider
    -- while the id is momentarily unavailable, and dedupe must not depend on it.
    resend_id     text
);

-- The cursor read is max(created_at); index it so resume is an index scan, not
-- a full-table max. Same shape as trade_signals' own read-cursor index (004).
CREATE INDEX IF NOT EXISTS ix_email_notifications_created_at
    ON email_notifications (created_at);

-- Headless / cross-service posture, consistent with trade_signals (004) and
-- the rest of the schema: the notifier connects with its own service role.
ALTER TABLE email_notifications ENABLE ROW LEVEL SECURITY;


-- ----------------------------------------------------------------------------
-- ROLLBACK (manual):
--   DROP TABLE IF EXISTS email_notifications;
-- Isolated to the notifier; dropping it only forces a full replay on next
-- start (bounded by the notifier's initial_cursor) and loses send-audit rows.
-- trade_signals is unaffected.
-- ----------------------------------------------------------------------------
