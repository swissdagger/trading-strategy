-- ============================================================================
-- 021_executor_alerts.sql
--
-- Human-visible alerts from the executor (ADR-001, Phase 3): every trade HALT
-- and every refused boot writes a row here, and the trigger below fires
-- pg_notify('executor_alert', <alert_id>) in the same transaction.
--
-- Why not migration 018: 018 is only the dedupe/cursor table of an email
-- notifier that consumes trade_signals. It has no trigger, and the notifier
-- itself does not live in this repo. This table is the executor's alert
-- outbox; the notifier (or any other sink) delivers from it by LISTENing on
-- 'executor_alert' and/or sweeping rows with delivered_at IS NULL.
--
-- Writes are best-effort from the executor's side: a failed alert insert is
-- logged and never blocks the HALT it describes.
-- ============================================================================

CREATE TABLE IF NOT EXISTS executor_alerts (
    alert_id      bigserial    PRIMARY KEY,
    kind          text         NOT NULL,   -- 'trade_halted' | 'boot_refused' | 'executor_boot_failed'
    trade_id      text,                    -- set for trade-scoped alerts
    symbol        text,
    detail        jsonb        NOT NULL DEFAULT '{}'::jsonb,
    created_at    timestamptz  NOT NULL DEFAULT now(),
    -- Set by the delivering sink once a human has been notified.
    delivered_at  timestamptz
);

CREATE INDEX IF NOT EXISTS ix_executor_alerts_undelivered
    ON executor_alerts (created_at) WHERE delivered_at IS NULL;

CREATE OR REPLACE FUNCTION notify_executor_alert()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_notify('executor_alert', NEW.alert_id::text);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_notify_executor_alert ON executor_alerts;
CREATE TRIGGER trg_notify_executor_alert
    AFTER INSERT ON executor_alerts
    FOR EACH ROW
    EXECUTE FUNCTION notify_executor_alert();

ALTER TABLE executor_alerts ENABLE ROW LEVEL SECURITY;

-- ROLLBACK (manual):
--   DROP TRIGGER IF EXISTS trg_notify_executor_alert ON executor_alerts;
--   DROP FUNCTION IF EXISTS notify_executor_alert();
--   DROP TABLE IF EXISTS executor_alerts;
