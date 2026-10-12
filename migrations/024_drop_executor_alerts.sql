-- ============================================================================
-- 024_drop_executor_alerts.sql
--
-- Removes the executor alert outbox (021). Nothing delivered from it and the
-- owner decided against alerting: HALTs and refused boots are logged at ERROR,
-- and HALTED trades stay visible in trades (state + halt_reason).
-- Idempotent; safe on a database that never had 021 applied.
-- ============================================================================

DROP TABLE IF EXISTS executor_alerts;   -- drops its trigger with it
DROP FUNCTION IF EXISTS notify_executor_alert();
