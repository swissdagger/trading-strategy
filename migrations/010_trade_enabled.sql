-- ============================================================================
-- 010_trade_enabled.sql
--
-- The MASTER TRADE ENABLE switch: a live, DB-backed on/off for order placement,
-- separate from and complementary to kill_switch (001).
--
-- WHY SEPARATE FROM kill_switch. kill_switch is an EMERGENCY halt with modes
-- (halt_new / flatten) — "something is wrong, stop." trade_enabled is the
-- ORDINARY operating state — "is live execution turned on at all." A fresh
-- deployment, a paper-only period, or a deliberate "signals-only, someone else
-- executes" phase all run with trade_enabled=false and kill_switch NOT engaged.
-- Conflating them would overload kill_switch's emergency semantics with routine
-- on/off. Both gate orders; they answer different questions.
--
-- PRECEDENCE (enforced in the executor/gating layer, not here): an order may be
-- placed only if trade_enabled=true AND kill_switch is not blocking. Either one
-- off => no order. trade_enabled is the master power switch; kill_switch is the
-- emergency stop on top of it.
--
-- A DB ROW, NOT AN ENV VAR — same rationale as kill_switch: you must be able to
-- flip live execution on/off instantly without a redeploy. config.py carries
-- the DEFAULT (what state to boot into); this row is the live override that
-- wins once the process is running. See execution/trade_enable.py.
--
-- SINGLETON, like kill_switch: exactly one row, guarded by a CHECK on a boolean
-- primary key.
-- ============================================================================

CREATE TABLE trade_enabled (
    id            boolean PRIMARY KEY DEFAULT true,   -- single-row guard
    enabled       boolean      NOT NULL DEFAULT false, -- master: may we place orders?
    -- Free-text audit of why the current state was set (e.g. "paper phase",
    -- "go-live 2026-07-03", "disabled pending reconcile fix").
    reason        text,
    set_by        text,
    updated_at    timestamptz  NOT NULL DEFAULT now(),

    CONSTRAINT ck_trade_enabled_singleton CHECK (id = true)
);

-- Boot DISABLED by default. A new deployment must be turned on deliberately —
-- the safe default for a system that moves real money is "off until someone
-- explicitly enables it," never "trading the moment the process starts."
INSERT INTO trade_enabled (id, enabled, reason)
    VALUES (true, false, 'initial: trading disabled until explicitly enabled')
    ON CONFLICT (id) DO NOTHING;


-- ----------------------------------------------------------------------------
-- Optional NOTIFY on state change, so a running executor can react to an
-- enable/disable flip immediately rather than on its next poll — same
-- fast-path pattern as 009's signal notify. The gating layer LISTENs on
-- 'trade_enabled_changed' and refreshes its cached state on notify, with a
-- periodic poll as the backstop (mirroring the signal listener's belt-and-
-- suspenders delivery).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION notify_trade_enabled_changed()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- Payload is the new enabled state as text ('t'/'f'); the listener can act
    -- on the payload directly or re-read the row. Fired in the UPDATE's txn.
    PERFORM pg_notify('trade_enabled_changed', NEW.enabled::text);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_notify_trade_enabled ON trade_enabled;
CREATE TRIGGER trg_notify_trade_enabled
    AFTER UPDATE OF enabled ON trade_enabled
    FOR EACH ROW
    WHEN (OLD.enabled IS DISTINCT FROM NEW.enabled)
    EXECUTE FUNCTION notify_trade_enabled_changed();


-- ----------------------------------------------------------------------------
-- ROLLBACK (manual):
--   DROP TRIGGER IF EXISTS trg_notify_trade_enabled ON trade_enabled;
--   DROP FUNCTION IF EXISTS notify_trade_enabled_changed();
--   DROP TABLE IF EXISTS trade_enabled;
-- ----------------------------------------------------------------------------