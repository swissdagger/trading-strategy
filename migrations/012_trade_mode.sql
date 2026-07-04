-- ============================================================================
-- 012_trade_mode.sql
--
-- Extends the master switch from a boolean on/off to a THREE-STATE mode:
--   'off'   — place no orders at all (the old enabled=false).
--   'paper' — place orders against the PAPER venue (IB gateway port 4002 /
--             Binance testnet): real order flow, real pricing/fills/reconcile,
--             FAKE money. The shakeout mode.
--   'live'  — place orders against the LIVE venue: real money.
--
-- WHY A MODE, NOT A SECOND FLAG. Paper and live are mutually exclusive
-- destinations for the same order path — a single order goes to exactly one
-- venue. A three-state mode expresses that directly; two booleans (enabled +
-- paper) would admit a nonsense state (enabled=false, paper=true) that then
-- needs guarding. One column, three values, no invalid combinations.
--
-- RELATIONSHIP TO enabled. The boolean `enabled` from migration 010 is kept and
-- DERIVED from mode (enabled = (mode != 'off')), so any existing reader of
-- `enabled` keeps working unchanged — a consumer that only cares "are we placing
-- orders at all" reads `enabled`; one that cares "paper or live" reads `mode`.
-- A trigger keeps them consistent so they can never disagree.
--
-- PAPER VS LIVE CHANGES THE VENUE, WHICH MEANS A RECONNECT. Unlike the plain
-- on/off (which the GatedAdapter can honour instantly by no-oping orders),
-- switching BETWEEN paper and live changes which gateway the adapter is
-- connected to — and that connection is made at connect() time. So a
-- paper<->live flip requires the adapter to reconnect to the other endpoint;
-- it is not a pure in-memory flag flip. The application layer
-- (execution/trade_mode.py) detects a venue-changing transition and reconnects.
-- off<->(paper|live) does NOT require a reconnect on its own (off just gates
-- orders), but going off->live vs off->paper still selects the endpoint the
-- next connect uses. See trade_mode.py.
--
-- A DB ROW for the same reason as before: flip paper/live/off live, no redeploy.
-- config carries the DEFAULT; this row is the live override.
-- ============================================================================

-- The mode enum. Kept as a CHECK-constrained text (not a pg enum type) so
-- adding a future mode (e.g. 'shadow') is an ALTER of the check, not an enum
-- migration.
ALTER TABLE trade_enabled
    ADD COLUMN IF NOT EXISTS mode text NOT NULL DEFAULT 'off'
        CHECK (mode IN ('off', 'paper', 'live'));

-- Backfill mode from the existing boolean for any pre-existing row: enabled
-- rows become 'live' (they were placing real orders before this migration
-- existed), disabled rows stay 'off'. A deployment that wants the newly-enabled
-- state to mean paper must set mode='paper' explicitly after applying.
UPDATE trade_enabled SET mode = CASE WHEN enabled THEN 'live' ELSE 'off' END
    WHERE mode = 'off' AND enabled = true;


-- ----------------------------------------------------------------------------
-- Keep `enabled` and `mode` consistent: enabled = (mode != 'off'). A single
-- trigger derives enabled from mode on any write, so the two columns can never
-- disagree regardless of which one a writer sets. Writers SHOULD set mode; if a
-- legacy writer sets only enabled, we still derive a sensible mode.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION sync_trade_mode_enabled()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- If mode changed (or is being set), derive enabled from it.
    IF NEW.mode IS DISTINCT FROM OLD.mode THEN
        NEW.enabled := (NEW.mode <> 'off');
    -- Else if only enabled changed (legacy writer), derive a mode: turning
    -- enabled off -> 'off'; turning it on -> keep an existing non-off mode, or
    -- default to 'live' (a legacy enable meant real orders).
    ELSIF NEW.enabled IS DISTINCT FROM OLD.enabled THEN
        IF NEW.enabled = false THEN
            NEW.mode := 'off';
        ELSIF OLD.mode = 'off' THEN
            NEW.mode := 'live';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_trade_mode_enabled ON trade_enabled;
CREATE TRIGGER trg_sync_trade_mode_enabled
    BEFORE UPDATE ON trade_enabled
    FOR EACH ROW
    EXECUTE FUNCTION sync_trade_mode_enabled();


-- The existing change-notify trigger (migration 010) fires on enabled changes.
-- Extend the notify to also fire when mode changes (paper<->live is a mode
-- change that may NOT flip enabled — both are 'enabled' — so the enabled-only
-- trigger would miss it). Re-create the notify trigger to watch mode too.
CREATE OR REPLACE FUNCTION notify_trade_enabled_changed()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- Payload is the new mode, so a listener can act on it directly.
    PERFORM pg_notify('trade_enabled_changed', NEW.mode);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_notify_trade_enabled ON trade_enabled;
CREATE TRIGGER trg_notify_trade_enabled
    AFTER UPDATE OF enabled, mode ON trade_enabled
    FOR EACH ROW
    WHEN (OLD.enabled IS DISTINCT FROM NEW.enabled OR OLD.mode IS DISTINCT FROM NEW.mode)
    EXECUTE FUNCTION notify_trade_enabled_changed();


-- ----------------------------------------------------------------------------
-- ROLLBACK (manual):
--   DROP TRIGGER IF EXISTS trg_sync_trade_mode_enabled ON trade_enabled;
--   DROP FUNCTION IF EXISTS sync_trade_mode_enabled();
--   ALTER TABLE trade_enabled DROP COLUMN IF EXISTS mode;
--   -- and restore migration 010's notify function/trigger (enabled-only).
-- ----------------------------------------------------------------------------