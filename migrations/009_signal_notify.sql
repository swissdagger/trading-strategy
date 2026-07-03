-- ============================================================================
-- 009_signal_notify.sql
--
-- Low-latency signal delivery: NOTIFY on every trade_signals insert, so a
-- listening executor is PUSHED each new signal instead of polling for it.
--
-- WHY A TRIGGER (not app-side NOTIFY). The notification MUST be part of the
-- same transaction as the insert, for two reasons:
--
--   1. Atomicity. pg_notify inside the INSERT's transaction is delivered iff
--      that transaction commits. So a notification can NEVER fire for a row
--      that didn't durably land, and a row can never land without its
--      notification — the dual-write divergence that in-process "fire to IB
--      directly" would risk is structurally impossible here. The durable row
--      and the execution trigger are one commit.
--
--   2. Can't be forgotten. Putting NOTIFY in a trigger means EVERY writer of
--      trade_signals gets it for free — the current SignalWriter, this
--      project's future position manager, a manual backfill INSERT, anything.
--      If it lived in app code, a new writer could omit it and silently break
--      the executor's fast path. The trigger makes it a property of the TABLE,
--      not of one caller.
--
-- PAYLOAD IS JUST THE signal_id. NOTIFY payloads are capped (8000 bytes) and
-- are not the place for the row's data — the listener takes the signal_id and
-- SELECTs the full row (it's the PK, so that read is a point lookup). Keeping
-- the payload to the id also avoids any encoding/size concerns with the
-- decision fields.
--
-- DELIVERY IS BEST-EFFORT, NOT DURABLE. This is the one thing to understand:
-- NOTIFY is delivered only to sessions with an active LISTEN at commit time.
-- If the executor's LISTEN connection is down when a signal commits, that
-- notification is LOST — Postgres does not queue it. That is NOT a data-loss
-- problem here, because the row is durably in trade_signals regardless; it is
-- only a "the push was missed" problem. The executor handles it with a
-- cursor-based catch-up sweep on (re)connect (reading rows with created_at
-- beyond its last-seen cursor), so LISTEN/NOTIFY is the fast path and the
-- cursor sweep is the guaranteed-delivery backstop. See execution/
-- signal_listener.py. This migration provides only the fast path.
--
-- CHANNEL NAME. 'trade_signal' (singular). Postgres channel names are plain
-- identifiers; the listener must LISTEN on exactly this name.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- FUNCTION: notify_trade_signal
-- Fires a NOTIFY on the 'trade_signal' channel carrying the new row's
-- signal_id. AFTER INSERT so it only fires once the row is actually inserted
-- (ON CONFLICT DO NOTHING inserts that skip do NOT fire the trigger — see the
-- note below, which is exactly the behaviour we want).
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION notify_trade_signal()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- pg_notify is the function form of NOTIFY, letting us pass a dynamic
    -- payload (the signal_id). Delivered on commit of THIS transaction.
    PERFORM pg_notify('trade_signal', NEW.signal_id);
    RETURN NEW;
END;
$$;


-- ----------------------------------------------------------------------------
-- TRIGGER: trg_notify_trade_signal
--
-- AFTER INSERT, per row. Critically, this interacts correctly with the
-- writer's idempotency: make_insert_signal uses INSERT ... ON CONFLICT
-- (signal_id) DO NOTHING. When a replay hits the conflict and DOES NOTHING,
-- NO row is inserted, so this AFTER INSERT trigger does NOT fire. That is the
-- desired behaviour: a replayed/duplicate emit must not re-notify the
-- executor (which would make it act on the same signal twice). Only a
-- genuinely new signal notifies — mirroring how insert_signal returns
-- newly=True only on a real insert.
-- ----------------------------------------------------------------------------
DROP TRIGGER IF EXISTS trg_notify_trade_signal ON trade_signals;
CREATE TRIGGER trg_notify_trade_signal
    AFTER INSERT ON trade_signals
    FOR EACH ROW
    EXECUTE FUNCTION notify_trade_signal();


-- ----------------------------------------------------------------------------
-- ROLLBACK (manual):
--   DROP TRIGGER IF EXISTS trg_notify_trade_signal ON trade_signals;
--   DROP FUNCTION IF EXISTS notify_trade_signal();
-- The trigger is additive and side-effect-only (it writes nothing), so it is
-- safe to drop at any time; the executor simply falls back to its cursor
-- sweep for delivery, losing only the push latency, not any signals.
-- ----------------------------------------------------------------------------