-- ============================================================================
-- 016_trade_signals_fraction.sql
--
-- PARTIAL EXITS on the trade-signals contract, for the prior_failure_scaleout
-- strategy (entry at P2, seven scale-out tranches banked as the chain
-- propagates, remainder closed at chain termination).
--
-- The 004 contract is binary: an EXIT closes the WHOLE position. This migration
-- makes an EXIT close a FRACTION of it, additively:
--
--   * fraction — how much of the position's ORIGINAL size this signal closes.
--     DEFAULT 1.0, so every existing row and every row from the existing
--     strategies (two_ladder, four_chain) keeps exact whole-position semantics
--     without a code change on their emit paths.
--   * two new exit-reason values (on 001's enum, used by the executor's order
--     rows): SCALE (one scheduled tranche) and TERMINAL (chain terminated —
--     close whatever remains).
--
-- CONSUMER SEMANTICS (the cross-team contract change — coordinate before any
-- SCALE/TERMINAL row is emitted; see the contract note shipped with this
-- migration):
--
--   * fraction is of the ORIGINAL entry size, not of the remainder. The seven
--     tranches sum to 1.0.
--   * Each tranche is a DISTINCT row with a DISTINCT deterministic signal_id
--     (the writer folds the tranche offset into the id). Dedupe by signal_id,
--     exactly as today.
--   * A position is closed when cumulative exited fraction reaches 1.0, OR on
--     a TERMINAL exit. TERMINAL means "close whatever you still hold" — derive
--     the remainder from YOUR OWN fill records, never from 1 - sum(fraction)
--     of the rows you happened to see (at-least-once delivery means you must
--     tolerate a replayed tranche; floor the remainder at zero).
--   * Delivery order is created_at within a session, but a reconnect overlap
--     can replay tranches out of order. Each tranche is idempotent by
--     signal_id; out-of-order application must not drive remainder negative.
--
-- NOT CHANGED: the exit_reason presence CHECK from 004 (an EXIT still always
-- carries a reason, an ENTER never does) and the position_key identity model.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- fraction: the portion of the position this signal concerns.
-- NUMERIC(5,4): exact decimal, four places — the schedule's values (0.05,
-- 0.10, ...) are exact, and cumulative sums are exact (no float drift in the
-- "have the tranches reached 1.0" arithmetic on either side of the contract).
-- ----------------------------------------------------------------------------
ALTER TABLE trade_signals
    ADD COLUMN fraction NUMERIC(5,4) NOT NULL DEFAULT 1.0;

-- A fraction is a portion: strictly positive, at most the whole position.
ALTER TABLE trade_signals
    ADD CONSTRAINT ck_trade_signals_fraction_range
        CHECK (fraction > 0 AND fraction <= 1.0);

-- An ENTER establishes the whole position; partiality is an EXIT concept.
-- Enforcing it keeps the contract self-documenting: a consumer can rely on
-- every ENTER meaning "the full position", forever.
ALTER TABLE trade_signals
    ADD CONSTRAINT ck_trade_signals_enter_whole
        CHECK (action <> 'ENTER' OR fraction = 1.0);


-- ----------------------------------------------------------------------------
-- Exit-reason vocabulary (001's enum — used by the executor's `orders` rows,
-- and mirrored by execution.types.ExitReason). trade_signals.exit_reason is
-- free text (004 checks presence, not values), so it needs no DDL — but the
-- enum must know the new values BEFORE the executor first writes an order row
-- carrying them (step 4 of the rollout). Added here so the whole reason
-- vocabulary changes in one migration.
--
--   SCALE    — one scheduled scale-out tranche fired (chain propagated one
--              level past entry on the schedule). No urgency: the chain is
--              going our way; a TARGET-like resting marketable limit is fine.
--   TERMINAL — the chain terminated; close the remainder. Protective-ish:
--              the structure supporting the position is gone, so this wants
--              STOP-like escalation posture, not TARGET patience.
-- ----------------------------------------------------------------------------
ALTER TYPE exit_reason ADD VALUE IF NOT EXISTS 'SCALE';
ALTER TYPE exit_reason ADD VALUE IF NOT EXISTS 'TERMINAL';


-- ----------------------------------------------------------------------------
-- ROLLBACK (manual):
--   ALTER TABLE trade_signals DROP CONSTRAINT ck_trade_signals_enter_whole;
--   ALTER TABLE trade_signals DROP CONSTRAINT ck_trade_signals_fraction_range;
--   ALTER TABLE trade_signals DROP COLUMN fraction;
-- Enum values cannot be dropped in place (Postgres); they are inert if unused.
-- The column is additive with a DEFAULT, so producers that don't name it are
-- unaffected in both directions.
--
-- Verify after applying:
--   SELECT count(*) FILTER (WHERE fraction <> 1.0) AS partial_rows,
--          count(*) AS total_rows
--   FROM trade_signals;
-- Expect: partial_rows = 0 until prior_failure_scaleout is enabled.
--   SELECT unnest(enum_range(NULL::exit_reason));
-- Expect: STOP, TARGET, TIME, MANUAL, KILL_SWITCH, SCALE, TERMINAL.
-- ============================================================================
