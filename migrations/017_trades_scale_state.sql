-- ============================================================================
-- 017_trades_scale_state.sql
--
-- Executor-side state for PARTIAL EXITS (the prior_failure_scaleout rollout,
-- step 4). Two additive columns on trades:
--
--   * scale_banked — the tranches this executor has ACCEPTED for this trade,
--     as a JSONB map {trigger_level: fraction} with both sides stored as
--     STRINGS ("3": "0.05") so the fractions stay exact decimals end-to-end
--     (a JSON number would round-trip through binary float). Keyed by the
--     signal's trigger_level (= entry level + tranche offset) because that is
--     what the signal row carries; the tranche offset itself is folded into
--     the opaque signal_id and is not a column. Presence of a level in this
--     map IS the replay dedupe: a re-delivered SCALE signal for a level
--     already banked is a no-op.
--
--     The map answers "how much of the position SHOULD be exited by now":
--     desired = sum(fractions) * entry_filled_qty. The executor issues exit
--     orders for (desired - exit_filled_qty), which makes lot-size
--     accumulation automatic — a tranche too small to place stays as unmet
--     deficit and rides into the next tranche's order.
--
--   * exit_reason — the reason driving the CURRENT/last exit lifecycle
--     (SCALE for a tranche order, TERMINAL/STOP/... for a final close).
--     Persisted so a restart can distinguish "CLOSING a tranche (returns to
--     OPEN when it fills)" from "CLOSING for good (finalizes CLOSED)" — the
--     two are the same trade_state but opposite continuations. Previously
--     in-memory only; per-order reasons remain on the orders rows.
--
-- Both additive with defaults: existing rows and the whole-exit strategies
-- (two_ladder, four_chain) never touch them ('{}'::jsonb, NULL).
-- ============================================================================

ALTER TABLE trades
    ADD COLUMN scale_banked jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE trades
    ADD COLUMN exit_reason text;


-- ----------------------------------------------------------------------------
-- CLIENT-ORDER-ID RUNG STRIDE (documentation; no DDL — recorded here because
-- this migration is where the convention becomes load-bearing).
--
-- The cv1 id scheme is cv1.<trade>.<E|X>.<rung>. Pre-017, an exit's rung was
-- the escalation re-issue counter (0, 1, 2...). A scaled position has SEVERAL
-- exit lifecycles (one per tranche batch + a possible final close), each with
-- its own escalation, so the rung now encodes both:
--
--     rung = tranche_block * 100 + escalation
--
-- where tranche_block is the batch's trigger_level (3..9 on the PF ladder)
-- for SCALE orders, and 0 for whole-position closes (TERMINAL and every
-- legacy reason). Block 0 rungs are therefore 0, 1, 2... — BYTE-IDENTICAL to
-- every pre-017 exit id, so old ids parse, dedupe, and attribute unchanged.
-- The cv1 parse pattern already accepts multi-digit rungs; no format bump.
-- ----------------------------------------------------------------------------


-- ----------------------------------------------------------------------------
-- ROLLBACK (manual):
--   ALTER TABLE trades DROP COLUMN exit_reason;
--   ALTER TABLE trades DROP COLUMN scale_banked;
--
-- Verify after applying:
--   SELECT count(*) FILTER (WHERE scale_banked <> '{}'::jsonb) AS scaled,
--          count(*) AS total FROM trades;
-- Expect: scaled = 0 until prior_failure_scaleout is enabled.
-- ============================================================================
