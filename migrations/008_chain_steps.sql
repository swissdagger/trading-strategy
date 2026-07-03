-- ============================================================================
-- 008_chain_steps.sql   (write-amplification fix — Layer 3)
--
-- Moves a chain's per-step propagation history OUT of the chains.history JSONB
-- column and into an append-only chain_steps table.
--
-- WHY
-- The old hot path did, on every propagation:
--     UPDATE chains SET history = history || $step::jsonb WHERE chain_id = $id
-- Postgres has no in-place JSONB append: each propagation rewrites the ENTIRE
-- (and growing) history column. A chain climbing to level 5 rewrites history
-- five times, each rewrite larger than the last — write amplification that
-- worsens with chain depth, on the system's second-highest-volume write.
--
-- A chain_steps row is a plain INSERT: fixed-size, append-only, no read-modify-
-- write of a growing value. Reconstruction of the full ordered history (needed
-- only on hydrate-at-restart, a cold path) is a single indexed SELECT.
--
-- WHAT chains.history BECOMES
-- chains.history is NO LONGER WRITTEN per-step. It is left in the schema
-- (nullable-in-spirit, defaults to '[]') so nothing that reads the column
-- breaks, but the authoritative per-step record is chain_steps. The two readers
-- that used history were updated to not depend on it:
--   * machine hydrate (make_load_active_chains) now reconstructs history from
--     chain_steps;
--   * strategy-view seed (make_load_chain_view_seed) now classifies Set A/B
--     from active_timeframes[1] (the baseline tf) instead of history[0], so it
--     needs no per-step data at all.
--
-- ORDERING / SEMANTICS PRESERVED
-- history was NOT deduplicated (every propagation appended a step, even when the
-- timeframe repeated), and make_load_active_chains derives current_level /
-- next_expected_level from len(history). chain_steps is likewise one-row-per-
-- step with no dedup, and reconstruction orders by step_index, so len() and
-- ordering semantics are byte-equivalent to the old JSONB array.
--
-- Apply after 002 (it references chains). On a fresh reset, apply in sequence
-- with the rest; there is no historical-row backfill because history was never
-- the source of truth for anything but hydrate, which now reads chain_steps.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- TABLE: chain_steps
--
-- One row per propagation step of a chain, INCLUDING the initiation step
-- (step_index = 0). Append-only: rows are never updated or deleted in the hot
-- path. (chain_id, step_index) is the natural key and enforces idempotency — a
-- replayed propagation INSERT ... ON CONFLICT DO NOTHING is a no-op, mirroring
-- the idempotency the old JSONB path got from being part of an upsert.
--
-- Mirrors the per-step dict shape the machine kept in history:
--   {level, timeframe, timestamp, price}
-- plus the chain_id and an explicit step_index for stable ordering (timestamp
-- ties are possible across timeframes, so order by an explicit monotonic index,
-- not by timestamp).
-- ----------------------------------------------------------------------------
CREATE TABLE chain_steps (
    chain_id     text         NOT NULL REFERENCES chains(chain_id) ON DELETE CASCADE,
    -- 0-based position of this step within the chain. step_index = 0 is the
    -- initiation step (level 1); step_index = N is the Nth propagation. This is
    -- the authoritative ordering key (NOT timestamp — distinct timeframes can
    -- share a bar timestamp).
    step_index   integer      NOT NULL,

    -- The step payload, columns not JSONB so each is a fixed-size append.
    level        integer      NOT NULL,   -- 1-based propagation level reached
    timeframe    text         NOT NULL,
    step_time    timestamptz  NOT NULL,   -- the bar timestamp of this step
    price        double precision,        -- per-step price (nullable, as before)

    recorded_at  timestamptz  NOT NULL DEFAULT now(),

    PRIMARY KEY (chain_id, step_index)
);

-- Hydrate reconstructs a chain's history with:
--   SELECT ... FROM chain_steps WHERE chain_id = $1 ORDER BY step_index
-- The PK (chain_id, step_index) already serves this exactly (leftmost prefix +
-- ordered range), so no additional index is needed.

-- NOTE on ON DELETE CASCADE: chain_steps is derived audit detail subordinate to
-- its chain, so if a chain row is ever deleted its steps go with it. This is the
-- OPPOSITE posture from trades.chain_id (RESTRICT) deliberately: a trade's
-- provenance must never be orphaned, but a chain's own step detail has no
-- independent meaning once the chain is gone.