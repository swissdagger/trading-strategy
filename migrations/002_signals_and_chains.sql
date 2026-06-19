-- ============================================================================
-- 002_signals_and_chains.sql
--
-- Research + audit tables: the raw Sumtyme signals and the chains derived from
-- them. Added because (a) raw signals are ground truth that is UNRECOVERABLE
-- once discarded — you cannot re-derive a -1/0/+1 Sumtyme output after the fact
-- without re-running the model against the exact historical buffer — and a
-- colleague wants them retained for future research; and (b) co-locating chains
-- here lets trades.chain_id be a REAL foreign key, turning "these strings happen
-- to match" into "this trade provably came from a real chain" — a bulletproof
-- audit trail for real capital.
--
-- These mirror CausaView's `signals` and `chains` schema column-for-column so
-- the research data is identically shaped and any CausaView tooling transfers.
-- This system COMPUTES its own chains (own Service-1/2-equivalent) and persists
-- them here; it shares no DB with CausaView at runtime. If CausaView's chain
-- schema evolves, these are the tables to keep in sync (same documented
-- manual-sync obligation accepted for the state machine itself).
--
-- ORDERING INVARIANT THIS RELIES ON: the chain row is written to `chains`
-- BEFORE the chain_initiated event is emitted (this is CausaView's existing
-- write-before-emit pattern — INSERT INTO chains precedes producer.send). The
-- FK on trades.chain_id depends on that ordering: the chain must exist before
-- the executor consumes the event and inserts a trade. Do NOT reorder to
-- emit-before-persist for latency without removing or deferring the FK — the FK
-- would (correctly) reject a trade that races ahead of its chain's persistence.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- TABLE: signals
--
-- Raw Sumtyme outputs, one row per (symbol, timeframe, timestamp). signal is
-- the -1/0/+1 the model emitted. This is the lowest-level ground truth in the
-- system — everything (chains, trades) derives from it, and it cannot be
-- reconstructed. Retained indefinitely for research.
--
-- Unique key (symbol, timeframe, timestamp) mirrors CausaView; UPSERTs are
-- idempotent via ON CONFLICT DO NOTHING at the writer, so a replay or restart
-- re-persisting the same signal is a no-op, never a duplicate.
-- ----------------------------------------------------------------------------
CREATE TABLE signals (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol          text         NOT NULL,
    timeframe       text         NOT NULL,
    timestamp       timestamptz  NOT NULL,
    signal          smallint     NOT NULL,   -- -1 | 0 | +1
    asset_class     text         NOT NULL,   -- 'crypto' | 'equity' | 'fx'
    -- The price at the bar that produced this signal. Carried so research can
    -- align signals to price without a join back to a prices table (which this
    -- DB deliberately does NOT keep — raw OHLC stays in the CausaView pipeline).
    price           double precision,
    created_at      timestamptz  NOT NULL DEFAULT now(),

    -- The -1/0/+1 domain, enforced. A signal outside this set is a bug at the
    -- writer, caught here rather than silently stored.
    CONSTRAINT ck_signals_value CHECK (signal IN (-1, 0, 1)),
    CONSTRAINT uq_signals_stream UNIQUE (symbol, timeframe, timestamp)
);

-- Research access patterns: by symbol over time, and by timeframe slice.
CREATE INDEX ix_signals_symbol_ts   ON signals (symbol, timestamp);
CREATE INDEX ix_signals_asset_class ON signals (asset_class);


-- ----------------------------------------------------------------------------
-- TABLE: chains
--
-- One row per chain, mirroring CausaView's chains table. chain_id is the
-- str(uuid4) the state machine assigned at INIT — here it is the PRIMARY KEY,
-- so trades.chain_id can foreign-key to it.
--
--   * direction          -1 | +1 (never 0 — a chain is directional once init'd)
--   * propagation_levels  highest level reached (1..N)
--   * active_timeframes   the timeframes the chain has propagated through
--   * history             JSONB array of {level, timeframe, timestamp, price}
--                         propagation steps (the per-step `price` was a later
--                         CausaView addition; older rows may lack it — kept
--                         nullable-in-spirit by living inside JSONB)
--   * origin_price        canonical price at initiation
--   * active / terminated_at  active=true & terminated_at IS NULL while live;
--                         on TERMINATE, active=false and terminated_at is set.
--
-- Service-2-equivalent hydrates active chains from here on restart so chain
-- state survives crashes — same as CausaView.
-- ----------------------------------------------------------------------------
CREATE TABLE chains (
    chain_id            text         PRIMARY KEY,   -- str(uuid4); FK target for trades
    symbol              text         NOT NULL,
    asset_class         text         NOT NULL,      -- 'crypto' | 'equity' | 'fx'
    direction           smallint     NOT NULL,      -- -1 | +1
    origin_time         timestamptz  NOT NULL,
    origin_price        double precision,           -- canonical price at initiation
    propagation_levels  integer      NOT NULL DEFAULT 1,
    active_timeframes   text[]       NOT NULL,
    history             jsonb        NOT NULL DEFAULT '[]'::jsonb,
    active              boolean      NOT NULL DEFAULT true,
    last_updated        timestamptz  NOT NULL DEFAULT now(),
    terminated_at       timestamptz,

    -- direction is strictly directional (matches ChainDirection's -1|1 on the
    -- wire; 0 is a signal value, never a chain direction).
    CONSTRAINT ck_chains_direction CHECK (direction IN (-1, 1)),
    -- active iff not terminated — keeps the two fields from disagreeing.
    CONSTRAINT ck_chains_active_terminated
        CHECK (active = (terminated_at IS NULL))
);

CREATE INDEX ix_chains_symbol   ON chains (symbol);
-- Hydrate-on-restart scans active chains; partial index keeps it small as
-- terminated chains accumulate.
CREATE INDEX ix_chains_active   ON chains (active) WHERE active = true;


-- ----------------------------------------------------------------------------
-- Wire the FK from trades.chain_id -> chains.chain_id.
--
-- Added here (not in 001) because 001 must stand alone as the trade-lifecycle
-- contract, and the chains table it references is introduced in this migration.
-- Apply 001 then 002 in order.
--
-- ON DELETE RESTRICT (the default, stated explicitly for the record): you
-- CANNOT delete a chain that has trades referencing it. This is correct for an
-- audit trail — never orphan a trade's provenance. If chains are ever pruned
-- for space, prune trades first (or never prune chains that bear trades).
-- RESTRICT makes "don't orphan a trade's chain" a DB guarantee, not a thing to
-- remember.
--
-- This FK encodes a dependency: a trade insert requires its chain to already
-- exist. Safe under the write-before-emit ordering documented at the top of
-- this file.
-- ----------------------------------------------------------------------------
ALTER TABLE trades
    ADD CONSTRAINT fk_trades_chain_id
    FOREIGN KEY (chain_id) REFERENCES chains(chain_id)
    ON DELETE RESTRICT;
