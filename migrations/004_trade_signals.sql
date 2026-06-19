-- ============================================================================
-- 004_trade_signals.sql
--
-- The TRADE-SIGNALS table: this system's PRODUCT and a CROSS-TEAM CONTRACT.
--
-- This system's entry/exit LOGIC produces discrete trade-intent rows here:
-- "enter long this position" / "exit this position". A SEPARATE script (another
-- team) consumes these rows and does its own sizing and position management.
-- Later, this project's OWN position manager (the trades/orders/fills/exit_state
-- schema in 001) will consume the SAME rows — one output, two consumers,
-- switchable.
--
-- THIS IS NOT POSITION STATE. It has no fills, quantities, or escalation — those
-- are position-management concerns DOWNSTREAM of a signal. A signal only says
-- WHAT THE STRATEGY DECIDED. Position state lives in 001's schema, owned by
-- whoever is managing positions (the other team now, us later).
--
-- MULTIPLICITY: the strategy can hold MULTIPLE concurrent positions in one
-- symbol (from different chains). So a signal is about a POSITION, not a symbol —
-- "exit AAPL" would be ambiguous. Every signal carries a position_key derived
-- from (chain_id, side): the entry signal DEFINES the key, exit signals
-- REFERENCE it. The consumer tracks open positions by position_key and matches
-- each exit to exactly the position it names.
--
-- CONTRACT DISCIPLINE: because a separate team's code reads this table, its
-- shape is an interface. Changes here are lockstep with that consumer — the same
-- discipline as a wire type shared across services, except the consumer is code
-- we can't see. Coordinate column changes with that team before deploying.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- ENUM: signal_action
-- The two things a trade signal can announce. ENTER establishes a position and
-- defines its key; EXIT references an existing position by key and asks to close
-- it. (No "modify"/"resize" — sizing is the consumer's job, not the signal's.)
-- ----------------------------------------------------------------------------
CREATE TYPE signal_action AS ENUM ('ENTER', 'EXIT');


-- ----------------------------------------------------------------------------
-- ENUM: signal_direction
-- Direction of the POSITION the signal concerns. Mirrors ChainDirection / the
-- position side: LONG | SHORT. On an ENTER this is the position's direction; on
-- an EXIT it identifies (with chain_id) which position to close.
-- ----------------------------------------------------------------------------
CREATE TYPE signal_direction AS ENUM ('LONG', 'SHORT');


-- ----------------------------------------------------------------------------
-- TABLE: trade_signals
--
-- Append-only log of trade intents. One row per decision. Consumers read new
-- rows (by created_at or an offset) and act; they NEVER update rows here — this
-- is the producer's log, immutable once written.
-- ----------------------------------------------------------------------------
CREATE TABLE trade_signals (
    -- DETERMINISTIC signal id: same logical decision -> same id, so a replayed
    -- emit dedupes (our writer) and a re-read dedupes (their consumer). Built
    -- from (position_key, action, a decision discriminator) — see the writer.
    -- TEXT, not a random uuid, precisely so it is reproducible.
    signal_id         text         PRIMARY KEY,

    -- --- Position identity (the heart of the multiplicity contract) ---
    -- position_key uniquely names the position this signal concerns, derived
    -- from (chain_id, direction). An ENTER defines it; an EXIT references it.
    -- The consumer tracks open positions keyed on this.
    position_key      text         NOT NULL,
    chain_id          text         NOT NULL,   -- provenance; component of position_key
    direction         signal_direction NOT NULL,

    -- --- What and where ---
    action            signal_action NOT NULL,
    symbol            text         NOT NULL,
    asset_class       text         NOT NULL,   -- 'crypto' | 'equity' | 'fx'

    -- The propagation level that triggered this signal (entry trigger level on
    -- ENTER; the level at which the exit logic fired on EXIT). Auditing + lets a
    -- consumer weight signals by depth if it chooses. Nullable: not every exit
    -- reason is level-derived.
    trigger_level     integer,

    -- Decision price: the price our strategy SAW when it decided (Polygon bar
    -- for equity/fx, Binance book for crypto). The consumer measures its own
    -- slippage/basis against this. NOT an execution price — we don't execute.
    decision_price    double precision,

    -- Why an EXIT fired (STOP/TARGET/TIME/...), free-ish but mirrors the exit
    -- reasons the position manager uses, so both consumers interpret it the same
    -- way. NULL for ENTER.
    exit_reason       text,

    -- When the decision was made (strategy time, from the triggering signal's
    -- timestamp), and when we wrote it (wall clock). Consumers order by
    -- decided_at for strategy semantics; created_at is for their read cursor.
    decided_at        timestamptz  NOT NULL,
    created_at        timestamptz  NOT NULL DEFAULT now(),

    -- exit_reason present iff EXIT; an ENTER never carries one.
    CONSTRAINT ck_trade_signals_exit_reason
        CHECK ((action = 'EXIT') = (exit_reason IS NOT NULL))
);

-- Consumers poll for new rows; index the read cursor.
CREATE INDEX ix_trade_signals_created_at ON trade_signals (created_at);
-- Look up all signals for a position (entry + its exit), for reconciliation on
-- either side of the contract.
CREATE INDEX ix_trade_signals_position_key ON trade_signals (position_key);
CREATE INDEX ix_trade_signals_symbol ON trade_signals (symbol);

-- Lock down for the headless / cross-service posture (consumers connect with
-- their own service-role/credentials, not the public anon path).
ALTER TABLE trade_signals ENABLE ROW LEVEL SECURITY;


-- ----------------------------------------------------------------------------
-- NOTE ON THE EXIT-REFERENCES-A-POSITION GAP (a coordination point, not a thing
-- this table can fully solve):
--
-- An EXIT names a position_key that SHOULD be open in the consumer's view. But
-- across a team boundary, the consumer's state may disagree (they missed the
-- ENTER, or already closed it). This table makes the mismatch DETECTABLE — the
-- exit carries position_key + chain_id + direction so the consumer can look for
-- the matching open position and LOG a reconciliation error rather than silently
-- dropping the exit. It does NOT fix the distributed-state problem; that is a
-- coordination agreement with the consuming team (how they handle an EXIT for a
-- position they don't have open). Flag this in the contract conversation.
-- ----------------------------------------------------------------------------
