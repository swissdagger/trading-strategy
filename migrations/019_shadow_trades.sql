-- ============================================================================
-- 019_shadow_trades.sql
--
-- The SHADOW BOOK (ADR-001, Phase 3). The account is UK retail spot: there is
-- no short side, so SHORT signals are never sized or sent to the venue. They
-- are still the only short-side data available, so the executor routes them to
-- execution/shadow_book.py, which records a VIRTUAL round trip here:
--
--   entry_px  — the touch the entry would take (a short SELLS: the bid)
--   exit_px   — the touch the exit would take (a short BUYS back: the ask)
--   fee_bps   — taker fee per leg, charged on both legs
--   pnl_bps   — net round-trip P&L in bps of entry_px (after both fees)
--
-- An UPPER BOUND on what the trade would have made: touch fills, no impact,
-- no latency (ADR-001 debt D8). Feeds the research ticket on using short-chain
-- state as a gate / exit / risk-off modifier for the long book.
--
-- Isolated from the trade-lifecycle schema (001): no FK into trades, nothing
-- in reconcile, sizing or the kill switch reads it.
-- ============================================================================

CREATE TABLE IF NOT EXISTS shadow_trades (
    shadow_id        bigserial    PRIMARY KEY,
    strategy         text         NOT NULL,
    position_key     text         NOT NULL,   -- "{chain_id}:{L|S}", as trade_signals
    symbol           text         NOT NULL,
    asset_class      text         NOT NULL,
    side             trade_side   NOT NULL,

    entry_signal_id  text         NOT NULL UNIQUE,   -- replayed ENTER dedupes here
    entry_px         numeric(38,18) NOT NULL,
    entry_at         timestamptz  NOT NULL,

    exit_signal_id   text         UNIQUE,
    exit_px          numeric(38,18),
    exit_at          timestamptz,
    exit_reason      text,

    fee_bps          numeric(12,4) NOT NULL,
    pnl_bps          numeric(12,4),

    created_at       timestamptz  NOT NULL DEFAULT now(),

    -- One virtual position per (strategy, position_key), like trades' (chain_id, side).
    CONSTRAINT uq_shadow_trades_position UNIQUE (strategy, position_key),
    -- An exit is all-or-nothing: either every exit column is set, or none is.
    CONSTRAINT ck_shadow_trades_exit_complete CHECK (
        (exit_at IS NULL) = (exit_px IS NULL)
        AND (exit_at IS NULL) = (exit_signal_id IS NULL)
        AND (exit_at IS NULL) = (pnl_bps IS NULL)
    )
);

-- Boot reloads open virtual positions.
CREATE INDEX IF NOT EXISTS ix_shadow_trades_open
    ON shadow_trades (strategy, position_key) WHERE exit_at IS NULL;

ALTER TABLE shadow_trades ENABLE ROW LEVEL SECURITY;

-- ROLLBACK (manual):  DROP TABLE IF EXISTS shadow_trades;
