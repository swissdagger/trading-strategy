-- ============================================================================
-- 022_paper_ledger.sql
--
-- The PAPER account (ADR-001 Phase 5). Coinbase has no paper venue, so paper
-- trading is simulated in-process by execution/paper_adapter.py against the
-- real market (touch, product rules, fee tier). These tables are that paper
-- account's ledger, so a restart resumes the same account and the executor's
-- boot reconcile runs against it exactly as it will against the real venue.
--
--   paper_balances — one row per currency (the quote currency is the cash).
--   paper_orders   — every simulated order, keyed by the executor's
--                    deterministic client_order_id (repeated id = same order).
--   paper_fills    — every simulated fill (fee in the quote currency).
--
-- A fill's three writes (fill row, order status, balance deltas) are one
-- transaction. Isolated from the live trade lifecycle: nothing here touches
-- trades/orders/fills (001), which the executor writes for paper and live alike.
-- To reset the paper account: TRUNCATE paper_fills, paper_orders, paper_balances
-- (it is re-seeded with config.PAPER_STARTING_CASH on the next executor start).
-- ============================================================================

CREATE TABLE IF NOT EXISTS paper_balances (
    currency    text           PRIMARY KEY,
    qty         numeric(38,18) NOT NULL,
    updated_at  timestamptz    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS paper_orders (
    client_order_id  text           PRIMARY KEY,
    venue_order_id   text           NOT NULL UNIQUE,
    symbol           text           NOT NULL,
    side             text           NOT NULL CHECK (side IN ('BUY', 'SELL')),
    intent           text           NOT NULL CHECK (intent IN ('ENTRY', 'EXIT')),
    qty              numeric(38,18) NOT NULL CHECK (qty > 0),
    limit_price      numeric(38,18) NOT NULL CHECK (limit_price > 0),
    status           text           NOT NULL CHECK (status IN ('OPEN', 'FILLED', 'CANCELLED')),
    filled_qty       numeric(38,18) NOT NULL DEFAULT 0,
    created_at       timestamptz    NOT NULL DEFAULT now(),
    updated_at       timestamptz    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_paper_orders_open ON paper_orders (symbol) WHERE status = 'OPEN';

CREATE TABLE IF NOT EXISTS paper_fills (
    fill_id          text           PRIMARY KEY,
    client_order_id  text           NOT NULL REFERENCES paper_orders (client_order_id),
    venue_order_id   text           NOT NULL,
    symbol           text           NOT NULL,
    intent           text           NOT NULL,
    qty              numeric(38,18) NOT NULL,
    price            numeric(38,18) NOT NULL,
    fee              numeric(38,18) NOT NULL,
    fee_currency     text           NOT NULL,
    filled_at        timestamptz    NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_paper_fills_filled_at ON paper_fills (filled_at);

ALTER TABLE paper_balances ENABLE ROW LEVEL SECURITY;
ALTER TABLE paper_orders   ENABLE ROW LEVEL SECURITY;
ALTER TABLE paper_fills    ENABLE ROW LEVEL SECURITY;

-- ROLLBACK (manual):  DROP TABLE IF EXISTS paper_fills, paper_orders, paper_balances;
