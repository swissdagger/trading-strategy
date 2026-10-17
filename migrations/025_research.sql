-- ============================================================================
-- 025_research.sql
--
-- Research data collection (PR 11), written ONLY by research_process.py — a
-- separate process that reads trade_signals (pg_notify + cursor, the
-- executor's mechanism) and Polygon, and shares nothing with the live path.
-- Nothing on the signal or executor side reads these tables.
--
-- Four questions, four tables:
--   research_quotes    NBBO as of each Rule 1 ENTER/EXIT: what spread would
--                      the trade have paid? (one row per trade_signals row)
--   research_bars_1m   1-minute bars WITH volume, VWAP and trade count, for
--                      the traded equities plus market references (SPY, QQQ)
--                      and VIXY: are extended-hours prints real? liquidity
--                      filter; forward-looking vol vs the backward filter.
--   research_earnings  earnings dates per symbol: is the vol filter just an
--                      earnings-day filter?
--   research_cursor    the research listener's durable cursor (same shape and
--                      semantics as executor_cursor, migration 020).
-- ============================================================================

CREATE TABLE IF NOT EXISTS research_quotes (
    signal_id     text         PRIMARY KEY REFERENCES trade_signals (signal_id),
    symbol        text         NOT NULL,
    action        text         NOT NULL,            -- ENTER | EXIT (copied for querying)
    strategy      text         NOT NULL,
    signal_at     timestamptz  NOT NULL,            -- trade_signals.created_at: the as-of time
    quote_ts      timestamptz,                      -- the NBBO's own SIP timestamp (<= signal_at)
    bid           numeric,
    ask           numeric,
    bid_size      numeric,
    ask_size      numeric,
    spread_bps    numeric,                          -- (ask - bid) / mid * 1e4, NULL if no quote
    decision_price numeric,                         -- the strategy's price, for the slippage diff
    source        text         NOT NULL DEFAULT 'polygon_v3_quotes',
    captured_at   timestamptz  NOT NULL DEFAULT now(),
    -- A row with NULL bid/ask records that we LOOKED and found nothing (no
    -- quote at or before signal_at in the window we asked for), so the
    -- backfill does not retry it forever. status says why.
    status        text         NOT NULL DEFAULT 'ok'   -- ok | no_quote | error
);
CREATE INDEX IF NOT EXISTS ix_research_quotes_symbol_at ON research_quotes (symbol, signal_at);

CREATE TABLE IF NOT EXISTS research_bars_1m (
    symbol        text    NOT NULL,
    timestamp_ms  bigint  NOT NULL,                 -- window start, epoch ms (as warmup_bars)
    open          double precision NOT NULL,
    high          double precision NOT NULL,
    low           double precision NOT NULL,
    close         double precision NOT NULL,
    volume        double precision,
    vwap          double precision,
    trade_count   integer,
    fetched_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, timestamp_ms)
);

CREATE TABLE IF NOT EXISTS research_earnings (
    symbol        text   NOT NULL,
    report_date   date   NOT NULL,
    time_of_day   text   NOT NULL DEFAULT 'unknown',  -- bmo | amc | unknown
    source        text   NOT NULL,                    -- csv:<file> | provider name
    fetched_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, report_date)
);

CREATE TABLE IF NOT EXISTS research_cursor (
    id          boolean      PRIMARY KEY DEFAULT true,
    cursor_at   timestamptz  NOT NULL,
    updated_at  timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT ck_research_cursor_singleton CHECK (id = true)
);

-- Deny-all RLS like every other table (003): services connect as a role that
-- bypasses it; there is no browser client.
ALTER TABLE research_quotes   ENABLE ROW LEVEL SECURITY;
ALTER TABLE research_bars_1m  ENABLE ROW LEVEL SECURITY;
ALTER TABLE research_earnings ENABLE ROW LEVEL SECURITY;
ALTER TABLE research_cursor   ENABLE ROW LEVEL SECURITY;

-- ROLLBACK (manual):
--   DROP TABLE IF EXISTS research_cursor, research_earnings, research_bars_1m, research_quotes;
