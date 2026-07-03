-- 006_warmup_bars.sql — durable buffer-seed store for cold-start warmup.
--
-- WHY THIS TABLE EXISTS
-- The chain machine forecasts only once a (symbol, timeframe) buffer holds
-- WINDOW_SIZE (5000) bars. On a cold start those buffers fill ONLY from the
-- live Kafka stream — one bar per period — so coarse timeframes take days to
-- weeks before the first forecast. The warmup process (warmup_process.py)
-- back-fills history via feed.fetch_history and lands it here; the signal
-- process reads it back through make_load_buffers() and hydrate() pre-fills
-- each machine's deque, so forecasting starts immediately.
--
-- WHY A NEW TABLE (not the Kafka topic)
-- The signal consumer runs auto_offset_reset='latest', so replaying history
-- through trading-bars would NOT reach a consumer sitting at the latest
-- offset. The handoff MUST be durable state the signal process reads at
-- startup — exactly the same pattern as feed_cursors (migration 005).
--
-- SHAPE
-- One row per (symbol, timeframe, timestamp) bar. The columns mirror
-- producer.bar.OHLCPoint / ServiceTwoChainMachine._tick_dict 1:1 so the
-- loader reconstructs buffer ticks with no transformation. timestamp is
-- epoch MILLISECONDS (int8), the system-wide bar convention — stored as
-- bigint, NOT a timestamptz, to avoid any tz round-trip on the hot
-- hydrate path.

CREATE TABLE IF NOT EXISTS warmup_bars (
    symbol       text   NOT NULL,
    timeframe    text   NOT NULL,
    timestamp_ms bigint NOT NULL,          -- epoch ms, matches OHLCPoint.timestamp
    open         double precision NOT NULL,
    high         double precision NOT NULL,
    low          double precision NOT NULL,
    close        double precision NOT NULL,
    asset_class  text   NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, timeframe, timestamp_ms)
);

-- The loader fetches one (symbol, timeframe) buffer at a time, newest bars
-- last, capped at WINDOW_SIZE. This index serves that ORDER BY ... LIMIT.
CREATE INDEX IF NOT EXISTS warmup_bars_stream_ts_idx
    ON warmup_bars (symbol, timeframe, timestamp_ms DESC);

-- A re-run of warmup re-UPSERTs the same rows (ON CONFLICT DO UPDATE);
-- the PK makes that idempotent. Old bars beyond the newest WINDOW_SIZE are
-- harmless (the loader's LIMIT ignores them) but accumulate — see the
-- prune note in persistence.make_persist_warmup_bars.