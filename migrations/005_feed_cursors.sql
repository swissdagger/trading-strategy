-- ============================================================================
-- 005_feed_cursors.sql
--
-- The cursor handoff between the signal process (where warmup runs and the chain
-- machine buffers live) and the feed process (which polls sources and publishes
-- bars). They are SEPARATE processes that restart independently, so the handoff
-- must be DURABLE — a DB table, not in-memory.
--
-- After warmup seeds the buffers, the signal process writes the high-water
-- timestamp per (symbol, timeframe) here. The feed process reads these at startup
-- and passes them to AssetFeed.start_live as initial_cursors, so live ingestion
-- resumes EXACTLY where warmup ended — no gap (missed bars), no overlap (double-
-- counted bars, which the position-based buffer can't dedupe).
--
-- The feed NEVER blocks on the signal process: it reads whatever cursors are
-- present and proceeds. On a fresh first boot the table is empty and the feed
-- starts from "now" (warmup's fetch_history fills the buffers regardless, so the
-- system self-corrects); on every subsequent boot the cursors are present.
--
-- timestamp is epoch milliseconds (matches OHLCPoint.timestamp and the bar wire
-- shape). Last-write-wins per (symbol, timeframe).
-- ============================================================================

CREATE TABLE feed_cursors (
    symbol        text        NOT NULL,
    timeframe     text        NOT NULL,
    -- High-water bar timestamp (epoch ms) the live feed should resume strictly
    -- after. Written by the signal process post-warmup.
    cursor_ms     bigint      NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, timeframe)
);

-- Lock down for the headless posture (service_role bypasses).
ALTER TABLE feed_cursors ENABLE ROW LEVEL SECURITY;