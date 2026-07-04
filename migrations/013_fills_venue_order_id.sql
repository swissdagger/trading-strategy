-- ============================================================================
-- 013_fills_venue_order_id.sql
--
-- Adds the venue's own ORDER id to the fills table, so an asynchronously
-- delivered fill can be attributed to the order (and thus the trade) it belongs
-- to. Every venue stamps a fill with its parent order — Binance userTrades has
-- `orderId`, IB executions have permId/orderId — and the executor now carries
-- that through on the Fill so attribution no longer depends on a fill arriving
-- synchronously in the submit response.
--
-- WHY A SEPARATE COLUMN FROM order_id. `order_id` is OUR internal uuid FK into
-- the orders table; `venue_order_id` is the VENUE's identifier. They serve
-- different masters: order_id joins our own tables, venue_order_id is how we
-- recognize a fill the venue pushes at us. Keeping both lets a fill be recorded
-- with the venue linkage even in the brief window before (or if) we resolve it
-- to our internal order row.
--
-- NULLABILITY. venue_order_id is nullable: a fill discovered during reconcile,
-- before we know our orders, may not carry it (or may be a foreign/manual
-- trade). The executor persists such fills for the position reconcile to
-- explain rather than dropping them. The existing UNIQUE(venue_exec_id) still
-- guarantees no double-count regardless of attribution state.
--
-- This is additive and non-breaking: existing rows get NULL, and every code
-- path that already wrote fills keeps working (the column is optional on write).
-- ============================================================================

ALTER TABLE fills
    ADD COLUMN IF NOT EXISTS venue_order_id text;

-- Index for the reconcile/attribution lookups that go fill -> order by the
-- venue's id (e.g. "which fills belong to this venue order?"). Partial: only
-- rows that actually carry the id.
CREATE INDEX IF NOT EXISTS ix_fills_venue_order_id
    ON fills (venue_order_id)
    WHERE venue_order_id IS NOT NULL;


-- ----------------------------------------------------------------------------
-- ROLLBACK (manual):
--   DROP INDEX IF EXISTS ix_fills_venue_order_id;
--   ALTER TABLE fills DROP COLUMN IF EXISTS venue_order_id;
-- ----------------------------------------------------------------------------