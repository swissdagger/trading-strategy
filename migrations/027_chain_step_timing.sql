-- ============================================================================
-- 027_chain_step_timing.sql
--
-- Bars are labelled by their OPEN time, so a chain step's step_time (and
-- chains.origin_time, trade_signals.decided_at) is the open of the bar that
-- decided it — one timeframe BEFORE the information existed. The signals
-- table, by contrast, stores Sumtyme's placeholder, which is that bar's
-- CLOSE. Readers had to infer which was which. Record every time explicitly:
--
--   chain_steps.bar_close          the deciding bar's close: when the
--                                  information existed (= step_time + one
--                                  timeframe; = signals.timestamp)
--   chain_steps.detected_at        when the chain machine made the step
--                                  (wall clock; recorded_at is the DB write)
--   trade_signals.decided_bar_close  the deciding bar's close, for an
--                                  event-driven signal (NULL for clock-driven
--                                  ones, whose decided_at is already the
--                                  wall clock)
--
-- Nullable: rows written before this migration have none. For those,
-- bar_close = step_time + the step's timeframe.
-- ============================================================================

ALTER TABLE chain_steps   ADD COLUMN IF NOT EXISTS bar_close         timestamptz;
ALTER TABLE chain_steps   ADD COLUMN IF NOT EXISTS detected_at       timestamptz;
ALTER TABLE trade_signals ADD COLUMN IF NOT EXISTS decided_bar_close timestamptz;

-- ROLLBACK (manual):
--   ALTER TABLE chain_steps   DROP COLUMN bar_close, DROP COLUMN detected_at;
--   ALTER TABLE trade_signals DROP COLUMN decided_bar_close;
