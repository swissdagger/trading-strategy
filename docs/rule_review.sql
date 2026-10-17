-- docs/rule_review.sql — the pre-registered review for the chain rules
-- (strategy package §6), on THIS repo's tables.
--
-- The package logged paper fills to its own strategy_trades (next 1m price
-- minus a half-spread). Here a rule's round trips are in shadow_trades (fill
-- at decision_price, fee per leg = SHADOW_FEE_BPS_BY_CLASS / the venue tier),
-- and the REAL spread at each signal is in research_quotes. So two numbers
-- per rule: net at the assumed fee, and net at the measured spread.
--
-- Run once per strategy. Keep a rule only if mean_net_pct > 0 after 200
-- closed trades AND it beat holding each trade to the opposing P1 (rebuild
-- that from chains + research_bars_1m). Stop if negative after 100. Never
-- pool across ladders or rules.

\set strategy 'rule1_v1'

-- 1. At the assumed fee (shadow book).
SELECT count(*)                                                         AS trades,
       avg(pnl_bps) / 100.0                                             AS mean_net_pct,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY pnl_bps) / 100.0    AS median_net_pct,
       avg((pnl_bps > 0)::int)                                          AS win_rate,
       avg(pnl_bps) FILTER (WHERE exit_reason = 'TIME') / 100.0         AS stalled_mean_pct,
       avg(pnl_bps) FILTER (WHERE exit_reason = 'STOP') / 100.0         AS held_mean_pct
FROM shadow_trades
WHERE strategy = :'strategy' AND exit_at IS NOT NULL;

-- 2. At the MEASURED spread: entry at the ask (long) / bid (short) as of the
--    ENTER signal, exit at the opposite touch as of the EXIT signal.
WITH legs AS (
    SELECT st.strategy, st.position_key, st.side,
           CASE WHEN st.side = 'LONG' THEN qe.ask ELSE qe.bid END AS entry_px,
           CASE WHEN st.side = 'LONG' THEN qx.bid ELSE qx.ask END AS exit_px,
           st.exit_reason
    FROM shadow_trades st
    JOIN research_quotes qe ON qe.signal_id = st.entry_signal_id AND qe.status = 'ok'
    JOIN research_quotes qx ON qx.signal_id = st.exit_signal_id  AND qx.status = 'ok'
    WHERE st.strategy = :'strategy' AND st.exit_at IS NOT NULL
)
SELECT count(*) AS trades_with_quotes,
       avg(CASE WHEN side = 'LONG' THEN (exit_px / entry_px - 1) ELSE (entry_px / exit_px - 1) END) * 100
           AS mean_net_pct_at_measured_spread
FROM legs;
