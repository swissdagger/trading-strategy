-- Paper-run review queries (RUNBOOK §6): P&L and fee drag, fill vs touch,
-- reconcile stability, shadow shorts, the paper account. Run against the trading DB.

-- Realized P&L and fee drag per symbol (closed trades, long-only).
WITH f AS (
  SELECT trade_id,
         sum(qty * price) FILTER (WHERE intent = 'ENTRY') AS bought,
         sum(qty * price) FILTER (WHERE intent = 'EXIT')  AS sold,
         sum(fee)                                          AS fees
    FROM fills GROUP BY trade_id)
SELECT t.symbol, count(*) AS trades,
       round(sum(f.sold - f.bought)::numeric, 2)            AS gross_pnl,
       round(sum(f.fees)::numeric, 2)                        AS fees,
       round(sum(f.sold - f.bought - f.fees)::numeric, 2)    AS net_pnl,
       round((10000 * sum(f.fees) / nullif(sum(f.bought), 0))::numeric, 1) AS fee_bps_of_entry
  FROM trades t JOIN f USING (trade_id)
 WHERE t.state = 'CLOSED'
 GROUP BY t.symbol ORDER BY net_pnl;

-- Fill vs. touch: what each fill paid relative to the touch it was priced off
-- (bps; positive = cost). Paper fills at the limit, so this is the cap in use.
SELECT f.intent, count(*) AS fills,
       round((avg(CASE WHEN f.intent = 'ENTRY'
                      THEN (f.price - o.venue_reference) / o.venue_reference
                      ELSE (o.venue_reference - f.price) / o.venue_reference END) * 10000)::numeric, 1)
         AS avg_cost_bps,
       round((max(CASE WHEN f.intent = 'ENTRY'
                      THEN (f.price - o.venue_reference) / o.venue_reference
                      ELSE (o.venue_reference - f.price) / o.venue_reference END) * 10000)::numeric, 1)
         AS worst_cost_bps
  FROM fills f JOIN orders o ON o.venue_order_id = f.venue_order_id
 WHERE o.venue_reference > 0
 GROUP BY f.intent;

-- Reconcile stability: HALTs and refused boots.
SELECT kind, count(*) AS n, max(created_at) AS last
  FROM executor_alerts GROUP BY kind ORDER BY n DESC;
SELECT state, count(*) FROM trades GROUP BY state ORDER BY state;
SELECT trade_id, symbol, halt_reason, last_updated FROM trades WHERE state = 'HALTED';

-- Shadow shorts: an UPPER BOUND on what the short side would have made.
SELECT strategy, count(*) AS closed,
       round(avg(pnl_bps), 1)                                        AS avg_net_bps,
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY pnl_bps))::numeric, 1) AS median_net_bps,
       round(100.0 * avg((pnl_bps > 0)::int), 1)                     AS win_pct
  FROM shadow_trades WHERE exit_at IS NOT NULL GROUP BY strategy;

-- The paper account.
SELECT currency, qty FROM paper_balances ORDER BY currency;
SELECT count(*) AS fills, round(sum(fee), 2) AS fees FROM paper_fills;

