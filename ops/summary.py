"""
ops/summary.py — the executor's daily Slack summary and kill-switch monitor.

Both read only the database, run on the executor's event loop, and report
through ops.alerts (log records tagged for Slack). Neither can affect trading:
a failed query is logged and retried on the next run.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from ops.alerts import alert

log = logging.getLogger("ops.summary")


def _fmt_rows(rows, fmt, empty="none") -> str:
    return "\n".join(fmt(r) for r in rows) if rows else f"  {empty}"


async def build_daily_summary(pool, since: datetime, *, mode: str = "?") -> str:
    """Slack text covering `since` -> now: real (paper/live) and shadow trades
    by strategy with realised P&L, positions open now, signals written and
    forecasts made."""
    async with pool.acquire() as conn:
        opened = await conn.fetch(
            """
            SELECT t.strategy, count(*) AS n
              FROM trades t
             WHERE (SELECT min(f.filled_at) FROM fills f
                     WHERE f.trade_id = t.trade_id AND f.intent = 'ENTRY') >= $1
             GROUP BY t.strategy ORDER BY t.strategy
            """, since)
        closed = await conn.fetch(
            """
            SELECT strategy, count(*) AS n, sum(pnl) AS pnl
              FROM (SELECT t.trade_id, t.strategy,
                           sum(CASE WHEN f.intent = 'EXIT' THEN f.qty * f.price
                                    ELSE -f.qty * f.price END) - sum(f.fee) AS pnl
                      FROM trades t JOIN fills f ON f.trade_id = t.trade_id
                     WHERE t.state = 'CLOSED' AND t.closed_at >= $1 AND t.side = 'LONG'
                     GROUP BY t.trade_id, t.strategy) x
             GROUP BY strategy ORDER BY strategy
            """, since)
        open_now = await conn.fetch(
            "SELECT state::text AS state, count(*) AS n FROM trades "
            "WHERE state NOT IN ('CLOSED', 'REJECTED') GROUP BY state ORDER BY state")
        sh_opened = await conn.fetch(
            "SELECT strategy, count(*) AS n FROM shadow_trades WHERE entry_at >= $1 "
            "GROUP BY strategy ORDER BY strategy", since)
        sh_closed = await conn.fetch(
            "SELECT strategy, count(*) AS n, avg(pnl_bps) AS avg_bps, sum(pnl_bps) AS sum_bps "
            "FROM shadow_trades WHERE exit_at >= $1 GROUP BY strategy ORDER BY strategy",
            since)
        sh_open = await conn.fetchval("SELECT count(*) FROM shadow_trades WHERE exit_at IS NULL")
        sigs = await conn.fetch(
            "SELECT strategy, action, count(*) AS n FROM trade_signals WHERE created_at >= $1 "
            "GROUP BY strategy, action ORDER BY strategy, action", since)
        forecasts = await conn.fetch(
            "SELECT asset_class, count(*) AS n FROM signals WHERE created_at >= $1 "
            "GROUP BY asset_class ORDER BY asset_class", since)
        ks = await conn.fetchrow("SELECT engaged, mode FROM kill_switch WHERE id = true")

    halted = sum(r["n"] for r in open_now if r["state"] == "HALTED")
    kill = (f"ENGAGED ({ks['mode']})" if ks is not None and ks["engaged"] else "off")
    lines = [
        f"*Daily summary* since {since:%Y-%m-%d %H:%M} UTC — trade mode *{mode}*, "
        f"kill switch {kill}" + (f", *{halted} HALTED*" if halted else ""),
        f"*Real trades ({mode})* opened:",
        _fmt_rows(opened, lambda r: f"  {r['strategy']}: {r['n']}"),
        "closed (realised P&L, quote currency, net of fees):",
        _fmt_rows(closed, lambda r: f"  {r['strategy']}: {r['n']} trade(s), {r['pnl']:+.2f}"),
        "open now: " + (", ".join(f"{r['state']} {r['n']}" for r in open_now) or "none"),
        "*Shadow trades* opened:",
        _fmt_rows(sh_opened, lambda r: f"  {r['strategy']}: {r['n']}"),
        "closed (P&L in bps, net of fees):",
        _fmt_rows(sh_closed, lambda r: f"  {r['strategy']}: {r['n']} trade(s), mean "
                  f"{float(r['avg_bps']):+.1f}, total {float(r['sum_bps']):+.1f}"),
        f"open now: {sh_open}",
        "*Signals written*: " + (", ".join(f"{r['strategy']} {r['action']} {r['n']}"
                                           for r in sigs) or "none"),
        "*Forecasts*: " + (", ".join(f"{r['asset_class']} {r['n']}" for r in forecasts)
                           or "none — is the signal process running?"),
    ]
    return "\n".join(lines)


def _next_run(now: datetime, hour_utc: int) -> datetime:
    run = now.replace(hour=hour_utc, minute=0, second=0, microsecond=0)
    return run if run > now else run + timedelta(days=1)


async def daily_summary_loop(pool, hour_utc: int, mode: Callable[[], str],
                             *, clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
                             ) -> None:
    """Post build_daily_summary once a day at hour_utc, covering 24 hours."""
    while True:
        now = clock()
        run_at = _next_run(now, hour_utc)
        await asyncio.sleep((run_at - now).total_seconds())
        try:
            text = await build_daily_summary(pool, run_at - timedelta(days=1), mode=mode())
            log.info("%s", text, extra=alert(f"daily:{run_at:%Y-%m-%d}", every=0))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("daily summary failed (next one tomorrow)")


async def kill_switch_monitor(pool, *, interval_s: float = 10.0) -> None:
    """Alert when the kill switch is engaged, changes mode, or is released —
    and once at startup if it is already engaged."""
    last: Optional[tuple] = None
    while True:
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT engaged, mode, reason, engaged_by FROM kill_switch WHERE id = true")
            state = (bool(row["engaged"]), row["mode"]) if row is not None else (False, None)
            if state != last and (last is not None or state[0]):
                if state[0]:
                    log.warning("kill switch ENGAGED (%s)%s%s — %s", state[1],
                                f": {row['reason']}" if row["reason"] else "",
                                f" by {row['engaged_by']}" if row["engaged_by"] else "",
                                "closing every position" if state[1] == "flatten"
                                else "no new entries",
                                extra=alert(f"kill:{state}", every=0))
                else:
                    log.warning("kill switch released — entries allowed again",
                                extra=alert("kill:released", every=0))
            last = state
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("kill switch read failed")
        await asyncio.sleep(interval_s)
