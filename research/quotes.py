"""
research/quotes.py — NBBO as of every equity trade_signals row.

QuoteCapture.handle(row) is the SignalListener handler (the executor's
listener class, reused: pg_notify fast path + cursor catch-up, at-least-once,
so handle() must be idempotent — the upsert keyed on signal_id is). It also
serves the backfill: the same function over rows the listener never saw.

As-of time is trade_signals.created_at — when the decision was WRITTEN. For
an event-driven intent that is within a second of the chain event; for a
clock-driven one (queued entry at the open, stall exit) it is the moment the
rule drained it. Either way it is the time the strategy committed, which is
the time a broker would have been asked.
"""
from __future__ import annotations

import logging
from typing import Awaitable, Callable, Optional

from research.polygon_research import PolygonResearchClient, Quote

log = logging.getLogger("research.quotes")


class QuoteCapture:
    def __init__(self, client: PolygonResearchClient,
                 upsert: Callable[[dict, Optional[Quote], str], Awaitable[None]]):
        self._client = client
        self._upsert = upsert
        self.stats = {"ok": 0, "no_quote": 0, "error": 0, "skipped": 0}

    async def handle(self, row: dict) -> None:
        if row.get("asset_class") != "equity":
            self.stats["skipped"] += 1
            return
        try:
            q = await self._client.nbbo_asof(row["symbol"], row["created_at"])
        except PermissionError as e:
            # No quotes entitlement: say so once per row, record error (retried
            # by backfill once the plan is fixed), keep the listener alive.
            self.stats["error"] += 1
            log.error("NBBO for %s %s: %s", row["symbol"], row["signal_id"], e)
            await self._upsert(row, None, "error")
            return
        except Exception:
            self.stats["error"] += 1
            log.exception("NBBO fetch failed for %s %s", row["symbol"], row["signal_id"])
            await self._upsert(row, None, "error")
            return
        if q is None:
            self.stats["no_quote"] += 1
            log.warning("no NBBO within lookback for %s at %s (%s)",
                        row["symbol"], row["created_at"], row["signal_id"])
            await self._upsert(row, None, "no_quote")
            return
        self.stats["ok"] += 1
        await self._upsert(row, q, "ok")
        log.info("NBBO %s %s %s: bid=%.4f ask=%.4f spread=%.1fbps decision=%s",
                 row["action"], row["symbol"], row["signal_id"][:12], q.bid, q.ask,
                 q.spread_bps or 0.0, row.get("decision_price"))
