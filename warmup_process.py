"""
warmup_process.py — Cold-start buffer seeder. The missing third process.

THE PROBLEM IT SOLVES
The chain machine forecasts only once a (symbol, timeframe) buffer holds
WINDOW_SIZE (5000) bars. Cold-started buffers fill ONLY from the live Kafka
stream — one bar per period — so coarse timeframes take days-to-weeks before
the first forecast (e.g. 10m ≈ 52 extended-session days to reach 5000). This
process back-fills that history up front so forecasting starts on day one.

WHY A SEPARATE PROCESS (not folded into feed or signal)
  - The FEED process can fetch_history but its output is the live Kafka topic;
    the signal consumer runs auto_offset_reset='latest', so replayed history
    would NOT reach it. The feed can't fill the buffers through its normal path.
  - The SIGNAL process owns the buffers but imports no feeds and can't fetch.
  - Warmup is a BOUNDED batch job that runs once and EXITS; both other
    processes are unbounded restart-looped streamers. Fusing a one-shot
    backfill into either means it re-runs on every restart or needs gating.
The codebase already anticipates this actor: feed_cursors is documented as
written "by the signal process AFTER warmup" and read by the feed at startup.
This process is that actor, finally implemented. It reuses _build_feeds and
the persistence layer; it imports neither process's runtime.

WHAT IT DOES
  1. Build feeds from ASSET_CONFIGS (same _build_feeds the feed process uses).
  2. For each (symbol, timeframe) in the class's configured union, fetch_history
     up to WINDOW_SIZE bars and UPSERT into warmup_bars (migration 006). All
     streams seed in PARALLEL, sharing a semaphore so no feed starves another.
  3. Write feed_cursors high-water marks so the feed's live poller resumes
     exactly where warmup ended (no gap, no overlap-replay).
  4. Exit 0. Compose runs it to completion before feed + signal start.

It does NOT touch the chain machine or its deques — the handoff is purely
durable state (warmup_bars + feed_cursors). The signal process pre-fills its
buffers from warmup_bars via make_load_buffers/hydrate at its own startup.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Dict, List

import asyncpg

from config import ASSET_CONFIGS
from feed_process import _build_feeds
from producer.persistence import make_persist_warmup_bars
from producer.service2_machine import WINDOW_SIZE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
log = logging.getLogger("warmup_process")

# Bound total concurrent fetches across ALL feeds so a many-symbol seed can't
# over-commit sockets or blow past Polygon's budget. Each feed ALSO has its own
# RateLimiter (constructed inside the feed); this is a coarser global ceiling
# on top of that. Raise via env if first warmup proves slow.
MAX_CONCURRENT_FETCHES = int(os.environ.get("WARMUP_MAX_CONCURRENT", "8"))


async def _seed_one_stream(
    feed,
    asset_class: str,
    symbol: str,
    timeframe: str,
    persist_warmup_bars,
    sem: asyncio.Semaphore,
    cursor_out: Dict[str, Dict[str, int]],
) -> None:
    """Fetch up to WINDOW_SIZE bars for one (symbol, timeframe) and upsert them
    into warmup_bars. Records the newest timestamp into cursor_out for the
    feed-cursor handoff. Failures are logged, not raised — one dead stream
    must not abort the whole warmup (mirrors fetch_history's own
    partial-return posture)."""
    async with sem:
        # fetch_history walks backward from now and returns ascending by
        # timestamp; it may return FEWER than WINDOW_SIZE (young listing,
        # sparse coarse series) — that's fine, the buffer just isn't full
        # yet and accumulates live, exactly the documented partial-ladder case.
        if timeframe not in feed.supported_timeframes():
            # e.g. 5s requested in config but the feed (or plan) doesn't
            # support it. Skip rather than crash — config/feed reconciliation
            # is a separate decision; warmup shouldn't be the thing that fails.
            log.warning(
                "skip %s:%s %s — not in feed.supported_timeframes()",
                asset_class, symbol, timeframe,
            )
            return
        try:
            points = await feed.fetch_history(symbol, timeframe, WINDOW_SIZE)
        except Exception:
            log.exception("fetch_history failed for %s:%s %s",
                          asset_class, symbol, timeframe)
            return

        if not points:
            log.warning("0 bars for %s:%s %s — buffer stays cold (will fill live)",
                        asset_class, symbol, timeframe)
            return

        await persist_warmup_bars(symbol, asset_class, timeframe, points)
        newest_ms = points[-1].timestamp   # ascending -> last is newest
        cursor_out.setdefault(symbol, {})[timeframe] = newest_ms
        log.info("seeded %s:%s %s with %d bars (newest=%d)",
                 asset_class, symbol, timeframe, len(points), newest_ms)


async def _warmup_all(pool: asyncpg.Pool) -> Dict[str, Dict[str, int]]:
    """Build feeds, fan out one seed task per (symbol, timeframe) across all
    feeds in parallel under the shared semaphore. Returns the cursor map to
    persist."""
    feeds = _build_feeds(ASSET_CONFIGS)
    if not feeds:
        log.warning("no feeds to warm — ASSET_CONFIGS has no symbols")
        return {}

    persist_warmup_bars = make_persist_warmup_bars(pool)
    sem = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)
    cursors: Dict[str, Dict[str, int]] = {}
    tasks: List[asyncio.Task] = []

    for asset_class, feed in feeds.items():
        cfg = ASSET_CONFIGS[asset_class]
        symbols = cfg.get("symbols", [])
        # Seed the UNION of configured timeframes once per symbol — a timeframe
        # shared across Set A and Set B (30s/1m/2m) is ONE buffer; the loader
        # filters per-set on read. Seeding per-set here would double-fetch.
        timeframes = cfg.get("timeframes", [])
        for symbol in symbols:
            for tf in timeframes:
                tasks.append(asyncio.create_task(
                    _seed_one_stream(
                        feed, asset_class, symbol, tf,
                        persist_warmup_bars, sem, cursors,
                    )
                ))

    if not tasks:
        log.warning("no (symbol, timeframe) streams to seed")
        return {}

    log.info("warmup: seeding %d streams (max %d concurrent fetches)",
             len(tasks), MAX_CONCURRENT_FETCHES)
    await asyncio.gather(*tasks)
    return cursors


async def amain() -> None:
    db_url = os.environ.get("TRADING_DB_URL")
    if not db_url:
        # Without the DB there's nowhere to land the handoff. Fail loudly:
        # a silent no-op warmup would leave operators thinking buffers were
        # seeded when they weren't.
        raise RuntimeError(
            "TRADING_DB_URL not set — warmup needs the DB to persist "
            "warmup_bars and feed_cursors"
        )

    pool = await asyncpg.create_pool(db_url, min_size=1, max_size=4)
    try:
        cursors = await _warmup_all(pool)
        if cursors:
            from producer.persistence import write_feed_cursors
            await write_feed_cursors(pool, cursors)
            n = sum(len(tfs) for tfs in cursors.values())
            log.info("warmup complete: wrote %d feed cursors across %d symbols",
                     n, len(cursors))
        else:
            log.info("warmup complete: nothing seeded")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(amain())