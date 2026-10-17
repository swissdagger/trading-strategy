"""
research_process.py — research data collection, OFF the live path.

A fifth process. Needs the database and POLYGON_API_KEY; not Kafka, not
Sumtyme, not Coinbase. It shares no lock, queue, connection or thread with
the feed, signal or executor processes, so nothing it does — a slow Polygon
response, a 429, a crash — can delay a bar, a forecast or a signal.

What it collects (migration 025):

  research_quotes     NBBO as of each equity trade_signals row. Driven by the
                      same LISTEN/NOTIFY + cursor mechanism the executor uses
                      (execution/signal_listener.py), with its own cursor
                      (research_cursor); at-least-once, idempotent upsert.
  research_bars_1m    1-minute bars with volume/VWAP/trade count for the
                      research symbols, every RESEARCH_BARS_INTERVAL_S.
  research_earnings   earnings dates: CSV import now, provider later.

    python research_process.py                      # run (listener + bars loop)
    python research_process.py --backfill           # quotes for signals written before it ran, then exit
    python research_process.py --earnings-csv f.csv # import, then exit
    python research_process.py --verify             # one quote + one bars read with the key, then exit

pg_notify fans out to every listener, so the write-path probes (which must be
run with the executor stopped) will also reach this process; their symbols
are unroutable, so each costs one no_quote row. Harmless.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal as os_signal
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

import asyncpg

import config
from execution.signal_listener import SignalListener
from research.bars import BarCollector
from research.earnings import import_csv
from research.persistence import (
    make_latest_bar_ms, make_load_cursor, make_load_signals_without_quotes,
    make_save_cursor, make_upsert_bars, make_upsert_earnings, make_upsert_quote,
)
from research.polygon_research import PolygonResearchClient
from research.quotes import QuoteCapture

log = logging.getLogger("research_process")


def _require(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"missing required env var {name}")
    return v


class ResearchConfig:
    def __init__(self) -> None:
        self.db_url = _require("TRADING_DB_URL")
        self.polygon_key = _require("POLYGON_API_KEY")
        self.bars_interval_s = float(os.environ.get("RESEARCH_BARS_INTERVAL_S", "900"))
        self.symbols = config.research_symbols()


class ResearchProcess:
    def __init__(self, cfg: ResearchConfig):
        self.cfg = cfg
        self.pool: Optional[asyncpg.Pool] = None
        self.client = PolygonResearchClient(cfg.polygon_key)
        self._stop = asyncio.Event()
        self.quotes: Optional[QuoteCapture] = None
        self.bars: Optional[BarCollector] = None
        self.listener: Optional[SignalListener] = None

    async def start(self) -> None:
        self.pool = await asyncpg.create_pool(self.cfg.db_url, min_size=1, max_size=4,
                                              statement_cache_size=0)
        self.quotes = QuoteCapture(self.client, make_upsert_quote(self.pool))
        self.bars = BarCollector(self.client, self.cfg.symbols,
                                 make_upsert_bars(self.pool), make_latest_bar_ms(self.pool))

    async def run(self) -> None:
        assert self.pool is not None and self.quotes is not None and self.bars is not None
        cursor = await make_load_cursor(self.pool)()
        if cursor is None:
            # First run: start from now; --backfill covers everything before.
            cursor = datetime.now(timezone.utc)
            log.info("no research cursor; starting from now (run --backfill for history)")
        self.listener = SignalListener(
            self.cfg.db_url, self.pool, self.quotes.handle,
            initial_cursor=cursor, on_cursor_advance=make_save_cursor(self.pool),
        )
        tasks = [
            asyncio.create_task(self.listener.run(), name="research-listener"),
            asyncio.create_task(self.bars.run_forever(self.cfg.bars_interval_s, self._stop),
                                name="research-bars"),
        ]
        log.info("research process started: %d symbols, bars every %.0fs",
                 len(self.cfg.symbols), self.cfg.bars_interval_s)
        await self._stop.wait()
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        await self.stop()

    async def backfill(self) -> int:
        """Quotes for every equity signal that has none (or errored). Then exit."""
        assert self.pool is not None and self.quotes is not None
        load = make_load_signals_without_quotes(self.pool)
        done = 0
        while True:
            rows = await load(limit=500)
            if not rows:
                break
            for r in rows:
                await self.quotes.handle(r)
                done += 1
            # 'error' rows are selected again; stop if a pass made no progress.
            if self.quotes.stats["ok"] + self.quotes.stats["no_quote"] == 0:
                break
        log.info("backfill: %d signal(s) processed, %s", done, self.quotes.stats)
        return done

    async def verify(self) -> int:
        """One quote and one bars read with the real key; no writes."""
        sym = self.cfg.symbols[0]
        now = datetime.now(timezone.utc)
        rc = 0
        try:
            q = await self.client.nbbo_asof(sym, now - timedelta(hours=18))
            print(f"quotes: {sym} as of 18h ago -> "
                  + (f"bid={q.bid} ask={q.ask} spread={q.spread_bps:.1f}bps at {q.quote_ts}"
                     if q else "no quote in lookback (closed session?)"))
        except PermissionError as e:
            print(f"quotes: FAIL {e}  (the key has no historical-quotes entitlement)")
            rc = 1
        now_ms = int(now.timestamp() * 1000)
        bars = await self.client.bars_1m(sym, now_ms - 3 * 3_600_000, now_ms)
        if bars:
            b = bars[-1]
            print(f"bars: {sym} {len(bars)} rows in last 3h; last @{b.timestamp_ms}: "
                  f"close={b.close} vol={b.volume} vwap={b.vwap} n={b.trade_count}")
            if b.volume is None or b.vwap is None or b.trade_count is None:
                print("bars: FAIL volume/vwap/trade_count missing from the response")
                rc = 1
        else:
            print(f"bars: {sym} 0 rows in last 3h (closed session?)")
        print("OK" if rc == 0 else "FAILED")
        return rc

    async def stop(self) -> None:
        self._stop.set()
        await self.client.aclose()
        if self.pool is not None:
            await self.pool.close()
        log.info("research process stopped")


async def amain(argv: list[str]) -> int:
    logging.basicConfig(
        level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    cfg = ResearchConfig()
    proc = ResearchProcess(cfg)

    if "--verify" in argv:
        try:
            return await proc.verify()
        finally:
            await proc.client.aclose()

    await proc.start()
    try:
        if "--earnings-csv" in argv:
            path = argv[argv.index("--earnings-csv") + 1]
            await import_csv(path, make_upsert_earnings(proc.pool))
            return 0
        if "--backfill" in argv:
            await proc.backfill()
            return 0
        loop = asyncio.get_running_loop()
        for sig in (os_signal.SIGTERM, os_signal.SIGINT):
            loop.add_signal_handler(sig, proc._stop.set)
        await proc.run()
        return 0
    finally:
        if not proc._stop.is_set():
            await proc.stop()


if __name__ == "__main__":
    sys.exit(asyncio.run(amain(sys.argv[1:])))
