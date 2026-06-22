"""
feed_process.py — Polls market data and publishes closed bars to Kafka.

The PRODUCER of the trading-bars topic the signal process consumes. It reuses the
existing, trusted AssetFeed implementations (BinanceFuturesFeed, PolygonStocksFeed,
PolygonForexFeed, ...) — it does NOT reimplement polling, pagination, or rate
limiting. Each feed's start_live(symbols, timeframes, cursors, callback) runs for
the process lifetime and calls back with batches of closed OHLC bars; this process
wires that callback to "publish each bar to Kafka trading-bars".

The feed has NO knowledge of Kafka (per the AssetFeed contract) — this process is
the only thing that does. One feed per asset class in ASSET_CONFIGS, launched
concurrently; each owns its own RateLimiter (feeds do not share rate budget —
Polygon and Binance have independent quotas).

SEPARATION FROM WARMUP. start_live takes initial_cursors[symbol][timeframe] = the
latest already-seen bar timestamp, so the feed emits only NEWER bars. This process
starts with EMPTY cursors (emit from "now" forward) — historical buffer seeding is
the SEPARATE warmup task's job (it reads history straight into the chain machine's
buffers, not through this live stream). Keeping the two apart means the live feed
isn't replaying weeks of history through Kafka on every restart.

This is a SEPARATE process from the signal process (and runs on the trading box).
Splitting feeds out means they restart independently and other consumers (the
position manager later) can read the same bars.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal as os_signal
from typing import Dict, List

log = logging.getLogger("feed_process")


class FeedConfig:
    def __init__(self) -> None:
        self.kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
        self.bars_topic = os.environ.get("TRADING_BARS_TOPIC", "trading-bars")
        self.db_url = os.environ.get("TRADING_DB_URL")   # for reading feed_cursors


def _build_feeds(asset_configs: dict) -> dict:
    """Instantiate one AssetFeed per asset class present in ASSET_CONFIGS.

    Imports the concrete feeds lazily so the module loads without the feed deps
    present (and so a class with no symbols configured doesn't force its feed's
    import). Maps asset_class -> feed instance. Extend this when adding a class:
    one import + one mapping entry, mirroring the feeds package's design.
    """
    feeds = {}
    polygon_api_key = os.environ.get("POLYGON_API_KEY")
    for asset_class, cfg in asset_configs.items():
        symbols = cfg.get("symbols")
        if not symbols:
            continue   # nothing to poll for this class
        timeframes = cfg["timeframes"]
        if asset_class == "crypto":
            from feeds.binance_futures import BinanceFuturesFeed
            feeds[asset_class] = BinanceFuturesFeed(symbols, timeframes)
        elif asset_class == "equity":
            if not polygon_api_key:
                raise RuntimeError("POLYGON_API_KEY not set — required for the equity feed")
            from feeds.polygon_stocks import PolygonStocksFeed
            feeds[asset_class] = PolygonStocksFeed(polygon_api_key, symbols, timeframes)
        elif asset_class == "fx":
            if not polygon_api_key:
                raise RuntimeError("POLYGON_API_KEY not set — required for the fx feed")
            from feeds.polygon_forex import PolygonForexFeed
            feeds[asset_class] = PolygonForexFeed(polygon_api_key, symbols, timeframes)
        else:
            log.warning("no feed wired for asset class %r — skipping", asset_class)
    return feeds

class FeedProcess:
    """Owns the per-class feeds and the Kafka producer."""

    def __init__(self, cfg: FeedConfig):
        self.cfg = cfg
        self.producer = None
        self._tasks: List[asyncio.Task] = []
        self._stop = asyncio.Event()

    async def start(self) -> None:
        from aiokafka import AIOKafkaProducer
        import json as _json
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.cfg.kafka_bootstrap,
            value_serializer=lambda v: _json.dumps(v).encode(),
            # Bars are high-volume but individually small; a little batching/linger
            # cuts overhead without meaningfully delaying a closed bar.
            linger_ms=20,
        )
        await self.producer.start()

        from config import ASSET_CONFIGS
        feeds = _build_feeds(ASSET_CONFIGS)
        if not feeds:
            raise RuntimeError("no feeds to run — ASSET_CONFIGS has no symbols")

        # Read warmup's high-water cursors so live ingestion resumes exactly where
        # warmup ended. Empty on first-ever boot -> feeds start from "now" (warmup
        # filled the buffers regardless, so the system self-corrects). The feed
        # NEVER blocks on the signal process; it reads whatever is present.
        all_cursors: Dict[str, Dict[str, int]] = {}
        if self.cfg.db_url:
            try:
                import asyncpg
                from producer.persistence import read_feed_cursors
                pool = await asyncpg.create_pool(self.cfg.db_url, min_size=1, max_size=2, statement_cache_size=0)
                all_cursors = await read_feed_cursors(pool)
                await pool.close()
                log.info("loaded feed cursors for %d symbols", len(all_cursors))
            except Exception:
                log.exception("failed to read feed_cursors; starting feeds from 'now'")
        else:
            log.warning("TRADING_DB_URL not set; feeds start from 'now' (no cursor handoff)")

        for asset_class, feed in feeds.items():
            cfg = ASSET_CONFIGS[asset_class]
            symbols = cfg["symbols"]
            timeframes = cfg["timeframes"]
            # Per-symbol cursors from warmup; symbols with no cursor entry start
            # from "now" (the feed treats an empty per-symbol dict that way).
            cursors: Dict[str, Dict[str, int]] = {
                s: all_cursors.get(s, {}) for s in symbols
            }
            callback = self._make_callback(asset_class)
            task = asyncio.create_task(
                self._run_feed(feed, symbols, timeframes, cursors, callback),
                name=f"feed-{asset_class}",
            )
            self._tasks.append(task)
            log.info("launched feed %s: %d symbols x %d timeframes",
                     asset_class, len(symbols), len(timeframes))

    async def _run_feed(self, feed, symbols, timeframes, cursors, callback) -> None:
        """Run one feed's live loop, restarting it if it errors out (a feed's
        start_live is expected never to return normally; if it raises, we back off
        and relaunch rather than lose that asset class for the process lifetime)."""
        while not self._stop.is_set():
            try:
                await feed.start_live(symbols, timeframes, cursors, callback)
                # Normal return is unexpected; treat as a restart trigger.
                log.warning("feed %s start_live returned; relaunching", feed.name)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("feed %s errored; backing off 5s then relaunching",
                              feed.name)
            if self._stop.is_set():
                break
            await asyncio.sleep(5)

    def _make_callback(self, asset_class: str):
        """Build the LiveDataCallback for a feed: publish each closed bar to Kafka
        trading-bars. The feed passes a List[OHLCPoint]; we serialize each to the
        wire shape the signal process's OHLCPoint(**raw) expects."""
        async def callback(points: list) -> None:
            if self.producer is None:
                return
            for p in points:
                try:
                    await self.producer.send(self.cfg.bars_topic, _bar_to_wire(p))
                except Exception:
                    # A publish failure for one bar must not drop the whole batch
                    # or kill the feed. Log and continue; the feed keeps streaming.
                    log.exception("failed to publish bar %s %s",
                                  getattr(p, "symbol", "?"), getattr(p, "timeframe", "?"))
        return callback

    async def run(self) -> None:
        """Wait until stopped; the feeds run in their own tasks."""
        await self._stop.wait()
        await self.stop()

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        if self.producer is not None:
            await self.producer.stop()
        log.info("feed process stopped cleanly")


def _bar_to_wire(p) -> dict:
    """OHLCPoint -> JSON dict matching the signal process's OHLCPoint(**raw).
    Works whether p is the local dataclass or CausaView's pydantic OHLCPoint
    (both expose these attributes)."""
    return {
        "symbol": p.symbol, "timestamp": p.timestamp,
        "open": p.open, "high": p.high, "low": p.low, "close": p.close,
        "timeframe": p.timeframe, "asset_class": p.asset_class,
    }


async def amain() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    proc = FeedProcess(FeedConfig())
    loop = asyncio.get_running_loop()
    for sig in (os_signal.SIGTERM, os_signal.SIGINT):
        loop.add_signal_handler(sig, lambda: proc._stop.set())
    await proc.start()
    await proc.run()


if __name__ == "__main__":
    asyncio.run(amain())
