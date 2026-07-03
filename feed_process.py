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


def _build_rest_feed(asset_class: str, symbols: list, timeframes: list, polygon_api_key: str | None):
    """Instantiate the REST (polling) AssetFeed for one class, or None if the
    class is unknown. This is the ONE place each REST feed is constructed —
    used both for transport='rest' (it IS the live feed) and transport='ws'
    (it becomes the ws feed's backfill engine), so the two paths can't build
    the REST feed differently.

    Lazy imports keep the module loadable without every feed's deps present
    and avoid importing a feed for a class that has no symbols configured.
    """
    if asset_class == "crypto":
        from feeds.binance_futures import BinanceFuturesFeed
        return BinanceFuturesFeed(symbols, timeframes)
    if asset_class == "equity":
        if not polygon_api_key:
            raise RuntimeError("POLYGON_API_KEY not set — required for the equity feed")
        from feeds.polygon_stocks import PolygonStocksFeed
        return PolygonStocksFeed(polygon_api_key, symbols, timeframes)
    if asset_class == "fx":
        if not polygon_api_key:
            raise RuntimeError("POLYGON_API_KEY not set — required for the fx feed")
        from feeds.polygon_forex import PolygonForexFeed
        return PolygonForexFeed(polygon_api_key, symbols, timeframes)
    return None


def _build_ws_feed(asset_class: str, backfill_feed, polygon_api_key: str | None):
    """Instantiate the websocket AssetFeed for one class, composing the given
    REST feed as its reconnect-backfill engine. Returns None if the class has
    no ws feed (config.transport_for already rejects ws for such classes, so
    this is defence in depth, not the primary guard).

    The ws feed and its backfill sibling share the same api_key, symbols, and
    timeframes — the sibling was built by _build_rest_feed for exactly this
    class, so the backfill path fetches the identical history the REST
    transport would.
    """
    if asset_class == "equity":
        from feeds.polygon_ws import PolygonStocksWSFeed
        return PolygonStocksWSFeed(polygon_api_key, backfill_feed)
    if asset_class == "fx":
        from feeds.polygon_ws import PolygonForexWSFeed
        return PolygonForexWSFeed(polygon_api_key, backfill_feed)
    if asset_class == "crypto":
        # Binance ws needs no api_key (public market streams); the backfill
        # sibling (BinanceFuturesFeed) is the outage-fetch engine, though it
        # only covers 1m+ rungs — sub-minute rungs self-heal live. See
        # binance_ws for the backfill-asymmetry note.
        from feeds.binance_ws import BinanceFuturesWSFeed
        return BinanceFuturesWSFeed(backfill_feed)
    return None


def _build_feeds(asset_configs: dict) -> dict:
    """Instantiate one live AssetFeed per asset class present in ASSET_CONFIGS,
    selecting the polling (rest) or websocket (ws) transport per class via
    config.transport_for. Maps asset_class -> feed instance.

    Extend for a new class: add its branch to _build_rest_feed (and, if it
    gains a websocket, _build_ws_feed) — the same one-import-one-entry shape
    as before, now split by transport.

    For a ws-transport class, the REST feed is still built (as the backfill
    engine) and wrapped by the ws feed; the ws feed is what goes live. Both
    satisfy the identical AssetFeed contract, so everything downstream of
    _build_feeds (start_live, the callback, cursor handoff, Kafka publish)
    is transport-agnostic.
    """
    from config import transport_for

    feeds = {}
    polygon_api_key = os.environ.get("POLYGON_API_KEY")
    for asset_class, cfg in asset_configs.items():
        symbols = cfg.get("symbols")
        if not symbols:
            continue   # nothing to stream for this class
        timeframes = cfg["timeframes"]

        rest_feed = _build_rest_feed(asset_class, symbols, timeframes, polygon_api_key)
        if rest_feed is None:
            log.warning("no feed wired for asset class %r — skipping", asset_class)
            continue

        transport = transport_for(asset_class)
        if transport == "ws":
            ws_feed = _build_ws_feed(asset_class, rest_feed, polygon_api_key)
            if ws_feed is None:
                # ws requested for a class with no ws feed (e.g. crypto until
                # binance_ws lands). Fail loudly at startup — a silent fall
                # back to polling would hide a real misconfiguration. Same
                # RuntimeError convention as the missing-API-key check above.
                raise RuntimeError(
                    f"{asset_class}: transport='ws' but no websocket feed exists "
                    f"for this class. Use transport='rest' or add a ws feed."
                )
            feeds[asset_class] = ws_feed
            log.info("class %r using WEBSOCKET transport (rest feed as backfill)",
                     asset_class)
        else:
            feeds[asset_class] = rest_feed
            log.info("class %r using REST (polling) transport", asset_class)
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
