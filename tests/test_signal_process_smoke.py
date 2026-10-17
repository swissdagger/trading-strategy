"""SignalProcess.start() wires the whole signal pipeline (Kafka, the pool,
Sumtyme, ladders, strategies, router, runtime, rules, bar store, health). No
component test runs that wiring, so a wrong argument in it would surface only
on deploy. This boots it for real against Postgres, with Kafka, Sumtyme and
the price feeds faked: one bar goes through the worker pool into the bar
store, its offset is committed, and stop() closes everything."""
import asyncio
import json
import os
import socket
import sys
import time
import types
import urllib.error
import urllib.request

import pytest

DSN = os.environ.get("QISS_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="QISS_TEST_DSN not set; needs real Postgres")


class FakeMsg:
    def __init__(self, offset, value):
        self.offset = offset
        self.value = value


class FakeConsumer:
    """Yields the queued messages, then ends (run() then calls stop())."""
    instances = []

    def __init__(self, topic, **kw):
        self.topic = topic
        self.kw = kw
        self.messages = []
        self.commits = []
        self.started = self.stopped = False
        FakeConsumer.instances.append(self)

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def commit(self, offsets):
        self.commits.append(dict(offsets))

    def __aiter__(self):
        return self._iter()

    async def _iter(self):
        for m in self.messages:
            yield m
            await asyncio.sleep(0)


class FakeProducer:
    instances = []

    def __init__(self, **kw):
        self.sent = []
        self.started = self.stopped = False
        FakeProducer.instances.append(self)

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def send(self, topic, value):
        self.sent.append((topic, value))


class FakeFeed:
    """Stands in for the Coinbase/Polygon 1m closes feeds the rules' vol
    filters read: no network, no history."""
    def __init__(self, *a, **kw):
        self.closed = False

    async def fetch_history(self, symbol, timeframe, n):
        return []

    async def aclose(self):
        self.closed = True


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.asyncio
async def test_signal_process_boots_processes_a_bar_and_stops_cleanly(monkeypatch):
    import aiokafka
    import feeds.coinbase_spot
    import feeds.polygon_stocks
    import signal_process as sp

    FakeConsumer.instances.clear()
    FakeProducer.instances.clear()
    monkeypatch.setattr(aiokafka, "AIOKafkaConsumer", FakeConsumer)
    monkeypatch.setattr(aiokafka, "AIOKafkaProducer", FakeProducer)
    monkeypatch.setattr(feeds.coinbase_spot, "CoinbaseSpotFeed", FakeFeed)
    monkeypatch.setattr(feeds.polygon_stocks, "PolygonStocksFeed", FakeFeed)
    fake_sumtyme = types.ModuleType("sumtyme")
    fake_sumtyme.client = lambda apikey, timeout=None: types.SimpleNamespace(
        forecast_path=lambda **kw: pytest.fail("no forecast expected: buffers are not full"))
    monkeypatch.setitem(sys.modules, "sumtyme", fake_sumtyme)

    port = _free_port()
    monkeypatch.setenv("TRADING_DB_URL", DSN)
    monkeypatch.setenv("SUMTYME_API_KEY", "test")
    monkeypatch.setenv("POLYGON_API_KEY", "test")
    monkeypatch.setenv("HEALTH_PORT", str(port))

    # A 1m BTC-USD bar newer than anything the test database holds, so the
    # machines' monotonic guard accepts it after hydrating.
    ts = (int(time.time()) // 60 + 600) * 60_000
    bar = {"symbol": "BTC-USD", "timestamp": ts, "open": 1.0, "high": 2.0, "low": 0.5,
           "close": 1.5, "timeframe": "1m", "asset_class": "crypto"}

    proc = sp.SignalProcess(sp.Config())
    try:
        await proc.start()
        (consumer,), (producer,) = FakeConsumer.instances, FakeProducer.instances
        assert consumer.started and producer.started
        assert consumer.kw["enable_auto_commit"] is False
        assert {s.name for s in proc.strategies.active()} >= {"rule1_v1", "rule3_v1"}

        def _get_health():
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=5) as r:
                    return json.load(r)
            except urllib.error.HTTPError as e:   # 503 until a watermark exists
                return json.load(e)
        assert (await asyncio.to_thread(_get_health))["booted"] is True

        consumer.messages = [FakeMsg(0, {"type": "status"}), FakeMsg(1, bar)]
        await asyncio.wait_for(proc.run(), timeout=30)   # ends with stop()

        assert consumer.stopped and producer.stopped
        assert consumer.commits and list(consumer.commits[-1].values()) == [2]
        assert all(f.closed for f in proc._closes_feeds.values())

        import asyncpg
        conn = await asyncpg.connect(DSN)
        try:
            row = await conn.fetchrow(
                "SELECT close FROM warmup_bars WHERE symbol = 'BTC-USD' "
                "AND timeframe = '1m' AND timestamp_ms = $1", ts)
        finally:
            await conn.close()
        assert row is not None and float(row["close"]) == 1.5
    finally:
        if not proc._stop.is_set():
            await proc.stop()
        import asyncpg
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute("DELETE FROM warmup_bars WHERE symbol = 'BTC-USD' "
                               "AND timestamp_ms = $1", ts)
        finally:
            await conn.close()
