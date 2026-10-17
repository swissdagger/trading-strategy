"""
PR 9: the dense equity ladder's feed rules and the ws→REST fallback.

  1. 2m/3m history is built from 1-minute bars and bucketed with the same
     epoch-aligned floor the live BarAggregator uses (DERIVE_FROM_MINUTE).
  2. Sub-minute REST live polls /v3/trades forward, emits only settled closed
     windows, never advances the cursor past an unclosed window, and never
     touches /v2/aggs for a seconds-rung.
  3. A streaming feed that cannot connect on a cold start raises
     StreamingUnavailable; feed_process swaps in the REST sibling.
  4. The record-only strategy owns its ladder and emits nothing.
"""
from __future__ import annotations

import asyncio
import json
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from feeds.polygon_stocks import PolygonStocksFeed
from feeds.streaming_base import StreamingFeedBase, StreamingUnavailable
from producer.bar import OHLCPoint

MIN = 60_000
KEY = "test-key"


# ---------------------------------------------------------------------------
# A fake Polygon that serves /v2/aggs (1-minute only) and /v3/trades
# ---------------------------------------------------------------------------

class FakePolygon:
    def __init__(self, *, minute_bars=None, trades=None):
        self.minute_bars = minute_bars or []     # [{t,o,h,l,c}] ascending
        self.trades = trades or []               # [(t_ms, price)] ascending
        self.requests: list[str] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(str(request.url))
        path = request.url.path
        q = parse_qs(request.url.query.decode())
        if "/v2/aggs/" in path:
            # /v2/aggs/ticker/{sym}/range/{mult}/{span}/{from}/{to}
            parts = path.split("/")
            mult, span = int(parts[6]), parts[7]
            lo, hi = int(parts[8]), int(parts[9])
            assert (mult, span) == (1, "minute"), f"unexpected granularity {mult}/{span}"
            rows = [b for b in self.minute_bars if lo <= b["t"] <= hi]
            if q.get("sort", ["asc"])[0] == "desc":
                rows = list(reversed(rows))
            rows = rows[: int(q.get("limit", ["5000"])[0])]
            return httpx.Response(200, json={"results": [
                {"t": b["t"], "o": b["o"], "h": b["h"], "l": b["l"], "c": b["c"], "v": 1}
                for b in rows]})
        if "/v3/trades/" in path:
            gte = int(q["timestamp.gte"][0]) // 1_000_000
            lt = int(q["timestamp.lt"][0]) // 1_000_000
            rows = [(t, p) for (t, p) in self.trades if gte <= t < lt]
            rows.sort(reverse=True)   # order=desc
            return httpx.Response(200, json={"results": [
                {"sip_timestamp": t * 1_000_000, "price": p, "size": 1} for (t, p) in rows]})
        return httpx.Response(404, json={})


def feed_with(fake: FakePolygon, timeframes) -> PolygonStocksFeed:
    f = PolygonStocksFeed(KEY, ["AAPL"], list(timeframes))
    f._client = httpx.AsyncClient(transport=httpx.MockTransport(fake.handler))
    return f


def minute_series(start_ms: int, n: int):
    return [{"t": start_ms + i * MIN, "o": 10.0 + i, "h": 10.5 + i, "l": 9.5 + i, "c": 10.2 + i}
            for i in range(n)]


# ---------------------------------------------------------------------------
# 1. 2m/3m history derived from 1-minute, epoch-aligned
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_3m_history_is_derived_from_1m_and_clock_aligned(monkeypatch):
    # 1-minute bars starting ONE minute after a 3m boundary, so a naive
    # "every 3 bars from the first" grouping would be misaligned.
    base = 1_700_000_000_000 // (3 * MIN) * (3 * MIN) + MIN
    bars = minute_series(base, 31)                 # ends mid-window
    fake = FakePolygon(minute_bars=bars)
    f = feed_with(fake, ["1m", "3m"])
    # "now" = just after the last 1m bar, so the trailing partial 3m window is forming
    now_ms = bars[-1]["t"] + MIN
    import feeds.polygon_aggs as pa
    class _Now:
        @staticmethod
        def now(tz=None):
            from datetime import datetime, timezone
            return datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
    monkeypatch.setattr(pa, "datetime", _Now)

    out = await f.fetch_history("AAPL", "3m", 5)

    assert all(isinstance(p, OHLCPoint) and p.timeframe == "3m" for p in out)
    # Every window starts on a 3m boundary, consecutive, and the forming one is dropped.
    ts = [p.timestamp for p in out]
    assert all(t % (3 * MIN) == 0 for t in ts)
    assert ts == list(range(ts[0], ts[0] + 5 * 3 * MIN, 3 * MIN))
    assert ts[-1] + 3 * MIN <= now_ms
    # OHLC is the bucket's: open of its first minute, high/low across, close of its last.
    last = out[-1]
    members = [b for b in bars if last.timestamp <= b["t"] < last.timestamp + 3 * MIN]
    assert len(members) == 3
    assert last.open == members[0]["o"] and last.close == members[-1]["c"]
    assert last.high == max(m["h"] for m in members)
    assert last.low == min(m["l"] for m in members)
    # Only 1-minute aggregates were requested (the fake asserts this too).
    assert all("/range/1/minute/" in u for u in fake.requests)


# ---------------------------------------------------------------------------
# 2. Sub-minute REST live: trades forward, settled windows only
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_subminute_live_poll_uses_trades_and_holds_unsettled_window():
    w = 30_000
    t0 = 1_700_000_000_000 // w * w
    # Three windows of trades: [t0, t0+30s), [t0+30s, t0+60s), [t0+60s, ...)
    trades = [(t0 + 1_000, 100.0), (t0 + 15_000, 101.0), (t0 + 29_000, 99.5),
              (t0 + 31_000, 102.0), (t0 + 45_000, 103.0),
              (t0 + 61_000, 104.0)]
    fake = FakePolygon(trades=trades)
    f = feed_with(fake, ["30s"])

    # now = 1s after the 2nd window closed: 1st is settled (>2s), 2nd is not.
    now_ms = t0 + 60_000 + 1_000
    pts, cursor = await f._poll_trades_once("AAPL", "30s", t0, now_ms)
    assert [p.timestamp for p in pts] == [t0]
    assert (pts[0].open, pts[0].high, pts[0].low, pts[0].close) == (100.0, 101.0, 99.5, 99.5)
    assert cursor == t0 + w                       # start of the first UNCLOSED window
    assert all("/v3/trades/" in u for u in fake.requests)
    assert not any("/v2/aggs/" in u for u in fake.requests)

    # Next cycle, 3 s later: the 2nd window has settled; the 3rd is forming.
    pts, cursor = await f._poll_trades_once("AAPL", "30s", cursor, now_ms + 3_000)
    assert [p.timestamp for p in pts] == [t0 + w]
    assert pts[0].close == 103.0
    assert cursor == t0 + 2 * w


@pytest.mark.asyncio
async def test_subminute_rest_live_loop_emits_via_callback(monkeypatch):
    w = 30_000
    t0 = 1_700_000_000_000 // w * w
    fake = FakePolygon(trades=[(t0 + 5_000, 50.0), (t0 + 25_000, 51.0)])
    f = feed_with(fake, ["30s"])
    import feeds.polygon_aggs as pa

    class _Now:
        @staticmethod
        def now(tz=None):
            from datetime import datetime, timezone
            return datetime.fromtimestamp((t0 + 40_000) / 1000, tz=timezone.utc)

    got = []

    async def cb(points):
        got.extend(points)
        f._running = False      # one cycle is enough

    # All patches scoped to this test (pa.random IS the stdlib module).
    monkeypatch.setattr(pa, "datetime", _Now)
    monkeypatch.setattr(pa, "is_market_open", lambda ac, now: True)
    monkeypatch.setattr(pa.random, "uniform", lambda a, b: 0.0)
    try:
        await asyncio.wait_for(
            # The seeded cursor is the newest bar ALREADY seen: live starts one
            # window after it (it used to re-emit the seeded bar).
            f.start_live(["AAPL"], ["30s"], {"AAPL": {"30s": t0 - w}}, cb), timeout=5)
    except asyncio.CancelledError:
        pass
    assert [p.timestamp for p in got] == [t0]
    assert got[0].timeframe == "30s" and got[0].asset_class == "equity"


# ---------------------------------------------------------------------------
# 3. ws → REST fallback
# ---------------------------------------------------------------------------

class _NeverConnects(StreamingFeedBase):
    asset_class = "equity"
    name = "never_ws"
    RECONNECT_BASE_S = 0.0
    RECONNECT_MAX_S = 0.0
    FIRST_CONNECT_MAX_ATTEMPTS = 2

    def __init__(self, rest):
        super().__init__(backfill_feed=rest)
        self.attempts = 0

    @property
    def rest_feed(self):
        return self._backfill_feed

    def supported_timeframes(self):
        return {"30s", "1m"}

    def to_native_symbol(self, symbol):
        return symbol

    async def _connect(self):
        self.attempts += 1
        raise ConnectionError("auth failed: not entitled")

    async def _subscribe(self, conn, symbols): ...
    def _parse_message(self, raw): return []
    async def _read(self, conn): return []
    async def _close(self, conn): ...


class _RestStub:
    name = "rest_stub"
    asset_class = "equity"

    def __init__(self):
        self.started = asyncio.Event()
        self.args = None

    async def fetch_history(self, *a, **k):
        return []

    async def start_live(self, symbols, timeframes, cursors, callback):
        self.args = (symbols, timeframes, cursors)
        self.started.set()
        await asyncio.Event().wait()      # run "forever"


@pytest.mark.asyncio
async def test_streaming_raises_unavailable_after_first_connect_budget():
    rest = _RestStub()
    ws = _NeverConnects(rest)
    with pytest.raises(StreamingUnavailable):
        await ws.start_live(["AAPL"], ["30s", "1m"], {"AAPL": {}}, lambda b: None)
    assert ws.attempts == 2


@pytest.mark.asyncio
async def test_feed_process_falls_back_to_rest_sibling():
    from feed_process import FeedConfig, FeedProcess
    rest = _RestStub()
    ws = _NeverConnects(rest)
    proc = FeedProcess(FeedConfig())
    cursors = {"AAPL": {"30s": 123}}

    async def cb(points): ...

    task = asyncio.create_task(proc._run_feed(ws, ["AAPL"], ["30s", "1m"], cursors, cb))
    await asyncio.wait_for(rest.started.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # The REST sibling got the SAME cursors (the ws feed never emitted).
    assert rest.args == (["AAPL"], ["30s", "1m"], cursors)


# ---------------------------------------------------------------------------
# 4. chain_recorder owns its ladder and never emits
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_chain_recorder_owns_ladder_and_emits_nothing():
    from strategy.chain_recorder import make_chain_recorder
    from strategy.ladder_registry import LadderRegistry
    from strategy.registry import StrategyRegistry
    import config

    reg = LadderRegistry()
    reg.register("DL", config.EXTRA_LADDERS["DL"])
    writes = []
    sr = StrategyRegistry()
    sr.register_factory("chain_recorder", lambda *, name, symbols, ladders, params:
                        make_chain_recorder(name=name, symbols=symbols, ladders=ladders,
                                            params=params, view_registry=reg,
                                            writer_factory=lambda n: writes))
    sr.build_from_config(
        {"chain_recorder": {"enabled": True, "symbols": ["AAPL"], "ladders": ["DL"], "params": {}}},
        all_symbols=["AAPL"])
    assert sr.owned_symbol_ladders() == {("AAPL", "DL")}

    strat = sr.active()[0]

    class Ev:
        symbol = "AAPL"; asset_class = "equity"; chain_id = "c1"
        direction = 1; timestamp = "2026-10-17T14:00:00+00:00"; decision_price = 1.0

    class Init(Ev): event_type = "chain_initiated"; propagation_level = 1
    class Prop(Ev): event_type = "chain_propagated"; propagation_level = 2

    assert await strat.entry.on_event(Init()) is None
    assert await strat.entry.on_event(Prop()) is None
    assert await strat.exit.maybe_emit_exit(symbol="AAPL", asset_class="equity",
                                            chain_prime_id="c1", trade_direction=1,
                                            decided_at="x") is None
    assert writes == []
    snap = strat.entry.snapshot()
    assert snap["inits"] == 1 and snap["propagations_by_level"] == {2: 1}


# ---------------------------------------------------------------------------
# 5. verify_polygon_feed: the alignment check (D16) fires on an off-grid rung
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_verifier_flags_misaligned_bars_and_passes_aligned_ones():
    import verify_polygon_feed as v

    t0 = 1_700_000_000_000 // (15 * MIN) * (15 * MIN)
    aligned = [OHLCPoint("AAPL", t0 + i * 15 * MIN, 1, 2, 0.5, 1.5, "15m", "equity")
               for i in range(v.SAMPLE_BARS)]
    fails, warns = v.check_bars("15m", aligned)
    assert fails == [] and warns == []

    off_grid = [OHLCPoint("AAPL", t0 + MIN + i * 15 * MIN, 1, 2, 0.5, 1.5, "15m", "equity")
                for i in range(v.SAMPLE_BARS)]
    fails, _ = v.check_bars("15m", off_grid)
    assert len(fails) == 1 and "DERIVE_FROM_MINUTE" in fails[0]

    # Gaps are warnings; bad OHLC and duplicates are failures.
    gappy = aligned[:50] + aligned[60:]
    fails, warns = v.check_bars("15m", gappy)
    assert fails == [] and any("non-uniform" in w for w in warns)
    bad = aligned[:10] + [OHLCPoint("AAPL", aligned[10].timestamp, 5, 2, 0.5, 1.5, "15m", "equity")]
    fails, _ = v.check_bars("15m", bad)
    assert any("inconsistent OHLC" in f for f in fails)


@pytest.mark.asyncio
async def test_verifier_rest_pass_runs_each_source_rule(monkeypatch):
    import verify_polygon_feed as v
    import feeds.polygon_aggs as pa

    base = 1_700_000_000_000 // (3 * MIN) * (3 * MIN)
    bars = minute_series(base, 3 * v.SAMPLE_BARS + 10)
    w = 30_000
    trades = [(base + i * 5_000, 100.0 + (i % 7)) for i in range(0, 6 * v.SAMPLE_BARS)]
    fake = FakePolygon(minute_bars=bars, trades=trades)
    f = feed_with(fake, ["30s", "1m", "3m"])
    now_ms = bars[-1]["t"] + MIN

    class _Now:
        @staticmethod
        def now(tz=None):
            from datetime import datetime, timezone
            return datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
    monkeypatch.setattr(pa, "datetime", _Now)
    # The trade walk needs a session containing `now`; pin one around the data.
    monkeypatch.setattr(pa, "session_bounds_for", lambda ac, ms: (base - MIN, now_ms + MIN))

    fails, warns = await v.verify_rest(f, "AAPL", ["1m", "3m", "30s"])
    assert fails == [], fails
    # Only 1-minute aggs and trades were requested — never a 3m or 30s aggregate.
    assert all("/range/1/minute/" in u or "/v3/trades/" in u for u in fake.requests)


# ---------------------------------------------------------------------------
# 5. REST live: rungs published in close-time order
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rest_live_publishes_rungs_in_close_time_order(monkeypatch):
    base = 1_700_000_000_000 // (3 * MIN) * (3 * MIN)
    fake = FakePolygon(minute_bars=minute_series(base, 12))
    f = feed_with(fake, ["1m", "3m"])
    import feeds.polygon_aggs as pa
    now_ms = base + 12 * MIN + 6_000          # every window through base+12m has settled

    class _Now:
        @staticmethod
        def now(tz=None):
            from datetime import datetime, timezone
            return datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)

    real_sleep = asyncio.sleep

    async def fast_sleep(_s):
        await real_sleep(0)

    monkeypatch.setattr(pa, "datetime", _Now)
    monkeypatch.setattr(pa, "is_market_open", lambda ac, now: True)
    monkeypatch.setattr(pa.random, "uniform", lambda a, b: 0.0)
    monkeypatch.setattr(pa.asyncio, "sleep", fast_sleep)
    got = []

    async def cb(points):
        got.extend(points)

    seeded = {"1m": base - MIN, "3m": base - 3 * MIN}     # newest bars already seen
    task = asyncio.create_task(f.start_live(["AAPL"], ["1m", "3m"], {"AAPL": seeded}, cb))
    for _ in range(200):
        await real_sleep(0)
    f._running = False
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    tf_ms = {"1m": MIN, "3m": 3 * MIN}
    keys = [(p.timestamp + tf_ms[p.timeframe], tf_ms[p.timeframe]) for p in got]
    assert keys == sorted(keys)                            # close order, 1m first on ties
    assert len(keys) == len(set(keys))
    expected = {(t + tf_ms[tf], tf_ms[tf]) for tf in ("1m", "3m")
                for t in range(base, base + 12 * MIN, tf_ms[tf])}
    assert set(keys) == expected
    # 3m is derived from 1-minute aggregates live, as in history.
    assert all("/range/1/minute/" in u for u in fake.requests)
