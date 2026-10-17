"""
Crypto seconds rungs (CN ladder) built from Coinbase Exchange trades, in
feeds/coinbase_spot.py: the backward history walk, the forward live poll,
the subminute_symbols restriction, the derived 2m rung, and the warmup
stream filter. One source for warmup and live (step mode: the sequence IS
the step).
"""
from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import parse_qs

import httpx
import pytest

import config
from feeds.coinbase_spot import (TRADE_SETTLE_MS, TRADES_PAGE, CoinbaseSpotFeed,
                                 is_seconds_tf)


class FakeExchange:
    """/products/{id}/trades with limit + after=<trade_id> (older), newest first.
    Trades are (trade_id, t_ms, price); ids ascend with time."""
    def __init__(self, trades):
        self.trades = sorted(trades)          # by id
        self.requests = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(str(request.url))
        q = parse_qs(request.url.query.decode())
        limit = int(q.get("limit", ["100"])[0])
        after = int(q["after"][0]) if "after" in q else None
        rows = [r for r in self.trades if after is None or r[0] < after]
        rows = sorted(rows, reverse=True)[:limit]
        return httpx.Response(200, json=[
            {"trade_id": tid, "time": datetime.fromtimestamp(t / 1000, tz=timezone.utc)
                .isoformat().replace("+00:00", "Z"),
             "price": str(p), "size": "0.01", "side": "buy"} for (tid, t, p) in rows])


def make_feed(fake, now_ms, symbols=("BTC-USD", "ETH-USD"), tfs=("30s", "45s", "1m", "2m"),
              sub=("BTC-USD",)):
    return CoinbaseSpotFeed(list(symbols), list(tfs), transport=httpx.MockTransport(fake.handler),
                            now_ms=lambda: now_ms, subminute_symbols=list(sub))


def trades_every(start_ms, end_ms, step_ms, price0=60000.0):
    out, tid = [], 1_000_000
    t = start_ms
    while t < end_ms:
        out.append((tid, t, price0 + (tid % 50)))
        tid += 1
        t += step_ms
    return out


# ---------------------------------------------------------------------------

def test_seconds_rungs_and_derived_2m_are_supported():
    assert is_seconds_tf("30s") and is_seconds_tf("45s") and not is_seconds_tf("1m")
    f = make_feed(FakeExchange([]), 0)
    assert {"30s", "45s", "2m"} <= f.supported_timeframes()


@pytest.mark.asyncio
async def test_history_walk_pages_backward_builds_aligned_bars_and_drops_forming_tail():
    w = 30_000
    now = 1_700_000_000_000 // w * w + 10_000          # 10 s into a window
    trades = trades_every(now - 4 * 3_600_000, now, 2_000)   # a trade every 2 s for 4 h (7200 trades)
    fake = FakeExchange(trades)
    f = make_feed(fake, now)
    bars = await f.fetch_subminute_history("BTC-USD", "30s", 200)
    assert len(bars) == 200
    ts = [b.timestamp for b in bars]
    assert ts == sorted(ts) and all(t % w == 0 for t in ts)
    assert ts == list(range(ts[0], ts[0] + 200 * w, w))      # contiguous
    # The forming window (now is 10 s into it) and anything unsettled are dropped.
    assert ts[-1] + w + TRADE_SETTLE_MS <= now
    # Paged with after= cursors, newest page first; stopped once enough span was covered.
    assert "after=" in fake.requests[1] and "after=" not in fake.requests[0]
    assert len(fake.requests) <= 7200 // TRADES_PAGE + 2
    # OHLC from the trades: open = first trade's price in the window, close = last.
    last = bars[-1]
    members = [p for (tid, t, p) in trades if last.timestamp <= t < last.timestamp + w]
    assert last.open == members[0] and last.close == members[-1]
    assert last.high == max(members) and last.low == min(members)


@pytest.mark.asyncio
async def test_fetch_history_routes_seconds_rungs_to_trades():
    w = 45_000
    now = 1_700_000_000_000 // w * w
    fake = FakeExchange(trades_every(now - 2 * 3_600_000, now, 5_000))
    f = make_feed(fake, now)
    bars = await f.fetch_history("BTC-USD", "45s", 50)
    assert len(bars) == 50 and all(b.timeframe == "45s" and b.timestamp % w == 0 for b in bars)
    assert all("/trades" in u for u in fake.requests)   # never a candles request


@pytest.mark.asyncio
async def test_symbol_outside_subminute_universe_gets_no_seeds_and_no_live_bars():
    now = 1_700_000_000_000
    fake = FakeExchange(trades_every(now - 3_600_000, now, 2_000))
    f = make_feed(fake, now)
    assert await f.fetch_subminute_history("ETH-USD", "30s", 100) == []
    assert fake.requests == []                              # not even a request
    assert config.serves("crypto", "ETH-USD", "30s") is False
    assert config.serves("crypto", "BTC-USD", "30s") is True
    assert config.serves("crypto", "ETH-USD", "1m") is True


@pytest.mark.asyncio
async def test_live_poll_emits_settled_windows_and_holds_the_cursor():
    w = 30_000
    t0 = 1_700_000_000_000 // w * w
    fake = FakeExchange([(1, t0 + 1_000, 100.0), (2, t0 + 15_000, 101.0), (3, t0 + 29_000, 99.5),
                         (4, t0 + 31_000, 102.0), (5, t0 + 45_000, 103.0),
                         (6, t0 + 61_000, 104.0)])
    f = make_feed(fake, t0 + 61_000)
    now = t0 + 60_000 + 1_000                     # 2nd window closed 1 s ago: not settled
    pts, cur = await f._poll_trades_once("BTC-USD", "30s", t0, now)
    assert [p.timestamp for p in pts] == [t0]
    assert (pts[0].open, pts[0].high, pts[0].low, pts[0].close) == (100.0, 101.0, 99.5, 99.5)
    assert cur == t0 + w
    pts, cur = await f._poll_trades_once("BTC-USD", "30s", cur, now + 3_000)
    assert [p.timestamp for p in pts] == [t0 + w] and pts[0].close == 103.0
    assert cur == t0 + 2 * w


@pytest.mark.asyncio
async def test_live_poll_pages_back_to_cover_an_old_cursor():
    w = 30_000
    t0 = 1_700_000_000_000 // w * w
    # 3000 trades over 100 minutes: the newest page (1000) does not reach a
    # cursor 100 minutes back, so the poll must page back.
    trades = trades_every(t0 - 100 * 60_000, t0, 2_000)
    fake = FakeExchange(trades)
    f = make_feed(fake, t0)
    pts, cur = await f._poll_trades_once("BTC-USD", "30s", t0 - 100 * 60_000, t0 + 5_000)
    assert len(fake.requests) >= 3
    assert pts[0].timestamp == t0 - 100 * 60_000 and pts[-1].timestamp == t0 - w
    assert cur == t0


@pytest.mark.asyncio
async def test_start_live_serves_seconds_rungs_only_for_subminute_symbols(monkeypatch):
    w = 30_000
    t0 = 1_700_000_000_000 // w * w
    fake = FakeExchange(trades_every(t0 - 120_000, t0 + 40_000, 2_000))
    f = make_feed(fake, t0 + 40_000)
    got = []

    async def cb(points):
        got.extend(points)
        f._running = False

    import feeds.coinbase_spot as cs
    monkeypatch.setattr(cs.asyncio, "sleep", _fast_sleep)
    import asyncio
    task = asyncio.create_task(f.start_live(["BTC-USD", "ETH-USD"], ["30s"],
                                            {"BTC-USD": {"30s": t0 - 60_000}, "ETH-USD": {"30s": t0 - 60_000}},
                                            cb))
    await asyncio.wait_for(task, timeout=5)
    assert got and all(p.symbol == "BTC-USD" and p.timeframe == "30s" for p in got)
    # The initial cursor is the newest bar ALREADY SEEN (AssetFeed contract),
    # so live resumes one window after it; the forming window is held.
    assert [p.timestamp for p in got] == [t0 - 30_000, t0]


async def _fast_sleep(_s):
    return None
