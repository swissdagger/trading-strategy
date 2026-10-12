"""
Tests for feeds/coinbase_spot.py against a fake Coinbase candles endpoint
(httpx MockTransport). The fake mirrors the documented public endpoint:

  GET /api/v3/brokerage/market/products/{product_id}/candles?start&end&granularity
  -> {"candles": [{"start": "<unix s>", "low", "high", "open", "close", "volume"}]}

newest first, string prices, the still-forming candle included, intervals with
no trades omitted, and a hard cap of 350 candles per request (a 400 beyond it).

These check the feed's logic (pagination, completeness, derived rungs, the live
emission rule). They are NOT a check of the live API's actual responses.
"""
import asyncio

import httpx
import pytest

from feeds.coinbase_spot import CoinbaseSpotFeed, SETTLE_MS

GRAN_S = {"ONE_MINUTE": 60, "FIVE_MINUTE": 300, "FIFTEEN_MINUTE": 900,
          "THIRTY_MINUTE": 1800, "ONE_HOUR": 3600, "TWO_HOUR": 7200,
          "SIX_HOUR": 21600, "ONE_DAY": 86400}

T0 = 1_760_000_040          # a 3-minute-aligned unix second (1760000040 % 180 == 0)


def ohlc(t):
    """Deterministic candle values keyed by start second."""
    base = 100 + (t // 60) % 50
    return {"open": base, "high": base + 2, "low": base - 1, "close": base + 1}


class FakeCoinbase:
    def __init__(self, *, now_s, listed_from_s=0, missing=(), products=("BTC-USD",),
                 fail_first=0):
        self.now_s = now_s
        self.listed_from_s = listed_from_s
        self.missing = set(missing)          # candle starts with no trades
        self.products = set(products)
        self.fail_first = fail_first         # leading 429s
        self.requests = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        parts = request.url.path.split("/")
        product = parts[-2]
        q = request.url.params
        gran = GRAN_S[q["granularity"]]
        start, end = int(q["start"]), int(q["end"])
        self.requests.append((product, q["granularity"], start, end))
        if self.fail_first > 0:
            self.fail_first -= 1
            return httpx.Response(429, json={"error": "rate limited"})
        if product not in self.products:
            return httpx.Response(404, json={"error": "NOT_FOUND"})
        if (end - start) // gran + 1 > 350:
            return httpx.Response(400, json={"error": "too many candles"})
        first = -(-max(start, self.listed_from_s) // gran) * gran
        last_start = min(end, self.now_s) // gran * gran   # includes the forming candle
        candles = []
        t = first
        while t <= last_start:
            if t not in self.missing:
                v = ohlc(t)
                candles.append({"start": str(t), **{k: str(x) for k, x in v.items()},
                                "volume": "1.0"})
            t += gran
        candles.reverse()                                   # newest first
        return httpx.Response(200, json={"candles": candles})


def make_feed(fake, timeframes=("1m", "3m", "5m", "15m", "1h"), symbols=("BTC-USD",)):
    return CoinbaseSpotFeed(list(symbols), list(timeframes),
                            transport=httpx.MockTransport(fake.handler),
                            now_ms=lambda: fake.now_s * 1000,
                            requests_per_second=10_000)


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch):
    # Retry backoff only; the rate limiter's spacing is negligible at 10k req/s.
    real_sleep = asyncio.sleep
    async def fast_sleep(s, *a, **k):
        await real_sleep(0)
    monkeypatch.setattr("feeds.coinbase_spot.asyncio.sleep", fast_sleep)


# --------------------------------------------------------------------
# Construction
# --------------------------------------------------------------------

def test_rejects_binance_form_symbols():
    with pytest.raises(ValueError, match="product ids"):
        CoinbaseSpotFeed(["BTCUSDT"], ["1m"])


def test_rejects_unsupported_timeframe():
    with pytest.raises(ValueError, match="10m"):
        CoinbaseSpotFeed(["BTC-USD"], ["10m"])


def test_native_symbol_is_the_product_id():
    assert CoinbaseSpotFeed(["BTC-USD"], ["1m"]).to_native_symbol("BTC-USD") == "BTC-USD"


# --------------------------------------------------------------------
# History
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_history_pages_back_to_points_needed_closed_only():
    now = T0 + 25                                      # mid-minute: a forming candle exists
    fake = FakeCoinbase(now_s=now)
    pts = await make_feed(fake).fetch_history("BTC-USD", "1m", 1000)
    assert len(pts) == 1000
    ts = [p.timestamp for p in pts]
    assert ts == sorted(ts) and len(set(ts)) == 1000
    assert all(b - a == 60_000 for a, b in zip(ts, ts[1:]))
    assert ts[-1] == (T0 - 60) * 1000                 # last CLOSED minute; forming one dropped
    assert all((e - s) // 60 + 1 <= 350 for _, _, s, e in fake.requests)
    assert all(g == "ONE_MINUTE" for _, g, _, _ in fake.requests)
    p = pts[-1]
    v = ohlc(T0 - 60)
    assert (p.open, p.high, p.low, p.close) == (v["open"], v["high"], v["low"], v["close"])
    assert p.symbol == "BTC-USD" and p.timeframe == "1m" and p.asset_class == "crypto"


@pytest.mark.asyncio
async def test_history_stops_at_product_listing():
    fake = FakeCoinbase(now_s=T0, listed_from_s=T0 - 500 * 60)
    pts = await make_feed(fake).fetch_history("BTC-USD", "1m", 5000)
    assert len(pts) == 500
    assert pts[0].timestamp == (T0 - 500 * 60) * 1000


@pytest.mark.asyncio
async def test_history_keeps_no_trade_gaps():
    gaps = {T0 - 10 * 60, T0 - 11 * 60}
    fake = FakeCoinbase(now_s=T0, missing=gaps)
    pts = await make_feed(fake).fetch_history("BTC-USD", "1m", 30)
    ts = {p.timestamp // 1000 for p in pts}
    assert not (ts & gaps)                             # no fabricated flat bars
    assert len(pts) == 30


@pytest.mark.asyncio
async def test_derived_3m_history_aggregates_1m():
    now = T0 + 100                                     # 3m window [T0, T0+180) still forming
    fake = FakeCoinbase(now_s=now)
    pts = await make_feed(fake).fetch_history("BTC-USD", "3m", 50)
    assert len(pts) == 50
    assert all(p.timestamp % 180_000 == 0 for p in pts)
    assert pts[-1].timestamp == (T0 - 180) * 1000     # forming 3m window excluded
    w = pts[-1].timestamp // 1000
    mins = [ohlc(w), ohlc(w + 60), ohlc(w + 120)]
    p = pts[-1]
    assert p.open == mins[0]["open"] and p.close == mins[2]["close"]
    assert p.high == max(m["high"] for m in mins) and p.low == min(m["low"] for m in mins)
    assert p.timeframe == "3m"
    assert {g for _, g, _, _ in fake.requests} == {"ONE_MINUTE"}


@pytest.mark.asyncio
@pytest.mark.parametrize("tf,gran", [("4h", "ONE_HOUR"), ("12h", "SIX_HOUR"),
                                     ("15m", "FIFTEEN_MINUTE"), ("1h", "ONE_HOUR")])
async def test_each_timeframe_uses_the_right_granularity(tf, gran):
    fake = FakeCoinbase(now_s=T0 + 86_400 * 30)
    pts = await make_feed(fake, timeframes=(tf,)).fetch_history("BTC-USD", tf, 5)
    assert len(pts) == 5
    assert {g for _, g, _, _ in fake.requests} == {gran}


@pytest.mark.asyncio
async def test_rate_limit_is_retried():
    fake = FakeCoinbase(now_s=T0, fail_first=2)
    pts = await make_feed(fake).fetch_history("BTC-USD", "1m", 10)
    assert len(pts) == 10


@pytest.mark.asyncio
async def test_unknown_product_returns_empty():
    fake = FakeCoinbase(now_s=T0)
    pts = await make_feed(fake, symbols=("FOO-USD",)).fetch_history("FOO-USD", "1m", 10)
    assert pts == []


# --------------------------------------------------------------------
# Live emission rule
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_live_native_waits_for_settle_then_emits_once():
    fake = FakeCoinbase(now_s=T0 + 2)                  # minute T0-60 ended 2s ago
    feed = make_feed(fake)
    cursors = {"BTC-USD": (T0 - 60) * 1000}
    assert await feed.poll_once(["BTC-USD"], "1m", cursors) == []
    fake.now_s = T0 + SETTLE_MS // 1000
    batch = await feed.poll_once(["BTC-USD"], "1m", cursors)
    assert [p.timestamp for p in batch] == [(T0 - 60) * 1000]
    assert cursors["BTC-USD"] == T0 * 1000
    assert await feed.poll_once(["BTC-USD"], "1m", cursors) == []   # no re-emit


@pytest.mark.asyncio
async def test_live_catches_up_missed_windows_in_order():
    fake = FakeCoinbase(now_s=T0 + 10)
    feed = make_feed(fake)
    cursors = {"BTC-USD": (T0 - 240) * 1000}
    batch = await feed.poll_once(["BTC-USD"], "1m", cursors)
    assert [p.timestamp // 1000 for p in batch] == [T0 - 240, T0 - 180, T0 - 120, T0 - 60]


@pytest.mark.asyncio
async def test_live_derived_waits_for_its_last_base_candle():
    # 3m window [T0-180, T0) has ended, but its last minute (T0-60) has not
    # been published yet: emitting now would lose that minute for good.
    fake = FakeCoinbase(now_s=T0 + 10, missing={T0 - 60})
    feed = make_feed(fake)
    cursors = {"BTC-USD": (T0 - 180) * 1000}
    assert await feed.poll_once(["BTC-USD"], "3m", cursors) == []
    fake.missing.clear()                               # the minute arrives
    batch = await feed.poll_once(["BTC-USD"], "3m", cursors)
    assert len(batch) == 1 and batch[0].close == ohlc(T0 - 60)["close"]


@pytest.mark.asyncio
async def test_live_derived_emits_after_grace_when_last_minute_had_no_trades():
    fake = FakeCoinbase(now_s=T0 + 10, missing={T0 - 60})
    feed = make_feed(fake)
    cursors = {"BTC-USD": (T0 - 180) * 1000}
    assert await feed.poll_once(["BTC-USD"], "3m", cursors) == []
    fake.now_s = T0 + 60 + SETTLE_MS // 1000           # one base interval of grace
    batch = await feed.poll_once(["BTC-USD"], "3m", cursors)
    assert [p.timestamp // 1000 for p in batch] == [T0 - 180]
    assert batch[0].close == ohlc(T0 - 120)["close"]   # last minute that traded


@pytest.mark.asyncio
async def test_live_polls_each_symbol():
    fake = FakeCoinbase(now_s=T0 + 10, products=("BTC-USD", "ETH-USD"))
    feed = make_feed(fake, symbols=("BTC-USD", "ETH-USD"))
    cursors = {"BTC-USD": (T0 - 60) * 1000, "ETH-USD": (T0 - 60) * 1000}
    batch = await feed.poll_once(["BTC-USD", "ETH-USD"], "1m", cursors)
    assert sorted(p.symbol for p in batch) == ["BTC-USD", "ETH-USD"]


# --------------------------------------------------------------------
# Live catch-up from a stale cursor (restart / slow start)
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_live_catches_up_from_a_cursor_older_than_the_lookback():
    # Cursor 40 minutes behind: a fixed lookback would have skipped ~34 bars.
    fake = FakeCoinbase(now_s=T0 + 10)
    feed = make_feed(fake)
    cursors = {"BTC-USD": (T0 - 40 * 60) * 1000}
    batch = await feed.poll_once(["BTC-USD"], "1m", cursors)
    ts = [p.timestamp // 1000 for p in batch]
    assert ts == list(range(T0 - 40 * 60, T0, 60))
    assert cursors["BTC-USD"] == T0 * 1000


@pytest.mark.asyncio
async def test_live_catch_up_pages_forward_within_the_request_cap():
    fake = FakeCoinbase(now_s=T0 + 10)
    feed = make_feed(fake)
    cursors = {"BTC-USD": (T0 - 700 * 60) * 1000}       # 700 minutes: three pages
    batch = await feed.poll_once(["BTC-USD"], "1m", cursors)
    assert len(batch) == 700
    assert len(fake.requests) == 3
    assert all((e - s) // 60 + 1 <= 350 for _, _, s, e in fake.requests)


@pytest.mark.asyncio
async def test_live_derived_catch_up_from_stale_cursor():
    fake = FakeCoinbase(now_s=T0 + 10)
    feed = make_feed(fake)
    cursors = {"BTC-USD": (T0 - 30 * 60) * 1000}
    batch = await feed.poll_once(["BTC-USD"], "3m", cursors)
    assert [p.timestamp // 1000 for p in batch] == list(range(T0 - 30 * 60, T0, 180))


@pytest.mark.asyncio
async def test_live_catch_up_is_clamped_for_very_old_cursors():
    from feeds.coinbase_spot import MAX_CATCHUP_PAGES, _PAGE_CANDLES
    fake = FakeCoinbase(now_s=T0 + 10)
    feed = make_feed(fake)
    cursors = {"BTC-USD": (T0 - 30 * 86_400) * 1000}    # a month behind
    batch = await feed.poll_once(["BTC-USD"], "1m", cursors)
    assert len(fake.requests) <= MAX_CATCHUP_PAGES + 1
    assert len(batch) <= MAX_CATCHUP_PAGES * _PAGE_CANDLES
    assert batch and batch[-1].timestamp == (T0 - 60) * 1000


# --------------------------------------------------------------------
# Cursor handoff from warmup
# --------------------------------------------------------------------

def test_initial_cursor_starts_one_window_past_the_persisted_bar():
    # feed_cursors holds warmup's NEWEST bar; re-emitting it would put a
    # duplicate timestamp into the chain machine's buffer.
    fake = FakeCoinbase(now_s=T0)
    feed = make_feed(fake)
    assert feed.initial_cursor({"BTC-USD": {"1m": 1_000_000}}, "BTC-USD", "1m") == 1_060_000
    assert feed.initial_cursor({"BTC-USD": {"3m": 1_080_000}}, "BTC-USD", "3m") == 1_260_000


def test_cold_stream_starts_at_the_last_closed_window():
    fake = FakeCoinbase(now_s=T0 + 30)
    feed = make_feed(fake)
    assert feed.initial_cursor({}, "BTC-USD", "1m") == (T0 - 60) * 1000


@pytest.mark.asyncio
async def test_start_live_does_not_reemit_warmups_newest_bar():
    fake = FakeCoinbase(now_s=T0 + 10)
    feed = make_feed(fake, timeframes=("1m",))
    warm_newest = (T0 - 180) * 1000
    got = []
    async def callback(batch):
        got.extend(batch)
        feed._running = False
        raise asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await feed.start_live(["BTC-USD"], ["1m"], {"BTC-USD": {"1m": warm_newest}}, callback)
    assert [p.timestamp // 1000 for p in got] == [T0 - 120, T0 - 60]
