"""strategy/vol_source.py: a failed refresh is not cached or computed on, and
clock-driven intents are priced from the bar stream, not a stale vol cache."""
from datetime import datetime, timedelta, timezone

import pytest

from producer.bar import OHLCPoint
from strategy.vol_source import FeedClosesSource, LiveCloses, VolStateProvider

UTC = timezone.utc
T0 = datetime(2026, 10, 16, 12, 5, tzinfo=UTC)


class _Bar:
    def __init__(self, ts_ms, close):
        self.timestamp, self.close = ts_ms, close


class FlakyFeed:
    def __init__(self, fail_times):
        self.fail_times, self.calls = fail_times, 0

    async def fetch_history(self, symbol, tf, n):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("venue down")
        end = int(T0.timestamp()) // 60
        return [_Bar(m * 60_000, 100.0 + (m % 7)) for m in range(end - n, end)]


@pytest.mark.asyncio
async def test_a_failed_refresh_is_retried_on_the_next_event_not_cached_for_the_hour():
    feed = FlakyFeed(fail_times=1)
    prov = VolStateProvider(FeedClosesSource(feed))
    assert await prov.prefetch("BTC-USD", T0) is None          # fetch failed: skip
    assert prov.get("BTC-USD", T0) is None
    state = await prov.prefetch("BTC-USD", T0 + timedelta(minutes=1))   # same hour: retried
    assert feed.calls == 2
    assert state is not None and state.ratio is not None


@pytest.mark.asyncio
async def test_a_failed_refill_does_not_compute_on_a_window_missing_its_newest_hour():
    feed = FlakyFeed(fail_times=0)
    src = FeedClosesSource(feed)
    assert await src.closes("BTC-USD", T0) is not None        # seeded
    feed.fail_times, feed.calls = 1, 0                         # the next hour's refill fails
    assert await src.closes("BTC-USD", T0 + timedelta(hours=1)) is None


def _pt(ts_ms, tf, close):
    return OHLCPoint(symbol="BTC-USD", timestamp=ts_ms, open=close, high=close, low=close,
                     close=close, timeframe=tf, asset_class="crypto")


def test_live_closes_keeps_the_latest_close_time_across_rungs():
    lc = LiveCloses(fallback=lambda s: 1.0)
    assert lc.latest_close("BTC-USD") == 1.0                   # nothing seen yet: fallback
    lc.observe(_pt(0, "1m", 10.0))                             # closes 60 s
    lc.observe(_pt(0, "5m", 50.0))                             # closes 300 s
    lc.observe(_pt(120_000, "1m", 12.0))                       # closes 180 s: older than the 5m
    assert lc.latest_close("BTC-USD") == 50.0
    lc.observe(_pt(240_000, "1m", 14.0))                       # closes 300 s: ties win
    assert lc.latest_close("BTC-USD") == 14.0
