"""
Rule 1 on the dense ladder, through strategy/rule1_adapter.py.

rule1.py is the owner's frozen logic (validated in its own replay harness);
these tests pin the ADAPTER: events + clock in, trade_signals rows out, acks
and retries against the rule's state machine, pricing of clock-driven
intents, restart hydration, and the ladder-level shift.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from execution.types import ExitReason, TradeSide
from strategy.rule1 import STALL_WINDOW, VOL_RATIO_MAX
from strategy.rule1_adapter import make_rule1
from strategy.vol import vol_ratio_from_closes
from strategy.vol_source import PolygonClosesSource, VolRatioProvider
from strategy.ladder_registry import LadderRegistry

DL = ["30s", "45s", "1m", "90s", "2m", "3m", "15m", "30m", "1h", "2h", "4h", "6h"]
UTC = timezone.utc
# Wed 2026-10-14. 15:00Z = 11:00 ET (EDT) is in session; 12:00Z = 08:00 ET is not.
IN_SESSION = datetime(2026, 10, 14, 15, 0, tzinfo=UTC)
PRE_OPEN = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
OPEN = datetime(2026, 10, 14, 13, 30, tzinfo=UTC)


class FakeWriter:
    def __init__(self):
        self.rows = []
        self.fail_next = 0

    async def emit_entry(self, **kw):
        if self.fail_next:
            self.fail_next -= 1
            raise RuntimeError("db down")
        self.rows.append(("ENTER", kw))
        return f"{kw['chain_id']}:{'L' if kw['side'] == TradeSide.LONG else 'S'}"

    async def emit_exit(self, **kw):
        if self.fail_next:
            self.fail_next -= 1
            raise RuntimeError("db down")
        self.rows.append(("EXIT", kw))
        return f"{kw['chain_id']}:{'L' if kw['side'] == TradeSide.LONG else 'S'}"


class FakeVol(VolRatioProvider):
    """Deterministic ratios by symbol; prefetch records that it was awaited."""
    def __init__(self, ratios):
        self.ratios = ratios
        self.prefetched = []

    async def prefetch(self, symbol, now):
        self.prefetched.append((symbol, now))
        return self.ratios.get(symbol)

    def get(self, symbol, now):
        return self.ratios.get(symbol)


class Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


def ev(event_type, symbol, direction, chain_id, level, ts, price):
    raw = {"event_type": event_type, "symbol": symbol, "asset_class": "equity",
           "direction": direction, "chain_id": chain_id, "ladder": "DL",
           "propagation_level": level, "metadata": {"propagation_level": level},
           "timestamp": ts.isoformat(), "decision_price": price}
    from strategy.multi_runtime import _RawEventView
    return _RawEventView(raw)


def build(*, ratios=None, now=IN_SESSION, entry_level=2, load_open=None, closes=None):
    reg = LadderRegistry()
    reg.register("DL", DL)
    writer = FakeWriter()
    vol = FakeVol(ratios if ratios is not None else {"NVDA": 1.0, "XOM": 1.0, "SPCX": 1.0})
    clock = Clock(now)
    closes = closes or {"NVDA": 500.0, "XOM": 110.0, "SPCX": 40.0}
    strat = make_rule1(name="rule1", symbols=["NVDA", "XOM", "SPCX"], ladders=["DL"],
                       params={"entry_level": entry_level}, view_registry=reg,
                       writer_factory=lambda n: writer, vol_provider=vol,
                       latest_close=closes.get, load_open=load_open, clock=clock)
    return strat, writer, vol, clock


# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_p2_in_session_writes_enter_and_acks_open():
    strat, writer, vol, clock = build()
    out = await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c1", 2, IN_SESSION, 501.5))
    assert out is None                                     # the rule tracks its own state
    assert vol.prefetched == [("NVDA", IN_SESSION)]
    assert len(writer.rows) == 1
    kind, kw = writer.rows[0]
    assert kind == "ENTER" and kw["symbol"] == "NVDA" and kw["side"] == TradeSide.LONG
    assert kw["decision_price"] == 501.5 and kw["trigger_level"] == 2
    assert kw["asset_class"] == "equity"
    t = strat.entry.rule.trades["c1"]
    assert t.state == "open" and t.entry_at == IN_SESSION and t.vol_ratio == 1.0


@pytest.mark.asyncio
async def test_vol_filter_blocks_entry():
    strat, writer, _, _ = build(ratios={"NVDA": VOL_RATIO_MAX + 0.01, "XOM": None})
    await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c1", 2, IN_SESSION, 500.0))
    await strat.entry.on_event(ev("chain_propagated", "XOM", -1, "c2", 2, IN_SESSION, 110.0))
    assert writer.rows == []
    assert "c1" not in strat.entry.rule.trades and "c2" not in strat.entry.rule.trades
    assert any("vol_ratio" in m for m in strat.entry.rule.log)
    assert any("no vol history" in m for m in strat.entry.rule.log)


@pytest.mark.asyncio
async def test_p2_outside_session_queues_then_opens_at_the_open_with_latest_close():
    # P2 at 09:00 ET (30 min before the open), L3 at 09:05 so it does not stall.
    pre = OPEN - timedelta(minutes=30)
    strat, writer, _, clock = build(now=pre)
    await strat.entry.on_event(ev("chain_propagated", "XOM", -1, "c1", 2, pre, 111.0))
    assert writer.rows == []                               # queued, not written
    assert strat.entry.rule.trades["c1"].state == "pending_entry"
    clock.now = pre + timedelta(minutes=5)
    await strat.entry.on_event(ev("chain_propagated", "XOM", -1, "c1", 3, clock.now, 110.8))
    clock.now = OPEN - timedelta(seconds=1)
    await strat.entry.tick()
    assert writer.rows == []
    clock.now = OPEN
    await strat.entry.tick()
    kind, kw = writer.rows[0]
    assert kind == "ENTER" and kw["side"] == TradeSide.SHORT
    assert kw["decision_price"] == 110.0                   # latest 1m close, not the P2 price
    assert kw["decided_at"] == OPEN.isoformat()
    assert strat.entry.rule.trades["c1"].state == "open"


@pytest.mark.asyncio
async def test_queued_entry_whose_stall_fires_before_the_open_is_dropped_unfilled():
    # The rule's own contract: a queued entry is dropped if its exit condition
    # fired while waiting. P2 at 08:00 ET, no L3 -> stalls at 09:00 -> never written.
    strat, writer, _, clock = build(now=PRE_OPEN)
    await strat.entry.on_event(ev("chain_propagated", "XOM", -1, "c1", 2, PRE_OPEN, 111.0))
    clock.now = OPEN
    await strat.entry.tick()
    assert writer.rows == []
    t = strat.entry.rule.trades["c1"]
    assert t.state == "skipped" and t.exit_reason == "stall_no_L3_1h"


@pytest.mark.asyncio
async def test_stall_exit_after_60min_without_l3_is_a_TIME_exit():
    strat, writer, _, clock = build()
    await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c1", 2, IN_SESSION, 500.0))
    clock.now = IN_SESSION + STALL_WINDOW
    await strat.entry.tick()
    assert len(writer.rows) == 1                           # not yet (now > p2 + 60m is strict)
    clock.now = IN_SESSION + STALL_WINDOW + timedelta(seconds=15)
    await strat.entry.tick()
    kind, kw = writer.rows[1]
    assert kind == "EXIT" and kw["reason"] == ExitReason.TIME
    assert kw["decision_price"] == 500.0                   # latest close (no event)
    t = strat.entry.rule.trades["c1"]
    assert t.state == "closed" and t.exit_reason == "stall_no_L3_1h"


@pytest.mark.asyncio
async def test_l3_within_window_prevents_stall_exit():
    strat, writer, _, clock = build()
    await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c1", 2, IN_SESSION, 500.0))
    clock.now = IN_SESSION + timedelta(minutes=5)
    await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c1", 3, clock.now, 502.0))
    clock.now = IN_SESSION + STALL_WINDOW + timedelta(minutes=5)
    await strat.entry.tick()
    assert [k for k, _ in writer.rows] == ["ENTER"]
    assert strat.entry.rule.trades["c1"].state == "open"


@pytest.mark.asyncio
async def test_opposing_chain_initiated_closes_with_STOP_at_event_price():
    strat, writer, _, clock = build()
    await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c1", 2, IN_SESSION, 500.0))
    clock.now = IN_SESSION + timedelta(minutes=10)
    await strat.entry.on_event(ev("chain_initiated", "NVDA", -1, "c9", 1, clock.now, 497.25))
    kind, kw = writer.rows[1]
    assert kind == "EXIT" and kw["reason"] == ExitReason.STOP
    assert kw["decision_price"] == 497.25 and kw["chain_id"] == "c1"
    assert strat.entry.rule.trades["c1"].exit_reason == "opposing_P1"
    # Same-direction init on another symbol does nothing.
    await strat.entry.on_event(ev("chain_initiated", "XOM", -1, "c8", 1, clock.now, 110.0))
    assert len(writer.rows) == 2


@pytest.mark.asyncio
async def test_write_failure_is_rejected_and_retried_on_next_tick():
    strat, writer, _, clock = build()
    writer.fail_next = 1
    await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c1", 2, IN_SESSION, 500.0))
    assert writer.rows == [] and strat.entry.stats["write_failures"] == 1
    assert strat.entry.rule.trades["c1"].state == "pending_entry"   # rule.reject()
    clock.now = IN_SESSION + timedelta(seconds=15)
    await strat.entry.tick()
    assert [k for k, _ in writer.rows] == ["ENTER"]
    assert strat.entry.rule.trades["c1"].state == "open"


@pytest.mark.asyncio
async def test_hydration_restores_open_trades_and_their_stall_clock():
    young = IN_SESSION - timedelta(minutes=50)
    old = IN_SESSION - timedelta(hours=3)

    async def load_open():
        return [
            {"chain_id": "cy", "symbol": "NVDA", "direction": "LONG", "created_at": young,
             "position_key": "cy:L"},
            {"chain_id": "co", "symbol": "XOM", "direction": "SHORT", "created_at": old,
             "position_key": "co:S"},
        ]

    strat, writer, _, clock = build(load_open=load_open)
    await strat.entry.start()
    trades = strat.entry.rule.trades
    assert trades["cy"].state == "open" and trades["cy"].l3_seen is False
    assert trades["co"].state == "open" and trades["co"].l3_seen is True
    # 11 minutes later the young one stalls (50 + 11 > 60); the old one does not.
    clock.now = IN_SESSION + timedelta(minutes=11)
    await strat.entry.tick()
    assert [(k, kw["chain_id"], kw["reason"]) for k, kw in writer.rows] == \
        [("EXIT", "cy", ExitReason.TIME)]
    # The old one still has the opposing-P1 failsafe.
    await strat.entry.on_event(ev("chain_initiated", "XOM", 1, "c7", 1, clock.now, 112.0))
    assert writer.rows[-1][1]["chain_id"] == "co" and writer.rows[-1][1]["reason"] == ExitReason.STOP


@pytest.mark.asyncio
async def test_entry_level_shifts_what_the_rule_sees_as_p2():
    # entry_level=6: DL L6 (3m) is the rule's P2, L7 (15m) its L3.
    strat, writer, vol, clock = build(entry_level=6)
    await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c1", 2, IN_SESSION, 500.0))
    assert writer.rows == [] and vol.prefetched == []        # L2 is nothing now
    await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c1", 6, IN_SESSION, 500.0))
    assert [k for k, _ in writer.rows] == ["ENTER"]
    assert writer.rows[0][1]["trigger_level"] == 6
    clock.now = IN_SESSION + timedelta(minutes=20)
    await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c1", 7, clock.now, 505.0))
    assert strat.entry.rule.trades["c1"].l3_seen is True


def test_entry_level_must_fit_the_ladder():
    with pytest.raises(ValueError):
        build(entry_level=12)


# ---------------------------------------------------------------------------
# Vol source + the research function on a known series
# ---------------------------------------------------------------------------

class _Bar:
    def __init__(self, t, c):
        self.timestamp, self.close = t, c


class FakeMinuteFeed:
    """fetch_history('1m', n): the last n minute bars of a synthetic series
    ending at `self.now`. Records the n asked for."""
    def __init__(self, now_ms, rng):
        self.now_ms, self.rng, self.calls = now_ms, rng, []
        self.closes = {}

    def _close(self, t):
        if t not in self.closes:
            self.closes[t] = 100.0 * float(np.exp(self.rng.normal(0, 1e-3)))
        return self.closes[t]

    async def fetch_history(self, symbol, tf, n):
        self.calls.append((symbol, tf, n))
        end = self.now_ms // 60_000 * 60_000
        return [_Bar(t, self._close(t)) for t in range(end - n * 60_000, end, 60_000)]


@pytest.mark.asyncio
async def test_closes_source_seeds_once_then_refills_hourly_and_ratio_is_cached():
    now = datetime(2026, 10, 14, 15, 7, tzinfo=UTC)
    feed = FakeMinuteFeed(int(now.timestamp() * 1000), np.random.default_rng(0))
    src = PolygonClosesSource(feed)
    prov = VolRatioProvider(src)

    r1 = await prov.prefetch("NVDA", now)
    assert len(feed.calls) == 1 and feed.calls[0][2] >= 17 * 24 * 60   # seed = lookback
    assert r1 is not None and 0.5 < r1 < 2.0                           # stationary vol ≈ 1
    assert prov.get("NVDA", now) == r1
    # Same hour: no fetch, cached.
    await prov.prefetch("NVDA", now + timedelta(minutes=20))
    assert len(feed.calls) == 1
    # Next hour: one SMALL refill, new ratio computed.
    later = now + timedelta(hours=1)
    feed.now_ms = int(later.timestamp() * 1000)
    r2 = await prov.prefetch("NVDA", later)
    assert len(feed.calls) == 2 and feed.calls[1][2] < 500
    assert r2 is not None
    assert prov.get("NVDA", later) == r2 and prov.get("NVDA", now) is None  # old hour evicted
    assert src.latest_close("NVDA") is not None
    # The window handed to the research function excludes the current hour.
    s = await src.closes("NVDA", later)
    assert s.index.max() < pd.Timestamp(later).floor("h")


def test_vol_ratio_from_closes_research_function_sanity():
    idx = pd.date_range("2026-09-20", "2026-10-14 15:00", freq="min", tz="UTC")
    rng = np.random.default_rng(1)
    closes = pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 1e-3, len(idx)))), index=idx)
    now = datetime(2026, 10, 14, 15, 7, tzinfo=UTC)
    r = vol_ratio_from_closes(closes, now)
    assert r is not None and 0.7 < r < 1.3
    # Too little history -> None.
    assert vol_ratio_from_closes(closes.iloc[-50:], now) is None
