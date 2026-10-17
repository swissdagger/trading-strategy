"""
Tests for producer/fanout_router_multi.py — the multi-ladder router.

Uses a fake ChainMachine and fake bars, so we test the router's fan-out /
tagging / lock behaviour without the real chain machine or Sumtyme.
"""
import asyncio
from dataclasses import dataclass

import pytest

from producer.fanout_router_multi import MultiLadderRouter
from strategy.ladder_registry import LadderRegistry


def default_registry() -> LadderRegistry:
    """Two overlapping ladders as test data (30s, 1m and 2m are in both)."""
    reg = LadderRegistry()
    reg.register("A", ["5s", "10s", "30s", "1m", "2m"])
    reg.register("B", ["30s", "1m", "2m", "5m", "10m"])
    return reg


@dataclass
class FakeBar:
    symbol: str
    timeframe: str


@dataclass
class FakeEvent:
    event_type: str
    chain_id: str
    symbol: str
    asset_class: str
    direction: int
    propagation_level: int
    timestamp: str
    # Mirror producer.chain_machine.ChainEvent: the router reads ce.price into
    # decision_price, so the fake must carry it.
    timeframe: str = "1m"
    price: float | None = 100.0


class FakeMachine:
    """Records bars, emits one event per bar tagged with the ladder it was built
    for (so we can assert fan-out)."""
    def __init__(self, symbol, ladder_name, timeframes):
        self.symbol = symbol
        self.ladder_name = ladder_name
        self.timeframes = timeframes
        self.hydrated = False
        self.bars = []
    async def hydrate(self):
        self.hydrated = True
    async def on_bar(self, bar):
        self.bars.append(bar)
        return [FakeEvent(
            event_type="chain_propagated", chain_id=f"{self.ladder_name}-1",
            symbol=bar.symbol, asset_class="crypto", direction=1,
            propagation_level=2, timestamp="2026-01-01T00:00:00Z",
        )]


def _registry_with_fc():
    reg = default_registry()
    reg.register("FC", ["1m", "3m", "5m", "15m", "1h"])
    return reg


def _make_router(published):
    reg = _registry_with_fc()
    machines = {}
    def factory(symbol, ladder_name, timeframes):
        m = FakeMachine(symbol, ladder_name, timeframes)
        machines[(symbol, ladder_name)] = m
        return m
    async def publish(raw):
        published.append(raw)
    router = MultiLadderRouter(reg, factory, publish)
    return router, machines


@pytest.mark.asyncio
async def test_1m_bar_fans_to_all_containing_ladders():
    published = []
    router, machines = _make_router(published)
    # 1m is in A, B, and FC.
    await router.on_bar(FakeBar("BTCUSDT", "1m"))
    ladders_hit = {r["ladder"] for r in published}
    assert ladders_hit == {"A", "B", "FC"}
    # One machine per (symbol, ladder) was created.
    assert set(machines.keys()) == {("BTCUSDT", "A"), ("BTCUSDT", "B"), ("BTCUSDT", "FC")}


@pytest.mark.asyncio
async def test_1h_bar_only_fc():
    published = []
    router, machines = _make_router(published)
    await router.on_bar(FakeBar("BTCUSDT", "1h"))
    assert {r["ladder"] for r in published} == {"FC"}


@pytest.mark.asyncio
async def test_5s_bar_only_a():
    published = []
    router, machines = _make_router(published)
    await router.on_bar(FakeBar("BTCUSDT", "5s"))
    assert {r["ladder"] for r in published} == {"A"}


@pytest.mark.asyncio
async def test_ab_events_carry_set_tag():
    published = []
    router, machines = _make_router(published)
    await router.on_bar(FakeBar("BTCUSDT", "1m"))
    # Events carry `set` equal to the ladder name (chain-events topic consumers).
    a_event = next(r for r in published if r["ladder"] == "A")
    assert a_event["set"] == "A"
    # metadata carries propagation_level.
    assert a_event["metadata"]["propagation_level"] == 2


@pytest.mark.asyncio
async def test_fc_event_carries_ladder():
    published = []
    router, machines = _make_router(published)
    await router.on_bar(FakeBar("BTCUSDT", "1h"))
    fc = published[0]
    assert fc["ladder"] == "FC"
    assert fc["chain_id"] == "FC-1"


@pytest.mark.asyncio
async def test_machine_hydrated_once():
    published = []
    router, machines = _make_router(published)
    await router.on_bar(FakeBar("BTCUSDT", "1h"))
    await router.on_bar(FakeBar("BTCUSDT", "1h"))
    m = machines[("BTCUSDT", "FC")]
    assert m.hydrated is True
    assert len(m.bars) == 2   # same machine reused (compute-once), hydrated once


@pytest.mark.asyncio
async def test_unknown_timeframe_dropped():
    published = []
    router, machines = _make_router(published)
    await router.on_bar(FakeBar("BTCUSDT", "7m"))   # in no ladder
    assert published == []
    assert machines == {}


@pytest.mark.asyncio
async def test_shared_lock_map():
    # The router uses the passed-in lock map, so it can be shared with the runtime.
    published = []
    reg = _registry_with_fc()
    shared = {}
    from collections import defaultdict
    shared_locks = defaultdict(asyncio.Lock)
    def factory(s, l, t): return FakeMachine(s, l, t)
    async def publish(raw): published.append(raw)
    router = MultiLadderRouter(reg, factory, publish, locks=shared_locks)
    assert router._locks is shared_locks


# --------------------------------------------------------------------
# End-to-end: router + runtime sharing a lock (deadlock-avoidance)
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_router_runtime_shared_lock_no_deadlock():
    """The router holds the symbol lock in on_bar and calls the runtime's
    process_raw_locked via publish; the runtime must NOT re-acquire the shared
    lock (which would deadlock). Proves the shared-lock contract end-to-end."""
    from strategy.multi_runtime import MultiStrategyRuntime, RuntimeHooks
    from strategy.registry import Strategy, StrategyRegistry

    class TLEntry:
        def __init__(self): self.ladders = []
        async def on_event(self, ev):
            self.ladders.append(ev.ladder); return None
    class TLExit:
        async def maybe_emit_exit(self, **kw): return None

    reg = default_registry()
    entry = TLEntry()
    sreg = StrategyRegistry()
    sreg._strategies["probe"] = Strategy(
        name="probe", entry=entry, exit=TLExit(), symbols=["AAPL"], ladders=["A", "B"])

    async def _empty(): return []
    async def _noop(*a): return None
    hooks = RuntimeHooks(load_open_positions=_empty, save_open_position=_noop, remove_open_position=_noop)

    from collections import defaultdict
    shared = defaultdict(asyncio.Lock)
    runtime = MultiStrategyRuntime(sreg, hooks, locks=shared)

    async def publish(raw):
        await runtime.process_raw_locked(raw)   # router holds the lock here

    router = MultiLadderRouter(reg, lambda s, l, t: FakeMachine(s, l, t), publish, locks=shared)

    # 30s is in both A and B — a genuine fan-out under the shared lock. If the
    # deadlock-avoidance were wrong, wait_for would time out.
    await asyncio.wait_for(router.on_bar(FakeBar("AAPL", "30s")), timeout=5.0)
    assert entry.ladders == ["A", "B"]