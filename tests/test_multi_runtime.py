"""
Tests for strategy/multi_runtime.py — the fan-out, namespacing, and exit
routing across multiple strategies.

Uses fake strategies whose entry/exit fire on command, an in-memory persistence
hooks, and a minimal ChainView stand-in, so we test the runtime's orchestration
without the real strategy or Sumtyme machinery.
"""
import pytest

from strategy.registry import Strategy, StrategyRegistry
from strategy.multi_runtime import MultiStrategyRuntime, OpenPosition, RuntimeHooks


# --------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------

class FakeView:
    def __init__(self):
        self.applied = []
    def apply(self, ev):
        self.applied.append(ev)


def raw_event(symbol, asset_class="equity", timestamp="2026-01-01T00:00:00Z",
              ladder="FC", chain_id="c", direction=1, propagation_level=1,
              event_type="chain_propagated"):
    """Build a raw event dict as the router produces. Defaults to a non-A/B
    ladder ('FC') so the two-ladder ChainView isn't folded (the fakes here don't
    exercise it); A/B routing is covered by the two-ladder path's own tests."""
    return {
        "symbol": symbol, "asset_class": asset_class, "timestamp": timestamp,
        "ladder": ladder, "set": ladder, "chain_id": chain_id,
        "direction": direction, "propagation_level": propagation_level,
        "event_type": event_type,
    }


class ScriptedEntry:
    """Entry that returns a fixed position_key when armed, else None."""
    def __init__(self):
        self.armed_pkey = None
        self.calls = 0
    async def on_event(self, ev):
        self.calls += 1
        pkey, self.armed_pkey = self.armed_pkey, None
        return pkey


class ScriptedExit:
    """Exit that returns its position_key (exiting) when armed for that key."""
    def __init__(self):
        self.exit_keys = set()
        self.evaluated = []
    async def maybe_emit_exit(self, *, symbol, asset_class, chain_prime_id, trade_direction, decided_at,
                              decision_price=None):
        base = f"{chain_prime_id}:{'L' if trade_direction == 1 else 'S'}"
        self.evaluated.append(base)
        if base in self.exit_keys:
            self.exit_keys.discard(base)
            return base
        return None


class MemHooks(RuntimeHooks):
    def __init__(self):
        self.store = {}
        super().__init__(
            load_open_positions=self._load,
            save_open_position=self._save,
            remove_open_position=self._remove,
        )
    async def _load(self):
        return list(self.store.values())
    async def _save(self, pos):
        self.store[pos.tracking_key] = pos
    async def _remove(self, key):
        self.store.pop(key, None)


def _strategy(name, entry, exit_, symbols, ladders=("FC",)):
    return Strategy(name=name, entry=entry, exit=exit_, symbols=symbols, ladders=list(ladders))


def _registry(*strategies):
    reg = StrategyRegistry()
    for s in strategies:
        reg._strategies[s.name] = s   # inject directly (bypass config for the test)
    return reg


# --------------------------------------------------------------------
# Fan-out: two strategies on one symbol
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_both_strategies_get_entry():
    e1, e2 = ScriptedEntry(), ScriptedEntry()
    x1, x2 = ScriptedExit(), ScriptedExit()
    s1 = _strategy("s1", e1, x1, ["AAPL"])
    s2 = _strategy("s2", e2, x2, ["AAPL"])
    rt = MultiStrategyRuntime(FakeView(), _registry(s1, s2), MemHooks())

    await rt.on_raw_event_direct(raw_event("AAPL"))
    # Both strategies' entry evaluated.
    assert e1.calls == 1 and e2.calls == 1


@pytest.mark.asyncio
async def test_namespaced_positions_do_not_collide():
    e1, e2 = ScriptedEntry(), ScriptedEntry()
    x1, x2 = ScriptedExit(), ScriptedExit()
    # Both strategies enter the SAME chain/direction on AAPL.
    e1.armed_pkey = "chainX:L"
    e2.armed_pkey = "chainX:L"
    s1 = _strategy("s1", e1, x1, ["AAPL"])
    s2 = _strategy("s2", e2, x2, ["AAPL"])
    hooks = MemHooks()
    rt = MultiStrategyRuntime(FakeView(), _registry(s1, s2), hooks)

    await rt.on_raw_event_direct(raw_event("AAPL"))
    # Two DISTINCT tracked positions despite the same base chain key.
    keys = set(hooks.store.keys())
    assert keys == {"s1/chainX:L", "s2/chainX:L"}


@pytest.mark.asyncio
async def test_exit_routes_to_owning_strategy():
    e1, e2 = ScriptedEntry(), ScriptedEntry()
    x1, x2 = ScriptedExit(), ScriptedExit()
    e1.armed_pkey = "chainX:L"
    e2.armed_pkey = "chainX:L"
    s1 = _strategy("s1", e1, x1, ["AAPL"])
    s2 = _strategy("s2", e2, x2, ["AAPL"])
    hooks = MemHooks()
    rt = MultiStrategyRuntime(FakeView(), _registry(s1, s2), hooks)

    # Open both positions.
    await rt.on_raw_event_direct(raw_event("AAPL"))
    assert set(hooks.store.keys()) == {"s1/chainX:L", "s2/chainX:L"}

    # Arm ONLY s1's exit for chainX:L. Next event should exit s1's position only.
    x1.exit_keys.add("chainX:L")
    await rt.on_raw_event_direct(raw_event("AAPL"))
    assert "s1/chainX:L" not in hooks.store   # s1 exited
    assert "s2/chainX:L" in hooks.store        # s2 still open


@pytest.mark.asyncio
async def test_disabled_strategy_position_not_evaluated(caplog):
    # A position whose owning strategy is no longer active must not be
    # evaluated (and must warn), not crash.
    e1 = ScriptedEntry()
    x1 = ScriptedExit()
    e1.armed_pkey = "chainX:L"
    s1 = _strategy("s1", e1, x1, ["AAPL"])
    hooks = MemHooks()
    reg = _registry(s1)
    rt = MultiStrategyRuntime(FakeView(), reg, hooks)
    await rt.on_raw_event_direct(raw_event("AAPL"))
    assert "s1/chainX:L" in hooks.store

    # Remove s1 from the registry (simulate it being disabled after a restart).
    reg._strategies.clear()
    await rt.on_raw_event_direct(raw_event("AAPL"))
    # Position still tracked (not silently dropped), and no crash.
    assert "s1/chainX:L" in hooks.store


@pytest.mark.asyncio
async def test_only_subscribed_strategies_act():
    e1, e2 = ScriptedEntry(), ScriptedEntry()
    x1, x2 = ScriptedExit(), ScriptedExit()
    s1 = _strategy("s1", e1, x1, ["AAPL"])          # AAPL only
    s2 = _strategy("s2", e2, x2, ["MSFT"])          # MSFT only
    rt = MultiStrategyRuntime(FakeView(), _registry(s1, s2), MemHooks())

    await rt.on_raw_event_direct(raw_event("AAPL"))
    assert e1.calls == 1
    assert e2.calls == 0   # s2 doesn't trade AAPL


@pytest.mark.asyncio
async def test_restart_restores_positions():
    hooks = MemHooks()
    # Pre-seed a persisted position as if from before a restart.
    hooks.store["s1/chainX:L"] = OpenPosition(
        tracking_key="s1/chainX:L", strategy_name="s1", position_key="chainX:L",
        symbol="AAPL", asset_class="equity", chain_prime_id="chainX", trade_direction=1,
    )
    e1, x1 = ScriptedEntry(), ScriptedExit()
    s1 = _strategy("s1", e1, x1, ["AAPL"])
    rt = MultiStrategyRuntime(FakeView(), _registry(s1), hooks)
    await rt.start()
    # The restored position is now tracked for exit evaluation.
    x1.exit_keys.add("chainX:L")
    await rt.on_raw_event_direct(raw_event("AAPL"))
    assert "s1/chainX:L" not in hooks.store   # restored, then exited


# --------------------------------------------------------------------
# Backward-compat: the two-ladder path through the raw-dict runtime
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_two_ladder_view_folded_for_ab_events():
    """An A/B-ladder raw event folds the two-ladder ChainView and reaches the
    strategy as a TaggedChainEvent; a non-A/B (FC) event does NOT touch the
    two-ladder view."""
    from strategy.chain_view import ChainView, ChainSet

    class TLEntry:
        def __init__(self): self.seen = []
        async def on_event(self, ev):
            self.seen.append(ev); return None
    class TLExit:
        async def maybe_emit_exit(self, **kw): return None

    view = ChainView()
    entry = TLEntry()
    s = Strategy(name="two_ladder", entry=entry, exit=TLExit(),
                 symbols=["AAPL"], ladders=["A", "B"])
    reg = _registry(s)
    rt = MultiStrategyRuntime(view, reg, MemHooks())

    # A-ladder event: view folded, strategy sees a TaggedChainEvent with .set.
    await rt.on_raw_event(raw_event("AAPL", ladder="A", chain_id="cA",
                                    propagation_level=5))
    assert view.get_chain("AAPL", ChainSet.A, "cA") is not None
    assert entry.seen[-1].set == ChainSet.A

    # FC-ladder event on the same symbol: two-ladder view must NOT gain a chain
    # from it (it isn't an A/B chain).
    before = len(view.active_chains("AAPL", ChainSet.A)) + len(view.active_chains("AAPL", ChainSet.B))
    await rt.on_raw_event(raw_event("AAPL", ladder="FC", chain_id="cFC",
                                    propagation_level=2))
    after = len(view.active_chains("AAPL", ChainSet.A)) + len(view.active_chains("AAPL", ChainSet.B))
    assert after == before   # FC event didn't touch the two-ladder view