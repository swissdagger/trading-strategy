"""
Tests for the multi-strategy machinery:
  - LadderRegistry: level-relativity preserved, union computation (compute-once),
    per-timeframe routing
  - StrategyRegistry: enable/disable, ladder/symbol unions, per-symbol routing,
    unregistered-factory error
  - namespacing helpers
"""
import pytest

from strategy.ladder_registry import (
    Ladder,
    LadderRegistry,
    default_registry,
    SET_A_LADDER,
    SET_B_LADDER,
)
from strategy.registry import (
    Strategy,
    StrategyRegistry,
    namespaced_position_key,
    split_namespaced_key,
)


# --------------------------------------------------------------------
# LadderRegistry — level relativity (the invariant)
# --------------------------------------------------------------------

def test_level_is_ladder_relative():
    reg = default_registry()
    a = reg.get("A")
    b = reg.get("B")
    # 2m is the TOP of A (level 5) but the MIDDLE of B (level 3).
    assert a.level_of("2m") == 5
    assert b.level_of("2m") == 3
    # 30s: level 3 in A, level 1 (baseline) in B.
    assert a.level_of("30s") == 3
    assert b.level_of("30s") == 1


def test_level_of_rejects_foreign_timeframe():
    reg = default_registry()
    with pytest.raises(ValueError, match="not in ladder"):
        reg.get("A").level_of("10m")   # 10m is only in B


def test_baseline_is_first_timeframe():
    reg = default_registry()
    assert reg.get("A").baseline_timeframe == "5s"
    assert reg.get("B").baseline_timeframe == "30s"


def test_top_level():
    reg = default_registry()
    assert reg.get("A").top_level == 5
    assert reg.get("B").top_level == 5


# --------------------------------------------------------------------
# LadderRegistry — routing and unions (compute-once)
# --------------------------------------------------------------------

def test_ladders_for_timeframe_overlap():
    reg = default_registry()
    # Overlap timeframes belong to both ladders.
    assert set(reg.ladders_for_timeframe("30s")) == {"A", "B"}
    assert set(reg.ladders_for_timeframe("1m")) == {"A", "B"}
    assert set(reg.ladders_for_timeframe("2m")) == {"A", "B"}
    # A-only / B-only.
    assert reg.ladders_for_timeframe("5s") == ["A"]
    assert reg.ladders_for_timeframe("10m") == ["B"]
    # Unknown -> [].
    assert reg.ladders_for_timeframe("99h") == []


def test_all_timeframes_is_union():
    reg = default_registry()
    tfs = reg.all_timeframes()
    # Union of A and B, first-seen order, deduplicated.
    assert tfs == ["5s", "10s", "30s", "1m", "2m", "5m", "10m"]


def test_timeframes_for_ladders_subset():
    reg = default_registry()
    reg.register("C", ["1m", "5m", "1h"])
    # A strategy reading only C needs only C's timeframes.
    assert reg.timeframes_for_ladders(["C"]) == ["1m", "5m", "1h"]
    # Reading A and C: union, deduped.
    assert reg.timeframes_for_ladders(["A", "C"]) == ["5s", "10s", "30s", "1m", "2m", "5m", "1h"]


def test_register_new_ladder_and_route():
    reg = default_registry()
    reg.register("C", ["1m", "5m", "1h"])
    # 1m now belongs to A, B, AND C — a bar at 1m feeds all three machines.
    assert set(reg.ladders_for_timeframe("1m")) == {"A", "B", "C"}
    # 1h belongs only to C.
    assert reg.ladders_for_timeframe("1h") == ["C"]


def test_duplicate_register_rejected():
    reg = default_registry()
    with pytest.raises(ValueError, match="already registered"):
        reg.register("A", ["1m"])


# --------------------------------------------------------------------
# namespacing
# --------------------------------------------------------------------

def test_namespaced_key_roundtrip():
    k = namespaced_position_key("two_ladder", "chainABC:L")
    assert k == "two_ladder/chainABC:L"
    name, base = split_namespaced_key(k)
    assert name == "two_ladder"
    assert base == "chainABC:L"


def test_split_rejects_unnamespaced():
    with pytest.raises(ValueError):
        split_namespaced_key("chainABC:L")


def test_two_strategies_same_chain_distinct_keys():
    base = "chainABC:L"
    k1 = namespaced_position_key("two_ladder", base)
    k2 = namespaced_position_key("mean_revert", base)
    assert k1 != k2   # same chain, different strategies -> distinct tracking keys


# --------------------------------------------------------------------
# StrategyRegistry
# --------------------------------------------------------------------

class DummyEntry:
    async def on_event(self, ev): return None

class DummyExit:
    async def maybe_emit_exit(self, **kw): return None


def _make_strategy(name, symbols, ladders, params):
    return Strategy(
        name=name, entry=DummyEntry(), exit=DummyExit(),
        symbols=symbols, ladders=ladders,
    )


def test_registry_builds_only_enabled():
    reg = StrategyRegistry()
    reg.register_factory("two_ladder", lambda **kw: _make_strategy(**{k: kw[k] for k in ("name","symbols","ladders","params")}))
    reg.register_factory("other", lambda **kw: _make_strategy(**{k: kw[k] for k in ("name","symbols","ladders","params")}))
    config = {
        "two_ladder": {"enabled": True, "symbols": ["AAPL"], "ladders": ["A", "B"]},
        "other":      {"enabled": False, "symbols": ["MSFT"], "ladders": ["A"]},
    }
    reg.build_from_config(config, all_symbols=["AAPL", "MSFT"])
    names = [s.name for s in reg.active()]
    assert names == ["two_ladder"]   # 'other' disabled -> not built


def test_registry_enabled_unregistered_factory_raises():
    reg = StrategyRegistry()
    config = {"ghost": {"enabled": True, "symbols": ["AAPL"], "ladders": ["A"]}}
    with pytest.raises(ValueError, match="no registered factory"):
        reg.build_from_config(config, all_symbols=["AAPL"])


def test_registry_requires_ladders():
    reg = StrategyRegistry()
    reg.register_factory("two_ladder", lambda **kw: _make_strategy(**{k: kw[k] for k in ("name","symbols","ladders","params")}))
    config = {"two_ladder": {"enabled": True, "symbols": ["AAPL"], "ladders": []}}
    with pytest.raises(ValueError, match="no ladders"):
        reg.build_from_config(config, all_symbols=["AAPL"])


def test_registry_star_symbols_expands():
    reg = StrategyRegistry()
    reg.register_factory("two_ladder", lambda **kw: _make_strategy(**{k: kw[k] for k in ("name","symbols","ladders","params")}))
    config = {"two_ladder": {"enabled": True, "symbols": "*", "ladders": ["A"]}}
    reg.build_from_config(config, all_symbols=["AAPL", "MSFT", "EURUSD"])
    assert set(reg.active()[0].symbols) == {"AAPL", "MSFT", "EURUSD"}


def test_registry_ladder_union_compute_once():
    reg = StrategyRegistry()
    for n in ("s1", "s2"):
        reg.register_factory(n, lambda **kw: _make_strategy(**{k: kw[k] for k in ("name","symbols","ladders","params")}))
    config = {
        "s1": {"enabled": True, "symbols": ["AAPL"], "ladders": ["A", "B"]},
        "s2": {"enabled": True, "symbols": ["AAPL"], "ladders": ["B", "C"]},
    }
    reg.build_from_config(config, all_symbols=["AAPL"])
    # Union of ladders across strategies — B appears once (compute-once).
    assert reg.all_ladders() == ["A", "B", "C"]
    # Both strategies read B -> both notified for a B event.
    readers = [s.name for s in reg.strategies_reading_ladder("B")]
    assert set(readers) == {"s1", "s2"}
    # Only s2 reads C.
    assert [s.name for s in reg.strategies_reading_ladder("C")] == ["s2"]


def test_registry_per_symbol_routing():
    reg = StrategyRegistry()
    for n in ("s1", "s2"):
        reg.register_factory(n, lambda **kw: _make_strategy(**{k: kw[k] for k in ("name","symbols","ladders","params")}))
    config = {
        "s1": {"enabled": True, "symbols": ["AAPL"], "ladders": ["A"]},
        "s2": {"enabled": True, "symbols": ["AAPL", "MSFT"], "ladders": ["A"]},
    }
    reg.build_from_config(config, all_symbols=["AAPL", "MSFT"])
    # AAPL is traded by both; MSFT only by s2.
    assert {s.name for s in reg.strategies_for("AAPL")} == {"s1", "s2"}
    assert [s.name for s in reg.strategies_for("MSFT")] == ["s2"]
    assert reg.strategies_for("TSLA") == []
