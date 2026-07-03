"""
strategy/registry.py — the multi-strategy registry.

QISS runs a SET of strategies, configurable at runtime: which strategies are
active, and which symbols each applies to. This module is the registry that
holds that configuration and hands the runtime the enabled strategies for a
given symbol.

## What a "strategy" is here

A Strategy is a named unit with two behavioural components the runtime already
knows how to drive (the two-ladder strategy established the shape):

  - an ENTRY component:  on_event(ev) -> Optional[position_key]
  - an EXIT  component:  maybe_emit_exit(...) -> Optional[position_key]

plus its identity (name) and the symbols it trades. That's the whole contract.
The two-ladder strategy satisfies it already; a future strategy satisfies it by
providing the same two components. We deliberately keep this a PROTOCOL + a dict
registry, not a plugin framework or a DSL — there is one strategy today and a
firm plan for more, so the abstraction earns its place, but building a loader/
DSL now would be speculative (YAGNI). When strategy #2 exists and shows what
actually varies, the abstraction can grow to fit; until then it stays minimal.

## Configuration shape (config.STRATEGIES)

    STRATEGIES = {
        "two_ladder": {
            "enabled": True,
            "symbols": ["AAPL", "EURUSD", ...],   # or "*" for all tracked
            "params": { ... },                    # strategy-specific, opaque here
        },
        "future_strategy": { "enabled": False, "symbols": [...], "params": {...} },
    }

The registry reads this, instantiates each ENABLED strategy via its registered
factory, and resolves per-symbol subscriptions. A disabled strategy is never
instantiated. Changing which strategies are active is a config edit (+ restart);
the DB-backed live toggle is a possible future addition (mirroring
trade_enabled), deliberately not built yet — flip-a-strategy-live wasn't
requested and adds moving parts.

## Multiple strategies on one symbol

Two enabled strategies may both trade the same symbol and both hold positions in
it concurrently (a deliberate choice). They SHARE one ChainView (the chain state
is the market's, not a strategy's — see runtime), but their positions must not
collide. So every position is namespaced by strategy: the runtime tracks open
positions by (strategy_name, position_key), and the strategy_name is woven into
the emitted signal's identity so the downstream consumer and the DB never merge
two strategies' positions on the same chain. See namespaced_position_key.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Protocol, runtime_checkable

from strategy.chain_view import ChainView, TaggedChainEvent
from execution.types import ExitReason

log = logging.getLogger("strategy.registry")


# ---------------------------------------------------------------------------
# The Strategy protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class EntryComponent(Protocol):
    async def on_event(self, ev: TaggedChainEvent) -> Optional[str]:
        """Process one tagged event; return the position_key if an ENTER was
        emitted, else None. The ChainView is already updated with `ev`."""
        ...


@runtime_checkable
class ExitComponent(Protocol):
    async def maybe_emit_exit(
        self,
        *,
        symbol: str,
        asset_class: str,
        chain_prime_id: str,
        trade_direction: int,
        decided_at: str,
    ) -> Optional[str]:
        """Evaluate exit for one open position; emit and return position_key if
        exiting, else None."""
        ...


@dataclass
class Strategy:
    """One configured strategy: identity, its two components, the symbols it
    trades, and the LADDERS it reads chains from. The runtime routes a symbol's
    events to every enabled Strategy whose `symbols` include that symbol; each
    strategy then evaluates against the chains of the ladders it declares.

    Two strategies declaring the same ladder share that ladder's chains (the
    chain machine runs once per (symbol, ladder), not per strategy) — the
    compute-once-across-strategies property. `ladders` is how a strategy says
    which ladders' chains it needs; the union across all strategies is what the
    system computes.
    """
    name: str
    entry: EntryComponent
    exit: ExitComponent
    symbols: List[str]
    ladders: List[str]          # names of ladders this strategy reads

    def trades(self, symbol: str) -> bool:
        return symbol in self._symbol_set

    def __post_init__(self):
        self._symbol_set = set(self.symbols)


# A factory builds a Strategy given the shared ChainView, the SignalWriter, and
# the strategy's config params. Registered per strategy name; called only for
# ENABLED strategies. Kept as a plain callable (not a class hierarchy) — the
# variation between strategies is behavioural (the components), not structural.
StrategyFactory = Callable[..., Strategy]


# ---------------------------------------------------------------------------
# Position-key namespacing
# ---------------------------------------------------------------------------

def namespaced_position_key(strategy_name: str, position_key: str) -> str:
    """Weave the strategy identity into a position_key so two strategies holding
    a position on the same chain/direction never collide.

    Base position_key is `chain_id:L|S` (signal_writer.position_key). Two
    strategies can both enter chain X long; without namespacing both would emit
    position_key "X:L" and the consumer/DB would treat them as ONE position.
    Prefixing with the strategy name disambiguates: "two_ladder/X:L" vs
    "other/X:L". The separator '/' is not used in chain ids or the L/S suffix,
    so the composite parses unambiguously.
    """
    return f"{strategy_name}/{position_key}"


def split_namespaced_key(key: str) -> tuple[str, str]:
    """Inverse of namespaced_position_key: -> (strategy_name, base_position_key).
    Raises if the key isn't namespaced (defensive; all runtime keys should be)."""
    if "/" not in key:
        raise ValueError(f"not a namespaced position key: {key!r}")
    name, base = key.split("/", 1)
    return name, base


# ---------------------------------------------------------------------------
# The registry
# ---------------------------------------------------------------------------

class StrategyRegistry:
    """Holds the enabled strategies and answers "which strategies trade symbol
    S". Built once at startup from config.STRATEGIES + registered factories.
    """

    def __init__(self) -> None:
        self._factories: Dict[str, StrategyFactory] = {}
        self._strategies: Dict[str, Strategy] = {}

    def register_factory(self, name: str, factory: StrategyFactory) -> None:
        """Register how to build a strategy by name. Call at import/startup for
        every strategy QISS knows how to run (enabled or not); the config then
        decides which are actually instantiated."""
        if name in self._factories:
            raise ValueError(f"strategy factory already registered: {name}")
        self._factories[name] = factory

    def build_from_config(
        self,
        strategies_config: dict,
        *,
        all_symbols: List[str],
        **factory_kwargs,
    ) -> None:
        """Instantiate every ENABLED strategy from config.STRATEGIES.

        strategies_config: the STRATEGIES dict (name -> {enabled, symbols, params}).
        all_symbols: the full tracked-symbol list, substituted where a strategy's
                     symbols is "*" (meaning "every tracked symbol").
        factory_kwargs: shared dependencies passed to every factory (view, writer).

        A strategy that is disabled is skipped entirely (never instantiated). An
        enabled strategy naming an unregistered factory raises — a config
        referencing a strategy QISS can't build is a loud error, not a silent
        skip.
        """
        for name, cfg in strategies_config.items():
            if not cfg.get("enabled", False):
                log.info("strategy %s disabled — skipping", name)
                continue
            factory = self._factories.get(name)
            if factory is None:
                raise ValueError(
                    f"strategy {name!r} is enabled in config but has no registered "
                    f"factory (registered: {sorted(self._factories)})"
                )
            symbols = cfg.get("symbols", [])
            if symbols == "*":
                symbols = list(all_symbols)
            ladders = cfg.get("ladders", [])
            if not ladders:
                raise ValueError(
                    f"strategy {name!r} is enabled but declares no ladders; "
                    f"a strategy must name the ladders it reads chains from"
                )
            params = cfg.get("params", {})
            strategy = factory(
                name=name, symbols=symbols, ladders=ladders,
                params=params, **factory_kwargs,
            )
            self._strategies[name] = strategy
            log.info("strategy %s enabled on %d symbol(s), ladders=%s",
                     name, len(symbols), ladders)

    def strategies_for(self, symbol: str) -> List[Strategy]:
        """Every enabled strategy that trades `symbol`. The runtime calls this
        per event to fan the event out to the right strategies."""
        return [s for s in self._strategies.values() if s.trades(symbol)]

    def active(self) -> List[Strategy]:
        """All enabled/instantiated strategies."""
        return list(self._strategies.values())

    def all_symbols(self) -> List[str]:
        """Union of every enabled strategy's symbols — the set the runtime needs
        to track and the feeds need to stream."""
        out: set[str] = set()
        for s in self._strategies.values():
            out.update(s.symbols)
        return sorted(out)

    def all_ladders(self) -> List[str]:
        """Union of every enabled strategy's ladders — the set of chain machines
        the system must run. This is the compute-once boundary: a ladder shared
        by two strategies appears once here and is computed once. The router and
        machine factory build chain machines for exactly these ladders."""
        out: set[str] = set()
        for s in self._strategies.values():
            out.update(s.ladders)
        return sorted(out)

    def strategies_reading_ladder(self, ladder_name: str) -> List[Strategy]:
        """Every enabled strategy that reads `ladder_name`. Lets the runtime,
        after a chain event on a given ladder, know which strategies to notify —
        the 'which strategies monitor this ladder' half of compute-once."""
        return [s for s in self._strategies.values() if ladder_name in s.ladders]