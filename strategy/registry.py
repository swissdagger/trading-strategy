"""
strategy/registry.py — the multi-strategy registry.

The system runs a SET of strategies, configured in config.STRATEGIES: which are
enabled, which symbols each trades, and which ladders each reads. This module
holds that configuration and hands the runtime the enabled strategies for a
given symbol.

## What a "strategy" is here

A Strategy is a named unit with two behavioural components the runtime drives:

  - an ENTRY component:  on_event(ev) -> Optional[position_key]
  - an EXIT  component:  maybe_emit_exit(...) -> Optional[position_key]

plus its identity (name), the symbols it trades and the ladders it reads. That
is the whole contract: a PROTOCOL + a dict registry, deliberately not a plugin
framework.

## Configuration shape (config.STRATEGIES)

    STRATEGIES = {
        "confirmed_15m": {
            "enabled": True,
            "symbols": ["BTC-USD", ...],          # or "*" for all tracked
            "ladders": ["FC"],
            "params": { ... },                    # strategy-specific, opaque here
        },
    }

The registry instantiates each ENABLED strategy via its registered factory and
resolves per-symbol subscriptions. A disabled strategy is never instantiated.
Changing which strategies are active is a config edit (+ restart).

## Multiple strategies on one symbol

Two enabled strategies may both trade the same symbol and hold positions in it
concurrently. Their positions must not collide, so every position is
namespaced by strategy: the runtime tracks open positions by
(strategy_name, position_key), and the strategy_name is woven into the emitted
signal's identity so the downstream consumer and the DB never merge two
strategies' positions on the same chain. See namespaced_position_key.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Protocol, runtime_checkable


log = logging.getLogger("strategy.registry")


# ---------------------------------------------------------------------------
# The Strategy protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class EntryComponent(Protocol):
    async def on_event(self, ev) -> Optional[str]:
        """Process one chain event (attribute access over the router's raw
        event); return the position_key if an ENTER was emitted, else None."""
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


# A factory builds a Strategy given its name, symbols, ladders and config
# params. Registered per strategy name; called only for
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
    Prefixing with the strategy name disambiguates: "rule3_v1/X:L" vs
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

    def owned_symbol_ladders(self) -> set[tuple[str, str]]:
        """The set of (symbol, ladder_name) pairs at least one enabled strategy
        actually reads.

        The router computes chains per (symbol, ladder), fanning a bar to EVERY
        ladder whose timeframes contain the bar's timeframe. That over-computes
        once a strategy is scoped to a SUBSET of symbols: e.g. the CN-ladder rules
        on BTC-USD only and rule3_v1 (FC) on every crypto symbol share the 1m
        timeframe, so an ETH-USD 1m bar would build CN machines that NO strategy reads
        (wasted Sumtyme forecasts + junk chains). This set is the ground truth of
        which (symbol, ladder) pairs are actually wanted; the router intersects
        its timeframe-derived targets with it so it computes exactly the chains a
        strategy consumes — no more. A symbol×ladder the strategy doesn't declare
        is simply absent."""
        out: set[tuple[str, str]] = set()
        for s in self._strategies.values():
            for sym in s.symbols:
                for ladder in s.ladders:
                    out.add((sym, ladder))
        return out