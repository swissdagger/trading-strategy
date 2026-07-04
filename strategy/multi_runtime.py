"""
strategy/multi_runtime.py — multi-strategy consumer-side runtime.

The multi-strategy evolution of strategy/runtime.py's StrategyRuntime. Same
ordering / per-symbol-lock / open-position-persistence discipline, generalised
from one hardcoded strategy to a REGISTRY of enabled strategies. For each event
it fans out to every enabled strategy that trades the event's symbol.

WHAT CARRIES OVER UNCHANGED (the invariants the single-strategy runtime
established, which the strategy logic depends on):

  1. ORDERING. Fold the event into the ChainView FIRST, then run strategies.
     Every strategy sees the current chain state.

  2. PER-SYMBOL LOCK. All work for a symbol is serialised under one lock — now
     spanning ALL strategies on that symbol, so two strategies mutating/reading
     the shared view for the same symbol can't interleave.

  3. OPEN-POSITION TRACKING + PERSISTENCE for continuous exit evaluation, and
     restore-on-restart.

WHAT'S NEW FOR MULTI-STRATEGY:

  * SHARED ChainView. All strategies read one view — the chain state is the
    market's, derived once from the events, not per-strategy. (Per-strategy
    views would duplicate the same computation and risk drift.)

  * POSITION NAMESPACING. Open positions are tracked by (strategy_name,
    base_position_key), and the emitted signal's position_key is namespaced
    (registry.namespaced_position_key) so two strategies holding a position on
    the same chain/direction never collide in tracking, in the signal contract,
    or in the DB. The tracking key here is the namespaced key itself.

  * FAN-OUT. on_raw_event routes to registry.strategies_for(symbol): each
    enabled strategy trading that symbol gets the entry call and its own open
    positions get the exit sweep.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Optional

from strategy.chain_view import ChainView, TaggedChainEvent
from strategy.registry import (
    Strategy,
    StrategyRegistry,
    namespaced_position_key,
    split_namespaced_key,
)

log = logging.getLogger("strategy.multi_runtime")

# Ladder names that map to the two-ladder ChainSet enum. Only these events are
# folded into the two-ladder ChainView; other ladders (e.g. FC) never touch it.
_CHAINSET_LADDER_NAMES = {"A", "B"}


class _RawEventView:
    """Thin attribute-access wrapper over a raw event dict, so a strategy
    component that reads ev.symbol / ev.chain_id / etc. (like the four-chain
    adapter) can consume a raw dict directly — no TaggedChainEvent construction
    needed for non-A/B ladders (TaggedChainEvent is A/B-specific)."""
    __slots__ = ("_raw",)

    def __init__(self, raw: dict):
        self._raw = raw

    def __getattr__(self, name):
        try:
            return self._raw[name]
        except KeyError as e:
            # asset_class may be absent on some events; default empty like the
            # TaggedChainEvent path.
            if name == "asset_class":
                return ""
            raise AttributeError(name) from e


@dataclass
class OpenPosition:
    """A position a specific strategy considers open, for exit evaluation.

    tracking_key is the NAMESPACED position key (strategy_name/base_key) — unique
    across strategies. strategy_name is carried explicitly so a restart routes
    the position's exit evaluation back to the right strategy's exit component.
    """
    tracking_key: str          # namespaced: "strategy_name/chain_id:L|S"
    strategy_name: str
    position_key: str          # base key (chain_id:L|S), for the exit call
    symbol: str
    asset_class: str
    chain_prime_id: str
    trade_direction: int


@dataclass
class RuntimeHooks:
    """Persistence for the open-position set (survives restart). Same shape as
    the single-strategy runtime's hooks; the stored key is now the namespaced
    tracking_key so positions from different strategies stay distinct."""
    load_open_positions: Callable[[], Awaitable[list[OpenPosition]]]
    save_open_position: Callable[[OpenPosition], Awaitable[None]]
    remove_open_position: Callable[[str], Awaitable[None]]   # by tracking_key


class MultiStrategyRuntime:
    """Drives the consumer side for a REGISTRY of strategies. One instance; one
    event ingress (on_raw_event)."""

    def __init__(
        self,
        view: ChainView,
        registry: StrategyRegistry,
        hooks: RuntimeHooks,
        locks: Optional[Dict[str, asyncio.Lock]] = None,
    ):
        self.view = view
        self.registry = registry
        self.hooks = hooks
        self._locks: Dict[str, asyncio.Lock] = (
            locks if locks is not None else defaultdict(asyncio.Lock)
        )
        # symbol -> {tracking_key -> OpenPosition}. Namespaced keys keep
        # different strategies' positions on one symbol distinct.
        self._open_by_symbol: Dict[str, Dict[str, OpenPosition]] = defaultdict(dict)

    def _lock(self, symbol: str) -> asyncio.Lock:
        return self._locks[symbol]

    async def start(self) -> None:
        """Restore open positions so exit evaluation resumes after a restart."""
        for p in await self.hooks.load_open_positions():
            self._open_by_symbol[p.symbol][p.tracking_key] = p
        n = sum(len(v) for v in self._open_by_symbol.values())
        log.info("multi-runtime restored %d open positions for exit evaluation", n)

    async def on_raw_event(self, raw: dict) -> None:
        """Single ingress per causal-events message. Parses the raw message into
        a tagged event, then processes it (apply-to-view, entry fan-out, exit
        sweep) under the symbol lock."""
        ev = TaggedChainEvent.from_raw(raw)
        await self.on_raw_event_direct(ev)

    async def on_raw_event(self, raw: dict) -> None:
        """Single ingress per causal-events message (a raw dict from the router,
        carrying `ladder` and, for A/B ladders, `set`). Acquires the symbol lock
        and processes."""
        symbol = raw["symbol"]
        async with self._lock(symbol):
            await self._process_raw_locked(raw)

    async def on_raw_event_direct(self, raw: dict) -> None:
        """Alias kept for callers/tests that drive the runtime with a raw dict
        while NOT holding the lock. Acquires the lock and processes."""
        await self.on_raw_event(raw)

    async def _process_raw_locked(self, raw: dict) -> None:
        """Process one raw tagged event, assuming the caller ALREADY HOLDS this
        symbol's lock (the shared-lock entry point the router's publish sink
        calls — re-acquiring a non-reentrant asyncio.Lock would deadlock).

        Routing by ladder is the heart of the multi-ladder design:
          * The two-ladder ChainView is A/B-only; we fold it ONLY for A/B events,
            so an FC-ladder event can't corrupt it.
          * Each strategy receives the event through its entry component. The
            two-ladder strategy reads a TaggedChainEvent + the shared ChainView;
            the four-chain strategy's adapter folds its OWN history view and
            reads the raw fields. So each strategy sees only what its ladder
            produces, and folding is per-monitor, not one global view.

        Ordering the strategy logic depends on: fold the relevant view FIRST,
        then entry fan-out, then the exit sweep."""
        symbol = raw["symbol"]
        ladder = raw.get("ladder", raw.get("set"))

        # 1. Fold the two-ladder ChainView, but ONLY for its ladders (A/B). This
        # keeps FC (or any non-A/B) events out of the A/B view.
        tagged = None
        if ladder in _CHAINSET_LADDER_NAMES and self.view is not None:
            tagged = TaggedChainEvent.from_raw(raw)
            self.view.apply(tagged)

        # 2. Entry: fan out to each enabled strategy trading this symbol. Each
        # strategy's entry component adapts the event to its own needs (the
        # two-ladder one expects a TaggedChainEvent; the four-chain adapter reads
        # the raw dict and folds its own view). We pass the object each expects:
        # a TaggedChainEvent when we built one (A/B), else the raw dict.
        event_for_strategies = tagged if tagged is not None else _RawEventView(raw)
        for strategy in self.registry.strategies_for(symbol):
            await self._try_entry(strategy, event_for_strategies, raw)

        # 3. Exit sweep: every open position on this symbol, routed to its owner.
        await self._sweep_exits(symbol, raw["timestamp"])

    async def _try_entry(self, strategy: Strategy, ev, raw: dict) -> None:
        base_pkey = await strategy.entry.on_event(ev)
        if base_pkey is None:
            return
        symbol = raw["symbol"]
        asset_class = raw.get("asset_class", "")
        tracking_key = namespaced_position_key(strategy.name, base_pkey)
        # Idempotency: a re-fired trigger the signal layer deduped must not
        # double-track. Check the namespaced key.
        if tracking_key in self._open_by_symbol[symbol]:
            return
        chain_prime_id, dir_code = base_pkey.rsplit(":", 1)
        pos = OpenPosition(
            tracking_key=tracking_key,
            strategy_name=strategy.name,
            position_key=base_pkey,
            symbol=symbol,
            asset_class=asset_class,
            chain_prime_id=chain_prime_id,
            trade_direction=1 if dir_code == "L" else -1,
        )
        self._open_by_symbol[symbol][tracking_key] = pos
        await self.hooks.save_open_position(pos)
        log.info("tracking open position %s (strategy=%s) for exit",
                 tracking_key, strategy.name)

    async def _sweep_exits(self, symbol: str, decided_at: str) -> None:
        """Evaluate exit for every open position on this symbol, each routed to
        the strategy that owns it. Emitting an EXIT stops tracking that
        position."""
        positions = list(self._open_by_symbol.get(symbol, {}).values())
        by_name = {s.name: s for s in self.registry.active()}
        for pos in positions:
            strategy = by_name.get(pos.strategy_name)
            if strategy is None:
                # The owning strategy was disabled/removed since this position
                # opened. We can no longer evaluate its exit rule; leave it
                # tracked but log — an operator must decide (disabling a strategy
                # with open positions is a deliberate act with consequences).
                log.warning(
                    "open position %s has no active strategy %s; exit not evaluated",
                    pos.tracking_key, pos.strategy_name,
                )
                continue
            exited = await strategy.exit.maybe_emit_exit(
                symbol=pos.symbol,
                asset_class=pos.asset_class,
                chain_prime_id=pos.chain_prime_id,
                trade_direction=pos.trade_direction,
                decided_at=decided_at,
            )
            if exited is not None:
                self._open_by_symbol[symbol].pop(pos.tracking_key, None)
                await self.hooks.remove_open_position(pos.tracking_key)
                log.info("stopped tracking %s (EXIT signalled)", pos.tracking_key)