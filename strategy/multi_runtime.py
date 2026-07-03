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

    async def on_raw_event_direct(self, ev) -> None:
        """Process an already-parsed tagged event. Under the symbol lock:
        apply-to-view, then for EVERY enabled strategy on this symbol run entry,
        then sweep exits for every open position on this symbol. Separated from
        on_raw_event so a caller (or test) holding a built event can drive the
        runtime directly."""
        async with self._lock(ev.symbol):
            # 1. Fold into the shared view first.
            self.view.apply(ev)
            # 2. Entry: fan out to each enabled strategy trading this symbol.
            for strategy in self.registry.strategies_for(ev.symbol):
                await self._try_entry(strategy, ev)
            # 3. Exit sweep: every open position on this symbol, routed back to
            #    its own strategy's exit component.
            await self._sweep_exits(ev.symbol, ev.timestamp)

    async def _try_entry(self, strategy: Strategy, ev: TaggedChainEvent) -> None:
        base_pkey = await strategy.entry.on_event(ev)
        if base_pkey is None:
            return
        tracking_key = namespaced_position_key(strategy.name, base_pkey)
        # Idempotency: a re-fired trigger the signal layer deduped must not
        # double-track. Check the namespaced key.
        if tracking_key in self._open_by_symbol[ev.symbol]:
            return
        chain_prime_id, dir_code = base_pkey.rsplit(":", 1)
        pos = OpenPosition(
            tracking_key=tracking_key,
            strategy_name=strategy.name,
            position_key=base_pkey,
            symbol=ev.symbol,
            asset_class=ev.asset_class,
            chain_prime_id=chain_prime_id,
            trade_direction=1 if dir_code == "L" else -1,
        )
        self._open_by_symbol[ev.symbol][tracking_key] = pos
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