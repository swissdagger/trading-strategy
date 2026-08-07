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

from strategy.chain_view import ChainSet, ChainView, TaggedChainEvent
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
    # Optional: load every active chain (all symbols, both sets) to hydrate the
    # shared two-ladder ChainView at startup — see chain_view.ChainView.seed_active
    # and producer.persistence.make_load_chain_view_seed. Optional (defaults to
    # None) so existing callers/tests that don't wire DB-backed chain hydration
    # keep working unchanged; start() simply skips seeding when it's absent.
    load_chain_view_seed: Optional[Callable[[], Awaitable[list[dict]]]] = None


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
        """Restore open positions so exit evaluation resumes after a restart,
        then hydrate the shared two-ladder ChainView from the DB so entry/exit
        decisions see chains that were active before the restart (not just ones
        that emit a fresh event after it). See ChainView.seed_active for why the
        view can't be reconstructed from the live event stream alone."""
        for p in await self.hooks.load_open_positions():
            self._open_by_symbol[p.symbol][p.tracking_key] = p
        n = sum(len(v) for v in self._open_by_symbol.values())
        log.info("multi-runtime restored %d open positions for exit evaluation", n)

        # Hydrate the shared two-ladder ChainView, separately from open positions.
        if self.view is not None and self.hooks.load_chain_view_seed is not None:
            seeds = await self.hooks.load_chain_view_seed()
            for s in seeds:
                self.view.seed_active(
                    symbol=s["symbol"], set_=ChainSet(s["set"]),
                    chain_id=s["chain_id"], direction=s["direction"],
                    peak_level=s["peak_level"],
                )
            log.info("multi-runtime seeded ChainView with %d active chains from DB",
                     len(seeds))

    async def on_raw_event(self, raw: dict) -> None:
        """Single ingress per causal-events message: a RAW event dict from the
        router (carrying `ladder` and, for A/B ladders, `set`). Acquires the
        symbol lock, then processes (fold-to-view for A/B, entry fan-out, exit
        sweep) via _process_raw_locked.

        The caller must NOT already hold the symbol lock — this is the entry
        point for callers OUTSIDE the shared-lock region (e.g. a Kafka consumer
        loop, or the tests). The in-lock entry point the router's publish sink
        uses is _process_raw_locked directly (see _publish in signal_process),
        which is why processing lives there and this method is a thin
        lock-acquire wrapper around it."""
        symbol = raw["symbol"]
        async with self._lock(symbol):
            await self._process_raw_locked(raw)

    # Backward-compatible alias: some callers/tests drive the runtime with a raw
    # dict via on_raw_event_direct. It has the same contract as on_raw_event
    # (takes a raw dict, acquires the lock, processes), so it simply delegates.
    async def on_raw_event_direct(self, raw: dict) -> None:
        """Alias for on_raw_event, kept for callers/tests using the older name.
        Same contract: raw dict in, lock acquired here, processed."""
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
            # Isolate per strategy. An exception raised inside one strategy's
            # entry path (a bad event field, a DB write that fails, a bug in the
            # rule) previously unwound all the way out through _publish ->
            # router.on_bar -> the consume loop, KILLING the pipeline silently —
            # the process stops draining Kafka and the DB just stops advancing.
            # That is the failure this system has already been bitten by twice
            # (the forecast hang, and the trade_signals decided_at bind). One
            # strategy failing on one event must degrade to "this strategy
            # skipped this event", never "the process stops".
            #
            # Scoped per-strategy, not around the whole loop, so a failure in
            # strategy #1 does not skip strategy #2's entry evaluation.
            # NEW GATEKEEPER: Only pass the event if the strategy declared this ladder
            if ladder not in strategy.ladders:
                continue
            try:
                await self._try_entry(strategy, event_for_strategies, raw)
            except Exception:
                log.exception(
                    "entry evaluation FAILED for strategy=%s symbol=%s "
                    "chain=%s event=%s — skipping this strategy for this event "
                    "(pipeline continues)",
                    strategy.name, symbol, raw.get("chain_id"),
                    raw.get("event_type"),
                )

        # 3. Exit sweep: every open position on this symbol, routed to its owner.
        # Deliberately OUTSIDE the entry try/except above: a failed entry must
        # never prevent an open position's exit from being evaluated. Exits are
        # risk-reducing; skipping one is strictly worse than skipping an entry.
        # (_sweep_exits does its own per-position isolation for the same reason.)
        #
        # The event's price is the EXIT's decision price: this event is what the
        # exit rule is being evaluated against, and its timestamp is already what
        # becomes decided_at — so price and time must come from the SAME event or
        # the consuming team's slippage reference is internally inconsistent.
        await self._sweep_exits(
            symbol, raw["timestamp"], raw.get("decision_price"))

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

    async def _sweep_exits(self, symbol: str, decided_at: str,
                           decision_price: Optional[float] = None) -> None:
        """Evaluate exit for every open position on this symbol, each routed to
        the strategy that owns it. Emitting an EXIT stops tracking that position.

        decision_price is the price carried by the event driving this sweep, and
        is written to trade_signals.decision_price on any EXIT emitted. The exit
        components already accepted this parameter (see FourChainExit) — the
        runtime simply never passed it, so it silently defaulted to None on every
        EXIT ever written. Optional, so a caller/test driving the runtime without
        a price keeps working."""
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
            # Isolate per position, for the same reason as the entry fan-out: a
            # failure evaluating ONE position's exit must not abort the sweep for
            # the others, and must not kill the consume loop.
            #
            # CRITICAL: on failure we CONTINUE, leaving the position TRACKED. The
            # untrack below happens only when maybe_emit_exit RETURNED a key —
            # i.e. the EXIT row was durably written. If the emit raised (say the
            # trade_signals insert failed), the exit condition is still true and
            # will be re-evaluated on this symbol's next event, so the EXIT is
            # retried rather than lost. Untracking on a failed emit would abandon
            # an open position with no EXIT row — precisely the reconciliation gap
            # migration 004 exists to make detectable, and the worst outcome
            # available here (the consumer keeps a position we've stopped
            # watching).
            try:
                exited = await strategy.exit.maybe_emit_exit(
                    symbol=pos.symbol,
                    asset_class=pos.asset_class,
                    chain_prime_id=pos.chain_prime_id,
                    trade_direction=pos.trade_direction,
                    decided_at=decided_at,
                    decision_price=decision_price,
                )
            except Exception:
                log.exception(
                    "exit evaluation FAILED for position=%s (strategy=%s) — "
                    "position REMAINS TRACKED and its exit will be retried on the "
                    "next event for %s",
                    pos.tracking_key, pos.strategy_name, symbol,
                )
                continue

            if exited is not None:
                self._open_by_symbol[symbol].pop(pos.tracking_key, None)
                await self.hooks.remove_open_position(pos.tracking_key)
                log.info("stopped tracking %s (EXIT signalled)", pos.tracking_key)