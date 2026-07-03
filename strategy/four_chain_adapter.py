"""
strategy/four_chain_adapter.py — bridges the four-chain strategy to the
registry's component protocol and feeds its private ChainHistoryView.

The MultiStrategyRuntime folds each event into the SHARED two-ladder ChainView
and then calls each strategy's entry.on_event(ev) / exit.maybe_emit_exit(...).
But the four-chain strategy:
  * reads from its OWN ChainHistoryView (retains terminated chains + per-tf
    timestamps), not the two-ladder ChainView;
  * needs that history fed the raw event BEFORE its on_event runs.

So this module provides thin adapters that:
  1. own the ChainHistoryView,
  2. fold each incoming event into it (entry side, once per event),
  3. translate the runtime's TaggedChainEvent into the four-chain components'
     keyword call.

This keeps the four-chain strategy fully decoupled from the two-ladder view
while satisfying the EntryComponent / ExitComponent protocols the registry and
runtime expect. The history view is SHARED between the entry and exit adapters
(one view per (symbol-space) strategy instance), so both see the same chains.

## Event shape

The runtime passes a TaggedChainEvent-like object with: symbol, asset_class,
chain_id, event_type, direction, propagation_level, timestamp. (This strategy
ignores the A/B `set` tag — it's on its own ladder.) The entry adapter folds
every event into the history; the exit adapter does not fold (the entry adapter
already did for this event, under the same per-symbol lock).
"""
from __future__ import annotations

from typing import Optional

from strategy.chain_history import ChainHistoryView
from strategy.four_chain_strategy import FourChainEntry, FourChainExit


class FourChainEntryAdapter:
    """EntryComponent for the registry. Folds each event into the shared
    ChainHistoryView, then runs the four-chain entry check."""

    def __init__(self, view: ChainHistoryView, entry: FourChainEntry):
        self._view = view
        self._entry = entry

    async def on_event(self, ev) -> Optional[str]:
        # Fold into the history FIRST (this strategy's view is separate from the
        # runtime's two-ladder view, so the runtime's apply didn't touch it).
        self._view.apply(
            symbol=ev.symbol,
            chain_id=ev.chain_id,
            event_type=ev.event_type,
            direction=ev.direction,
            propagation_level=ev.propagation_level,
            timestamp=ev.timestamp,
        )
        # Then run the entry check on the freshly-updated history.
        return await self._entry.on_event(
            symbol=ev.symbol,
            asset_class=ev.asset_class,
            chain_id=ev.chain_id,
            event_type=ev.event_type,
            direction=ev.direction,
            propagation_level=ev.propagation_level,
            timestamp=ev.timestamp,
            decision_price=getattr(ev, "decision_price", None),
        )


class FourChainExitAdapter:
    """ExitComponent for the registry. Does NOT fold the event (the entry
    adapter already did, under the same per-symbol lock); just evaluates exit
    against the shared history view."""

    def __init__(self, exit_: FourChainExit):
        self._exit = exit_

    async def maybe_emit_exit(
        self, *, symbol: str, asset_class: str, chain_prime_id: str,
        trade_direction: int, decided_at: str,
    ) -> Optional[str]:
        return await self._exit.maybe_emit_exit(
            symbol=symbol,
            asset_class=asset_class,
            chain_prime_id=chain_prime_id,
            trade_direction=trade_direction,
            decided_at=decided_at,
        )


def make_four_chain(*, name, symbols, ladders, params, view_registry, writer_factory):
    """Strategy factory for the registry.

    Builds the four-chain strategy: its own ChainHistoryView on the ladder it
    declares (must be exactly one — this is a single-ladder strategy), a
    SignalWriter bound to `name`, and the entry/exit adapters.

    view_registry: the LadderRegistry, to resolve the ladder by name.
    writer_factory: callable(strategy_name) -> SignalWriter, so each strategy
                    gets a writer tagged with its own name.
    params may carry 'retention_hours' to tune history retention.
    """
    from datetime import timedelta
    from strategy.registry import Strategy

    if len(ladders) != 1:
        raise ValueError(
            f"four_chain strategy {name!r} must declare exactly one ladder, got {ladders}"
        )
    ladder = view_registry.get(ladders[0])
    retention = timedelta(hours=params.get("retention_hours", 3))
    history = ChainHistoryView(ladder, retention=retention)
    writer = writer_factory(name)

    entry = FourChainEntry(history, ladder, writer)
    exit_ = FourChainExit(history, ladder, writer)
    return Strategy(
        name=name,
        entry=FourChainEntryAdapter(history, entry),
        exit=FourChainExitAdapter(exit_),
        symbols=symbols,
        ladders=ladders,
    )
