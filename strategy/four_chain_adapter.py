"""
strategy/four_chain_adapter.py — bridges the strategy to the
registry's component protocol and feeds its private ChainHistoryView.

The internal logic executes the 2-chain markdown rules, but the interface 
retains the 'four_chain' naming convention to prevent upstream refactoring.
"""
from __future__ import annotations

from typing import Optional

from strategy.chain_history import ChainHistoryView
from strategy.four_chain_strategy import FourChainEntry, FourChainExit


class FourChainEntryAdapter:
    """EntryComponent for the registry. Folds each event into the shared
    ChainHistoryView, then runs the entry check."""

    def __init__(self, view: ChainHistoryView, entry: FourChainEntry):
        self._view = view
        self._entry = entry

    async def on_event(self, ev) -> Optional[str]:
        self._view.apply(
            symbol=ev.symbol,
            chain_id=ev.chain_id,
            event_type=ev.event_type,
            direction=ev.direction,
            propagation_level=ev.propagation_level,
            timestamp=ev.timestamp,
        )
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
    """ExitComponent for the registry. Does NOT fold the event; just evaluates 
    exit against the shared history view."""

    def __init__(self, exit_: FourChainExit):
        self._exit = exit_

    async def maybe_emit_exit(
        self, *, symbol: str, asset_class: str, chain_prime_id: str,
        trade_direction: int, decided_at: str,
        decision_price: Optional[float] = None,
    ) -> Optional[str]:
        return await self._exit.maybe_emit_exit(
            symbol=symbol,
            asset_class=asset_class,
            chain_prime_id=chain_prime_id,
            trade_direction=trade_direction,
            decided_at=decided_at,
            decision_price=decision_price,
        )


def make_four_chain(*, name, symbols, ladders, params, view_registry, writer_factory):
    """Strategy factory for the registry."""
    from strategy.registry import Strategy

    if len(ladders) != 1:
        raise ValueError(
            f"four_chain strategy {name!r} must declare exactly one ladder, got {ladders}"
        )
    ladder = view_registry.get(ladders[0])
    cap = params.get("max_chains_per_symbol")
    history = (
        ChainHistoryView(ladder, max_chains_per_symbol=cap)
        if cap is not None
        else ChainHistoryView(ladder)
    )
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