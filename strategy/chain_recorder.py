"""
strategy/chain_recorder.py — a record-only strategy: owns a ladder, trades nothing.

WHY THIS EXISTS. The router computes chains only for (symbol, ladder) pairs
that some ENABLED strategy owns (MultiLadderRouter's `owns` predicate), so a
ladder being evaluated purely as DATA — chains and signals rows, no
trade_signals — still needs an owner. This is that owner. It folds every
event into a ChainHistoryView (so the view's depth/history bookkeeping is
exercised exactly as a trading strategy would) and never emits.

The dense equity ladder (config.EXTRA_LADDERS["DL"]) runs under this until
its real strategy lands; swapping is a STRATEGIES config change (same
symbols/ladders, different key), not a code change. Nothing here depends on
the ladder's rungs, so it works on any registered ladder.

Counts what it saw (events, chains that reached each level) so /health can
show the ladder is alive and how deep its chains run — the one thing a
record-only owner can usefully report.
"""
from __future__ import annotations

import logging
from collections import Counter
from typing import Optional

from strategy.chain_history import ChainHistoryView

log = logging.getLogger("strategy.chain_recorder")


class ChainRecorderEntry:
    """EntryComponent: folds events into the view, counts, never enters."""

    def __init__(self, view: ChainHistoryView, name: str):
        self._view = view
        self._name = name
        self.events = 0
        # propagation_level -> number of chain_propagated events seen at it.
        # Level 1 is INIT (chain_initiated), counted separately.
        self.propagations: Counter = Counter()
        self.inits = 0
        self.terminations = 0

    async def on_event(self, ev) -> Optional[str]:
        self._view.apply(
            symbol=ev.symbol,
            chain_id=ev.chain_id,
            event_type=ev.event_type,
            direction=ev.direction,
            propagation_level=ev.propagation_level,
            timestamp=ev.timestamp,
        )
        self.events += 1
        if ev.event_type == "chain_initiated":
            self.inits += 1
        elif ev.event_type == "chain_propagated":
            self.propagations[int(ev.propagation_level)] += 1
        elif ev.event_type == "chain_terminated":
            self.terminations += 1
        if self.events % 500 == 0:
            self.log_stats()
        return None   # never a position

    def log_stats(self) -> None:
        depth = sorted(self.propagations.items())
        log.info("chain_recorder %s: events=%d inits=%d terms=%d propagations_by_level=%s",
                 self._name, self.events, self.inits, self.terminations, depth)

    def snapshot(self) -> dict:
        return {
            "events": self.events,
            "inits": self.inits,
            "terminations": self.terminations,
            "propagations_by_level": dict(sorted(self.propagations.items())),
        }


class ChainRecorderExit:
    """ExitComponent: there are never open positions, so never exits."""

    async def maybe_emit_exit(self, **_) -> Optional[str]:
        return None


def make_chain_recorder(*, name, symbols, ladders, params, view_registry, writer_factory):
    """Strategy factory for the registry (same signature as the trading ones;
    `writer_factory` is accepted and unused — this strategy writes nothing)."""
    from strategy.registry import Strategy

    if len(ladders) != 1:
        raise ValueError(
            f"chain_recorder strategy {name!r} must declare exactly one ladder, got {ladders}"
        )
    ladder = view_registry.get(ladders[0])
    cap = params.get("max_chains_per_symbol")
    view = (ChainHistoryView(ladder, max_chains_per_symbol=cap)
            if cap is not None else ChainHistoryView(ladder))
    return Strategy(
        name=name,
        entry=ChainRecorderEntry(view, name),
        exit=ChainRecorderExit(),
        symbols=symbols,
        ladders=ladders,
    )
