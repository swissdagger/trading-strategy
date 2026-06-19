"""
producer/fanout_router.py — Bars in, tagged chain events out.

THE CORE NEW PRODUCER LOGIC. CausaView's chain machine runs ONE ladder per
symbol. The two-ladder design runs TWO (Set A and Set B), and the overlap
timeframes (30s, 1m, 2m) feed BOTH. This router is what makes that happen:

  * It owns one ChainMachine instance per (symbol, set) — two per symbol.
  * For each closed bar (symbol, timeframe), it routes the bar to EVERY set whose
    ladder contains that timeframe (the fan-out): a 1m bar goes to both A and B; a
    5s bar only to A; a 10m bar only to B.
  * It runs each targeted machine's on_bar, TAGS the returned (untagged) events
    with that machine's set, and publishes them as TaggedChainEvents — exactly the
    contract strategy/chain_view.py consumes.

LOCKING. All work for one symbol — both its machines — runs under that symbol's
lock, so the two ladders for a symbol never run concurrently with each other or
with the consumer-side runtime (which shares the same lock map). One bar is fully
processed (fanned out, both machines advanced, all events published) before the
next bar for that symbol begins.

ORDERING. A bar is fed to the machines and the resulting events published in the
order the machines return them. Since a single overlap bar can yield events from
both A and B, they're published A-then-B by set order — deterministic, so a
downstream consumer sees a stable interleaving.

This router does NOT compute chains (the machines do) and does NOT decide trades
(the strategy does). It is pure routing + tagging + the symbol-lock guarantee.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Awaitable, Callable, Dict, List, Optional

from models import OHLCPoint
from producer.chain_machine import ChainEvent, ChainMachine
from producer.ladders import sets_for_timeframe
from strategy.chain_view import ChainSet, TaggedChainEvent

log = logging.getLogger("producer.fanout")


# Factory the app supplies: build a ChainMachine for one (symbol, set). The
# router stays free of the concrete machine's constructor details (DB handles,
# Sumtyme client) — those are bound in the factory at wiring time. This is also
# the seam where the EXTRACTED service_2 implementation gets plugged in.
MachineFactory = Callable[[str, ChainSet], ChainMachine]

# Sink the router publishes tagged events to (Kafka producer, the consumer-side
# runtime's on_event, or both). Async so it can be a real Kafka send.
PublishTagged = Callable[[TaggedChainEvent], Awaitable[None]]


class FanoutRouter:
    """Routes closed bars to per-(symbol, set) chain machines and publishes
    tagged events."""

    def __init__(
        self,
        make_machine: MachineFactory,
        publish: PublishTagged,
        locks: Optional[Dict[str, asyncio.Lock]] = None,
    ):
        self._make_machine = make_machine
        self._publish = publish
        # Shared with the consumer runtime and executor so all work for a symbol
        # is mutually exclusive across producer and consumer sides.
        self._locks: Dict[str, asyncio.Lock] = locks if locks is not None else defaultdict(asyncio.Lock)
        # symbol -> set -> machine. Lazily created on first bar for a symbol.
        self._machines: Dict[str, Dict[ChainSet, ChainMachine]] = defaultdict(dict)
        self._hydrated: set[tuple[str, ChainSet]] = set()

    def _lock(self, symbol: str) -> asyncio.Lock:
        return self._locks[symbol]

    async def _machine_for(self, symbol: str, set_: ChainSet) -> ChainMachine:
        """Get-or-create-and-hydrate the machine for (symbol, set). Hydration
        runs once, before the machine's first bar, so chain state survives a
        restart (service_2's hydrate-on-restart, preserved). Caller holds the
        symbol lock."""
        m = self._machines[symbol].get(set_)
        if m is None:
            m = self._make_machine(symbol, set_)
            self._machines[symbol][set_] = m
        key = (symbol, set_)
        if key not in self._hydrated:
            await m.hydrate()
            self._hydrated.add(key)
        return m

    async def on_bar(self, bar: OHLCPoint) -> None:
        """Route ONE closed bar. Fans out to every set whose ladder contains the
        bar's timeframe, runs each machine, tags and publishes the events. All
        under the symbol lock."""
        targets = sets_for_timeframe(bar.timeframe)
        if not targets:
            # Timeframe belongs to no ladder — not part of the two-ladder design.
            # Drop it (a feed producing a timeframe neither set uses).
            log.debug("dropping bar %s %s: timeframe in no ladder",
                      bar.symbol, bar.timeframe)
            return

        async with self._lock(bar.symbol):
            for set_ in targets:   # deterministic order (A before B)
                machine = await self._machine_for(bar.symbol, set_)
                try:
                    events: List[ChainEvent] = await machine.on_bar(bar)
                except Exception:
                    # One machine failing on a bar must not poison the other set's
                    # processing of the same bar. Log and continue; the chain
                    # machine's own persistence/recovery handles its state.
                    log.exception("chain machine error: symbol=%s set=%s tf=%s",
                                  bar.symbol, set_.value, bar.timeframe)
                    continue
                for ce in events:
                    await self._publish(self._tag(ce, set_))

    def _tag(self, ce: ChainEvent, set_: ChainSet) -> TaggedChainEvent:
        """Apply the set tag, producing the consumer-contract event. The machine
        emits untagged events (it runs one ladder, oblivious to sets); the router
        knows which set's machine produced it and stamps it here."""
        return TaggedChainEvent(
            set=set_,
            event_type=ce.event_type,
            chain_id=ce.chain_id,
            symbol=ce.symbol,
            asset_class=ce.asset_class,
            direction=ce.direction,
            propagation_level=ce.propagation_level,
            timestamp=ce.timestamp,
        )

    # Introspection for wiring/health checks.
    def machine_count(self) -> int:
        return sum(len(d) for d in self._machines.values())
