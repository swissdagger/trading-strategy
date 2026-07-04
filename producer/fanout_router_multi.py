"""
producer/fanout_router_multi.py — generalized bars-in, tagged-events-out router.

The multi-ladder generalization of producer/fanout_router.py. The original router
is hardwired to the two-ladder ChainSet enum (A/B); this one keys machines by
LADDER NAME from the LadderRegistry, so it can run ANY set of ladders — the
two-ladder A/B AND the four-chain FC ladder [1m,3m,5m,15m,1h] — in one process,
computing each ladder's chains once (compute-once: a ladder shared by strategies
runs a single machine per symbol).

WHAT CARRIES OVER UNCHANGED (the original router's guarantees):
  * one ChainMachine per (symbol, ladder), lazily created + hydrated once;
  * for each closed bar, fan out to EVERY ladder whose timeframes contain the
    bar's timeframe (registry.ladders_for_timeframe);
  * run each machine, tag events with the producing ladder, publish;
  * ALL work for a symbol under that symbol's lock (shared with the consumer
    runtime + executor), so producer and consumer never interleave for a symbol;
  * deterministic ladder order for stable interleaving;
  * one machine failing on a bar does not poison the others.

WHAT'S GENERALIZED:
  * MachineFactory is (symbol, ladder_name, ladder_timeframes) -> ChainMachine,
    not (symbol, ChainSet). The factory builds the right machine for any ladder.
  * Tagging: events carry the LADDER NAME. For backward compatibility with the
    two-ladder ChainView (which consumes a ChainSet `set` tag), when the ladder
    name is a known ChainSet value ("A"/"B") we ALSO populate `set`; for other
    ladders `set` is left as the ladder name string. Consumers that key on the
    ladder name (the four-chain adapter) read `ladder`; the two-ladder ChainView
    reads `set` and only ever sees A/B events (its strategies only subscribe to
    A/B), so it is unaffected.

## The tagged event shape

TaggedChainEvent already has `set`. We add `ladder` (the ladder name) via a thin
wrapper dict at publish time — the wire/raw form carries both `set` and `ladder`,
and TaggedChainEvent.from_raw reads `set`. The multi-runtime's strategies get the
raw event through their adapters and can read `ladder`. This keeps the existing
consumer contract intact while adding the ladder name for new consumers.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Awaitable, Callable, Dict, List, Optional

from strategy.ladder_registry import LadderRegistry

if TYPE_CHECKING:
    from producer.bar import OHLCPoint
    from producer.chain_machine import ChainEvent, ChainMachine

log = logging.getLogger("producer.fanout_multi")


# Factory: build a ChainMachine for one (symbol, ladder_name, ladder_timeframes).
# Generalized from the original (symbol, ChainSet) so any ladder can be built.
MachineFactory = Callable[[str, str, List[str]], "ChainMachine"]

# Sink the router publishes RAW tagged-event dicts to. Raw (not TaggedChainEvent)
# so the dict can carry both `set` (back-comp) and `ladder` (the general name),
# and so the same publish can go to Kafka (needs a dict) and the runtime.
PublishRaw = Callable[[dict], Awaitable[None]]


# Ladder names that correspond to the two-ladder ChainSet enum, so we can keep
# populating `set` for them (backward compatibility with the two-ladder view).
_CHAINSET_LADDERS = {"A", "B"}


class MultiLadderRouter:
    """Routes closed bars to per-(symbol, ladder) chain machines and publishes
    tagged raw events, for any set of registered ladders."""

    def __init__(
        self,
        registry: LadderRegistry,
        make_machine: MachineFactory,
        publish: PublishRaw,
        locks: Optional[Dict[str, asyncio.Lock]] = None,
    ):
        self._registry = registry
        self._make_machine = make_machine
        self._publish = publish
        self._locks: Dict[str, asyncio.Lock] = (
            locks if locks is not None else defaultdict(asyncio.Lock)
        )
        # symbol -> ladder_name -> machine. Lazily created on first bar.
        self._machines: Dict[str, Dict[str, ChainMachine]] = defaultdict(dict)
        self._hydrated: set[tuple[str, str]] = set()

    def _lock(self, symbol: str) -> asyncio.Lock:
        return self._locks[symbol]

    async def _machine_for(self, symbol: str, ladder_name: str) -> ChainMachine:
        """Get-or-create-and-hydrate the machine for (symbol, ladder). Hydration
        runs once before the first bar so chain state survives a restart. Caller
        holds the symbol lock."""
        m = self._machines[symbol].get(ladder_name)
        if m is None:
            timeframes = list(self._registry.get(ladder_name).timeframes)
            m = self._make_machine(symbol, ladder_name, timeframes)
            self._machines[symbol][ladder_name] = m
        key = (symbol, ladder_name)
        if key not in self._hydrated:
            await m.hydrate()
            self._hydrated.add(key)
        return m

    async def on_bar(self, bar: OHLCPoint) -> None:
        """Route ONE closed bar to every ladder whose timeframes contain the
        bar's timeframe. All under the symbol lock."""
        targets = self._registry.ladders_for_timeframe(bar.timeframe)
        if not targets:
            log.debug("dropping bar %s %s: timeframe in no registered ladder",
                      bar.symbol, bar.timeframe)
            return

        async with self._lock(bar.symbol):
            for ladder_name in targets:   # deterministic registry order
                machine = await self._machine_for(bar.symbol, ladder_name)
                try:
                    events: List[ChainEvent] = await machine.on_bar(bar)
                except Exception:
                    log.exception("chain machine error: symbol=%s ladder=%s tf=%s",
                                  bar.symbol, ladder_name, bar.timeframe)
                    continue
                for ce in events:
                    await self._publish(self._tag(ce, ladder_name))

    def _tag(self, ce: ChainEvent, ladder_name: str) -> dict:
        """Build the raw tagged-event dict. Carries `ladder` (the general name)
        and, for A/B ladders, `set` too (so the two-ladder ChainView's
        from_raw, which requires `set`, keeps working). For non-ChainSet ladders
        `set` is set to the ladder name so from_raw doesn't raise if a consumer
        routes such an event through it — though the four-chain adapter reads the
        raw dict directly and doesn't require `set`."""
        set_tag = ladder_name  # for A/B this equals the ChainSet value ("A"/"B")
        return {
            "set": set_tag,
            "ladder": ladder_name,
            "event_type": ce.event_type,
            "chain_id": ce.chain_id,
            "symbol": ce.symbol,
            "asset_class": ce.asset_class,
            "direction": ce.direction,
            "propagation_level": ce.propagation_level,
            "metadata": {"propagation_level": ce.propagation_level},
            "timestamp": ce.timestamp,
        }

    def machine_count(self) -> int:
        return sum(len(d) for d in self._machines.values())