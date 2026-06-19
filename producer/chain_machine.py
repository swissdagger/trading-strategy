"""
producer/chain_machine.py — The chain-machine INTERFACE (the extraction seam).

The two-ladder producer runs ONE chain state machine instance per (symbol, set).
That state machine is the subtle INIT -> PROPAGATE -> TERMINATE logic that today
lives in CausaView's service_2_sumtyme_bridge.py. We do NOT reimplement it here —
we specify the INTERFACE it must satisfy, so the real implementation can be
extracted and dropped in behind this seam mechanically, WITHOUT the fan-out
router (or the rest of the producer) needing to change.

WHY AN INTERFACE, NOT A REIMPLEMENTATION. service_2's state machine carries
hard-won invariants (the per-symbol asyncio.Lock around active_chains, the
>= WINDOW_SIZE buffer check, the asyncio.to_thread Sumtyme call, hydrate-on-
restart, the price-on-every-history-step rule). Reconstructing it from fragments
would risk silently dropping one. So this file pins the CONTRACT; the
implementation is supplied later by extracting the real code.

THE CONTRACT (what any ChainMachine implementation must guarantee):

  * ONE INSTANCE PER (symbol, set). Each instance owns the chain state for one
    symbol on one ladder. It knows its ladder (the ordered timeframes) and its
    baseline timeframe (level 1).

  * on_bar(bar) FEEDS ONE CLOSED BAR. The router calls this with a closed OHLC
    bar whose timeframe is in THIS instance's ladder. The instance appends to the
    right per-timeframe buffer, runs its forecast when the buffer is ready, and
    advances its chain state. It RETURNS the chain events that resulted (zero or
    more) as ChainEvent objects — initiate / propagate / terminate. The router
    tags these with the set and publishes them.

  * EVENTS ARE UNTAGGED HERE. A ChainMachine instance doesn't know about "sets" —
    it just runs one ladder. The SET TAG is applied by the router (which knows
    which instance it called). This keeps the state machine identical to the
    single-ladder original; "set" is a producer-side concept layered on top.

  * PER-SYMBOL LOCKING IS THE CALLER'S JOB ACROSS SETS. A single symbol has TWO
    instances (set A and set B). The router serializes a symbol's bars under the
    per-symbol lock so the two instances for that symbol don't run concurrently
    with each other or with anything else touching that symbol. Within an
    instance, its own internal locking (inherited from service_2) still applies.

  * HYDRATE/PERSIST. Implementations hydrate chain state from the DB on
    construction (chain survival across restarts) and persist on mutation — same
    as service_2. The interface exposes hydrate() for the router to await at
    startup before feeding live bars.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

from producer.bar import OHLCPoint


@dataclass(frozen=True)
class ChainEvent:
    """An UNTAGGED chain event returned by a ChainMachine. The router adds the
    set tag to produce the TaggedChainEvent the consumer contract expects. Fields
    mirror what the consumer needs (chain_view.TaggedChainEvent) minus `set`.

    direction is -1 | +1 (never 0). propagation_level is 1-based within this
    machine's ladder. price is the canonical price at this step (origin price for
    initiate; the propagating bar's price for propagate; last price for
    terminate) — carried so the signal's decision_price is available downstream.
    """
    event_type: str            # 'chain_initiated' | 'chain_propagated' | 'chain_terminated'
    chain_id: str
    symbol: str
    asset_class: str
    direction: int
    propagation_level: int
    timeframe: str             # the timeframe this event occurred at
    timestamp: str             # ISO; the closing bar's time
    price: Optional[float] = None


class ChainMachine(ABC):
    """One chain state machine for one (symbol, ladder). The real implementation
    is extracted from service_2_sumtyme_bridge.py and dropped in behind this
    interface. Constructor (impl-defined) receives at least: symbol, asset_class,
    the ordered ladder timeframes, the baseline timeframe, and whatever DB handle
    it needs to hydrate/persist.
    """

    # Implementations set these so the router can introspect without reaching
    # into internals.
    symbol: str = ""
    asset_class: str = ""
    ladder: List[str] = []          # ordered timeframes, ascending
    baseline_timeframe: str = ""    # ladder[0]

    @abstractmethod
    async def hydrate(self) -> None:
        """Load chain state for this (symbol, ladder) from the DB so chains
        survive restarts. Awaited once at startup BEFORE any on_bar. Same
        hydrate-on-restart behavior as service_2."""

    @abstractmethod
    async def on_bar(self, bar: OHLCPoint) -> List[ChainEvent]:
        """Feed ONE closed bar whose timeframe is in this machine's ladder.
        Appends to the right per-timeframe buffer, runs the Sumtyme forecast when
        ready (via asyncio.to_thread, never blocking the loop), advances chain
        state through INIT/PROPAGATE/TERMINATE, persists mutations, and returns
        the resulting chain events (possibly empty).

        MUST be safe to call repeatedly; the buffer/forecast/state-advance
        mechanics and their invariants are service_2's, unchanged. The router
        guarantees bars for one symbol are delivered serially (under the
        per-symbol lock), so an implementation need not guard against concurrent
        on_bar for the same symbol from the router — but its own internal
        per-symbol lock from service_2 remains as defense in depth.
        """

    def handles_timeframe(self, timeframe: str) -> bool:
        """True if `timeframe` is in this machine's ladder. The router checks this
        before feeding a bar (belt-and-braces; the router already routes by
        ladder membership)."""
        return timeframe in self.ladder
