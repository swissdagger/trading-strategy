"""
producer/fanout_router_multi.py — generalized bars-in, tagged-events-out router.

The multi-ladder generalization of producer/fanout_router.py. The original router
is hardwired to the two-ladder ChainSet enum (A/B); this one keys machines by
LADDER NAME from the LadderRegistry, so it can run ANY set of ladders — the
two-ladder A/B AND the confirmed_15m FC ladder [1m,3m,5m,15m,1h] — in one process,
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
    ladder name (the confirmed_15m adapter) read `ladder`; the two-ladder ChainView
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
from dataclasses import dataclass, field
from datetime import datetime, timezone
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

# Predicate: does at least one enabled strategy read (symbol, ladder)? Injected
# so the router can skip computing chains no strategy consumes (see on_bar). A
# None predicate means "compute every timeframe-matched ladder" — the original
# behaviour, preserved for callers/tests that don't supply ownership.
OwnsPredicate = Callable[[str, str], bool]


# Ladder names that correspond to the two-ladder ChainSet enum, so we can keep
# populating `set` for them (backward compatibility with the two-ladder view).
_CHAINSET_LADDERS = {"A", "B"}


@dataclass
class RoutedBar:
    """One bar, prepared on every ladder that owns it, awaiting forecast and
    apply. `items` is (machine, ladder_name, pending) in deterministic registry
    order; `pending` is None for a machine that does not implement the split
    (it then runs its whole on_bar inside apply(), serially — the legacy path)."""
    bar: "OHLCPoint"
    items: list = field(default_factory=list)
    # Did any machine APPEND this bar to its buffer (not a duplicate or an
    # out-of-order drop)? Only appended bars go to the bar store.
    appended: bool = False


def _supports_split(machine) -> bool:
    """Does this machine implement the prepare/forecast/apply split? The
    ChainMachine ABC only requires on_bar; the split is an optional refinement
    (ServiceTwoChainMachine has it; test fakes and future simple machines need
    not)."""
    return all(callable(getattr(machine, n, None))
               for n in ("prepare", "forecast", "apply"))


class MultiLadderRouter:
    """Routes closed bars to per-(symbol, ladder) chain machines and publishes
    tagged raw events, for any set of registered ladders."""

    def __init__(
        self,
        registry: LadderRegistry,
        make_machine: MachineFactory,
        publish: PublishRaw,
        locks: Optional[Dict[str, asyncio.Lock]] = None,
        owns: Optional[OwnsPredicate] = None,
        persist_bar: Optional[Callable[["OHLCPoint"], Awaitable[None]]] = None,
    ):
        self._registry = registry
        # The BAR STORE write (warmup_bars): every bar a machine appended, so a
        # restart hydrates buffers up to where this process got instead of to
        # where warmup left them. Written once the bar is fully applied — a bar
        # stored but never applied would hydrate into the buffer and its Kafka
        # replay would then be dropped as a duplicate, never forecast. None in
        # tests / standalone.
        self._persist_bar = persist_bar
        self._make_machine = make_machine
        self._publish = publish
        self._locks: Dict[str, asyncio.Lock] = (
            locks if locks is not None else defaultdict(asyncio.Lock)
        )
        # (symbol, ladder) ownership gate. None => allow all (original behaviour).
        # When set, on_bar only builds/runs machines for pairs a strategy reads,
        # so a symbol-scoped strategy doesn't drag in ladders no one consumes.
        self._owns = owns
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

    # ------------------------------------------------------------------
    # The three-stage path. signal_process's SymbolWorkerPool drives these
    # separately so the Sumtyme call runs outside the lock and concurrently
    # across bars, while prepare/apply stay under the lock in arrival order.
    # on_bar() below composes them serially for callers that don't pipeline.
    # ------------------------------------------------------------------

    def _targets(self, bar: OHLCPoint) -> List[str]:
        targets = self._registry.ladders_for_timeframe(bar.timeframe)
        if self._owns is not None:
            # Drop ladders no enabled strategy reads for this symbol — a
            # symbol-scoped strategy must not make the router compute chains
            # nobody consumes (wasted forecasts + junk chains). With no owner
            # gate this is a no-op (original fan-to-all behaviour).
            targets = [ln for ln in targets if self._owns(bar.symbol, ln)]
        return targets

    async def prepare(self, bar: OHLCPoint) -> Optional[RoutedBar]:
        """STAGE 1. Under the symbol lock: hydrate-if-needed and append the bar
        to every owning ladder's machine, in deterministic ladder order. Fast —
        no API call, no DB write (hydrate aside, once per machine).

        Returns None when no ladder has anything to forecast for this bar
        (unowned timeframe, duplicate/out-of-order, buffers not yet full), so
        the caller can retire the bar immediately.
        """
        targets = self._targets(bar)
        if not targets:
            log.debug("dropping bar %s %s: no owned ladder for this timeframe",
                      bar.symbol, bar.timeframe)
            return None

        routed = RoutedBar(bar=bar)
        async with self._lock(bar.symbol):
            for ladder_name in targets:   # deterministic registry order
                machine = await self._machine_for(bar.symbol, ladder_name)
                if not _supports_split(machine):
                    routed.items.append((machine, ladder_name, None))
                    continue
                try:
                    pending = machine.prepare(bar)
                except Exception:
                    log.exception("chain machine prepare error: symbol=%s ladder=%s tf=%s",
                                  bar.symbol, ladder_name, bar.timeframe)
                    continue
                if getattr(machine, "last_prepare_appended", False):
                    routed.appended = True
                if pending is not None:
                    routed.items.append((machine, ladder_name, pending))
        if routed.items:
            return routed
        # Nothing to forecast (buffers not yet full): the bar is fully handled.
        await self._store(routed)
        return None

    async def _store(self, routed: RoutedBar) -> None:
        if self._persist_bar is None or not routed.appended:
            return
        try:
            await self._persist_bar(routed.bar)
        except Exception:
            log.exception("bar store write failed %s/%s ts=%s — a restart before the "
                          "next warmup will hydrate without this bar",
                          routed.bar.symbol, routed.bar.timeframe, routed.bar.timestamp)

    async def forecast(self, routed: RoutedBar) -> None:
        """STAGE 2. NO lock. Run every prepared ladder's forecast concurrently.
        Two ladders sharing this bar's timeframe share one API call through the
        memo (owner/join). A machine's forecast() never raises for the normal
        failure modes (it leaves pending.forecast None); anything else is
        logged here and the ladder's apply becomes a no-op."""
        coros = [m.forecast(p) for (m, ln, p) in routed.items if p is not None]
        if not coros:
            return
        results = await asyncio.gather(*coros, return_exceptions=True)
        for (m, ln, p), r in zip(
                [it for it in routed.items if it[2] is not None], results):
            if isinstance(r, asyncio.CancelledError):
                raise r
            if isinstance(r, BaseException):
                log.exception("chain machine forecast error: symbol=%s ladder=%s tf=%s",
                              routed.bar.symbol, ln, routed.bar.timeframe,
                              exc_info=r)

    async def apply(self, routed: RoutedBar) -> None:
        """STAGE 3. Under the symbol lock, in deterministic ladder order: apply
        each ladder's forecast (cache + INIT/PROPAGATE/TERMINATE) and publish
        the events. The publish sink runs the strategy runtime under this same
        lock (see signal_process._publish). One machine failing does not poison
        the others; one bad event drops that event, not the pipeline."""
        try:
            await self._apply_locked(routed)
        finally:
            await self._store(routed)

    async def _apply_locked(self, routed: RoutedBar) -> None:
        bar = routed.bar
        async with self._lock(bar.symbol):
            for machine, ladder_name, pending in routed.items:
                try:
                    if pending is None:
                        # Legacy machine (no split): whole on_bar, serially,
                        # under the lock — exactly the pre-split behaviour.
                        events: List[ChainEvent] = await machine.on_bar(bar)
                    else:
                        events = await machine.apply(pending)
                except Exception:
                    log.exception("chain machine error: symbol=%s ladder=%s tf=%s",
                                  bar.symbol, ladder_name, bar.timeframe)
                    continue
                for ce in events:
                    # The publish sink runs the WHOLE consumer side (Kafka send +
                    # the strategy runtime + trade_signals writes). It was outside
                    # the guard above, so anything raising downstream unwound out
                    # of on_bar and killed the consume loop. The runtime now
                    # isolates per-strategy internally; this is the outer backstop
                    # for everything else on the path (Kafka, event shaping, a
                    # consumer we add later). One bad event drops that event, not
                    # the pipeline.
                    try:
                        await self._publish(self._tag(ce, ladder_name))
                    except Exception:
                        log.exception(
                            "publish FAILED: symbol=%s ladder=%s event=%s chain=%s "
                            "— dropping this event (pipeline continues)",
                            bar.symbol, ladder_name, ce.event_type, ce.chain_id,
                        )

    async def on_bar(self, bar: OHLCPoint) -> None:
        """Route ONE closed bar, serially: prepare -> forecast -> apply.

        The original single-call contract, kept for probes, tests and any
        caller that does not pipeline. Semantically identical to the pipelined
        path for one bar; what the pipeline adds is overlapping the forecast of
        bar N with the prepare of bars N+1.. for the same symbol (see
        producer/symbol_workers.py), which this serial form does not do."""
        routed = await self.prepare(bar)
        if routed is None:
            return
        await self.forecast(routed)
        await self.apply(routed)

    def _tag(self, ce: ChainEvent, ladder_name: str) -> dict:
        """Build the raw tagged-event dict. Carries `ladder` (the general name)
        and, for A/B ladders, `set` too (so the two-ladder ChainView's
        from_raw, which requires `set`, keeps working). For non-ChainSet ladders
        `set` is set to the ladder name so from_raw doesn't raise if a consumer
        routes such an event through it — though the confirmed_15m adapter reads the
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
            # The price the strategy SAW when this event fired. ChainEvent has
            # carried it all along ("so the signal's decision_price is available
            # downstream" — chain_machine.py), but it was dropped HERE, so
            # trade_signals.decision_price was NULL on every row ever written.
            # Migration 004 defines that column as what the consuming team measures
            # their slippage/basis against, so dropping it silently broke the
            # cross-team contract. Named decision_price (not price) because that is
            # the field name both strategies' entry/exit components already read.
            "decision_price": ce.price,
            # When this event was DETECTED (wall clock), as opposed to
            # `timestamp` (the closing bar, one timeframe earlier). The
            # strategy package's replay harness needs it to time entries the
            # way live does (its spec §2.4); additive, ignored by everything
            # else.
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }

    def machine_count(self) -> int:
        return sum(len(d) for d in self._machines.values())