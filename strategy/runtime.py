"""
strategy/runtime.py — Consumer-side runtime assembly.

Closes the signal-producing loop: tagged chain events in, trade_signals rows out.
This is the orchestration layer that enforces the ordering and locking the
strategy logic depends on but deliberately does NOT do itself.

WHAT THIS OWNS (and why it must):

  1. ORDERING. For each event: fold it into the ChainView FIRST, then run the
     strategy. The entry trigger must see the triggering chain's own 2m level
     already in the view, and the exit must see the latest opposing-chain state.
     Strategy code assumes this ordering; the runtime guarantees it.

  2. PER-SYMBOL LOCK. The ChainView is mutated by events and read by strategy
     evaluation; both happen per symbol. A single lock map serializes all work
     for a symbol — the SAME discipline as the executor (and, when the position
     manager is switched on, the SAME lock objects, so signal-side and
     execution-side work on one symbol can't interleave). For the signal-only
     world this map can be standalone; the wiring note below covers sharing it.

  3. OPEN-POSITION TRACKING FOR EXITS. Entries are event-triggered, but exits
     must be evaluated CONTINUOUSLY against the view (an opposing chain reaching
     2m, or chain_prime reaching 10m, can happen on any later event). So the
     runtime keeps the set of open positions and, after applying each event,
     re-evaluates the exit for every open position ON THAT SYMBOL. Position
     context (chain_prime_id, trade_direction) is what the exit needs and what a
     restart must restore — see the persistence note.

THIS IS SIGNAL-PRODUCING ONLY. It emits ENTER/EXIT signals. It does not size,
execute, or manage positions — that's the other team's script now, and this
project's position manager later. "Open position" here means "a position our
STRATEGY believes is open for exit-evaluation purposes," tracked from our own
ENTER/EXIT signals — NOT a venue position.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Optional

from strategy.chain_view import ChainSet, ChainView, TaggedChainEvent
from strategy.two_ladder_strategy import TwoLadderEntry, TwoLadderExit

log = logging.getLogger("strategy.runtime")


@dataclass
class OpenPosition:
    """A position our strategy considers open, for exit evaluation. Keyed by
    position_key. Carries exactly what the exit logic needs — and exactly what a
    restart must restore from persistence to resume exit evaluation faithfully."""
    position_key: str
    symbol: str
    asset_class: str
    chain_prime_id: str
    trade_direction: int


# Persistence hooks: the runtime's open-position set must survive a restart, or
# we'd stop watching exits for positions opened before the crash. These persist/
# load the OpenPosition set. (For the signal-only deliverable this can be a small
# table; when the position manager is on, exit_state.rule_state already holds
# chain_prime_id + trade_direction and can be the source of truth instead.)
@dataclass
class RuntimeHooks:
    load_open_positions: Callable[[], Awaitable[list[OpenPosition]]]
    save_open_position: Callable[[OpenPosition], Awaitable[None]]
    remove_open_position: Callable[[str], Awaitable[None]]   # by position_key
    # Optional: load every active chain (all symbols, both sets) to hydrate the
    # ChainView at startup — see chain_view.ChainView.seed_active and
    # producer.persistence.make_load_chain_view_seed. Optional (defaults to
    # None) so existing callers / tests that don't wire DB-backed chain
    # hydration keep working unchanged; start() simply skips seeding if absent.
    load_chain_view_seed: Optional[Callable[[], Awaitable[list[dict]]]] = None


class StrategyRuntime:
    """Drives the consumer side. One instance; one event ingress (on_raw_event)."""

    def __init__(
        self,
        view: ChainView,
        entry: TwoLadderEntry,
        exit_rule: TwoLadderExit,
        hooks: RuntimeHooks,
        locks: Optional[Dict[str, asyncio.Lock]] = None,
    ):
        self.view = view
        self.entry = entry
        self.exit_rule = exit_rule
        self.hooks = hooks
        # Standalone lock map by default; pass the executor's map in to share
        # locks when the position manager is switched on.
        self._locks: Dict[str, asyncio.Lock] = locks if locks is not None else defaultdict(asyncio.Lock)
        # Open positions indexed for O(1) per-symbol exit sweeps.
        self._open_by_symbol: Dict[str, Dict[str, OpenPosition]] = defaultdict(dict)

    def _lock(self, symbol: str) -> asyncio.Lock:
        return self._locks[symbol]

    async def start(self) -> None:
        """Restore open positions from persistence so exit evaluation resumes for
        anything opened before a restart. Call once before consuming events."""
        for p in await self.hooks.load_open_positions():
            self._open_by_symbol[p.symbol][p.position_key] = p
        n = sum(len(v) for v in self._open_by_symbol.values())
        log.info("runtime restored %d open positions for exit evaluation", n)

        # Hydrate the ChainView itself, separately from open positions. Without
        # this, every restart resets the strategy's view of which chains are
        # active to empty — even though both the DB and the producer-side
        # machine still know — and entry/exit decisions would only ever see
        # chains that happen to emit a FRESH event after this restart. See
        # ChainView.seed_active for why this can't be reconstructed from events.
        if self.hooks.load_chain_view_seed is not None:
            seeds = await self.hooks.load_chain_view_seed()
            for s in seeds:
                self.view.seed_active(
                    symbol=s["symbol"], set_=ChainSet(s["set"]),
                    chain_id=s["chain_id"], direction=s["direction"],
                    peak_level=s["peak_level"],
                )
            log.info("runtime seeded ChainView with %d active chains from DB", len(seeds))

    async def on_event(self, ev: TaggedChainEvent) -> None:
        """Single ingress for an already-built tagged event when the lock is NOT held."""
        async with self._lock(ev.symbol):
            await self._on_event_locked(ev)

    async def _on_event_locked(self, ev: TaggedChainEvent) -> None:
        """Processes the event assuming the caller ALREADY holds the symbol lock."""
        # 1. Fold into the view FIRST so strategy sees current state.
        self.view.apply(ev)
        # 2. Entry: only the Set A 2m trigger acts; on_event self-filters.
        await self._try_entry(ev)
        # 3. Exit sweep: re-evaluate every open position on this symbol against
        #    the freshly-updated view.
        await self._sweep_exits(ev.symbol, ev.timestamp)

    async def on_raw_event(self, raw: dict) -> None:
        """Ingress for a raw causal-events dict (e.g. from Kafka)."""
        ev = TaggedChainEvent.from_raw(raw)   # raises on missing 'set' tag
        await self.on_event(ev)

    async def _try_entry(self, ev: TaggedChainEvent) -> None:
        pkey = await self.entry.on_event(ev)
        if pkey is None:
            return
        # An ENTER was emitted. Record the open position for exit evaluation. The
        # entry keyed the position on chain_prime; recover chain_prime_id and
        # direction from the position_key (chain_id:L/S).
        chain_prime_id, dir_code = pkey.rsplit(":", 1)
        pos = OpenPosition(
            position_key=pkey, symbol=ev.symbol, asset_class=ev.asset_class,
            chain_prime_id=chain_prime_id,
            trade_direction=1 if dir_code == "L" else -1,
        )
        # Idempotency: if we already track this position (a re-fired trigger that
        # the signal layer deduped), don't double-track.
        if pkey in self._open_by_symbol[ev.symbol]:
            return
        self._open_by_symbol[ev.symbol][pkey] = pos
        await self.hooks.save_open_position(pos)
        log.info("runtime now tracking open position %s for exit", pkey)

    async def _sweep_exits(self, symbol: str, decided_at: str) -> None:
        """Evaluate exit for every open position on this symbol. Emitting an EXIT
        signal closes our strategy-side tracking of that position."""
        positions = list(self._open_by_symbol.get(symbol, {}).values())
        for pos in positions:
            exited = await self.exit_rule.maybe_emit_exit(
                symbol=pos.symbol, asset_class=pos.asset_class,
                chain_prime_id=pos.chain_prime_id,
                trade_direction=pos.trade_direction, decided_at=decided_at,
            )
            if exited is not None:
                # Stop tracking; the EXIT signal is emitted. (The consumer of our
                # signals handles the actual position close; we only stop watching
                # for further exit conditions on a position we've signalled out.)
                self._open_by_symbol[symbol].pop(pos.position_key, None)
                await self.hooks.remove_open_position(pos.position_key)
                log.info("runtime stopped tracking %s (EXIT signalled)", pos.position_key)