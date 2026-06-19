"""
strategy/two_ladder_strategy.py — The entry/exit logic, wired to SignalWriter.

Implements the specified strategy over the two parallel chain ladders (Set A:
5s,10s,30s,1m,2m — Set B: 30s,1m,2m,5m,10m), driving the SignalWriter so each
decision becomes a trade_signals row.

ENTRY (evaluated on every Set A 'reached 2m' event):
  Step 1  A Set B chain (chain_prime) must exist for the symbol.
  Step 2  A Set A chain has just reached 2m (this is the triggering event). Call
          its direction the TRADE DIRECTION.
  Step 2b If ANY opposing-direction Set A chain exists at all (any level), ABORT
          this attempt (re-evaluate later; chain_prime is not permanently
          disqualified).
  Step 3  chain_prime must be in the SAME direction as the Set A 2m trigger. If a
          same-direction chain_prime exists -> emit ENTER.

EXIT (evaluated continuously while open) — fire on EITHER:
  * chain_prime reaches 10m (top of Set B), OR
  * any opposing Set B chain reaches 2m+.

Both questions need state beyond a single event, so the logic reads ChainView.
chain_prime's identity and the trade direction are stashed in exit_state.rule_state
at entry so the exit rule resumes faithfully after a restart (resume-management).

All evaluation is serialized by the caller under the executor's per-symbol lock
(the ChainView is mutated by events on the same lock), so this code holds no lock.
"""
from __future__ import annotations

import logging
from typing import Optional

from execution.signal_writer import SignalWriter
from execution.types import ExitReason, TradeSide
from strategy.chain_view import (
    A_LEVEL_2M,
    B_LEVEL_2M,
    B_LEVEL_10M,
    ChainSet,
    ChainView,
    TaggedChainEvent,
)

log = logging.getLogger("strategy.two_ladder")


def _side(direction: int) -> TradeSide:
    return TradeSide.LONG if direction > 0 else TradeSide.SHORT


class TwoLadderEntry:
    """Entry logic. Call on_event for every tagged chain event; it self-filters
    to the Set A 'reached 2m' trigger and runs the four steps."""

    def __init__(self, view: ChainView, writer: SignalWriter):
        self.view = view
        self.writer = writer

    async def on_event(self, ev: TaggedChainEvent) -> Optional[str]:
        """Process one tagged event. Returns the position_key if an ENTER was
        emitted, else None. The ChainView must already have `ev` applied (caller
        folds the event into the view, THEN calls this, so the trigger chain's
        own 2m level is visible)."""
        # Trigger only on a Set A chain reaching 2m (initiate or propagate that
        # lands at level >= 2m). Terminations and other sets are not triggers.
        if ev.set is not ChainSet.A:
            return None
        if ev.event_type not in ("chain_initiated", "chain_propagated"):
            return None
        if ev.propagation_level < A_LEVEL_2M:
            return None

        symbol = ev.symbol
        trade_direction = ev.direction          # Step 2: trade dir = the 2m trigger's dir

        # Step 1: chain_prime (a Set B chain) must exist for this symbol.
        if not self.view.prime_chains(symbol):
            return None

        # Step 2b: veto if there is ANY opposing-direction Set A chain at all
        # (active, any level — not just 30s+). Broader veto: any live opposing
        # structural activity in Set A disqualifies this attempt. Terminated
        # chains don't count (the view holds only active chains).
        opposing = self.view.chains_reaching(
            symbol, ChainSet.A, level=1, direction=-trade_direction)
        if opposing:
            log.debug("entry abort %s: opposing Set A chain present (any level)", symbol)
            return None   # abort this attempt; re-evaluate on later events

        # Step 3: chain_prime in the SAME direction as the Set A trigger?
        same_dir_prime = self.view.prime_chains(symbol, direction=trade_direction)
        if not same_dir_prime:
            return None

        # All steps pass -> ENTER. The position is keyed on chain_prime's chain_id
        # + side: chain_prime is the position's spine (exit watches it reach 10m),
        # so the position identity follows chain_prime, not the Set A trigger.
        prime = same_dir_prime[0]
        side = _side(trade_direction)
        pkey = await self.writer.emit_entry(
            chain_id=prime.chain_id,
            side=side,
            symbol=symbol,
            asset_class=ev.asset_class,
            decided_at=ev.timestamp,
            trigger_level=ev.propagation_level,
        )
        log.info("ENTER %s %s: prime=%s trigger(setA 2m)=%s",
                 symbol, side.value, prime.chain_id, ev.chain_id)
        return pkey


class TwoLadderExit:
    """Exit logic. Evaluated continuously for each open position; emits an EXIT
    signal when EITHER condition holds. The position carries (chain_prime_id,
    trade_direction) — passed in from the entry context (persisted in
    exit_state.rule_state for resume)."""

    def __init__(self, view: ChainView, writer: SignalWriter):
        self.view = view
        self.writer = writer

    async def evaluate(
        self,
        *,
        symbol: str,
        asset_class: str,
        chain_prime_id: str,
        trade_direction: int,
        decided_at: str,
    ) -> Optional[ExitReason]:
        """Return the ExitReason if the position should exit now, else None.
        Caller emits the EXIT signal (or this can emit directly — see
        maybe_emit_exit). Pure read of the view."""
        side = _side(trade_direction)

        # Condition 1: chain_prime reaches 10m (top of Set B). chain_prime may
        # have terminated — if it's gone from the view, it cannot reach 10m, so
        # this condition simply doesn't fire on a vanished prime.
        prime = self.view.get_chain(symbol, ChainSet.B, chain_prime_id)
        if prime is not None and prime.peak_level >= B_LEVEL_10M:
            return ExitReason.TARGET   # reached the top of its trajectory

        # Condition 2: any OPPOSING Set B chain reaches 2m+ (note: 2m is level 3
        # in Set B's ladder [30s,1m,2m,5m,10m], NOT level 5 as in Set A — the
        # exit checks Set B's 2m, so it uses B_LEVEL_2M).
        if self.view.has_opposing_at(symbol, ChainSet.B, B_LEVEL_2M, trade_direction):
            return ExitReason.STOP     # opposing structural move confirmed deep

        return None

    async def maybe_emit_exit(
        self,
        *,
        symbol: str,
        asset_class: str,
        chain_prime_id: str,
        trade_direction: int,
        decided_at: str,
    ) -> Optional[str]:
        """Evaluate and, if exiting, emit the EXIT signal referencing the
        position's key. Returns the position_key if an exit was emitted, else
        None."""
        reason = await self.evaluate(
            symbol=symbol, asset_class=asset_class, chain_prime_id=chain_prime_id,
            trade_direction=trade_direction, decided_at=decided_at,
        )
        if reason is None:
            return None
        side = _side(trade_direction)
        pkey = await self.writer.emit_exit(
            chain_id=chain_prime_id,
            side=side,
            symbol=symbol,
            asset_class=asset_class,
            reason=reason,
            decided_at=decided_at,
        )
        log.info("EXIT %s %s reason=%s prime=%s",
                 symbol, side.value, reason.value, chain_prime_id)
        return pkey
