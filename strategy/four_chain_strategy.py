"""
strategy/four_chain_strategy.py — the four-chain confirmation strategy.

A SEPARATE strategy (distinct from the two-ladder A/B design) on its own single
ladder [1m, 3m, 5m, 15m, 1h] (levels 1..5). Entry/exit components match the
registry's Strategy protocol, so it plugs into the multi-strategy runtime like
any other strategy.

Built from the PROSE specification (the research script is a backtest
approximation whose id-gap arithmetic is a CSV artifact that doesn't translate
to live events; where the prose is vague the script's concrete choices are used
to disambiguate — e.g. "reached 15m" = peak level >= 4, and the 3m-after-15m
ordering).

## The pattern (prose)

INITIATION
  * Two separate chains that BOTH propagate to 15m (level 4) in the SAME
    direction d.
  * BETWEEN them, an opposing chain fails to propagate (starts on 1m, terminates
    before reaching 3m).
  * Chain 2's 15m timestamp >= Chain 1's 15m timestamp.

CONFIRMATION (the trade trigger)
  * A new opposing chain triggers on 1m and fails before 3m (part of the noisy
    backdrop; not separately required as a trigger — see note below).
  * A fourth chain, direction d, reaches 3m (level 2). THIS event is the trigger.
  * Its 3m timestamp is AFTER the last 15m timestamp from initiation.
  * At least one chain is currently OPEN in direction d (the Chain Overview Table
    'evolving directional move' confirmation).
  * -> ENTER in direction d.

TRADE MANAGEMENT
  * TARGET: the position's prime chain reaches FULL propagation (level 5, 1h).
  * STOP: an opposing chain reaches 5m (level 3).

## Design notes

- The ENTRY component's on_event is called for EVERY chain event; it self-filters
  to "a chain reaching 3m" as the trigger, then checks the initiation history +
  confirmation. This mirrors how TwoLadderEntry self-filters to its trigger.

- The trigger chain (the 4th) is a Set-neutral chain on THIS ladder. The
  position's "prime" chain — the one whose full propagation to 1h is the target —
  is the trigger chain itself (the confirming directional chain we entered on),
  consistent with the two-ladder design where chain_prime is the trigger chain.

- The confirmation's "opposing chain fails on 1m before this" is a HARD GATE:
  a new opposing chain must have started on 1m and terminated before reaching
  3m in the window between the end of initiation (Chain 2's 15m) and the
  trigger. The trade is taken only when the opposing side is being actively
  rejected right up to entry, confirming the directional move is holding.
"""
from __future__ import annotations

import logging
from typing import Optional

from execution.signal_writer import SignalWriter
from execution.types import ExitReason, TradeSide
from strategy.chain_history import ChainHistoryView, ChainLifecycle
from strategy.ladder_registry import Ladder

log = logging.getLogger("strategy.four_chain")


def _side(direction: int) -> TradeSide:
    return TradeSide.LONG if direction == 1 else TradeSide.SHORT


class FourChainEntry:
    """Entry logic for the four-chain confirmation strategy. Call on_event for
    every chain event on this ladder; it self-filters to the confirming-3m
    trigger and checks the full initiation+confirmation pattern.
    """

    def __init__(self, view: ChainHistoryView, ladder: Ladder, writer: SignalWriter):
        self.view = view
        self.ladder = ladder
        self.writer = writer
        self._L_3M = ladder.level_of("3m")
        self._L_15M = ladder.level_of("15m")

    async def on_event(
        self, *, symbol: str, asset_class: str, chain_id: str, event_type: str,
        direction: int, propagation_level: int, timestamp: str,
        decision_price: Optional[float] = None,
    ) -> Optional[str]:
        """Process one chain event. The view has ALREADY been updated with this
        event (the runtime folds first). Returns the position_key if an ENTER
        was emitted, else None.

        Trigger: this event is a chain reaching 3m (peak advanced to >= 3m) in
        some direction d. That chain is the candidate 'fourth' confirming chain.
        """
        # Self-filter: only a chain reaching 3m is a candidate trigger.
        if propagation_level < self._L_3M:
            return None
        # Terminations aren't triggers.
        if event_type == "chain_terminated":
            return None

        d = direction
        trigger_ts = self.view.parse_ts(timestamp)

        # --- Initiation: two same-direction chains that reached 15m, with a
        #     failed opposing chain between them. ---
        initiators = self._find_initiators(symbol, d, before_ts=trigger_ts, trigger_chain_id=chain_id)
        if initiators is None:
            return None
        chain1, chain2 = initiators
        c1_15m = chain1.reached_at_ts(self._L_15M)
        c2_15m = chain2.reached_at_ts(self._L_15M)
        # Ordering: Chain 2's 15m at or after Chain 1's 15m (prose + script).
        if c1_15m is None or c2_15m is None or c2_15m < c1_15m:
            return None

        # A failed opposing chain must sit between the two initiators.
        failed = self.view.failed_between(
            symbol, direction=d, after=c1_15m, before=c2_15m,
        )
        if not failed:
            return None

        # --- Confirmation ---
        # This 3m trigger's timestamp must be AFTER the last (Chain 2's) 15m.
        if trigger_ts <= c2_15m:
            return None
        # A new opposing chain must have failed (1m, terminated before 3m)
        # between the end of initiation (Chain 2's 15m) and this trigger — the
        # confirmation-phase second fail. Hard gate: the trade is only taken
        # when the noisy backdrop confirms the opposing side is being rejected
        # right up to the entry, not merely earlier in initiation.
        confirm_fail = self.view.failed_between(
            symbol, direction=d, after=c2_15m, before=trigger_ts,
        )
        if not confirm_fail:
            return None
        # At least one chain currently OPEN in the same direction (the Chain
        # Overview Table 'evolving directional move' confirmation). The trigger
        # chain itself counts as open (it's active, just reached 3m).
        open_same = self.view.active_chains(symbol, direction=d)
        if not open_same:
            return None

        # All conditions met -> ENTER. The confirming (trigger) chain is the
        # position's prime chain — its full propagation to 1h is the target.
        pkey = await self.writer.emit_entry(
            chain_id=chain_id,
            side=_side(d),
            symbol=symbol,
            asset_class=asset_class,
            decided_at=timestamp,
            decision_price=decision_price,
            trigger_level=propagation_level,
        )
        log.info(
            "four_chain ENTER %s dir=%d chain=%s (initiators=%s,%s failed=%d)",
            symbol, d, chain_id, chain1.chain_id, chain2.chain_id, len(failed),
        )
        return pkey

    def _find_initiators(
        self, symbol: str, direction: int, before_ts, trigger_chain_id: str,
    ) -> Optional[tuple[ChainLifecycle, ChainLifecycle]]:
        """Find the two most-recent same-direction chains (other than the trigger)
        that reached 15m, before the trigger, ordered by their 15m timestamp.

        Returns (chain1, chain2) with chain1's 15m <= chain2's 15m, or None if
        two such chains don't exist. 'Two separate chains' — distinct ids, and
        distinct from the trigger chain.
        """
        candidates = []
        for r in self.view.chains(symbol):
            if r.chain_id == trigger_chain_id:
                continue
            if r.direction != direction:
                continue
            if not r.reached_level(self._L_15M):
                continue
            ts15 = r.reached_at_ts(self._L_15M)
            if ts15 is None or ts15 > before_ts:
                continue
            candidates.append((ts15, r))
        if len(candidates) < 2:
            return None
        # The two most recent by 15m timestamp; return oldest-first so chain1 is
        # the earlier initiator.
        candidates.sort(key=lambda x: x[0])
        chain1 = candidates[-2][1]
        chain2 = candidates[-1][1]
        return chain1, chain2


class FourChainExit:
    """Exit logic: TARGET when the prime chain reaches full propagation (1h,
    level 5); STOP when an opposing chain reaches 5m (level 3)."""

    def __init__(self, view: ChainHistoryView, ladder: Ladder, writer: SignalWriter):
        self.view = view
        self.ladder = ladder
        self.writer = writer
        self._L_5M = ladder.level_of("5m")
        self._L_1H = ladder.level_of("1h")

    def evaluate(
        self, *, symbol: str, chain_prime_id: str, trade_direction: int,
    ) -> Optional[ExitReason]:
        """Return an ExitReason if the position should exit now, else None. Pure
        read of the history view."""
        # TARGET: the prime (entry) chain reaches full propagation to 1h.
        prime = None
        for r in self.view.chains(symbol):
            if r.chain_id == chain_prime_id:
                prime = r
                break
        if prime is not None and prime.reached_level(self._L_1H):
            return ExitReason.TARGET

        # STOP: any opposing chain reaches 5m (level 3) or deeper.
        opposing = -trade_direction
        for r in self.view.chains(symbol):
            if r.direction == opposing and r.reached_level(self._L_5M):
                return ExitReason.STOP

        return None

    async def maybe_emit_exit(
        self, *, symbol: str, asset_class: str, chain_prime_id: str,
        trade_direction: int, decided_at: str,
        decision_price: Optional[float] = None,
    ) -> Optional[str]:
        """Evaluate and, if exiting, emit the EXIT signal referencing the
        position. Returns the position_key if an exit was emitted, else None.
        Signature matches the registry's ExitComponent protocol."""
        reason = self.evaluate(
            symbol=symbol, chain_prime_id=chain_prime_id,
            trade_direction=trade_direction,
        )
        if reason is None:
            return None
        pkey = await self.writer.emit_exit(
            chain_id=chain_prime_id,
            side=_side(trade_direction),
            symbol=symbol,
            asset_class=asset_class,
            reason=reason,
            decided_at=decided_at,
            decision_price=decision_price,
        )
        log.info("four_chain EXIT %s prime=%s reason=%s", symbol, chain_prime_id, reason.value)
        return pkey