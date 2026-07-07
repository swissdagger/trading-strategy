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
        # Self-filter: only a chain reaching 3m is a candidate trigger. This is
        # the candidate chain_conf.
        if propagation_level < self._L_3M:
            return None
        # Terminations aren't triggers.
        if event_type == "chain_terminated":
            return None

        d = direction
        conf = self.view.chain(symbol, chain_id)
        if conf is None:
            return None   # trigger not yet folded; nothing to reason over
        conf_start = conf.first_seen

        # --- Find prime + init and the two failures that satisfy the full
        #     start-ordered pattern:
        #       prime.start <= fail_1.start <= init.start
        #                    <= fail_2.start <= conf.start
        #     with conf.start >= both initiators' 15m-reach timestamps.
        match = self._match_pattern(symbol, d, conf_start=conf_start,
                                    conf_chain_id=chain_id)
        if match is None:
            return None
        prime, init = match

        # --- Confirmation: at least one chain OTHER THAN chain_conf is currently
        #     open in direction d (the 'evolving directional move' check). The
        #     confirmer is excluded so this is genuine corroboration, not the
        #     trigger satisfying its own gate.
        open_same = [
            r for r in self.view.active_chains(symbol, direction=d)
            if r.chain_id != chain_id
        ]
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
            "four_chain ENTER %s dir=%d conf=%s (prime=%s init=%s)",
            symbol, d, chain_id, prime.chain_id, init.chain_id,
        )
        return pkey

    def _match_pattern(
        self, symbol: str, direction: int, conf_start, conf_chain_id: str,
    ) -> Optional[tuple[ChainLifecycle, ChainLifecycle]]:
        """Find a (prime, init) pair, both same-direction 15m chains distinct
        from the confirmer, such that the full start-ordered pattern holds:

            prime.start <= fail_1.start <= init.start <= fail_2.start <= conf.start

        where fail_1 and fail_2 are DISTINCT opposing failures (1m, terminated
        before 3m), and conf.start >= max(prime.15m, init.15m).

        Returns the (earlier-start, later-start) initiator pair, or None. All
        bounds inclusive (ties allowed). The confirmer's start bounds candidacy:
        both initiators must have started at or before it.
        """
        # Candidate initiators: same-direction 15m chains started <= conf.start,
        # ordered by start. prime is the earlier-started, init the later.
        cands = self.view.initiators_by_start(
            symbol, direction=direction, exclude_chain_id=conf_chain_id,
            before_start=conf_start,
        )
        if len(cands) < 2:
            return None

        # Search ordered pairs (prime before init by start). We want ANY pair
        # that satisfies both failure windows and the 15m gate; iterate newest
        # init first so the tightest, most-recent pattern wins.
        for j in range(len(cands) - 1, 0, -1):
            init = cands[j]
            init_15m = init.reached_at_ts(self._L_15M)
            if init_15m is None or init_15m > conf_start:
                continue   # conf must start at/after init's 15m
            # Second failure: opposing fail started between init.start and
            # conf.start.
            fails_2 = self.view.failed_between(
                symbol, direction=direction,
                after=init.first_seen, before=conf_start,
            )
            if not fails_2:
                continue
            for i in range(j - 1, -1, -1):
                prime = cands[i]
                prime_15m = prime.reached_at_ts(self._L_15M)
                if prime_15m is None or prime_15m > conf_start:
                    continue   # conf must start at/after prime's 15m too
                # First failure: opposing fail started between prime.start and
                # init.start, and DISTINCT from any second-window failure.
                fails_1 = self.view.failed_between(
                    symbol, direction=direction,
                    after=prime.first_seen, before=init.first_seen,
                )
                if self._distinct_failures(fails_1, fails_2):
                    return prime, init
        return None

    @staticmethod
    def _distinct_failures(
        fails_1: list[ChainLifecycle], fails_2: list[ChainLifecycle],
    ) -> bool:
        """True if there is a way to pick one failure from each window such that
        the two are distinct chains. With overlapping windows a single failure
        could appear in both lists; the pattern requires TWO separate rejections,
        so we need at least one pair of distinct ids across the windows."""
        if not fails_1 or not fails_2:
            return False
        ids_1 = {r.chain_id for r in fails_1}
        ids_2 = {r.chain_id for r in fails_2}
        # Distinct pair exists unless both windows contain exactly the same
        # single failure and nothing else.
        if ids_1 - ids_2:
            return True   # a window-1 failure not in window-2
        if ids_2 - ids_1:
            return True   # a window-2 failure not in window-1
        return len(ids_1) >= 2   # identical id-sets: need 2+ to form a pair


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