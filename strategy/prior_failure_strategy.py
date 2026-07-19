"""
strategy/prior_failure_strategy.py — the prior-failure scale-out strategy.

The loose-gate sibling of four_chain (spec: prior_failure_scaleout). Same key
ingredient — a recent opposing failure — with the corroboration requirements
dropped: the research found the opposing-failure component does most of the
work, so this trades ~3x the frequency for a similar edge. Runs on its own
ladder (PF, [1m..12h], levels 1..10); entry at P2 (3m), scale-out schedule
topping at P9 (6h). Do NOT modify four_chain — this is additive.

## Entry (spec §2)

Trigger: a chain reaching propagation level `entry_level` (P2). Gate: admit iff
there exists a prior chain on the SAME symbol that is OPPOSING, FAILED at
EXACTLY depth `failure_exact_depth` (initiated and terminated without
propagating — a chain that reached L3 then died is NOT a failure), and whose
START falls within `failure_window_hours` before this chain's start. One
qualifying failure is enough. No corroboration check, no concurrent-open check.

## Exit (spec §3) — the back-loaded scale-out

Bank `schedule[k]` of the ORIGINAL position at each level k PAST entry, as the
chain propagates. Any unsold remainder exits at the chain's final level when
the chain terminates (the remainder rule — nothing ever strands). NO STOP LOSS:
deliberate; giveback and trailing stops failed as a class in the research. The
chain's own termination is the stop. Do not add one without re-running that
analysis.

The exit component is STATELESS with respect to what has banked: the banked
set is derived from the chain's peak level (offset k is banked iff
peak >= entry_level + k), and every SCALE emit is idempotent by a
tranche-discriminated deterministic signal_id (migration 016 + the writer).
Re-evaluating on every sweep is therefore safe by construction; an in-memory
per-position set exists only to avoid re-attempting deduped inserts every bar
(a chatter guard, never a correctness dependency — it starts empty after a
restart and the first sweep re-emits, which the DB dedupes).

## Runtime protocol notes (why return values are shaped this way)

The runtime UNTRACKS a position the moment maybe_emit_exit returns a key. A
SCALE tranche leaves the position open, so tranche-only sweeps return None;
the key is returned exactly when the position is DONE — all tranches banked,
or TERMINAL emitted at termination.

## Restart edge (documented, absorbed by the contract)

The ChainHistoryView is rebuilt from live events after a restart (it is not
DB-hydrated). A restored open position whose chain terminates before the view
re-learns its peak will emit TERMINAL with a fraction computed against an
understated peak (possibly 1.0). That is safe: the contract defines TERMINAL
as "close whatever you still hold, derived from YOUR OWN fills" — its fraction
is informational, and consumers floor their remainder at zero.
"""
from __future__ import annotations

import logging
from datetime import timedelta
from decimal import Decimal
from typing import Dict, Optional, Set

from execution.signal_writer import SignalWriter, position_key
from execution.types import ExitReason, TradeSide
from strategy.chain_history import ChainHistoryView
from strategy.ladder_registry import Ladder

log = logging.getLogger("strategy.prior_failure")


def _side(direction: int) -> TradeSide:
    return TradeSide.LONG if direction == 1 else TradeSide.SHORT


def validate_schedule(schedule: Dict[int, float]) -> Dict[int, Decimal]:
    """Validate the scale-out schedule and return it with EXACT Decimal
    fractions. Raises ValueError on anything malformed — a bad schedule is a
    startup misconfiguration, and it must fail loudly at build time, not
    quietly mis-bank in production.

      * keys: ints >= 1 (offsets past entry);
      * fractions: each in (0, 1], and their exact-decimal sum is exactly 1.0
        (float arithmetic would drift; Decimal(str(...)) makes 0.05 exact and
        the remainder rule's "banked + remainder == 1.0" hold to the cent).
    """
    if not schedule:
        raise ValueError("scale-out schedule is empty")
    out: Dict[int, Decimal] = {}
    for k, frac in schedule.items():
        if not isinstance(k, int) or k < 1:
            raise ValueError(f"schedule offset must be an int >= 1, got {k!r}")
        d = Decimal(str(frac))
        if not (0 < d <= 1):
            raise ValueError(f"schedule fraction for offset {k} must be in (0, 1], got {frac}")
        out[k] = d
    total = sum(out.values())
    if total != Decimal("1"):
        raise ValueError(f"schedule fractions must sum to exactly 1.0, got {total}")
    return out


class PriorFailureEntry:
    """Entry component: trigger on a chain reaching `entry_level`, gated by the
    prior-opposing-failure predicate. Call on_event for every chain event on
    this ladder (via the adapter); it self-filters to the trigger.
    """

    def __init__(
        self, view: ChainHistoryView, ladder: Ladder, writer: SignalWriter,
        *, entry_level: int, failure_window_hours: float, failure_exact_depth: int,
    ):
        self.view = view
        self.ladder = ladder
        self.writer = writer
        self.entry_level = entry_level
        self._window = timedelta(hours=failure_window_hours)
        self._exact_depth = failure_exact_depth

    async def on_event(
        self, *, symbol: str, asset_class: str, chain_id: str, event_type: str,
        direction: int, propagation_level: int, timestamp: str,
        decision_price: Optional[float] = None,
    ) -> Optional[str]:
        """Process one chain event (view already updated). Returns the
        position_key if an ENTER was emitted, else None.

        The filter is `>= entry_level`, mirroring FourChainEntry: a deeper
        event on the same chain re-checks the gate and re-emits, which the
        deterministic ENTER signal_id dedupes to ONE row — so a restart that
        missed the exact P2 event still catches the entry late (and the exit
        sweep then immediately banks every already-reached tranche, which is
        the correct late-entry behaviour, not a bug).
        """
        if propagation_level < self.entry_level:
            return None
        if event_type == "chain_terminated":
            return None   # a termination is never a trigger

        chain = self.view.chain(symbol, chain_id)
        if chain is None:
            return None   # trigger not yet folded; nothing to reason over

        # The gate (spec §2.2): one prior opposing exact-depth failure whose
        # START is within [chain.start - window, chain.start], inclusive.
        window_start = chain.first_seen - self._window
        failures = self.view.opposing_failures_in_window(
            symbol, direction=direction,
            after=window_start, before=chain.first_seen,
            exact_depth=self._exact_depth,
        )
        if not failures:
            return None

        pkey = await self.writer.emit_entry(
            chain_id=chain_id,
            side=_side(direction),
            symbol=symbol,
            asset_class=asset_class,
            decided_at=timestamp,
            decision_price=decision_price,
            trigger_level=propagation_level,
        )
        log.info(
            "prior_failure ENTER %s dir=%d chain=%s @L%d (qualifying failure=%s)",
            symbol, direction, chain_id, propagation_level, failures[0].chain_id,
        )
        return pkey


class PriorFailureScaleOutExit:
    """Exit component: the back-loaded scale-out plus the remainder rule.

    Evaluated by the runtime's sweep for each open position after each event
    batch. Emits any newly-satisfied SCALE tranches; returns None while the
    position remains open, and the position_key exactly when it is done (all
    tranches banked, or TERMINAL emitted on termination) so the runtime
    untracks it at the right moment and never earlier.
    """

    def __init__(
        self, view: ChainHistoryView, writer: SignalWriter,
        *, entry_level: int, schedule: Dict[int, Decimal],
    ):
        self.view = view
        self.writer = writer
        self.entry_level = entry_level
        self.schedule = schedule            # offset -> EXACT Decimal fraction
        self._max_offset = max(schedule)
        # position_key -> offsets emitted THIS process lifetime. Chatter guard
        # only: correctness is the tranche-discriminated deterministic
        # signal_id (a re-emit after restart dedupes at the DB).
        self._banked: Dict[str, Set[int]] = {}

    async def maybe_emit_exit(
        self, *, symbol: str, asset_class: str, chain_prime_id: str,
        trade_direction: int, decided_at: str,
        decision_price: Optional[float] = None,
    ) -> Optional[str]:
        chain = self.view.chain(symbol, chain_prime_id)
        if chain is None:
            # Not (re-)learned by the view yet (e.g. just after a restart).
            # Keep the position tracked; the chain's next event refolds it.
            return None

        pkey = position_key(chain_prime_id, _side(trade_direction))
        banked = self._banked.setdefault(pkey, set())

        # 1. Bank every newly-satisfied tranche, in offset order. Adding to the
        # in-memory set only AFTER a successful emit means a raised emit leaves
        # the tranche pending: the sweep's per-position isolation keeps the
        # position tracked and the next sweep retries it (deduped by id).
        for k in sorted(self.schedule):
            if k in banked:
                continue
            if chain.peak_level < self.entry_level + k:
                break   # levels are reached in order; nothing deeper is banked
            await self.writer.emit_exit(
                chain_id=chain_prime_id,
                side=_side(trade_direction),
                symbol=symbol,
                asset_class=asset_class,
                reason=ExitReason.SCALE,
                decided_at=decided_at,
                decision_price=decision_price,
                trigger_level=self.entry_level + k,
                fraction=float(self.schedule[k]),
                tranche=k,
            )
            banked.add(k)
            log.info("prior_failure SCALE %s %s tranche=%d fraction=%s @L%d",
                     symbol, pkey, k, self.schedule[k], self.entry_level + k)

        # 2. All tranches banked -> the position is fully closed on schedule.
        # (If the chain later terminates, there is no remainder to close.)
        if chain.peak_level >= self.entry_level + self._max_offset:
            self._banked.pop(pkey, None)
            return pkey

        # 3. The remainder rule: on termination, whatever the schedule has not
        # banked exits at the chain's final level. EXACT Decimal arithmetic —
        # banked fractions sum exactly, so banked + remainder == 1.0 always.
        if chain.terminated:
            banked_total = sum(
                (self.schedule[k] for k in self.schedule
                 if chain.peak_level >= self.entry_level + k),
                Decimal("0"),
            )
            remaining = Decimal("1") - banked_total
            if remaining > 0:
                await self.writer.emit_exit(
                    chain_id=chain_prime_id,
                    side=_side(trade_direction),
                    symbol=symbol,
                    asset_class=asset_class,
                    reason=ExitReason.TERMINAL,
                    decided_at=decided_at,
                    decision_price=decision_price,
                    trigger_level=chain.peak_level or None,
                    fraction=float(remaining),
                )
                log.info("prior_failure TERMINAL %s %s remainder=%s (peak=L%d)",
                         symbol, pkey, remaining, chain.peak_level)
            self._banked.pop(pkey, None)
            return pkey

        return None   # position remains open; keep it tracked