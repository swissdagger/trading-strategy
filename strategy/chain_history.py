"""
strategy/chain_history.py — a chain-history view for the four-chain
confirmation strategy.

The two-ladder ChainView tracks only CURRENTLY-ACTIVE chains and keeps just
(direction, peak_level) — it forgets a chain the moment it terminates and never
records timestamps. The four-chain confirmation strategy needs more:

  * TERMINATED chains, because its initiation phase reasons about a chain that
    FAILED (an opposing 1m chain that terminated before reaching 3m);
  * PER-TIMEFRAME timestamps, because it compares "Chain 2's 15m timestamp >=
    Chain 1's 15m timestamp" and "Chain 4's 3m timestamp is after Chain 2's 15m
    and before Chain 2's 1h";
  * chain ORDERING in time, to identify "two initiators then a failed opposing
    chain then a confirming chain".

So this strategy owns its own history structure rather than reusing the
two-ladder ChainView. It is DELIBERATELY SEPARATE: this is a distinct strategy
on its own ladder [1m,3m,5m,15m,1h], not part of the A/B two-ladder design, and
coupling them would entangle two unrelated strategies.

## Retention: count cap only, no time-based eviction

The four-chain pattern has NO specified span bound: chain_prime and chain_conf
can in principle be arbitrarily far apart, so a chain far in the past may still
participate. We therefore do NOT age chains out by time — doing so would silently
cap the pattern span at the retention window and drop valid signals. Memory is
bounded only by a per-symbol COUNT cap (max_chains_per_symbol), which drops the
oldest chains by first_seen if a symbol ever exceeds it.

In normal operation chain volume is far below the cap (order tens of chains per
symbol per month), so the cap is a backstop against pathological growth, not a
routine trim. If it ever bites in practice, raise the cap; if match cost ever
becomes noticeable at high chain counts, the initiator search in the strategy is
the place to optimize (see four_chain_strategy._match_pattern).

## FUTURE — chain-monitoring encapsulation (deferred, YAGNI)

This is the SECOND chain-monitoring structure in QISS (the two-ladder ChainView
is the first). They overlap — both fold events, retain state, answer queries —
but differ in WHAT they retain (this one keeps terminated chains + per-level
timestamps; ChainView keeps only active chains + peak level). With only two
strategies that is an acceptable, small duplication, and the right shared
abstraction is better DISCOVERED from a third example than guessed from two.

When a third strategy arrives, revisit: if it can reuse this view, extract
nothing; if it needs a third variant, the three together will reveal the real
shared base. The likely long-term shape is a `ChainMonitor` protocol
(apply(event) + queries) with the runtime owning a SET of monitors and folding
each event into all of them once — inverting today's "each strategy folds its
own" (see four_chain_adapter's double-fold) into "runtime folds once per
monitor, strategies subscribe". Deferred deliberately: doing it now would be
premature generalization from N=2. Do NOT build it until a concrete third
strategy forces the question.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

from strategy.ladder_registry import Ladder


def _parse_ts(ts: str) -> datetime:
    """ISO timestamp -> aware datetime (UTC assumed if naive). Chain events
    carry ISO strings; we compare them as datetimes for the ordering rules."""
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


@dataclass
class ChainLifecycle:
    """One chain's life as tracked by the history view. reached_at maps a level ->
    the timestamp it FIRST reached that level; peak_level is the deepest reached;
    terminated marks whether it has ended (failed or completed)."""
    chain_id: str
    direction: int
    first_seen: datetime
    last_update: datetime
    peak_level: int
    reached_at: Dict[int, datetime] = field(default_factory=dict)
    terminated: bool = False

    def reached_level(self, level: int) -> bool:
        return self.peak_level >= level

    def reached_at_ts(self, level: int) -> Optional[datetime]:
        """Timestamp this chain first reached `level`, or None if it never did.
        Because propagation only ascends, reaching a higher level implies the
        lower ones were reached; but we only have explicit timestamps for levels
        an event actually reported. reached_at holds those."""
        return self.reached_at.get(level)


class ChainHistoryView:
    """Per-symbol rolling history of chains on ONE ladder, retaining terminated
    chains and per-level timestamps for the confirmation strategy.

    Fed the same raw chain events as the two-ladder view, but decoupled from it:
    it keys purely on this strategy's ladder and ignores the A/B `set` tag.
    Serialized per symbol by the runtime's per-symbol lock, same as ChainView.
    """

    def __init__(
        self,
        ladder: Ladder,
        *,
        max_chains_per_symbol: int = 2000,
    ):
        self._ladder = ladder
        self._max = max_chains_per_symbol
        # symbol -> {chain_id -> ChainLifecycle}, insertion-ordered so eviction
        # of the oldest (by first_seen) under the count cap is cheap.
        self._chains: Dict[str, "dict[str, ChainLifecycle]"] = {}

    def apply(self, *, symbol: str, chain_id: str, event_type: str,
              direction: int, propagation_level: int, timestamp: str) -> None:
        """Fold one raw chain event into the history.

        initiate/propagate upsert the chain and record the reached-at timestamp
        for the new level; terminate marks it terminated (but KEEPS it — the
        strategy needs failed chains, and the pattern span is unbounded so we do
        not age chains out by time; only a per-symbol COUNT cap bounds memory).
        """
        ts = _parse_ts(timestamp)
        bucket = self._chains.setdefault(symbol, {})
        rec = bucket.get(chain_id)
        if rec is None:
            rec = ChainLifecycle(
                chain_id=chain_id, direction=direction,
                first_seen=ts, last_update=ts, peak_level=0,
            )
            bucket[chain_id] = rec
        rec.last_update = ts

        if event_type == "chain_terminated":
            rec.terminated = True
        else:
            # initiate or propagate: advance peak and record the reached-at for
            # any newly-reached level. Only record if this is a NEW peak, so the
            # timestamp is when the level was FIRST reached.
            if propagation_level > rec.peak_level:
                rec.peak_level = propagation_level
                rec.reached_at[propagation_level] = ts
            elif propagation_level not in rec.reached_at:
                # A same-or-lower level we haven't timestamped yet (e.g. an
                # initiate at level 1): record it.
                rec.reached_at[propagation_level] = ts

        self._enforce_cap(symbol)

    def _enforce_cap(self, symbol: str) -> None:
        """Bound memory by a per-symbol COUNT cap only (no time-based eviction).
        The four-chain pattern span is unbounded, so a chain arbitrarily far in
        the past can still participate; we never age chains out by time. If a
        symbol exceeds the cap, drop the oldest by first_seen as a safety net.
        Expected chain volume is far below the cap in normal operation; this is
        a backstop against pathological growth, not a routine trim.
        """
        bucket = self._chains.get(symbol)
        if not bucket or len(bucket) <= self._max:
            return
        ordered = sorted(bucket.values(), key=lambda r: r.first_seen)
        for r in ordered[: len(bucket) - self._max]:
            bucket.pop(r.chain_id, None)

    # ---- queries the confirmation strategy asks -------------------------

    def chains(self, symbol: str) -> List[ChainLifecycle]:
        """All retained chains for a symbol, oldest-first by first_seen."""
        bucket = self._chains.get(symbol, {})
        return sorted(bucket.values(), key=lambda r: r.first_seen)

    def chain(self, symbol: str, chain_id: str) -> Optional[ChainLifecycle]:
        """The lifecycle record for one chain, or None if not retained."""
        return self._chains.get(symbol, {}).get(chain_id)

    def active_chains(self, symbol: str, direction: Optional[int] = None) -> List[ChainLifecycle]:
        """Currently-open (non-terminated) chains, optionally one direction.
        Used by the confirmation's 'at least one chain open in the same
        direction' check (the Chain Overview Table requirement)."""
        out = [r for r in self.chains(symbol) if not r.terminated]
        if direction is not None:
            out = [r for r in out if r.direction == direction]
        return out

    def failed_between(
        self, symbol: str, direction: int,
        after: datetime, before: datetime,
    ) -> List[ChainLifecycle]:
        """Opposing chains (direction = -`direction`) that FAILED — terminated
        before reaching 3m (level 2) — whose START falls within [after, before].

        A failure is a chain that began at 1m and terminated before propagating
        to 3m; on this ladder that is necessarily a chain stuck at level 1. The
        window is bounded by chain-START time (first_seen), which on this ladder
        equals the 1m-initiate time: chains always start at 1m and propagation
        never skips levels, so the first event folded for a chain is its start.
        Bounds are INCLUSIVE — simultaneous starts are permitted.

        This is the live-detection of the prose's 'an opposing chain must fail to
        propagate' between two ordered chains, replacing the research script's
        id-gap approximation.
        """
        opposing = -direction
        three_m = self._ladder.level_of("3m")
        out = []
        for r in self.chains(symbol):
            if r.direction != opposing:
                continue
            if not r.terminated:
                continue
            if r.peak_level >= three_m:
                continue   # it DID reach 3m — not a failure
            # Ordered by chain start: the failure must have STARTED within the
            # window between the two neighbouring chains' starts.
            if after <= r.first_seen <= before:
                out.append(r)
        return out

    def opposing_failures_in_window(
        self, symbol: str, direction: int,
        after: datetime, before: datetime, *, exact_depth: int = 1,
    ) -> List[ChainLifecycle]:
        """Opposing chains (direction = -`direction`) that FAILED at EXACTLY
        `exact_depth` — terminated with peak_level == exact_depth — whose START
        (first_seen) falls within [after, before], bounds INCLUSIVE.

        The prior_failure gate's predicate (spec §2.2), generalized from
        failed_between in two deliberate ways:

          * DEPTH IS EXACT, not "below some timeframe". A chain that reached
            L3 and then terminated is NOT a failure for this gate — the signal
            is a chain that initiated and died without propagating. And it is
            expressed as a LEVEL, not a hardcoded timeframe name, so the same
            predicate is correct on any ladder (failed_between's "before 3m"
            hardcodes the FC shape).
          * TERMINATED IS REQUIRED, same as failed_between, and it matters
            here: an opposing chain currently ALIVE at L1 has not failed —
            it may be about to run. Only a completed L1-only life is the
            rejection signal the research keyed on.

        The window is on chain START (the research predicate keys on p.start,
        not p.terminated_at), which on any ladder equals the baseline-initiate
        time (chains always start at the baseline; propagation never skips).
        """
        opposing = -direction
        out = []
        for r in self.chains(symbol):
            if r.direction != opposing:
                continue
            if not r.terminated:
                continue
            if r.peak_level != exact_depth:
                continue
            if after <= r.first_seen <= before:
                out.append(r)
        return out

    def initiators_by_start(
        self, symbol: str, direction: int, exclude_chain_id: str,
        before_start: datetime,
    ) -> List[ChainLifecycle]:
        """Same-direction chains (excluding `exclude_chain_id`) that reached 15m
        and started at or before `before_start`, ordered by START time
        (first_seen), oldest-first.

        The four-chain pattern orders chain_prime and chain_init by chain start,
        not by when they reached 15m, so ordering keys on first_seen. On this
        ladder first_seen is the true 1m-start (chains always start at 1m and
        propagation never skips). The caller picks the pair whose starts bracket
        a failure and whose 15m timestamps gate the confirmer.
        """
        fifteen_m = self._ladder.level_of("15m")
        out = [
            r for r in self.chains(symbol)
            if r.chain_id != exclude_chain_id
            and r.direction == direction
            and r.reached_level(fifteen_m)
            and r.first_seen <= before_start
        ]
        out.sort(key=lambda r: r.first_seen)
        return out

    def parse_ts(self, ts: str) -> datetime:
        """Expose the timestamp parser so the strategy compares event timestamps
        against stored reached-at datetimes consistently."""
        return _parse_ts(ts)