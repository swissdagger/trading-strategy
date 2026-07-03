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

## Retention: a rolling window, not all history

The pattern is a RECENT causal sequence — the confirming chain's 3m must fall
before an initiator's 1h, so the whole pattern spans at most ~an hour or two of
chains. Retaining every terminated chain forever would be unbounded memory for a
24/7 process over years, with zero benefit (a chain from last week cannot
participate in a pattern gated by "before Chain 2's 1h"). We bound retention by
TIME (a window comfortably longer than the longest span the pattern references)
with a COUNT cap as a safety net. Chains older than the window are evicted.

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
from datetime import datetime, timedelta, timezone
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
    """One chain's life within the retention window. reached_at maps a level ->
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
        retention: timedelta = timedelta(hours=3),
        max_chains_per_symbol: int = 2000,
    ):
        self._ladder = ladder
        self._retention = retention
        self._max = max_chains_per_symbol
        # symbol -> {chain_id -> ChainLifecycle}, insertion-ordered so eviction
        # of the oldest is cheap.
        self._chains: Dict[str, "dict[str, ChainLifecycle]"] = {}

    def apply(self, *, symbol: str, chain_id: str, event_type: str,
              direction: int, propagation_level: int, timestamp: str) -> None:
        """Fold one raw chain event into the history.

        initiate/propagate upsert the chain and record the reached-at timestamp
        for the new level; terminate marks it terminated (but KEEPS it in the
        window — the strategy needs failed chains). Eviction of stale chains runs
        opportunistically on each apply, keyed off this event's timestamp as
        'now' (event time, not wall clock, so replay is deterministic).
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

        self._evict_stale(symbol, now=ts)

    def _evict_stale(self, symbol: str, now: datetime) -> None:
        bucket = self._chains.get(symbol)
        if not bucket:
            return
        cutoff = now - self._retention
        # Evict by age: drop chains whose last update is older than the window.
        stale = [cid for cid, r in bucket.items() if r.last_update < cutoff]
        for cid in stale:
            del bucket[cid]
        # Count cap safety net: if still over, drop oldest by first_seen.
        if len(bucket) > self._max:
            ordered = sorted(bucket.values(), key=lambda r: r.first_seen)
            for r in ordered[: len(bucket) - self._max]:
                bucket.pop(r.chain_id, None)

    # ---- queries the confirmation strategy asks -------------------------

    def chains(self, symbol: str) -> List[ChainLifecycle]:
        """All retained chains for a symbol, oldest-first by first_seen."""
        bucket = self._chains.get(symbol, {})
        return sorted(bucket.values(), key=lambda r: r.first_seen)

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
        before reaching 3m (level 2) — with activity in (after, before). This is
        the direct live-detection of the prose's 'an opposing chain must fail to
        propagate' between the two initiators, rather than the research script's
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
            # Its life overlaps the window between the initiators.
            if after <= r.first_seen <= before or after <= r.last_update <= before:
                out.append(r)
        return out

    def parse_ts(self, ts: str) -> datetime:
        """Expose the timestamp parser so the strategy compares event timestamps
        against stored reached-at datetimes consistently."""
        return _parse_ts(ts)