"""
strategy/chain_view.py — The tagged-event contract and per-set chain-state view.

The entry logic asks questions a single event cannot answer ("has a Set A chain
reached 2m?", "are there opposing Set A chains at 30s?"). Those are questions
about the CURRENT STATE of many chains across two sets. So the policy maintains a
live VIEW, updated as events arrive and queried at decision time. This module is
that view, plus the exact event shape it consumes.

TWO-LADDER PRECONDITION. This system forks from CausaView precisely to run TWO
parallel chain computations per symbol — Set A and Set B — over different
timeframe ladders. The chain state machine must tag every emitted event with
which set produced it. THIS MODULE DEFINES THAT CONTRACT (TaggedChainEvent); the
two-ladder machine is separate work that must emit exactly this shape. Until it
does, the view has nothing correctly-shaped to consume.

The view is maintained under the executor's per-symbol lock (entry evaluation
reads it while events mutate it), so it holds no lock itself — callers serialize.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class ChainSet(str, Enum):
    """Which parallel ladder a chain belongs to. The discriminator the
    two-ladder machine MUST stamp on every event."""
    A = "A"   # Set A ladder: 5s, 10s, 30s, 1m, 2m
    B = "B"   # Set B ladder: 30s, 1m, 2m, 5m, 10m


# Canonical level<->timeframe maps per set. Level is 1-based: level 1 is the
# baseline (first) timeframe of the set's ladder, ascending. The entry/exit logic
# speaks in timeframes ("reached 2m"); these translate to the level integers the
# chain events actually carry. The two-ladder machine MUST use these same ladders
# so its propagation_level values line up with what the policy expects.
SET_A_LADDER: List[str] = ["5s", "10s", "30s", "1m", "2m"]
SET_B_LADDER: List[str] = ["30s", "1m", "2m", "5m", "10m"]

_LADDERS = {ChainSet.A: SET_A_LADDER, ChainSet.B: SET_B_LADDER}


def level_of(set_: ChainSet, timeframe: str) -> int:
    """1-based level of a timeframe within a set's ladder. Raises if the
    timeframe isn't in that set — a guard against asking Set A about '10m' (which
    only exists in Set B) or vice versa."""
    ladder = _LADDERS[set_]
    try:
        return ladder.index(timeframe) + 1
    except ValueError:
        raise ValueError(f"timeframe {timeframe!r} not in set {set_.value} ladder {ladder}")


# Pre-computed level constants the policy uses, so the four-step logic reads in
# timeframe terms while comparing the integer levels the events carry.
A_LEVEL_30S = level_of(ChainSet.A, "30s")   # entry veto level (opposing)
A_LEVEL_2M = level_of(ChainSet.A, "2m")     # entry trigger level (Set A 2m)
B_LEVEL_2M = level_of(ChainSet.B, "2m")     # exit: opposing Set B chain at 2m+
B_LEVEL_10M = level_of(ChainSet.B, "10m")   # exit: chain_prime reaches top of Set B


@dataclass(frozen=True)
class TaggedChainEvent:
    """THE CONTRACT the two-ladder machine must emit. A CausaView chain event
    PLUS the set tag. event_type is one of chain_initiated/propagated/terminated.
    direction is -1 | +1 (never 0). propagation_level is the 1-based level within
    the chain's set ladder. chain_id is the machine's uuid for this chain.
    """
    set: ChainSet
    event_type: str            # 'chain_initiated' | 'chain_propagated' | 'chain_terminated'
    chain_id: str
    symbol: str
    asset_class: str
    direction: int             # -1 | +1
    propagation_level: int     # 1-based within the set's ladder
    timestamp: str             # ISO; the decision time carried into signals

    @classmethod
    def from_raw(cls, ev: dict) -> "TaggedChainEvent":
        """Build from a raw event dict. Requires a 'set' field (A|B) — a raw
        event WITHOUT a set tag is from the untagged (CausaView-style) machine and
        cannot be used here; we raise rather than guess a set, because guessing
        would silently corrupt the two-ladder logic."""
        if "set" not in ev:
            raise ValueError("event missing 'set' tag — not from the two-ladder machine")
        md = ev.get("metadata") or {}
        return cls(
            set=ChainSet(ev["set"]),
            event_type=ev["event_type"],
            chain_id=ev["chain_id"],
            symbol=ev["symbol"],
            asset_class=ev.get("asset_class", ""),
            direction=int(ev["direction"]),
            propagation_level=int(md.get("propagation_level", ev.get("propagation_level", 1))),
            timestamp=ev["timestamp"],
        )


@dataclass
class ChainRecord:
    """One currently-active chain in the view: its direction and the deepest
    level it has reached. Peak level (not current) because propagation only
    ascends and the logic asks 'has it REACHED 2m' — once reached, it counts even
    if a later event is about a different concern."""
    chain_id: str
    direction: int
    peak_level: int


class ChainView:
    """Live registry of currently-active chains, per symbol, per set. Updated on
    every tagged event; queried by the entry/exit logic. Not thread/async-safe on
    its own — the caller (policy, under the executor's per-symbol lock) serializes
    access per symbol.
    """

    def __init__(self) -> None:
        # symbol -> set -> {chain_id -> ChainRecord}
        self._chains: Dict[str, Dict[ChainSet, Dict[str, ChainRecord]]] = {}

    def _bucket(self, symbol: str, set_: ChainSet) -> Dict[str, ChainRecord]:
        return self._chains.setdefault(symbol, {}).setdefault(set_, {})

    def apply(self, ev: TaggedChainEvent) -> None:
        """Fold one event into the view. initiate/propagate upsert the chain and
        advance its peak level; terminate removes it (a terminated chain is no
        longer 'active' for any of the logic's questions)."""
        bucket = self._bucket(ev.symbol, ev.set)
        if ev.event_type == "chain_terminated":
            bucket.pop(ev.chain_id, None)
            return
        rec = bucket.get(ev.chain_id)
        if rec is None:
            bucket[ev.chain_id] = ChainRecord(ev.chain_id, ev.direction, ev.propagation_level)
        else:
            # peak only ascends
            if ev.propagation_level > rec.peak_level:
                rec.peak_level = ev.propagation_level

    # ----- queries the four-step entry logic asks -----------------------

    def active_chains(self, symbol: str, set_: ChainSet) -> List[ChainRecord]:
        return list(self._bucket(symbol, set_).values())

    def chains_reaching(
        self, symbol: str, set_: ChainSet, level: int,
        direction: Optional[int] = None,
    ) -> List[ChainRecord]:
        """Active chains in `set_` whose peak level is >= `level` (REACHED that
        level or deeper — 'highest available', not exact match, per the codebase's
        established rule). Optionally filtered to one direction."""
        out = []
        for rec in self._bucket(symbol, set_).values():
            if rec.peak_level >= level and (direction is None or rec.direction == direction):
                out.append(rec)
        return out

    def has_opposing_at(
        self, symbol: str, set_: ChainSet, level: int, trade_direction: int,
    ) -> bool:
        """True if any active chain in `set_` OPPOSING `trade_direction` has
        reached `level` or deeper. Used by both the entry veto (opposing at 30s)
        and the exit trigger (opposing at 2m)."""
        return bool(self.chains_reaching(symbol, set_, level, direction=-trade_direction))

    def prime_chains(self, symbol: str, direction: Optional[int] = None) -> List[ChainRecord]:
        """Active Set B chains (chain_prime candidates), optionally one direction.
        Step 1 asks whether any exist; Step 3 asks whether one matches the Set A
        trigger's direction."""
        return self.active_chains(symbol, ChainSet.B) if direction is None \
            else [r for r in self._bucket(symbol, ChainSet.B).values() if r.direction == direction]

    def get_chain(self, symbol: str, set_: ChainSet, chain_id: str) -> Optional[ChainRecord]:
        return self._bucket(symbol, set_).get(chain_id)
