"""
strategy/ladder_registry.py — named ladders as a registry (generalizes the
fixed Set A / Set B enum).

A "ladder" is an ordered, ascending list of timeframes over which a chain
propagates. The original design hardcoded exactly two — Set A (5s,10s,30s,1m,2m)
and Set B (30s,1m,2m,5m,10m) — as a two-member enum. QISS runs multiple
strategies that may use DIFFERENT ladders, so ladders become a registry: any
number of named ladders, each an ordered timeframe list.

## The invariant this preserves

A chain's propagation_level is LADDER-RELATIVE: level = ladder.index(tf) + 1.
The SAME timeframe is a different level in different ladders — 2m is level 5 in
A (its top) but level 3 in B (its middle). The entry/exit logic depends on this
exact numbering, so level_of MUST stay ladder-relative here. Generalizing the
container (enum -> registry) must not change the numbering rule.

## Compute-once across strategies

Because a chain is a ladder-relative object, two strategies that read the SAME
ladder share its chains for free — the chain machine runs one instance per
(symbol, ladder), not per (symbol, strategy). The union of all active
strategies' ladders is what gets computed; overlap is deduplicated at the ladder
level. This is what makes "compute chains once, let each strategy monitor the
ladders it cares about" work: the shared unit is the LADDER, not the strategy.

Every ladder is registered at startup from config.EXTRA_LADDERS; there are no
built-in ones.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class Ladder:
    """One named ladder: an ordered, ascending list of timeframes. The baseline
    (level-1, where a chain can initiate) is the first timeframe."""
    name: str
    timeframes: tuple[str, ...]

    @property
    def baseline_timeframe(self) -> str:
        return self.timeframes[0]

    def level_of(self, timeframe: str) -> int:
        """1-based level of a timeframe within THIS ladder. Raises if the
        timeframe isn't in it — the same guard the original level_of had."""
        try:
            return self.timeframes.index(timeframe) + 1
        except ValueError:
            raise ValueError(
                f"timeframe {timeframe!r} not in ladder {self.name!r} {self.timeframes}"
            )

    def contains(self, timeframe: str) -> bool:
        return timeframe in self.timeframes

    @property
    def top_level(self) -> int:
        return len(self.timeframes)


class LadderRegistry:
    """All ladders QISS knows, keyed by name. Answers the router's question —
    'which ladders contain this timeframe' — over the full registered set."""

    def __init__(self) -> None:
        self._ladders: Dict[str, Ladder] = {}
        # timeframe -> [ladder_name], rebuilt on register.
        self._by_timeframe: Dict[str, List[str]] = {}

    def register(self, name: str, timeframes: List[str]) -> Ladder:
        if name in self._ladders:
            raise ValueError(f"ladder already registered: {name}")
        ladder = Ladder(name=name, timeframes=tuple(timeframes))
        self._ladders[name] = ladder
        for tf in ladder.timeframes:
            self._by_timeframe.setdefault(tf, []).append(name)
        return ladder

    def get(self, name: str) -> Ladder:
        try:
            return self._ladders[name]
        except KeyError:
            raise ValueError(f"unknown ladder: {name!r} (registered: {sorted(self._ladders)})")

    def names(self) -> List[str]:
        return list(self._ladders.keys())

    def ladders_for_timeframe(self, timeframe: str) -> List[str]:
        """Ladder names whose timeframe list contains `timeframe`. A closed bar
        at this timeframe must be fed to the chain machine of EACH — the
        generalized form of ladders.sets_for_timeframe. Unknown timeframe -> []
        (router drops it; it belongs to no ladder)."""
        return list(self._by_timeframe.get(timeframe, []))

    def all_timeframes(self) -> List[str]:
        """Every distinct timeframe across all registered ladders — the union
        the feeds must produce. First-seen order preserved."""
        seen: List[str] = []
        for ladder in self._ladders.values():
            for tf in ladder.timeframes:
                if tf not in seen:
                    seen.append(tf)
        return seen

    def timeframes_for_ladders(self, ladder_names: List[str]) -> List[str]:
        """The union of timeframes across a subset of ladders — what a single
        strategy (reading only those ladders) needs streamed. First-seen order."""
        seen: List[str] = []
        for name in ladder_names:
            for tf in self.get(name).timeframes:
                if tf not in seen:
                    seen.append(tf)
        return seen
