"""
producer/ladders.py — Set A and Set B as data-driven named ladders.

A "set" is a NAMED LADDER (an ordered list of ascending timeframes), not code.
The two-ladder design runs both ladders per symbol; this module defines them once
and answers the one question the fan-out router asks: "which ladders contain this
timeframe?" — because a closed bar must be routed to EVERY ladder whose timeframe
list includes it (the overlap timeframes 30s/1m/2m feed BOTH sets).

These ladders MUST match strategy/chain_view.py's SET_A_LADDER / SET_B_LADDER
exactly — they are the same ladders seen from the producer side. If they drift,
the producer's propagation_level values won't line up with what the consumer's
logic expects. They are duplicated rather than imported across the
producer/strategy boundary ONLY if those packages must stay decoupled; here we
import from the single source to guarantee they cannot drift.
"""
from __future__ import annotations

from typing import Dict, List

from strategy.chain_view import SET_A_LADDER, SET_B_LADDER, ChainSet

# The authoritative ladder for each set — sourced from the consumer-side
# definition so producer and consumer cannot disagree on level numbering.
LADDERS: Dict[ChainSet, List[str]] = {
    ChainSet.A: SET_A_LADDER,   # 5s, 10s, 30s, 1m, 2m
    ChainSet.B: SET_B_LADDER,   # 30s, 1m, 2m, 5m, 10m
}

# The baseline (level-1) timeframe of each set — the timeframe at which a chain
# can INITIATE. The chain machine for a set treats this as its baseline_timeframe.
BASELINE: Dict[ChainSet, str] = {s: tf[0] for s, tf in LADDERS.items()}

# Inverted index: timeframe -> the sets whose ladder contains it. THIS is what the
# fan-out router consults per bar. Built once at import.
_SETS_FOR_TIMEFRAME: Dict[str, List[ChainSet]] = {}
for _set, _tfs in LADDERS.items():
    for _tf in _tfs:
        _SETS_FOR_TIMEFRAME.setdefault(_tf, []).append(_set)


def sets_for_timeframe(timeframe: str) -> List[ChainSet]:
    """The sets whose ladder contains `timeframe`. A bar at this timeframe must be
    fed to the chain machine instance of EACH returned set. Overlap timeframes
    (30s, 1m, 2m) return both sets; 5s/10s return [A]; 5m/10m return [B]; an
    unknown timeframe returns [] (the router drops it — it belongs to no ladder)."""
    return _SETS_FOR_TIMEFRAME.get(timeframe, [])


def all_timeframes() -> List[str]:
    """Every distinct timeframe across both ladders — the full set of bar
    granularities the feeds must produce for the two-ladder machine to run. This
    is what config.py's ASSET_CONFIGS must cover (and what gates the 5s/10s
    Polygon-entitlement question)."""
    seen: List[str] = []
    for tfs in LADDERS.values():
        for tf in tfs:
            if tf not in seen:
                seen.append(tf)
    return seen


def level_in_set(set_: ChainSet, timeframe: str) -> int:
    """1-based level of `timeframe` within `set_`'s ladder. Raises if the
    timeframe isn't in that set (guards a bar being attributed to a level it
    doesn't occupy in that ladder)."""
    return LADDERS[set_].index(timeframe) + 1
