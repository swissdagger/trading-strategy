"""
strategy/prior_failure_adapter.py — bridges prior_failure_scaleout to the
registry's component protocol and feeds its private ChainHistoryView.

Mirrors four_chain_adapter exactly (see its docstring for the pattern's
rationale): the strategy reads its OWN ChainHistoryView on its own ladder (PF),
so the entry adapter folds every incoming event into that view before the
entry check runs; the exit adapter does not fold (the entry adapter already
did, under the same per-symbol lock) and just evaluates.

The factory is where this strategy's configuration is VALIDATED, loudly, at
startup — most importantly that the schedule actually FITS the declared
ladder. The schedule's offsets are levels past entry; on a ladder too short
for them, the deep tranches (65% of the weight) could never fire and every
position would structurally exit at termination — precisely the degradation
the back-loading exists to avoid, and precisely what a 5-level ladder would
have silently done to this 7-offset schedule. That is a config error, not a
market outcome, so it refuses to build.
"""
from __future__ import annotations

from typing import Optional

from strategy.chain_history import ChainHistoryView
from strategy.prior_failure_strategy import (
    PriorFailureEntry,
    PriorFailureScaleOutExit,
    validate_schedule,
)


class PriorFailureEntryAdapter:
    """EntryComponent for the registry. Folds each event into the strategy's
    ChainHistoryView, then runs the gated entry check."""

    def __init__(self, view: ChainHistoryView, entry: PriorFailureEntry):
        self._view = view
        self._entry = entry

    async def on_event(self, ev) -> Optional[str]:
        self._view.apply(
            symbol=ev.symbol,
            chain_id=ev.chain_id,
            event_type=ev.event_type,
            direction=ev.direction,
            propagation_level=ev.propagation_level,
            timestamp=ev.timestamp,
        )
        return await self._entry.on_event(
            symbol=ev.symbol,
            asset_class=ev.asset_class,
            chain_id=ev.chain_id,
            event_type=ev.event_type,
            direction=ev.direction,
            propagation_level=ev.propagation_level,
            timestamp=ev.timestamp,
            decision_price=getattr(ev, "decision_price", None),
        )


class PriorFailureExitAdapter:
    """ExitComponent for the registry. No fold (the entry adapter already did);
    passes the sweep's arguments through, INCLUDING decision_price — the
    runtime supplies it on every sweep, so an adapter that doesn't accept it
    turns every exit evaluation into a TypeError (the four_chain adapter had
    exactly that bug)."""

    def __init__(self, exit_: PriorFailureScaleOutExit):
        self._exit = exit_

    async def maybe_emit_exit(
        self, *, symbol: str, asset_class: str, chain_prime_id: str,
        trade_direction: int, decided_at: str,
        decision_price: Optional[float] = None,
    ) -> Optional[str]:
        return await self._exit.maybe_emit_exit(
            symbol=symbol,
            asset_class=asset_class,
            chain_prime_id=chain_prime_id,
            trade_direction=trade_direction,
            decided_at=decided_at,
            decision_price=decision_price,
        )


def make_prior_failure_scaleout(
    *, name, symbols, ladders, params, view_registry, writer_factory,
):
    """Strategy factory for the registry (config.STRATEGIES entry
    'prior_failure_scaleout'). Validates the full parameter set before
    building anything — every check below guards a silent-degradation mode:

      * exactly one ladder (single-ladder strategy, like four_chain);
      * entry_level a valid level ON that ladder;
      * schedule offsets/fractions well-formed and summing to exactly 1.0
        (validate_schedule);
      * THE FIT CHECK: entry_level + max offset <= ladder.top_level. See the
        module docstring — a schedule that overhangs the ladder is the P9-on-
        a-5-level-ladder mistake, refused at startup rather than discovered
        in the exit-reason mix months later.
    """
    from strategy.registry import Strategy

    if len(ladders) != 1:
        raise ValueError(
            f"prior_failure_scaleout {name!r} must declare exactly one ladder, got {ladders}"
        )
    ladder = view_registry.get(ladders[0])

    entry_level = params.get("entry_level", 2)
    if not isinstance(entry_level, int) or not (1 <= entry_level <= ladder.top_level):
        raise ValueError(
            f"{name!r}: entry_level must be a level on ladder {ladder.name!r} "
            f"(1..{ladder.top_level}), got {entry_level!r}"
        )

    exit_cfg = params.get("exit", {})
    schedule = validate_schedule(exit_cfg.get("schedule", {}))
    max_offset = max(schedule)
    if entry_level + max_offset > ladder.top_level:
        raise ValueError(
            f"{name!r}: schedule does not fit ladder {ladder.name!r}: entry P{entry_level} "
            f"+ max offset {max_offset} = P{entry_level + max_offset}, but the ladder "
            f"tops at P{ladder.top_level} ({ladder.timeframes[-1]}). The overhung "
            f"tranches could never bank and would structurally exit at termination."
        )
    if exit_cfg.get("stop") is not None:
        raise ValueError(
            f"{name!r}: a stop is configured, but stops failed as a class in the "
            f"research and this strategy deliberately has none. Remove it, or "
            f"re-run that analysis before wiring one in."
        )

    gate_cfg = params.get("gate", {})
    window_h = gate_cfg.get("failure_window_hours", 6)
    if not (isinstance(window_h, (int, float)) and window_h > 0):
        raise ValueError(f"{name!r}: failure_window_hours must be > 0, got {window_h!r}")
    exact_depth = gate_cfg.get("failure_exact_depth", 1)
    if not isinstance(exact_depth, int) or not (1 <= exact_depth < entry_level):
        raise ValueError(
            f"{name!r}: failure_exact_depth must be an int in [1, entry_level), "
            f"got {exact_depth!r} (a 'failure' at or past the entry trigger level "
            f"is not a failure)"
        )

    cap = params.get("max_chains_per_symbol")
    history = (
        ChainHistoryView(ladder, max_chains_per_symbol=cap)
        if cap is not None
        else ChainHistoryView(ladder)
    )
    writer = writer_factory(name)

    entry = PriorFailureEntry(
        history, ladder, writer,
        entry_level=entry_level,
        failure_window_hours=window_h,
        failure_exact_depth=exact_depth,
    )
    exit_ = PriorFailureScaleOutExit(
        history, writer,
        entry_level=entry_level,
        schedule=schedule,
    )
    return Strategy(
        name=name,
        entry=PriorFailureEntryAdapter(history, entry),
        exit=PriorFailureExitAdapter(exit_),
        symbols=symbols,
        ladders=ladders,
    )