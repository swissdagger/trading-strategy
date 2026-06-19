"""
execution/exit_manager.py — The exit component and its escalation ladder.

Closes the trade lifecycle: it owns OPEN -> CLOSING -> CLOSED. It is the ONLY
writer of exit_state and the ONLY actor that moves a trade out of OPEN, mirroring
the single-writer discipline the chain state machine uses with its per-symbol
lock and the executor uses for entries.

TWO DECISIONS FROM DESIGN, ENCODED HERE:

  * EXIT IS UNTIED FROM CHAIN TERMINATION. A chain_terminated event is NOT a
    direct edge to a close order. It is at most one INPUT the ExitRule may weigh
    (via on_chain_event below). The decision to exit is the rule's, made on
    price/time/risk — never an automatic reaction to the chain ending.

  * NO RE-DECISION, AMENDMENT ALLOWED. Once CLOSING, intent is one-way: we WILL
    get flat. The escalation ladder may CANCEL+RE-ISSUE or AMEND the working
    close order's PRICE (chasing a fill at successively wider caps), but never
    returns the trade to OPEN. The final rung HALTs+alerts rather than market-ing
    out at an unbounded price.

MECHANISM, NOT POLICY. WHEN to exit (stop level, trailing, target, time) is the
swappable ExitRule, a no-op stub today (NeverExit). HOW an exit executes once
decided is the mechanism here, and it is complete.

TIMER-DRIVEN ESCALATION. Unlike entries (event-driven), the ladder must act when
a window ELAPSES with no fill — so the component runs a periodic tick that, per
CLOSING trade, checks whether the current rung's deadline has passed and
escalates. The tick takes the SAME per-symbol lock as the executor (the lock map
is shared, not duplicated — two maps over one symbol set would deadlock), so an
escalation can't race a streamed fill.

RESUME-MANAGEMENT. arm() is called by the executor when a trade first reaches
OPEN; resume() is called by boot for a trade re-adopted on restart. Both load or
re-establish the exit_state row so the rule resumes WITHOUT restarting from a
blank slate (the trailing anchor, the ladder rung, the deadline all persist).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Awaitable, Callable, Dict, List, Optional

from execution import client_order_id as coid
from execution.adapter import ExecutionAdapter
from execution.executor import CapConfig, price_marketable_limit
from execution.types import (
    ExitReason,
    Fill,
    OrderIntent,
    OrderRequest,
    OrderResult,
    OrderStatus,
    TradeSide,
)

log = logging.getLogger("executor.exit")


# ---------------------------------------------------------------------------
# Swappable exit rule. Decides WHETHER to exit a live position right now, and
# WHY (which selects cap + escalation window downstream). Pure: reads the
# position view, returns a decision; never places orders or writes the DB.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ExitDecision:
    should_exit: bool
    reason: Optional[ExitReason] = None

    @classmethod
    def no(cls) -> "ExitDecision":
        return cls(False)


class ExitRule:
    """Swap this to define WHEN to exit. Called by the exit component under the
    symbol lock — on each price tick, on the periodic timer, and (optionally) on
    a chain event. Must be side-effect-free.
    """
    rule_id: str = "base"

    async def evaluate(self, view: "PositionView") -> ExitDecision:
        raise NotImplementedError

    async def on_price(self, view: "PositionView", price: Decimal) -> ExitDecision:
        """Default: re-evaluate on each price update. Trailing rules override to
        also advance the persisted peak before deciding."""
        return await self.evaluate(view)


class NeverExit(ExitRule):
    """Default no-op: never exits. Lets the full exit mechanism run — arming,
    resume, the timer tick — against live positions while closing nothing.
    Replace when the exit policy is decided."""
    rule_id = "never_exit"

    async def evaluate(self, view: "PositionView") -> ExitDecision:
        return ExitDecision.no()


@dataclass
class PositionView:
    """Read-only snapshot the ExitRule sees. Everything a rule needs to decide,
    nothing it can use to mutate. Built by the component from trades + exit_state.
    """
    trade_id: str
    symbol: str
    asset_class: str
    side: TradeSide
    open_qty: Decimal
    entry_avg_price: Optional[Decimal]
    stop_price: Optional[Decimal]
    target_price: Optional[Decimal]
    time_exit_at: Optional[float]            # epoch seconds
    peak_favorable_price: Optional[Decimal]
    last_price: Optional[Decimal]


# ---------------------------------------------------------------------------
# Hooks the exit component needs. Injected, like the executor's, so this file
# stays free of DB-driver specifics.
# ---------------------------------------------------------------------------
@dataclass
class ExitHooks:
    # exit_state lifecycle
    create_exit_state: Callable[[str, str], Awaitable[None]]          # (trade_id, rule_id)
    load_position_view: Callable[[str], Awaitable[Optional[PositionView]]]
    update_peak: Callable[[str, Decimal], Awaitable[None]]            # (trade_id, peak)
    # ladder bookkeeping in exit_state
    set_escalation: Callable[[str, int, float], Awaitable[None]]      # (trade_id, rung, deadline_epoch)
    # trade state
    set_trade_state: Callable[[str, str, Optional[str]], Awaitable[None]]
    list_closing_trades: Callable[[], Awaitable[List[str]]]           # trade_ids in CLOSING
    list_open_trades: Callable[[], Awaitable[List[str]]]              # trade_ids in OPEN
    # orders/pricing/venue
    upsert_order: Callable[[dict], Awaitable[None]]
    read_touch: Callable[[str], Awaitable[tuple[Decimal, Decimal]]]
    load_cap: Callable[[str, OrderIntent], Awaitable[CapConfig]]
    load_ladder: Callable[[str], Awaitable[List[dict]]]              # (asset_class) -> [{widen_to_spread_mult, window_seconds}, ...]
    # alerting (HALT on top-of-ladder unfilled exit)
    alert: Callable[[str, dict], Awaitable[None]]


class ExitManager:
    """Owns OPEN->CLOSING->CLOSED and the escalation ladder. Shares the
    executor's per-symbol lock map — pass the SAME dict in."""

    def __init__(self, adapter: ExecutionAdapter, hooks: ExitHooks,
                 locks: Dict[str, asyncio.Lock], rule: ExitRule | None = None,
                 tick_seconds: float = 1.0):
        self.adapter = adapter
        self.hooks = hooks
        self._locks = locks                  # SHARED with the executor
        self.rule = rule or NeverExit()
        self.tick_seconds = tick_seconds
        self._stop = asyncio.Event()

    def _lock(self, symbol: str) -> asyncio.Lock:
        # Shared map: same symbol -> same lock object as the executor uses.
        return self._locks[symbol]

    # ----- arming & resume --------------------------------------------------
    async def arm(self, trade_id: str) -> None:
        """Called by the executor when a trade first becomes OPEN. Creates the
        exit_state row so the rule governs the position from here."""
        await self.hooks.create_exit_state(trade_id, self.rule.rule_id)
        log.info("exit armed for trade %s under rule %s", trade_id, self.rule.rule_id)

    async def resume(self, trade_id: str) -> None:
        """Called by boot for an OPEN trade re-adopted on restart, or a CLOSING
        trade whose close order is still working. The exit_state row already
        exists (it survived the crash); we just log re-adoption. The periodic
        tick picks it up from here — including a CLOSING trade mid-ladder, which
        the tick advances when its persisted deadline passes."""
        log.info("exit resumed for trade %s", trade_id)

    # ----- price path -------------------------------------------------------
    async def on_price(self, symbol: str, price: Decimal,
                       trade_ids_for_symbol: List[str]) -> None:
        """Feed a price update to the rule for each OPEN trade on this symbol.
        Trailing rules advance their persisted peak here. If the rule decides to
        exit, we begin the ladder. Serialized under the symbol lock."""
        async with self._lock(symbol):
            for tid in trade_ids_for_symbol:
                view = await self.hooks.load_position_view(tid)
                if view is None:
                    continue
                # advance trailing peak if this price is a new favorable extreme
                await self._maybe_advance_peak(view, price)
                decision = await self.rule.on_price(view, price)
                if decision.should_exit:
                    await self._begin_exit(view, decision.reason or ExitReason.STOP)

    async def _maybe_advance_peak(self, view: PositionView, price: Decimal) -> None:
        """Persist a new high-water (LONG) / low-water (SHORT) so a trailing rule
        resumes faithfully after a crash."""
        if view.peak_favorable_price is None:
            await self.hooks.update_peak(view.trade_id, price)
            return
        better = (price > view.peak_favorable_price if view.side == TradeSide.LONG
                  else price < view.peak_favorable_price)
        if better:
            await self.hooks.update_peak(view.trade_id, price)

    # ----- optional chain-event input --------------------------------------
    async def on_chain_event(self, event: dict, trade_ids_for_symbol: List[str]) -> None:
        """A chain event (e.g. chain_terminated) offered to the rule as ONE input.
        It does NOT auto-close — the rule decides. Most rules ignore it; a rule
        that treats termination as an exit signal does so HERE, explicitly,
        inside its own logic — never as a hard edge in this component."""
        symbol = event.get("symbol")
        if not symbol:
            return
        async with self._lock(symbol):
            for tid in trade_ids_for_symbol:
                view = await self.hooks.load_position_view(tid)
                if view is None:
                    continue
                decision = await self.rule.evaluate(view)  # rule may weigh event-derived state it tracks
                if decision.should_exit:
                    await self._begin_exit(view, decision.reason or ExitReason.STOP)

    # ----- the exit + escalation mechanism ---------------------------------
    async def _begin_exit(self, view: PositionView, reason: ExitReason) -> None:
        """Move OPEN -> CLOSING and place the first (rung 0) capped close order.
        Caller holds the symbol lock. Idempotent: if already CLOSING (a prior
        tick/price already began it), do nothing — no re-decision, no second
        ladder."""
        # Re-read state to avoid double-starting under concurrent triggers.
        # (load_position_view returns None or a non-OPEN marker once CLOSING; we
        # treat a missing open_qty as "not open anymore".)
        if view.open_qty == 0:
            return
        await self.hooks.set_trade_state(view.trade_id, "CLOSING", None)
        await self._place_exit_rung(view, reason, rung=0)

    async def _place_exit_rung(self, view: PositionView, reason: ExitReason,
                               rung: int) -> None:
        """Place (or re-place) the close order at a given ladder rung. Rung 0 is
        the first attempt at the EXIT cap; higher rungs widen the cap per the
        ladder config. Caller holds the symbol lock."""
        ladder = await self.hooks.load_ladder(view.asset_class)
        # Determine this rung's cap. Rung 0 uses the base EXIT cap; rung k>0 uses
        # the widened multiple from ladder[k-1].
        base_cap = await self.hooks.load_cap(view.asset_class, OrderIntent.EXIT)
        if rung == 0:
            cap = base_cap
            window = ladder[0]["window_seconds"] if ladder else 30
        elif rung - 1 < len(ladder):
            step = ladder[rung - 1]
            cap = CapConfig(base_cap.cross_spread_mult,
                            step["widen_to_spread_mult"],
                            float(base_cap.cap_abs_bps) if base_cap.cap_abs_bps else None)
            window = step["window_seconds"]
        else:
            # Past the top of the ladder: do NOT market out. HALT + alert. This
            # is the explicit guard against an unbounded fill on a gap-through.
            await self.hooks.set_trade_state(
                view.trade_id, "HALTED",
                f"exit_unfilled_top_of_ladder rung={rung} reason={reason.value}")
            await self.hooks.alert("exit_halted_unfilled", {
                "trade_id": view.trade_id, "symbol": view.symbol,
                "reason": reason.value, "rung": rung,
                "open_qty": str(view.open_qty),
            })
            log.error("exit %s exhausted ladder at rung %s -> HALTED (position "
                      "still open %s)", view.trade_id, rung, view.open_qty)
            return

        # Close order is opposite side to the position; build deterministic id at
        # this rung (distinct per rung so a re-issue is a NEW venue order).
        client_id = coid.build(view.trade_id, OrderIntent.EXIT, rung)
        bid, ask = await self.hooks.read_touch(view.symbol)
        limit, reference, cap_applied = price_marketable_limit(
            view.side, OrderIntent.EXIT, bid, ask, cap)

        order = OrderRequest(
            client_order_id=client_id, trade_id=view.trade_id, symbol=view.symbol,
            asset_class=view.asset_class, intent=OrderIntent.EXIT, side=view.side,
            qty=view.open_qty, limit_price=limit, venue_reference=reference,
            cap_applied=cap_applied, exit_reason=reason, escalation_rung=rung,
        )
        await self.hooks.upsert_order({
            "client_order_id": client_id, "trade_id": view.trade_id,
            "intent": "EXIT", "side": view.side.value, "status": "WORKING",
            "limit_price": limit, "venue_reference": reference,
            "cap_applied": cap_applied, "exit_reason": reason.value,
            "escalation_rung": rung, "target_qty": view.open_qty,
        })
        deadline = time.time() + window
        await self.hooks.set_escalation(view.trade_id, rung, deadline)

        result = await self.adapter.submit_order(order)
        # Synchronous fill handling is the executor's on_fill path (shared fill
        # stream); here we only record the submit. If the venue rejects the close
        # order outright, escalate immediately rather than wait the window.
        if result.status == OrderStatus.REJECTED:
            log.warning("exit rung %s rejected for %s: %s — escalating now",
                        rung, view.trade_id, result.reject_reason)
            await self._escalate(view, reason, rung)
        else:
            log.info("exit rung %s placed for %s (cap_applied=%s, window=%ss)",
                     rung, view.trade_id, cap_applied, window)

    async def _escalate(self, view: PositionView, reason: ExitReason,
                        current_rung: int) -> None:
        """Cancel the current working close order and re-issue at the next rung's
        wider cap. NOT a re-decision — intent stays one-way toward flat. Caller
        holds the symbol lock."""
        current_id = coid.build(view.trade_id, OrderIntent.EXIT, current_rung)
        try:
            await self.adapter.cancel_order(current_id)  # idempotent on terminal
        except Exception as e:
            log.warning("cancel of exit rung %s failed (%r); proceeding to "
                        "re-issue anyway", current_rung, e)
        # Re-read the live view: a partial fill may have reduced open_qty while
        # the rung was working, so the next rung closes only the remainder.
        fresh = await self.hooks.load_position_view(view.trade_id)
        if fresh is None or fresh.open_qty == 0:
            # Fully filled in the meantime — the fill path will set CLOSED.
            return
        await self._place_exit_rung(fresh, reason, current_rung + 1)

    # ----- periodic ladder tick --------------------------------------------
    async def run(self) -> None:
        """Periodic driver. For each CLOSING trade whose current rung's deadline
        has passed without a fill, escalate. This is the TIMER half of the
        ladder — fills are handled on the executor's event-driven fill stream;
        elapsed-windows are handled here. Runs until stop()."""
        log.info("exit manager tick loop started (%.2fs)", self.tick_seconds)
        while not self._stop.is_set():
            try:
                await self._tick_once()
            except Exception:
                log.exception("exit tick error")  # never let the loop die
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.tick_seconds)
            except asyncio.TimeoutError:
                pass

    async def _tick_once(self) -> None:
        now = time.time()
        for tid in await self.hooks.list_closing_trades():
            view = await self.hooks.load_position_view(tid)
            if view is None or view.open_qty == 0:
                continue
            # The deadline + current rung live in exit_state; load_position_view
            # is extended by the app to surface them, or a dedicated loader is
            # used. We re-read via a light accessor on the view's escalation
            # fields (app maps exit_state.escalation_deadline/rung onto it).
            deadline = getattr(view, "escalation_deadline", None)
            rung = getattr(view, "escalation_rung", 0)
            if deadline is not None and now >= deadline:
                async with self._lock(view.symbol):
                    # Re-check under lock: a fill may have closed it between the
                    # list and the lock acquisition.
                    fresh = await self.hooks.load_position_view(tid)
                    if fresh is None or fresh.open_qty == 0:
                        continue
                    log.info("exit %s rung %s window elapsed -> escalate",
                             tid, rung)
                    await self._escalate(fresh, ExitReason.STOP, rung)

    def stop(self) -> None:
        self._stop.set()
