"""
execution/_legacy_executor.py — RETIRED. Do not use in the live path.

    ┌──────────────────────────────────────────────────────────────────────┐
    │ FROZEN / DEPRECATED. The live executor is execution/signal_executor.py.│
    │ This module (the old Kafka chain-event executor) is NO LONGER WIRED     │
    │ INTO THE RUNNING SYSTEM: run_executor.py launches SignalExecutor only,  │
    │ and nothing instantiates this Executor. It is kept ONLY as a reference  │
    │ for the entry/pricing mechanism.                                        │
    │                                                                         │
    │ Known-divergent from the live path (deliberately NOT fixed — fixing     │
    │ dead code is wasted effort): fill attribution here matches by symbol,   │
    │ not by parsed cv1 client_order_id (cf. signal_executor + boot Fix #4);  │
    │ restart does not restore escalation rung/deadline (cf. Fix #1); the     │
    │ filled-qty write-back and async-fill race fixes (#2/#6) are absent.     │
    │ DO NOT "fix" these here and DO NOT import this module into live code.   │
    │ Pricing is the one thing still shared — re-exported from execution.     │
    │ pricing so the crossed-book guard applies even to this frozen file.     │
    └──────────────────────────────────────────────────────────────────────┘

--- original module docstring follows ---

execution/executor.py — The live event-and-order path.

This is the executor's steady-state loop, entered only AFTER boot
(execution/boot.py) has returned True. It consumes chain events off Kafka,
decides (via a swappable EntryPolicy) whether to open a trade, prices and
submits marketable-limit entries through the ExecutionAdapter, and processes the
venue fill stream to advance trade state.

It is MECHANISM, not POLICY. The entry trigger (open on initiate? on level N?)
lives behind EntryPolicy, a no-op stub today. The pricing of the marketable
limit (how far to cross, the basis cap) reads execution_caps. Swapping the
policy or the caps changes behavior without touching this file.

TWO INVARIANTS THIS FILE EXISTS TO UPHOLD:

  1. PER-SYMBOL LOCK. Mirrors service_2's per-symbol asyncio.Lock around chain
     state. Multiple coroutines can touch one symbol's trade concurrently — a
     chain event, a streamed fill, the reconcile watchdog. The lock is acquired
     BEFORE reading-or-mutating, so the check-then-act of "open a trade only if
     one isn't already open for this chain" is atomic. The DB unique constraint
     on (chain_id, side) is the ultimate backstop; the lock stops us even
     attempting the duplicate.

  2. KILL SWITCH BEFORE EVERY ENTRY. Checked inside the lock, immediately before
     any new entry order. Engaged => consume events, place NO new entries. This
     is the real-money "stop faster than you can deploy" guarantee.

The fill stream runs concurrently and also takes the per-symbol lock before
advancing a trade — so a fill landing the same instant a new event arrives can't
interleave with it.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from decimal import Decimal
from typing import Awaitable, Callable, Dict, Optional

from execution import client_order_id as coid
from execution.adapter import ExecutionAdapter
from execution.types import (
    Fill,
    OrderIntent,
    OrderRequest,
    OrderResult,
    OrderStatus,
    TradeSide,
)

log = logging.getLogger("executor.live")


# ---------------------------------------------------------------------------
# Swappable entry policy. Today a no-op (NeverEnter) so the mechanism runs live
# against real events while placing nothing. A real policy decides, per chain
# event, whether to open and at what size. It is PURE: given the event and a
# read-only view, return an EntryDecision. It does NOT place orders or touch the
# DB — the executor does that, under the lock.
# ---------------------------------------------------------------------------
class EntryDecision:
    __slots__ = ("should_enter", "side", "qty", "reason")

    def __init__(self, should_enter: bool, side: Optional[TradeSide] = None,
                 qty: Decimal = Decimal(0), reason: str = ""):
        self.should_enter = should_enter
        self.side = side
        self.qty = qty
        self.reason = reason

    @classmethod
    def no(cls, reason: str = "") -> "EntryDecision":
        return cls(False, reason=reason)


class EntryPolicy:
    """Swap this out to define WHEN to trade. The executor calls evaluate() for
    each chain event under the symbol lock; the policy must be side-effect-free.
    """
    async def evaluate(self, event: dict) -> EntryDecision:
        raise NotImplementedError


class NeverEnter(EntryPolicy):
    """Default no-op policy: never opens a trade. Lets the full live path run —
    event consumption, locking, kill-switch checks, fill handling — against real
    events while placing zero orders. Replace with a real policy when the entry
    trigger is decided."""
    async def evaluate(self, event: dict) -> EntryDecision:
        return EntryDecision.no("NeverEnter (no-op policy)")


# ---------------------------------------------------------------------------
# Pricing: marketable-limit price from a venue reference + the per-class,
# per-side cap config. THE ONE implementation lives in execution.pricing; this
# module (the legacy executor) re-exports it so both executors price identically
# and the crossed/invalid-book guard (InvalidTouch) applies everywhere. Kept as
# re-exports rather than a second copy — the previous duplicate here had silently
# diverged from the shared version (it lacked the touch guard).
# ---------------------------------------------------------------------------
from execution.pricing import (  # re-export: one canonical pricing impl
    CapConfig,
    InvalidTouch,
    price_marketable_limit,
    validate_touch,
)
from execution.pricing import is_buy as _is_buy  # legacy private name preserved


# ---------------------------------------------------------------------------
# Hooks the executor needs from the surrounding app: DB persistence, venue touch
# reads, cap/kill-switch reads, and the chain-row guarantee. Injected (like
# BootHooks) so this file stays free of DB-driver and venue specifics.
# ---------------------------------------------------------------------------
class LiveHooks:
    # Returns the open trade for (chain_id, side) if one exists, else None.
    find_trade: Callable[[str, TradeSide], Awaitable[Optional[dict]]]
    # Returns a trade by its trade_id (symbol, side, state, target qtys) — used
    # by the fill path to resolve a fill (attributed via its client_order_id) to
    # the symbol it must lock and the state it must advance. None if unknown.
    find_trade_by_id: Callable[[str], Awaitable[Optional[dict]]]
    # Insert a PENDING trade row; returns trade_id. Honors the (chain_id, side)
    # unique constraint — raises on duplicate, which the executor treats as
    # "already opened, skip" (idempotency backstop).
    insert_pending_trade: Callable[[dict], Awaitable[str]]
    # Persist an order row (on submit) and update it (on result/fill).
    upsert_order: Callable[[dict], Awaitable[None]]
    # Write a fill (idempotent by venue_exec_id) and return the new cumulative
    # filled qty for the trade's relevant side.
    write_fill: Callable[[Fill, str], Awaitable[Decimal]]
    # Advance trade state.
    set_trade_state: Callable[[str, str, Optional[str]], Awaitable[None]]
    # Read current venue touch (bid, ask) for a symbol — IBKR snapshot for
    # equity/fx, Binance book for crypto. Used to price the marketable limit.
    read_touch: Callable[[str], Awaitable[tuple[Decimal, Decimal]]]
    # Load the hot CapConfig for (asset_class, intent).
    load_cap: Callable[[str, OrderIntent], Awaitable[CapConfig]]
    # True if the kill switch is engaged (any mode that blocks new entries).
    kill_switch_blocks_entry: Callable[[], Awaitable[bool]]
    # Hand a newly-OPEN trade to the exit component (arms its exit rule).
    arm_exit: Callable[[str], Awaitable[None]]


class Executor:
    """Owns the per-symbol locks and the live loop. One instance per process."""

    def __init__(self, adapter: ExecutionAdapter, hooks: LiveHooks,
                 policy: EntryPolicy | None = None):
        self.adapter = adapter
        self.hooks = hooks
        self.policy = policy or NeverEnter()
        # Per-symbol lock — THE race guard. defaultdict so a symbol's first
        # event lazily creates its lock; locks persist for process life (bounded
        # by universe size, so no leak concern at these scales).
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def _lock(self, symbol: str) -> asyncio.Lock:
        return self._locks[symbol]

    # ----- Chain-event path -------------------------------------------------
    async def on_chain_event(self, event: dict) -> None:
        """Entry point for each causal-events message. Only chain_* events can
        open trades; price_update and others are ignored here (the chart/heartbeat
        concerns of CausaView don't apply to the executor). All work for a symbol
        is serialized under its lock.
        """
        etype = event.get("event_type", "")
        symbol = event.get("symbol")
        if not symbol or not etype.startswith("chain_"):
            return

        async with self._lock(symbol):
            decision = await self.policy.evaluate(event)
            if not decision.should_enter:
                return

            chain_id = event["chain_id"]
            side = decision.side
            assert side is not None  # should_enter implies a side

            # Idempotency check INSIDE the lock: already have a trade for this
            # (chain_id, side)? Then this event is a duplicate/re-propagation —
            # skip. (The DB unique constraint is the backstop; this avoids even
            # attempting the insert.)
            existing = await self.hooks.find_trade(chain_id, side)
            if existing is not None:
                return

            # Kill switch — checked immediately before creating any entry.
            if await self.hooks.kill_switch_blocks_entry():
                log.warning("kill switch engaged; suppressing entry for %s %s",
                            symbol, side.value)
                return

            await self._open_trade(event, chain_id, symbol, side, decision)

    async def _open_trade(self, event: dict, chain_id: str, symbol: str,
                          side: TradeSide, decision: EntryDecision) -> None:
        """Create the PENDING trade, price the marketable-limit entry, submit it,
        and record the outcome. Caller holds the symbol lock.
        """
        asset_class = event.get("asset_class", "")
        decision_price = _event_price(event)

        # 1. Insert PENDING trade. trade_id is needed to build the deterministic
        #    client_order_id, so the row comes first. A duplicate (chain_id,side)
        #    raises -> treat as already-open, skip.
        try:
            trade_id = await self.hooks.insert_pending_trade({
                "chain_id": chain_id, "symbol": symbol, "asset_class": asset_class,
                "side": side.value, "entry_target_qty": decision.qty,
                "entry_decision_price": decision_price,
                "entry_trigger_level": (event.get("metadata") or {}).get("propagation_level"),
            })
        except Exception as e:
            # Unique-violation path: someone (a racing process, a replay that beat
            # us) already opened it. Idempotency backstop did its job.
            log.info("entry insert rejected (already open) %s %s: %r",
                     symbol, side.value, e)
            return

        client_id = coid.build(trade_id, OrderIntent.ENTRY, 0)

        # 2. Price the marketable limit off the venue touch + caps.
        bid, ask = await self.hooks.read_touch(symbol)
        bad = validate_touch(bid, ask)
        if bad is not None:
            # Crossed/garbage book: don't open off a wrong-side price. The PENDING
            # trade is terminated REJECTED (no venue order was placed); a later
            # event can open the position when the book is priceable again.
            log.warning("invalid touch for %s (%s) — rejecting entry trade %s",
                        symbol, bad, trade_id)
            await self.hooks.set_trade_state(trade_id, "REJECTED", f"invalid_touch:{bad}")
            return
        cap = await self.hooks.load_cap(asset_class, OrderIntent.ENTRY)
        limit, reference, cap_applied = price_marketable_limit(
            side, OrderIntent.ENTRY, bid, ask, cap)

        order = OrderRequest(
            client_order_id=client_id, trade_id=trade_id, symbol=symbol,
            asset_class=asset_class, intent=OrderIntent.ENTRY, side=side,
            qty=decision.qty, limit_price=limit, venue_reference=reference,
            cap_applied=cap_applied,
        )
        await self.hooks.upsert_order(_order_row(order, OrderStatus.WORKING))

        # 3. Submit. A rejection is a normal outcome (trade -> REJECTED), not an
        #    exception. The brief submit->ack window is covered by boot reconcile
        #    if we crash here: the PENDING trade + deterministic id let us query
        #    the venue on restart.
        result = await self.adapter.submit_order(order)
        await self._apply_order_result(trade_id, side, result)

    # ----- Fill-stream path -------------------------------------------------
    async def on_fill(self, fill: Fill) -> None:
        """Entry point for each streamed venue fill. Dedup is by venue_exec_id at
        write_fill; advancing state happens under the symbol lock so a fill can't
        interleave with a chain event for the same symbol.
        """
        # The fill carries its order's client_order_id where the venue provides
        # it; parse to recover the trade. (Adapters that can't attach it match by
        # venue_exec_id at the DB layer; here we require attribution to advance
        # state, so a fill without a parseable id is logged for the watchdog.)
        cid = getattr(fill, "client_order_id", None)
        if not cid or not coid.is_ours(cid):
            log.warning("fill without attributable cv1 id: exec=%s", fill.venue_exec_id)
            return
        parsed = coid.parse(cid)
        trade_id = parsed.trade_id

        # Need the symbol to lock; the trade row has it. (find-by-trade hook
        # elided for brevity — assume a trade lookup returns symbol+side.)
        trade = await self.hooks.find_trade_by_id(trade_id)
        if trade is None:
            log.warning("fill for unknown trade %s exec=%s", trade_id, fill.venue_exec_id)
            return
        symbol = trade["symbol"]
        side = TradeSide(trade["side"])

        async with self._lock(symbol):
            cum = await self.hooks.write_fill(fill, trade_id)  # idempotent
            await self._advance_on_fill(trade_id, side, parsed.intent, cum, trade)

    async def _advance_on_fill(self, trade_id: str, side: TradeSide,
                               intent: OrderIntent, cum_filled: Decimal,
                               trade: dict) -> None:
        """Advance trade state as filled quantity crosses thresholds. Caller
        holds the symbol lock. Partial-fill aware: ANY nonzero entry fill means
        the position is live and the exit component must own it.
        """
        target = Decimal(str(trade["entry_target_qty"]))
        if intent == OrderIntent.ENTRY:
            if cum_filled > 0 and trade["state"] in ("PENDING", "SUBMITTED"):
                # First fill (even partial) => OPEN and hand to the exit
                # component. OPEN means "we hold a position", NOT "entry done":
                # the remainder may still be working.
                await self.hooks.set_trade_state(trade_id, "OPEN", None)
                await self.hooks.arm_exit(trade_id)
                log.info("trade %s OPEN (filled %s/%s)", trade_id, cum_filled, target)
        else:  # EXIT fill
            exit_target = Decimal(str(trade.get("exit_target_qty", target)))
            if cum_filled >= exit_target:
                await self.hooks.set_trade_state(trade_id, "CLOSED", None)
                log.info("trade %s CLOSED (exit filled %s)", trade_id, cum_filled)

    # ----- shared -----------------------------------------------------------
    async def _apply_order_result(self, trade_id: str, side: TradeSide,
                                  result: OrderResult) -> None:
        """Record a submit/cancel result. Synchronous fills (common for
        marketable limits) are applied immediately; async fills arrive via
        on_fill. Caller holds the symbol lock."""
        await self.hooks.upsert_order(_order_row_from_result(result))
        if result.status == OrderStatus.REJECTED:
            await self.hooks.set_trade_state(
                trade_id, "REJECTED", result.reject_reason or "entry_rejected")
            log.warning("entry rejected trade=%s reason=%s",
                        trade_id, result.reject_reason)
            return
        for f in result.fills:           # synchronous fills, if any
            cum = await self.hooks.write_fill(f, trade_id)
            trade = await self.hooks.find_trade_by_id(trade_id)
            if trade is not None:
                await self._advance_on_fill(trade_id, side, f.intent, cum, trade)


# --- row-shaping helpers (executor-side, map objects -> DB dict rows) -------
def _order_row(order: OrderRequest, status: OrderStatus) -> dict:
    return {
        "client_order_id": order.client_order_id, "trade_id": order.trade_id,
        "intent": order.intent.value, "side": order.side.value,
        "status": status.value, "limit_price": order.limit_price,
        "venue_reference": order.venue_reference, "cap_applied": order.cap_applied,
        "exit_reason": order.exit_reason.value if order.exit_reason else None,
        "escalation_rung": order.escalation_rung, "target_qty": order.qty,
    }


def _order_row_from_result(result: OrderResult) -> dict:
    return {
        "client_order_id": result.client_order_id, "status": result.status.value,
        "venue_order_id": result.venue_order_id, "filled_qty": result.filled_qty,
    }


def _event_price(event: dict):
    md = event.get("metadata") or {}
    return md.get("price")


def _is_chain_open_event(etype: str) -> bool:
    return etype in ("chain_initiated", "chain_propagated")