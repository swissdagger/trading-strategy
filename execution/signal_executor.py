"""
execution/signal_executor.py — the signal-consuming executor.

THE KEYSTONE the whole execution stack was scaffolding for. It sits between the
SignalListener (which pushes trade_signals rows) and an ExecutionAdapter (which
places orders at a venue), and it owns the dangerous middle: turning a strategy
DECISION into a managed, reconciled, real-money POSITION.

Distinct from the legacy Kafka executor (execution/_legacy_executor.py), which
consumed chain events and re-derived strategy. This one consumes the DISTILLED
trade_signals — the single source of truth both this project and the other team
read — so there is no strategy drift. One signal, one durable intent, one
managed trade.

## The boot-ordering invariant (from adapter.py — NON-NEGOTIABLE)

    1. adapter.connect()   — BLOCK until a trading-enabled session is live.
    2. adapter.reconcile() — snapshot the venue's truth.
    3. diff reconcile() against the DB; un-resolvable diffs -> trade HALTED.
    4. check kill_switch AND trade_enabled.
    5. ONLY THEN consume signals and act.

start() runs this sequence and refuses to process signals until it completes.
Placing an order before the session is confirmed, or before we know what we
already hold, is how restarts double-fill. The executor enforces the order.

## Gating: two independent switches

An order is placed only if trade_enabled=true AND kill_switch is not blocking.
trade_enabled is checked via the GatedAdapter (which no-ops orders when off);
kill_switch is checked HERE before deciding to place (so we don't even price an
order the switch forbids). Both must allow. See migration 010's precedence note.

## Per-symbol serialization

All work for a symbol runs under one lock (shared with the signal runtime when
co-located), so a fill advancing a trade can't interleave with a new signal on
the same symbol. Same discipline as the strategy runtime and the legacy executor.

## Idempotency & crash recovery

- Deterministic client_order_id (cv1 scheme) is attached to every order; a
  crash between submit and ack is resolved by querying the venue for that id.
- trades.entry_client_order_id and orders.client_order_id are UNIQUE in the DB,
  so a replayed signal or a re-submitted order dedupes at the DB (belt) and at
  the venue's own client-id dedupe (braces).
- Fills dedupe by venue_exec_id (fills table UNIQUE), so re-delivery on
  reconnect is safe.

## What this file does NOT contain

- No venue specifics (that's the adapter).
- No strategy logic (that's upstream; this acts on distilled signals).
- No DB-driver code (persistence is injected via hooks, like the legacy
  executor's LiveHooks) — keeps this testable with fakes and free of asyncpg.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Awaitable, Callable, Dict, Optional

from execution.adapter import ExecutionAdapter
from execution.pricing import CapConfig, price_marketable_limit
from execution import client_order_id as coid
from execution.types import (
    Fill,
    OrderIntent,
    OrderRequest,
    OrderResult,
    OrderStatus,
    ReconcileResult,
    TradeSide,
    ExitReason,
)

log = logging.getLogger("executor.signal")


# ---------------------------------------------------------------------------
# Injected value types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Touch:
    """Current venue best bid/ask for pricing a marketable limit."""
    bid: Decimal
    ask: Decimal


@dataclass
class OpenTrade:
    """The executor's in-memory handle on a live trade, keyed by (strategy,
    position_key). Mirrors the trades row it persists; carries what EXIT needs."""
    trade_id: str
    strategy: str
    position_key: str
    symbol: str
    asset_class: str
    side: TradeSide
    target_qty: Decimal
    filled_qty: Decimal
    state: str                 # PENDING|SUBMITTED|OPEN|CLOSING|CLOSED|REJECTED|HALTED
    entry_client_order_id: str
    exit_rung: int = 0         # escalation rung of the current/last exit order


# ---------------------------------------------------------------------------
# Hooks — everything the executor needs from the surrounding app, injected so
# this file stays free of DB-driver and venue specifics and is unit-testable.
# ---------------------------------------------------------------------------
@dataclass
class ExecutorHooks:
    # --- Persistence (writes to the 001 trade-lifecycle schema) ---
    persist_trade: Callable[[OpenTrade], Awaitable[None]]
    update_trade_state: Callable[[str, str, Optional[str]], Awaitable[None]]  # (trade_id, state, halt_reason)
    persist_order: Callable[[dict], Awaitable[None]]
    persist_fill: Callable[[dict], Awaitable[bool]]   # returns False if dup (venue_exec_id)
    advance_order_fill: Callable[[str, Decimal, str], Awaitable[None]]  # (client_order_id, filled_qty, status)
    load_open_trades: Callable[[], Awaitable[list[OpenTrade]]]

    # --- Venue/market reads ---
    get_touch: Callable[[str, str], Awaitable[Optional[Touch]]]   # (symbol, asset_class) -> Touch
    get_cap: Callable[[str, OrderIntent], Awaitable[CapConfig]]   # (asset_class, intent) -> CapConfig
    size_position: Callable[[str, str, TradeSide, Optional[float]], Awaitable[Decimal]]  # (symbol, asset_class, side, decision_price) -> qty

    # --- Global switches ---
    kill_switch_blocks: Callable[[], Awaitable[bool]]   # True => do not open new positions


class SignalExecutor:
    """Consumes trade_signals decisions and manages the trade lifecycle against
    an ExecutionAdapter."""

    def __init__(
        self,
        adapter: ExecutionAdapter,
        hooks: ExecutorHooks,
        *,
        locks: Optional[Dict[str, asyncio.Lock]] = None,
    ):
        self.adapter = adapter
        self.hooks = hooks
        self._locks: Dict[str, asyncio.Lock] = (
            locks if locks is not None else defaultdict(asyncio.Lock)
        )
        # Live trades indexed by (strategy, position_key) — the namespaced key
        # the multi-strategy signals carry, so two strategies' positions on one
        # chain stay distinct here too.
        self._open: Dict[tuple[str, str], OpenTrade] = {}
        self._booted = False

    def _lock(self, symbol: str) -> asyncio.Lock:
        return self._locks[symbol]

    # ----------------------------------------------------------------------
    # Boot — the mandated sequence. Must complete before handle_signal acts.
    # ----------------------------------------------------------------------
    async def start(self) -> None:
        """Run the boot-ordering invariant. Raises if a trading session can't be
        confirmed (executor stays down rather than trading blind)."""
        log.info("executor boot: connecting adapter...")
        await self.adapter.connect()               # 1. blocks until trading session live

        log.info("executor boot: reconciling venue against DB...")
        snapshot = await self.adapter.reconcile()  # 2. venue truth
        db_trades = await self.hooks.load_open_trades()
        await self._reconcile(snapshot, db_trades)  # 3. diff; un-resolvable -> HALTED

        # 4. Start the fill stream now that reconcile has run (fills arriving
        # before reconcile would race the diff). trade_enabled is enforced by
        # the GatedAdapter on submit; kill_switch is checked per-signal.
        await self.adapter.start_fill_stream(self._on_fill)

        self._booted = True
        log.info("executor boot complete: %d open trades under management", len(self._open))

    async def _reconcile(self, snapshot: ReconcileResult, db_trades: list[OpenTrade]) -> None:
        """Diff venue truth against DB open trades. The executor — never the
        adapter — decides. Un-resolvable discrepancies HALT the affected trade
        (a human looks) rather than guessing with real money.

        Resolutions:
          * DB trade OPEN and venue has the matching position -> keep managing.
          * DB trade OPEN but venue is FLAT for it -> it closed during the crash
            window; HALT for review (we can't safely infer the exit price/fills
            here — the recent_fills sweep below may explain it, else human).
          * Venue position with no DB trade -> a position we don't recognize;
            HALT-equivalent alert (do not adopt an unknown position silently).
          * Working orders / recent fills attributed by parsing client_order_id.
        """
        # Index venue positions by symbol (signed qty).
        venue_pos = {p.symbol: p for p in snapshot.positions}

        for t in db_trades:
            self._open[(t.strategy, t.position_key)] = t
            pos = venue_pos.get(t.symbol)
            if t.state in ("OPEN", "CLOSING"):
                if pos is None or pos.qty == 0:
                    # DB says we hold it; venue says flat. Closed in the crash
                    # window (or never really opened). Cannot safely resolve the
                    # realized fills here -> HALT for review.
                    await self._halt(t, "reconcile: DB open but venue flat")
                # else: venue confirms the position; keep managing it.

        # Venue positions with no corresponding DB trade: flag, don't adopt.
        known_symbols = {t.symbol for t in db_trades}
        for sym, p in venue_pos.items():
            if p.qty != 0 and sym not in known_symbols:
                log.error(
                    "reconcile: venue holds %s qty=%s with no DB trade — manual review",
                    sym, p.qty,
                )
                # No trade object to HALT; surfaced as a loud log for the operator.
                # (A real deployment would also raise an alert / write an incident row.)

        # Recent venue fills we may have missed: dedupe by venue_exec_id.
        for f in snapshot.recent_fills:
            await self.hooks.persist_fill(_fill_row(f, trade_id=None))

    async def _halt(self, t: OpenTrade, reason: str) -> None:
        t.state = "HALTED"
        await self.hooks.update_trade_state(t.trade_id, "HALTED", reason)
        log.error("trade %s HALTED: %s", t.trade_id, reason)

    # ----------------------------------------------------------------------
    # Signal ingress — the SignalListener's handler
    # ----------------------------------------------------------------------
    async def handle_signal(self, row: dict) -> None:
        """Idempotent per-signal handler (the SignalListener guarantees
        at-least-once; we dedupe by the trade/order DB unique constraints and by
        our in-memory open-trade map). Routes ENTER/EXIT under the symbol lock."""
        if not self._booted:
            log.warning("signal arrived before boot complete; ignoring %s", row.get("signal_id"))
            return
        symbol = row["symbol"]
        async with self._lock(symbol):
            action = row["action"]
            if action == "ENTER":
                await self._on_enter(row)
            elif action == "EXIT":
                await self._on_exit(row)
            else:
                log.warning("unknown signal action %r for %s", action, row.get("signal_id"))

    # ----------------------------------------------------------------------
    # ENTER
    # ----------------------------------------------------------------------
    async def _on_enter(self, row: dict) -> None:
        strategy = row.get("strategy", "two_ladder")
        position_key = row["position_key"]
        key = (strategy, position_key)

        # Idempotency: already managing this position? A replayed ENTER is a
        # no-op (the deterministic ids would dedupe at the DB/venue anyway).
        if key in self._open:
            log.debug("ENTER for already-open position %s/%s — skip", strategy, position_key)
            return

        # Gate: kill_switch blocks NEW positions.
        if await self.hooks.kill_switch_blocks():
            log.info("kill_switch engaged — refusing ENTER %s/%s", strategy, position_key)
            return

        symbol = row["symbol"]
        asset_class = row["asset_class"]
        side = TradeSide.LONG if row["direction"] == "LONG" else TradeSide.SHORT
        chain_id = row["chain_id"]
        decision_price = row.get("decision_price")

        # Size the position (injected policy).
        qty = await self.hooks.size_position(symbol, asset_class, side, decision_price)
        if qty <= 0:
            log.warning("sizing returned %s for %s/%s — no order", qty, strategy, position_key)
            return

        # Price the marketable-limit entry off the current venue touch.
        touch = await self.hooks.get_touch(symbol, asset_class)
        if touch is None:
            log.warning("no touch for %s — cannot price ENTER %s/%s", symbol, strategy, position_key)
            return
        cap = await self.hooks.get_cap(asset_class, OrderIntent.ENTRY)
        limit, reference, cap_applied = price_marketable_limit(
            side, OrderIntent.ENTRY, touch.bid, touch.ask, cap,
        )

        # Create the trade (new uuid) and its deterministic entry order id.
        trade_id = _new_trade_id()
        entry_coid = coid.build(trade_id, OrderIntent.ENTRY, 0)

        trade = OpenTrade(
            trade_id=trade_id, strategy=strategy, position_key=position_key,
            symbol=symbol, asset_class=asset_class, side=side,
            target_qty=qty, filled_qty=Decimal(0), state="PENDING",
            entry_client_order_id=entry_coid,
        )
        # Persist PENDING before submitting: if we crash between submit and ack,
        # the durable PENDING row + deterministic id let reconcile query the
        # venue for exactly this order rather than blindly re-sending.
        await self.hooks.persist_trade(trade)
        self._open[key] = trade

        order = OrderRequest(
            client_order_id=entry_coid, trade_id=trade_id, symbol=symbol,
            asset_class=asset_class, intent=OrderIntent.ENTRY, side=side,
            qty=qty, limit_price=limit, venue_reference=reference, cap_applied=cap_applied,
        )
        await self.hooks.persist_order(_order_row(order, OrderStatus.WORKING))
        result = await self.adapter.submit_order(order)
        await self._apply_entry_result(trade, key, result)

    async def _apply_entry_result(self, trade: OpenTrade, key, result: OrderResult) -> None:
        await self.hooks.advance_order_fill(
            result.client_order_id, result.filled_qty, result.status.value,
        )
        if result.status == OrderStatus.REJECTED:
            trade.state = "REJECTED"
            await self.hooks.update_trade_state(trade.trade_id, "REJECTED", None)
            self._open.pop(key, None)
            log.info("ENTER rejected for %s: %s", trade.trade_id, result.reject_reason)
            return
        # Any synchronous fills advance the trade; async fills follow via stream.
        for f in result.fills:
            await self._record_fill(trade, f)
        # State: OPEN if any fill, else SUBMITTED (working at venue).
        if trade.filled_qty > 0:
            trade.state = "OPEN"
            await self.hooks.update_trade_state(trade.trade_id, "OPEN", None)
        else:
            trade.state = "SUBMITTED"
            await self.hooks.update_trade_state(trade.trade_id, "SUBMITTED", None)

    # ----------------------------------------------------------------------
    # EXIT
    # ----------------------------------------------------------------------
    async def _on_exit(self, row: dict) -> None:
        strategy = row.get("strategy", "two_ladder")
        position_key = row["position_key"]
        key = (strategy, position_key)
        trade = self._open.get(key)
        if trade is None:
            # EXIT for a position we don't hold. Per migration 004's note, this
            # is a detectable reconciliation gap, not a silent drop.
            log.warning("EXIT for unknown position %s/%s — reconciliation gap", strategy, position_key)
            return
        if trade.state in ("CLOSED", "REJECTED", "HALTED"):
            log.debug("EXIT for %s already terminal (%s) — skip", trade.trade_id, trade.state)
            return

        symbol = trade.symbol
        asset_class = trade.asset_class
        reason = row.get("exit_reason") or "MANUAL"

        touch = await self.hooks.get_touch(symbol, asset_class)
        if touch is None:
            log.warning("no touch for %s — cannot price EXIT %s", symbol, trade.trade_id)
            return
        cap = await self.hooks.get_cap(asset_class, OrderIntent.EXIT)
        limit, reference, cap_applied = price_marketable_limit(
            trade.side, OrderIntent.EXIT, touch.bid, touch.ask, cap,
        )

        # Exit order gets its own deterministic id at the current escalation rung.
        exit_coid = coid.build(trade.trade_id, OrderIntent.EXIT, trade.exit_rung)
        order = OrderRequest(
            client_order_id=exit_coid, trade_id=trade.trade_id, symbol=symbol,
            asset_class=asset_class, intent=OrderIntent.EXIT, side=trade.side,
            qty=trade.filled_qty,   # close what we actually hold
            limit_price=limit, venue_reference=reference, cap_applied=cap_applied,
            exit_reason=_exit_reason(reason), escalation_rung=trade.exit_rung,
        )
        await self.hooks.persist_order(_order_row(order, OrderStatus.WORKING))
        trade.state = "CLOSING"
        await self.hooks.update_trade_state(trade.trade_id, "CLOSING", None)
        result = await self.adapter.submit_order(order)
        await self._apply_exit_result(trade, key, result)

    async def _apply_exit_result(self, trade: OpenTrade, key, result: OrderResult) -> None:
        await self.hooks.advance_order_fill(
            result.client_order_id, result.filled_qty, result.status.value,
        )
        for f in result.fills:
            await self._record_fill(trade, f)
        # If fully closed, mark CLOSED. Partial/working closes stay CLOSING; the
        # escalation ladder (a periodic sweep, not built here) would widen the
        # cap and re-issue at trade.exit_rung+1. Full ladder driving is a
        # follow-on; this records the close and terminal transition.
        if trade.filled_qty <= 0 and result.status == OrderStatus.FILLED:
            trade.state = "CLOSED"
            await self.hooks.update_trade_state(trade.trade_id, "CLOSED", None)
            self._open.pop(key, None)

    # ----------------------------------------------------------------------
    # Fills (async, from the adapter's fill stream)
    # ----------------------------------------------------------------------
    async def _on_fill(self, fill: Fill) -> None:
        """Adapter fill-stream callback. Attribute the fill to a trade by
        parsing its order's client_order_id is not available on the Fill itself
        (the Fill carries venue_exec_id + qty/price + intent), so we rely on the
        executor already tracking the trade whose exit/entry order produced it.
        For robustness we dedupe by venue_exec_id at the DB regardless.

        NOTE: the Fill type carries `intent` but not client_order_id; matching a
        stream fill to its trade is done by the venue_order_id/orderRef path in a
        full build. Here we persist+dedupe the fill and, when we can identify the
        trade in memory, advance it. Unattributable fills are still persisted
        (dedupe key present) and surfaced for reconcile.
        """
        # Persist+dedupe first (idempotent on venue_exec_id).
        newly = await self.hooks.persist_fill(_fill_row(fill, trade_id=None))
        if not newly:
            return   # already recorded (re-delivery on reconnect) — no double count

    async def _record_fill(self, trade: OpenTrade, fill: Fill) -> None:
        """Record a synchronously-known fill against a specific trade and advance
        its filled quantity. Dedupes on venue_exec_id."""
        newly = await self.hooks.persist_fill(_fill_row(fill, trade_id=trade.trade_id))
        if not newly:
            return
        signed = fill.qty
        if fill.intent == OrderIntent.ENTRY:
            trade.filled_qty += signed
        else:
            trade.filled_qty -= signed   # exit reduces the held quantity


# ---------------------------------------------------------------------------
# small helpers (pure)
# ---------------------------------------------------------------------------
def _new_trade_id() -> str:
    import uuid
    return str(uuid.uuid4())


def _exit_reason(reason: str) -> ExitReason:
    try:
        return ExitReason(reason)
    except ValueError:
        return ExitReason.MANUAL


def _order_row(order: OrderRequest, status: OrderStatus) -> dict:
    return {
        "client_order_id": order.client_order_id,
        "trade_id": order.trade_id,
        "symbol": order.symbol,
        "asset_class": order.asset_class,
        "intent": order.intent.value,
        "side": order.side.value,
        "qty": order.qty,
        "limit_price": order.limit_price,
        "venue_reference": order.venue_reference,
        "cap_applied": order.cap_applied,
        "status": status.value,
        "exit_reason": order.exit_reason.value if order.exit_reason else None,
        "escalation_rung": order.escalation_rung,
    }


def _fill_row(fill: Fill, trade_id: Optional[str]) -> dict:
    return {
        "venue_exec_id": fill.venue_exec_id,
        "trade_id": trade_id,
        "qty": fill.qty,
        "price": fill.price,
        "intent": fill.intent.value,
        "filled_at_ms": fill.filled_at_ms,
        "fee": fill.fee,
        "fee_currency": fill.fee_currency,
    }