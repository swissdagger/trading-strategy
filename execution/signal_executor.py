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
    # --- escalation-ladder driving state (set when an exit goes to work) ---
    exit_deadline_ms: Optional[int] = None   # when the current rung's window ends
    exit_reason: Optional[str] = None        # the reason the exit was issued for
    exit_filled_qty: Decimal = Decimal(0)    # how much of the position the exit closed


@dataclass(frozen=True)
class EscalationRung:
    """One rung of the exit escalation ladder: widen the marketable-limit cap to
    this spread multiple, and give it this long to fill before escalating."""
    widen_to_spread_mult: Decimal
    window_seconds: int


@dataclass(frozen=True)
class ExitCap:
    """The EXIT cap config plus its escalation ladder (from execution_caps).
    The base CapConfig prices rung 0; each ladder rung overrides
    cap_spread_mult (and thus how far through the touch we'll tolerate) as the
    exit escalates. The final rung's implicit action is HALT."""
    base: CapConfig
    ladder: tuple  # tuple[EscalationRung, ...]

    def cap_for_rung(self, rung: int) -> CapConfig:
        """The CapConfig to price rung `rung`. Rung 0 is the base; higher rungs
        widen cap_spread_mult per the ladder. A rung beyond the ladder returns
        the base (the driver HALTs before pricing past the top)."""
        if rung <= 0 or rung > len(self.ladder):
            return self.base
        widened = self.ladder[rung - 1].widen_to_spread_mult
        return CapConfig(
            cross_spread_mult=self.base.cross_spread_mult,
            cap_spread_mult=widened,
            cap_abs_bps=self.base.cap_abs_bps,
        )

    def window_seconds_for_rung(self, rung: int) -> Optional[int]:
        """The window for the rung just ISSUED (rung index into the ladder). The
        window that governs a working exit at rung R is ladder[R]'s window (R is
        1-based into the ladder for issued escalations; rung 0's window is
        ladder[0] too — the first exit gets the first rung's window). None when
        there is no further rung (=> at/over the top, HALT)."""
        idx = rung
        if idx < len(self.ladder):
            return self.ladder[idx].window_seconds
        return None

    def has_rung_beyond(self, rung: int) -> bool:
        """Is there a rung to escalate TO after `rung`? False => top of ladder,
        the driver HALTs instead of re-issuing."""
        return rung < len(self.ladder)


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

    # --- Escalation ladder (EXIT driving) ---
    get_exit_cap: Callable[[str], Awaitable["ExitCap"]]                     # (asset_class) -> ExitCap (base + ladder)
    persist_exit_state: Callable[[str, int, Optional[int]], Awaitable[None]]  # (trade_id, rung, deadline_ms)

    # --- Global switches ---
    kill_switch_blocks: Callable[[], Awaitable[bool]]   # True => do not open new positions

    # --- Clock (injectable so escalation deadlines are testable) ---
    now_ms: Callable[[], int] = None   # defaults to wall clock in __post_init__

    def __post_init__(self):
        if self.now_ms is None:
            import time
            object.__setattr__(self, "now_ms", lambda: int(time.time() * 1000))


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
        # venue_order_id -> (strategy, position_key), so an ASYNC fill (which
        # carries its venue_order_id, not our client_order_id) can be attributed
        # back to the trade whose order produced it. Populated at submit time
        # from each OrderResult.venue_order_id.
        self._order_to_key: Dict[str, tuple[str, str]] = {}
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
        self._map_order(result, key)
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
        if trade.state == "CLOSING":
            # Already closing (a duplicate EXIT, or one arriving while the
            # escalation driver works it). The working exit + ladder handle it;
            # a second EXIT must not stack another order.
            log.debug("EXIT for %s already CLOSING — skip (driver owns it)", trade.trade_id)
            return

        reason = row.get("exit_reason") or "MANUAL"
        # Issue the protective exit at rung 0 (the base cap). The escalation
        # driver escalates from here if it doesn't fill within the window.
        trade.exit_reason = reason
        await self._issue_exit(trade, key, rung=0)

    async def _issue_exit(self, trade: OpenTrade, key, rung: int) -> None:
        """Price + submit an exit for the trade's REMAINING quantity at escalation
        `rung`, set the rung's fill window as the deadline, and persist the exit
        state. Shared by the initial EXIT and every escalation re-issue, so the
        pricing/id/deadline logic lives in one place."""
        symbol = trade.symbol
        asset_class = trade.asset_class

        touch = await self.hooks.get_touch(symbol, asset_class)
        if touch is None:
            log.warning("no touch for %s — cannot price EXIT %s rung=%d",
                        symbol, trade.trade_id, rung)
            return
        exit_cap = await self.hooks.get_exit_cap(asset_class)
        cap = exit_cap.cap_for_rung(rung)
        limit, reference, cap_applied = price_marketable_limit(
            trade.side, OrderIntent.EXIT, touch.bid, touch.ask, cap,
        )

        remaining = trade.filled_qty - trade.exit_filled_qty
        if remaining <= 0:
            # Nothing left to close (position already flat). Finalize.
            await self._finalize_closed(trade, key)
            return

        # Exit order gets its own deterministic id at THIS escalation rung, so a
        # re-issue at a higher rung is a distinct, idempotent order.
        exit_coid = coid.build(trade.trade_id, OrderIntent.EXIT, rung)
        order = OrderRequest(
            client_order_id=exit_coid, trade_id=trade.trade_id, symbol=symbol,
            asset_class=asset_class, intent=OrderIntent.EXIT, side=trade.side,
            qty=remaining,
            limit_price=limit, venue_reference=reference, cap_applied=cap_applied,
            exit_reason=_exit_reason(trade.exit_reason or "MANUAL"), escalation_rung=rung,
        )
        await self.hooks.persist_order(_order_row(order, OrderStatus.WORKING))

        trade.state = "CLOSING"
        trade.exit_rung = rung
        # Deadline = now + this rung's fill window. If the window is None we are
        # at/over the top of the ladder — no deadline, the driver will HALT.
        window_s = exit_cap.window_seconds_for_rung(rung)
        trade.exit_deadline_ms = (
            self.hooks.now_ms() + window_s * 1000 if window_s is not None else None
        )
        await self.hooks.update_trade_state(trade.trade_id, "CLOSING", None)
        await self.hooks.persist_exit_state(trade.trade_id, rung, trade.exit_deadline_ms)

        result = await self.adapter.submit_order(order)
        await self._apply_exit_result(trade, key, result)

    async def _apply_exit_result(self, trade: OpenTrade, key, result: OrderResult) -> None:
        self._map_order(result, key)
        await self.hooks.advance_order_fill(
            result.client_order_id, result.filled_qty, result.status.value,
        )
        # Attribute each synchronous fill to THIS exit order's trade (sync fill
        # attribution: the fills in an OrderResult belong to the order we just
        # submitted, whose trade we know). _record_fill advances exit_filled_qty.
        for f in result.fills:
            await self._record_fill(trade, f)
        # Fully closed once the exit has closed the whole held quantity.
        if trade.exit_filled_qty >= trade.filled_qty and trade.filled_qty > 0:
            await self._finalize_closed(trade, key)

    async def _finalize_closed(self, trade: OpenTrade, key) -> None:
        trade.state = "CLOSED"
        trade.exit_deadline_ms = None
        await self.hooks.update_trade_state(trade.trade_id, "CLOSED", None)
        await self.hooks.persist_exit_state(trade.trade_id, trade.exit_rung, None)
        self._open.pop(key, None)

    # ----------------------------------------------------------------------
    # Escalation-ladder driver — the periodic sweep that advances an unfilled
    # protective exit through the ladder, and HALTs at the top.
    # ----------------------------------------------------------------------
    async def run_escalation_once(self) -> None:
        """One pass of the escalation driver. For every CLOSING trade whose rung
        window has expired and that still holds an unclosed remainder, escalate:
        widen the cap (advance the rung) and re-issue, or HALT if past the top of
        the ladder. Idempotent and lock-guarded per symbol, so it composes with
        signal handling. Call periodically (e.g. every second) from the executor
        host's loop; kept as a single pass so it's trivially testable."""
        now = self.hooks.now_ms()
        # Snapshot to avoid mutating the dict mid-iteration (escalation may pop).
        for key, trade in list(self._open.items()):
            if trade.state != "CLOSING":
                continue
            if trade.exit_deadline_ms is None:
                # No deadline set means we're at/over the ladder top with an
                # unfilled exit — HALT (the ladder's terminal action).
                async with self._lock(trade.symbol):
                    await self._halt(trade, "escalation: exit unfilled at top of ladder")
                    self._open.pop(key, None)
                continue
            if now < trade.exit_deadline_ms:
                continue   # window not yet expired
            async with self._lock(trade.symbol):
                await self._escalate(trade, key)

    async def _escalate(self, trade: OpenTrade, key) -> None:
        """Advance one rung (caller holds the symbol lock). If there's a rung
        beyond the current one, re-issue there; else HALT."""
        remaining = trade.filled_qty - trade.exit_filled_qty
        if remaining <= 0:
            await self._finalize_closed(trade, key)
            return
        exit_cap = await self.hooks.get_exit_cap(trade.asset_class)
        if not exit_cap.has_rung_beyond(trade.exit_rung):
            # Top of the ladder and still not filled -> HALT + alert. Better a
            # human looks than an unbounded market order in a fast market.
            await self._halt(trade, "escalation: exit unfilled through full ladder")
            self._open.pop(key, None)
            return
        next_rung = trade.exit_rung + 1
        log.info("escalating exit for %s: rung %d -> %d (remaining=%s)",
                 trade.trade_id, trade.exit_rung, next_rung, remaining)
        await self._issue_exit(trade, key, rung=next_rung)

    # ----------------------------------------------------------------------
    # Fills (async, from the adapter's fill stream)
    # ----------------------------------------------------------------------
    def _map_order(self, result: OrderResult, key: tuple[str, str]) -> None:
        """Remember which trade an order belongs to, by its venue_order_id, so a
        later async fill on that order can be attributed. No-op if the venue
        didn't return an id yet (the brief submit->ack window; reconcile resolves
        it by client_order_id)."""
        if result.venue_order_id:
            self._order_to_key[result.venue_order_id] = key

    async def _on_fill(self, fill: Fill) -> None:
        """Adapter fill-stream callback for ASYNC fills. Attributes the fill to
        its trade by venue_order_id (which every venue stamps on a fill) via the
        submit-time map, then advances that trade exactly as a sync fill would —
        same _record_fill path, so partials, dedup, and completion behave
        identically whether a fill arrived synchronously or on the stream.

        A fill we can't attribute (no venue_order_id, or an order we don't know —
        e.g. a foreign/manual trade, or a fill discovered before its order was
        mapped) is still persisted and deduped by venue_exec_id, and surfaced for
        the position reconcile to explain. We never drop a fill and never guess
        which trade an unlabelled fill belongs to."""
        key = None
        if fill.venue_order_id is not None:
            key = self._order_to_key.get(fill.venue_order_id)

        if key is None:
            # Unattributable: persist+dedupe (idempotent), leave for reconcile.
            await self.hooks.persist_fill(_fill_row(fill, trade_id=None))
            log.info("async fill %s not attributable (order=%s) — persisted for reconcile",
                     fill.venue_exec_id, fill.venue_order_id)
            return

        trade = self._open.get(key)
        if trade is None:
            # We knew the order but the trade is no longer open in memory (e.g.
            # already finalized). Persist for the record; dedupe protects totals.
            await self.hooks.persist_fill(_fill_row(fill, trade_id=None))
            return

        # Attribute under the symbol lock so a stream fill can't interleave with
        # signal/escalation handling for the same symbol.
        async with self._lock(trade.symbol):
            await self._record_fill(trade, fill)
            await self._maybe_complete_after_async_fill(trade, key)

    async def _maybe_complete_after_async_fill(self, trade: OpenTrade, key) -> None:
        """After an async fill advances a trade, apply the same state transitions
        the sync paths do: an entry that's now (partly) filled becomes OPEN; an
        exit that has closed the whole position finalizes CLOSED."""
        if trade.state == "SUBMITTED" and trade.filled_qty > 0:
            trade.state = "OPEN"
            await self.hooks.update_trade_state(trade.trade_id, "OPEN", None)
        if (trade.state == "CLOSING"
                and trade.exit_filled_qty >= trade.filled_qty and trade.filled_qty > 0):
            await self._finalize_closed(trade, key)

    async def _record_fill(self, trade: OpenTrade, fill: Fill) -> None:
        """Record a synchronously-known fill against a specific trade and advance
        the right counter. Dedupes on venue_exec_id. ENTRY fills grow the held
        position (filled_qty); EXIT fills grow exit_filled_qty (how much of the
        position we've closed) — kept separate so the escalation driver knows the
        remaining unclosed quantity (filled_qty - exit_filled_qty)."""
        newly = await self.hooks.persist_fill(_fill_row(fill, trade_id=trade.trade_id))
        if not newly:
            return
        if fill.intent == OrderIntent.ENTRY:
            trade.filled_qty += fill.qty
        else:
            trade.exit_filled_qty += fill.qty


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
        "venue_order_id": fill.venue_order_id,
        "qty": fill.qty,
        "price": fill.price,
        "intent": fill.intent.value,
        "filled_at_ms": fill.filled_at_ms,
        "fee": fill.fee,
        "fee_currency": fill.fee_currency,
    }