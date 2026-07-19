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
import time
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Awaitable, Callable, Dict, Optional

from execution.adapter import ExecutionAdapter
from execution.observability import Metrics, build_health_document
from execution.pricing import CapConfig, price_marketable_limit, validate_touch
from execution import client_order_id as coid
from execution.types import (
    Fill,
    OrderIntent,
    OrderRequest,
    OrderResult,
    OrderStatus,
    ReconcileResult,
    Touch,
    TradeSide,
    ExitReason,
    qty_match_epsilon,
)

log = logging.getLogger("executor.signal")

# How long to wait before retrying an exit whose book couldn't be priced (no or
# crossed/invalid touch). Short, because an unpriceable protective exit is urgent:
# we want the driver to come back and try again within a couple of ticks, not sit
# on a long window. Kept well under a typical rung fill window.
_EXIT_RETRY_MS = 2_000


# ---------------------------------------------------------------------------
# Injected value types
# ---------------------------------------------------------------------------

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
    # True once the CURRENT rung's order has actually been submitted to the venue.
    # False while a CLOSING trade is committed to exit but could not yet be priced
    # (no/invalid touch). The escalation driver uses this to RETRY the same rung
    # (order was never placed) vs. ESCALATE to the next rung (a placed order didn't
    # fill in its window). In-memory only; a trade re-loaded on restart defaults to
    # True (boot reconcile independently re-issues a vanished close order).
    exit_order_submitted: bool = True

    @property
    def open_qty(self) -> Decimal:
        """Signed position we believe we hold: + for LONG, - for SHORT. Net of the
        exit fills so far. Used by reconcile to compare against the venue's signed
        position (size AND sign), not just 'flat vs not'."""
        net = self.filled_qty - self.exit_filled_qty
        return net if self.side == TradeSide.LONG else -net


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
        metrics: Optional["Metrics"] = None,
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
        # Attribution maps for ASYNC fills arriving on the adapter's fill stream.
        #
        # PRIMARY: client_order_id -> (strategy, position_key). The client id is
        # DETERMINISTIC and known BEFORE we submit, so we register it FIRST (in
        # _register_order, called ahead of submit_order). This closes the race
        # where a fill streams in during the submit->ack window: even before the
        # venue tells us the order's venue_order_id, the fill's client_order_id
        # (which the adapter stamps when it can) resolves to the right trade.
        self._coid_to_key: Dict[str, tuple[str, str]] = {}
        # BRIDGE: venue_order_id -> client_order_id, learned at ack. Lets a fill
        # that carries ONLY a venue_order_id (an adapter that can't stamp the
        # client id on the stream) still resolve through to the key.
        self._venue_to_coid: Dict[str, str] = {}
        self._booted = False
        # Signals the escalation loop to exit and marks the executor stopping.
        # Shared by run_escalation_loop() and stop() so the host has one switch.
        self._stop = asyncio.Event()
        # Optional observability. metrics is a plain counter store (or None, in
        # which case every _m*() call is a no-op — the executor never depends on
        # it). last_tick_at is the wall-clock of the escalation loop's most
        # recent pass; health uses its age to detect a wedged loop.
        self._metrics = metrics
        self._last_tick_at: Optional[float] = None

    def _lock(self, symbol: str) -> asyncio.Lock:
        return self._locks[symbol]

    # ----------------------------------------------------------------------
    # Observability — all no-ops when no Metrics was injected, so instrumenting
    # a hot path never forces the dependency and never branches at the call site
    # beyond this one guard.
    # ----------------------------------------------------------------------
    def _mincr(self, name: str, by: int = 1) -> None:
        if self._metrics is not None:
            self._metrics.incr(name, by)

    def _mgauge(self, name: str, value: float) -> None:
        if self._metrics is not None:
            self._metrics.gauge(name, value)

    def health_snapshot(self) -> dict:
        """The /health document: liveness (booted + escalation loop ticking) plus
        current open/closing/halted counts and the metrics snapshot. Pass this
        method as HealthServer's snapshot_fn. Cheap and side-effect-free, so it's
        safe to call on every health probe.

        Uses a throwaway Metrics if none was injected, so the surface still
        serves a coherent document (with zeroed counters) rather than erroring
        when the executor is run without observability wired."""
        closing = sum(1 for t in self._open.values() if t.state == "CLOSING")
        halted = sum(1 for t in self._open.values() if t.state == "HALTED")
        return build_health_document(
            booted=self._booted,
            last_tick_at=self._last_tick_at,
            open_trades=len(self._open),
            closing_trades=closing,
            halted_trades=halted,
            metrics=self._metrics if self._metrics is not None else Metrics(),
        )

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
        self._mgauge("executor_booted", 1)
        self._mgauge("open_trades", len(self._open))
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
                eps = qty_match_epsilon(t.asset_class)
                venue_qty = pos.qty if pos is not None else Decimal(0)
                if abs(venue_qty) <= eps:
                    # DB says we hold it; venue says flat. Closed in the crash
                    # window (or never really opened). Cannot safely resolve the
                    # realized fills here -> HALT for review.
                    await self._halt(t, "reconcile: DB open but venue flat")
                elif abs(venue_qty - t.open_qty) > eps:
                    # Venue holds a DIFFERENT size/sign than we think — a partial
                    # close (or extra fill) during downtime we can't explain here.
                    # HALT rather than silently manage a position we can't reconcile
                    # to our quantity (previously this branch went undetected: we
                    # only checked flat-vs-not and adopted any nonzero position).
                    await self._halt(
                        t, f"reconcile: position mismatch db={t.open_qty} venue={venue_qty}")
                # else: venue confirms size AND sign; keep managing it.

        # Venue positions with no corresponding DB trade: flag, don't adopt. Use
        # the crypto epsilon as a permissive floor for the unknown-symbol dust test
        # (we have no trade to key asset_class on; a real orphan is a whole unit).
        known_symbols = {t.symbol for t in db_trades}
        for sym, p in venue_pos.items():
            if abs(p.qty) > qty_match_epsilon("") and sym not in known_symbols:
                log.error(
                    "reconcile: venue holds %s qty=%s with no DB trade — manual review",
                    sym, p.qty,
                )
                # No trade object to HALT; surfaced as a loud log for the operator.
                # (A real deployment would also raise an alert / write an incident row.)

        # Recent venue fills we may have missed during the crash window. Attribute
        # each to its trade by parsing the cv1 client id (fills carry it, or it's
        # reachable via the venue bridge); persist against that real trade_id so
        # the FK is satisfied and the fill is recorded. Dedupe by venue_exec_id
        # protects against re-read. A fill we can't attribute to one of our trades
        # is foreign and skipped.
        for f in snapshot.recent_fills:
            trade_id = self._trade_id_for_fill(f)
            if trade_id is None:
                self._mincr("reconcile_fills_unattributable")
                continue
            await self.hooks.persist_fill(_fill_row(f, trade_id=trade_id))

    async def _halt(self, t: OpenTrade, reason: str) -> None:
        t.state = "HALTED"
        self._mincr("trades_halted")
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
            self._mincr("signals_handled")
            if action == "ENTER":
                await self._on_enter(row)
            elif action == "EXIT":
                await self._on_exit(row)
            else:
                self._mincr("signals_unknown_action")
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
        bad = validate_touch(touch.bid, touch.ask)
        if bad is not None:
            # Crossed/garbage book — don't build a wrong-side entry off it. An
            # entry is discretionary, so skipping is safe: a later signal (or a
            # re-fetched touch) can still open the position.
            log.warning("invalid touch for %s (%s) — skipping ENTER %s/%s",
                        symbol, bad, strategy, position_key)
            self._mincr("entries_skipped_bad_touch")
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
        # Register attribution BEFORE submit: a fill can stream in before
        # submit_order even returns, and it must resolve to this trade.
        self._register_order(entry_coid, key)
        result = await self.adapter.submit_order(order)
        await self._apply_entry_result(trade, key, result)

    async def _apply_entry_result(self, trade: OpenTrade, key, result: OrderResult) -> None:
        self._learn_venue_order_id(result)
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
        # INTERLOCK (remove in the partial-exit rollout step): this executor
        # closes the WHOLE remaining position on any EXIT. A SCALE tranche
        # (migration 016, fraction < 1) reaching it would flatten the position
        # at the first 0.05 tranche — a silent 20x over-close. Refuse loudly
        # instead; the signal row is durable, so once partial support ships the
        # catch-up sweep can still act on it if the position remains open.
        fraction = row.get("fraction")
        if reason == "SCALE" or (fraction is not None and float(fraction) < 1.0):
            self._mincr("exits_refused_partial_unsupported")
            log.error(
                "partial EXIT %s (reason=%s fraction=%s) for %s/%s but this "
                "executor has no partial-exit support — refusing (deploy-order "
                "guard; enable the strategy only after the executor upgrade)",
                row.get("signal_id"), reason, fraction, strategy, position_key,
            )
            return
        # Issue the protective exit at rung 0 (the base cap). The escalation
        # driver escalates from here if it doesn't fill within the window.
        trade.exit_reason = reason
        await self._issue_exit(trade, key, rung=0)

    async def _issue_exit(self, trade: OpenTrade, key, rung: int) -> None:
        """Price + submit an exit for the trade's REMAINING quantity at escalation
        `rung`, set the rung's fill window as the deadline, and persist the exit
        state. Shared by the initial EXIT and every escalation re-issue, so the
        pricing/id/deadline logic lives in one place.

        The trade is committed to CLOSING and persisted BEFORE we try to price, so
        the intent is durable even if pricing fails or we crash here. If the touch
        is missing or invalid (crossed/garbage book) we do NOT silently drop the
        protective exit: we leave the trade CLOSING at the SAME rung with a short
        retry deadline and exit_order_submitted=False, so the escalation driver
        retries this rung (rather than escalating past it) until the book is
        priceable. A protective exit that can't price is a retry, never a no-op."""
        symbol = trade.symbol
        asset_class = trade.asset_class

        remaining = trade.filled_qty - trade.exit_filled_qty
        if remaining <= 0:
            # Nothing left to close (position already flat). Finalize.
            await self._finalize_closed(trade, key)
            return

        # Commit the CLOSING intent + this rung FIRST, so it is durable regardless
        # of what happens during pricing/submit below.
        exit_cap = await self.hooks.get_exit_cap(asset_class)
        trade.state = "CLOSING"
        trade.exit_rung = rung
        window_s = exit_cap.window_seconds_for_rung(rung)

        touch = await self.hooks.get_touch(symbol, asset_class)
        bad = validate_touch(touch.bid, touch.ask) if touch is not None else "no touch"
        if bad is not None:
            # Can't price the exit right now. Keep the trade CLOSING at THIS rung
            # with a short retry deadline; the driver will call _issue_exit again
            # at the same rung. exit_order_submitted=False marks "no order placed",
            # so the driver retries rather than escalating past an unplaced rung.
            trade.exit_order_submitted = False
            trade.exit_deadline_ms = self.hooks.now_ms() + _EXIT_RETRY_MS
            await self.hooks.update_trade_state(trade.trade_id, "CLOSING", None)
            await self.hooks.persist_exit_state(trade.trade_id, rung, trade.exit_deadline_ms)
            log.warning("cannot price EXIT %s rung=%d (%s) — will retry",
                        trade.trade_id, rung, bad)
            self._mincr("exits_retry_bad_touch")
            return

        cap = exit_cap.cap_for_rung(rung)
        limit, reference, cap_applied = price_marketable_limit(
            trade.side, OrderIntent.EXIT, touch.bid, touch.ask, cap,
        )

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

        # Deadline = now + this rung's fill window. If the window is None we are
        # at/over the top of the ladder — no deadline, the driver will HALT.
        trade.exit_deadline_ms = (
            self.hooks.now_ms() + window_s * 1000 if window_s is not None else None
        )
        trade.exit_order_submitted = True
        await self.hooks.update_trade_state(trade.trade_id, "CLOSING", None)
        await self.hooks.persist_exit_state(trade.trade_id, rung, trade.exit_deadline_ms)

        # Register attribution BEFORE submit (see _on_enter): a fill on this exit
        # order can arrive before submit_order returns its ack.
        self._register_order(exit_coid, key)
        result = await self.adapter.submit_order(order)
        await self._apply_exit_result(trade, key, result)

    async def _apply_exit_result(self, trade: OpenTrade, key, result: OrderResult) -> None:
        self._learn_venue_order_id(result)
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
        # Refresh the live-count gauges once per pass — cheap, and the pass runs
        # on the loop cadence, so the operator's view stays current without
        # threading a gauge update through every state mutation.
        self._mgauge("open_trades", len(self._open))
        self._mgauge("closing_trades",
                     sum(1 for t in self._open.values() if t.state == "CLOSING"))
        self._mgauge("halted_trades",
                     sum(1 for t in self._open.values() if t.state == "HALTED"))
        # Snapshot to avoid mutating the dict mid-iteration (escalation may pop).
        # The unlocked reads below are only a cheap pre-filter; every action
        # re-validates the live trade UNDER THE SYMBOL LOCK, because a streamed
        # fill (or an in-flight _issue_exit) running between this snapshot and the
        # lock can close the position or move its deadline. Acting on the stale
        # snapshot would HALT or escalate a trade that no longer needs it.
        for key, trade in list(self._open.items()):
            if trade.state != "CLOSING":
                continue
            if trade.exit_deadline_ms is not None and now < trade.exit_deadline_ms:
                continue   # window not yet expired (re-checked under lock below)
            async with self._lock(trade.symbol):
                # Re-read the authoritative in-memory trade under the lock. It may
                # have advanced (filled -> CLOSED and popped, or a fresh rung with
                # a new deadline) since the unlocked pre-filter above.
                fresh = self._open.get(key)
                if fresh is None or fresh.state != "CLOSING":
                    continue
                if fresh.exit_deadline_ms is None:
                    # No deadline while CLOSING means we're at/over the ladder top
                    # with an unfilled exit — HALT (the ladder's terminal action).
                    await self._halt(fresh, "escalation: exit unfilled at top of ladder")
                    self._open.pop(key, None)
                    continue
                if self.hooks.now_ms() < fresh.exit_deadline_ms:
                    continue   # deadline moved out from under us (rung re-issued)
                await self._escalate(fresh, key)

    async def run_escalation_loop(self, tick_seconds: float = 1.0) -> None:
        """Periodic host driver for the escalation ladder: call
        run_escalation_once() every `tick_seconds` until stop() is set. This is
        the TIMER half of the ladder — fills advance trades on the adapter's
        event-driven stream, while elapsed exit windows are advanced here.

        Launch it from the executor host alongside the SignalListener, e.g.
        `asyncio.create_task(executor.run_escalation_loop())`, and call stop()
        (or cancel the task) to shut it down. A single pass never raises to here
        (each trade's escalation is guarded), but we still wrap the pass so a
        transient error can't kill the loop and freeze every other CLOSING
        trade's ladder — mirroring the exit-manager tick's discipline.

        Cadence uses wait_for on the stop event rather than a bare sleep so a
        stop is observed immediately instead of after the current tick elapses.
        """
        log.info("escalation loop started (%.2fs tick)", tick_seconds)
        while not self._stop.is_set():
            try:
                await self.run_escalation_once()
            except Exception:
                log.exception("escalation pass failed; continuing")
            # Record wall-clock AFTER the pass returns: a pass that wedges never
            # updates this, so health sees a stale tick — which is the point. Use
            # time.time() (not the injectable ms clock) because health compares
            # this against time.time(); keeping one clock avoids a false 'fresh'.
            self._last_tick_at = time.time()
            self._mincr("escalation_passes")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=tick_seconds)
            except asyncio.TimeoutError:
                pass   # normal: no stop within the tick, run the next pass
        log.info("escalation loop stopped")

    def stop(self) -> None:
        """Signal the escalation loop to exit at its next wake. Idempotent."""
        self._stop.set()

    async def _escalate(self, trade: OpenTrade, key) -> None:
        """Advance one rung, OR retry the current rung if its order never got
        placed (caller holds the symbol lock). If there's a rung beyond the
        current one, re-issue there; else HALT."""
        remaining = trade.filled_qty - trade.exit_filled_qty
        if remaining <= 0:
            await self._finalize_closed(trade, key)
            return
        if not trade.exit_order_submitted:
            # The current rung was committed but never actually placed (couldn't
            # price the book). Retry the SAME rung — do NOT burn a ladder step for
            # a book we never got an order onto.
            self._mincr("exits_retry_same_rung")
            log.info("retrying exit for %s at same rung %d (order not yet placed)",
                     trade.trade_id, trade.exit_rung)
            await self._issue_exit(trade, key, rung=trade.exit_rung)
            return
        exit_cap = await self.hooks.get_exit_cap(trade.asset_class)
        if not exit_cap.has_rung_beyond(trade.exit_rung):
            # Top of the ladder and still not filled -> HALT + alert. Better a
            # human looks than an unbounded market order in a fast market.
            await self._halt(trade, "escalation: exit unfilled through full ladder")
            self._open.pop(key, None)
            return
        next_rung = trade.exit_rung + 1
        self._mincr("escalations_issued")
        log.info("escalating exit for %s: rung %d -> %d (remaining=%s)",
                 trade.trade_id, trade.exit_rung, next_rung, remaining)
        await self._issue_exit(trade, key, rung=next_rung)

    # ----------------------------------------------------------------------
    # Fills (async, from the adapter's fill stream)
    # ----------------------------------------------------------------------
    def _register_order(self, client_order_id: str, key: tuple[str, str]) -> None:
        """Register the client_order_id -> trade mapping BEFORE submitting, so a
        fill that streams in during the submit->ack window is attributable the
        instant it arrives. The client id is deterministic and known up front;
        this is the map that actually closes the async-fill race."""
        self._coid_to_key[client_order_id] = key

    def _learn_venue_order_id(self, result: OrderResult) -> None:
        """At ack, bridge venue_order_id -> client_order_id so a fill that carries
        only the venue's order id (an adapter that can't stamp our client id on
        the stream) still resolves through to the key. No-op until the venue
        returns an id (reconcile resolves that residual window by client id)."""
        if result.venue_order_id and result.client_order_id:
            self._venue_to_coid[result.venue_order_id] = result.client_order_id

    def _resolve_fill(
        self, fill: Fill,
    ) -> tuple[Optional[str], Optional[tuple[str, str]]]:
        """Resolve a streamed fill to (client_order_id, trade_key).

        Client id first (deterministic, known before submit); fall back to the
        venue_order_id bridge for adapters that stamp only the venue id. Returns
        the resolved client_order_id even when the trade is no longer in memory,
        so a late fill on a finalized trade can still be persisted against its
        real trade_id (parsed from the id) rather than dropped. (None, None) =>
        genuinely unattributable (foreign/manual, or arrived before the bridge)."""
        cid = fill.client_order_id
        if cid is None and fill.venue_order_id is not None:
            cid = self._venue_to_coid.get(fill.venue_order_id)
        if cid is None:
            return None, None
        return cid, self._coid_to_key.get(cid)

    def _trade_id_for_fill(self, fill: Fill) -> Optional[str]:
        """The real trade_id a fill should be persisted against, or None if it
        can't be attributed to one of our trades. Recovers the cv1 client id (from
        the fill or the venue bridge) and parses out the trade_id. Used wherever a
        fill must satisfy the fills NOT NULL trade_id FK without a live in-memory
        trade — the reconcile sweep and late fills on finalized trades."""
        cid, _ = self._resolve_fill(fill)
        if cid is None or not coid.is_ours(cid):
            return None
        return coid.parse(cid).trade_id

    async def _on_fill(self, fill: Fill) -> None:
        """Adapter fill-stream callback for ASYNC fills. Attributes the fill to
        its trade (client_order_id first, venue_order_id bridge second) via the
        submit-time maps, then advances that trade exactly as a sync fill would —
        same _record_fill path, so partials, dedup, and completion behave
        identically whether a fill arrived synchronously or on the stream.

        A fill we can attribute but whose trade is no longer live in memory (e.g.
        a straggler exit fill after the trade finalized to CLOSED) is still
        persisted against its REAL trade_id — recovered by parsing the resolved
        cv1 client id — so late fills are recorded for honest P&L, not dropped.

        A genuinely unattributable fill (no matching id — a foreign/manual trade,
        or one that arrived before we learned its order's bridge) cannot satisfy
        the fills NOT NULL trade_id FK and so cannot be stored here. We do NOT
        pretend to persist it: we log loudly and count it, so a persistent stream
        of them is visible. The boot reconcile sweep — which re-reads recent venue
        fills fresh — is the recovery path for any that later become attributable.
        We never guess which trade an unlabelled fill belongs to."""
        cid, key = self._resolve_fill(fill)
        trade_id = self._trade_id_for_fill(fill)

        if trade_id is None:
            # Genuinely unattributable to one of our trades (foreign/manual id, or
            # a fill that arrived before we learned its order's bridge). It cannot
            # satisfy the fills NOT NULL trade_id FK, so it cannot be stored here.
            # We do NOT pretend to persist it — we log loudly and count it so a
            # persistent stream is visible; the boot reconcile sweep re-reads
            # recent venue fills fresh and is the recovery path for any that later
            # become attributable. We never guess which trade a fill belongs to.
            self._mincr("fills_unattributable")
            log.warning("async fill %s unattributable (order=%s, id=%s) — cannot "
                        "persist; left for reconcile",
                        fill.venue_exec_id, fill.venue_order_id, cid)
            return

        trade = self._open.get(key) if key is not None else None
        if trade is None:
            # Attributable, but the trade isn't live in memory (already
            # finalized/popped). Persist against the real trade_id so a straggler
            # fill is still recorded for honest P&L; venue_exec_id dedupe protects
            # totals against re-delivery. Nothing to advance in memory.
            await self.hooks.persist_fill(_fill_row(fill, trade_id=trade_id))
            self._mincr("fills_late_finalized")
            log.info("async fill %s on non-live trade %s — persisted",
                     fill.venue_exec_id, trade_id)
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
        remaining unclosed quantity (filled_qty - exit_filled_qty).

        After advancing, the new quantities are persisted to the trades row so a
        restart re-adopts the REAL filled position. Without this, only the initial
        (zero) quantities are durable and load_open_trades reads back open_qty=0 —
        which false-triggers the 'DB open but venue flat' HALT on reconcile and
        mis-sizes the escalation remainder. persist_trade's UPSERT updates exactly
        state + entry/exit filled qty, so this is a targeted write despite reusing
        the full-row helper. Persist is gated by the dedup below, so a re-delivered
        fill neither double-counts nor rewrites."""
        newly = await self.hooks.persist_fill(_fill_row(fill, trade_id=trade.trade_id))
        if not newly:
            self._mincr("fills_duplicate")
            return
        if fill.intent == OrderIntent.ENTRY:
            trade.filled_qty += fill.qty
            self._mincr("fills_entry")
        else:
            trade.exit_filled_qty += fill.qty
            self._mincr("fills_exit")
        # Durably record the advanced quantities so restart recovery is faithful.
        await self.hooks.persist_trade(trade)


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