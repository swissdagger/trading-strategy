"""
execution/signal_executor.py — the signal-consuming executor.

THE KEYSTONE the whole execution stack was scaffolding for. It sits between the
SignalListener (which pushes trade_signals rows) and an ExecutionAdapter (which
places orders at a venue), and it owns the dangerous middle: turning a strategy
DECISION into a managed, reconciled, real-money POSITION.

Replaces the legacy Kafka executor (deleted in ADR-001 Phase 1), which
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
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Awaitable, Callable, Dict, FrozenSet, Optional

from execution.adapter import ExecutionAdapter
from execution.observability import Metrics, build_health_document
from execution.pricing import (
    CapConfig, price_marketable_limit, quantize_limit_price, validate_touch,
)
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

if TYPE_CHECKING:
    from execution.shadow_book import ShadowBook

log = logging.getLogger("executor.signal")

# Non-terminal trade states. HALTED is non-terminal on purpose: a halted trade
# may still hold capital at the venue, so it counts toward the open-position
# cap and its quantity toward the symbol's netted reconcile.
_LIVE_STATES = ("PENDING", "SUBMITTED", "OPEN", "CLOSING")
_NON_TERMINAL_STATES = _LIVE_STATES + ("HALTED",)


class OrphanPositionError(RuntimeError):
    """Boot refused: the venue holds a non-dust balance the executor has no
    trade for (and it isn't allowlisted). We do not model the account's state,
    so we do not trade on it. A human reconciles, then restarts."""

# How long to wait before retrying an exit whose book couldn't be priced (no or
# crossed/invalid touch). Short, because an unpriceable protective exit is urgent:
# we want the driver to come back and try again within a couple of ticks, not sit
# on a long window. Kept well under a typical rung fill window.
_EXIT_RETRY_MS = 2_000

# --- Client-order-id rung stride for partial exits (migration 017) ---------
# One trade can now run SEVERAL exit lifecycles (one per scale-out tranche
# batch, plus a possible final close), each with its own escalation ladder.
# The cv1 rung encodes both:  rung = tranche_block * 100 + escalation, where
# tranche_block is the batch's trigger_level for SCALE orders and 0 for
# whole-position closes (TERMINAL and every legacy reason). Block-0 rungs are
# 0, 1, 2... — byte-identical to every pre-017 exit id, so history parses,
# dedupes, and attributes unchanged. _esc() recovers the escalation component,
# which is what the ExitCap ladder indexes on.
_TRANCHE_STRIDE = 100


def _esc(rung: int) -> int:
    """The escalation component of a (possibly strided) exit rung."""
    return rung % _TRANCHE_STRIDE


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
    # --- Scale-out state (migration 017; prior_failure_scaleout) ---
    # Tranches ACCEPTED for this trade: {trigger_level -> fraction of the
    # ORIGINAL position}. Presence of a level IS the replay dedupe. The map
    # drives the DEFICIT model: desired exit = sum(fractions) * filled_qty;
    # the executor issues orders for (desired - exit_filled_qty), which makes
    # lot-size accumulation automatic (an unplaceable sliver stays as deficit
    # and rides into the next tranche's order). Empty for whole-exit
    # strategies. Persisted on every persist_trade.
    scale_banked: Dict[int, Decimal] = None  # set in __post_init__
    # --- Working-exit-cycle tracking (in-memory) ---
    # The CURRENT exit order's quantity, and exit_filled_qty as of its issue.
    # Together they answer "has the working order itself completed?" — needed
    # because a SCALE tranche's deficit can GROW while its order works (new
    # tranches banking), so 'deficit <= 0' alone would never end the cycle and
    # the coalesced follow-up would wait on the escalation timer instead of
    # issuing immediately. In-memory only: after a restart both default to 0,
    # and the escalation driver's cancel-and-reissue path (which recomputes the
    # deficit from durable state) is the recovery route for a mid-cycle crash.
    exit_order_qty: Decimal = Decimal(0)
    exit_cycle_base: Decimal = Decimal(0)

    def __post_init__(self):
        if self.scale_banked is None:
            self.scale_banked = {}

    @property
    def exit_cycle_done(self) -> bool:
        """True when the current working exit order has been filled in full
        (fills since its issue cover its quantity)."""
        return (self.exit_order_qty > 0
                and self.exit_filled_qty - self.exit_cycle_base >= self.exit_order_qty)

    @property
    def closing_final(self) -> bool:
        """While CLOSING: is this a FINAL close (finalizes CLOSED when filled)
        rather than a scale tranche (returns to OPEN when filled)? Any reason
        other than SCALE — TERMINAL and every legacy reason — closes for good."""
        return self.exit_reason != ExitReason.SCALE.value

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

    # --- Venue quantity constraints (optional) ---
    # (symbol, asset_class, qty) -> largest venue-valid qty <= input (respecting
    # lot step / min qty / min notional), or 0 if the input is unplaceable.
    # None => identity (no constraint knowledge; the adapter's own rejection is
    # the backstop). Used by the scale-out path so a tranche below the venue
    # minimum ACCUMULATES into the next tranche instead of being rejected —
    # wire it from the venue's exchange-info filters when available.
    quantize_exit_qty: Optional[
        Callable[[str, str, Decimal], Awaitable[Decimal]]] = None

    # --- Alerting (optional) ---
    # (kind, detail) -> raise a human-visible alert. Called on every HALT and on
    # a refused boot. Best-effort: a failing alert is logged, never allowed to
    # block the HALT itself. Wired to executor_alerts (migration 021).
    alert: Optional[Callable[[str, dict], Awaitable[None]]] = None

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
        shadow_book: Optional["ShadowBook"] = None,
        max_open_positions: Optional[int] = None,
        enter_max_age_ms: Optional[int] = None,
        orphan_allowlist: FrozenSet[str] = frozenset(),
    ):
        """
        shadow_book: where SHORT signals go when the adapter cannot short
            (adapter.supports_short is False). None => such signals are skipped.
        max_open_positions: cap on non-terminal trades across all symbols,
            checked before every ENTER. None => no cap.
        enter_max_age_ms: an ENTER whose row was written (created_at) longer
            ago than this is skipped (replayed after downtime). EXITs are never
            age-gated. None => no age gate.
        orphan_allowlist: base assets or symbols whose venue balances are
            ignored by the boot orphan check.
        """
        self.adapter = adapter
        self.hooks = hooks
        self.shadow_book = shadow_book
        self._max_open_positions = max_open_positions
        self._enter_max_age_ms = enter_max_age_ms
        self._orphan_allowlist = frozenset(orphan_allowlist)
        # Serializes the open-position cap check with the trade insert that
        # consumes the slot, across symbols (the per-symbol lock alone would let
        # two symbols' ENTERs both pass the check). Always taken INSIDE a symbol
        # lock, never the other way round.
        self._entry_lock = asyncio.Lock()
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
        adapter — decides. Un-resolvable discrepancies HALT the affected trades
        (a human looks) rather than guessing with real money.

        NETTED PER SYMBOL. A venue reports ONE position per symbol (on spot it is
        simply the base-asset balance), so the comparison is per symbol: the sum
        of our non-terminal trades' signed open_qty against the venue's signed
        qty, once. Comparing the venue's per-symbol qty against each trade
        individually made two open trades on one symbol HALT each other.

        Resolutions:
          * Netted qty matches (within one venue quantity increment) -> keep
            managing every trade on the symbol.
          * Mismatch (including "DB open but venue flat") -> HALT every live
            trade on the symbol. Netting cannot say WHICH trade is wrong, so all
            of them stop — consistent with never guessing with real money.
          * Venue balance with no DB trade -> orphan: refuse boot unless it is
            dust or allowlisted (OrphanPositionError, after alerting).
          * Recent fills attributed by parsing the cv1 client_order_id.
        """
        venue_pos = {p.symbol: p for p in snapshot.positions}

        by_symbol: Dict[str, list[OpenTrade]] = defaultdict(list)
        for t in db_trades:
            self._open[(t.strategy, t.position_key)] = t
            by_symbol[t.symbol].append(t)

        for sym, trades in by_symbol.items():
            live = [t for t in trades if t.state in _LIVE_STATES]
            if not live:
                continue   # only HALTED trades: already parked for a human
            # HALTED trades' quantity is still at the venue, so it is part of the
            # expected net even though those trades are not re-checked.
            expected = sum((t.open_qty for t in trades), Decimal(0))
            pos = venue_pos.get(sym)
            venue_qty = pos.qty if pos is not None else Decimal(0)
            eps = await self._qty_epsilon(sym, trades[0].asset_class)
            if abs(venue_qty - expected) <= eps:
                continue   # venue confirms the symbol's net size AND sign
            if abs(venue_qty) <= eps:
                reason = f"reconcile: DB open but venue flat (symbol {sym} db_net={expected})"
            else:
                reason = (f"reconcile: symbol {sym} position mismatch "
                          f"db_net={expected} venue={venue_qty}")
            for t in live:
                await self._halt(t, reason)

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
            cid, _ = self._resolve_fill(f)
            await self.hooks.persist_fill(_fill_row(f, trade_id=trade_id, client_order_id=cid))

        # Venue balances with no corresponding DB trade. Checked LAST so the fill
        # sweep above still records what it can before a refused boot.
        orphans = []
        for sym, p in venue_pos.items():
            if sym in by_symbol:
                continue
            if self._orphan_allowed(sym):
                log.info("reconcile: venue holds allowlisted %s qty=%s — ignored", sym, p.qty)
                continue
            if await self._is_dust(sym, p.qty):
                log.info("reconcile: venue holds dust %s qty=%s — ignored", sym, p.qty)
                continue
            orphans.append({"symbol": sym, "qty": str(p.qty)})
        if orphans:
            self._mincr("boot_refused_orphans")
            detail = {"reason": "orphan venue balances with no DB trade",
                      "orphans": orphans}
            await self._alert("boot_refused", detail)
            log.error("reconcile: ORPHAN balances %s — refusing to boot", orphans)
            raise OrphanPositionError(
                f"venue holds balances with no DB trade: {orphans}. Reconcile by "
                f"hand (or allowlist them), then restart.")

    async def _qty_epsilon(self, symbol: str, asset_class: str) -> Decimal:
        """Tolerance for "venue qty matches ours": one venue quantity increment
        (Coinbase base_increment, via get_step_size) — anything smaller is
        unplaceable rounding. Falls back to the per-class epsilon when the venue
        can't say."""
        try:
            step = await self.adapter.get_step_size(symbol, asset_class)
        except Exception:
            log.exception("get_step_size(%s) failed; using class epsilon", symbol)
            step = None
        return step if step is not None and step > 0 else qty_match_epsilon(asset_class)

    def _orphan_allowed(self, symbol: str) -> bool:
        """Allowlisted by full symbol, or by base asset (the part before '-' in
        the Coinbase product id form, e.g. 'ETH' for 'ETH-USD')."""
        return (symbol in self._orphan_allowlist
                or symbol.split("-", 1)[0] in self._orphan_allowlist)

    def _default_asset_class(self) -> str:
        classes = getattr(self.adapter, "supported_classes", set()) or set()
        return next(iter(classes)) if len(classes) == 1 else ""

    async def _is_dust(self, symbol: str, qty: Decimal) -> bool:
        """A balance too small to trade: below the venue's minimum base order
        size when known (it could not be sold anyway), else within one quantity
        increment."""
        asset_class = self._default_asset_class()
        try:
            mins = await self.adapter.get_min_order_size(symbol, asset_class)
        except Exception:
            log.exception("get_min_order_size(%s) failed", symbol)
            mins = None
        if mins is not None and mins.base is not None and mins.base > 0:
            return abs(qty) < mins.base
        return abs(qty) <= await self._qty_epsilon(symbol, asset_class)

    async def _alert(self, kind: str, detail: dict) -> None:
        """Best-effort human alert. Never raises: an alert sink that is down
        must not stop a HALT or mask the original error."""
        if self.hooks.alert is None:
            return
        try:
            await self.hooks.alert(kind, detail)
        except Exception:
            self._mincr("alerts_failed")
            log.exception("alert %s failed to send: %s", kind, detail)

    async def _halt(self, t: OpenTrade, reason: str) -> None:
        t.state = "HALTED"
        self._mincr("trades_halted")
        await self.hooks.update_trade_state(t.trade_id, "HALTED", reason)
        log.error("trade %s HALTED: %s", t.trade_id, reason)
        await self._alert("trade_halted", {
            "trade_id": t.trade_id, "strategy": t.strategy,
            "position_key": t.position_key, "symbol": t.symbol,
            "side": t.side.value, "reason": reason,
        })

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

        # Idempotency: already managing this position (real or shadow)? A
        # replayed ENTER is a no-op (the deterministic ids would dedupe at the
        # DB/venue anyway).
        if key in self._open or (
                self.shadow_book is not None and self.shadow_book.holds(*key)):
            log.debug("ENTER for already-open position %s/%s — skip", strategy, position_key)
            return

        # Gate: stale ENTER (catch-up replay after downtime). Before the side
        # gate, so the shadow book records only entries we would really take.
        if self._enter_is_stale(row):
            self._mincr("entries_skipped_stale")
            log.warning("stale ENTER %s/%s (written %s) — skipped",
                        strategy, position_key, row.get("created_at"))
            return

        side = TradeSide.LONG if row["direction"] == "LONG" else TradeSide.SHORT

        # Gate: side. A venue that cannot short never sizes or sees a SHORT; the
        # signal goes to the shadow book (virtual fills) instead. Before the
        # kill switch and the position cap: the shadow book takes no risk.
        if side == TradeSide.SHORT and not getattr(self.adapter, "supports_short", False):
            if self.shadow_book is not None:
                self._mincr("entries_shadowed")
                await self.shadow_book.on_enter(row)
            else:
                self._mincr("entries_short_unsupported")
                log.warning("SHORT ENTER %s/%s: venue cannot short and no shadow "
                            "book — skipped", strategy, position_key)
            return

        # Gate: kill_switch blocks NEW positions.
        if await self.hooks.kill_switch_blocks():
            log.info("kill_switch engaged — refusing ENTER %s/%s", strategy, position_key)
            return

        async with self._entry_lock:
            # Gate: open-position cap. Under the entry lock, and the slot is
            # consumed (trade inserted into _open) before the lock is released.
            if self._max_open_positions is not None:
                n_open = sum(1 for t in self._open.values()
                             if t.state in _NON_TERMINAL_STATES)
                if n_open >= self._max_open_positions:
                    self._mincr("entries_skipped_max_open")
                    log.warning("max open positions (%d) reached — refusing ENTER %s/%s",
                                self._max_open_positions, strategy, position_key)
                    return
            await self._open_position(row, key, side)

    def _enter_is_stale(self, row: dict) -> bool:
        """True if the ENTER signal was WRITTEN (trade_signals.created_at) more
        than enter_max_age_ms ago — i.e. it piled up while the executor was
        down and is being replayed by the catch-up sweep.

        Deliberately not decided_at: that is MARKET time (the triggering bar's
        window start), so a signal triggered on a 15m bar is already 15+
        minutes "old" the moment it is written, and gating on it would skip
        every such entry. A row without created_at is not judged stale
        (logged): refusing on missing data would silently disable entries."""
        if self._enter_max_age_ms is None:
            return False
        written = row.get("created_at")
        if written is None:
            log.warning("ENTER %s has no created_at — age gate not applied",
                        row.get("signal_id"))
            return False
        if isinstance(written, str):
            written = datetime.fromisoformat(written.replace("Z", "+00:00"))
        if written.tzinfo is None:
            written = written.replace(tzinfo=timezone.utc)
        age_ms = self.hooks.now_ms() - int(written.timestamp() * 1000)
        return age_ms > self._enter_max_age_ms

    async def _price_increment(self, symbol: str, asset_class: str) -> Optional[Decimal]:
        try:
            return await self.adapter.get_price_increment(symbol, asset_class)
        except Exception:
            log.exception("get_price_increment(%s) failed; limit sent unquantized", symbol)
            return None

    async def _open_position(self, row: dict, key, side: TradeSide) -> None:
        strategy, position_key = key
        symbol = row["symbol"]
        asset_class = row["asset_class"]
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
        limit = quantize_limit_price(
            limit, side, OrderIntent.ENTRY, await self._price_increment(symbol, asset_class))

        # Venue minimums: refuse an undersized entry before submitting, rather
        # than spending a round trip on a certain rejection.
        try:
            mins = await self.adapter.get_min_order_size(symbol, asset_class)
        except Exception:
            log.exception("get_min_order_size(%s) failed; venue is the backstop", symbol)
            mins = None
        if mins is not None:
            if mins.base is not None and qty < mins.base:
                self._mincr("entries_skipped_below_min")
                log.warning("ENTER %s/%s qty %s below venue base minimum %s — no order",
                            strategy, position_key, qty, mins.base)
                return
            if mins.quote is not None and qty * limit < mins.quote:
                self._mincr("entries_skipped_below_min")
                log.warning("ENTER %s/%s notional %s below venue quote minimum %s — no order",
                            strategy, position_key, qty * limit, mins.quote)
                return

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
            await self._record_fill(trade, f, result.client_order_id)
        # State: OPEN if any fill, else SUBMITTED (working at venue).
        if trade.filled_qty > 0:
            trade.state = "OPEN"
            await self.hooks.update_trade_state(trade.trade_id, "OPEN", None)
            # Tranches can have been banked while the entry was in flight (a
            # catch-up replay, or a fast chain). Now there is a position for
            # the deficit to act on.
            await self._advance_scale(trade, key)
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
        if trade is None and self.shadow_book is not None and self.shadow_book.holds(*key):
            # A virtual (shadow) position: close it there. Never touches the venue.
            self._mincr("exits_shadowed")
            await self.shadow_book.on_exit(row)
            return
        if trade is None:
            # EXIT for a position we don't hold. Per migration 004's note, this
            # is a detectable reconciliation gap, not a silent drop.
            log.warning("EXIT for unknown position %s/%s — reconciliation gap", strategy, position_key)
            return
        if trade.state in ("CLOSED", "REJECTED", "HALTED"):
            log.debug("EXIT for %s already terminal (%s) — skip", trade.trade_id, trade.state)
            return
        # NOTE: CLOSING is deliberately NOT a blanket skip any more. Whether a
        # working exit absorbs, defers, or is superseded by this signal depends
        # on the REASONS on both sides — routed below. (A SCALE tranche while a
        # tranche works just banks and coalesces; a final close while a tranche
        # works cancels and supersedes; a final close while a final close works
        # keeps the original blanket-skip behaviour.)

        reason = row.get("exit_reason") or "MANUAL"

        # --- SCALE: a partial-exit tranche (migration 016/017). Its own path:
        # bank the tranche, then work the cumulative deficit as at most ONE
        # exit order at a time. Never flattens the position.
        if reason == ExitReason.SCALE.value:
            await self._on_scale_exit(trade, key, row)
            return

        # --- Non-SCALE (TERMINAL + every legacy reason): close the WHOLE
        # remaining position.
        if trade.state == "CLOSING":
            if trade.closing_final:
                # Already closing for good (a duplicate EXIT, or one arriving
                # while the escalation driver works it). The working exit +
                # ladder handle it; a second EXIT must not stack another order.
                log.debug("EXIT for %s already CLOSING — skip (driver owns it)", trade.trade_id)
                return
            # A SCALE tranche order is working, but a FINAL close supersedes
            # it: the position must go flat, not shed 5%. Cancel the working
            # tranche order (absorbing any fills it got before the cancel),
            # then fall through to issue the full-remaining close in a fresh
            # block-0 lifecycle. Cancel is idempotent on a terminal order.
            log.info("final EXIT (%s) supersedes working SCALE tranche on %s — "
                     "cancelling tranche order", reason, trade.trade_id)
            await self._cancel_working_exit(trade)
        reason_final = reason
        trade.exit_reason = reason_final
        await self.hooks.persist_trade(trade)   # durably record the driving reason
        # Issue the protective exit at block-0 rung 0 (the base cap). The
        # escalation driver escalates from here if it doesn't fill in window.
        await self._issue_exit(trade, key, rung=0)

    async def _on_scale_exit(self, trade: OpenTrade, key, row: dict) -> None:
        """Accept one SCALE tranche and advance the deficit machine.

        The tranche is identified by its trigger_level (= entry level + offset
        — the offset itself is folded into the opaque signal_id, so the level
        is the replay-dedupe key the row actually carries). Banking is
        persisted BEFORE any order work so a crash between accept and issue
        replays safely: the level is already in the map, the deficit is
        recomputed from durable state, and the catch-up redelivery no-ops."""
        lvl = row.get("trigger_level")
        frac = row.get("fraction")
        if lvl is None or frac is None:
            # Can't dedupe or size a tranche without both. Refuse loudly; the
            # row stays durable for a fixed producer to be re-read later.
            self._mincr("exits_scale_malformed")
            log.error("SCALE exit %s missing trigger_level/fraction (%r/%r) — refusing",
                      row.get("signal_id"), lvl, frac)
            return
        lvl = int(lvl)
        if lvl in trade.scale_banked:
            log.debug("SCALE tranche @L%d already banked for %s — replay skip",
                      lvl, trade.trade_id)
            return
        trade.scale_banked[lvl] = Decimal(str(frac))
        total = sum(trade.scale_banked.values())
        if total > 1:
            # Producer-side invariant broken (fractions sum past the whole
            # position). Clamp the DEFICIT model at 1.0 (we can never exit more
            # than we hold) and scream — this is a bug upstream, not here.
            self._mincr("exits_scale_overbanked")
            log.error("scale_banked for %s sums to %s > 1 — clamping to whole "
                      "position; producer bug upstream", trade.trade_id, total)
        await self.hooks.persist_trade(trade)
        self._mincr("exits_scale_banked")
        log.info("SCALE tranche banked %s @L%d fraction=%s (cum=%s)",
                 trade.trade_id, lvl, frac, min(total, 1))
        await self._advance_scale(trade, key)

    def _scale_deficit(self, trade: OpenTrade) -> Decimal:
        """How much MORE of the position should be exited right now, per the
        banked tranches: desired = min(sum(fractions), 1) * filled_qty, minus
        what exit fills have already closed. Derived purely from durable state,
        so it is restart- and replay-stable; can be <= 0 (nothing owed)."""
        banked = min(sum(trade.scale_banked.values(), Decimal(0)), Decimal(1))
        desired = banked * trade.filled_qty
        return desired - trade.exit_filled_qty

    async def _advance_scale(self, trade: OpenTrade, key) -> None:
        """Issue the next tranche-batch exit order if one is owed and nothing
        is working. THE single continuation point of the scale machine — called
        on tranche accept, on tranche-order completion, and when an entry with
        pre-banked tranches becomes OPEN. At most one exit order works per
        trade at any time; tranches arriving while one works simply grow the
        deficit and coalesce into the NEXT order (the same-tick multi-level
        propagation case)."""
        if trade.state != "OPEN":
            # PENDING/SUBMITTED: no position yet (deficit is against filled 0);
            # CLOSING: an order is already working — completion re-calls us.
            return
        deficit = self._scale_deficit(trade)
        if deficit <= 0:
            return
        qty = deficit
        if self.hooks.quantize_exit_qty is not None:
            qty = await self.hooks.quantize_exit_qty(trade.symbol, trade.asset_class, deficit)
        if qty <= 0:
            # Below the venue minimum: accumulate. The deficit persists and
            # rides into the next tranche (or the TERMINAL close sweeps it).
            self._mincr("exits_scale_below_lot")
            log.info("scale deficit %s for %s below venue minimum — accumulating",
                     deficit, trade.trade_id)
            return
        trade.exit_reason = ExitReason.SCALE.value
        await self.hooks.persist_trade(trade)
        # Deterministic block for this batch: the deepest banked level. A new
        # tranche arriving mid-escalation changes the deficit, not the block —
        # ids need distinctness and parseability, and the orders row records
        # the true quantity.
        rung = max(trade.scale_banked) * _TRANCHE_STRIDE
        await self._issue_exit(trade, key, rung=rung)

    async def _cancel_working_exit(self, trade: OpenTrade) -> None:
        """Cancel the trade's current working exit order (by its deterministic
        id) and absorb any fills the cancel reports (a partial fill racing the
        cancel). Used when escalating past a rung and when a final close
        supersedes a working tranche — the adapter contract (adapter.py) names
        cancel-then-reissue as the escalation mechanism precisely so two live
        exit orders can never work the same position at once."""
        if not trade.exit_order_submitted:
            return   # the rung was committed but no order was ever placed
        cid = coid.build(trade.trade_id, OrderIntent.EXIT, trade.exit_rung)
        try:
            result = await self.adapter.cancel_order(cid)
        except Exception:
            # A cancel that errors leaves an unknown order state — the fill
            # stream and reconcile remain the safety net; log and continue
            # (the replacement order's own qty is computed net of fills).
            log.exception("cancel_order(%s) raised for %s — continuing", cid, trade.trade_id)
            return
        self._learn_venue_order_id(result)
        await self.hooks.advance_order_fill(cid, result.filled_qty, result.status.value)
        for f in result.fills:
            await self._record_fill(trade, f, cid)

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

        # Quantity: a FINAL close (block 0) always works the full remainder; a
        # SCALE tranche works the banked DEFICIT, clamped to the position. If
        # fills that raced us already satisfied the deficit (or the deficit is
        # below the venue minimum), the tranche lifecycle is complete-for-now:
        # back to OPEN, no order.
        if trade.exit_reason == ExitReason.SCALE.value:
            deficit = self._scale_deficit(trade)
            if deficit > 0 and self.hooks.quantize_exit_qty is not None:
                deficit = await self.hooks.quantize_exit_qty(
                    trade.symbol, trade.asset_class, deficit)
            if deficit <= 0:
                await self._return_to_open(trade, key)
                return
            order_qty = min(deficit, remaining)
        else:
            order_qty = remaining

        # Commit the CLOSING intent + this rung FIRST, so it is durable regardless
        # of what happens during pricing/submit below.
        exit_cap = await self.hooks.get_exit_cap(asset_class)
        trade.state = "CLOSING"
        trade.exit_rung = rung
        # The ExitCap ladder indexes on the ESCALATION component of the strided
        # rung (block * 100 + escalation) — block 0 reproduces pre-017 behaviour
        # exactly.
        window_s = exit_cap.window_seconds_for_rung(_esc(rung))

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

        cap = exit_cap.cap_for_rung(_esc(rung))
        limit, reference, cap_applied = price_marketable_limit(
            trade.side, OrderIntent.EXIT, touch.bid, touch.ask, cap,
        )
        limit = quantize_limit_price(
            limit, trade.side, OrderIntent.EXIT,
            await self._price_increment(symbol, asset_class))

        # Exit order gets its own deterministic id at THIS (strided) rung, so a
        # re-issue at a higher escalation — or a different tranche block — is a
        # distinct, idempotent order.
        exit_coid = coid.build(trade.trade_id, OrderIntent.EXIT, rung)
        order = OrderRequest(
            client_order_id=exit_coid, trade_id=trade.trade_id, symbol=symbol,
            asset_class=asset_class, intent=OrderIntent.EXIT, side=trade.side,
            qty=order_qty,
            limit_price=limit, venue_reference=reference, cap_applied=cap_applied,
            exit_reason=_exit_reason(trade.exit_reason or "MANUAL"), escalation_rung=rung,
        )
        await self.hooks.persist_order(_order_row(order, OrderStatus.WORKING))

        # Working-cycle tracking: what THIS order is for, and where exit fills
        # stood when it went out — so completion of the ORDER (not just of the
        # whole deficit) is detectable while new tranches keep banking.
        trade.exit_order_qty = order_qty
        trade.exit_cycle_base = trade.exit_filled_qty
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
            await self._record_fill(trade, f, result.client_order_id)
        await self._after_exit_fills(trade, key)

    async def _after_exit_fills(self, trade: OpenTrade, key) -> None:
        """Shared completion check after exit fills advance a trade (sync result
        and async stream both land here). Order matters: position flat always
        finalizes CLOSED — even for a tranche, a flat position is a done
        position. Otherwise a satisfied SCALE tranche returns the trade to OPEN
        and immediately re-advances the scale machine (tranches that arrived
        while this one worked coalesce into the next order)."""
        if trade.exit_filled_qty >= trade.filled_qty and trade.filled_qty > 0:
            await self._finalize_closed(trade, key)
            return
        if (trade.state == "CLOSING" and not trade.closing_final
                and (self._scale_deficit(trade) <= 0 or trade.exit_cycle_done)):
            await self._return_to_open(trade, key)
            await self._advance_scale(trade, key)

    async def _return_to_open(self, trade: OpenTrade, key) -> None:
        """A SCALE tranche lifecycle completed without flattening the position:
        the trade goes back to OPEN (the escalation driver stops watching it),
        the deadline clears, and the durable rows record both. The exit_rung
        keeps its last strided value — the next tranche batch overwrites it."""
        trade.state = "OPEN"
        trade.exit_deadline_ms = None
        trade.exit_order_submitted = True
        trade.exit_order_qty = Decimal(0)
        trade.exit_cycle_base = trade.exit_filled_qty
        await self.hooks.update_trade_state(trade.trade_id, "OPEN", None)
        await self.hooks.persist_exit_state(trade.trade_id, trade.exit_rung, None)
        await self.hooks.persist_trade(trade)
        self._mincr("exits_scale_tranche_complete")
        log.info("scale tranche complete for %s — back to OPEN (exited %s of %s)",
                 trade.trade_id, trade.exit_filled_qty, trade.filled_qty)

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
        if not exit_cap.has_rung_beyond(_esc(trade.exit_rung)):
            # Top of the ladder and still not filled -> HALT + alert. Better a
            # human looks than an unbounded market order in a fast market. This
            # applies to a SCALE tranche too: a 5% slice that can't fill through
            # the full ladder means the book is broken enough that the whole
            # position deserves eyes, not more automation.
            await self._halt(trade, "escalation: exit unfilled through full ladder")
            self._open.pop(key, None)
            return
        next_rung = trade.exit_rung + 1
        self._mincr("escalations_issued")
        log.info("escalating exit for %s: rung %d -> %d (remaining=%s)",
                 trade.trade_id, trade.exit_rung, next_rung, remaining)
        # Cancel the outgoing rung's order BEFORE issuing its replacement — the
        # adapter contract names cancel-then-reissue as the escalation
        # mechanism. Without it, both rungs work the book at once and can BOTH
        # fill (an over-close that flips the position). Any fills the cancel
        # reports are absorbed first, so the replacement's qty is computed net.
        await self._cancel_working_exit(trade)
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
            await self.hooks.persist_fill(_fill_row(fill, trade_id=trade_id,
                                                    client_order_id=cid))
            self._mincr("fills_late_finalized")
            log.info("async fill %s on non-live trade %s — persisted",
                     fill.venue_exec_id, trade_id)
            return

        # Attribute under the symbol lock so a stream fill can't interleave with
        # signal/escalation handling for the same symbol.
        async with self._lock(trade.symbol):
            await self._record_fill(trade, fill, cid)
            await self._maybe_complete_after_async_fill(trade, key)

    async def _maybe_complete_after_async_fill(self, trade: OpenTrade, key) -> None:
        """After an async fill advances a trade, apply the same state transitions
        the sync paths do: an entry that's now (partly) filled becomes OPEN (and
        any pre-banked scale tranches start working); exit fills route through
        the shared completion check (flat -> CLOSED; tranche satisfied -> OPEN)."""
        if trade.state == "SUBMITTED" and trade.filled_qty > 0:
            trade.state = "OPEN"
            await self.hooks.update_trade_state(trade.trade_id, "OPEN", None)
            await self._advance_scale(trade, key)
        if trade.state == "CLOSING":
            await self._after_exit_fills(trade, key)

    async def _record_fill(self, trade: OpenTrade, fill: Fill,
                           client_order_id: Optional[str] = None) -> None:
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
        newly = await self.hooks.persist_fill(
            _fill_row(fill, trade_id=trade.trade_id, client_order_id=client_order_id))
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


def _fill_row(fill: Fill, trade_id: Optional[str],
              client_order_id: Optional[str] = None) -> dict:
    """The fills row. client_order_id is how persistence finds the parent
    order (orders.client_order_id is always written at submit; the venue's
    order id is not known then), so every caller that knows it passes it."""
    return {
        "venue_exec_id": fill.venue_exec_id,
        "trade_id": trade_id,
        "client_order_id": client_order_id or fill.client_order_id,
        "venue_order_id": fill.venue_order_id,
        "qty": fill.qty,
        "price": fill.price,
        "intent": fill.intent.value,
        "filled_at_ms": fill.filled_at_ms,
        "fee": fill.fee,
        "fee_currency": fill.fee_currency,
    }