"""
execution/types.py — The concrete objects the ExecutionAdapter traffics in.

These are the execution-side analogue of models.py's OHLCPoint: small, typed
payloads that cross between the executor and the venue adapter. Every field maps
to a column in the trading schema (001_trade_lifecycle.sql) so an OrderResult or
Fill returned by an adapter can be written straight into `orders` / `fills`
without a translation layer.

Design points carried over from the schema and the order-flow decisions:
  * All orders are MARKETABLE (crossing) LIMITS with a basis cap. There is no
    "order type" field with a market option — the adapter always sends a limit;
    `limit_price` is the actual price, priced off `venue_reference` with
    `cap_applied`. Pure market orders were deliberately excluded (they make basis
    unbounded).
  * client_order_id is DETERMINISTIC, derived from the trade key, never random.
    It is the bridge across a crash — recovery queries the venue by this id.
  * Partial fills are first-class: an OrderResult reports filled_qty separately
    from the request's qty, and a single order may yield many Fills over time.
  * Money quantities are Decimal, not float. Float share/coin counts accumulate
    rounding error; the schema uses numeric(38,18) and these mirror it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import List, Optional


# --- Enums mirror the schema's enum types exactly (same string values), so an
# --- adapter never invents a category the DB can't store. ---

class OrderIntent(str, Enum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"


class TradeSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class OrderStatus(str, Enum):
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class ExitReason(str, Enum):
    STOP = "STOP"
    TARGET = "TARGET"
    TIME = "TIME"
    MANUAL = "MANUAL"
    KILL_SWITCH = "KILL_SWITCH"
    # Partial-exit reasons (migration 016, prior_failure_scaleout). SCALE is a
    # scheduled scale-out tranche — TARGET-like, no urgency. TERMINAL is "the
    # chain terminated, close the remainder" — protective, STOP-like posture.
    # Until the executor supports partial exits (rollout step 4) it must not
    # receive SCALE rows; see the interlock in signal_executor._on_exit.
    SCALE = "SCALE"
    TERMINAL = "TERMINAL"


@dataclass(frozen=True)
class OrderRequest:
    """An instruction to place ONE marketable-limit order at the venue.

    Built by the executor (for ENTRY) or the exit component (for EXIT) and
    handed to ExecutionAdapter.submit_order. Carries everything the adapter
    needs to place AND everything needed to write the `orders` row.

    The adapter does NOT decide the limit price or the cap — those are computed
    upstream (off a venue reference + the per-class cap config) and passed in,
    so the pricing policy lives in one place (the executor/exit component) and
    the adapter stays a thin venue translator. The adapter's only pricing job
    is to express `limit_price` in the venue's native tick/precision.
    """
    # --- Identity / provenance ---
    client_order_id: str          # DETERMINISTIC, derived from trade key. Crash-recovery key.
    trade_id: str                 # the trades.trade_id this order belongs to
    symbol: str                   # canonical symbol; adapter maps to native
    asset_class: str              # 'crypto' | 'equity' | 'fx' — selects the adapter & cap config
    intent: OrderIntent
    side: TradeSide               # direction of the POSITION this order serves

    # --- Marketable-limit pricing (the order-flow contract) ---
    qty: Decimal                  # quantity to transact
    limit_price: Decimal          # the actual crossing limit to send
    venue_reference: Optional[Decimal] = None   # touch priced off of (audit)
    cap_applied: Optional[Decimal] = None       # basis cap distance used (audit)

    # --- EXIT-only: why, and where on the escalation ladder ---
    # exit_reason selects cap + window upstream; it is recorded on the order so
    # reconcile/forensics can see why an exit order existed. escalation_rung
    # counts re-issues (0 = first attempt). Both None/0 for ENTRY.
    exit_reason: Optional[ExitReason] = None
    escalation_rung: int = 0


@dataclass(frozen=True)
class Fill:
    """One execution reported by the venue. Maps 1:1 to a `fills` row.

    venue_exec_id is the venue's own execution id and is the idempotency key:
    the same execution reported twice (venue resend, or re-read during reconcile)
    must dedupe to one row, never double-count into filled_qty. The schema's
    UNIQUE(venue_exec_id) enforces this; the adapter just reports faithfully.
    """
    venue_exec_id: str
    qty: Decimal
    price: Decimal
    intent: OrderIntent
    filled_at_ms: int             # venue execution time, epoch ms
    fee: Decimal = Decimal(0)
    fee_currency: Optional[str] = None
    # The venue's order id this fill belongs to. Every venue stamps a fill with
    # its parent order (Binance userTrades.orderId, IB execution.orderId), so a
    # fill arriving asynchronously on the stream can be attributed to the trade
    # whose order produced it — the executor maps venue_order_id -> trade at
    # submit time. Optional because a fill discovered during reconcile (before we
    # know our orders) may lack it; such fills are persisted and left for the
    # position reconcile to explain.
    venue_order_id: Optional[str] = None
    # Our canonical cv1 client_order_id for the order this fill belongs to, when
    # the adapter can recover it (Binance userTrades carries clientOrderId, which
    # reverses through the adapter's compression map; IB stamps orderRef on the
    # execution). This is the PRIMARY attribution key: it is DETERMINISTIC and
    # known BEFORE submit, so a fill streamed in the brief submit->ack window —
    # before we've learned this order's venue_order_id — is still attributable.
    # Optional because reconcile may surface a foreign/older fill without it; such
    # fills fall back to venue_order_id, then to the position reconcile.
    client_order_id: Optional[str] = None


@dataclass(frozen=True)
class OrderResult:
    """The outcome of submit_order / cancel_order at the venue, at the moment
    the call returns. Maps to an `orders` row's mutable fields plus any fills
    known synchronously.

    Note `status` here is the order's status AS OF THIS RETURN — a marketable
    limit often returns FILLED or PARTIALLY_FILLED synchronously, but a venue
    may also ack WORKING and report fills asynchronously later (via the adapter's
    fill stream). The executor writes what it gets and lets later fill events
    advance the row. `venue_order_id` is None only in the brief submit→ack
    window; if the call raised before ack, the executor keeps the order PENDING
    and resolves it by client_order_id on the next reconcile.
    """
    client_order_id: str
    status: OrderStatus
    venue_order_id: Optional[str] = None
    filled_qty: Decimal = Decimal(0)
    fills: List[Fill] = field(default_factory=list)
    # Populated when status is REJECTED — the venue's reason, for the trades
    # halt_reason / order forensics. Free text; not parsed.
    reject_reason: Optional[str] = None


@dataclass(frozen=True)
class VenuePosition:
    """What the venue says we actually hold for one symbol, right now. The
    unit of truth for reconcile. `qty` is signed by convention: positive = long,
    negative = short, zero = flat. `avg_price` is the venue's reported average
    entry, used only as a cross-check (our authoritative entry price is the
    qty-weighted average over our own `fills`).
    """
    symbol: str
    qty: Decimal                  # signed: + long, - short, 0 flat
    avg_price: Optional[Decimal] = None


@dataclass(frozen=True)
class VenueOrder:
    """A working order the venue currently has on our behalf. Returned by
    reconcile so the executor can diff the venue's open orders against `orders`
    rows in WORKING/PARTIALLY_FILLED state. Matched by client_order_id — the
    deterministic id is what makes the diff possible across a crash.
    """
    client_order_id: Optional[str]   # None if the venue lost/!echoed our ref
    venue_order_id: str
    symbol: str
    side: TradeSide
    qty: Decimal
    filled_qty: Decimal
    status: OrderStatus


@dataclass(frozen=True)
class ReconcileResult:
    """The full snapshot reconcile() returns at boot (and on the periodic
    watchdog): everything the venue says is true, for the executor to diff
    against the DB. The executor — never the adapter — decides how to resolve
    discrepancies (and routes the un-resolvable ones to HALTED). The adapter's
    job is only to report the venue's truth faithfully.
    """
    positions: List[VenuePosition]
    open_orders: List[VenueOrder]
    # Recent terminal fills the venue reports, so the executor can catch fills
    # that landed during a crash window and were never written locally. Bounded
    # by the adapter to a sane recent window.
    recent_fills: List[Fill] = field(default_factory=list)



@dataclass(frozen=True)
class Touch:
    """Current venue best bid/ask for a symbol, used to price a marketable limit.

    The unit of pricing truth, fetched live from the venue adapter (IB snapshot
    for equity/fx, Binance bookTicker for crypto) at order time — NOT stored in
    the DB (the 001 schema deliberately has no quotes table). bid/ask are Decimal
    so all downstream pricing arithmetic (price_marketable_limit) stays in Decimal;
    the float->Decimal boundary is crossed once, in the adapter, via str().

    Previously defined in signal_executor.py; moved here so the adapter layer can
    return it without an import cycle.
    """
    bid: Decimal
    ask: Decimal


@dataclass(frozen=True)
class AccountEquity:
    """A venue account's current equity, for position sizing.

    total is the account's total equity in `currency` (IB: NetLiquidation from
    the account summary; Binance USD-M: total wallet/margin balance in USDT).
    available is the portion free for new positions (IB: AvailableFunds; Binance:
    availableBalance) — sizing that respects buying power uses this, while a
    simple "% of equity" rule uses total. available is Optional because not every
    venue read cheaply distinguishes it; when absent, sizing falls back to total.

    Decimal throughout, str-converted at the venue boundary, same as Touch.
    """
    total: Decimal
    currency: str
    available: Optional[Decimal] = None


@dataclass(frozen=True)
class MinOrderSize:
    """A venue's minimum order size for a symbol, in base units and/or in quote
    notional (Coinbase: base_min_size / quote_min_size on the product). Either
    may be None when the venue doesn't publish it. The executor rejects an entry
    below either minimum BEFORE submitting, rather than paying a venue round
    trip for a certain rejection."""
    base: Optional[Decimal] = None
    quote: Optional[Decimal] = None


# ---------------------------------------------------------------------------
# Quantity-match tolerances. Venues report position sizes with their own
# rounding, so reconcile compares |db - venue| against an epsilon rather than
# demanding exact equality. The tolerance MUST scale with the instrument's
# precision — 1e-9 is right for 8-dp crypto but absurdly tight for whole-share
# equities, where a venue's fractional-share rounding would spuriously HALT.
# Single source of truth so boot reconcile and the live-executor reconcile agree.
# ---------------------------------------------------------------------------
_DEFAULT_QTY_EPSILON = Decimal("1e-9")
_QTY_EPSILON_BY_CLASS = {
    "crypto": Decimal("1e-8"),   # 8-dp coins; below a satoshi is dust
    "equity": Decimal("0.5"),    # whole-share venues; < half a share = flat
    "fx":     Decimal("1"),      # FX units are large; < 1 unit is noise
}


def qty_match_epsilon(asset_class: str) -> Decimal:
    """The qty-match tolerance for an asset class, falling back to a tiny default
    for unknown classes. Used by both reconcile paths so 'position matches' means
    the same thing everywhere."""
    return _QTY_EPSILON_BY_CLASS.get(asset_class, _DEFAULT_QTY_EPSILON)


# ---------------------------------------------------------------------------
# VenuePosition sign/validity contract.
#
# THE CONTRACT (relied on by reconcile): VenuePosition.qty is SIGNED — positive
# for long, negative for short. Reconcile compares the venue's signed qty against
# our signed open_qty in BOTH size and sign (see boot._position_matches and the
# live executor's reconcile). An adapter that returned an unsigned magnitude for a
# short would make every short look like a mismatch and spuriously HALT it.
#
# The sign IS the side — neither Binance positionRisk nor IB reqPositions returns
# a separate side field — so we cannot cross-check sign against an independent
# side per position. What we CAN enforce at the adapter boundary: qty is a real,
# finite Decimal, and a flat (zero) position never leaks into the list (both are
# detectable corruption, e.g. IB's NaN, which Decimal(str(nan)) turns into a
# Decimal('NaN') that silently defeats every downstream comparison).
#
# Every adapter's get_positions runs its result through validate_venue_positions,
# so the contract is enforced identically and any future adapter is forced past
# the same gate — failing loudly at the source rather than as a confusing HALT
# three layers away in reconcile.
# ---------------------------------------------------------------------------
def validate_venue_positions(
    positions: List["VenuePosition"], *, venue: str = "",
) -> List["VenuePosition"]:
    """Validate an adapter's position list against the signed-qty contract and
    return it unchanged. Raises ValueError naming the venue + symbol on the first
    violation (a non-Decimal, NaN/non-finite, or zero qty) — a programmer/adapter
    error that must fail loudly, not be silently reconciled."""
    for p in positions:
        if not isinstance(p.qty, Decimal):
            raise ValueError(
                f"{venue} position {p.symbol}: qty must be Decimal, got "
                f"{type(p.qty).__name__} — adapters cross the float boundary via str()")
        if not p.qty.is_finite():
            raise ValueError(
                f"{venue} position {p.symbol}: qty is non-finite ({p.qty}) — a NaN/Inf "
                f"venue value would silently defeat every reconcile comparison")
        if p.qty == 0:
            raise ValueError(
                f"{venue} position {p.symbol}: qty is 0 — flat positions must be "
                f"filtered out, not emitted (reconcile treats 0 as 'no position')")
    return positions