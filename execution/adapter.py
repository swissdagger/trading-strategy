"""
execution/adapter.py — The ExecutionAdapter interface.

Symmetric to feeds/base.py's AssetFeed: feeds bring market data IN, execution
adapters send orders OUT. The executor (the trade-managing service) knows
nothing about IBKR's gateway session or Binance's HMAC signing — that lives
behind the concrete class, exactly as Polygon's `C:`/`O:` prefixes live behind
the concrete feeds. Adding a venue means writing one file that inherits
ExecutionAdapter; the executor treats all venues uniformly through this
interface and routes by asset_class.

Two concrete adapters are planned:
  * IBKRAdapter  — equity + fx, via TWS API through a headless IB Gateway
                   (supervised by IBC). Owns the gateway session lifecycle and
                   the daily re-auth. connect() must confirm the TRADING-enabled
                   brokerage session, not merely the read-only portal tier.
  * BinanceAdapter — crypto, via Binance USDⓈ-M Futures. Reads and fills at the
                   same venue, so no cross-venue price reference is needed.

THE BOOT-ORDERING INVARIANT (read before implementing the executor):

    1. adapter.connect()      — BLOCKS until a trading-enabled session is live.
    2. adapter.reconcile()    — snapshot the venue's truth.
    3. executor diffs reconcile() against the DB; un-resolvable diffs -> HALTED.
    4. check kill_switch.
    5. ONLY THEN subscribe to causal-events and begin acting.

Order methods (submit_order, cancel_order) are NEVER called before connect()
returns AND reconcile() has been diffed. Placing an order against an unconfirmed
session, or before knowing what we already hold, is how restarts double-fill.
The interface cannot enforce call order at the type level — the executor's boot
sequence enforces it — but every order method's docstring restates the
precondition so an implementer can't miss it.

Each adapter owns its own connection, auth, and rate-limit budget. Adapters do
NOT share rate budget — IBKR and Binance have independent quotas — mirroring the
feeds' "no shared RateLimiter across sources" rule.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Awaitable, Callable, List, Optional, Set

from execution.types import (
    AccountEquity,
    Fill,
    MinOrderSize,
    OrderRequest,
    OrderResult,
    ReconcileResult,
    Touch,
    VenuePosition,
)


# The async callback the adapter pumps fills into as the venue reports them.
# A marketable limit may fill synchronously (reported on submit_order's return)
# OR asynchronously (later, especially partials trickling in). The executor
# wires this to "dedupe by venue_exec_id, write to `fills`, advance the order
# and trade quantities, fire OPEN/CLOSING/CLOSED transitions as quantities
# cross their thresholds." The adapter has no knowledge of the trade lifecycle —
# it only reports executions.
FillCallback = Callable[[Fill], Awaitable[None]]


class ExecutionAdapter(ABC):
    """One adapter per execution venue. Concrete subclasses set `venue` and
    `supported_classes` as class attributes.

    Lifecycle: the executor calls connect() once at boot and blocks until a
    trading-enabled session is confirmed; then reconcile() once before acting;
    then start_fill_stream() to receive async fills for the process lifetime;
    then submit_order/cancel_order as trades open and close. get_positions /
    reconcile are also called periodically by the reconciliation watchdog.
    """

    # Subclasses MUST set these as class attributes.
    venue: str = ""                    # e.g. 'coinbase'
    supported_classes: Set[str] = set()  # e.g. {'crypto'}
    # Can this venue/account hold a SHORT position? False by default: a venue
    # must opt IN to shorting. When False the executor never sizes or sends a
    # SHORT entry; it routes the signal to the shadow book instead (ADR-001:
    # UK retail spot has no short side).
    supports_short: bool = False

    # ----------------------------------------------------------------------
    # Session lifecycle
    # ----------------------------------------------------------------------
    @abstractmethod
    async def connect(self) -> None:
        """Establish and CONFIRM a trading-enabled session, then return. Must
        not return until orders can actually be placed.

        IBKR: IBC keeps the gateway logged in, but connect() is responsible for
        VERIFYING the brokerage (trading) session is live — the /iserver-tier
        session, not just the read-only portal tier. A gateway that is up but
        not trading-authenticated is the classic trap: some calls succeed, order
        calls silently fail. Also enforce the single-brokerage-session reality
        by using the dedicated bot username (a manual TWS login on the same
        username would evict this session).

        Binance: confirm signed/authenticated REST+WS connectivity and that the
        futures account is reachable.

        Runs until a live trading session is confirmed; raises if it cannot
        establish one, so the executor stays HALTED rather than trading blind.
        Idempotent: safe to call again after a session drop to re-establish.
        """

    @abstractmethod
    async def start_fill_stream(self, callback: FillCallback) -> None:
        """Begin delivering venue fills to `callback` as they occur, until
        cancelled. This is how asynchronous and partial fills reach the executor
        after submit_order has already returned.

        Each delivered Fill carries the venue_exec_id idempotency key; the
        executor dedupes, so the adapter may safely re-deliver on reconnect
        rather than risk dropping a fill. Dropping a fill is worse than
        repeating one — a missed fill desyncs position state; a repeat is a
        no-op at the executor.
        """

    # ----------------------------------------------------------------------
    # Reconciliation — the most important methods for real money
    # ----------------------------------------------------------------------
    @abstractmethod
    async def reconcile(self) -> ReconcileResult:
        """Return the venue's CURRENT truth: open positions, working orders, and
        recent fills. Called once at boot AFTER connect() and before any order,
        then periodically by the watchdog.

        The executor diffs this against the DB to resolve crash-window
        discrepancies (an order that filled at the venue but was never recorded
        locally, a DB 'OPEN' the venue has no position for, etc.). The ADAPTER
        DOES NOT RESOLVE anything — it reports faithfully; the executor decides,
        and routes the un-resolvable to HALTED. Keeping resolution out of the
        adapter keeps the dangerous judgement in one auditable place.
        """

    @abstractmethod
    async def get_positions(self) -> List[VenuePosition]:
        """Current positions only (lighter than full reconcile). Used by the
        periodic watchdog to catch position drift during normal operation,
        distinct from the boot reconcile."""

    # ----------------------------------------------------------------------
    # Market data / account reads — callable any time after connect().
    # Distinct from reconcile/get_positions: these feed PRICING and SIZING,
    # not reconciliation. The 001 schema has no quotes/equity tables by
    # design, so the venue is the only source — hence they live on the adapter.
    # ----------------------------------------------------------------------
    @abstractmethod
    async def get_touch(self, symbol: str, asset_class: str) -> Optional[Touch]:
        """Return the venue's CURRENT best bid/ask for `symbol`, or None if no
        two-sided quote is available right now (halted, pre-open, no book).

        The executor prices every marketable limit off this touch, so it is
        fetched fresh at order time — NOT cached and NOT stored in the DB (the
        001 schema has no quotes table, by design). A None return means the
        executor cannot price an order for this symbol and must skip/defer it
        rather than guess a price; returning a stale or one-sided touch would be
        worse than None.

        IBKR: a snapshot market-data request (reqTickers) on the qualified
        contract; equity/fx bid/ask. FX quotes are the pair's touch.
        Binance: the futures bookTicker (best bid/ask) for the symbol — the same
        venue orders fill at, so no cross-venue reference skew.

        bid/ask are Decimal (str-converted at the venue boundary) so downstream
        pricing stays in Decimal.
        """

    @abstractmethod
    async def get_account_equity(self) -> AccountEquity:
        """Return the account's current equity, for position sizing.

        Read live from the venue (the DB stores no equity). Used by the sizing
        policy — e.g. "5% of equity per position" reads total here, converts to a
        quantity via the symbol's price, and rounds to the venue's step size.

        IBKR: NetLiquidation (and AvailableFunds) from the account summary — the
        same summary connect() already reads to confirm the trading session.
        Binance USD-M: total and available balance from the futures account
        (the USDT-margined wallet).

        Raises if equity cannot be read (an unreadable account is a sizing-blocking
        condition — better to fail the entry loudly than size off a stale/zero
        number). Decimal throughout.
        """

    @abstractmethod
    async def get_step_size(self, symbol: str, asset_class: str) -> Optional[Decimal]:
        """Return the venue's minimum QUANTITY increment (lot/step size) for
        `symbol`, or None if it can't be determined.

        Sizing floors a computed quantity to this so a sub-increment order can't
        be rejected. A separate concern from PRICE tick size (which the executor's
        marketable-limit pricing already respects via the adapter's price
        formatting); this is the QUANTITY granularity only.

        IBKR: the qualified contract's minimum size / size increment from contract
        details (whole shares for most equity; fx has its own lot). Cached with the
        contract, since it doesn't change intraday.
        Binance USD-M: the LOT_SIZE filter's stepSize from exchangeInfo for the
        symbol. Cached per symbol.

        None means "unknown" — the sizing policy then returns the un-floored
        quantity and relies on the adapter to express it in the symbol's
        precision at submit. Decimal, str-converted at the venue boundary.
        """

    async def get_price_increment(self, symbol: str, asset_class: str) -> Optional[Decimal]:
        """Return the venue's minimum PRICE increment (tick) for `symbol`, or None
        if unknown (Coinbase: the product's quote_increment).

        The executor quantizes every limit price to this, rounding TOWARD the
        touch (buys floor, sells ceil), so a quantized limit never becomes more
        aggressive than the cap allowed. Not abstract: the default None means
        "unknown — send the limit as priced". Cached per symbol by adapters.
        """
        return None

    async def get_min_order_size(self, symbol: str, asset_class: str) -> Optional[MinOrderSize]:
        """Return the venue's minimum order size for `symbol` (base qty and/or
        quote notional), or None if unknown. Used to reject an undersized ENTRY
        before submit. Not abstract: the default None skips the check and leaves
        the venue's own rejection as the backstop."""
        return None

    # ----------------------------------------------------------------------
    # Order placement — preconditions: connect() returned, reconcile() diffed,
    # kill_switch checked. See the boot-ordering invariant at module top.
    # ----------------------------------------------------------------------
    @abstractmethod
    async def submit_order(self, order: OrderRequest) -> OrderResult:
        """Place ONE marketable-limit order. Preconditions above.

        Attaches order.client_order_id as the venue's client reference (IBKR
        orderRef; Binance newClientOrderId) so a crash between submit and ack is
        resolvable by querying the venue for that exact id rather than blindly
        re-sending. IDEMPOTENT BY CONTRACT: submitting the same client_order_id
        twice must not create two orders — the adapter relies on the venue's own
        client-id dedupe, and the executor ALSO guards with the DB unique
        constraint. Belt and braces; neither alone suffices for real money.

        Expresses order.limit_price in the venue's native tick size/precision.
        Returns the synchronous outcome; later fills arrive via the fill stream.
        On venue rejection, returns status REJECTED with reject_reason rather
        than raising — a rejection is a normal outcome the executor records
        (trade -> REJECTED), not an exception.
        """

    @abstractmethod
    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel a working order by its deterministic client id. Used by the
        escalation ladder: a capped exit that hasn't filled within its window is
        cancelled here, then re-issued at the next rung's wider cap (a new order
        with a new deterministic client_order_id). Cancelling is NOT a
        re-decision — intent stays one-way toward flat; only the working order's
        price changes. Idempotent: cancelling an already-terminal order returns
        its terminal status, not an error.
        """

    @abstractmethod
    async def amend_order(
        self, client_order_id: str, new_limit_price: Decimal,
    ) -> OrderResult:
        """Amend the limit PRICE of a working order in place, where the venue
        supports it. This is the cheaper alternative to cancel+re-issue for
        chasing a fill within CLOSING (intent unchanged, price mutable — the
        explicit allowance from the no-re-decision rule). Where a venue does not
        support native amend, the adapter MAY implement this as cancel+replace
        internally, preserving the deterministic-id discipline. Idempotent on
        terminal orders.
        """

    # ----------------------------------------------------------------------
    # Symbol translation
    # ----------------------------------------------------------------------
    @abstractmethod
    def to_venue_symbol(self, symbol: str) -> str:
        """Canonical symbol -> the venue's native form. Mirrors AssetFeed's
        to_native_symbol exactly. IBKR needs a contract spec (symbol + exchange
        + currency + secType); Binance is near pass-through. The canonical form
        is what flows through Kafka and the DB; the native form never leaves the
        adapter."""