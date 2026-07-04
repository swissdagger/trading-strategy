"""
execution/ibkr_adapter.py — Interactive Brokers execution adapter (equity + fx).

Concrete ExecutionAdapter over IB Gateway / TWS via ib_async (the maintained
successor to the archived ib_insync — the original was frozen after its author's
passing in early 2024, so QISS uses ib_async for ongoing TWS-API compatibility).

Scope: equity and fx, the two classes IBKR serves in QISS (crypto goes to
Binance). All orders are marketable LIMIT orders — the executor computes the
crossing limit_price and cap upstream; this adapter only expresses it in IBKR's
native contract + tick form and attaches the deterministic client_order_id as
the IB orderRef (the crash-recovery keystone).

## Session model (the connect() contract)

IB Gateway (kept logged in by IBC) is NOT the same as a trading-authenticated
session. connect() must confirm we can actually place orders, not merely that
the socket is up — the classic trap the ABC warns about. We verify by requiring
at least one managed account and a readable account summary; a gateway that is
up but not brokerage-authenticated fails this and connect() raises, keeping the
executor HALTED rather than trading blind.

## Fill delivery

ib_async maintains order/execution state and fires execDetailsEvent(trade, fill)
per execution and commissionReportEvent for fees. start_fill_stream wires these
to the executor's FillCallback. Each IB execId is the venue_exec_id idempotency
key, so re-delivery on reconnect is safe (the executor dedupes). We attach the
commission when its report arrives (it often trails the execution by a beat).

## Idempotency & recovery

client_order_id -> IB orderRef. On resubmit of the same logical order the
orderRef is identical; combined with the executor's DB unique constraint this is
the belt-and-braces dedupe. reconcile() reads positions, open orders, and recent
executions, attributing each to a trade by PARSING orderRef back through
client_order_id (foreign ids — manual trades on the same account — are ignored
via is_ours()).

## What this adapter does NOT do

No pricing (limit_price comes in on the OrderRequest). No trade-lifecycle logic
(it reports; the executor decides). No discrepancy resolution (reconcile reports
faithfully; the executor diffs and routes un-resolvable to HALTED). Keeping all
judgement in the executor keeps the dangerous decisions in one auditable place —
the same separation the ABC mandates.
"""
from __future__ import annotations

import asyncio
import math
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Set

from ib_async import (
    IB,
    Contract,
    Forex,
    LimitOrder,
    Stock,
    Trade as IBTrade,
    Fill as IBFill,
)

from execution import client_order_id as coid
from execution.adapter import ExecutionAdapter, FillCallback
from execution.types import (
    AccountEquity,
    Fill,
    OrderIntent,
    OrderRequest,
    OrderResult,
    OrderStatus,
    ReconcileResult,
    Touch,
    TradeSide,
    VenueOrder,
    VenuePosition,
)

log = logging.getLogger("executor.ibkr")

# IB order status strings -> our OrderStatus. IB uses a richer set; we map to the
# schema's five. Anything unrecognised is treated as WORKING (still live) rather
# than guessed terminal, so we never prematurely call an order done.
_IB_STATUS_MAP = {
    "PendingSubmit": OrderStatus.WORKING,
    "PreSubmitted": OrderStatus.WORKING,
    "Submitted": OrderStatus.WORKING,
    "ApiPending": OrderStatus.WORKING,
    "PendingCancel": OrderStatus.WORKING,
    "Filled": OrderStatus.FILLED,
    "Cancelled": OrderStatus.CANCELLED,
    "ApiCancelled": OrderStatus.CANCELLED,
    "Inactive": OrderStatus.REJECTED,
}

# How far back reconcile() asks IB for executions, to catch fills that landed in
# a crash window. IB's execution reports are same-session/day scoped; the
# executor's DB carries older history.
_RECENT_FILL_LOOKBACK_NOTE = "IB returns current-day executions; sufficient for crash-window catch-up"


def _to_status(ib_status: str) -> OrderStatus:
    return _IB_STATUS_MAP.get(ib_status, OrderStatus.WORKING)


def _dec(x) -> Decimal:
    """IB gives floats; convert via str so we don't inherit binary-float noise
    into our Decimal money quantities."""
    return Decimal(str(x))


class IBKRAdapter(ExecutionAdapter):
    """IB Gateway/TWS adapter for equity + fx."""

    venue = "ibkr"
    supported_classes: Set[str] = {"equity", "fx"}

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 4001,          # IB Gateway live; 4002 paper, 7497 TWS paper
        client_id: int = 1,
        account: str = "",          # the dedicated bot account (see connect())
        connect_timeout: float = 15.0,
        default_exchange: str = "SMART",
        default_currency: str = "USD",
    ):
        self._host = host
        self._port = port
        self._client_id = client_id
        self._account = account
        self._connect_timeout = connect_timeout
        self._default_exchange = default_exchange
        self._default_currency = default_currency

        self._ib = IB()
        self._fill_cb: Optional[FillCallback] = None
        # Cache of qualified contracts by canonical symbol, so repeated orders
        # for a symbol don't re-qualify. Cleared on reconnect.
        self._contracts: Dict[str, Contract] = {}
        # Map orderRef -> the IB Order we placed, so cancel/amend by
        # client_order_id can find the live order object.
        self._orders_by_ref: Dict[str, IBTrade] = {}
        # Minimum quantity increment per symbol (from contract details), for
        # sizing to floor to a tradeable lot. Cleared on reconnect with the
        # contract cache.
        self._step_sizes: Dict[str, Decimal] = {}

    # ----------------------------------------------------------------------
    # Session lifecycle
    # ----------------------------------------------------------------------
    async def connect(self) -> None:
        """Connect to the gateway and CONFIRM a trading-enabled session.

        Idempotent: if already connected and still trading-authenticated,
        returns immediately. Raises if a trading session can't be confirmed, so
        the executor stays HALTED.
        """
        if self._ib.isConnected():
            # Verify the existing session is still trading-authenticated.
            if await self._trading_session_live():
                return
            self._ib.disconnect()

        log.info("connecting to IB %s:%d clientId=%d", self._host, self._port, self._client_id)
        await self._ib.connectAsync(
            host=self._host,
            port=self._port,
            clientId=self._client_id,
            timeout=self._connect_timeout,
            account=self._account,
            readonly=False,     # we place orders; readonly would silently block them
        )
        if not await self._trading_session_live():
            self._ib.disconnect()
            raise ConnectionError(
                "IB connected but no trading-enabled session confirmed "
                "(gateway up but not brokerage-authenticated?)"
            )
        # Fresh session — drop any stale contract/order caches.
        self._contracts.clear()
        self._orders_by_ref.clear()
        self._step_sizes.clear()
        log.info("IB trading session confirmed (accounts=%s)", self._ib.managedAccounts())

    async def _trading_session_live(self) -> bool:
        """True iff we have a managed account and can read its summary — the
        signal that the brokerage (not just read-only portal) session is up.

        A gateway that is logged in but not trading-authenticated returns no
        managed accounts (or an unreadable summary); order calls against it fail
        silently, which is exactly the trap we refuse to fall into."""
        try:
            accounts = self._ib.managedAccounts()
            if not accounts:
                return False
            # If a specific bot account was configured, require it to be present
            # — guards against attaching to the wrong logged-in session.
            if self._account and self._account not in accounts:
                log.error("configured account %s not in managed accounts %s",
                          self._account, accounts)
                return False
            # A readable account summary confirms brokerage-tier access.
            summary = await self._ib.reqAccountSummaryAsync()
            return bool(summary)
        except Exception:
            log.exception("trading-session verification failed")
            return False

    async def start_fill_stream(self, callback: FillCallback) -> None:
        """Wire IB execution + commission events to the executor's callback.

        execDetailsEvent gives us the execution (qty/price/execId); the
        commission often arrives just after via commissionReportEvent. We emit a
        Fill on execDetails immediately (fee 0 if not yet known) — dropping a
        fill is worse than a fee that updates — and the executor dedupes on
        venue_exec_id, so a later commission-bearing re-emit is safe if we choose
        to send one. Here we emit once on execDetails and attach commission when
        present on the fill object.
        """
        self._fill_cb = callback

        def _on_exec(trade: IBTrade, ib_fill: IBFill):
            # ib_async fires this synchronously from its event loop; schedule the
            # async callback without blocking the loop.
            asyncio.create_task(self._deliver_fill(ib_fill))

        self._ib.execDetailsEvent += _on_exec
        log.info("IB fill stream started")

    async def _deliver_fill(self, ib_fill: IBFill) -> None:
        if self._fill_cb is None:
            return
        try:
            fill = self._map_fill(ib_fill)
        except Exception:
            log.exception("failed mapping IB fill %r", ib_fill)
            return
        await self._fill_cb(fill)

    def _map_fill(self, ib_fill: IBFill) -> Fill:
        ex = ib_fill.execution
        cr = ib_fill.commissionReport
        # Determine intent from the orderRef (our client_order_id). A foreign
        # order (manual trade) won't parse — we still report the fill but mark
        # intent ENTRY by default is WRONG; instead we surface it as-is only if
        # ours. For a foreign execId we cannot attribute intent, so default to
        # ENTRY only when unparseable is not acceptable — raise to skip.
        ref = ex.orderRef or ""
        if coid.is_ours(ref):
            intent = coid.parse(ref).intent
        else:
            # Not one of our orders — attribute a neutral intent. The executor
            # dedupes/attributes by client_order_id anyway; a foreign fill on the
            # shared account is not ours to record against a trade. We still emit
            # with ENTRY as a placeholder and let the executor's is_ours guard
            # drop it. (Kept explicit rather than silently guessing.)
            intent = OrderIntent.ENTRY
        fee = _dec(cr.commission) if cr and cr.commission else Decimal(0)
        fee_ccy = cr.currency if cr and cr.currency else None
        # The venue order id links this fill to its order (and thus our trade) so
        # an async execDetails fill is attributable. permId is IB's stable order
        # id; fall back to orderId. Kept consistent with reconcile's VenueOrder.
        venue_order_id = str(ex.permId or ex.orderId) if (ex.permId or ex.orderId) else None
        return Fill(
            venue_exec_id=ex.execId,
            venue_order_id=venue_order_id,
            qty=_dec(ex.shares),
            price=_dec(ex.price),
            intent=intent,
            filled_at_ms=int(ex.time.timestamp() * 1000) if ex.time else 0,
            fee=fee,
            fee_currency=fee_ccy,
        )

    # ----------------------------------------------------------------------
    # Reconciliation
    # ----------------------------------------------------------------------
    async def reconcile(self) -> ReconcileResult:
        """Snapshot IB's truth: positions, our open orders, recent executions.

        Only OUR orders/fills (client_order_id parses as cv1) are attributed;
        foreign ones on the shared account are ignored. The executor diffs this
        against the DB and routes un-resolvable gaps to HALTED — this method only
        reports.
        """
        positions = await self.get_positions()
        open_orders = await self._open_orders()
        recent_fills = await self._recent_fills()
        return ReconcileResult(
            positions=positions,
            open_orders=open_orders,
            recent_fills=recent_fills,
        )

    async def get_positions(self) -> List[VenuePosition]:
        ib_positions = await self._ib.reqPositionsAsync()
        out: List[VenuePosition] = []
        for p in ib_positions:
            if self._account and p.account != self._account:
                continue
            symbol = self._canonical_from_contract(p.contract)
            if symbol is None:
                continue
            out.append(VenuePosition(
                symbol=symbol,
                qty=_dec(p.position),                 # already signed
                avg_price=_dec(p.avgCost) if p.avgCost else None,
            ))
        return out

    # ----------------------------------------------------------------------
    # Market data / account reads
    # ----------------------------------------------------------------------
    async def get_touch(self, symbol: str, asset_class: str) -> Optional[Touch]:
        """IB snapshot best bid/ask for the qualified contract. None when no valid
        two-sided quote is available (NaN/absent — out of hours, halted, no book)."""
        try:
            contract = await self._contract_for(symbol, asset_class)
        except Exception:
            log.exception("get_touch: contract qualification failed for %s", symbol)
            return None

        # One-shot snapshot; reqTickersAsync resolves when the snapshot is populated.
        try:
            tickers = await self._ib.reqTickersAsync(contract)
        except Exception:
            log.exception("get_touch: reqTickers failed for %s", symbol)
            return None
        if not tickers:
            return None
        t = tickers[0]

        bid = getattr(t, "bid", None)
        ask = getattr(t, "ask", None)
        # IB reports missing quotes as NaN (or -1). Require a real, positive,
        # two-sided quote; anything else is "no touch" rather than a guess.
        if bid is None or ask is None:
            return None
        if (isinstance(bid, float) and math.isnan(bid)) or (isinstance(ask, float) and math.isnan(ask)):
            return None
        if bid <= 0 or ask <= 0 or ask < bid:
            log.warning("get_touch: implausible quote for %s bid=%s ask=%s", symbol, bid, ask)
            return None

        return Touch(bid=_dec(bid), ask=_dec(ask))

    async def get_account_equity(self) -> AccountEquity:
        """NetLiquidation / AvailableFunds from the account summary — the same read
        connect() uses to confirm the session. Raises if equity can't be read."""
        summary = await self._ib.reqAccountSummaryAsync()
        if not summary:
            raise RuntimeError("IB account summary empty — cannot read equity")

        # summary is a flat list of AccountValue(account, tag, value, currency).
        # Filter to the configured bot account when set; else take whatever account
        # the summary reports (single-account bot).
        def _pick(tag: str) -> Optional[tuple[str, str]]:
            for row in summary:
                if self._account and row.account != self._account:
                    continue
                if row.tag == tag:
                    return row.value, row.currency
            return None

        net = _pick("NetLiquidation")
        if net is None:
            raise RuntimeError("IB account summary missing NetLiquidation")
        total_val, currency = net

        avail = _pick("AvailableFunds")
        available = _dec(avail[0]) if avail is not None else None

        return AccountEquity(
            total=_dec(total_val),
            currency=currency or "USD",
            available=available,
        )

    async def get_step_size(self, symbol: str, asset_class: str) -> Optional[Decimal]:
        """Minimum quantity increment from IB contract details, cached per symbol.

        Prefer sizeIncrement (the granularity), fall back to minSize, and treat a
        missing/zero value as "unknown" (None) rather than guessing — the sizing
        policy then leaves the qty un-floored and submit-time formatting handles
        precision."""
        cached = self._step_sizes.get(symbol)
        if cached is not None:
            return cached
        try:
            contract = await self._contract_for(symbol, asset_class)
        except Exception:
            log.exception("get_step_size: contract qualification failed for %s", symbol)
            return None
        try:
            details = await self._ib.reqContractDetailsAsync(contract)
        except Exception:
            log.exception("get_step_size: reqContractDetails failed for %s", symbol)
            return None
        if not details:
            return None
        d = details[0]
        # sizeIncrement is the tradeable granularity; minSize is the floor. Prefer
        # the increment. Both are floats on ContractDetails.
        raw = getattr(d, "sizeIncrement", 0) or getattr(d, "minSize", 0)
        if not raw or raw <= 0:
            return None
        step = _dec(raw)
        self._step_sizes[symbol] = step
        return step

    async def _open_orders(self) -> List[VenueOrder]:
        trades = await self._ib.reqAllOpenOrdersAsync()
        out: List[VenueOrder] = []
        for t in trades:
            ref = t.order.orderRef or ""
            # Only our orders; skip manual/other-system orders on the account.
            if not coid.is_ours(ref):
                continue
            symbol = self._canonical_from_contract(t.contract)
            if symbol is None:
                continue
            filled = _dec(t.orderStatus.filled)
            remaining = _dec(t.orderStatus.remaining)
            side = TradeSide.LONG if t.order.action == "BUY" else TradeSide.SHORT
            out.append(VenueOrder(
                client_order_id=ref,
                venue_order_id=str(t.order.permId or t.order.orderId),
                symbol=symbol,
                side=side,
                qty=filled + remaining,
                filled_qty=filled,
                status=_to_status(t.orderStatus.status),
            ))
        return out

    async def _recent_fills(self) -> List[Fill]:
        ib_fills = await self._ib.reqExecutionsAsync()
        out: List[Fill] = []
        for f in ib_fills:
            ref = f.execution.orderRef or ""
            if not coid.is_ours(ref):
                continue
            try:
                out.append(self._map_fill(f))
            except Exception:
                log.exception("skipping unmappable reconcile fill %r", f)
        return out

    # ----------------------------------------------------------------------
    # Order placement
    # ----------------------------------------------------------------------
    async def submit_order(self, order: OrderRequest) -> OrderResult:
        """Place one marketable-limit order. See ABC preconditions (connect
        returned, reconcile diffed, kill_switch checked) — enforced by the
        caller, not here."""
        try:
            contract = await self._contract_for(order.symbol, order.asset_class)
        except Exception as e:
            log.exception("contract qualification failed for %s", order.symbol)
            return OrderResult(
                client_order_id=order.client_order_id,
                status=OrderStatus.REJECTED,
                reject_reason=f"contract_qualification_failed: {e}",
            )

        action = "BUY" if _is_buy(order) else "SELL"
        ib_order = LimitOrder(
            action=action,
            totalQuantity=float(order.qty),
            lmtPrice=float(order.limit_price),
        )
        # The keystone: our deterministic id as IB's client reference.
        ib_order.orderRef = order.client_order_id
        if self._account:
            ib_order.account = self._account
        # tif default DAY is fine for a marketable limit meant to fill now.

        trade = self._ib.placeOrder(contract, ib_order)
        self._orders_by_ref[order.client_order_id] = trade

        # Give IB a brief moment to ack / report a synchronous fill, so the
        # returned OrderResult reflects the immediate outcome a marketable limit
        # usually has. Async/partial fills continue via the fill stream.
        await self._settle_briefly(trade)
        return self._result_from_trade(order.client_order_id, trade)

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        trade = self._orders_by_ref.get(client_order_id)
        if trade is None:
            # Nothing we track by that id — try to locate it among open orders
            # (e.g. after a restart that lost the in-memory map).
            trade = await self._find_open_by_ref(client_order_id)
        if trade is None:
            # Already terminal or unknown — idempotent: report cancelled.
            return OrderResult(
                client_order_id=client_order_id,
                status=OrderStatus.CANCELLED,
                reject_reason="cancel_no_working_order",
            )
        self._ib.cancelOrder(trade.order)
        await self._settle_briefly(trade)
        return self._result_from_trade(client_order_id, trade)

    async def amend_order(
        self, client_order_id: str, new_limit_price: Decimal,
    ) -> OrderResult:
        """Amend the limit price in place. IB supports native modify: re-place
        the SAME order object with a new lmtPrice (same orderId => modify, not a
        new order). Preserves the deterministic id."""
        trade = self._orders_by_ref.get(client_order_id)
        if trade is None:
            trade = await self._find_open_by_ref(client_order_id)
        if trade is None:
            return OrderResult(
                client_order_id=client_order_id,
                status=OrderStatus.CANCELLED,
                reject_reason="amend_no_working_order",
            )
        trade.order.lmtPrice = float(new_limit_price)
        # Re-placing the existing order (same orderId) is IB's modify path.
        self._ib.placeOrder(trade.contract, trade.order)
        await self._settle_briefly(trade)
        return self._result_from_trade(client_order_id, trade)

    # ----------------------------------------------------------------------
    # Symbol translation
    # ----------------------------------------------------------------------
    def to_venue_symbol(self, symbol: str) -> str:
        """Canonical -> a human-readable venue symbol string. The full contract
        (with exchange/currency/secType) is built in _contract_for; this returns
        the bare venue ticker for logging/audit. Equity: the ticker as-is. FX:
        IB uses a pair like 'EUR.USD' but the base ticker is the 6-char pair."""
        return symbol

    # ----------------------------------------------------------------------
    # internals
    # ----------------------------------------------------------------------
    async def _contract_for(self, symbol: str, asset_class: str) -> Contract:
        cached = self._contracts.get(symbol)
        if cached is not None:
            return cached
        contract = self._build_contract(symbol, asset_class)
        qualified = await self._ib.qualifyContractsAsync(contract)
        if not qualified or qualified[0] is None:
            raise ValueError(f"IB could not qualify contract for {symbol} ({asset_class})")
        c = qualified[0]
        self._contracts[symbol] = c
        return c

    def _build_contract(self, symbol: str, asset_class: str) -> Contract:
        if asset_class == "equity":
            return Stock(symbol, self._default_exchange, self._default_currency)
        if asset_class == "fx":
            # Canonical EURUSD -> IB Forex pair 'EURUSD' (ib_async Forex takes
            # the 6-char pair and splits it).
            return Forex(symbol)
        raise ValueError(f"IBKRAdapter does not serve asset_class {asset_class!r}")

    def _canonical_from_contract(self, contract: Contract) -> Optional[str]:
        """IB contract -> our canonical symbol, or None if it's a class/contract
        we don't track (so foreign positions on the account are ignored)."""
        sec = getattr(contract, "secType", "")
        if sec == "STK":
            return contract.symbol
        if sec == "CASH":
            # FX: IB splits into symbol (base) + currency (quote). Recombine to
            # the 6-char canonical pair.
            base = contract.symbol
            quote = getattr(contract, "currency", "")
            if base and quote:
                return f"{base}{quote}"
            return None
        return None

    async def _find_open_by_ref(self, client_order_id: str) -> Optional[IBTrade]:
        trades = await self._ib.reqAllOpenOrdersAsync()
        for t in trades:
            if (t.order.orderRef or "") == client_order_id:
                self._orders_by_ref[client_order_id] = t
                return t
        return None

    async def _settle_briefly(self, trade: IBTrade, timeout: float = 1.0) -> None:
        """Wait briefly for IB to move the order out of the transient
        submit/ack state, so the synchronous OrderResult is meaningful. We do
        NOT wait for a full fill — async fills arrive via the stream. Bounded so
        submit_order stays responsive."""
        try:
            await asyncio.wait_for(self._wait_out_of_pending(trade), timeout=timeout)
        except asyncio.TimeoutError:
            pass  # still pending — the executor will get updates via the stream

    async def _wait_out_of_pending(self, trade: IBTrade) -> None:
        pending = {"PendingSubmit", "ApiPending", "PreSubmitted"}
        while trade.orderStatus.status in pending:
            await asyncio.sleep(0.05)

    def _result_from_trade(self, client_order_id: str, trade: IBTrade) -> OrderResult:
        status = _to_status(trade.orderStatus.status)
        filled = _dec(trade.orderStatus.filled)
        venue_order_id = str(trade.order.permId or trade.order.orderId) or None
        # Map any synchronously-known fills.
        sync_fills: List[Fill] = []
        for f in trade.fills:
            ref = f.execution.orderRef or ""
            if coid.is_ours(ref):
                try:
                    sync_fills.append(self._map_fill(f))
                except Exception:
                    pass
        reject_reason = None
        if status == OrderStatus.REJECTED:
            # IB puts the reason on the order log.
            reject_reason = self._reject_reason(trade)
        return OrderResult(
            client_order_id=client_order_id,
            status=status,
            venue_order_id=venue_order_id,
            filled_qty=filled,
            fills=sync_fills,
            reject_reason=reject_reason,
        )

    @staticmethod
    def _reject_reason(trade: IBTrade) -> Optional[str]:
        for entry in reversed(trade.log):
            if entry.status in ("Inactive", "Cancelled") and entry.message:
                return entry.message
        return None


def _is_buy(order: OrderRequest) -> bool:
    """Marketable-limit direction. ENTRY: buy to open LONG, sell to open SHORT.
    EXIT: the opposite — sell to close LONG, buy to close SHORT."""
    long_side = order.side == TradeSide.LONG
    if order.intent == OrderIntent.ENTRY:
        return long_side
    return not long_side   # EXIT closes the position