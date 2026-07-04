"""
execution/binance_adapter.py — Binance USDⓈ-M Futures execution adapter (crypto).

Concrete ExecutionAdapter over Binance USD-M perpetual futures (fapi), the crypto
venue for QISS (equity/fx go to IB). Signed REST via HMAC-SHA256. Reads and fills
at the same venue, so unlike IB there is no cross-venue price reference — the
touch, positions, and fills all come from Binance.

## Paper vs live is the endpoint (mirrors the IB adapter's port choice)

Binance testnet (demo-fapi) is the paper venue; production fapi is live. The
adapter is endpoint-agnostic: it takes base_url + keys, so the adapter factory
picks testnet for PAPER mode and production for LIVE, exactly as IB picks port
4002 vs 4001. FAKE money on testnet, real on production.

## The client-order-id length wrinkle (the one real venue quirk)

Our canonical cv1 client_order_id is ~40 chars (cv1.<32hex>.<E|X>.<rung>), but
Binance's newClientOrderId is capped at 36 chars. client_order_id.py anticipated
exactly this: "the adapter MAY hash-compress the trade_short into its own space
WHILE preserving determinism (same input -> same compressed id) and keep a local
map back." So this adapter deterministically compresses the cv1 id to a
<=36-char venue id, and keeps a reverse map (venue id -> canonical id) so
reconcile can attribute a venue order/fill back to the right trade. The CANONICAL
id in orders.client_order_id stays the full cv1 form; the compression is the
adapter's private business and round-trips deterministically.

## Order model

All orders are marketable LIMIT orders (type=LIMIT, timeInForce=GTC), priced by
the executor upstream — the adapter only expresses price/qty in the symbol's
tick/step precision and attaches the compressed client id as newClientOrderId.
newOrderRespType=RESULT returns the synchronous fill outcome. Rejections come
back as an error code -> OrderResult(REJECTED), not an exception.

## What this adapter does NOT do

No pricing, no lifecycle logic, no discrepancy resolution — same separation as
the IB adapter. Reports faithfully; the executor decides and HALTs the
un-resolvable.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time
from decimal import Decimal
from typing import Dict, List, Optional, Set
from urllib.parse import urlencode

import httpx

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

log = logging.getLogger("executor.binance")

# Binance order status -> our OrderStatus. Unknown -> WORKING (never guess
# terminal), same posture as the IB adapter.
_BINANCE_STATUS_MAP = {
    "NEW": OrderStatus.WORKING,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELLED,
    "EXPIRED": OrderStatus.CANCELLED,
    "EXPIRED_IN_MATCH": OrderStatus.CANCELLED,
    "REJECTED": OrderStatus.REJECTED,
}

# Binance newClientOrderId max length. Our cv1 ids are longer, so we compress.
_MAX_CLIENT_ID_LEN = 36
# Our compressed venue id scheme: a short tag + a hash of the canonical id.
# 'q' + 31 hex chars of sha256 = 32 chars, well within 36 and Binance's charset.
_VENUE_ID_PREFIX = "q"


def _to_status(s: str) -> OrderStatus:
    return _BINANCE_STATUS_MAP.get(s, OrderStatus.WORKING)


def _dec(x) -> Decimal:
    return Decimal(str(x))


class BinanceAdapter(ExecutionAdapter):
    """Binance USD-M futures adapter for crypto."""

    venue = "binance"
    supported_classes: Set[str] = {"crypto"}

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        base_url: str = "https://fapi.binance.com",   # testnet: https://demo-fapi.binance.com
        recv_window_ms: int = 5000,
        request_timeout: float = 10.0,
        poll_fills_interval_s: float = 2.0,
    ):
        self._api_key = api_key
        self._api_secret = api_secret.encode()
        self._base = base_url.rstrip("/")
        self._recv_window = recv_window_ms
        self._timeout = request_timeout
        self._poll_interval = poll_fills_interval_s

        self._client: Optional[httpx.AsyncClient] = None
        self._fill_cb: Optional[FillCallback] = None
        self._fill_task: Optional[asyncio.Task] = None
        # Compressed venue id -> canonical cv1 id, for reconcile attribution.
        self._id_map: Dict[str, str] = {}
        # venue orderId (str) -> OrderIntent, so the fill poller can label a fill
        # ENTRY vs EXIT by the order that produced it (userTrades has orderId).
        self._order_intent: Dict[str, "OrderIntent"] = {}
        # Track the symbols we've placed orders on, so the fill poller knows
        # which symbols to query userTrades for.
        self._active_symbols: Set[str] = set()
        # venue_exec_id we've already delivered, so the poller doesn't re-deliver
        # (the executor also dedupes, but this avoids needless callbacks).
        self._seen_exec_ids: Set[str] = set()
        # Minimum quantity increment per venue symbol (LOT_SIZE.stepSize), for
        # sizing to floor to a tradeable lot. Cached — it doesn't change intraday.
        self._step_sizes: Dict[str, Decimal] = {}

    # ----------------------------------------------------------------------
    # Signing / request
    # ----------------------------------------------------------------------
    def _sign(self, params: dict) -> str:
        """HMAC-SHA256 over the urlencoded query string (Binance's signing)."""
        query = urlencode(params)
        sig = hmac.new(self._api_secret, query.encode(), hashlib.sha256).hexdigest()
        return sig

    async def _signed_request(self, method: str, path: str, params: dict) -> dict:
        """Send a signed request. Adds timestamp + recvWindow + signature.
        Returns parsed JSON. Raises httpx.HTTPStatusError on non-2xx so callers
        can turn a venue rejection into an OrderResult(REJECTED)."""
        p = dict(params)
        p["timestamp"] = int(time.time() * 1000)
        p["recvWindow"] = self._recv_window
        p["signature"] = self._sign(p)
        headers = {"X-MBX-APIKEY": self._api_key}
        client = await self._get_client()
        resp = await client.request(method, self._base + path, params=p, headers=headers)
        resp.raise_for_status()
        return resp.json()

    async def _public_request(self, path: str, params: dict) -> dict:
        """Unsigned GET for public market-data endpoints (bookTicker, exchangeInfo).
        No timestamp, no signature, no API key. Same client/timeout as signed.
        """
        client = await self._get_client()
        resp = await client.get(self._base + path, params=params)
        resp.raise_for_status()
        return resp.json()

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        return self._client

    # ----------------------------------------------------------------------
    # Client-id compression (the venue quirk)
    # ----------------------------------------------------------------------
    def _venue_client_id(self, canonical: str) -> str:
        """Deterministically compress a canonical cv1 id into a <=36-char venue
        id, and remember the mapping back. Same canonical -> same venue id, so
        the venue's own client-id dedupe still works across a resubmit."""
        h = hashlib.sha256(canonical.encode()).hexdigest()[:31]
        vid = _VENUE_ID_PREFIX + h
        self._id_map[vid] = canonical
        return vid

    def _canonical_from_venue_id(self, vid: str) -> Optional[str]:
        """Reverse the compression for reconcile. Returns the canonical cv1 id
        if we placed this order (it's in our map), else None (foreign order)."""
        return self._id_map.get(vid)

    # ----------------------------------------------------------------------
    # Session lifecycle
    # ----------------------------------------------------------------------
    async def connect(self) -> None:
        """Confirm signed connectivity and that the futures account is reachable
        — the crypto analogue of IB's trading-session verification. Raises if the
        account can't be read, so the executor stays HALTED rather than trading
        against an unauthenticated session."""
        try:
            # A signed account read confirms the key/secret are valid and the
            # futures account is reachable.
            await self._signed_request("GET", "/fapi/v2/balance", {})
        except Exception as e:
            raise ConnectionError(f"Binance connect failed: account not reachable ({e})")
        log.info("Binance futures session confirmed (base=%s)", self._base)

    async def start_fill_stream(self, callback: FillCallback) -> None:
        """Begin delivering fills. Binance's push path is the user-data websocket
        stream; here we use a lighter userTrades POLL (bounded, simple, reliable)
        that queries recent trades per active symbol and delivers new ones. The
        executor dedupes by venue_exec_id, so at-least-once polling is safe and
        dropping a fill (the real hazard) is avoided. A websocket user-data
        upgrade is a later optimization; the poll is correct and sufficient for
        the fill volumes QISS produces."""
        self._fill_cb = callback
        self._fill_task = asyncio.create_task(self._poll_fills_loop(), name="binance-fills")
        log.info("Binance fill poller started (interval=%.1fs)", self._poll_interval)

    async def _poll_fills_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._poll_interval)
                for symbol in list(self._active_symbols):
                    await self._poll_symbol_fills(symbol)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Binance fill poll error")

    async def _poll_symbol_fills(self, symbol: str) -> None:
        try:
            trades = await self._signed_request(
                "GET", "/fapi/v1/userTrades", {"symbol": symbol, "limit": 50},
            )
        except Exception:
            log.exception("userTrades poll failed for %s", symbol)
            return
        for t in trades:
            exec_id = str(t["id"])
            if exec_id in self._seen_exec_ids:
                continue
            self._seen_exec_ids.add(exec_id)
            if self._fill_cb is not None:
                await self._fill_cb(self._map_trade_fill(t))

    def _map_trade_fill(self, t: dict) -> Fill:
        """Map a Binance userTrades row to our Fill. `id` is the venue_exec_id
        idempotency key; `orderId` links the fill to its order (and thus, via the
        executor's submit-time map, to our trade). commission carries the fee.

        Intent is derived from the order this fill belongs to: if we placed the
        order (its venue orderId is in our submitted-order intent map) we know
        whether it was an ENTRY or EXIT. If we can't tell (a fill for an order we
        didn't place, or one placed before this process started), we leave intent
        ENTRY as a neutral default — the executor attributes by venue_order_id and
        will only advance a trade it actually owns, so a mislabelled foreign fill
        can't corrupt our accounting."""
        order_id = str(t["orderId"])
        intent = self._order_intent.get(order_id, OrderIntent.ENTRY)
        return Fill(
            venue_exec_id=str(t["id"]),
            venue_order_id=order_id,
            qty=_dec(t["qty"]),
            price=_dec(t["price"]),
            intent=intent,
            filled_at_ms=int(t["time"]),
            fee=_dec(t.get("commission", 0)),
            fee_currency=t.get("commissionAsset"),
        )

    # ----------------------------------------------------------------------
    # Reconciliation
    # ----------------------------------------------------------------------
    async def reconcile(self) -> ReconcileResult:
        positions = await self.get_positions()
        open_orders = await self._open_orders()
        recent_fills = await self._recent_fills()
        return ReconcileResult(
            positions=positions, open_orders=open_orders, recent_fills=recent_fills,
        )

    async def get_positions(self) -> List[VenuePosition]:
        rows = await self._signed_request("GET", "/fapi/v3/positionRisk", {})
        out: List[VenuePosition] = []
        for r in rows:
            amt = _dec(r["positionAmt"])   # signed: + long, - short
            if amt == 0:
                continue
            out.append(VenuePosition(
                symbol=r["symbol"],
                qty=amt,
                avg_price=_dec(r["entryPrice"]) if r.get("entryPrice") else None,
            ))
        return out

    # ----------------------------------------------------------------------
    # Market data / account reads
    # ----------------------------------------------------------------------
    async def get_touch(self, symbol: str, asset_class: str) -> Optional[Touch]:
        """Best bid/ask from the futures bookTicker. None if the symbol has no
        two-sided quote (delisted, halted) or the read fails."""
        venue_symbol = self.to_venue_symbol(symbol)
        try:
            r = await self._public_request(
                "/fapi/v1/ticker/bookTicker", {"symbol": venue_symbol},
            )
        except Exception:
            log.exception("get_touch: bookTicker failed for %s", symbol)
            return None

        bid_raw = r.get("bidPrice")
        ask_raw = r.get("askPrice")
        if bid_raw is None or ask_raw is None:
            return None
        bid = _dec(bid_raw)
        ask = _dec(ask_raw)
        # Binance returns "0" for an absent side; require a real two-sided quote.
        if bid <= 0 or ask <= 0 or ask < bid:
            log.warning("get_touch: implausible quote for %s bid=%s ask=%s", symbol, bid, ask)
            return None
        return Touch(bid=bid, ask=ask)

    async def get_account_equity(self) -> AccountEquity:
        """Total + available USDT balance from the futures account. Raises if the
        balance can't be read or has no USDT asset (USD-M futures are USDT-margined,
        so no USDT balance means nothing to size against)."""
        rows = await self._signed_request("GET", "/fapi/v2/balance", {})
        for r in rows:
            if r.get("asset") == "USDT":
                total = _dec(r["balance"])            # total wallet balance
                available = _dec(r["availableBalance"]) if r.get("availableBalance") is not None else None
                return AccountEquity(total=total, currency="USDT", available=available)
        raise RuntimeError("Binance balance has no USDT asset — cannot read equity")

    async def get_step_size(self, symbol: str, asset_class: str) -> Optional[Decimal]:
        """LOT_SIZE.stepSize from exchangeInfo for the symbol, cached per symbol.

        exchangeInfo is a public endpoint (uses _public_request). Each symbol
        carries a `filters` array; the LOT_SIZE filter's `stepSize` is the quantity
        increment. Queried per-symbol and cached. Returns None on any miss, and the
        sizing policy then leaves the qty un-floored."""
        venue_symbol = self.to_venue_symbol(symbol)
        cached = self._step_sizes.get(venue_symbol)
        if cached is not None:
            return cached
        try:
            info = await self._public_request(
                "/fapi/v1/exchangeInfo", {"symbol": venue_symbol},
            )
        except Exception:
            log.exception("get_step_size: exchangeInfo failed for %s", symbol)
            return None

        symbols = info.get("symbols") or []
        row = next((sym for sym in symbols if sym.get("symbol") == venue_symbol), None)
        if row is None:
            log.warning("get_step_size: %s not in exchangeInfo", venue_symbol)
            return None
        lot = next((f for f in row.get("filters", []) if f.get("filterType") == "LOT_SIZE"), None)
        if lot is None or not lot.get("stepSize"):
            return None
        step = _dec(lot["stepSize"])
        if step <= 0:
            return None
        self._step_sizes[venue_symbol] = step
        return step

    async def _open_orders(self) -> List[VenueOrder]:
        rows = await self._signed_request("GET", "/fapi/v1/openOrders", {})
        out: List[VenueOrder] = []
        for r in rows:
            vid = r.get("clientOrderId", "")
            canonical = self._canonical_from_venue_id(vid)
            if canonical is None:
                continue   # not one of ours (foreign/manual order)
            filled = _dec(r["executedQty"])
            total = _dec(r["origQty"])
            side = TradeSide.LONG if r["side"] == "BUY" else TradeSide.SHORT
            out.append(VenueOrder(
                client_order_id=canonical,
                venue_order_id=str(r["orderId"]),
                symbol=r["symbol"], side=side,
                qty=total, filled_qty=filled,
                status=_to_status(r["status"]),
            ))
        return out

    async def _recent_fills(self) -> List[Fill]:
        out: List[Fill] = []
        for symbol in list(self._active_symbols):
            try:
                trades = await self._signed_request(
                    "GET", "/fapi/v1/userTrades", {"symbol": symbol, "limit": 50},
                )
            except Exception:
                continue
            for t in trades:
                out.append(self._map_trade_fill(t))
        return out

    # ----------------------------------------------------------------------
    # Order placement
    # ----------------------------------------------------------------------
    async def submit_order(self, order: OrderRequest) -> OrderResult:
        self._active_symbols.add(self.to_venue_symbol(order.symbol))
        vid = self._venue_client_id(order.client_order_id)
        side = "BUY" if _is_buy(order) else "SELL"
        params = {
            "symbol": self.to_venue_symbol(order.symbol),
            "side": side,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": _fmt(order.qty),
            "price": _fmt(order.limit_price),
            "newClientOrderId": vid,
            "newOrderRespType": "RESULT",   # synchronous fill outcome
        }
        try:
            resp = await self._signed_request("POST", "/fapi/v1/order", params)
        except httpx.HTTPStatusError as e:
            reason = _error_reason(e)
            log.info("Binance rejected order %s: %s", order.client_order_id, reason)
            return OrderResult(
                client_order_id=order.client_order_id,
                status=OrderStatus.REJECTED, reject_reason=reason,
            )
        return self._result_from_order_resp(order.client_order_id, resp, order.intent)

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        vid = self._venue_client_id(client_order_id)   # deterministic -> same vid
        # We need the symbol; find it among our tracked open orders. Binance
        # cancel requires symbol + (orderId | origClientOrderId).
        symbol = await self._symbol_for_client_id(vid)
        if symbol is None:
            return OrderResult(
                client_order_id=client_order_id, status=OrderStatus.CANCELLED,
                reject_reason="cancel_no_working_order",
            )
        try:
            resp = await self._signed_request(
                "DELETE", "/fapi/v1/order",
                {"symbol": symbol, "origClientOrderId": vid},
            )
        except httpx.HTTPStatusError as e:
            return OrderResult(
                client_order_id=client_order_id, status=OrderStatus.CANCELLED,
                reject_reason=_error_reason(e),
            )
        return self._result_from_order_resp(client_order_id, resp, OrderIntent.EXIT)

    async def amend_order(
        self, client_order_id: str, new_limit_price: Decimal,
    ) -> OrderResult:
        """Binance PUT /fapi/v1/order modifies a LIMIT order's price (and
        quantity — required by the endpoint). We re-send the same quantity with
        the new price, keyed by origClientOrderId, preserving the deterministic
        id."""
        vid = self._venue_client_id(client_order_id)
        symbol = await self._symbol_for_client_id(vid)
        if symbol is None:
            return OrderResult(
                client_order_id=client_order_id, status=OrderStatus.CANCELLED,
                reject_reason="amend_no_working_order",
            )
        # Fetch current qty (modify requires both price and quantity).
        working = await self._get_order(symbol, vid)
        if working is None:
            return OrderResult(
                client_order_id=client_order_id, status=OrderStatus.CANCELLED,
                reject_reason="amend_no_working_order",
            )
        side = working["side"]
        qty = working["origQty"]
        try:
            resp = await self._signed_request(
                "PUT", "/fapi/v1/order",
                {"symbol": symbol, "origClientOrderId": vid, "side": side,
                 "quantity": qty, "price": _fmt(new_limit_price)},
            )
        except httpx.HTTPStatusError as e:
            return OrderResult(
                client_order_id=client_order_id, status=OrderStatus.WORKING,
                reject_reason=_error_reason(e),
            )
        return self._result_from_order_resp(client_order_id, resp, OrderIntent.EXIT)

    async def _get_order(self, symbol: str, vid: str) -> Optional[dict]:
        try:
            return await self._signed_request(
                "GET", "/fapi/v1/order", {"symbol": symbol, "origClientOrderId": vid},
            )
        except Exception:
            return None

    async def _symbol_for_client_id(self, vid: str) -> Optional[str]:
        """Find which symbol has a working order with this venue client id, by
        scanning our open orders. Bounded by the small number of active symbols."""
        try:
            rows = await self._signed_request("GET", "/fapi/v1/openOrders", {})
        except Exception:
            return None
        for r in rows:
            if r.get("clientOrderId") == vid:
                return r["symbol"]
        return None

    # ----------------------------------------------------------------------
    # Symbol translation
    # ----------------------------------------------------------------------
    def to_venue_symbol(self, symbol: str) -> str:
        """Binance symbols are pass-through (BTCUSDT, ETHUSDT). Mirrors the
        feed's to_native_symbol."""
        return symbol

    # ----------------------------------------------------------------------
    # response mapping
    # ----------------------------------------------------------------------
    def _result_from_order_resp(
        self, canonical_id: str, resp: dict, intent: OrderIntent,
    ) -> OrderResult:
        status = _to_status(resp.get("status", "NEW"))
        filled = _dec(resp.get("executedQty", 0))
        venue_order_id = str(resp.get("orderId")) if resp.get("orderId") else None
        # Remember this order's intent by its venue orderId, so a later async
        # userTrades fill for it is labelled ENTRY/EXIT correctly.
        if venue_order_id is not None:
            self._order_intent[venue_order_id] = intent
        fills: List[Fill] = []
        # RESULT respType gives avgPrice/executedQty but not per-trade ids; a
        # synchronous full/partial fill is reflected in executedQty. Per-trade
        # Fills (with venue_exec_id) arrive via the fill poller. If Binance
        # returned a fills array (FULL respType), map it — stamping the parent
        # venue_order_id so the executor can attribute it.
        for f in resp.get("fills", []) or []:
            fills.append(Fill(
                venue_exec_id=str(f.get("tradeId", f.get("id", ""))),
                venue_order_id=venue_order_id,
                qty=_dec(f["qty"]), price=_dec(f["price"]),
                intent=intent, filled_at_ms=int(resp.get("updateTime", 0)),
                fee=_dec(f.get("commission", 0)),
                fee_currency=f.get("commissionAsset"),
            ))
        reject = None
        if status == OrderStatus.REJECTED:
            reject = "venue_rejected"
        return OrderResult(
            client_order_id=canonical_id, status=status,
            venue_order_id=venue_order_id, filled_qty=filled,
            fills=fills, reject_reason=reject,
        )

    async def aclose(self) -> None:
        if self._fill_task is not None:
            self._fill_task.cancel()
            try:
                await self._fill_task
            except asyncio.CancelledError:
                pass
        if self._client is not None:
            await self._client.aclose()


# ---------------------------------------------------------------------------
# helpers (pure)
# ---------------------------------------------------------------------------
def _is_buy(order: OrderRequest) -> bool:
    """ENTRY: buy to open LONG, sell to open SHORT. EXIT: the opposite."""
    long_side = order.side == TradeSide.LONG
    if order.intent == OrderIntent.ENTRY:
        return long_side
    return not long_side


def _fmt(x: Decimal) -> str:
    """Binance wants numeric strings; normalize to avoid scientific notation and
    trailing-zero noise. Tick/step-size rounding is a symbol-precision concern
    the executor/config supplies; here we render the Decimal faithfully."""
    return format(x.normalize(), "f")


def _error_reason(e: "httpx.HTTPStatusError") -> str:
    try:
        body = e.response.json()
        return f"binance {body.get('code')}: {body.get('msg')}"
    except Exception:
        return f"http {e.response.status_code}"