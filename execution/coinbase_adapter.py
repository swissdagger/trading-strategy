"""
execution/coinbase_adapter.py — Coinbase Advanced Trade SPOT as an ExecutionAdapter.

ADR-001 Phase 4. Long-only spot in a DEDICATED portfolio, reached with a CDP key
scoped to that portfolio. Request/response shapes follow Coinbase's published
OpenAPI spec for the Advanced Trade REST API.

## Safety at connect()

connect() refuses to return (raises) unless the key is exactly what the design
assumes — "never guess with real money":
  * GET /key_permissions: the key is bound to the configured portfolio_uuid;
    it can view; it can trade (live mode); and it CANNOT transfer (a key that
    can move funds out of the account is refused outright).
  * GET /accounts: every account the key sees belongs to that portfolio.

## Positions and equity

Spot has no positions, only balances. A position is a non-zero base-asset
balance (available + hold) in the bot portfolio, reported as
"{BASE}-{QUOTE}" and always positive (long-only). Equity: available = the
quote currency's available balance (what a new BUY can spend); total = quote
(available + hold) + sum(base balance x mid).

## Orders

Every order is limit_limit_gtc (the executor prices a marketable limit; this
adapter never sends a market order), post_only false. base_size is floored to
the product's base_increment, the limit rounded to its price_increment toward
the touch. client_order_id is the executor's deterministic cv1 id; Coinbase
returns the existing order for a repeated id instead of creating a second one,
which is the venue half of the executor's idempotency.

A rejection is an HTTP 200 with success=false (or a 4xx) -> REJECTED with the
reason; a transport failure after retries RAISES, because the order may have
landed (the executor then resolves it by client_order_id at reconcile).

## Fills

Fills are POLLED from /orders/historical/fills (~2 s, cursored, overlapping
window, deduped) — the user websocket reports order-level cumulative state, not
per-execution ids, and the executor dedupes on venue_exec_id (ADR-001 trade-off).
Fills carry order_id but NOT client_order_id, so each unknown order is looked up
once (cached) to recover its client_order_id for attribution.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Callable, Dict, List, Optional

from execution import client_order_id as coid
from execution.adapter import ExecutionAdapter, FillCallback
from execution.coinbase_client import CoinbaseClient, CoinbaseHTTPError
from execution.pricing import is_buy, quantize_limit_price
from execution.types import (
    AccountEquity,
    Fill,
    MinOrderSize,
    OrderIntent,
    OrderRequest,
    OrderResult,
    OrderStatus,
    ReconcileResult,
    Touch,
    TradeSide,
    VenueOrder,
    VenuePosition,
    validate_venue_positions,
)

log = logging.getLogger("executor.coinbase")

# Coinbase order status -> ours. PARTIALLY_FILLED is derived from filled_size.
_WORKING = {"PENDING", "OPEN", "QUEUED", "EDIT_QUEUED", "UNKNOWN_ORDER_STATUS"}
_CANCELLED = {"CANCELLED", "EXPIRED"}
_TERMINAL = {"FILLED", "CANCELLED", "EXPIRED", "FAILED"}

_FILL_OVERLAP_S = 300          # re-read this much behind the newest fill seen
_SEEN_FILLS_CAP = 20_000
_CANCEL_SETTLE_POLLS = 10      # wait up to ~2 s for a cancel to become terminal


class CoinbaseConnectError(RuntimeError):
    """connect() refused: the key or account is not what the design assumes."""


@dataclass(frozen=True)
class Product:
    product_id: str
    base_increment: Decimal
    price_increment: Decimal
    base_min_size: Optional[Decimal]
    quote_min_size: Optional[Decimal]
    tradable: bool          # online, not trading_disabled, not cancel_only


def _dec(v) -> Optional[Decimal]:
    if v in (None, ""):
        return None
    return Decimal(str(v))


def _fmt(d: Decimal) -> str:
    """Plain decimal string (never scientific notation)."""
    s = format(d, "f")
    return s.rstrip("0").rstrip(".") if "." in s else s


def _iso_to_ms(s: str) -> int:
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)


def _ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class CoinbaseAdapter(ExecutionAdapter):
    venue = "coinbase"
    supported_classes = {"crypto"}
    supports_short = False

    def __init__(
        self,
        client: CoinbaseClient,
        *,
        portfolio_uuid: str,
        quote_currency: str = "USD",
        require_trade: bool = True,
        fill_poll_s: float = 2.0,
        recent_fills_lookback_s: int = 86_400,
        now_ms: Optional[Callable[[], int]] = None,
    ):
        """
        require_trade: refuse a key without trade permission (live). False for
            read-only use (OFF mode, verification).
        """
        self.client = client
        self.portfolio_uuid = portfolio_uuid
        self.quote = quote_currency
        self.require_trade = require_trade
        self.fill_poll_s = fill_poll_s
        self.recent_fills_lookback_s = recent_fills_lookback_s
        self._now_ms = now_ms or (lambda: int(time.time() * 1000))
        self._products: Dict[str, Product] = {}
        self._coid_to_order: Dict[str, str] = {}
        self._order_to_coid: Dict[str, Optional[str]] = {}
        self._seen_fills: "OrderedDict[str, None]" = OrderedDict()
        self._fill_task: Optional[asyncio.Task] = None
        self._connected = False
        self.permissions: dict = {}

    # ------------------------------------------------------------------
    # Session
    # ------------------------------------------------------------------
    async def connect(self) -> None:
        if not self.portfolio_uuid:
            raise CoinbaseConnectError(
                "COINBASE_PORTFOLIO_UUID is not set: the bot must run in a dedicated "
                "portfolio with a key scoped to it")
        perms = await self.client.get("/key_permissions")
        self.permissions = perms
        problems = []
        if not perms.get("can_view"):
            problems.append("key cannot view")
        if perms.get("portfolio_uuid") != self.portfolio_uuid:
            problems.append(f"key is bound to portfolio {perms.get('portfolio_uuid')!r}, "
                            f"not the configured {self.portfolio_uuid!r}")
        if perms.get("can_transfer"):
            problems.append("key can TRANSFER funds — create a view+trade key without "
                            "transfer permission")
        if self.require_trade and not perms.get("can_trade"):
            problems.append("key cannot trade")
        if problems:
            raise CoinbaseConnectError("refusing Coinbase key: " + "; ".join(problems))

        foreign = {a.get("retail_portfolio_id") for a in await self._accounts()} - {self.portfolio_uuid}
        if foreign:
            raise CoinbaseConnectError(
                f"key sees accounts outside the bot portfolio ({sorted(map(str, foreign))}); "
                "refusing — scope the key to the bot portfolio only")
        self._connected = True
        log.info("coinbase connected: portfolio=%s can_trade=%s quote=%s",
                 self.portfolio_uuid, perms.get("can_trade"), self.quote)

    async def _accounts(self) -> List[dict]:
        out, cursor = [], None
        while True:
            page = await self.client.get("/accounts", limit=250, cursor=cursor)
            out.extend(page.get("accounts", []))
            if not page.get("has_next"):
                return out
            cursor = page.get("cursor")

    # ------------------------------------------------------------------
    # Products / constraints
    # ------------------------------------------------------------------
    async def product(self, symbol: str) -> Product:
        p = self._products.get(symbol)
        if p is None:
            d = await self.client.get(f"/products/{symbol}")
            p = Product(
                product_id=symbol,
                base_increment=_dec(d.get("base_increment")),
                price_increment=_dec(d.get("price_increment")) or _dec(d.get("quote_increment")),
                base_min_size=_dec(d.get("base_min_size")),
                quote_min_size=_dec(d.get("quote_min_size")),
                tradable=(d.get("status") == "online" and not d.get("trading_disabled")
                          and not d.get("cancel_only") and not d.get("is_disabled")),
            )
            self._products[symbol] = p
        return p

    async def get_step_size(self, symbol: str, asset_class: str) -> Optional[Decimal]:
        return (await self.product(symbol)).base_increment

    async def get_price_increment(self, symbol: str, asset_class: str) -> Optional[Decimal]:
        return (await self.product(symbol)).price_increment

    async def get_min_order_size(self, symbol: str, asset_class: str) -> Optional[MinOrderSize]:
        p = await self.product(symbol)
        return MinOrderSize(base=p.base_min_size, quote=p.quote_min_size)

    def to_venue_symbol(self, symbol: str) -> str:
        return symbol

    # ------------------------------------------------------------------
    # Market / account reads
    # ------------------------------------------------------------------
    async def _touches(self, symbols: List[str]) -> Dict[str, Touch]:
        if not symbols:
            return {}
        d = await self.client.get("/best_bid_ask", product_ids=symbols)
        out = {}
        for book in d.get("pricebooks", []):
            bids, asks = book.get("bids") or [], book.get("asks") or []
            if bids and asks:
                out[book["product_id"]] = Touch(Decimal(str(bids[0]["price"])),
                                                Decimal(str(asks[0]["price"])))
        return out

    async def get_touch(self, symbol: str, asset_class: str) -> Optional[Touch]:
        return (await self._touches([symbol])).get(symbol)

    def _balances(self, accounts: List[dict]) -> Dict[str, Dict[str, Decimal]]:
        out: Dict[str, Dict[str, Decimal]] = {}
        for a in accounts:
            cur = a.get("currency")
            avail = _dec((a.get("available_balance") or {}).get("value")) or Decimal(0)
            hold = _dec((a.get("hold") or {}).get("value")) or Decimal(0)
            b = out.setdefault(cur, {"available": Decimal(0), "hold": Decimal(0)})
            b["available"] += avail
            b["hold"] += hold
        return out

    async def get_positions(self) -> List[VenuePosition]:
        bal = self._balances(await self._accounts())
        positions = [
            VenuePosition(symbol=f"{cur}-{self.quote}", qty=b["available"] + b["hold"])
            for cur, b in bal.items()
            if cur != self.quote and b["available"] + b["hold"] > 0
        ]
        return validate_venue_positions(positions, venue=self.venue)

    async def get_account_equity(self) -> AccountEquity:
        bal = self._balances(await self._accounts())
        q = bal.get(self.quote, {"available": Decimal(0), "hold": Decimal(0)})
        held = {f"{cur}-{self.quote}": b["available"] + b["hold"]
                for cur, b in bal.items()
                if cur != self.quote and b["available"] + b["hold"] > 0}
        touches = await self._touches(list(held))
        total = q["available"] + q["hold"]
        for sym, qty in held.items():
            t = touches.get(sym)
            if t is None:
                # Unpriceable holding: say so; sizing uses `available`, not total.
                log.warning("equity: no touch for held %s qty=%s — excluded from total", sym, qty)
                continue
            total += qty * (t.bid + t.ask) / 2
        return AccountEquity(total=total, currency=self.quote, available=q["available"])

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------
    def _reject(self, cid: str, reason: str) -> OrderResult:
        log.warning("order %s rejected: %s", cid, reason)
        return OrderResult(client_order_id=cid, status=OrderStatus.REJECTED,
                           reject_reason=reason)

    async def submit_order(self, order: OrderRequest) -> OrderResult:
        cid = order.client_order_id
        buy = is_buy(order.side, order.intent)
        if order.side == TradeSide.SHORT:
            return self._reject(cid, "short positions are not supported on spot")
        p = await self.product(order.symbol)
        if not p.tradable:
            return self._reject(cid, f"{order.symbol} is not tradable right now")
        size = order.qty
        if p.base_increment:
            size = (size / p.base_increment).to_integral_value(rounding=ROUND_DOWN) * p.base_increment
        if size <= 0:
            return self._reject(cid, f"size {order.qty} rounds to 0 at base_increment {p.base_increment}")
        limit = quantize_limit_price(order.limit_price, order.side, order.intent, p.price_increment)
        body = {
            "client_order_id": cid,
            "product_id": order.symbol,
            "side": "BUY" if buy else "SELL",
            "order_configuration": {"limit_limit_gtc": {
                "base_size": _fmt(size), "limit_price": _fmt(limit), "post_only": False,
            }},
        }
        try:
            resp = await self.client.post("/orders", body)
        except CoinbaseHTTPError as e:
            if 400 <= e.status < 500:
                return self._reject(cid, f"HTTP {e.status}: {e.body[:200]}")
            raise   # unknown outcome: the order may exist; reconcile resolves it
        if not resp.get("success"):
            err = resp.get("error_response") or {}
            reason = " / ".join(str(x) for x in (
                err.get("error"), err.get("message"), err.get("error_details"),
                err.get("preview_failure_reason"), err.get("new_order_failure_reason"))
                if x and x != "UNKNOWN_FAILURE_REASON")
            return self._reject(cid, reason or "rejected (no reason given)")
        oid = (resp.get("success_response") or {}).get("order_id")
        if oid:
            self._coid_to_order[cid] = oid
            self._order_to_coid[oid] = cid
        return OrderResult(client_order_id=cid, status=OrderStatus.WORKING,
                           venue_order_id=oid)

    async def _get_order(self, order_id: str) -> dict:
        return (await self.client.get(f"/orders/historical/{order_id}")).get("order", {})

    def _result_from_order(self, cid: str, o: dict) -> OrderResult:
        filled = _dec(o.get("filled_size")) or Decimal(0)
        st = o.get("status", "UNKNOWN_ORDER_STATUS")
        if st == "FILLED":
            status = OrderStatus.FILLED
        elif st in _CANCELLED:
            status = OrderStatus.CANCELLED
        elif st == "FAILED":
            status = OrderStatus.REJECTED
        else:
            status = OrderStatus.PARTIALLY_FILLED if filled > 0 else OrderStatus.WORKING
        return OrderResult(client_order_id=cid, status=status,
                           venue_order_id=o.get("order_id"), filled_qty=filled,
                           reject_reason=o.get("reject_message") or o.get("cancel_message") or None)

    async def _order_id_for(self, cid: str) -> Optional[str]:
        oid = self._coid_to_order.get(cid)
        if oid:
            return oid
        # Open orders first, then the most recent page of all orders (a just-
        # terminal one); never the whole history.
        for o in (await self._orders(order_status=["OPEN"])
                  + await self._orders(max_pages=1)):
            if o.get("client_order_id") == cid:
                self._coid_to_order[cid] = o["order_id"]
                self._order_to_coid[o["order_id"]] = cid
                return o["order_id"]
        return None

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        oid = await self._order_id_for(client_order_id)
        if oid is None:
            # Never landed (or too old to find): nothing to cancel.
            return OrderResult(client_order_id=client_order_id, status=OrderStatus.CANCELLED)
        o = await self._get_order(oid)
        if o.get("status") not in _TERMINAL:
            await self.client.post("/orders/batch_cancel", {"order_ids": [oid]})
            # A cancel is queued, not instant: wait for a terminal status so the
            # executor never issues a replacement while this order can still fill.
            for _ in range(_CANCEL_SETTLE_POLLS):
                o = await self._get_order(oid)
                if o.get("status") in _TERMINAL:
                    break
                await asyncio.sleep(0.2)
            else:
                log.error("cancel of %s (%s) not terminal after waiting: %s",
                          client_order_id, oid, o.get("status"))
        return self._result_from_order(client_order_id, o)

    async def amend_order(self, client_order_id: str, new_limit_price: Decimal) -> OrderResult:
        oid = await self._order_id_for(client_order_id)
        if oid is None:
            return OrderResult(client_order_id=client_order_id, status=OrderStatus.CANCELLED)
        o = await self._get_order(oid)
        if o.get("status") in _TERMINAL:
            return self._result_from_order(client_order_id, o)
        cfg = (o.get("order_configuration") or {}).get("limit_limit_gtc") or {}
        p = await self.product(o.get("product_id"))
        side = TradeSide.LONG
        intent = OrderIntent.ENTRY if o.get("side") == "BUY" else OrderIntent.EXIT
        price = quantize_limit_price(new_limit_price, side, intent, p.price_increment)
        resp = await self.client.post("/orders/edit", {
            "order_id": oid, "price": _fmt(price), "size": cfg.get("base_size")})
        if not resp.get("success"):
            return self._reject(client_order_id, f"edit failed: {resp.get('errors')}")
        return self._result_from_order(client_order_id, await self._get_order(oid))

    # ------------------------------------------------------------------
    # Fills
    # ------------------------------------------------------------------
    async def _client_id_for_order(self, order_id: str) -> Optional[str]:
        if order_id not in self._order_to_coid:
            try:
                cid = (await self._get_order(order_id)).get("client_order_id") or None
            except CoinbaseHTTPError:
                log.exception("order lookup failed for fill attribution (%s)", order_id)
                return None   # not cached: retried on the next poll
            self._order_to_coid[order_id] = cid
            if cid:
                self._coid_to_order.setdefault(cid, order_id)
        return self._order_to_coid[order_id]

    async def _fill_from(self, f: dict) -> Fill:
        oid = f.get("order_id")
        cid = await self._client_id_for_order(oid) if oid else None
        price = Decimal(str(f["price"]))
        size = Decimal(str(f["size"]))
        if f.get("size_in_quote"):
            size = size / price
        if cid and coid.is_ours(cid):
            intent = coid.parse(cid).intent
        else:
            intent = OrderIntent.ENTRY if f.get("side") == "BUY" else OrderIntent.EXIT
        return Fill(
            venue_exec_id=str(f.get("entry_id") or f.get("trade_id")),
            qty=size, price=price, intent=intent,
            filled_at_ms=_iso_to_ms(f["trade_time"]),
            fee=_dec(f.get("commission")) or Decimal(0),
            fee_currency=self.quote,
            venue_order_id=oid, client_order_id=cid,
        )

    async def _fills_since(self, start_ms: int) -> List[dict]:
        out, cursor = [], None
        while True:
            page = await self.client.get(
                "/orders/historical/fills", start_sequence_timestamp=_ms_to_iso(start_ms),
                limit=250, cursor=cursor)
            out.extend(page.get("fills", []))
            cursor = page.get("cursor")
            if not cursor or not page.get("fills"):
                return out

    async def poll_fills_once(self, since_ms: int, callback: FillCallback) -> int:
        """One poll: deliver every fill not yet seen since since_ms. Returns the
        newest trade time seen (ms), for the next cursor. Split out of the loop
        so it is testable without sleeping."""
        newest = since_ms
        for f in sorted(await self._fills_since(since_ms), key=lambda x: x.get("trade_time", "")):
            fid = str(f.get("entry_id") or f.get("trade_id"))
            newest = max(newest, _iso_to_ms(f["trade_time"]))
            if fid in self._seen_fills:
                continue
            fill = await self._fill_from(f)
            try:
                await callback(fill)
            except Exception:
                log.exception("fill callback failed for %s; will redeliver", fid)
                continue   # not marked seen: redelivered next poll
            self._seen_fills[fid] = None
            while len(self._seen_fills) > _SEEN_FILLS_CAP:
                self._seen_fills.popitem(last=False)
        return newest

    async def start_fill_stream(self, callback: FillCallback) -> None:
        async def loop():
            cursor = self._now_ms() - _FILL_OVERLAP_S * 1000
            while True:
                try:
                    newest = await self.poll_fills_once(cursor, callback)
                    cursor = max(cursor, newest - _FILL_OVERLAP_S * 1000)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("fill poll failed; retrying")
                await asyncio.sleep(self.fill_poll_s)
        self._fill_task = asyncio.create_task(loop(), name="coinbase-fills")

    # ------------------------------------------------------------------
    # Reconcile
    # ------------------------------------------------------------------
    async def _orders(self, max_pages: Optional[int] = None, **filters) -> List[dict]:
        out, cursor, pages = [], None, 0
        while True:
            page = await self.client.get("/orders/historical/batch", limit=250,
                                         cursor=cursor, **filters)
            out.extend(page.get("orders", []))
            pages += 1
            cursor = page.get("cursor")
            if not page.get("has_next") or not cursor or (max_pages and pages >= max_pages):
                return out

    async def reconcile(self) -> ReconcileResult:
        positions = await self.get_positions()
        open_orders = []
        for o in await self._orders(order_status=["OPEN"]):
            cfg = (o.get("order_configuration") or {}).get("limit_limit_gtc") or {}
            r = self._result_from_order(o.get("client_order_id") or "", o)
            if o.get("client_order_id"):
                self._coid_to_order[o["client_order_id"]] = o["order_id"]
                self._order_to_coid[o["order_id"]] = o["client_order_id"]
            open_orders.append(VenueOrder(
                client_order_id=o.get("client_order_id") or None,
                venue_order_id=o["order_id"], symbol=o.get("product_id"),
                side=TradeSide.LONG,   # spot is long-only: every order serves a long
                qty=_dec(cfg.get("base_size")) or Decimal(0),
                filled_qty=r.filled_qty, status=r.status,
            ))
        since = self._now_ms() - self.recent_fills_lookback_s * 1000
        recent = [await self._fill_from(f) for f in await self._fills_since(since)]
        return ReconcileResult(positions=positions, open_orders=open_orders,
                               recent_fills=recent)

    async def aclose(self) -> None:
        if self._fill_task is not None:
            self._fill_task.cancel()
        await self.client.aclose()
