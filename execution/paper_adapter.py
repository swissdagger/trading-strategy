"""
execution/paper_adapter.py — paper trading against Coinbase's REAL market.

ADR-001 Phase 5 (Option P2). Coinbase has no paper venue (its sandbox has no
matching engine), so paper trading happens in-process: every READ goes to the
real Coinbase adapter (the live touch, product rules, the account's taker fee
tier); every WRITE is simulated against a ledger in the database. Everything
above the adapter — sizing, pricing, reconcile, escalation, kill switch, the
shadow book — runs exactly as it will live.

## Fill model (deliberately pessimistic; ADR-001 debt D3)

  * An order that CROSSES the current book (a BUY limit >= ask, a SELL limit
    <= bid) fills IN FULL, immediately, at its LIMIT price — the worst price
    the cap allows, not the touch. Paper P&L is therefore biased down.
  * An order that does not cross RESTS and is re-checked against the live
    touch on every fill-poll tick; it fills (at its limit) once the book
    reaches it. So an exit whose cap is too tight stays unfilled and the
    executor's escalation ladder runs as it would live.
  * The real taker fee (GET /transaction_summary) is charged on every fill, in
    the quote currency. No partials, no latency, no queue position, no impact.

## Venue rules enforced as Coinbase would

Long-only (SHORT refused), product must be tradable, size floored to
base_increment, price rounded to price_increment toward the touch, base and
quote minimums, and funds: a BUY needs cash for size x limit + fee net of
cash reserved by resting BUYs; a SELL needs base not already reserved by
resting SELLs. A repeated client_order_id returns the existing order.

## State

Balances, orders and fills persist through PaperLedgerHooks (migration 022's
paper_balances / paper_orders / paper_fills), so a restart resumes the same
paper account and the executor's boot reconcile runs against it for real. The
ledger is seeded with starting cash on the first connect. Ledger writes for a
fill (fill row + order + balances) are one atomic call.

The market adapter is expected to be the real CoinbaseAdapter wrapped in
ReadOnlyAdapter: this class never calls a market write, and the wrapper makes
sure no write could reach the venue anyway.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field, replace
from decimal import ROUND_DOWN, Decimal
from typing import Awaitable, Callable, Dict, List, Optional

from execution import client_order_id as coid
from execution.adapter import ExecutionAdapter, FillCallback
from execution.pricing import is_buy, quantize_limit_price, validate_touch
from execution.types import (
    AccountEquity, Fill, MinOrderSize, OrderIntent, OrderRequest, OrderResult,
    OrderStatus, ReconcileResult, Touch, TradeSide, VenueOrder, VenuePosition,
    validate_venue_positions,
)

log = logging.getLogger("executor.paper")

_BPS = Decimal(10_000)
_FEE_REFRESH_MS = 6 * 3600 * 1000


@dataclass
class PaperOrder:
    client_order_id: str
    venue_order_id: str
    symbol: str
    side: str                 # 'BUY' | 'SELL'
    intent: str               # 'ENTRY' | 'EXIT'
    qty: Decimal
    limit_price: Decimal
    status: str               # 'OPEN' | 'FILLED' | 'CANCELLED'
    filled_qty: Decimal = Decimal(0)
    created_ms: int = 0


@dataclass
class PaperState:
    balances: Dict[str, Decimal] = field(default_factory=dict)
    orders: Dict[str, PaperOrder] = field(default_factory=dict)   # by client_order_id
    fills: List[Fill] = field(default_factory=list)               # recent, for reconcile


@dataclass
class PaperLedgerHooks:
    # Full state at connect (fills: the recent window used by reconcile).
    load: Callable[[int], Awaitable[PaperState]]          # (fills_since_ms) -> state
    # Seed starting balances; called only when the ledger has none.
    seed: Callable[[Dict[str, Decimal]], Awaitable[None]]
    save_order: Callable[[PaperOrder], Awaitable[None]]
    # Atomically: insert the fill, save the order, add the balance deltas.
    apply_fill: Callable[[Fill, str, PaperOrder, Dict[str, Decimal]], Awaitable[None]]


class MemPaperLedger:
    """In-memory PaperLedgerHooks (tests, and a DB-less dry run)."""
    def __init__(self):
        self.state = PaperState()

    def hooks(self) -> PaperLedgerHooks:
        async def load(since_ms):
            s = self.state
            return PaperState(dict(s.balances), {k: replace(v) for k, v in s.orders.items()},
                              [f for f in s.fills if f.filled_at_ms >= since_ms])
        async def seed(bal):
            if not self.state.balances:
                self.state.balances.update(bal)
        async def save_order(o):
            self.state.orders[o.client_order_id] = replace(o)
        async def apply_fill(fill, symbol, o, deltas):
            self.state.fills.append(fill)
            self.state.orders[o.client_order_id] = replace(o)
            for cur, d in deltas.items():
                self.state.balances[cur] = self.state.balances.get(cur, Decimal(0)) + d
        return PaperLedgerHooks(load=load, seed=seed, save_order=save_order,
                                apply_fill=apply_fill)


def _base_of(symbol: str) -> str:
    return symbol.split("-", 1)[0]


class PaperCoinbaseAdapter(ExecutionAdapter):
    venue = "coinbase"
    supported_classes = {"crypto"}
    supports_short = False

    def __init__(
        self,
        market: ExecutionAdapter,
        ledger: PaperLedgerHooks,
        *,
        quote_currency: str = "USD",
        starting_cash: Decimal = Decimal("10000"),
        fallback_fee_bps: Decimal = Decimal("120"),
        fill_poll_s: float = 2.0,
        recent_fills_lookback_s: int = 86_400,
        now_ms: Optional[Callable[[], int]] = None,
    ):
        self.market = market
        self.ledger = ledger
        self.quote = quote_currency
        self.starting_cash = Decimal(str(starting_cash))
        self.fallback_fee_bps = Decimal(str(fallback_fee_bps))
        self.fill_poll_s = fill_poll_s
        self.recent_fills_lookback_s = recent_fills_lookback_s
        self._now_ms = now_ms or (lambda: int(time.time() * 1000))
        self.state = PaperState()
        self._lock = asyncio.Lock()
        self._fee_bps: Optional[Decimal] = None
        self._fee_at_ms = 0
        self._fill_task: Optional[asyncio.Task] = None

    # The underlying real adapter (for product tradability / fee tier), if the
    # market is a ReadOnlyAdapter around one.
    @property
    def _real(self):
        return getattr(self.market, "inner", self.market)

    # ------------------------------------------------------------------
    # Session
    # ------------------------------------------------------------------
    async def connect(self) -> None:
        await self.market.connect()
        since = self._now_ms() - self.recent_fills_lookback_s * 1000
        self.state = await self.ledger.load(since)
        if not self.state.balances:
            await self.ledger.seed({self.quote: self.starting_cash})
            self.state.balances = {self.quote: self.starting_cash}
            log.warning("paper ledger seeded with %s %s", self.starting_cash, self.quote)
        log.info("paper account: %s; %d open order(s)",
                 {k: str(v) for k, v in self.state.balances.items() if v},
                 sum(1 for o in self.state.orders.values() if o.status == "OPEN"))

    # ------------------------------------------------------------------
    # Reads (real market)
    # ------------------------------------------------------------------
    async def get_touch(self, symbol: str, asset_class: str) -> Optional[Touch]:
        return await self.market.get_touch(symbol, asset_class)

    async def get_step_size(self, symbol: str, asset_class: str) -> Optional[Decimal]:
        return await self.market.get_step_size(symbol, asset_class)

    async def get_price_increment(self, symbol: str, asset_class: str) -> Optional[Decimal]:
        return await self.market.get_price_increment(symbol, asset_class)

    async def get_min_order_size(self, symbol: str, asset_class: str) -> Optional[MinOrderSize]:
        return await self.market.get_min_order_size(symbol, asset_class)

    def to_venue_symbol(self, symbol: str) -> str:
        return symbol

    async def get_taker_fee_bps(self) -> Decimal:
        """The account's real taker fee in bps, refreshed every 6 h; the
        configured fallback if the venue can't say."""
        now = self._now_ms()
        if self._fee_bps is None or now - self._fee_at_ms > _FEE_REFRESH_MS:
            fn = getattr(self._real, "get_taker_fee_bps", None)
            bps = None
            if fn is not None:
                try:
                    bps = await fn()
                except Exception:
                    log.exception("fee tier read failed; using fallback %s bps",
                                  self.fallback_fee_bps)
            self._fee_bps = bps if bps is not None else self.fallback_fee_bps
            self._fee_at_ms = now
        return self._fee_bps

    async def _tradable(self, symbol: str) -> bool:
        fn = getattr(self._real, "product", None)
        if fn is None:
            return True
        return (await fn(symbol)).tradable

    # ------------------------------------------------------------------
    # Paper account
    # ------------------------------------------------------------------
    def _bal(self, cur: str) -> Decimal:
        return self.state.balances.get(cur, Decimal(0))

    def _reserved_cash(self, fee_rate: Decimal) -> Decimal:
        return sum((o.qty * o.limit_price * (1 + fee_rate)
                    for o in self.state.orders.values()
                    if o.status == "OPEN" and o.side == "BUY"), Decimal(0))

    def _reserved_base(self, base: str) -> Decimal:
        return sum((o.qty for o in self.state.orders.values()
                    if o.status == "OPEN" and o.side == "SELL" and _base_of(o.symbol) == base),
                   Decimal(0))

    async def get_positions(self) -> List[VenuePosition]:
        return validate_venue_positions([
            VenuePosition(symbol=f"{cur}-{self.quote}", qty=q)
            for cur, q in self.state.balances.items() if cur != self.quote and q > 0
        ], venue="coinbase-paper")

    async def get_account_equity(self) -> AccountEquity:
        rate = await self.get_taker_fee_bps() / _BPS
        cash = self._bal(self.quote)
        total = cash
        for p in await self.get_positions():
            t = await self.market.get_touch(p.symbol, "crypto")
            if t is not None:
                total += p.qty * (t.bid + t.ask) / 2
        return AccountEquity(total=total, currency=self.quote,
                             available=cash - self._reserved_cash(rate))

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------
    def _result(self, o: PaperOrder, fills: Optional[List[Fill]] = None,
                reason: Optional[str] = None) -> OrderResult:
        status = {"OPEN": OrderStatus.WORKING, "FILLED": OrderStatus.FILLED,
                  "CANCELLED": OrderStatus.CANCELLED}[o.status]
        return OrderResult(client_order_id=o.client_order_id, status=status,
                           venue_order_id=o.venue_order_id, filled_qty=o.filled_qty,
                           fills=fills or [], reject_reason=reason)

    def _reject(self, cid: str, reason: str) -> OrderResult:
        log.warning("paper order %s rejected: %s", cid, reason)
        return OrderResult(client_order_id=cid, status=OrderStatus.REJECTED,
                           reject_reason=reason)

    @staticmethod
    def _crosses(o: PaperOrder, t: Optional[Touch]) -> bool:
        if t is None or validate_touch(t.bid, t.ask) is not None:
            return False
        return o.limit_price >= t.ask if o.side == "BUY" else o.limit_price <= t.bid

    async def _fill(self, o: PaperOrder) -> Fill:
        """Fill o in full at its limit price; persist atomically. Caller holds
        the lock and has checked funds at submit."""
        rate = await self.get_taker_fee_bps() / _BPS
        notional = o.qty * o.limit_price
        fee = notional * rate
        base = _base_of(o.symbol)
        if o.side == "BUY":
            deltas = {self.quote: -(notional + fee), base: o.qty}
        else:
            deltas = {base: -o.qty, self.quote: notional - fee}
        fill = Fill(
            venue_exec_id=f"paper-{uuid.uuid4()}", qty=o.qty, price=o.limit_price,
            intent=OrderIntent(o.intent), filled_at_ms=self._now_ms(), fee=fee,
            fee_currency=self.quote, venue_order_id=o.venue_order_id,
            client_order_id=o.client_order_id,
        )
        filled = replace(o, status="FILLED", filled_qty=o.qty)
        await self.ledger.apply_fill(fill, o.symbol, filled, deltas)
        # Mirror in memory only after the durable write succeeded.
        self.state.orders[o.client_order_id] = filled
        for cur, d in deltas.items():
            self.state.balances[cur] = self._bal(cur) + d
        self.state.fills.append(fill)
        log.info("paper FILL %s %s %s @ %s fee=%s %s", o.side, o.qty, o.symbol,
                 o.limit_price, fee.quantize(Decimal("0.0001")), self.quote)
        return fill

    async def submit_order(self, order: OrderRequest) -> OrderResult:
        cid = order.client_order_id
        async with self._lock:
            existing = self.state.orders.get(cid)
            if existing is not None:            # Coinbase returns the existing order
                return self._result(existing)
            if order.side == TradeSide.SHORT:
                return self._reject(cid, "short positions are not supported on spot")
            if not await self._tradable(order.symbol):
                return self._reject(cid, f"{order.symbol} is not tradable right now")
            step = await self.market.get_step_size(order.symbol, order.asset_class)
            size = order.qty
            if step:
                size = (size / step).to_integral_value(rounding=ROUND_DOWN) * step
            if size <= 0:
                return self._reject(cid, f"size {order.qty} rounds to 0 at base_increment {step}")
            limit = quantize_limit_price(
                order.limit_price, order.side, order.intent,
                await self.market.get_price_increment(order.symbol, order.asset_class))
            mins = await self.market.get_min_order_size(order.symbol, order.asset_class)
            if mins is not None:
                if mins.base is not None and size < mins.base:
                    return self._reject(cid, f"size {size} below base_min_size {mins.base}")
                if mins.quote is not None and size * limit < mins.quote:
                    return self._reject(cid, f"notional {size * limit} below quote_min_size {mins.quote}")
            buy = is_buy(order.side, order.intent)
            rate = await self.get_taker_fee_bps() / _BPS
            if buy:
                need = size * limit * (1 + rate)
                free = self._bal(self.quote) - self._reserved_cash(rate)
                if need > free:
                    return self._reject(cid, f"INSUFFICIENT_FUND: need {need} {self.quote}, "
                                             f"available {free}")
            else:
                base = _base_of(order.symbol)
                free = self._bal(base) - self._reserved_base(base)
                if size > free:
                    return self._reject(cid, f"INSUFFICIENT_FUND: need {size} {base}, "
                                             f"available {free}")
            o = PaperOrder(
                client_order_id=cid, venue_order_id=f"paper-{uuid.uuid4()}",
                symbol=order.symbol, side="BUY" if buy else "SELL",
                intent=order.intent.value, qty=size, limit_price=limit,
                status="OPEN", created_ms=self._now_ms(),
            )
            await self.ledger.save_order(o)
            self.state.orders[cid] = o
            touch = await self.market.get_touch(order.symbol, order.asset_class)
            if self._crosses(o, touch):
                fill = await self._fill(o)
                return self._result(self.state.orders[cid], [fill])
            log.info("paper order %s rests (%s %s @ %s; touch %s)", cid, o.side,
                     o.symbol, o.limit_price, touch)
            return self._result(o)

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        async with self._lock:
            o = self.state.orders.get(client_order_id)
            if o is None:
                return OrderResult(client_order_id=client_order_id,
                                   status=OrderStatus.CANCELLED)
            if o.status == "OPEN":
                o = replace(o, status="CANCELLED")
                await self.ledger.save_order(o)
                self.state.orders[client_order_id] = o
            return self._result(o)

    async def amend_order(self, client_order_id: str, new_limit_price: Decimal) -> OrderResult:
        async with self._lock:
            o = self.state.orders.get(client_order_id)
            if o is None:
                return OrderResult(client_order_id=client_order_id,
                                   status=OrderStatus.CANCELLED)
            if o.status != "OPEN":
                return self._result(o)
            intent = OrderIntent(o.intent)
            price = quantize_limit_price(new_limit_price, TradeSide.LONG, intent,
                                         await self.market.get_price_increment(o.symbol, "crypto"))
            o = replace(o, limit_price=price)
            await self.ledger.save_order(o)
            self.state.orders[client_order_id] = o
            if self._crosses(o, await self.market.get_touch(o.symbol, "crypto")):
                fill = await self._fill(o)   # before reading the order's new state
                return self._result(self.state.orders[client_order_id], [fill])
            return self._result(o)

    # ------------------------------------------------------------------
    # Resting orders -> async fills
    # ------------------------------------------------------------------
    async def check_resting_once(self, callback: FillCallback) -> int:
        """Fill every resting order the live book now reaches; deliver each
        fill to callback. Returns how many filled."""
        n = 0
        async with self._lock:
            resting = [o for o in self.state.orders.values() if o.status == "OPEN"]
            fills = []
            for o in resting:
                if self._crosses(o, await self.market.get_touch(o.symbol, "crypto")):
                    fills.append(await self._fill(o))
        for f in fills:
            try:
                await callback(f)
                n += 1
            except Exception:
                log.exception("paper fill callback failed for %s; the boot reconcile "
                              "re-reads recent fills", f.venue_exec_id)
        return n

    async def start_fill_stream(self, callback: FillCallback) -> None:
        async def loop():
            while True:
                try:
                    await self.check_resting_once(callback)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("paper resting-order check failed")
                await asyncio.sleep(self.fill_poll_s)
        self._fill_task = asyncio.create_task(loop(), name="paper-fills")

    # ------------------------------------------------------------------
    # Reconcile (the executor's real boot path, against the paper ledger)
    # ------------------------------------------------------------------
    async def reconcile(self) -> ReconcileResult:
        open_orders = [
            VenueOrder(client_order_id=o.client_order_id, venue_order_id=o.venue_order_id,
                       symbol=o.symbol, side=TradeSide.LONG, qty=o.qty,
                       filled_qty=o.filled_qty, status=OrderStatus.WORKING)
            for o in self.state.orders.values() if o.status == "OPEN"
        ]
        since = self._now_ms() - self.recent_fills_lookback_s * 1000
        return ReconcileResult(positions=await self.get_positions(), open_orders=open_orders,
                               recent_fills=[f for f in self.state.fills if f.filled_at_ms >= since])

    async def aclose(self) -> None:
        if self._fill_task is not None:
            self._fill_task.cancel()
        close = getattr(self._real, "aclose", None)
        if close is not None:
            await close()
