"""
execution/gated_adapter.py — the master on/off wrapper for any ExecutionAdapter.

Wraps a concrete adapter (CoinbaseAdapter, PaperCoinbaseAdapter) and gates
ORDER-PLACING calls on the master trade-enable state (execution/trade_enable.py).
When trading is disabled, submit/cancel/amend become SAFE NO-OPS — they return
a synthetic terminal result and touch no venue — while read/observe calls
(reconcile, get_positions) and the session lifecycle (connect,
start_fill_stream) pass straight through.

## Why a wrapper, not `if enabled` inside CoinbaseAdapter

The concrete adapter is the dangerous code — it talks to a real brokerage with
real money. Scattering enable-checks through it means the safety switch is
entangled with the venue logic, and a missed check is a live order that should
not have gone out. Isolating the gate in one wrapper means:

  - the Coinbase adapter stays a pure venue translator, with no concept of "enabled";
  - there is exactly ONE place an order can be suppressed, auditable at a glance;
  - the SAME gate protects the paper adapter and any future one for free;
  - the gate composes with kill_switch: the executor checks kill_switch, this
    checks trade_enabled, and BOTH must allow an order. This wrapper owns only
    the trade_enabled half — it does NOT know about kill_switch, keeping the two
    switches independent (see migration 010's precedence note).

## Reads pass through ON PURPOSE

With trading off you still want to OBSERVE the venue — reconcile and
get_positions are how you watch what's actually held, run the boot reconcile,
and let the reconciliation watchdog work, all without placing an order. Gating
those would blind you exactly when you're running disabled to watch. Only the
three mutating calls are gated.

## The no-op result

A gated submit_order returns OrderStatus.REJECTED with a reject_reason marking
it as gated (not a venue rejection). REJECTED is a terminal status the executor
already handles (trade -> REJECTED), so a gated order flows through the existing
lifecycle as "did not execute" without special-casing. The synthetic result
carries the original client_order_id so the executor can still record the
attempt against the right trade. cancel/amend of a gated (never-placed) order
likewise return a benign terminal result — there is nothing at the venue to act
on.

IMPORTANT: because a gated submit returns REJECTED, the executor must treat a
gated-reject the same as any reject (record it, do not retry-as-if-transient).
The reject_reason prefix GATED_ lets the executor distinguish a gate-suppression
from a real venue rejection if it wants to (e.g. to log differently), but the
lifecycle handling is identical.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import List, Optional

from execution.adapter import ExecutionAdapter, FillCallback
from execution.read_only_adapter import ReadOnlyAdapter
from execution.trade_enable import TradeEnableState
from execution.types import (
    AccountEquity,
    MinOrderSize,
    OrderRequest,
    OrderResult,
    OrderStatus,
    ReconcileResult,
    Touch,
    VenuePosition,
)

log = logging.getLogger("executor.gated_adapter")

# reject_reason prefix marking a suppression by the master switch, so the
# executor / forensics can tell a gate no-op from a genuine venue rejection.
GATED_PREFIX = "GATED_"


class GatedAdapter(ExecutionAdapter):
    """Delegates to an inner ExecutionAdapter, suppressing order placement when
    the master trade-enable state is off.

    Presents the full ExecutionAdapter interface, so the executor uses it
    exactly as it would a bare adapter — the gating is transparent except that
    orders placed while disabled come back REJECTED with a GATED_ reason.
    """

    def __init__(self, inner: ExecutionAdapter, enable_state: TradeEnableState):
        self._inner = inner
        self._enable = enable_state
        # Mirror the inner adapter's identity so callers routing by venue /
        # asset_class see no difference.
        self.venue = inner.venue
        self.supported_classes = inner.supported_classes
        self.supports_short = inner.supports_short

    @property
    def inner(self) -> ExecutionAdapter:
        return self._inner

    def block_all_writes(self) -> None:
        """From now on every write comes back REJECTED (READ_ONLY), whatever
        the enable state; reads still pass through. For a trade-mode change
        that switches venue, until the process restarts against the new one.
        Idempotent."""
        if not isinstance(self._inner, ReadOnlyAdapter):
            self._inner = ReadOnlyAdapter(self._inner)

    # ---- session lifecycle: pass through ----
    # connect/start_fill_stream are NOT gated: we still want a live session and
    # fill stream while disabled (to observe, reconcile, and be ready to enable).

    async def connect(self) -> None:
        await self._inner.connect()

    async def start_fill_stream(self, callback: FillCallback) -> None:
        await self._inner.start_fill_stream(callback)

    # ---- reconciliation / reads: pass through ----
    # Observing the venue must work with trading off.

    async def reconcile(self) -> ReconcileResult:
        return await self._inner.reconcile()

    async def get_positions(self) -> List[VenuePosition]:
        return await self._inner.get_positions()

    # Market/account reads: pass through (sizing and pricing need them even
    # while trading is off, and they place nothing).

    async def get_touch(self, symbol: str, asset_class: str) -> Optional[Touch]:
        return await self._inner.get_touch(symbol, asset_class)

    async def get_account_equity(self) -> AccountEquity:
        return await self._inner.get_account_equity()

    async def get_step_size(self, symbol: str, asset_class: str) -> Optional[Decimal]:
        return await self._inner.get_step_size(symbol, asset_class)

    async def get_price_increment(self, symbol: str, asset_class: str) -> Optional[Decimal]:
        return await self._inner.get_price_increment(symbol, asset_class)

    async def get_min_order_size(self, symbol: str, asset_class: str) -> Optional[MinOrderSize]:
        return await self._inner.get_min_order_size(symbol, asset_class)

    # ---- order placement: GATED ----

    async def submit_order(self, order: OrderRequest) -> OrderResult:
        """Place an order only if trading is enabled; otherwise a safe no-op
        returning a GATED reject. Nothing reaches the venue when disabled."""
        if not self._enable.is_enabled():
            log.info(
                "trade disabled — suppressing submit_order %s (%s %s qty=%s)",
                order.client_order_id, order.symbol, order.side.value, order.qty,
            )
            return OrderResult(
                client_order_id=order.client_order_id,
                status=OrderStatus.REJECTED,
                reject_reason=f"{GATED_PREFIX}trade_disabled",
            )
        return await self._inner.submit_order(order)

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel is gated too: if trading is off, there is nothing we placed to
        cancel (a gated submit never reached the venue), so return a benign
        terminal result rather than calling the venue. If trading is on, pass
        through — a real working order may need cancelling."""
        if not self._enable.is_enabled():
            log.info("trade disabled — no-op cancel_order %s", client_order_id)
            return OrderResult(
                client_order_id=client_order_id,
                status=OrderStatus.CANCELLED,
                reject_reason=f"{GATED_PREFIX}trade_disabled",
            )
        return await self._inner.cancel_order(client_order_id)

    async def amend_order(
        self, client_order_id: str, new_limit_price: Decimal,
    ) -> OrderResult:
        """Amend is gated: nothing to amend at the venue when disabled."""
        if not self._enable.is_enabled():
            log.info("trade disabled — no-op amend_order %s", client_order_id)
            return OrderResult(
                client_order_id=client_order_id,
                status=OrderStatus.CANCELLED,
                reject_reason=f"{GATED_PREFIX}trade_disabled",
            )
        return await self._inner.amend_order(client_order_id, new_limit_price)

    # ---- symbol translation: pass through (pure, no venue call) ----

    def to_venue_symbol(self, symbol: str) -> str:
        return self._inner.to_venue_symbol(symbol)