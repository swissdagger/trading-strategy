"""
execution/read_only_adapter.py — wraps a real adapter and refuses every write.

Used around the real Coinbase adapter wherever only its market data may be
used (ADR-001 Phase 4): as the paper adapter's market source in OFF and PAPER
mode, and by GatedAdapter.block_all_writes when a mode change switches venue.
Reads, reconcile and fills pass through; submit/cancel/amend never reach the
venue — regardless of what the GatedAdapter above it decides. Belt and braces:
OFF and PAPER must never be able to place a real order, even through a bug in
the mode gate.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import List, Optional

from execution.adapter import ExecutionAdapter, FillCallback
from execution.types import (
    AccountEquity, MinOrderSize, OrderRequest, OrderResult, OrderStatus,
    ReconcileResult, Touch, VenuePosition,
)

log = logging.getLogger("executor.read_only")

READ_ONLY_REASON = "READ_ONLY: adapter is in read-only mode"


class ReadOnlyAdapter(ExecutionAdapter):
    def __init__(self, inner: ExecutionAdapter):
        self._inner = inner
        self.venue = inner.venue
        self.supported_classes = inner.supported_classes
        self.supports_short = inner.supports_short

    @property
    def inner(self) -> ExecutionAdapter:
        return self._inner

    async def connect(self) -> None:
        await self._inner.connect()

    async def start_fill_stream(self, callback: FillCallback) -> None:
        await self._inner.start_fill_stream(callback)

    async def reconcile(self) -> ReconcileResult:
        return await self._inner.reconcile()

    async def get_positions(self) -> List[VenuePosition]:
        return await self._inner.get_positions()

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

    def to_venue_symbol(self, symbol: str) -> str:
        return self._inner.to_venue_symbol(symbol)

    async def submit_order(self, order: OrderRequest) -> OrderResult:
        log.warning("read-only: refusing submit_order %s (%s %s qty=%s)",
                    order.client_order_id, order.symbol, order.side.value, order.qty)
        return OrderResult(client_order_id=order.client_order_id,
                           status=OrderStatus.REJECTED, reject_reason=READ_ONLY_REASON)

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        log.warning("read-only: refusing cancel_order %s", client_order_id)
        return OrderResult(client_order_id=client_order_id,
                           status=OrderStatus.REJECTED, reject_reason=READ_ONLY_REASON)

    async def amend_order(self, client_order_id: str, new_limit_price: Decimal) -> OrderResult:
        log.warning("read-only: refusing amend_order %s", client_order_id)
        return OrderResult(client_order_id=client_order_id,
                           status=OrderStatus.REJECTED, reject_reason=READ_ONLY_REASON)
