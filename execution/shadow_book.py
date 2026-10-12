"""
execution/shadow_book.py — virtual fills for signals the venue cannot take.

ADR-001: the account is UK retail spot, so there is no short side. SHORT
signals are still produced (negative chains), and they are the only short-side
data we will get, so instead of dropping them the executor routes them here:

  * ENTER fills virtually at the touch on the side the order would take — a
    short opens by SELLING, so it fills at the BID; a long would fill at the ask.
  * EXIT fills at the touch on the opposite side — a short closes by BUYING at
    the ASK.
  * The taker fee is charged on both legs; pnl_bps is net of it.
  * Whole positions only: SCALE tranches are ignored and the position closes on
    the final EXIT.

Nothing here is ever sized or sent to a venue, and nothing here touches the
real book: shadow positions are not part of reconcile, sizing, the
open-position cap or the kill switch. The results are an UPPER BOUND (touch
fills, no impact, no latency) — ADR-001 debt D8.

Runs inside SignalExecutor.handle_signal, i.e. under the per-symbol lock, so a
shadow ENTER and EXIT for one symbol can never interleave.

Persistence is injected (shadow_trades, migration 019), mirroring the executor's
hook pattern, so this module stays free of asyncpg and testable with fakes.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

from execution.pricing import is_buy, validate_touch
from execution.types import OrderIntent, Touch, TradeSide

log = logging.getLogger("executor.shadow")

_BPS = Decimal(10_000)


@dataclass
class ShadowPosition:
    """An open virtual position. Mirrors one open shadow_trades row."""
    strategy: str
    position_key: str
    symbol: str
    asset_class: str
    side: TradeSide
    entry_signal_id: str
    entry_px: Decimal
    fee_bps: Decimal


@dataclass
class ShadowHooks:
    # (symbol, asset_class) -> live touch, or None if unavailable.
    get_touch: Callable[[str, str], Awaitable[Optional[Touch]]]
    # Insert an open shadow_trades row. Returns False if it already exists
    # (replayed ENTER: entry_signal_id / (strategy, position_key) unique).
    insert_entry: Callable[[dict], Awaitable[bool]]
    # Close the open row for (strategy, position_key). Returns False if there
    # was no open row (already closed by a replayed EXIT).
    close: Callable[[dict], Awaitable[bool]]
    # Open rows at boot, so a restart keeps tracking positions entered earlier.
    load_open: Callable[[], Awaitable[List[ShadowPosition]]]
    now_ms: Optional[Callable[[], int]] = None

    def __post_init__(self):
        if self.now_ms is None:
            import time
            self.now_ms = lambda: int(time.time() * 1000)


def fill_price(side: TradeSide, intent: OrderIntent, touch: Touch) -> Decimal:
    """The touch price a taker order of this side/intent would hit: buys lift
    the ask, sells hit the bid."""
    return touch.ask if is_buy(side, intent) else touch.bid


def net_pnl_bps(side: TradeSide, entry_px: Decimal, exit_px: Decimal,
                fee_bps: Decimal) -> Decimal:
    """Round-trip P&L in basis points of the entry price, net of the taker fee
    charged on both legs."""
    move = (exit_px - entry_px) if side == TradeSide.LONG else (entry_px - exit_px)
    gross = move / entry_px * _BPS
    return (gross - 2 * fee_bps).quantize(Decimal("0.0001"))


def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


class ShadowBook:
    def __init__(self, hooks: ShadowHooks, *, taker_fee_bps: Decimal,
                 fee_bps_fn: Optional[Callable[[], Awaitable[Optional[Decimal]]]] = None):
        """taker_fee_bps: the fallback fee per leg. fee_bps_fn: optional async
        source of the account's real taker fee (the venue's fee tier); when it
        returns a value, that is what a new virtual entry is charged."""
        self.hooks = hooks
        self.taker_fee_bps = Decimal(str(taker_fee_bps))
        self.fee_bps_fn = fee_bps_fn
        self._open: Dict[Tuple[str, str], ShadowPosition] = {}

    async def _fee_bps(self) -> Decimal:
        if self.fee_bps_fn is not None:
            try:
                bps = await self.fee_bps_fn()
                if bps is not None:
                    return Decimal(str(bps))
            except Exception:
                log.exception("shadow: fee tier read failed; using fallback %s bps",
                              self.taker_fee_bps)
        return self.taker_fee_bps

    async def start(self) -> None:
        for p in await self.hooks.load_open():
            self._open[(p.strategy, p.position_key)] = p
        log.info("shadow book: %d open virtual position(s) re-loaded", len(self._open))

    def holds(self, strategy: str, position_key: str) -> bool:
        return (strategy, position_key) in self._open

    def open_count(self) -> int:
        return len(self._open)

    async def _price(self, row: dict, side: TradeSide, intent: OrderIntent
                     ) -> Tuple[Optional[Decimal], str]:
        """(price, source). Prefer the live touch; fall back to the signal's
        decision_price (logged — it is the strategy's price, not a fill price)."""
        try:
            touch = await self.hooks.get_touch(row["symbol"], row["asset_class"])
        except Exception:
            log.exception("shadow: touch read failed for %s", row["symbol"])
            touch = None
        if touch is not None and validate_touch(touch.bid, touch.ask) is None:
            return fill_price(side, intent, touch), "touch"
        dp = row.get("decision_price")
        if dp is not None and Decimal(str(dp)) > 0:
            return Decimal(str(dp)), "decision_price"
        return None, "none"

    async def on_enter(self, row: dict) -> None:
        strategy = row.get("strategy", "two_ladder")
        position_key = row["position_key"]
        if self.holds(strategy, position_key):
            return   # replayed ENTER
        side = TradeSide.LONG if row["direction"] == "LONG" else TradeSide.SHORT
        px, source = await self._price(row, side, OrderIntent.ENTRY)
        if px is None:
            log.warning("shadow ENTER %s/%s: no touch and no decision_price — skipped",
                        strategy, position_key)
            return
        if source != "touch":
            log.warning("shadow ENTER %s/%s priced off %s (no usable touch)",
                        strategy, position_key, source)
        fee_bps = await self._fee_bps()
        pos = ShadowPosition(
            strategy=strategy, position_key=position_key, symbol=row["symbol"],
            asset_class=row["asset_class"], side=side,
            entry_signal_id=row["signal_id"], entry_px=px,
            fee_bps=fee_bps,
        )
        inserted = await self.hooks.insert_entry({
            "strategy": strategy, "position_key": position_key,
            "symbol": pos.symbol, "asset_class": pos.asset_class,
            "side": side.value, "entry_signal_id": pos.entry_signal_id,
            "entry_px": px, "entry_at": _ms_to_dt(self.hooks.now_ms()),
            "fee_bps": fee_bps,
        })
        if not inserted:
            # Already in the table (a restart between insert and the in-memory
            # add). Track it; the DB row is the original.
            log.info("shadow ENTER %s/%s already recorded", strategy, position_key)
        self._open[(strategy, position_key)] = pos
        log.info("shadow ENTER %s %s %s @ %s (%s)", strategy, side.value,
                 pos.symbol, px, source)

    async def on_exit(self, row: dict) -> None:
        strategy = row.get("strategy", "two_ladder")
        position_key = row["position_key"]
        pos = self._open.get((strategy, position_key))
        if pos is None:
            log.debug("shadow EXIT %s/%s: no open virtual position", strategy, position_key)
            return
        if row.get("exit_reason") == "SCALE":
            # Partial tranches are not modelled: the virtual position closes in
            # full on the final (non-SCALE) EXIT. Only prior_failure_scaleout
            # emits SCALE, and it is disabled.
            log.debug("shadow EXIT %s/%s: SCALE tranche ignored", strategy, position_key)
            return
        px, source = await self._price(row, pos.side, OrderIntent.EXIT)
        if px is None:
            # Leave it open: a later EXIT for the position (or an operator) can
            # still close it. Never invent an exit price.
            log.error("shadow EXIT %s/%s: no touch and no decision_price — left open",
                      strategy, position_key)
            return
        pnl = net_pnl_bps(pos.side, pos.entry_px, px, pos.fee_bps)
        await self.hooks.close({
            "strategy": strategy, "position_key": position_key,
            "exit_signal_id": row["signal_id"], "exit_px": px,
            "exit_at": _ms_to_dt(self.hooks.now_ms()),
            "exit_reason": row.get("exit_reason") or "MANUAL",
            "pnl_bps": pnl,
        })
        self._open.pop((strategy, position_key), None)
        log.info("shadow EXIT %s %s %s @ %s (%s) pnl=%sbps net", strategy,
                 pos.side.value, pos.symbol, px, source, pnl)
