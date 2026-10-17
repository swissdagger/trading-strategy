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
  * Whole positions only: an EXIT closes the position. A historical SCALE
    (partial-exit) row is ignored.
  * Shadow-only strategies (the chain rules) fill both legs at the signal's
    decision_price instead, per their spec; the touch only if it is missing.
  * Manual-trade strategies (config.MANUAL_TRADE_STRATEGIES: equity CFDs the
    owner places by hand) post each open and close to Slack at once, naming
    where to place it, instead of joining the batched shadow digest.

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
from ops.alerts import alert

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


def _signal_time(row: dict) -> str:
    """When the signal was written, for a manual-trade alert: the owner acts
    on it by hand and must see how old it is."""
    at = row.get("created_at")
    if isinstance(at, str):
        try:
            at = datetime.fromisoformat(at.replace("Z", "+00:00"))
        except ValueError:
            return at
    if isinstance(at, datetime):
        at = at if at.tzinfo else at.replace(tzinfo=timezone.utc)
        return at.astimezone(timezone.utc).strftime("%H:%M:%S UTC")
    return "time unknown"


def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


class ShadowBook:
    def __init__(self, hooks: ShadowHooks, *, taker_fee_bps: Decimal,
                 fee_bps_fn: Optional[Callable[[], Awaitable[Optional[Decimal]]]] = None,
                 fee_bps_by_class: Optional[dict] = None,
                 opened_digest=None, manual_strategies: Optional[dict] = None):
        """taker_fee_bps: the fallback fee per leg. fee_bps_fn: optional async
        source of the account's real taker fee (the venue's fee tier); when it
        returns a value, that is what a new virtual entry is charged."""
        self.hooks = hooks
        self.taker_fee_bps = Decimal(str(taker_fee_bps))
        self.fee_bps_fn = fee_bps_fn
        # Per-asset-class override (bps per leg): for classes the venue does
        # not trade, its fee tier is meaningless (an equity CFD's cost is the
        # half-spread, not Coinbase's taker fee).
        self.fee_bps_by_class = {k: Decimal(str(v)) for k, v in (fee_bps_by_class or {}).items()}
        # Where "shadow trade opened" alerts collect for a batched Slack post
        # (ops.alerts.AlertDigest); None alerts once per open.
        self.opened_digest = opened_digest
        # strategy -> where its trades are placed by hand (e.g. "Trading 212").
        self.manual_strategies = dict(manual_strategies or {})
        self._open: Dict[Tuple[str, str], ShadowPosition] = {}

    async def _fee_bps(self, asset_class: Optional[str] = None) -> Decimal:
        if asset_class in self.fee_bps_by_class:
            return self.fee_bps_by_class[asset_class]
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

    async def _price(self, row: dict, side: TradeSide, intent: OrderIntent,
                     at_decision_price: bool = False) -> Tuple[Optional[Decimal], str]:
        """(price, source). Prefer the live touch; fall back to the signal's
        decision_price (logged — it is the strategy's price, not a fill price).
        at_decision_price (shadow-only strategies, whose spec fills at the
        signal's price): decision_price first, the touch only without one — so
        a signal replayed after downtime is not priced at the replay-time book."""
        dp = row.get("decision_price")
        if at_decision_price and dp is not None and Decimal(str(dp)) > 0:
            return Decimal(str(dp)), "decision_price"
        try:
            touch = await self.hooks.get_touch(row["symbol"], row["asset_class"])
        except Exception:
            log.exception("shadow: touch read failed for %s", row["symbol"])
            touch = None
        if touch is not None and validate_touch(touch.bid, touch.ask) is None:
            return fill_price(side, intent, touch), "touch"
        if dp is not None and Decimal(str(dp)) > 0:
            return Decimal(str(dp)), "decision_price"
        return None, "none"

    async def on_enter(self, row: dict, at_decision_price: bool = False) -> None:
        strategy = row["strategy"]
        position_key = row["position_key"]
        if self.holds(strategy, position_key):
            return   # replayed ENTER
        side = TradeSide.LONG if row["direction"] == "LONG" else TradeSide.SHORT
        px, source = await self._price(row, side, OrderIntent.ENTRY, at_decision_price)
        if px is None:
            log.warning("shadow ENTER %s/%s: no touch and no decision_price — skipped",
                        strategy, position_key)
            return
        if source != "touch" and not at_decision_price:
            log.warning("shadow ENTER %s/%s priced off %s (no usable touch)",
                        strategy, position_key, source)
        fee_bps = await self._fee_bps(row.get("asset_class"))
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
        if inserted and strategy in self.manual_strategies:
            log.warning("MANUAL TRADE — OPEN %s %s @ %s on %s (%s; signal %s; position %s)",
                        side.value, pos.symbol, px, self.manual_strategies[strategy],
                        strategy, _signal_time(row), position_key,
                        extra=alert(f"manual-open:{strategy}:{position_key}", every=0))
        elif inserted:
            line = (f"{side.value} {pos.symbol} {pos.asset_class} @ {px} ({source}; fee "
                    f"{fee_bps} bps/leg; strategy {strategy}, position {position_key})")
            if self.opened_digest is not None:
                # Batched: one Slack message per interval lists every open.
                self.opened_digest.add(line)
                log.info("SHADOW trade OPENED: %s", line)
            else:
                log.info("SHADOW trade OPENED: %s", line,
                         extra=alert(f"shadow-open:{strategy}:{position_key}", every=0))
        else:
            log.info("shadow ENTER %s %s %s @ %s (%s)", strategy, side.value,
                     pos.symbol, px, source)

    async def on_exit(self, row: dict, at_decision_price: bool = False) -> None:
        strategy = row["strategy"]
        position_key = row["position_key"]
        pos = self._open.get((strategy, position_key))
        if pos is None:
            log.debug("shadow EXIT %s/%s: no open virtual position", strategy, position_key)
            return
        if row.get("exit_reason") == "SCALE":
            # A partial-exit tranche from the removed prior_failure_scaleout
            # strategy (only a historical row can carry it): never a close.
            log.debug("shadow EXIT %s/%s: SCALE tranche ignored", strategy, position_key)
            return
        px, source = await self._price(row, pos.side, OrderIntent.EXIT, at_decision_price)
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
        if strategy in self.manual_strategies:
            log.warning("MANUAL TRADE — CLOSE %s %s @ %s on %s (%s, reason %s; signal %s; "
                        "opened @ %s, %s bps net on the shadow book)",
                        pos.side.value, pos.symbol, px, self.manual_strategies[strategy],
                        strategy, row.get("exit_reason") or "MANUAL", _signal_time(row),
                        pos.entry_px, pnl,
                        extra=alert(f"manual-close:{strategy}:{position_key}", every=0))
