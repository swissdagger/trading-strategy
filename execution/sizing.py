"""
execution/sizing.py — position sizing policy.

Sizing is a RISK DECISION, not persistence, so it lives here (not in
persistence.py) and reads live venue state (equity + touch) rather than the DB.
The executor calls it through the ExecutorHooks.size_position hook; this module
provides the concrete policy the launcher wires in.

CURRENT POLICY: 5% of account equity per position (notional). For a signal to
open a position in `symbol`, target notional = 0.05 * equity.total; quantity =
notional / reference_price, where reference_price is the mid of the current
touch. The quantity is then floored to the venue's step size (see rounding note).

This is deliberately simple — one knob (EQUITY_FRACTION) — and equity-based, so
it self-scales with the account. It uses total equity (not available/buying
power); a leveraged or margin-aware variant would use equity.available and a
per-class leverage cap instead. Documented as the interim rule agreed for now.
"""
from __future__ import annotations

import logging
from decimal import Decimal, ROUND_DOWN
from typing import Awaitable, Callable, Optional

from execution.adapter import ExecutionAdapter
from execution.types import AccountEquity, Touch, TradeSide

log = logging.getLogger("executor.sizing")

# Fraction of account equity to commit per position. 5% for now.
EQUITY_FRACTION = Decimal("0.05")


def make_size_position(
    get_adapter: Callable[[], ExecutionAdapter],
    *,
    equity_fraction: Decimal = EQUITY_FRACTION,
    floor_to_step: bool = True,
) -> Callable[[str, str, TradeSide, Optional[float]], Awaitable[Decimal]]:
    """Build the size_position hook.

    get_adapter: returns the CURRENT raw adapter. A callable (not the adapter
        itself) so sizing follows a paper<->live adapter swap — the same reason
        get_touch is wired through current["adapter"] in the launcher.
    equity_fraction: fraction of total equity per position (default 5%).
    floor_to_step: when True (default), floor the computed quantity to the
        venue's LOT_SIZE/contract step via adapter.get_step_size, so a
        sub-increment order can't be rejected. When the adapter returns None
        (step unknown), or this is False, the quantity is returned un-floored and
        the adapter expresses it in the symbol's precision at submit time.

    The returned hook signature matches ExecutorHooks.size_position exactly:
        (symbol, asset_class, side, decision_price) -> Decimal qty
    Returns Decimal(0) on any condition that makes sizing impossible (no equity,
    no touch), which the executor already treats as "no order" (it logs
    "sizing returned 0 — no order" and skips). We never raise into the signal
    path for a transient read miss — a missed size skips one entry, it doesn't
    take the process down.
    """
    async def size_position(symbol: str, asset_class: str, side: TradeSide,
                            decision_price: Optional[float]) -> Decimal:
        adapter = get_adapter()

        # 1. Live equity.
        try:
            equity: AccountEquity = await adapter.get_account_equity()
        except Exception:
            log.exception("size_position: equity read failed for %s — no order", symbol)
            return Decimal(0)
        if equity.total <= 0:
            log.warning("size_position: non-positive equity %s — no order", equity.total)
            return Decimal(0)

        # 2. Reference price: prefer the live touch mid; fall back to the signal's
        #    decision_price only if the touch is unavailable (better than nothing,
        #    but the touch is truer to what we'll pay).
        reference: Optional[Decimal] = None
        try:
            touch: Optional[Touch] = await adapter.get_touch(symbol, asset_class)
        except Exception:
            log.exception("size_position: touch read failed for %s", symbol)
            touch = None
        if touch is not None:
            reference = (touch.bid + touch.ask) / Decimal(2)
        elif decision_price is not None and decision_price > 0:
            reference = Decimal(str(decision_price))
            log.info("size_position: no touch for %s; sizing off decision_price %s",
                     symbol, reference)
        if reference is None or reference <= 0:
            log.warning("size_position: no usable reference price for %s — no order", symbol)
            return Decimal(0)

        # 3. notional = fraction * equity; qty = notional / price.
        notional = equity_fraction * equity.total
        qty = notional / reference

        # 4. Floor to the venue step size when we have one, so we never send a
        #    sub-increment qty the venue will reject. adapter.get_step_size caches
        #    per symbol; a None return (unknown step) leaves the qty un-floored.
        if floor_to_step:
            try:
                step = await adapter.get_step_size(symbol, asset_class)
            except Exception:
                log.exception("size_position: step read failed for %s; leaving un-floored", symbol)
                step = None
            if step and step > 0:
                qty = (qty // step) * step
                qty = qty.quantize(step, rounding=ROUND_DOWN)

        if qty <= 0:
            log.warning("size_position: computed qty %s for %s (equity=%s ref=%s) — no order",
                        qty, symbol, equity.total, reference)
            return Decimal(0)

        log.info("size_position: %s %s qty=%s (%.1f%% of %s %s @ ref %s)",
                 symbol, side.value, qty, float(equity_fraction * 100),
                 equity.total, equity.currency, reference)
        return qty

    return size_position