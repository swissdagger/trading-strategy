"""
execution/pricing.py — marketable-limit pricing (extracted, shared).

The one implementation of how QISS prices a crossing limit order, extracted so
BOTH the legacy Kafka executor and the new signal executor use it — pricing
policy must live in exactly one place. Pure functions, Decimal throughout (the
float->Decimal boundary is crossed once, at CapConfig construction).
"""
from __future__ import annotations

from decimal import Decimal

from execution.types import OrderIntent, TradeSide


class CapConfig:
    """One (asset_class, intent) row of execution_caps, as the executor sees it.
    spread is the current venue spread used to scale the multiples. Multiples are
    Decimal so all pricing arithmetic stays in Decimal — the float->Decimal
    boundary is crossed ONCE here at construction."""
    __slots__ = ("cross_spread_mult", "cap_spread_mult", "cap_abs_bps")

    def __init__(self, cross_spread_mult, cap_spread_mult, cap_abs_bps):
        self.cross_spread_mult = Decimal(str(cross_spread_mult))
        self.cap_spread_mult = Decimal(str(cap_spread_mult))
        self.cap_abs_bps = (None if cap_abs_bps is None
                            else Decimal(str(cap_abs_bps)))


def is_buy(side: TradeSide, intent: OrderIntent) -> bool:
    """A BUY order: opening a LONG, or closing a SHORT. A SELL: opening a SHORT,
    or closing a LONG."""
    if intent == OrderIntent.ENTRY:
        return side == TradeSide.LONG
    return side == TradeSide.SHORT


def price_marketable_limit(
    side: TradeSide,
    intent: OrderIntent,
    touch_bid: Decimal,
    touch_ask: Decimal,
    cap: CapConfig,
) -> tuple[Decimal, Decimal, Decimal]:
    """Return (limit_price, venue_reference, cap_applied) for a crossing limit.

    Cross THROUGH the touch by cross_spread_mult * spread so the order fills
    immediately like a market order, but cap how far beyond the touch we tolerate
    at cap_spread_mult * spread (and never worse than cap_abs_bps if set). BUY
    crosses UP from the ask; SELL crosses DOWN from the bid.
    """
    spread = touch_ask - touch_bid
    buying = is_buy(side, intent)
    if buying:
        reference = touch_ask
        crossed = touch_ask + cap.cross_spread_mult * spread
        cap_edge = touch_ask + cap.cap_spread_mult * spread
        if cap.cap_abs_bps is not None:
            cap_edge = min(cap_edge, touch_ask * (Decimal(1) + cap.cap_abs_bps / 10000))
        limit = min(crossed, cap_edge)
    else:
        reference = touch_bid
        crossed = touch_bid - cap.cross_spread_mult * spread
        cap_edge = touch_bid - cap.cap_spread_mult * spread
        if cap.cap_abs_bps is not None:
            cap_edge = max(cap_edge, touch_bid * (Decimal(1) - cap.cap_abs_bps / 10000))
        limit = max(crossed, cap_edge)
    cap_applied = abs(limit - reference)
    return limit, reference, cap_applied
