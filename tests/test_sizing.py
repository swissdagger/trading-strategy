"""
Tests for execution/sizing.py — the 10%-of-available-equity position-sizing policy.

Uses a fake adapter exposing only the three reads sizing depends on
(get_account_equity, get_touch, get_step_size), so the policy's arithmetic and
its safety guards are tested without a venue. Mirrors the fake-adapter style of
test_signal_executor.py.

The contract under test (from execution/sizing.make_size_position):
  * notional = equity_fraction * equity.total; qty = notional / touch-mid.
  * qty is floored to adapter.get_step_size (None => un-floored).
  * ANY condition that makes sizing impossible or sub-step returns Decimal(0),
    which the executor treats as "no order" — sizing never raises into the
    signal path for a transient read miss.
"""
from decimal import Decimal
from typing import Optional

import pytest

from execution.sizing import make_size_position, EQUITY_FRACTION
from execution.types import AccountEquity, Touch, TradeSide


# --------------------------------------------------------------------
# Fake adapter — only the reads sizing uses. Each read can be set to a
# value or to an Exception instance, which the fake will raise, so the
# "read failed" branches are exercised.
# --------------------------------------------------------------------
class FakeAdapter:
    venue = "binance"
    supported_classes = {"crypto"}

    def __init__(self, *, equity=None, touch=None, step=None):
        self._equity = equity
        self._touch = touch
        self._step = step

    async def get_account_equity(self) -> AccountEquity:
        if isinstance(self._equity, Exception):
            raise self._equity
        if self._equity is None:
            raise RuntimeError("no equity configured")
        return self._equity

    async def get_touch(self, symbol: str, asset_class: str) -> Optional[Touch]:
        if isinstance(self._touch, Exception):
            raise self._touch
        return self._touch

    async def get_step_size(self, symbol: str, asset_class: str) -> Optional[Decimal]:
        if isinstance(self._step, Exception):
            raise self._step
        return self._step


def _sizer(adapter, **kwargs):
    """Build the size_position hook over a fixed fake adapter."""
    return make_size_position(lambda: adapter, **kwargs)


def eq(total, currency="USDT", available=None):
    return AccountEquity(total=Decimal(str(total)), currency=currency,
                         available=None if available is None else Decimal(str(available)))


def touch(bid, ask):
    return Touch(bid=Decimal(str(bid)), ask=Decimal(str(ask)))


# --------------------------------------------------------------------
# Core arithmetic
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ten_percent_notional_is_exact():
    # $100k equity, mid 60001 -> 10% = $10000 notional / 60001 ≈ 0.166663...
    a = FakeAdapter(equity=eq(100_000), touch=touch(60_000, 60_002), step=None)
    qty = await _sizer(a)("BTCUSDT", "crypto", TradeSide.LONG, None)
    # Notional reconciles to exactly 10% of equity (un-floored path).
    assert qty * Decimal("60001") == Decimal("10000")
    assert qty == (EQUITY_FRACTION * Decimal("100000")) / Decimal("60001")


@pytest.mark.asyncio
async def test_uses_touch_mid_not_a_single_side():
    # Asymmetric spread: mid must be (bid+ask)/2 = 100.5, not bid or ask.
    a = FakeAdapter(equity=eq(1_000), touch=touch(100, 101), step=None)
    qty = await _sizer(a)("X", "crypto", TradeSide.LONG, None)
    assert qty == (EQUITY_FRACTION * Decimal("1000")) / Decimal("100.5")


@pytest.mark.asyncio
async def test_custom_equity_fraction():
    a = FakeAdapter(equity=eq(100_000), touch=touch(100, 100), step=None)
    qty = await _sizer(a, equity_fraction=Decimal("0.10"))("X", "crypto", TradeSide.LONG, None)
    assert qty == Decimal("10000") / Decimal("100")   # 10% of 100k / 100


# --------------------------------------------------------------------
# Step flooring
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_floors_to_step_size():
    # 0.166663... floored to a 0.001 step -> 0.166.
    a = FakeAdapter(equity=eq(100_000), touch=touch(60_000, 60_002), step=Decimal("0.001"))
    qty = await _sizer(a)("BTCUSDT", "crypto", TradeSide.LONG, None)
    assert qty == Decimal("0.166")


@pytest.mark.asyncio
async def test_whole_share_step():
    # ~$220 stock, 10% of 100k = $10000 / 220 = 45.4 -> floored to 45 whole shares.
    a = FakeAdapter(equity=eq(100_000), touch=touch(219.98, 220.02), step=Decimal("1"))
    qty = await _sizer(a)("AAPL", "equity", TradeSide.LONG, None)
    assert qty == Decimal("45")


@pytest.mark.asyncio
async def test_unknown_step_passes_through_unfloored():
    # Adapter returns None (step unknown) -> qty is not floored.
    a = FakeAdapter(equity=eq(100_000), touch=touch(60_000, 60_002), step=None)
    qty = await _sizer(a)("BTCUSDT", "crypto", TradeSide.LONG, None)
    assert qty == (EQUITY_FRACTION * Decimal("100000")) / Decimal("60001")


@pytest.mark.asyncio
async def test_floor_to_step_disabled():
    # floor_to_step=False ignores the step even when one is available.
    a = FakeAdapter(equity=eq(100_000), touch=touch(60_000, 60_002), step=Decimal("0.001"))
    qty = await _sizer(a, floor_to_step=False)("BTCUSDT", "crypto", TradeSide.LONG, None)
    assert qty == (EQUITY_FRACTION * Decimal("100000")) / Decimal("60001")   # un-floored


# --------------------------------------------------------------------
# Touch fallback to decision_price
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_falls_back_to_decision_price_when_no_touch():
    a = FakeAdapter(equity=eq(100_000), touch=None, step=None)
    qty = await _sizer(a)("BTCUSDT", "crypto", TradeSide.LONG, 60_000.0)
    assert qty == (EQUITY_FRACTION * Decimal("100000")) / Decimal("60000")


@pytest.mark.asyncio
async def test_no_touch_and_no_decision_price_is_no_order():
    a = FakeAdapter(equity=eq(100_000), touch=None, step=None)
    qty = await _sizer(a)("BTCUSDT", "crypto", TradeSide.LONG, None)
    assert qty == Decimal(0)


# --------------------------------------------------------------------
# Safety guards — every impossible/degenerate case returns 0 ("no order"),
# never raises, never a placeholder size.
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_zero_equity_is_no_order():
    a = FakeAdapter(equity=eq(0), touch=touch(60_000, 60_002), step=Decimal("0.001"))
    qty = await _sizer(a)("BTCUSDT", "crypto", TradeSide.LONG, None)
    assert qty == Decimal(0)


@pytest.mark.asyncio
async def test_sub_step_quantity_is_no_order():
    # Equity so small that 10% buys less than one step: $100 * 10% = $10 /
    # 60001 = 0.000167, floored to a 0.001 step -> 0 -> no order (rather than
    # sending a sub-minimum qty the venue would reject).
    a = FakeAdapter(equity=eq(100), touch=touch(60_000, 60_002), step=Decimal("0.001"))
    qty = await _sizer(a)("BTCUSDT", "crypto", TradeSide.LONG, None)
    assert qty == Decimal(0)


@pytest.mark.asyncio
async def test_equity_read_failure_is_no_order_not_raise():
    a = FakeAdapter(equity=RuntimeError("account unreadable"),
                    touch=touch(60_000, 60_002), step=None)
    # Must not propagate — a transient read miss skips one entry.
    qty = await _sizer(a)("BTCUSDT", "crypto", TradeSide.LONG, None)
    assert qty == Decimal(0)


@pytest.mark.asyncio
async def test_touch_read_failure_falls_back_then_no_order_without_price():
    a = FakeAdapter(equity=eq(100_000), touch=RuntimeError("book unavailable"), step=None)
    # No decision_price to fall back on -> no order, no raise.
    qty = await _sizer(a)("BTCUSDT", "crypto", TradeSide.LONG, None)
    assert qty == Decimal(0)


@pytest.mark.asyncio
async def test_step_read_failure_leaves_unfloored_not_raise():
    # A failing get_step_size must not sink the order — sizing logs and leaves
    # the qty un-floored (submit-time precision handles it).
    a = FakeAdapter(equity=eq(100_000), touch=touch(60_000, 60_002),
                    step=RuntimeError("exchangeInfo down"))
    qty = await _sizer(a)("BTCUSDT", "crypto", TradeSide.LONG, None)
    assert qty == (EQUITY_FRACTION * Decimal("100000")) / Decimal("60001")


@pytest.mark.asyncio
async def test_follows_adapter_swap():
    # get_adapter is re-read each call, so a paper->live swap is picked up
    # without rebuilding the hook (the launcher relies on this).
    current = {"adapter": FakeAdapter(equity=eq(100_000), touch=touch(100, 100), step=None)}
    size = make_size_position(lambda: current["adapter"])
    first = await size("X", "crypto", TradeSide.LONG, None)
    assert first == Decimal("10000") / Decimal("100")
    # Swap to a different account; same hook must now size off the new equity.
    current["adapter"] = FakeAdapter(equity=eq(200_000), touch=touch(100, 100), step=None)
    second = await size("X", "crypto", TradeSide.LONG, None)
    assert second == Decimal("20000") / Decimal("100")