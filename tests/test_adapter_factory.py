"""
Tests for execution/adapter_factory.py — the mode -> venue-endpoint mapping.

The safety-critical property: LIVE maps to the live endpoint, PAPER and OFF map
to the paper endpoint, and OFF NEVER maps to live (a stray order in OFF must not
be able to reach real money).
"""
import pytest

from execution.adapter_factory import (
    build_ibkr_adapter, endpoint_kind_for_mode,
)
from execution.trade_enable import TradeMode


VENUE_CFG = {
    "paper": {"host": "127.0.0.1", "port": 4002, "account": "DU123"},
    "live":  {"host": "127.0.0.1", "port": 4001, "account": "U456"},
}


def test_live_mode_uses_live_endpoint():
    adapter, kind = build_ibkr_adapter(VENUE_CFG, TradeMode.LIVE)
    assert kind == "live"
    assert adapter._port == 4001
    assert adapter._account == "U456"


def test_paper_mode_uses_paper_endpoint():
    adapter, kind = build_ibkr_adapter(VENUE_CFG, TradeMode.PAPER)
    assert kind == "paper"
    assert adapter._port == 4002
    assert adapter._account == "DU123"


def test_off_mode_uses_paper_never_live():
    # OFF must never build a live adapter — a stray order in OFF hits paper.
    adapter, kind = build_ibkr_adapter(VENUE_CFG, TradeMode.OFF)
    assert kind == "paper"
    assert adapter._port == 4002


def test_endpoint_kind_mapping():
    assert endpoint_kind_for_mode(TradeMode.LIVE) == "live"
    assert endpoint_kind_for_mode(TradeMode.PAPER) == "paper"
    assert endpoint_kind_for_mode(TradeMode.OFF) == "paper"


def test_venue_change_detection():
    # off<->paper is NOT a venue change (both paper) -> no rebuild needed.
    assert endpoint_kind_for_mode(TradeMode.OFF) == endpoint_kind_for_mode(TradeMode.PAPER)
    # anything involving live IS a venue change.
    assert endpoint_kind_for_mode(TradeMode.PAPER) != endpoint_kind_for_mode(TradeMode.LIVE)
    assert endpoint_kind_for_mode(TradeMode.OFF) != endpoint_kind_for_mode(TradeMode.LIVE)


# --------------------------------------------------------------------
# Binance factory
# --------------------------------------------------------------------

BINANCE_CFG = {
    "paper": {"api_key": "pk", "api_secret": "ps", "base_url": "https://demo-fapi.binance.com"},
    "live":  {"api_key": "lk", "api_secret": "ls", "base_url": "https://fapi.binance.com"},
}


def test_binance_live_uses_production():
    from execution.adapter_factory import build_binance_adapter
    a, kind = build_binance_adapter(BINANCE_CFG, TradeMode.LIVE)
    assert kind == "live"
    assert a._base == "https://fapi.binance.com"


def test_binance_paper_uses_testnet():
    from execution.adapter_factory import build_binance_adapter
    a, kind = build_binance_adapter(BINANCE_CFG, TradeMode.PAPER)
    assert kind == "paper"
    assert a._base == "https://demo-fapi.binance.com"


def test_binance_off_uses_testnet_never_live():
    from execution.adapter_factory import build_binance_adapter
    a, kind = build_binance_adapter(BINANCE_CFG, TradeMode.OFF)
    assert kind == "paper"
    assert a._base == "https://demo-fapi.binance.com"