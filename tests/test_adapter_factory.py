"""
Tests for execution/adapter_factory.py — the mode -> endpoint-kind mapping.

The safety-critical property: LIVE maps to the live endpoint kind, PAPER and OFF
map to paper, and OFF NEVER maps to live (a stray order in OFF must not be able
to reach real money).
"""
import pytest

from execution.adapter_factory import (
    build_coinbase_adapter, endpoint_kind_for_mode,
)
from execution.trade_enable import TradeMode


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


@pytest.mark.parametrize("mode", [TradeMode.OFF, TradeMode.LIVE])
def test_coinbase_builder_fails_fast_without_credentials(mode):
    # No key configured: refuse at build time, never connect half-configured.
    from execution.coinbase_client import CoinbaseAuthError
    with pytest.raises(CoinbaseAuthError):
        build_coinbase_adapter({}, mode)


def test_coinbase_paper_refuses_until_phase5():
    with pytest.raises(NotImplementedError, match="Phase 5"):
        build_coinbase_adapter({}, TradeMode.PAPER)
