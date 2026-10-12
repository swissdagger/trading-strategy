"""
Tests for feed_process._build_feeds transport selection and config.transport_for.

Construction-only: we build feed objects and inspect their type/composition,
never calling start_live (no network). Verifies:
  - default transport is 'rest' (absent key => polling feed)
  - transport='ws' fails loudly: no class has a websocket feed since the
    crypto feed moved to Coinbase (ADR-001 Phase 2)
  - a class with no feed (equity/fx since ADR-001 Phase 1) is skipped
  - empty-symbol classes are skipped
  - config.transport_for defaulting
"""
import pytest

import config
from feed_process import _build_feeds
from feeds.coinbase_spot import CoinbaseSpotFeed


def _cfg(asset_class="crypto", transport=None, symbols=("BTC-USD",)):
    c = {
        "symbols": list(symbols),
        "timeframes": ["1m", "3m", "5m", "1h"],
        "points": 5000,
        "baseline_timeframe": "1m",
    }
    if transport is not None:
        c["transport"] = transport
    return {asset_class: c}


# --------------------------------------------------------------------
# config.transport_for
# --------------------------------------------------------------------

def test_transport_defaults_to_rest(monkeypatch):
    monkeypatch.setattr(config, "ASSET_CONFIGS", _cfg())
    assert config.transport_for("crypto") == "rest"


def test_transport_explicit_ws(monkeypatch):
    monkeypatch.setattr(config, "ASSET_CONFIGS", _cfg(transport="ws"))
    assert config.transport_for("crypto") == "ws"


def test_transport_unknown_class_defaults_rest(monkeypatch):
    monkeypatch.setattr(config, "ASSET_CONFIGS", {})
    assert config.transport_for("nonexistent") == "rest"


# --------------------------------------------------------------------
# _build_feeds transport selection
# --------------------------------------------------------------------

def test_default_builds_coinbase_rest_crypto(monkeypatch):
    monkeypatch.setattr(config, "ASSET_CONFIGS", _cfg())
    feeds = _build_feeds(config.ASSET_CONFIGS)
    assert isinstance(feeds["crypto"], CoinbaseSpotFeed)


def test_ws_transport_fails_loudly(monkeypatch):
    # No websocket feed for Coinbase: a silent fallback to polling would hide
    # the misconfiguration.
    monkeypatch.setattr(config, "ASSET_CONFIGS", _cfg(transport="ws"))
    with pytest.raises(RuntimeError, match="no websocket feed"):
        _build_feeds(config.ASSET_CONFIGS)


def test_binance_form_symbol_is_rejected(monkeypatch):
    monkeypatch.setattr(config, "ASSET_CONFIGS", _cfg(symbols=("BTCUSDT",)))
    with pytest.raises(ValueError, match="product ids"):
        _build_feeds(config.ASSET_CONFIGS)


@pytest.mark.parametrize("asset_class", ["equity", "fx"])
def test_class_without_feed_is_skipped(monkeypatch, asset_class):
    # ADR-001 Phase 1 deleted the Polygon feeds; a stray equity/fx entry must
    # not crash the feed process, just be skipped with a warning.
    monkeypatch.setattr(config, "ASSET_CONFIGS", _cfg(asset_class, symbols=("AAPL",)))
    assert _build_feeds(config.ASSET_CONFIGS) == {}


def test_empty_symbols_skipped(monkeypatch):
    monkeypatch.setattr(config, "ASSET_CONFIGS", _cfg(symbols=()))
    feeds = _build_feeds(config.ASSET_CONFIGS)
    assert "crypto" not in feeds


def test_shipped_config_builds_one_crypto_feed():
    # The real config: crypto only, Coinbase product ids, REST transport.
    feeds = _build_feeds(config.ASSET_CONFIGS)
    assert list(feeds) == ["crypto"]
    assert isinstance(feeds["crypto"], CoinbaseSpotFeed)
    assert all("-" in s for s in feeds["crypto"].symbols)
