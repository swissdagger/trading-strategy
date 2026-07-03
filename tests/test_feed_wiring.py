"""
Tests for feed_process._build_feeds transport selection and config.transport_for.

Construction-only: we build feed objects and inspect their type/composition,
never calling start_live (no network). Verifies:
  - default transport is 'rest' (absent key => polling feed)
  - transport='ws' builds the ws feed AND composes the REST sibling as its
    backfill engine
  - transport='ws' on crypto raises (no binance_ws yet) — fail loud
  - empty-symbol classes are skipped
  - config.transport_for defaulting
"""
import os

import pytest

import config
from feed_process import _build_feeds
from feeds.polygon_ws import PolygonStocksWSFeed, PolygonForexWSFeed
from feeds.polygon_stocks import PolygonStocksFeed
from feeds.polygon_forex import PolygonForexFeed
from feeds.streaming_base import StreamingFeedBase


@pytest.fixture(autouse=True)
def polygon_key(monkeypatch):
    monkeypatch.setenv("POLYGON_API_KEY", "TESTKEY")


def _cfg(asset_class, transport=None, symbols=("AAPL",)):
    c = {
        "symbols": list(symbols),
        "timeframes": ["5s", "10s", "30s", "1m", "2m", "5m", "10m"],
        "points": 5000,
        "baseline_timeframe": "5s",
    }
    if transport is not None:
        c["transport"] = transport
    return {asset_class: c}


# --------------------------------------------------------------------
# config.transport_for
# --------------------------------------------------------------------

def test_transport_defaults_to_rest(monkeypatch):
    monkeypatch.setattr(config, "ASSET_CONFIGS", _cfg("equity"))
    assert config.transport_for("equity") == "rest"


def test_transport_explicit_ws(monkeypatch):
    monkeypatch.setattr(config, "ASSET_CONFIGS", _cfg("equity", transport="ws"))
    assert config.transport_for("equity") == "ws"


def test_transport_unknown_class_defaults_rest(monkeypatch):
    monkeypatch.setattr(config, "ASSET_CONFIGS", {})
    assert config.transport_for("nonexistent") == "rest"


# --------------------------------------------------------------------
# _build_feeds transport selection
# --------------------------------------------------------------------

def test_default_builds_rest_equity(monkeypatch):
    monkeypatch.setattr(config, "ASSET_CONFIGS", _cfg("equity"))
    feeds = _build_feeds(config.ASSET_CONFIGS)
    assert isinstance(feeds["equity"], PolygonStocksFeed)
    assert not isinstance(feeds["equity"], StreamingFeedBase)


def test_ws_builds_streaming_equity_with_rest_backfill(monkeypatch):
    monkeypatch.setattr(config, "ASSET_CONFIGS", _cfg("equity", transport="ws"))
    feeds = _build_feeds(config.ASSET_CONFIGS)
    feed = feeds["equity"]
    assert isinstance(feed, PolygonStocksWSFeed)
    # The composed backfill engine is the REST sibling.
    assert isinstance(feed._backfill_feed, PolygonStocksFeed)


def test_ws_builds_streaming_fx_with_rest_backfill(monkeypatch):
    monkeypatch.setattr(
        config, "ASSET_CONFIGS", _cfg("fx", transport="ws", symbols=("EURUSD",)),
    )
    feeds = _build_feeds(config.ASSET_CONFIGS)
    feed = feeds["fx"]
    assert isinstance(feed, PolygonForexWSFeed)
    assert isinstance(feed._backfill_feed, PolygonForexFeed)


def test_ws_builds_streaming_crypto_with_rest_backfill(monkeypatch):
    # crypto now HAS a ws feed (binance_ws). transport='ws' must build the
    # BinanceFuturesWSFeed composing the REST BinanceFuturesFeed as backfill.
    # Uses Binance-native timeframes so the REST-feed (backfill engine, built
    # first) constructs cleanly.
    from feeds.binance_ws import BinanceFuturesWSFeed
    from feeds.binance_futures import BinanceFuturesFeed
    cfg = {
        "crypto": {
            "symbols": ["BTCUSDT"],
            "timeframes": ["1m", "5m", "1h"],  # all Binance-supported
            "points": 5000,
            "baseline_timeframe": "1m",
            "transport": "ws",
        }
    }
    monkeypatch.setattr(config, "ASSET_CONFIGS", cfg)
    feeds = _build_feeds(config.ASSET_CONFIGS)
    feed = feeds["crypto"]
    assert isinstance(feed, BinanceFuturesWSFeed)
    assert isinstance(feed._backfill_feed, BinanceFuturesFeed)


def test_empty_symbols_skipped(monkeypatch):
    monkeypatch.setattr(config, "ASSET_CONFIGS", _cfg("equity", symbols=()))
    feeds = _build_feeds(config.ASSET_CONFIGS)
    assert "equity" not in feeds


def test_mixed_transports_coexist(monkeypatch):
    cfg = {}
    cfg.update(_cfg("equity", transport="ws"))
    cfg.update(_cfg("fx", transport="rest", symbols=("EURUSD",)))
    monkeypatch.setattr(config, "ASSET_CONFIGS", cfg)
    feeds = _build_feeds(config.ASSET_CONFIGS)
    # equity streams, fx polls — one migrated, one not, at the same time.
    assert isinstance(feeds["equity"], StreamingFeedBase)
    assert isinstance(feeds["fx"], PolygonForexFeed)
    assert not isinstance(feeds["fx"], StreamingFeedBase)
