"""
Tests for the transport wiring: config.transport_for and
feed_process._build_feeds transport selection.

We verify the WIRING, not the feeds themselves (those have their own tests):
  - transport_for defaults to 'rest', validates the value, and rejects 'ws'
    for a class with no ws feed
  - _build_feeds builds a ws feed for a ws-transport class, composing the
    REST sibling as its backfill engine
  - _build_feeds builds a plain REST feed for a rest-transport (or default)
    class
  - a class with no symbols is skipped

_build_feeds reads config.transport_for and os.environ; we monkeypatch
ASSET_CONFIGS and stub the feed constructors so no real feed deps or network
are involved.
"""
import importlib

import pytest


# --------------------------------------------------------------------
# config.transport_for
# --------------------------------------------------------------------

def _reload_config(monkeypatch, asset_configs):
    import config
    importlib.reload(config)
    monkeypatch.setattr(config, "ASSET_CONFIGS", asset_configs, raising=True)
    return config


def test_transport_defaults_to_rest(monkeypatch):
    config = _reload_config(monkeypatch, {"crypto": {"symbols": ["BTCUSDT"]}})
    assert config.transport_for("crypto") == "rest"
    # An entirely absent class also defaults to rest (empty cfg).
    assert config.transport_for("nonexistent") == "rest"


def test_transport_explicit_ws(monkeypatch):
    config = _reload_config(monkeypatch, {"equity": {"symbols": ["AAPL"], "transport": "ws"}})
    assert config.transport_for("equity") == "ws"


def test_transport_rejects_unknown(monkeypatch):
    config = _reload_config(monkeypatch, {"equity": {"symbols": ["AAPL"], "transport": "carrier-pigeon"}})
    with pytest.raises(ValueError, match="unknown transport"):
        config.transport_for("equity")


# Note: the "ws requested for a class with no ws feed" check lives in
# feed_process._build_feeds (raising RuntimeError, the startup-misconfig
# convention), not in transport_for — so it's covered by the feed-wiring
# tests, not here. transport_for only validates the transport VALUE.


# --------------------------------------------------------------------
# feed_process._build_feeds transport selection
# --------------------------------------------------------------------

@pytest.fixture
def stub_feeds(monkeypatch):
    """Replace the real feed builders with stubs that record their args, so
    we test wiring/selection without importing real feeds."""
    import feed_process

    built = {"rest": [], "ws": []}

    class StubRest:
        def __init__(self, asset_class):
            self.asset_class = asset_class
            self.name = f"stub_rest_{asset_class}"

    class StubWS:
        def __init__(self, asset_class, backfill_feed):
            self.asset_class = asset_class
            self.name = f"stub_ws_{asset_class}"
            self.backfill_feed = backfill_feed

    def fake_build_rest(asset_class, symbols, timeframes):
        f = StubRest(asset_class)
        built["rest"].append(f)
        return f

    def fake_build_ws(asset_class, backfill_feed):
        f = StubWS(asset_class, backfill_feed)
        built["ws"].append(f)
        return f

    monkeypatch.setattr(feed_process, "_build_rest_feed", fake_build_rest)
    monkeypatch.setattr(feed_process, "_build_ws_feed", fake_build_ws)
    return feed_process, built, StubRest, StubWS


def _patch_transport(monkeypatch, mapping):
    """Patch config.transport_for (imported inside _build_feeds) to return
    from a fixed mapping."""
    import config
    monkeypatch.setattr(config, "transport_for", lambda ac: mapping.get(ac, "rest"))


def test_build_feeds_ws_composes_rest_sibling(stub_feeds, monkeypatch):
    feed_process, built, StubRest, StubWS = stub_feeds
    _patch_transport(monkeypatch, {"equity": "ws"})
    feeds = feed_process._build_feeds({
        "equity": {"symbols": ["AAPL"], "timeframes": ["5s", "1m"]},
    })
    # The live feed is the ws feed...
    assert isinstance(feeds["equity"], StubWS)
    # ...and its backfill engine is the REST sibling built for the same class.
    assert isinstance(feeds["equity"].backfill_feed, StubRest)
    assert feeds["equity"].backfill_feed.asset_class == "equity"
    # Exactly one of each was built.
    assert len(built["rest"]) == 1 and len(built["ws"]) == 1


def test_build_feeds_rest_transport_no_ws(stub_feeds, monkeypatch):
    feed_process, built, StubRest, StubWS = stub_feeds
    _patch_transport(monkeypatch, {"crypto": "rest"})
    feeds = feed_process._build_feeds({
        "crypto": {"symbols": ["BTCUSDT"], "timeframes": ["1m"]},
    })
    assert isinstance(feeds["crypto"], StubRest)
    assert built["ws"] == []   # no ws feed built for a rest class


def test_build_feeds_default_transport_is_rest(stub_feeds, monkeypatch):
    feed_process, built, StubRest, StubWS = stub_feeds
    # No transport in the mapping -> defaults to rest.
    _patch_transport(monkeypatch, {})
    feeds = feed_process._build_feeds({
        "equity": {"symbols": ["AAPL"], "timeframes": ["1m"]},
    })
    assert isinstance(feeds["equity"], StubRest)


def test_build_feeds_skips_empty_symbols(stub_feeds, monkeypatch):
    feed_process, built, StubRest, StubWS = stub_feeds
    _patch_transport(monkeypatch, {"equity": "ws"})
    feeds = feed_process._build_feeds({
        "equity": {"symbols": [], "timeframes": ["1m"]},
        "fx": {"symbols": ["EURUSD"], "timeframes": ["1m"]},
    })
    assert "equity" not in feeds       # no symbols -> skipped entirely
    assert "fx" in feeds               # fx defaulted to rest, still built
    assert len(built["rest"]) == 1     # only fx's rest feed was built


def test_build_feeds_mixed_transports(stub_feeds, monkeypatch):
    # equity on ws, crypto on rest, in one process — the whole point of
    # Option B (migrate one class at a time).
    feed_process, built, StubRest, StubWS = stub_feeds
    _patch_transport(monkeypatch, {"equity": "ws", "crypto": "rest"})
    feeds = feed_process._build_feeds({
        "equity": {"symbols": ["AAPL"], "timeframes": ["5s"]},
        "crypto": {"symbols": ["BTCUSDT"], "timeframes": ["1m"]},
    })
    assert isinstance(feeds["equity"], StubWS)
    assert isinstance(feeds["crypto"], StubRest)
