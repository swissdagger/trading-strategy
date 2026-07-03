"""
config.py — The trading system's universe and ladder configuration.

This is the trading repo's OWN config (not CausaView's). It is the single source
of truth for:
  * which symbols are tracked, grouped by asset class (ASSET_CONFIGS), and
  * the two-ladder timeframe sets every symbol runs.

producer/universe.py reads ASSET_CONFIGS to resolve symbol -> asset_class.
producer/ladders.py owns the authoritative Set A / Set B ladders (sourced from
strategy/chain_view.py so producer and consumer cannot disagree); the timeframes
listed here under each class MUST equal the union of those two ladders, because
they are what the feeds must fetch for the two-ladder machine to run.

LADDERS ARE UNIVERSAL across asset classes (confirmed design decision): Set A and
Set B use the same timeframes for every class. So every class below lists the same
seven timeframes. The class only affects the TAG carried through the pipeline,
never routing or level numbering.

FILL IN the real symbol lists. The lists below are PLACEHOLDERS showing the shape.
A symbol must appear under exactly ONE class (universe.build_symbol_class_map
raises if a symbol is configured under two).
"""
from __future__ import annotations

# The seven timeframes spanning Set A (5s,10s,30s,1m,2m) and Set B
# (30s,1m,2m,5m,10m). Union, ascending. These are what the feeds must produce.
# NOTE: 5s/10s availability is a data-entitlement question (Polygon plan for
# equity/fx; Binance klines bottom out at 1m for crypto — sub-minute crypto bars
# need a different source). Confirm before relying on Set A's lowest rungs.
TWO_LADDER_TIMEFRAMES = ["5s", "10s", "30s", "1m", "2m", "5m", "10m"]


# Per-asset-class universe + the points/baseline the feeds and machine need.
# 'symbols'    — the tracked universe for this class (FILL IN real symbols).
# 'timeframes' — the bar granularities the feed must produce (= union of ladders).
# 'points'     — history depth to seed per (symbol, timeframe). WINDOW_SIZE-aligned.
# 'baseline_timeframe' — informational; the actual per-set baselines are the first
#   entry of each ladder (5s for Set A, 30s for Set B), owned by producer/ladders.
ASSET_CONFIGS = {
    "crypto": {
        "symbols": [
            "BTCUSDT",
        ],
        # The four_chain strategy's ladder [1m,3m,5m,15m,1h] — all >= 1m, all
        # Binance-native, so warmup seeds 5000 bars per timeframe and the ws
        # feed streams them natively (no sub-minute derivation needed here).
        # If a sub-minute strategy is later added on crypto, extend this to the
        # union of all crypto strategies' ladder timeframes.
        "timeframes": ["1m", "3m", "5m", "15m", "1h"],
        "points": 5000,
        "baseline_timeframe": "1m",
        "transport": "ws",
    },
    "equity": {
        "symbols": [
            "AAPL"
        ],
        "timeframes": list(TWO_LADDER_TIMEFRAMES),
        "points": 5000,
        "baseline_timeframe": "5s",
        # 'transport' selects the live-data path for this class:
        #   "rest" (default) — the polling AssetFeed (PolygonStocksFeed, ...)
        #   "ws"             — the websocket AssetFeed (PolygonStocksWSFeed)
        # Absent key => "rest", so existing configs are unchanged. Only the
        # LIVE path differs; warmup and reconnect-backfill use the REST feed
        # either way. Migrate one class at a time; fall back to "rest" if a
        # ws feed misbehaves in early live trading. All three classes now
        # have ws feeds (equity/fx via Polygon clusters, crypto via Binance).
        # Note: crypto sub-minute rungs (5s/10s/30s) are DERIVED from the
        # Binance 1s kline and cannot be REST-backfilled on reconnect — they
        # self-heal live. See feeds/binance_ws for detail.
        "transport": "ws",
    },
    # "fx": {
    #     "symbols": [
    #         # e.g. "EURUSD", ...   ← FILL IN
    #     ],
    #     "timeframes": list(TWO_LADDER_TIMEFRAMES),
    #     "points": 5000,
    #     "baseline_timeframe": "5s",
    # },
}


def all_tracked_symbols() -> list[str]:
    """Every symbol across all classes — convenience for feed startup / health."""
    out: list[str] = []
    for cfg in ASSET_CONFIGS.values():
        out.extend(cfg.get("symbols", []))
    return out


# Valid live-data transports. "rest" = the polling feeds; "ws" = the
# websocket feeds. Kept as a module constant so feed_process and config
# validation agree on the allowed set.
VALID_TRANSPORTS = ("rest", "ws")

# Asset classes that actually HAVE a websocket feed today. A class not
# listed here must not be configured with transport="ws" — the feed
# builder would have nothing to instantiate.
WS_CAPABLE_CLASSES = ("equity", "fx", "crypto")


def transport_for(asset_class: str) -> str:
    """The configured live-data transport for a class: 'ws' or 'rest'.

    Defaults to 'rest' when the key is absent (existing configs unchanged).
    Raises on an unknown transport VALUE. Whether a class actually HAS a ws
    feed is a feed-construction concern, checked in feed_process._build_feeds
    (which raises RuntimeError, the codebase's startup-misconfig convention) —
    kept out of here so this stays a pure config-value accessor.
    """
    cfg = ASSET_CONFIGS.get(asset_class, {})
    transport = cfg.get("transport", "rest")
    if transport not in VALID_TRANSPORTS:
        raise ValueError(
            f"{asset_class}: unknown transport {transport!r}; "
            f"expected one of {VALID_TRANSPORTS}"
        )
    return transport

# ---------------------------------------------------------------------------
# Master trade-enable DEFAULT (execution/trade_enable.py).
# The boot/fallback state for live order placement. The trade_enabled DB row
# (migration 010) is the LIVE override that wins once running; this is only the
# starting state and the fallback if the row can't be read. False = boot with
# trading OFF (the safe default — go live deliberately, never by accident).
# ---------------------------------------------------------------------------
TRADE_ENABLED_DEFAULT = False

# ---------------------------------------------------------------------------
# STRATEGIES — the multi-strategy registry config (strategy/registry.py).
# Each entry: enabled, the symbols it trades ('*' = all tracked), the LADDERS
# it reads chains from (strategy/ladder_registry.py), and opaque params. A
# disabled strategy is never instantiated. The union of all enabled strategies'
# ladders is what the system computes (compute-once); shared ladders are
# computed once. Add a strategy: register its factory + add an entry here.
# ---------------------------------------------------------------------------
STRATEGIES = {
    "two_ladder": {
        "enabled": True,
        "symbols": "*",              # all tracked symbols
        "ladders": ["A", "B"],       # the original two-ladder design
        "params": {},
    },
    "four_chain": {
        "enabled": True,              # ladder is streamed + warmed (crypto config)
        "symbols": ["BTCUSDT"],
        "ladders": ["FC"],            # the [1m,3m,5m,15m,1h] ladder below
        "params": {"retention_hours": 3},
    },
    # Example of a future strategy on different ladders (disabled):
    # "mean_revert": {
    #     "enabled": False,
    #     "symbols": ["EURUSD"],
    #     "ladders": ["C"],           # e.g. a 1m/5m/1h ladder registered at startup
    #     "params": {"lookback": 20},
    # },
}

# Extra ladders to register beyond the built-in A and B (name -> timeframes).
# A strategy referencing a ladder name not here (and not A/B) fails at startup.
EXTRA_LADDERS = {
    "FC": ["1m", "3m", "5m", "15m", "1h"],   # four_chain strategy's ladder
}

