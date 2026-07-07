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
            "ETHUSDT",
            "SOLUSDT",
            "WLDUSDT",
            "BNBUSDT",
            "XRPUSDT",
            "HYPEUSDT",
            "DOGEUSDT",
            "ADAUSDT",
            "LINKUSDT",
        ],
        # four_chain's ladder [1m,3m,5m,15m,1h] — all >= 1m, all Binance-native.
        # Served by CONTINUOUS REST polling (BinanceFuturesFeed.start_live runs a
        # per-timeframe poll loop at ~tf/2 cadence, closed-window-only, relaunched
        # on error by feed_process). transport is 'rest' NOT 'ws': the Binance
        # futures WEBSOCKET (fstream) is unreachable from this host — Binance
        # silences futures market data to cloud/datacenter egress IPs, so the ws
        # connects but delivers zero frames. REST (fapi) is reachable and is all
        # four_chain needs (no sub-minute rungs here). Sub-minute crypto would
        # require the ws and is therefore not available from this host — two_ladder
        # is scoped off crypto for that reason (see non_crypto_symbols).
        "timeframes": ["1m", "3m", "5m", "15m", "1h"],
        "points": 5000,
        "baseline_timeframe": "1m",
        "transport": "rest",
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


def non_crypto_symbols() -> list[str]:
    """Every tracked symbol EXCEPT crypto ones.

    two_ladder runs a SUB-MINUTE ladder (5s/10s/30s in Set A/B). Crypto
    sub-minute bars can only come from the Binance futures WEBSOCKET (REST
    klines bottom out at 1m), and that websocket is unreachable from this
    host's cloud egress IP — Binance silences futures market data to
    datacenter IPs. So two_ladder cannot function on crypto here regardless
    of code. Scoping two_ladder to non-crypto symbols keeps it on the
    equity/fx venues where sub-minute data is available, while still
    auto-including any NEW equity/fx symbol added later (the reason this is a
    helper, not a hardcoded list). Crypto runs only four_chain, whose ladder
    is 1m+ and served fine by continuous REST polling.
    """
    out: list[str] = []
    for asset_class, cfg in ASSET_CONFIGS.items():
        if asset_class == "crypto":
            continue
        out.extend(cfg.get("symbols", []))
    return out


def crypto_symbols() -> list[str]:
    """Every tracked CRYPTO symbol.

    four_chain runs only on crypto (its [1m,3m,5m,15m,1h] ladder is served by
    Binance REST polling; see the crypto ASSET_CONFIGS note). Deriving the
    strategy's universe from ASSET_CONFIGS here — rather than a second hardcoded
    list in STRATEGIES — keeps the traded set in lockstep with the FETCHED set:
    a coin added to ASSET_CONFIGS['crypto'] auto-enrolls in four_chain, and a
    symbol can never be traded without also being fed. Mirrors non_crypto_symbols
    for two_ladder.
    """
    return list(ASSET_CONFIGS.get("crypto", {}).get("symbols", []))


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
# Master trade MODE default (execution/trade_enable.py). Three states:
#   "off"   — place no orders.
#   "paper" — place orders against the PAPER venue (fake money): the shakeout.
#   "live"  — place orders against the LIVE venue (real money).
# The trade_enabled DB row's `mode` (migration 012) is the LIVE override that
# wins once running; this is only the boot/fallback mode. Default "off" — go to
# paper or live deliberately, never by accident.
# ---------------------------------------------------------------------------
TRADE_MODE_DEFAULT = "off"

# Per-venue PAPER and LIVE connection endpoints. The adapter factory
# (execution/adapter_factory.py) picks by mode: paper mode -> paper endpoint
# (IB gateway 4002), live mode -> live endpoint (4001). FILL IN real accounts.
VENUE_ENDPOINTS = {
    "ibkr": {
        "paper": {
            "host": "127.0.0.1",
            "port": 4002,               # IB Gateway PAPER
            "client_id": 1,
            "account": "",              # paper account (DU...)
        },
        "live": {
            "host": "127.0.0.1",
            "port": 4001,               # IB Gateway LIVE
            "client_id": 1,
            "account": "",              # live account (U...)
        },
    },
    "binance": {
        # Paper = Binance USD-M futures TESTNET (demo-fapi). Live = production.
        # Fill in API keys per environment (env vars recommended over literals).
        "paper": {
            "api_key": "",
            "api_secret": "",
            "base_url": "https://demo-fapi.binance.com",
        },
        "live": {
            "api_key": "",
            "api_secret": "",
            "base_url": "https://fapi.binance.com",
        },
    },
}

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
        # Non-crypto only: two_ladder's sub-minute rungs (5s/10s/30s) can't be
        # sourced for crypto from this host (Binance futures ws is IP-blocked;
        # REST klines don't go sub-minute). Resolved just below the dict to the
        # current non-crypto universe so new equity/fx symbols auto-join.
        "symbols": [],               # set to non_crypto_symbols() below
        "ladders": ["A", "B"],       # the original two-ladder design
        "params": {},
    },
    "four_chain": {
        "enabled": True,              # ladder is streamed + warmed (crypto config)
        # Crypto-only: resolved just below the dict to the current crypto
        # universe so new crypto symbols auto-join (mirrors two_ladder).
        "symbols": [],                # set to crypto_symbols() below
        "ladders": ["FC"],            # the [1m,3m,5m,15m,1h] ladder below
        # No time-based retention (four_chain's pattern span is unbounded); the
        # ChainHistoryView is bounded only by a per-symbol count cap. Override it
        # here with 'max_chains_per_symbol' if a symbol ever approaches the
        # default; omitted => the view's default cap.
        "params": {},
    },
    # Example of a future strategy on different ladders (disabled):
    # "mean_revert": {
    #     "enabled": False,
    #     "symbols": ["EURUSD"],
    #     "ladders": ["C"],           # e.g. a 1m/5m/1h ladder registered at startup
    #     "params": {"lookback": 20},
    # },
}

# two_ladder trades every non-crypto tracked symbol. Assigned here (not in the
# literal above) because non_crypto_symbols() is defined earlier in the module
# but the STRATEGIES literal can't reference config values mid-definition
# cleanly; this keeps the crypto-exclusion knowledge in config.py rather than
# leaking a marker string into the strategy registry.
STRATEGIES["two_ladder"]["symbols"] = non_crypto_symbols()
STRATEGIES["four_chain"]["symbols"] = crypto_symbols()

# Extra ladders to register beyond the built-in A and B (name -> timeframes).
# A strategy referencing a ladder name not here (and not A/B) fails at startup.
EXTRA_LADDERS = {
    "FC": ["1m", "3m", "5m", "15m", "1h"],   # four_chain strategy's ladder
}