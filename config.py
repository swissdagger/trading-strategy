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
    # "crypto": {
    #     "symbols": [
    #         "BTCUSDT",
    #     ],
    #     "timeframes": list(TWO_LADDER_TIMEFRAMES),
    #     "points": 5000,
    #     "baseline_timeframe": "5s",
    # },
    "equity": {
        "symbols": [
            "AAPL"
        ],
        "timeframes": list(TWO_LADDER_TIMEFRAMES),
        "points": 5000,
        "baseline_timeframe": "5s",
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
