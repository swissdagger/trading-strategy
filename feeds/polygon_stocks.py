"""
feeds/polygon_stocks.py — Polygon US cash-equity aggregates as an AssetFeed.

Restored for the equities dense ladder (PR 9). Built on _PolygonAggsFeedBase
(feeds/polygon_aggs.py), which holds the fetch/poll mechanics and the three
source rules (trades for seconds-rungs, 1-minute-derived for 2m/3m, Polygon
buckets for the rest).

What's equity-specific:

1. **Canonical symbol form is the native form.** US equity tickers (`AAPL`)
   need no prefix — to_native_symbol is identity.

2. **Session gating.** is_market_open('equity') is the extended-hours
   envelope (04:00–20:00 ET on XNYS session days) defined in feeds/base.py.
   Warmup's session walk and the live poller use the same definition, which
   is load-bearing: in step mode the bar sequence IS the definition of a step.

3. **Static universe, no discovery.** Symbols come straight from
   ASSET_CONFIGS["equity"]["symbols"].

4. **Partial ladders are expected for young listings.** A coarse rung on a
   recent IPO accumulates toward WINDOW_SIZE and starts forecasting when it
   gets there. No gate, no error — the stream is simply not yet forecastable.
"""
from __future__ import annotations

from typing import Set

from feeds.polygon_aggs import _PolygonAggsFeedBase


# Every label the dense equity ladder (config.EXTRA_LADDERS["DL"]) uses, plus
# nearby labels for experimentation. Supporting a label and configuring it
# are different decisions.
_SUPPORTED_TIMEFRAMES: Set[str] = {
    # Active ladder (DL)
    "30s", "45s", "1m", "90s", "2m", "3m", "15m", "30m", "1h", "2h", "4h", "6h",
    # Compat
    "5s", "10s", "15s", "5m", "10m", "8h", "12h", "1d",
}


class PolygonStocksFeed(_PolygonAggsFeedBase):
    """Polygon US cash-equity aggregates. `symbols` are exchange
    tickers used verbatim as Polygon tickers.
    """

    asset_class = "equity"
    name = "polygon_stocks"

    SUPPORTED_TIMEFRAMES = _SUPPORTED_TIMEFRAMES

    def to_native_symbol(self, symbol: str) -> str:
        return symbol