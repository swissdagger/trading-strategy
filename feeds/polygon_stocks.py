"""
feeds/polygon_stocks.py — Polygon US cash-equity aggregates as an
AssetFeed.

Phase 7. Built on the shared _PolygonAggsFeedBase
(feeds/polygon_aggs.py) alongside the reinstated forex feed.

What's equity-specific (everything else lives in the base):

1. **Canonical symbol form is the native form.** US equity tickers
   (`AAPL`, `BCS`) need no prefix — to_native_symbol is identity.

2. **Cash-session gating.** is_market_open('equity') shares the
   options branch (13:30–20:00 UTC weekdays) — the equity cash
   session IS the session the tracked options trade against, and the
   single shared branch in feeds/base.py guarantees they never drift.

3. **Static universe, no discovery.** Unlike options, an equity
   ticker doesn't expire, so there is no contract_discovery analogue
   and no option_contracts-style table. Service 1 seeds and polls
   straight from ASSET_CONFIGS["equity"]["symbols"] via the generic
   iter_streams() / start_live paths — the same shape as crypto.

4. **Partial ladders are expected for young listings.** SPCX (IPO
   June 2026) can only fill its lowest timeframes' 5000-bar buffers
   for now; coarser streams accumulate toward WINDOW_SIZE and start
   forecasting whenever they get there. No gate, no error — the
   stream is simply not yet forecastable.
"""
from __future__ import annotations

from typing import Set

from feeds.polygon_aggs import _PolygonAggsFeedBase


# Every label the 10-level equity ladder uses (TIMEFRAMES_EQUITY in
# config.py) plus nearby labels for compat/experimentation. 6h/12h
# are deliberately ABSENT from the active ladder (see the buffer
# arithmetic in config.py) but supported here — supporting a label
# and configuring it are different decisions.
_SUPPORTED_TIMEFRAMES: Set[str] = {
    # Active ladder
    "1m", "5m", "15m",
    "30m", "1h", "2h", "4h",
    # Compat
    "5s", "10s", "15s", "30s", "2m", "3m", "10m", "6h", "8h", "12h", "1d",
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