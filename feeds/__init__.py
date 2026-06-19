"""
feeds — Per-source asset data feeds.

Each concrete feed (PolygonOptionsFeed, BinanceFuturesFeed) implements
the AssetFeed interface in base.py. Service 1 instantiates one feed per
asset class enabled in ASSET_CONFIGS and treats them uniformly through
that interface.

Adding a new source means writing one file in this package and one
entry in ASSET_CONFIGS — no changes to Services 2 or 3.

Phase 6: PolygonForexFeed and the legacy feed.py module are deleted as
part of the forex sunset. The two live feeds are crypto (Binance
Futures) and option (Polygon options + contract_discovery).
"""
from .base import AssetFeed, is_market_open
from .binance_futures import BinanceFuturesFeed
from .polygon_stocks import PolygonStocksFeed
from .polygon_forex import PolygonForexFeed

__all__ = ["AssetFeed", "is_market_open", "BinanceFuturesFeed",
           "PolygonStocksFeed", "PolygonForexFeed"]
