"""
feeds — Per-source asset data feeds.

Each concrete feed implements the AssetFeed interface in base.py. The feed
process instantiates one feed per asset class enabled in ASSET_CONFIGS and
treats them uniformly through that interface.

Adding a new source means writing one file in this package and one
entry in ASSET_CONFIGS — no changes to Services 2 or 3.

Crypto: CoinbaseSpotFeed (REST). Equity: PolygonStocksFeed (REST) and
PolygonStocksWSFeed (websocket, falling back to REST). base.py holds the
interface and the market-hours plumbing (is_market_open, exchange_calendars).
"""
from .base import AssetFeed, is_market_open
from .coinbase_spot import CoinbaseSpotFeed

__all__ = ["AssetFeed", "is_market_open", "CoinbaseSpotFeed"]
