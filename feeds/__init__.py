"""
feeds — Per-source asset data feeds.

Each concrete feed implements the AssetFeed interface in base.py. The feed
process instantiates one feed per asset class enabled in ASSET_CONFIGS and
treats them uniformly through that interface.

Adding a new source means writing one file in this package and one
entry in ASSET_CONFIGS — no changes to Services 2 or 3.

ADR-001 Phase 1: crypto-only. The Polygon equity/fx/options feeds and
contract_discovery are deleted; base.py (incl. is_market_open and the
exchange_calendars plumbing) is kept for future equities. The live crypto
feed is BinanceFuturesFeed until the Coinbase spot feed lands (Phase 2).
"""
from .base import AssetFeed, is_market_open
from .binance_futures import BinanceFuturesFeed

__all__ = ["AssetFeed", "is_market_open", "BinanceFuturesFeed"]
