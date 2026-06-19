"""
feeds/polygon_forex.py — Polygon forex aggregates as an AssetFeed.

Phase 7 reinstatement. This is a NEW implementation on the shared
_PolygonAggsFeedBase (feeds/polygon_aggs.py), not a resurrection of
the pre-Phase-6 file — the old feed predated the AssetFeed cursor
contract, the resample-to-uniform-windows rule, and the closed-window
emission invariant that the options feed established. Do not restore
the deleted file from git history.

What's fx-specific (everything else lives in the base):

1. **Canonical symbol form is the bare pair.** The DB and frontend
   see `EURUSD`; Polygon's native ticker is `C:EURUSD`. This matches
   the pre-Phase-6 DB convention so the archived fx rows and the new
   streams share a symbol namespace (they don't collide — different
   timeframes, and SymbolsCache's recency window hides the archive).

2. **24/5 session.** is_market_open('fx') gates the live poll to the
   Sun 22:00 → Fri 22:00 UTC trading week; the poller sleeps through
   the weekend gap rather than burning budget on empty fetches.

3. **Inverse/indicative pairs may not exist.** The configured list
   includes non-convention quotes (USDGBP, USDCNY) and an NDF-ish
   pair (USDKRW). fetch_history returning zero rows for a pair is
   log-and-skip at the seeder, not fatal — see the base class's
   fetch_history docstring for the operator workflow.
"""
from __future__ import annotations

from typing import Set

from feeds.polygon_aggs import _PolygonAggsFeedBase


# Every label the 12-level Phase 7 fx ladder uses, plus the coarser
# legacy labels (pre-Phase-6 fx ran 1m+) so historical tooling can
# still construct the feed against archived configs.
_SUPPORTED_TIMEFRAMES: Set[str] = {
    # Active ladder: TIMEFRAMES_FX in config.py
   "1m", "5m", "15m",
    "30m", "1h", "2h", "4h", "6h", "12h",
    # Compat
    "5s", "10s", "15s", "30s", "2m", "3m", "10m", "8h", "1d",
}


class PolygonForexFeed(_PolygonAggsFeedBase):
    """Polygon forex aggregates. `symbols` are bare 6-letter pairs
    (`EURUSD`); to_native_symbol adds the `C:` prefix per-request.
    """

    asset_class = "fx"
    name = "polygon_forex"

    SUPPORTED_TIMEFRAMES = _SUPPORTED_TIMEFRAMES

    def to_native_symbol(self, symbol: str) -> str:
        # Defensive: accept an already-prefixed ticker without
        # double-prefixing (mirrors the options feed's O: handling).
        return symbol if symbol.startswith("C:") else f"C:{symbol}"