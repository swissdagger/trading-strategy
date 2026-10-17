"""
strategy/vol_source.py — where the chain rules' volatility filters get 1m closes.

vol.py's research functions need up to 32 days of 1m closes per symbol
(14-day average for vol_ratio; 30-day percentile for the crypto squeeze
filter), ending at the last COMPLETE hour. CausaView read them from its
`prices` table; this repo does not persist live bars, so the closes come from
the same feeds that serve the ladders: Polygon 1-minute aggregates for
equities, Coinbase candles for crypto. Both expose
fetch_history(symbol, '1m', n) -> ascending OHLCPoints, which is all this needs.

  * FeedClosesSource — per symbol, an in-memory Series of 1m closes. Seeded
    once with LOOKBACK of history, topped up with a small forward fetch at
    most once per clock hour, trimmed to LOOKBACK. The window the rules
    evaluate ends at floor(now, h) − 1h, so hourly is exactly enough.
  * VolStateProvider — vol_state_from_closes cached per (symbol, evaluation
    hour). The rule's vol_fn is SYNCHRONOUS; the adapter awaits
    prefetch(symbol, now) before handing an entry-level event to the rule so
    the sync lookup is a hit. A failed fetch yields None and the rule skips
    with its own "no vol history".
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Optional, Tuple

import pandas as pd

from strategy.vol import LOOKBACK, vol_state_from_closes

log = logging.getLogger("strategy.vol_source")


def _floor_hour(now: datetime) -> pd.Timestamp:
    return pd.Timestamp(now).tz_convert("UTC").floor("h")


class FeedClosesSource:
    def __init__(self, feed, *, lookback=LOOKBACK, refill_slack_bars: int = 90):
        self._feed = feed
        self._lookback = pd.Timedelta(lookback)
        self._slack = refill_slack_bars
        self._closes: Dict[str, pd.Series] = {}
        self._refreshed_hour: Dict[str, pd.Timestamp] = {}

    async def closes(self, symbol: str, now: datetime) -> pd.Series:
        hour = _floor_hour(now)
        if self._refreshed_hour.get(symbol) != hour:
            await self._refresh(symbol, hour)
        s = self._closes.get(symbol)
        if s is None or s.empty:
            return pd.Series(dtype=float)
        return s[(s.index >= hour - self._lookback) & (s.index < hour)]

    def latest_close(self, symbol: str) -> Optional[float]:
        s = self._closes.get(symbol)
        return None if s is None or s.empty else float(s.iloc[-1])

    async def _refresh(self, symbol: str, hour: pd.Timestamp) -> None:
        have = self._closes.get(symbol)
        if have is None or have.empty:
            n, what = int(self._lookback.total_seconds() // 60), "seed"
        else:
            n = int((hour - have.index[-1]).total_seconds() / 60.0) + self._slack
            what = "refill"
        try:
            bars = await self._feed.fetch_history(symbol, "1m", n)
        except Exception:
            log.exception("vol closes %s for %s failed (filter will be None this hour)", what, symbol)
            return
        if not bars:
            log.warning("vol closes %s for %s returned no bars", what, symbol)
            self._refreshed_hour[symbol] = hour
            return
        idx = pd.DatetimeIndex([pd.Timestamp(b.timestamp, unit="ms", tz="UTC") for b in bars])
        new = pd.Series([float(b.close) for b in bars], index=idx)
        merged = new if have is None or have.empty else pd.concat([have, new])
        merged = merged[~merged.index.duplicated(keep="last")].sort_index()
        merged = merged[merged.index >= hour - self._lookback]
        self._closes[symbol] = merged
        self._refreshed_hour[symbol] = hour
        log.info("vol closes %s for %s: %d bars (%s → %s)", what, symbol, len(merged),
                 merged.index[0], merged.index[-1])


class VolStateProvider:
    def __init__(self, source: FeedClosesSource):
        self._source = source
        self._cache: Dict[Tuple[str, pd.Timestamp], object] = {}

    @staticmethod
    def eval_hour(now: datetime) -> pd.Timestamp:
        return _floor_hour(now) - pd.Timedelta(hours=1)

    async def prefetch(self, symbol: str, now: datetime):
        key = (symbol, self.eval_hour(now))
        if key in self._cache:
            return self._cache[key]
        closes = await self._source.closes(symbol, now)
        state = vol_state_from_closes(closes, now) if not closes.empty else None
        self._cache[key] = state
        for k in [k for k in self._cache if k[0] == symbol and k != key]:
            del self._cache[k]
        return state

    def get(self, symbol: str, now: datetime):
        """The rules' SYNC vol_fn: VolState, or None (= no history, skip)."""
        return self._cache.get((symbol, self.eval_hour(now)))
