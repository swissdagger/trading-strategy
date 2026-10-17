"""vol_ratio = 24h realised vol / its own 14-day average, at the last COMPLETE hour.

Owner's file (strategy package). vol_state_from_closes and vol_ratio_from_closes are
VERBATIM the research code except the VolState import path. The original
fetch_closes() read a `prices` table this repo does not have (live bars are not
persisted); closes come from strategy/vol_source.py (the feeds' own 1m history,
32-day window, refreshed hourly) and are handed to these functions unchanged.

Matches the research code exactly (regime_feats.py / build_vr.py):
  * r = diff(log close) over consecutive stored 1m bars
  * hourly realised variance = sum(r^2) per clock hour (empty hours = 0)
  * vol24[h]  = sqrt(sum of the 24 hourly values ending at h) * 100     (min 12 hours)
  * ratio[h]  = vol24[h] / mean(vol24 over the 336 hours ending at h)   (min 72 hours)
  * evaluated at h = floor(now, hour) - 1 hour
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

LOOKBACK = timedelta(days=32)   # 30-day percentile (crypto) needs 31 days; 14-day average needs 17


def vol_state_from_closes(closes: pd.Series, now: datetime):
    """ratio (24h / 14-day avg), level (percentile of 24h vol in its last 30 days), short (4h / 24h).
    Same definitions as the research (regime_feats.py / build_vr.py)."""
    from strategy.rules import VolState
    h = pd.Timestamp(now).floor("h") - pd.Timedelta(hours=1)
    s = closes[closes.index < h + pd.Timedelta(hours=1)].dropna()
    if len(s) < 100:
        return None
    rv = (np.log(s).diff() ** 2).resample("h").sum()
    rv = rv.reindex(pd.date_range(rv.index.min(), h, freq="h"), fill_value=0.0)
    vol24 = np.sqrt(rv.rolling(24, min_periods=12).sum()) * 100
    vol4 = np.sqrt(rv.rolling(4, min_periods=2).sum() * 6) * 100
    ref = vol24.rolling(24 * 14, min_periods=24 * 3).mean()
    lvl = vol24.rolling(24 * 30, min_periods=24 * 5).apply(lambda w: (w[-1] >= w).mean(), raw=True)
    v, r, l4, L = (x.get(h, np.nan) for x in (vol24, ref, vol4, lvl))
    if not np.isfinite(v) or not np.isfinite(r) or r <= 0:
        return None
    return VolState(ratio=float(v / r), level=float(L) if np.isfinite(L) else None,
                    short=float(l4 / v) if np.isfinite(l4) and v > 0 else None)


def vol_ratio_from_closes(closes: pd.Series, now: datetime) -> Optional[float]:
    """closes: 1m close prices indexed by tz-aware bar timestamps, covering >= 17 days
    before `now` and nothing at or after the current hour."""
    h = pd.Timestamp(now).floor("h") - pd.Timedelta(hours=1)
    s = closes[closes.index < h + pd.Timedelta(hours=1)].dropna()
    if len(s) < 100:
        return None
    rv = (np.log(s).diff() ** 2).resample("h").sum()
    full = pd.date_range(rv.index.min(), h, freq="h")
    rv = rv.reindex(full, fill_value=0.0)
    vol24 = np.sqrt(rv.rolling(24, min_periods=12).sum()) * 100
    ref = vol24.rolling(24 * 14, min_periods=24 * 3).mean()
    v, r = vol24.get(h, np.nan), ref.get(h, np.nan)
    if not np.isfinite(v) or not np.isfinite(r) or r <= 0:
        return None
    return float(v / r)
