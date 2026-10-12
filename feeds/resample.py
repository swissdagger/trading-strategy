"""
feeds/resample.py — timeframe parsing and closed-window OHLC resampling.

Used by the REST feeds' history path (coinbase_spot: derived rungs such as 3m
from 1m). Moved here from the deleted feeds/polygon_options.py
so a crypto feed no longer imports an options feed for two pure helpers.
feeds/bar_aggregator.py is the live-path counterpart and applies the same
bucketing rule incrementally.
"""
from __future__ import annotations


# Unit -> (Polygon-style timespan name, ms per unit). The timespan name is
# vestigial (no caller uses it today) but kept so _parse_timeframe's 3-tuple
# return shape is unchanged for existing call sites.
_UNIT_MAP = {"s": "second", "m": "minute", "h": "hour", "d": "day"}
_MS_MAP   = {"s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}


def _parse_timeframe(timeframe: str):
    """Return (multiplier, polygon_timespan, candle_ms)."""
    multiplier = int(timeframe[:-1])
    unit = timeframe[-1].lower()
    return multiplier, _UNIT_MAP.get(unit, "minute"), _MS_MAP.get(unit, 60_000)


def _resample_to_timeframe(ticks, timeframe: str) -> list:
    """Re-aggregate raw bars into uniform timeframe-aligned windows.
    Returns a list of {t, o, h, l, c} dicts, sorted ascending by timestamp.

    Why this exists: a venue does not always return bars at the requested
    spacing (sparse markets skip empty windows; derived rungs such as 3m-from-1m
    have no native endpoint). Sumtyme's wall-clock-anchored mode
    rejects non-uniform spacing (and is the wrong tool for this
    problem anyway — see Service 2's step-mode rationale). To
    satisfy the "bars labelled '5m' must be 5 minutes apart" contract
    that Sumtyme's interval/interval_unit imposes, we re-aggregate
    here before returning anything to the seeder.

    Aggregation rule (standard OHLC bucketing):
      - window_start = floor(t / tf_ms) * tf_ms
      - open  = first bar's open  (lowest t in the window)
      - high  = max of all highs in the window
      - low   = min of all lows in the window
      - close = last bar's close  (highest t in the window)
      - t     = window_start

    Windows with no source bars (no trades during that 5-minute
    period) produce NO output row. This creates gaps in the returned
    series — exactly the gaps that Service 2's step-mode call is
    designed to handle. The alternative (forward-filling with
    synthetic flat bars) would corrupt Sumtyme's input with bars
    that don't reflect real trading; gaps preserve the "same input
    → same output" guarantee.
    """
    if not ticks:
        return []
    multiplier, _, base_ms = _parse_timeframe(timeframe)
    # _parse_timeframe returns base ms per UNIT, not per timeframe.
    # `5m` → (5, 'minute', 60_000). Multiply to get the actual window size.
    tf_ms = multiplier * base_ms

    # Bucket by window-start. Use a dict so we don't need to pre-sort.
    by_window: dict = {}
    for tick in ticks:
        t = tick["t"]
        window_start = (t // tf_ms) * tf_ms
        bucket = by_window.get(window_start)
        if bucket is None:
            by_window[window_start] = {
                "t":         window_start,
                "o":         tick["o"],
                "h":         tick["h"],
                "l":         tick["l"],
                "c":         tick["c"],
                "_first_t":  t,
                "_last_t":   t,
            }
        else:
            bucket["h"] = max(bucket["h"], tick["h"])
            bucket["l"] = min(bucket["l"], tick["l"])
            if t < bucket["_first_t"]:
                bucket["o"] = tick["o"]
                bucket["_first_t"] = t
            if t > bucket["_last_t"]:
                bucket["c"] = tick["c"]
                bucket["_last_t"] = t

    # Strip the bookkeeping fields and return sorted ascending.
    out = []
    for ws in sorted(by_window):
        b = by_window[ws]
        out.append({"t": b["t"], "o": b["o"], "h": b["h"], "l": b["l"], "c": b["c"]})
    return out
