"""
producer/service2_utils.py — Sumtyme marshalling helpers, extracted from
service_2_sumtyme_bridge.

Three pure functions the chain machine injects. Faithful extractions of the
CausaView originals (prepare_dataframe_for_sumtyme, _to_sumtyme_interval) plus the
buffer->DataFrame builder that service_2 constructed inline. No CausaView import —
this is a standalone copy, carrying the same sync-obligation as the state machine:
if service_2's marshalling changes, change it here too (breadcrumb at both ends).

Requires pandas (add to requirements.txt) — same dependency service_2 has.
"""
from __future__ import annotations

from datetime import timezone
from typing import Iterable, Tuple

import pandas as pd


def build_sumtyme_df(buffer: Iterable[dict]) -> pd.DataFrame:
    """Turn a buffer of tick dicts into the DataFrame prepare_dataframe_for_sumtyme
    expects. Tick dicts carry symbol, timestamp (epoch ms), open/high/low/close,
    timeframe, asset_class (the shape service2_machine._tick_dict produces). The
    columns prepare_* needs are timestamp, symbol, OHLC, timeframe."""
    return pd.DataFrame(list(buffer))


def to_sumtyme_interval(timeframe: str) -> Tuple[int, str]:
    """Convert a canonical timeframe ('12h', '30s') to the (interval, interval_unit)
    Sumtyme accepts. Faithful to service_2's _to_sumtyme_interval.

    Sumtyme accepts 'milliseconds' | 'seconds' | 'minutes' | 'days' — NOT 'hours'
    (returns 422). Hours are mapped to minutes (x60); the duration is unchanged.
    Raises KeyError on an unrecognised unit so misconfiguration fails loudly rather
    than silently mis-forecasting.
    """
    multiplier = int(timeframe[:-1])
    unit_char = timeframe[-1]
    if unit_char == "s":
        return multiplier, "seconds"
    if unit_char == "m":
        return multiplier, "minutes"
    if unit_char == "h":
        return multiplier * 60, "minutes"   # Sumtyme rejects 'hours'
    if unit_char == "d":
        return multiplier, "days"
    raise KeyError(f"Unrecognised timeframe unit {unit_char!r} in {timeframe!r}")


def prepare_dataframe_for_sumtyme(df: pd.DataFrame, timeframe: str):
    """Build the dataframe Sumtyme's forecast_path expects, in STEP mode.

    Faithful extraction of service_2's prepare_dataframe_for_sumtyme. Sumtyme
    requires 5001 rows (5000 real + 1 placeholder) and step-mode indexing
    (timeframe_dependent=False at the call site) so the column is `step`
    (integer 1..5001) rather than `datetime`. Step mode treats the series as an
    ordered sequence of morphisms, so session gaps (overnight/weekend/illiquid)
    don't corrupt the state-space reconstruction the way wall-clock mode would.

    Returns (api_df, placeholder_dt). placeholder_dt is the datetime the next bar
    WOULD be at; the caller persists the resulting raw signal against it, since the
    forecast response is keyed by step number (e.g. {"5001": -1}) and carries no
    datetime back.
    """
    api_df = df.copy()
    if "timestamp" in api_df.columns:
        api_df = api_df.drop(columns=["timestamp"])

    # Placeholder's wall-clock datetime (not sent to Sumtyme in step mode; needed
    # locally to persist the signal against the right timestamp).
    multiplier = int(timeframe[:-1])
    unit = timeframe[-1]
    unit_to_ms = {"s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}
    delta_ms = multiplier * unit_to_ms.get(unit.lower(), 60_000)
    last_raw_ts = df["timestamp"].iloc[-1]
    next_ts_ms = last_raw_ts + delta_ms
    placeholder_dt = (
        pd.to_datetime(next_ts_ms, unit="ms").to_pydatetime().replace(tzinfo=timezone.utc)
    )

    placeholder_row = pd.DataFrame([{
        "symbol": api_df["symbol"].iloc[0],
        "open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0,
        "timeframe": timeframe,
    }])

    api_df = pd.concat([api_df, placeholder_row], ignore_index=True)
    # 1-indexed step column per Sumtyme's docs (rows 1..5000 real, 5001 placeholder).
    api_df["step"] = range(1, len(api_df) + 1)
    return api_df, placeholder_dt
