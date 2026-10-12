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

    # Send ONLY the columns Sumtyme's step-mode contract calls for.
    #
    # This used to drop `timestamp` alone, leaving `symbol`, `timeframe` AND
    # `asset_class` in the payload — as full strings, repeated on all 5001 rows.
    # Three problems, all of which showed up as a SLOW API rather than an error:
    #
    #  1. asset_class is not part of the contract at all. It is in _tick_dict
    #     because the DB schema denormalises it onto every bar; it was never meant
    #     to reach Sumtyme. (This module's own docstring lists the needed columns
    #     as "timestamp, symbol, OHLC, timeframe" — asset_class is not among them.
    #     It arrived when the buffer shape changed and the marshalling was not
    #     re-checked. CausaView's service_2 never sent it, which is why CausaView
    #     runs 30 assets on 1m with no trouble and this did not.)
    #
    #  2. Worse: the placeholder row below was built WITHOUT asset_class, so
    #     pd.concat introduced a NaN in that column on row 5001 — the placeholder,
    #     i.e. THE ROW BEING FORECAST. We were handing the API a null cell in an
    #     undocumented column on the one row that matters. It did not reject this;
    #     it ingested it and got slow (observed: p50 = 10.3s per call, zero errors).
    #
    #  3. symbol/timeframe are constant across every row and redundant in step
    #     mode (the series is an ordered sequence of morphisms — Sumtyme is told
    #     which instrument and interval via the interval/interval_unit args). 5001
    #     repeated copies of "BTC-USD"/"1m"/"crypto" nearly DOUBLED the payload:
    #     664KB -> 369KB per call once removed.
    #
    # Keep OHLC (the actual data) and nothing else. The step column is added below.
    _CONTRACT_COLUMNS = ["open", "high", "low", "close"]
    missing = [c for c in _CONTRACT_COLUMNS if c not in api_df.columns]
    if missing:
        # Loud, not silent: a missing OHLC column would otherwise be forecast as
        # garbage.
        raise KeyError(
            f"buffer is missing required OHLC column(s) {missing}; "
            f"got {list(api_df.columns)}"
        )

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

    # The placeholder row: the step Sumtyme is being asked to forecast. It must be
    # STRUCTURALLY IDENTICAL to the real rows — same columns, no NaN. Previously it
    # was built with a different column set from the real rows, so pd.concat
    # silently introduced a NaN (in asset_class) on precisely this row. Building
    # both from the same _CONTRACT_COLUMNS list makes that class of bug impossible.
    placeholder_row = pd.DataFrame([{
        "open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0,
    }])

    api_df = api_df[_CONTRACT_COLUMNS]
    api_df = pd.concat([api_df, placeholder_row], ignore_index=True)

    # Fail loudly if anything is still null — a NaN in the payload is what made the
    # API pathologically slow, and it must never silently recur.
    if api_df.isna().any().any():
        bad = api_df.columns[api_df.isna().any()].tolist()
        raise ValueError(
            f"NaN in Sumtyme payload columns {bad} — refusing to send. "
            f"(A null in the placeholder row previously caused ~10s API latency.)"
        )

    # 1-indexed step column per Sumtyme's docs (rows 1..5000 real, 5001 placeholder).
    api_df["step"] = range(1, len(api_df) + 1)
    return api_df, placeholder_dt