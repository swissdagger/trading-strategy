"""
research/earnings.py — earnings dates per symbol.

Two sources, both landing in research_earnings:

  * CSV import (works today, reproducible): `symbol,report_date[,time_of_day]`
    with dates ISO (2026-10-29) and time_of_day in {bmo, amc, unknown}.
    `python research_process.py --earnings-csv path.csv`.
  * A provider fetch (EarningsProvider protocol) on a daily timer, when one is
    configured. None is shipped: the right provider is an account decision
    (Polygon has no earnings-calendar endpoint in its core stocks API), and
    an unverified integration is worse than a CSV. Implement `fetch(symbols)`
    returning (symbol, date, time_of_day) tuples and pass it to the process.
"""
from __future__ import annotations

import csv
import logging
from datetime import date
from pathlib import Path
from typing import Awaitable, Callable, Iterable, List, Protocol, Tuple

log = logging.getLogger("research.earnings")

EarningsRow = Tuple[str, date, str]   # (symbol, report_date, time_of_day)
_TOD = {"bmo", "amc", "unknown"}


class EarningsProvider(Protocol):
    name: str
    async def fetch(self, symbols: List[str]) -> List[EarningsRow]: ...


def parse_earnings_csv(path: str | Path) -> List[EarningsRow]:
    out: List[EarningsRow] = []
    with open(path, newline="") as f:
        reader = csv.reader(f)
        for i, raw in enumerate(reader, 1):
            if not raw or raw[0].strip().startswith("#"):
                continue
            if i == 1 and raw[0].strip().lower() == "symbol":
                continue   # header
            if len(raw) < 2:
                raise ValueError(f"{path}:{i}: need symbol,report_date[,time_of_day]")
            sym = raw[0].strip().upper()
            d = date.fromisoformat(raw[1].strip())
            tod = (raw[2].strip().lower() if len(raw) > 2 and raw[2].strip() else "unknown")
            if tod not in _TOD:
                raise ValueError(f"{path}:{i}: time_of_day {tod!r} not in {sorted(_TOD)}")
            out.append((sym, d, tod))
    return out


async def import_csv(path: str | Path,
                     upsert: Callable[[Iterable[tuple]], Awaitable[int]]) -> int:
    rows = parse_earnings_csv(path)
    n = await upsert([(s, d, t, f"csv:{Path(path).name}") for (s, d, t) in rows])
    log.info("earnings: imported %d row(s) from %s", n, path)
    return n


async def fetch_provider(provider: EarningsProvider, symbols: List[str],
                         upsert: Callable[[Iterable[tuple]], Awaitable[int]]) -> int:
    rows = await provider.fetch(symbols)
    n = await upsert([(s, d, t, provider.name) for (s, d, t) in rows])
    log.info("earnings: %d row(s) from %s", n, provider.name)
    return n
