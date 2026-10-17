"""
research/polygon_research.py — the two Polygon reads the research process makes.

Its OWN httpx client and RateLimiter, deliberately not shared with the feed's:
the feed process and this one are different processes anyway, and Polygon
meters them against the same key, so this limiter is sized small (the
collection is a few requests an hour plus one per signal).

  nbbo_asof(symbol, ts)   GET /v3/quotes/{ticker}?timestamp.lte=<ns>&order=desc&limit=1
                          -> the last NBBO at or before ts. Historical, so it
                          is the quote AT decision time, reproducible, and
                          backfillable for signals written before this ran.
  bars_1m(symbol, lo, hi) GET /v2/aggs/ticker/{ticker}/range/1/minute/{lo}/{hi}
                          -> 1m bars WITH v (volume), vw (VWAP), n (trade count).
                          The feed discards those three; the chain machine
                          needs only OHLC and the Sumtyme payload allowlist
                          must never see them.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import httpx

from producer.bar import RateLimiter

log = logging.getLogger("research.polygon")

BASE = "https://api.polygon.io"
# How far back from the as-of time to look for a quote. A quote older than
# this is not "the quote at decision time"; record no_quote instead.
QUOTE_LOOKBACK = timedelta(minutes=30)


@dataclass
class Quote:
    symbol: str
    quote_ts: datetime          # SIP timestamp, UTC
    bid: float
    ask: float
    bid_size: float
    ask_size: float

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread_bps(self) -> Optional[float]:
        return (self.ask - self.bid) / self.mid * 1e4 if self.mid > 0 else None


@dataclass
class Bar1m:
    symbol: str
    timestamp_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float]
    vwap: Optional[float]
    trade_count: Optional[int]


class PolygonResearchClient:
    def __init__(self, api_key: str, *, requests_per_second: float = 4.0,
                 max_concurrent: int = 2, client: Optional[httpx.AsyncClient] = None):
        self.api_key = api_key
        self.limiter = RateLimiter(requests_per_second, max_concurrent)
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0))

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _get(self, url: str, params: dict) -> dict:
        params = {**params, "apiKey": self.api_key}
        async with self.limiter:
            resp = await self._client.get(url, params=params)
        if resp.status_code == 429:
            raise RuntimeError("polygon 429 (rate limited)")
        if resp.status_code in (401, 403):
            raise PermissionError(f"polygon {resp.status_code}: {resp.text[:200]}")
        resp.raise_for_status()
        return resp.json()

    async def nbbo_asof(self, symbol: str, ts: datetime) -> Optional[Quote]:
        """Last NBBO at or before ts, within QUOTE_LOOKBACK; None if none."""
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        lte_ns = int(ts.timestamp() * 1e9)
        gte_ns = int((ts - QUOTE_LOOKBACK).timestamp() * 1e9)
        data = await self._get(f"{BASE}/v3/quotes/{symbol}", {
            "timestamp.lte": lte_ns, "timestamp.gte": gte_ns,
            "order": "desc", "limit": 1, "sort": "timestamp",
        })
        results = data.get("results") or []
        if not results:
            return None
        r = results[0]
        t_ns = r.get("sip_timestamp") or r.get("participant_timestamp")
        bid, ask = r.get("bid_price"), r.get("ask_price")
        if t_ns is None or bid is None or ask is None:
            return None
        return Quote(
            symbol=symbol,
            quote_ts=datetime.fromtimestamp(t_ns / 1e9, tz=timezone.utc),
            bid=float(bid), ask=float(ask),
            bid_size=float(r.get("bid_size") or 0), ask_size=float(r.get("ask_size") or 0),
        )

    async def bars_1m(self, symbol: str, lo_ms: int, hi_ms: int) -> List[Bar1m]:
        """1-minute bars in [lo_ms, hi_ms], ascending, with volume/VWAP/count."""
        data = await self._get(
            f"{BASE}/v2/aggs/ticker/{symbol}/range/1/minute/{lo_ms}/{hi_ms}",
            {"adjusted": "true", "sort": "asc", "limit": 50000})
        out: List[Bar1m] = []
        for r in data.get("results") or []:
            out.append(Bar1m(
                symbol=symbol, timestamp_ms=int(r["t"]),
                open=float(r["o"]), high=float(r["h"]), low=float(r["l"]), close=float(r["c"]),
                volume=float(r["v"]) if r.get("v") is not None else None,
                vwap=float(r["vw"]) if r.get("vw") is not None else None,
                trade_count=int(r["n"]) if r.get("n") is not None else None,
            ))
        return out
