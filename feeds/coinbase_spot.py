"""
feeds/coinbase_spot.py — Coinbase Advanced Trade spot candles as an AssetFeed.

ADR-001 Phase 2 (Option 2a): the signal series comes from the SAME venue the
executor trades on — no cross-venue basis, and no live dependency on Binance
(which the account cannot trade and which geo-blocks UK/US IPs).

Source: the PUBLIC market-data endpoint (no API key)

    GET https://api.coinbase.com/api/v3/brokerage/market/products/{product_id}/candles
        ?start=<unix s>&end=<unix s>&granularity=ONE_MINUTE

which returns {"candles": [{"start": "<unix s>", "low", "high", "open", "close",
"volume"}, ...]} — prices as strings, NEWEST FIRST, at most 350 candles per
request, and the CURRENT (still-forming) candle included. Intervals with no
trades are omitted rather than returned flat, so a series can have gaps; those
pass through untouched, exactly as the batch resampler leaves them (Sumtyme's
step mode is built for gaps; fabricating flat bars would change its input).

Canonical symbol = the Coinbase product id ("BTC-USD"): what flows through
Kafka, chains, trades and trade_signals. to_native_symbol is identity.

## Timeframes

Native granularities are fetched as-is. A rung Coinbase does not offer is
DERIVED from a finer native one with the shared resampling rule
(feeds/resample.py): 3m from 1m (the four_chain ladder's second rung), and 4h
from 1h / 12h from 6h (the disabled prior_failure_scaleout ladder), so
re-enabling PF needs no feed change.

## Live path

One REST poll loop per timeframe at ~tf/2 (8s floor, 600s ceiling), closed
windows only, per-(symbol, tf) high-water cursor seeded from warmup's
feed_cursors — the same shape as the Binance feed it replaces. A window is
emitted only once it is provably complete:
  * its end is at least SETTLE_MS in the past (Coinbase publishes a candle a
    moment after the minute closes), and
  * for a DERIVED window, its last constituent base candle is present — or one
    base interval of grace has passed (that minute had no trades). Otherwise a
    3m bar polled just after its window closed could miss its final minute and,
    because the cursor then moves past it, never be corrected.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Set

import httpx

from feeds.base import AssetFeed, LiveDataCallback, is_market_open, timeframe_to_ms
from feeds.resample import _resample_to_timeframe
from producer.bar import OHLCPoint, RateLimiter

logger = logging.getLogger(__name__)

BASE_URL = "https://api.coinbase.com/api/v3/brokerage/market/products"

# Canonical timeframe -> Coinbase granularity enum.
_NATIVE: Dict[str, str] = {
    "1m": "ONE_MINUTE",
    "5m": "FIVE_MINUTE",
    "15m": "FIFTEEN_MINUTE",
    "30m": "THIRTY_MINUTE",
    "1h": "ONE_HOUR",
    "2h": "TWO_HOUR",
    "6h": "SIX_HOUR",
    "1d": "ONE_DAY",
}

# Derived timeframe -> the native timeframe it is built from.
_DERIVED: Dict[str, str] = {
    "3m": "1m",
    "4h": "1h",
    "12h": "6h",
}

_SUPPORTED: Set[str] = set(_NATIVE) | set(_DERIVED)

# Candles per request. The API caps a request at 350; stay under it so an
# inclusive start/end boundary can never push a page over the limit.
_PAGE_CANDLES = 300

# A closed candle is trusted only this long after its window ends.
SETTLE_MS = 5_000

# Live polls look back this many target windows per request: enough to cover
# a missed poll or two without re-reading history.
_LIVE_LOOKBACK_WINDOWS = 6


def base_timeframe(tf: str) -> str:
    """The native timeframe a target is fetched at (itself, if native)."""
    return _DERIVED.get(tf, tf)


class CoinbaseSpotFeed(AssetFeed):
    asset_class = "crypto"
    name = "coinbase_spot"

    def __init__(
        self,
        symbols: List[str],
        timeframes: List[str],
        requests_per_second: float = 8.0,
        max_concurrent: int = 4,
        *,
        transport: Optional[httpx.AsyncBaseTransport] = None,
        now_ms: Optional[Callable[[], int]] = None,
    ):
        """
        requests_per_second: Coinbase's public market endpoints allow about
            10 req/s per IP; 8 leaves headroom for retries.
        transport / now_ms: injection points for tests (an httpx MockTransport
            and a fixed clock). Production leaves both None.
        """
        unsupported = [tf for tf in timeframes if tf not in _SUPPORTED]
        if unsupported:
            raise ValueError(
                f"CoinbaseSpotFeed does not support timeframe(s): {unsupported}. "
                f"Supported: {sorted(_SUPPORTED, key=timeframe_to_ms)}")
        bad = [s for s in symbols if "-" not in s]
        if bad:
            # Catches a config still in Binance form (BTCUSDT) before it turns
            # into a stream of 404s.
            raise ValueError(
                f"CoinbaseSpotFeed symbols must be Coinbase product ids like "
                f"'BTC-USD'; got {bad}")
        self.symbols = symbols
        self.timeframes = timeframes
        self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second, max_concurrent=max_concurrent)
        self._transport = transport
        self._now_ms = now_ms or (lambda: int(time.time() * 1000))
        self._client: Optional[httpx.AsyncClient] = None
        self._running = False

    def supported_timeframes(self) -> Set[str]:
        return _SUPPORTED

    def to_native_symbol(self, symbol: str) -> str:
        return symbol

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(transport=self._transport, timeout=30.0)
        return self._client

    async def aclose(self) -> None:
        self._running = False
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # One page of candles
    # ------------------------------------------------------------------
    async def _fetch_page(self, product_id: str, base_tf: str,
                          start_s: int, end_s: int) -> Optional[List[dict]]:
        """Candles for [start_s, end_s] at base_tf as ascending {t,o,h,l,c}
        dicts (t in ms). None on a non-retryable failure (unknown product,
        repeated errors); [] when the range simply has no candles."""
        client = await self._get_client()
        url = f"{BASE_URL}/{product_id}/candles"
        params = {"start": str(start_s), "end": str(end_s),
                  "granularity": _NATIVE[base_tf]}
        for attempt in range(4):
            try:
                async with self.rate_limiter:
                    resp = await client.get(url, params=params)
                if resp.status_code == 429 or resp.status_code >= 500:
                    await asyncio.sleep(2 ** attempt)
                    continue
                if resp.status_code in (400, 404):
                    logger.error("coinbase candles %s %s: HTTP %d %s", product_id,
                                 base_tf, resp.status_code, resp.text[:200])
                    return None
                resp.raise_for_status()
                rows = resp.json().get("candles", [])
                out = [
                    {"t": int(r["start"]) * 1000, "o": float(r["open"]),
                     "h": float(r["high"]), "l": float(r["low"]),
                     "c": float(r["close"])}
                    for r in rows
                ]
                out.sort(key=lambda r: r["t"])
                return out
            except (httpx.HTTPError, ValueError, KeyError) as e:
                logger.warning("coinbase candles %s %s attempt %d failed: %r",
                               product_id, base_tf, attempt + 1, e)
                await asyncio.sleep(2 ** attempt)
        logger.error("coinbase candles %s %s: giving up after retries", product_id, base_tf)
        return None

    # ------------------------------------------------------------------
    # HISTORY (warmup)
    # ------------------------------------------------------------------
    async def fetch_history(self, symbol: str, timeframe: str,
                            points_needed: int) -> List[OHLCPoint]:
        """Up to points_needed CLOSED candles for (symbol, timeframe), oldest
        first. Pages backwards from now until enough target bars exist, the
        product's history runs out (an empty page), or the page budget is spent."""
        if timeframe not in _SUPPORTED:
            raise ValueError(f"unsupported timeframe {timeframe!r}")
        base_tf = base_timeframe(timeframe)
        base_ms = timeframe_to_ms(base_tf)
        tf_ms = timeframe_to_ms(timeframe)
        ratio = tf_ms // base_ms
        now = self._now_ms()

        collected: Dict[int, dict] = {}
        bars: List[dict] = []
        end_s = now // 1000
        page_s = _PAGE_CANDLES * base_ms // 1000
        # Generous budget: enough pages for the request plus slack for gaps.
        max_pages = (points_needed * ratio) // _PAGE_CANDLES + 10
        for _ in range(max_pages):
            start_s = end_s - page_s
            page = await self._fetch_page(symbol, base_tf, start_s, end_s)
            if not page:
                break   # None (error) or [] (before the product's first trade)
            for r in page:
                collected[r["t"]] = r
            bars = self._closed_target_bars(collected, timeframe, now, final=True)
            if len(bars) >= points_needed:
                break
            end_s = start_s

        return [self._point(symbol, timeframe, b) for b in bars[-points_needed:]]

    def _closed_target_bars(self, base_by_t: Dict[int, dict], timeframe: str,
                            now_ms: int, *, final: bool) -> List[dict]:
        """Target-timeframe bars that are complete as of now_ms, ascending.

        final=True (history): any window whose end has passed is complete.
        final=False (live): additionally apply the settle delay and, for a
        derived window, require its last base candle (or a grace period)."""
        base_tf = base_timeframe(timeframe)
        base_ms = timeframe_to_ms(base_tf)
        tf_ms = timeframe_to_ms(timeframe)
        settle = 0 if final else SETTLE_MS

        base = [base_by_t[t] for t in sorted(base_by_t)
                if t + base_ms + settle <= now_ms]   # drop the forming candle
        if timeframe == base_tf:
            return base
        out = []
        for w in _resample_to_timeframe(base, timeframe):
            end = w["t"] + tf_ms
            if end + settle > now_ms:
                continue
            if not final:
                last_present = (end - base_ms) in base_by_t
                if not last_present and now_ms < end + base_ms + settle:
                    continue   # last minute may still be published
            out.append(w)
        return out

    def _point(self, symbol: str, timeframe: str, b: dict) -> OHLCPoint:
        return OHLCPoint(symbol=symbol, timestamp=b["t"], open=b["o"], high=b["h"],
                         low=b["l"], close=b["c"], timeframe=timeframe,
                         asset_class=self.asset_class)

    async def recent_closed(self, symbol: str, timeframe: str) -> List[OHLCPoint]:
        """The last few complete windows for (symbol, timeframe), by the live
        completeness rule. One request."""
        base_tf = base_timeframe(timeframe)
        tf_ms = timeframe_to_ms(timeframe)
        now = self._now_ms()
        start_s = (now - _LIVE_LOOKBACK_WINDOWS * tf_ms) // 1000
        page = await self._fetch_page(symbol, base_tf, start_s, now // 1000)
        if not page:
            return []
        by_t = {r["t"]: r for r in page}
        return [self._point(symbol, timeframe, b)
                for b in self._closed_target_bars(by_t, timeframe, now, final=False)]

    # ------------------------------------------------------------------
    # LIVE (REST polling)
    # ------------------------------------------------------------------
    async def start_live(
        self, symbols: List[str], timeframes: List[str],
        initial_cursors: Dict[str, Dict[str, int]], callback: LiveDataCallback,
    ) -> None:
        self._running = True

        async def poll_one_tf(tf: str) -> None:
            tf_ms = timeframe_to_ms(tf)
            interval = min(max(tf_ms // 2000, 8), 600)
            # Cursor = start of the next window we expect. Seeded from warmup;
            # a cold rung starts at the most recent closed window.
            cold = (self._now_ms() // tf_ms - 1) * tf_ms
            cursors = {
                sym: initial_cursors.get(sym, {}).get(tf) or cold
                for sym in symbols
            }
            logger.info("[%s:%s] REST polling every %ss", self.name, tf, interval)
            while self._running:
                try:
                    now = datetime.now(timezone.utc)
                    if is_market_open(self.asset_class, now):   # crypto: always
                        batch = await self.poll_once(symbols, tf, cursors)
                        if batch:
                            await callback(batch)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("[%s:%s] poll error", self.name, tf)
                await asyncio.sleep(interval)

        tasks = [asyncio.create_task(poll_one_tf(tf)) for tf in timeframes]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()

    async def poll_once(self, symbols: List[str], tf: str,
                        cursors: Dict[str, int]) -> List[OHLCPoint]:
        """One poll pass for one timeframe: newly-complete windows at or past
        each symbol's cursor, advancing the cursors. Split out of start_live so
        the emission rule is testable without the sleep loop."""
        tf_ms = timeframe_to_ms(tf)
        batch: List[OHLCPoint] = []
        for sym in symbols:
            for p in await self.recent_closed(sym, tf):
                if p.timestamp >= cursors[sym]:
                    batch.append(p)
                    cursors[sym] = p.timestamp + tf_ms
        return batch
