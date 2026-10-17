"""
verify_polygon_feed.py — read-only check of the Polygon equity feed (PR 9).

Run BEFORE the first equity warmup, with the real key. It exercises each of
the three source rules in feeds/polygon_aggs.py against the real API, checks
the one thing the tests could not (D16: Polygon's bucket alignment for the
15m/30m/hour rungs), and tries the websocket auth handshake on the configured
host so the ws→REST fallback is a decision you make here, not a surprise at
04:00 ET. No writes anywhere: nothing is persisted, Kafka is not touched.

    POLYGON_API_KEY=... python verify_polygon_feed.py            # AAPL, all DL rungs
    POLYGON_API_KEY=... python verify_polygon_feed.py MSFT NVDA  # other symbols
    POLYGON_API_KEY=... python verify_polygon_feed.py --no-ws    # REST checks only

Per (symbol, rung) it fetches a SMALL window (120 bars, not 5000) and reports
bars, span, alignment and spacing. Exit 0 when every check passes; non-zero
with the reasons otherwise. A rung that returns fewer bars than asked is a
WARNING, not a failure (young listing / illiquid / coarse rung — the
documented partial-ladder case); a misaligned rung IS a failure, because in
step mode a bar sequence bucketed differently from the live aggregator is a
different series.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from datetime import datetime, timezone

import config
from feeds.base import is_market_open, timeframe_to_ms

SAMPLE_BARS = 120
WS_TIMEOUT_S = 20.0


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def check_bars(tf: str, bars) -> tuple[list[str], list[str]]:
    """Return (failures, warnings) for one rung's sample."""
    fails, warns = [], []
    tf_ms = timeframe_to_ms(tf)
    if not bars:
        warns.append(f"{tf}: 0 bars (young listing, illiquid, or coarse rung — fills live)")
        return fails, warns
    if len(bars) < SAMPLE_BARS:
        warns.append(f"{tf}: {len(bars)}/{SAMPLE_BARS} bars")
    ts = [b.timestamp for b in bars]
    if ts != sorted(ts):
        fails.append(f"{tf}: bars not ascending")
    if len(set(ts)) != len(ts):
        fails.append(f"{tf}: duplicate timestamps")
    misaligned = [t for t in ts if t % tf_ms != 0]
    if misaligned:
        # D16. The live aggregator buckets on floor(t / tf_ms) * tf_ms; a
        # warmup bar not on that grid is a different series. Fix: add the
        # rung to _PolygonAggsFeedBase.DERIVE_FROM_MINUTE.
        fails.append(f"{tf}: {len(misaligned)}/{len(ts)} bars off the {tf} grid "
                     f"(first: {_iso(misaligned[0])}) — add {tf!r} to DERIVE_FROM_MINUTE")
    gaps = [b - a for a, b in zip(ts, ts[1:]) if b - a != tf_ms]
    if gaps:
        # Gaps are expected across session boundaries and no-trade windows;
        # report them so you can see the shape, don't fail on them.
        warns.append(f"{tf}: {len(gaps)} non-uniform step(s) (session/no-trade gaps; "
                     f"largest {max(gaps) / 60000:.0f} min)")
    bad_ohlc = [b for b in bars if not (b.low <= min(b.open, b.close) <= max(b.open, b.close) <= b.high)]
    if bad_ohlc:
        fails.append(f"{tf}: {len(bad_ohlc)} bar(s) with inconsistent OHLC")
    return fails, warns


async def verify_rest(feed, symbol: str, timeframes: list[str]) -> tuple[list[str], list[str]]:
    fails, warns = [], []
    print(f"\n[{symbol}] REST history, {SAMPLE_BARS} bars per rung")
    for tf in timeframes:
        t0 = time.perf_counter()
        try:
            if tf.endswith("s"):
                bars = await feed.fetch_subminute_history(symbol, tf, SAMPLE_BARS)
                src = "trades"
            else:
                bars = await feed.fetch_history(symbol, tf, SAMPLE_BARS)
                src = "1m→resample" if tf in feed.DERIVE_FROM_MINUTE else "aggs"
        except Exception as e:
            fails.append(f"{tf}: fetch raised {type(e).__name__}: {e}")
            print(f"  {tf:>4} FAIL  {type(e).__name__}: {e}")
            continue
        dt = time.perf_counter() - t0
        f, w = check_bars(tf, bars)
        fails += f
        warns += w
        span = f"{_iso(bars[0].timestamp)} → {_iso(bars[-1].timestamp)}" if bars else "—"
        mark = "FAIL" if f else ("warn" if w else " ok ")
        print(f"  {tf:>4} {mark}  {len(bars):>4} bars  {src:<12} {dt:5.1f}s  {span}")
    return fails, warns


async def verify_ws(api_key: str, rest_feed) -> list[str]:
    """Auth-only: connect, authenticate, subscribe one symbol, read until the
    first message or timeout, close. Proves the host + entitlement; nothing
    is consumed into a buffer."""
    from feeds.polygon_ws import PolygonStocksWSFeed
    ws = PolygonStocksWSFeed(api_key, rest_feed)
    print(f"\n[ws] {ws._ws_url()}")
    try:
        conn = await asyncio.wait_for(ws._connect(), timeout=WS_TIMEOUT_S)
    except Exception as e:
        return [f"ws: could not connect/authenticate on {ws.WS_HOST}: {type(e).__name__}: {e} "
                f"(the feed process will FALL BACK to REST; set POLYGON_WS_HOST if the "
                f"entitlement host moved)"]
    try:
        await ws._subscribe(conn, [rest_feed.symbols[0]])
        print("  auth OK; subscribed; waiting for a message "
              f"(market {'open' if is_market_open('equity', datetime.now(timezone.utc)) else 'CLOSED — none expected'})")
        try:
            raw = await asyncio.wait_for(conn.recv(), timeout=WS_TIMEOUT_S)
            n = len(ws._parse_message(raw))
            print(f"  received a message ({n} aggregate(s) parsed)")
        except asyncio.TimeoutError:
            print("  no message within timeout (fine if the session is closed)")
    finally:
        await ws._close(conn)
    return []


async def main(argv: list[str]) -> int:
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        print("POLYGON_API_KEY not set")
        return 2
    do_ws = "--no-ws" not in argv
    symbols = [a for a in argv if not a.startswith("--")] or config.ASSET_CONFIGS["equity"]["symbols"]
    timeframes = list(config.ASSET_CONFIGS["equity"]["timeframes"])

    from feeds.polygon_stocks import PolygonStocksFeed
    feed = PolygonStocksFeed(api_key, symbols, timeframes)
    fails, warns = [], []
    try:
        for sym in symbols:
            f, w = await verify_rest(feed, sym, timeframes)
            fails += [f"{sym} {x}" for x in f]
            warns += [f"{sym} {x}" for x in w]
        if do_ws:
            fails += await verify_ws(api_key, feed)
    finally:
        await feed.aclose()

    # Capacity, so the number is in front of you before you add a symbol.
    per_day = sum(16 * 3600 * 1000 // timeframe_to_ms(tf) for tf in timeframes)
    print(f"\ncapacity: {len(timeframes)} rungs → ~{per_day:,} forecasts/symbol per "
          f"16h session (~{per_day / (16 * 3600):.3f}/s while open); "
          f"{len(symbols)} symbol(s) → ~{len(symbols) * per_day / (16 * 3600):.2f}/s "
          f"against FORECAST_CONCURRENCY ÷ latency (measured peak 0.36/s)")

    for w in warns:
        print(f"WARN: {w}")
    for f in fails:
        print(f"FAIL: {f}")
    if fails:
        print(f"\n{len(fails)} check(s) failed")
        return 1
    print("\nOK: Polygon feed verification passed")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[1:])))
