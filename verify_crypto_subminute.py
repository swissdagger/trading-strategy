"""
verify_crypto_subminute.py — read-only check of the Coinbase trades source
that builds the CN ladder's 30s/45s bars (feeds/coinbase_spot.py).

Run BEFORE the first warmup that includes the seconds rungs. It reads a few
pages of the Exchange trades endpoint for each subminute symbol, builds 30s
and 45s bars from them with the feed's own code, and reports alignment,
spacing, the trade rate and the page budget a 5000-bar warmup would need.
No writes; Kafka and the database are not touched.

    python verify_crypto_subminute.py            # ASSET_CONFIGS["crypto"]["subminute_symbols"]
    python verify_crypto_subminute.py ETH-USD    # other symbols

Exit 0 when every check passes.
"""
from __future__ import annotations

import asyncio
import sys
import time

import config
from feeds.base import timeframe_to_ms
from feeds.coinbase_spot import TRADES_MAX_PAGES, TRADES_PAGE, CoinbaseSpotFeed

PAGES = 5
WINDOW = 5000


async def check(feed: CoinbaseSpotFeed, symbol: str) -> list[str]:
    fails: list[str] = []
    trades = []
    after = None
    t0 = time.perf_counter()
    for _ in range(PAGES):
        page = await feed._fetch_trades_page(symbol, after)
        if page is None:
            return [f"{symbol}: trades endpoint failed (see log) — is api.exchange.coinbase.com reachable?"]
        if not page:
            break
        trades.extend(page)
        after = page[-1]["trade_id"]
    dt = time.perf_counter() - t0
    if not trades:
        return [f"{symbol}: no trades returned"]
    span_s = (trades[0]["t"] - trades[-1]["t"]) / 1000.0
    rate = len(trades) / span_s if span_s > 0 else float("inf")
    print(f"\n[{symbol}] {len(trades)} trades over {span_s / 60:.1f} min in {dt:.1f}s "
          f"({rate:.1f} trades/s); ids {trades[-1]['trade_id']}..{trades[0]['trade_id']}")
    for tf in ("30s", "45s"):
        tf_ms = timeframe_to_ms(tf)
        bars = feed._trades_to_windows(trades, tf)
        ts = [w["t"] for w in bars]
        bad_align = [t for t in ts if t % tf_ms != 0]
        gaps = sum(1 for a, b in zip(ts, ts[1:]) if b - a != tf_ms)
        need_trades = rate * WINDOW * tf_ms / 1000.0
        pages = need_trades / TRADES_PAGE
        mark = "FAIL" if bad_align else " ok "
        print(f"  {tf:>4} {mark} {len(bars):>4} bars, {gaps} empty-window gap(s); a {WINDOW}-bar "
              f"warmup ≈ {need_trades / 1e3:.0f}k trades ≈ {pages:.0f} pages "
              f"(budget {TRADES_MAX_PAGES}){'  <-- over budget: partial buffer' if pages > TRADES_MAX_PAGES else ''}")
        if bad_align:
            fails.append(f"{symbol} {tf}: {len(bad_align)} bars off the grid")
    return fails


async def main(argv: list[str]) -> int:
    symbols = [a for a in argv if not a.startswith("--")] or \
        list(config.ASSET_CONFIGS["crypto"].get("subminute_symbols") or [])
    if not symbols:
        print("no subminute_symbols configured and none given")
        return 2
    feed = CoinbaseSpotFeed(symbols, ["30s", "45s"], subminute_symbols=symbols)
    fails: list[str] = []
    try:
        for s in symbols:
            fails += await check(feed, s)
    finally:
        await feed.aclose()
    for f in fails:
        print(f"FAIL: {f}")
    print("\nOK: trades source verification passed" if not fails else f"\n{len(fails)} check(s) failed")
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[1:])))
