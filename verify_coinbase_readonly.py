"""
verify_coinbase_readonly.py — read-only check of the Coinbase key and portfolio.

ADR-001 Phase 4's verification step, run BEFORE the executor ever trades. It
uses the same CoinbaseAdapter the executor uses, wrapped read-only, and makes
NO writes: no orders, no cancels.

    COINBASE_API_KEY_NAME=organizations/.../apiKeys/...  \\
    COINBASE_API_PRIVATE_KEY="-----BEGIN EC PRIVATE KEY-----\\n...\\n-----END EC PRIVATE KEY-----\\n" \\
    COINBASE_PORTFOLIO_UUID=... \\
    python verify_coinbase_readonly.py

Exit 0 when every check passes; non-zero with the reason otherwise.
"""
from __future__ import annotations

import asyncio
import sys

import config
from execution.adapter_factory import build_coinbase_adapter
from execution.trade_enable import TradeMode


async def main() -> int:
    adapter, _ = build_coinbase_adapter(config.VENUE_ENDPOINTS["coinbase"], TradeMode.OFF)
    inner = adapter.inner
    try:
        await adapter.connect()
        p = inner.permissions
        print(f"key: view={p.get('can_view')} trade={p.get('can_trade')} "
              f"transfer={p.get('can_transfer')} portfolio={p.get('portfolio_uuid')} "
              f"({p.get('portfolio_type')})")
        if not p.get("can_trade"):
            print("WARNING: key cannot trade — fine for 'off', but 'live' will refuse it")

        eq = await adapter.get_account_equity()
        print(f"equity: available={eq.available} {eq.currency}  total={eq.total} {eq.currency}")
        positions = await adapter.get_positions()
        print(f"positions ({len(positions)}): " +
              (", ".join(f"{x.symbol}={x.qty}" for x in positions) or "none"))

        bad = 0
        for sym in config.crypto_symbols():
            prod = await inner.product(sym)
            t = await adapter.get_touch(sym, "crypto")
            ok = prod.tradable and t is not None
            bad += not ok
            print(f"  {sym:9} tradable={prod.tradable!s:5} touch="
                  f"{(str(t.bid) + ' / ' + str(t.ask)) if t else 'NONE':24} "
                  f"base_inc={prod.base_increment} px_inc={prod.price_increment} "
                  f"min={prod.base_min_size}/{prod.quote_min_size}")

        snap = await adapter.reconcile()
        print(f"open orders: {len(snap.open_orders)}; fills in last 24h: {len(snap.recent_fills)}")
        known = set(config.crypto_symbols())
        stray = [x for x in positions if x.symbol not in known]
        if stray:
            print("NOTE: balances outside the traded universe (the executor's orphan "
                  "check will refuse to boot unless they are dust or allowlisted): "
                  + ", ".join(f"{x.symbol}={x.qty}" for x in stray))
        if bad:
            print(f"FAIL: {bad} product(s) not tradable or without a two-sided book")
            return 1
        print("OK: read-only verification passed")
        return 0
    finally:
        await inner.aclose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
