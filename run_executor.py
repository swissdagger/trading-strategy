"""
run_executor.py — process entrypoint for the execution side.

This is the standalone launcher the repo was missing: the signal side ships in
docker-compose.yml (kafka + warmup + feed + signal), but the executor had no
`__main__`. This wires the pieces exactly as IB_ADAPTER_WIRING.md and
SIGNAL_LISTENER_WIRING.md describe:

    SignalListener (LISTEN/NOTIFY on trade_signals)
        -> executor.handle_signal(row)          # idempotent on signal_id
    SignalExecutor
        -> GatedAdapter(raw_adapter, TradeEnableState)   # off => orders no-op
            -> IBKRAdapter | BinanceAdapter (paper 4002 / live 4001 by mode)

Boot ordering (enforced by SignalExecutor.start()):
    connect() -> reconcile() -> diff DB vs venue -> start fill stream -> subscribe

Run it AFTER: migrations 001..013 applied, the signal side is producing rows,
IB Gateway (or Binance testnet reachability) is up, and .env is populated.

    python run_executor.py            # boots in whatever mode the DB row says
                                      # (config default "off" until you flip it)

================================  READ THIS  ================================
ONE THING IS NOT WIRED, because it does not exist in the repo yet: the concrete
ExecutorHooks / BootHooks implementations (the ~13 persistence + market-read
callables SignalExecutor and boot.run_boot_reconcile depend on). The producer
side has producer/persistence.py with make_* factories over an asyncpg pool;
the execution side needs the equivalent against the 001 trade-lifecycle schema
(trades, orders, fills) plus the market-read helpers.

`build_executor_hooks()` below is the single seam where that plugs in. It raises
NotImplementedError today with a checklist of every callable and the table each
touches. Implement those (ideally in a new execution/persistence.py of make_*
factories, mirroring producer/persistence.py), point this function at them, and
the process is complete. Everything else here is real and runnable.
============================================================================
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal as _signal
import sys

import asyncpg

import config
from execution.adapter_factory import (
    build_ibkr_adapter,
    build_binance_adapter,
    endpoint_kind_for_mode,
)
from execution.gated_adapter import GatedAdapter
from execution.trade_enable import TradeEnableState, TradeMode
from execution.signal_listener import SignalListener
from execution.signal_executor import SignalExecutor
from execution.persistence import build_executor_hooks
from execution.sizing import make_size_position

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("executor.main")


# --- which venue this executor drives ---------------------------------------
# IBKR for equity/fx, Binance for crypto. One executor process drives one venue;
# run two processes (with different VENUE) if you trade both. Override with
# EXECUTOR_VENUE=binance.
VENUE = os.getenv("EXECUTOR_VENUE", "ibkr").lower()

_ADAPTER_BUILDERS = {
    "ibkr": build_ibkr_adapter,
    "binance": build_binance_adapter,
}


def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        log.error("missing required env var %s", name)
        sys.exit(2)
    return val


def build_raw_adapter(mode: TradeMode):
    """Construct the mode-appropriate venue adapter (paper vs live endpoint)."""
    builder = _ADAPTER_BUILDERS.get(VENUE)
    if builder is None:
        raise RuntimeError(
            f"unknown EXECUTOR_VENUE={VENUE!r}; expected one of {list(_ADAPTER_BUILDERS)}"
        )
    venue_cfg = config.VENUE_ENDPOINTS[VENUE]
    adapter, kind = builder(venue_cfg, mode)
    log.info("built %s adapter for mode=%s (endpoint=%s)", VENUE, mode.value, kind)
    return adapter


async def main() -> None:
    dsn = _require_env("TRADING_DB_URL")

    # Shared pool: catch-up SELECTs (listener), mode reads (trade_enable), and
    # all executor persistence. statement_cache_size=0 for Supabase/pgbouncer,
    # matching the convention used everywhere else in the codebase.
    pool = await asyncpg.create_pool(dsn, statement_cache_size=0)
    log.info("db pool up")

    # `current` holds the live raw adapter so the mode-change callback can swap
    # it under the GatedAdapter without rebuilding the executor. Mirrors the
    # pattern in IB_ADAPTER_WIRING.md.
    current: dict = {"adapter": None}

    async def on_mode_change(old: TradeMode, new: TradeMode) -> None:
        # off<->paper: same (paper) venue, no rebuild. Anything to/from live
        # changes the endpoint and needs a reconnect.
        if endpoint_kind_for_mode(old) == endpoint_kind_for_mode(new):
            log.info("mode %s -> %s: same endpoint, no adapter rebuild", old.value, new.value)
            return
        log.warning("mode %s -> %s: rebuilding adapter for new venue", old.value, new.value)
        new_raw = build_raw_adapter(new)
        await new_raw.connect()
        current["adapter"] = new_raw
        log.info("adapter swapped to %s endpoint", endpoint_kind_for_mode(new))

    # Mode state: reads the trade_enabled row, LISTENs for changes, fires the
    # reconnect callback. Boots to config default ("off") until the row is read.
    mode = TradeEnableState(
        dsn,
        pool,
        config_default=config.TRADE_MODE_DEFAULT,
        on_mode_change=on_mode_change,
    )
    await mode.start()
    log.info("trade mode at startup: %s", mode.mode().value)

    # Initial adapter for the startup mode, wrapped in the gate (off => orders
    # no-op, reads pass through). The executor only ever sees the gated wrapper.
    current["adapter"] = build_raw_adapter(mode.mode())
    adapter = GatedAdapter(current["adapter"], mode)

    # DB-backed hooks (execution/persistence.py). Two members — get_touch and
    # size_position — are NOT DB-backed by design: the touch is the venue's live
    # book and equity-based sizing reads the venue account. Both are wired to the
    # adapter here, through a closure that follows the paper<->live adapter swap
    # (current["adapter"] is rebound by on_mode_change), so a mode change doesn't
    # leave them pointing at a dead connection.
    hooks = build_executor_hooks(pool)

    def _adapter() -> "object":
        return current["adapter"]

    # get_touch -> the live adapter's touch. The 001 schema has no quotes table,
    # so without this the pricer has no reference and can price nothing.
    async def _get_touch(symbol: str, asset_class: str):
        return await current["adapter"].get_touch(symbol, asset_class)
    hooks.get_touch = _get_touch

    # size_position -> 5%-of-equity policy (execution/sizing.py), reading live
    # equity + touch from the adapter, and flooring the quantity to the venue's
    # LOT_SIZE/contract step via adapter.get_step_size (cached per symbol). No
    # separate step map to maintain — the adapter owns it.
    hooks.size_position = make_size_position(_adapter)
    log.info("get_touch and size_position (5%% equity) wired to venue adapter")

    executor = SignalExecutor(adapter, hooks)

    # Boot ordering invariant: connect -> reconcile -> diff -> fill stream.
    # Raises if a trading session can't be confirmed; we stay down rather than
    # trade blind.
    await executor.start()

    # Only NOW subscribe to signals. initial_cursor=None => act on signals from
    # startup forward. Persist executor.hooks / listener.cursor if you want to
    # resume-without-replay across restarts.
    listener = SignalListener(
        dsn=dsn,
        pool=pool,
        handler=executor.handle_signal,   # idempotent on row['signal_id']
        initial_cursor=None,
    )

    stop = asyncio.Event()

    def _request_stop(*_):
        log.info("shutdown signal received")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (_signal.SIGINT, _signal.SIGTERM):
        loop.add_signal_handler(sig, _request_stop)

    listener_task = asyncio.create_task(listener.run(), name="signal-listener")
    log.info("executor live: listening for trade_signals")

    await stop.wait()

    # Graceful teardown.
    log.info("stopping...")
    listener_task.cancel()
    try:
        await listener_task
    except asyncio.CancelledError:
        pass
    await mode.stop()
    await pool.close()
    log.info("executor stopped cleanly")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except NotImplementedError as e:
        log.error("cannot start: %s", e)
        sys.exit(1)