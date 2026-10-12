"""
run_executor.py — process entrypoint for the execution side.

The signal side ships in docker-compose.yml (kafka + warmup + feed + signal);
this is the executor's standalone launcher. Wiring:

    SignalListener (LISTEN/NOTIFY on trade_signals)
        -> executor.handle_signal(row)          # idempotent on signal_id
    SignalExecutor
        -> GatedAdapter(raw_adapter, TradeEnableState)   # off => orders no-op
            -> venue adapter for EXECUTOR_VENUE (paper vs live by mode)

Persistence: ExecutorHooks come from execution/persistence.build_executor_hooks
(make_* factories over the asyncpg pool, against the 001 trade-lifecycle schema
and later migrations). get_touch and size_position are deliberately NOT
DB-backed; they are rewired below to the live adapter.

Boot ordering (enforced by SignalExecutor.start()):
    connect() -> reconcile() -> diff DB vs venue -> start fill stream -> subscribe

ADR-001 status: the venue is Coinbase Advanced Trade spot (Phase 4). Mode
"live" trades through CoinbaseAdapter; mode "off" connects the real adapter
READ-ONLY (reconcile, balances, fills; no write can reach the venue). Mode
"paper" needs PaperCoinbaseAdapter (Phase 5) and refuses to start until then.
connect() refuses a key that is not scoped to the bot portfolio, can transfer
funds, or (live) cannot trade.

Run it AFTER: migrations applied, the signal side is producing rows, and .env is
populated.

    python run_executor.py            # boots in whatever mode the DB row says
                                      # (config default "off" until you flip it)
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
    build_coinbase_adapter,
    endpoint_kind_for_mode,
)
from execution.gated_adapter import GatedAdapter
from execution.trade_enable import TradeEnableState, TradeMode
from execution.signal_listener import SignalListener
from execution.signal_executor import OrphanPositionError, SignalExecutor
from execution.coinbase_adapter import CoinbaseConnectError
from execution.coinbase_client import CoinbaseAuthError
from execution.persistence import (
    build_executor_hooks,
    make_load_executor_cursor,
    make_save_executor_cursor,
    make_shadow_hooks,
)
from execution.shadow_book import ShadowBook
from execution.sizing import make_size_position

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("executor.main")


# --- which venue this executor drives ---------------------------------------
# One executor process drives one venue. ADR-001: Coinbase Advanced Trade spot
# is the only venue; EXECUTOR_VENUE is kept so a future equities venue is a new
# builder entry, not a launcher rewrite.
VENUE = os.getenv("EXECUTOR_VENUE", "coinbase").lower()

_ADAPTER_BUILDERS = {
    "coinbase": build_coinbase_adapter,
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
    # it under the GatedAdapter without rebuilding the executor.
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

    # Shadow book: SHORT signals the venue can't take (spot has no short side)
    # get virtual fills at the live touch instead (migration 019).
    shadow = ShadowBook(make_shadow_hooks(pool, _get_touch),
                        taker_fee_bps=config.SHADOW_TAKER_FEE_BPS)
    await shadow.start()

    executor = SignalExecutor(
        adapter, hooks,
        shadow_book=shadow,
        max_open_positions=config.MAX_OPEN_POSITIONS,
        enter_max_age_ms=config.ENTER_MAX_AGE_SECONDS * 1000,
        orphan_allowlist=config.ORPHAN_BALANCE_ALLOWLIST,
    )

    # Boot ordering invariant: connect -> reconcile -> diff -> fill stream.
    # Raises if a trading session can't be confirmed or the venue holds orphan
    # balances; we stay down rather than trade blind. Orphans alert from inside
    # start(); any other boot failure alerts here.
    try:
        await executor.start()
    except (OrphanPositionError, CoinbaseConnectError):
        raise   # already explained (and, for orphans, alerted) at the source
    except Exception as e:
        try:
            await hooks.alert("executor_boot_failed", {"error": repr(e)})
        except Exception:
            log.exception("could not record executor_boot_failed alert")
        raise

    # Only NOW subscribe to signals. Resume from the persisted cursor
    # (migration 020) so signals written while we were down — EXITs above all —
    # are replayed; stale ENTERs are skipped by age inside the executor. No
    # cursor yet (first ever run) => start from now.
    initial_cursor = await make_load_executor_cursor(pool)()
    log.info("listener resuming from %s",
             initial_cursor.isoformat() if initial_cursor else "now (no persisted cursor)")
    listener = SignalListener(
        dsn=dsn,
        pool=pool,
        handler=executor.handle_signal,   # idempotent on row['signal_id']
        initial_cursor=initial_cursor,
        on_cursor_advance=make_save_executor_cursor(pool),
    )

    stop = asyncio.Event()

    def _request_stop(*_):
        log.info("shutdown signal received")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (_signal.SIGINT, _signal.SIGTERM):
        loop.add_signal_handler(sig, _request_stop)

    listener_task = asyncio.create_task(listener.run(), name="signal-listener")
    # The escalation driver: widens unfilled exits rung by rung and HALTs at the
    # top of the ladder. Without it, a working exit that never fills is never
    # escalated.
    escalation_task = asyncio.create_task(
        executor.run_escalation_loop(), name="escalation-loop")
    log.info("executor live: listening for trade_signals")

    await stop.wait()

    # Graceful teardown.
    log.info("stopping...")
    executor.stop()
    listener_task.cancel()
    for task in (listener_task, escalation_task):
        try:
            await task
        except asyncio.CancelledError:
            pass
    await mode.stop()
    await pool.close()
    log.info("executor stopped cleanly")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (NotImplementedError, OrphanPositionError, CoinbaseConnectError,
            CoinbaseAuthError) as e:
        log.error("cannot start: %s", e)
        sys.exit(1)