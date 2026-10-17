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

ADR-001 status: the venue is Coinbase Advanced Trade spot.
  * "paper" (and "off"): PaperCoinbaseAdapter — real touch, product rules and
    fee tier from Coinbase through a READ-ONLY real adapter; fills simulated in
    the paper ledger (migration 022). "off" places nothing at all.
  * "live": CoinbaseAdapter — real orders.
connect() refuses a key that is not scoped to the bot portfolio, can transfer
funds, or (live) cannot trade. All modes need the key (paper reads real data).

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
import time

import asyncpg

import config
from execution.adapter_factory import (
    build_coinbase_adapter,
    endpoint_kind_for_mode,
)
from execution.gated_adapter import GatedAdapter
from execution.read_only_adapter import ReadOnlyAdapter
from execution.trade_enable import TradeEnableState, TradeMode
from execution.signal_listener import SignalListener
from execution.signal_executor import OrphanPositionError, SignalExecutor
from execution.coinbase_adapter import CoinbaseConnectError
from execution.coinbase_client import CoinbaseAuthError
from execution.persistence import (
    build_executor_hooks,
    make_load_executor_cursor,
    make_paper_ledger_hooks,
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


def build_raw_adapter(mode: TradeMode, pool):
    """Construct the mode-appropriate venue adapter: the paper adapter (real
    reads, simulated fills in the paper ledger) for off/paper, the real one for
    live."""
    builder = _ADAPTER_BUILDERS.get(VENUE)
    if builder is None:
        raise RuntimeError(
            f"unknown EXECUTOR_VENUE={VENUE!r}; expected one of {list(_ADAPTER_BUILDERS)}"
        )
    venue_cfg = config.VENUE_ENDPOINTS[VENUE]
    adapter, kind = builder(
        venue_cfg, mode,
        paper_ledger=make_paper_ledger_hooks(pool),
        paper_starting_cash=config.PAPER_STARTING_CASH,
        fallback_fee_bps=config.SHADOW_TAKER_FEE_BPS,
    )
    log.info("built %s adapter for mode=%s (endpoint=%s)", VENUE, mode.value, kind)
    return adapter


def make_on_mode_change(current: dict, stop: asyncio.Event, restart_required: dict):
    """The trade-mode callback. off<->paper: nothing (same paper adapter; the
    GatedAdapter starts or stops allowing simulated orders). paper<->live: the
    venue changes, and a hot swap is never safe — the executor's open trades
    and attribution maps belong to the old venue and must be reconciled
    against the new one at boot. (The previous swap only rebound
    current["adapter"], so orders kept flowing to the OLD adapter: live ->
    paper would have kept placing real orders.) So it blocks every write right
    now, on both venues, and stops the process; the process manager restarts
    it and boot reconciles against the new venue."""
    async def on_mode_change(old: TradeMode, new: TradeMode) -> None:
        if endpoint_kind_for_mode(old) == endpoint_kind_for_mode(new):
            log.info("mode %s -> %s: same endpoint, no adapter change", old.value, new.value)
            return
        gated = current.get("gated")
        if gated is not None and not isinstance(gated._inner, ReadOnlyAdapter):
            gated._inner = ReadOnlyAdapter(gated._inner)
        restart_required["flag"] = True
        log.error("mode %s -> %s changes the venue: orders blocked; stopping so the "
                  "executor restarts and reconciles against the new venue",
                  old.value, new.value)
        stop.set()
    return on_mode_change


async def main() -> None:
    dsn = _require_env("TRADING_DB_URL")

    # Shared pool: catch-up SELECTs (listener), mode reads (trade_enable), and
    # all executor persistence. statement_cache_size=0 for Supabase/pgbouncer,
    # matching the convention used everywhere else in the codebase.
    pool = await asyncpg.create_pool(dsn, statement_cache_size=0)
    log.info("db pool up")

    # `current["adapter"]` is the raw adapter for the startup mode; `gated` the
    # GatedAdapter the executor uses. Neither is swapped at runtime (see below).
    current: dict = {"adapter": None, "gated": None}
    stop = asyncio.Event()
    restart_required = {"flag": False}

    on_mode_change = make_on_mode_change(current, stop, restart_required)

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
    current["adapter"] = build_raw_adapter(mode.mode(), pool)
    adapter = GatedAdapter(current["adapter"], mode)
    current["gated"] = adapter

    # DB-backed hooks (execution/persistence.py). Two members — get_touch and
    # size_position — are NOT DB-backed by design: the touch is the venue's live
    # book and equity-based sizing reads the venue account. Both are wired to the
    # adapter here. (A venue change restarts the process instead of swapping
    # adapters, so these always read the venue the executor trades on.)
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
    # Fee per virtual leg: the account's live taker fee tier (paper and live
    # adapters both expose it), falling back to config.SHADOW_TAKER_FEE_BPS.
    async def _fee_bps():
        fn = getattr(current["adapter"], "get_taker_fee_bps", None)
        return await fn() if fn is not None else None
    shadow = ShadowBook(make_shadow_hooks(pool, _get_touch),
                        taker_fee_bps=config.SHADOW_TAKER_FEE_BPS, fee_bps_fn=_fee_bps,
                        fee_bps_by_class=getattr(config, "SHADOW_FEE_BPS_BY_CLASS", {}))
    await shadow.start()

    executor = SignalExecutor(
        adapter, hooks,
        shadow_book=shadow,
        shadow_only_strategies=getattr(config, "SHADOW_ONLY_STRATEGIES", frozenset()),
        max_open_positions=config.MAX_OPEN_POSITIONS,
        enter_max_age_ms=config.ENTER_MAX_AGE_SECONDS * 1000,
        orphan_allowlist=config.ORPHAN_BALANCE_ALLOWLIST,
    )

    # Boot ordering invariant: connect -> reconcile -> diff -> fill stream.
    # Raises if a trading session can't be confirmed or the venue holds orphan
    # balances; we stay down rather than trade blind (logged at the source and
    # in __main__ below).
    await executor.start()

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

    def _request_stop(*_):
        log.info("shutdown signal received")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (_signal.SIGINT, _signal.SIGTERM):
        loop.add_signal_handler(sig, _request_stop)

    listener_task = asyncio.create_task(listener.run(), name="signal-listener")
    # The escalation driver: widens unfilled exits rung by rung and HALTs at the
    # top of the ladder, and closes every position while the kill switch is in
    # 'flatten' mode. Without it, a working exit that never fills is never
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
    if restart_required["flag"]:
        log.warning("executor stopped for a venue change; exiting 3 so it restarts")
        sys.exit(3)
    log.info("executor stopped cleanly")


# A refused or failed boot waits this long before exiting, so a restart loop
# under docker-compose (restart: unless-stopped) retries at most every few
# minutes instead of every few seconds.
BOOT_RETRY_S = float(os.getenv("EXECUTOR_BOOT_RETRY_S", "300"))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except SystemExit:
        raise
    except (NotImplementedError, OrphanPositionError, CoinbaseConnectError,
            CoinbaseAuthError) as e:
        log.error("cannot start: %s — retrying in %.0fs", e, BOOT_RETRY_S)
        time.sleep(BOOT_RETRY_S)
        sys.exit(1)
    except Exception:
        log.exception("executor failed — retrying in %.0fs", BOOT_RETRY_S)
        time.sleep(BOOT_RETRY_S)
        sys.exit(1)