"""
execution/trade_enable.py — the master trade-mode state (off / paper / live).

Single source of truth for QISS's execution mode. Read by the GatedAdapter (to
gate orders) and by the adapter-selection layer (to pick the paper vs live
adapter, execution/adapter_factory.py). Three states:

  OFF   — place no orders at all.
  PAPER — place orders with the in-process PaperCoinbaseAdapter: real Coinbase
          market data, real order flow/pricing/reconcile, SIMULATED fills,
          FAKE money.
  LIVE  — place orders against the LIVE venue: real money.

Two inputs, in precedence order (unchanged from the original boolean design):

  1. LIVE DB OVERRIDE — the trade_enabled singleton's `mode` (migration 012).
     Authoritative once running; flip it to change mode with no redeploy.
  2. CONFIG DEFAULT — config.TRADE_MODE_DEFAULT. The boot/fallback mode; used
     until the DB row is read, or if it never can be.

## is_enabled() is preserved

The GatedAdapter still calls is_enabled() — now DERIVED as (mode != OFF). So
both PAPER and LIVE are "enabled" (orders flow); only OFF gates them. Existing
callers of is_enabled() keep working; mode-aware callers use mode().

## Paper<->live changes the VENUE, so it needs a reconnect

Unlike OFF (which the GatedAdapter honours instantly by no-oping orders),
switching between PAPER and LIVE changes which adapter (simulated or real
Coinbase) the executor trades through — established at connect() time. A pure
in-memory flag flip is NOT enough: the executor must switch adapters. So this
class fires an `on_mode_change(old, new)` async callback whenever the mode
changes, off the refresh loop, so a mode flip propagates without a redeploy —
but it is the application's job to act on it. run_executor.py blocks every
write on the old adapter at once (GatedAdapter.block_all_writes) and restarts
the executor, which boots and reconciles against the new venue, so no order
goes to the wrong venue in the gap.

## Fail-safe default

If the mode can't be determined (DB unreachable at startup, no cached value),
mode() returns the config default, and if THAT is unset it is OFF. "Unsure =>
don't trade" — drifting into PAPER or LIVE by error must be impossible.
"""
from __future__ import annotations

import asyncio
import logging
from enum import Enum
from typing import Awaitable, Callable, Optional

import asyncpg

log = logging.getLogger("executor.trade_enable")

CHANNEL = "trade_enabled_changed"

_POLL_INTERVAL_S = 5.0
_RECONNECT_BASE_S = 1.0
_RECONNECT_MAX_S = 30.0


class TradeMode(str, Enum):
    OFF = "off"
    PAPER = "paper"
    LIVE = "live"


# Fired when the mode changes: (old_mode, new_mode) -> awaitable. The app wires
# this to reconnect/reselect the adapter when the VENUE changes (paper<->live,
# or off->paper/live selecting the endpoint the next connect uses).
ModeChangeCallback = Callable[[TradeMode, TradeMode], Awaitable[None]]


class TradeEnableState:
    """Caches the master trade mode (off/paper/live), refreshed via NOTIFY +
    poll. Preserves is_enabled() as (mode != OFF) for existing callers.

    Lifecycle: construct, `await start()`, then call mode()/is_enabled() freely
    (in-memory, no I/O). `await stop()` on shutdown.
    """

    def __init__(
        self,
        dsn: str,
        pool: asyncpg.Pool,
        *,
        config_default: TradeMode = TradeMode.OFF,
        on_mode_change: Optional[ModeChangeCallback] = None,
        poll_interval_s: float = _POLL_INTERVAL_S,
        statement_cache_size: int = 0,
    ):
        """
        config_default: boot/fallback mode (config.TRADE_MODE_DEFAULT). Accepts a
              TradeMode or a str ('off'|'paper'|'live'). Used only until the DB
              row is read, or if it never can be.
        on_mode_change: optional async callback fired on every mode change, for
              the adapter reconnect/reselect. Not called for the initial read
              (there's no 'previous' mode to transition from) — the app selects
              the initial venue from mode() at startup.
        """
        self._dsn = dsn
        self._pool = pool
        self._config_default = TradeMode(config_default)
        self._on_mode_change = on_mode_change
        self._poll_interval_s = poll_interval_s
        self._stmt_cache = statement_cache_size

        # Cached mode. None => not yet determined (falls back to config default).
        self._mode: Optional[TradeMode] = None
        self._running = False
        self._conn: Optional[asyncpg.Connection] = None
        self._refresh_now = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    # ---- accessors (in-memory, no I/O) ----

    def mode(self) -> TradeMode:
        """Current mode. Fail-safe: config default until the first successful
        read; config default itself defaults to OFF."""
        if self._mode is None:
            return self._config_default
        return self._mode

    def is_enabled(self) -> bool:
        """Derived: are we placing orders at all (paper OR live)? Preserves the
        GatedAdapter's existing contract."""
        return self.mode() != TradeMode.OFF

    def is_live(self) -> bool:
        """True only in LIVE (real money). The adapter-selection layer uses this
        (and is_paper) to pick the venue endpoint."""
        return self.mode() == TradeMode.LIVE

    def is_paper(self) -> bool:
        return self.mode() == TradeMode.PAPER

    # ---- lifecycle ----

    async def start(self) -> None:
        self._running = True
        await self._read_once(initial=True)   # populate before returning
        self._task = asyncio.create_task(self._refresh_loop(), name="trade-mode-refresh")

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._close_conn()

    async def refresh(self) -> TradeMode:
        """Force an immediate re-read and return the new mode."""
        await self._read_once()
        return self.mode()

    # ---- internals ----

    async def _read_once(self, initial: bool = False) -> None:
        """Read the singleton's mode into the cache. On a mode CHANGE (and not
        the initial read), fire on_mode_change so the app can reconnect the
        adapter to the new venue. On failure, keep the last known mode."""
        try:
            row = await self._pool.fetchrow(
                "SELECT mode FROM trade_enabled WHERE id = true"
            )
            if row is not None:
                new_mode = TradeMode(row["mode"])
            else:
                if self._mode is not TradeMode.OFF:
                    log.warning("trade_enabled row missing; treating as OFF")
                new_mode = TradeMode.OFF
        except Exception:
            log.exception("failed to read trade mode; keeping last known")
            return

        old_mode = self._mode
        if new_mode != old_mode:
            log.info("trade mode: %s -> %s", old_mode, new_mode)
        self._mode = new_mode

        # Fire the reconnect hook on a genuine change (not the initial read, and
        # only once we had a prior value to transition from).
        if (not initial and old_mode is not None and new_mode != old_mode
                and self._on_mode_change is not None):
            try:
                await self._on_mode_change(old_mode, new_mode)
            except Exception:
                log.exception("on_mode_change callback failed for %s -> %s",
                              old_mode, new_mode)

    async def _refresh_loop(self) -> None:
        attempt = 0
        while self._running:
            try:
                await self._connect_and_listen()
                attempt = 0
                await self._read_once()   # catch up on connect
                while self._running:
                    try:
                        await asyncio.wait_for(
                            self._refresh_now.wait(), timeout=self._poll_interval_s
                        )
                    except asyncio.TimeoutError:
                        pass  # periodic poll
                    self._refresh_now.clear()
                    if self._conn is None or self._conn.is_closed():
                        raise ConnectionError("trade mode LISTEN connection lost")
                    await self._read_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("trade mode refresh loop error")
            finally:
                await self._close_conn()

            if not self._running:
                break
            attempt += 1
            delay = min(_RECONNECT_MAX_S, _RECONNECT_BASE_S * (2 ** (attempt - 1)))
            await asyncio.sleep(delay)

    async def _connect_and_listen(self) -> None:
        self._conn = await asyncpg.connect(self._dsn, statement_cache_size=self._stmt_cache)
        await self._conn.add_listener(CHANNEL, self._on_notify)

    async def _close_conn(self) -> None:
        conn, self._conn = self._conn, None
        if conn is None:
            return
        # Best effort: the connection is being dropped (often because it
        # already died), so a failure here is expected and changes nothing.
        try:
            await conn.remove_listener(CHANNEL, self._on_notify)
        except Exception:
            log.debug("remove_listener on a closing connection failed", exc_info=True)
        try:
            await conn.close()
        except Exception:
            log.debug("closing the LISTEN connection failed", exc_info=True)

    def _on_notify(self, conn, pid, channel, payload) -> None:
        """Wake the refresh loop to re-read. We re-read the row rather than
        trusting the payload, so the cache always reflects the committed row."""
        self._refresh_now.set()