"""
execution/trade_enable.py — the master trade-enable state.

Single source of truth for "may QISS place live orders right now." Read by the
GatedAdapter before every order-placing call. Two inputs, in precedence order:

  1. LIVE DB OVERRIDE — the trade_enabled singleton (migration 010). Once the
     process is running, this row is authoritative: flipping it turns live
     execution on/off instantly, no redeploy (same reason kill_switch is a row).

  2. CONFIG DEFAULT — config.TRADE_ENABLED_DEFAULT. Only used as the STARTING
     state / fallback if the DB row can't be read at all. The DB wins whenever
     it's readable.

The design goal from the requirement: "master enable/disable that no-ops safely
when off, config default + live DB override." This class owns the state; the
GatedAdapter owns the no-op-when-off behaviour.

## Freshness: NOTIFY fast path + poll backstop

A naive reader would query the row on every order — a DB round-trip on the hot
path. Instead this caches the state and refreshes it two ways:

  - FAST: LISTEN on 'trade_enabled_changed' (migration 010's trigger). A flip
    updates the cache near-instantly.
  - BACKSTOP: a periodic poll (default 5s) re-reads the row, so a missed NOTIFY
    (listener disconnected during the flip) self-corrects within one interval.

This is the same belt-and-suspenders pattern as the signal listener: push for
speed, poll for guarantee. The cached read that the GatedAdapter calls is then
just an in-memory bool — no per-order DB hit.

## Fail-safe default

If the state can't be determined (DB unreachable at startup and no cached
value), is_enabled() returns FALSE. For a system moving real money, "unsure =>
don't trade" is the only safe default. An operator turning trading ON is a
deliberate act; drifting ON due to an error must be impossible.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import asyncpg

log = logging.getLogger("executor.trade_enable")

CHANNEL = "trade_enabled_changed"

_POLL_INTERVAL_S = 5.0
_RECONNECT_BASE_S = 1.0
_RECONNECT_MAX_S = 30.0


class TradeEnableState:
    """Caches the master trade-enable flag, refreshed via NOTIFY + poll.

    Lifecycle: construct, `await start()` (does the initial read + starts the
    refresh task), then call is_enabled() freely (in-memory, no I/O). `await
    stop()` on shutdown.
    """

    def __init__(
        self,
        dsn: str,
        pool: asyncpg.Pool,
        *,
        config_default: bool = False,
        poll_interval_s: float = _POLL_INTERVAL_S,
        statement_cache_size: int = 0,
    ):
        """
        dsn:  connection string for the dedicated LISTEN connection.
        pool: shared pool, for the poll/read queries.
        config_default: the boot/fallback state (config.TRADE_ENABLED_DEFAULT).
              Used only until the DB row is read, or if it never can be.
        """
        self._dsn = dsn
        self._pool = pool
        self._config_default = config_default
        self._poll_interval_s = poll_interval_s
        self._stmt_cache = statement_cache_size

        # Cached state. None => not yet determined (is_enabled falls back to the
        # fail-safe below until the first successful read).
        self._enabled: Optional[bool] = None
        self._running = False
        self._conn: Optional[asyncpg.Connection] = None
        self._refresh_now = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    def is_enabled(self) -> bool:
        """The current master trade-enable state, in memory (no I/O).

        Fail-safe: if the state has not yet been determined (initial read hasn't
        succeeded), returns the config default — which itself defaults to False.
        Never returns True unless something (DB row or explicit config default)
        affirmatively said so.
        """
        if self._enabled is None:
            return self._config_default
        return self._enabled

    async def start(self) -> None:
        """Do the initial read and start the background refresh task."""
        self._running = True
        await self._read_once()   # populate cache before returning
        self._task = asyncio.create_task(self._refresh_loop(), name="trade-enable-refresh")

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._close_conn()

    async def refresh(self) -> bool:
        """Force an immediate re-read from the DB and return the new state.
        Useful at boot after connect()/reconcile, or in tests."""
        await self._read_once()
        return self.is_enabled()

    # ---- internals ----

    async def _read_once(self) -> None:
        """Read the singleton row into the cache. On failure, leave the cache as
        it is (so a transient DB blip doesn't flip us to the fail-safe if we
        already had a good value)."""
        try:
            row = await self._pool.fetchrow(
                "SELECT enabled FROM trade_enabled WHERE id = true"
            )
            if row is not None:
                new_state = bool(row["enabled"])
                if new_state != self._enabled:
                    log.info("trade_enabled -> %s", new_state)
                self._enabled = new_state
            else:
                # Row missing entirely (migration not applied?). Fail safe.
                if self._enabled is not False:
                    log.warning("trade_enabled row missing; treating as DISABLED")
                self._enabled = False
        except Exception:
            log.exception("failed to read trade_enabled; keeping last known state")

    async def _refresh_loop(self) -> None:
        """Maintain a LISTEN connection and poll periodically, refreshing the
        cache on either trigger. Reconnects with backoff."""
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
                        raise ConnectionError("trade_enabled LISTEN connection lost")
                    await self._read_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("trade_enable refresh loop error")
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
        try:
            await conn.remove_listener(CHANNEL, self._on_notify)
        except Exception:
            pass
        try:
            await conn.close()
        except Exception:
            pass

    def _on_notify(self, conn, pid, channel, payload) -> None:
        """Wake the refresh loop to re-read. We re-read the row rather than
        trusting the payload, so the cache always reflects the committed row."""
        self._refresh_now.set()
