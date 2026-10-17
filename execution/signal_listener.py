"""
execution/signal_listener.py — low-latency trade_signals consumer.

Bridges the producer (SignalWriter -> trade_signals rows, migration 004) to a
consumer (the IB executor, or any handler) with minimal latency and guaranteed
delivery. Two mechanisms, layered:

  FAST PATH — LISTEN/NOTIFY. A dedicated connection LISTENs on the
    'trade_signal' channel (migration 009's trigger fires it on every insert).
    When a signal commits, this listener is PUSHED the signal_id and dispatches
    it immediately. No polling latency.

  BACKSTOP — cursor catch-up sweep. NOTIFY is best-effort: a notification fired
    while this listener's connection is down is LOST (Postgres doesn't queue
    it). So on every (re)connect we SELECT all rows with created_at strictly
    after our last-seen cursor and dispatch them. This closes the gap the fast
    path can't cover. The (created_at) index from migration 004 makes the sweep
    a range scan, not a full table scan.

Together: the NOTIFY gives speed, the cursor gives a guarantee. Neither alone
is sufficient — NOTIFY can drop across a disconnect, and a pure cursor poll has
the very latency we're removing.

## Idempotent dispatch is REQUIRED of the handler

The same signal_id can be delivered twice: once by the NOTIFY fast path and
again by a catch-up sweep that overlaps it (e.g. a signal commits, we're
notified, then we reconnect and the sweep re-reads it because our cursor hadn't
advanced past it yet). This is by design — de-duplication belongs at the
handler, which acts on a signal, exactly as the cross-team contract already
requires (the deterministic signal_id exists precisely so a re-read dedupes;
see signal_writer.py and migration 004). The listener guarantees AT-LEAST-ONCE
delivery; the handler must be idempotent on signal_id. The listener does keep a
small recently-seen set to suppress the most common duplicate (fast-path then
immediate sweep), but that is an optimisation, not the correctness boundary —
the handler is.

## Cursor semantics

The cursor is a created_at timestamp (the row's DB-write time, migration 004's
read-cursor column). We advance it to the max created_at we've dispatched. On
restart the cursor is reloaded (the caller supplies the initial value — e.g.
persisted, or 'now' to only act on signals from startup forward, or epoch to
replay everything). created_at can tie (two rows same instant); we handle that
by making the sweep '>=' the cursor and relying on the recently-seen set +
handler idempotency to suppress the re-dispatched boundary rows, rather than
'>' which could SKIP a row that shares the cursor's exact timestamp.

created_at is the INSERT's transaction start, not its commit, so with
concurrent writers a row with an earlier created_at can commit after a later
one was dispatched. The sweep therefore starts _SWEEP_LAG_S before the cursor;
the same dedupe makes the overlap safe.

## Failed signals are retried

A signal whose handler raised is kept in a retry set and re-dispatched with
per-signal exponential backoff (5 s up to 5 min) while the listener runs; one
that keeps failing is logged at ERROR and counted in stuck_signals(), never
dropped. The persisted cursor never moves
past the oldest such signal (later successes still advance the in-memory
high-water), so after a restart the sweep re-reads it.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Dict, Optional

import asyncpg

from ops.alerts import alert

log = logging.getLogger("executor.signal_listener")

# The NOTIFY channel migration 009's trigger fires on.
CHANNEL = "trade_signal"

# How many recently-dispatched signal_ids to remember for fast-path/sweep
# de-dup. Small: it only needs to cover the overlap window between a NOTIFY and
# a near-simultaneous catch-up sweep. Handler idempotency is the real backstop.
_RECENT_CAPACITY = 4096

# How far behind the cursor the catch-up sweep starts, to pick up rows whose
# created_at (transaction start) precedes a row already dispatched but which
# committed after it. Re-read rows are deduped.
_SWEEP_LAG_S = 30.0

# Signals whose handler raised are retried with per-signal exponential
# backoff: 5 s, 10 s, ... capped at 5 min. One that keeps failing is never
# dropped (it may be an EXIT); past _STUCK_AFTER attempts it is logged at
# ERROR, then about hourly, and counted in stuck_signals().
_RETRY_INTERVAL_S = 5.0
_RETRY_MAX_S = 300.0
_STUCK_AFTER = 10
_STUCK_LOG_EVERY = 12

# Reconnect backoff bounds for the dedicated LISTEN connection.
_RECONNECT_BASE_S = 1.0
_RECONNECT_MAX_S = 30.0


# A handler takes the full signal row (dict) and acts on it. Must be idempotent
# on row['signal_id'] (see module docstring). Returns when the signal has been
# handled; raising is logged and does NOT crash the listener (one bad signal
# must not stop the stream); the signal is retried with backoff and the
# persisted cursor does not move past it until it succeeds.
SignalHandler = Callable[[dict], Awaitable[None]]


class SignalListener:
    """Consumes trade_signals via LISTEN/NOTIFY with a cursor catch-up backstop.

    Owns a DEDICATED asyncpg connection for LISTEN (a listening connection is
    occupied for its lifetime and must not come from the shared pool). Uses the
    shared pool only for the catch-up SELECTs.
    """

    def __init__(
        self,
        dsn: str,
        pool: asyncpg.Pool,
        handler: SignalHandler,
        *,
        initial_cursor: Optional[datetime] = None,
        statement_cache_size: int = 0,
        on_cursor_advance: Optional[Callable[[datetime], Awaitable[None]]] = None,
    ):
        """
        dsn:   connection string for the dedicated LISTEN connection (same
               TRADING_DB_URL the rest of the system uses).
        pool:  shared pool, used for catch-up SELECTs only.
        handler: idempotent per-signal callback.
        initial_cursor: created_at high-water to start from. None => 'now'
               (only signals committed from startup forward are acted on). Pass
               a persisted value to resume, or datetime.min (tz-aware) to replay
               the whole table.
        statement_cache_size: passed to the dedicated connect; 0 for pgbouncer/
               Supabase compatibility, matching the pool convention elsewhere.
        on_cursor_advance: awaited with the cursor to persist each time it
               advances (after a successful dispatch), so the caller can resume
               there after a restart. Never later than the oldest signal whose
               handler failed. A failure is logged and retried on the next
               advance: the worst case is replaying a few signals the handler
               already deduplicates.
        """
        self._dsn = dsn
        self._pool = pool
        self._handler = handler
        self._cursor: datetime = initial_cursor or datetime.now(timezone.utc)
        self._stmt_cache = statement_cache_size
        self._on_cursor_advance = on_cursor_advance
        # The last cursor value persisted (or the starting one): only a later
        # value is saved.
        self._persisted: datetime = self._cursor
        # Signals whose handler raised, by signal_id -> row, retried with
        # backoff; attempts and next retry time per signal.
        self._failed: Dict[str, dict] = {}
        self._attempts: Dict[str, int] = {}
        self._next_retry: Dict[str, float] = {}

        self._conn: Optional[asyncpg.Connection] = None
        self._running = False
        self._notify_queue: asyncio.Queue[str] = asyncio.Queue()
        # Set when the current connection is detected dead (termination
        # callback or explicit close), so the drain loop reacts immediately.
        # Recreated per connection in _connect_and_listen.
        self._terminated: Optional[asyncio.Event] = None
        # Recently dispatched ids, insertion-ordered for cheap LRU eviction.
        self._recent: "OrderedDict[str, None]" = OrderedDict()

    @property
    def cursor(self) -> datetime:
        """Current created_at high-water. Callers may persist this to resume
        across restarts without replaying."""
        return self._cursor

    def connected(self) -> bool:
        """The LISTEN connection is open (for the heartbeat)."""
        return self._conn is not None and not self._conn.is_closed()

    async def run(self) -> None:
        """Run until cancelled. Maintains the LISTEN connection (reconnecting
        with backoff), runs a catch-up sweep on every (re)connect, and
        dispatches notifications as they arrive. Never returns normally."""
        self._running = True
        attempt = 0
        while self._running:
            try:
                await self._connect_and_listen()
                attempt = 0
                # Catch up on anything missed while we were disconnected (or,
                # on first connect, anything after the initial cursor) BEFORE
                # draining live notifications — so a signal that arrived during
                # the connect gap is handled by the sweep, and the live queue
                # then carries only genuinely new ones.
                await self._catch_up()
                await self._drain_notifications()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("signal listener connection error")
            finally:
                await self._close_conn()

            if not self._running:
                break
            attempt += 1
            delay = min(_RECONNECT_MAX_S, _RECONNECT_BASE_S * (2 ** (attempt - 1)))
            log.info("signal listener reconnecting in %.1fs (attempt %d)", delay, attempt)
            await asyncio.sleep(delay)

    async def stop(self) -> None:
        self._running = False
        await self._close_conn()

    # ---- connection lifecycle ----

    async def _connect_and_listen(self) -> None:
        self._conn = await asyncpg.connect(self._dsn, statement_cache_size=self._stmt_cache)
        # Reset the per-connection termination signal for this fresh connection.
        self._terminated = asyncio.Event()
        self._conn.add_termination_listener(self._on_terminate)
        await self._conn.add_listener(CHANNEL, self._on_notify)
        log.info("signal listener connected; LISTEN %s", CHANNEL)

    async def _close_conn(self) -> None:
        conn, self._conn = self._conn, None
        # Signal the drain loop that this connection is gone, so it stops
        # waiting immediately rather than after its poll timeout.
        if self._terminated is not None:
            self._terminated.set()
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
        """asyncpg listener callback — runs in the connection's read loop.
        Must not block or do async work; just enqueue the signal_id for the
        drain loop to handle. payload is the signal_id (migration 009)."""
        if payload:
            self._notify_queue.put_nowait(payload)

    def _on_terminate(self, conn) -> None:
        """asyncpg termination callback — fires the instant the connection is
        detected dead (server close, network drop). Wakes the drain loop
        immediately so reconnect + catch-up start without waiting for the
        liveness poll."""
        if self._terminated is not None:
            self._terminated.set()

    # ---- dispatch paths ----

    async def _drain_notifications(self) -> None:
        """Consume signal_ids pushed by NOTIFY until the connection drops.

        Primary drop detection is the termination event (_on_terminate), which
        fires the instant asyncpg sees the connection die — so a real drop
        triggers reconnect + catch-up near-instantly. A periodic liveness poll
        remains as a secondary net for a silently-wedged connection that never
        fires termination.
        """
        while self._running:
            await self._maybe_retry_failed()
            get_task = asyncio.ensure_future(self._notify_queue.get())
            term_task = asyncio.ensure_future(self._terminated.wait())
            try:
                done, _ = await asyncio.wait(
                    {get_task, term_task},
                    timeout=5.0,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                pass

            if term_task in done:
                get_task.cancel()
                raise ConnectionError("LISTEN connection terminated")
            term_task.cancel()

            if get_task in done:
                signal_id = get_task.result()
                await self._dispatch_by_id(signal_id)
                continue

            # Neither fired within the timeout — secondary liveness check.
            get_task.cancel()
            if self._conn is None or self._conn.is_closed():
                raise ConnectionError("LISTEN connection lost")

    async def _catch_up(self) -> None:
        """Dispatch every signal with created_at >= cursor - _SWEEP_LAG_S that
        we haven't already dispatched. Runs on each (re)connect. Ordered by
        created_at so the cursor advances monotonically and signals are handled
        in write order (which, per migration 004, consumers may rely on
        alongside decided_at for strategy semantics)."""
        rows = await self._pool.fetch(
            """
            SELECT signal_id, position_key, chain_id, direction, action,
                   symbol, asset_class, strategy, trigger_level, decision_price,
                   exit_reason, fraction, decided_at, created_at
            FROM trade_signals
            WHERE created_at >= $1
            ORDER BY created_at ASC, signal_id ASC
            """,
            self._cursor - timedelta(seconds=_SWEEP_LAG_S),
        )
        if rows:
            log.info("signal catch-up sweep: %d row(s) from cursor %s",
                     len(rows), self._cursor.isoformat())
        for row in rows:
            await self._dispatch_row(dict(row))

    async def _dispatch_by_id(self, signal_id: str) -> None:
        """Fast path: fetch the full row for a notified signal_id and dispatch.
        A point lookup on the PK. If the row is missing (shouldn't happen — the
        NOTIFY is post-commit), we log and move on."""
        if signal_id in self._recent:
            return  # already handled via a catch-up sweep; suppress duplicate
        row = await self._pool.fetchrow(
            """
            SELECT signal_id, position_key, chain_id, direction, action,
                   symbol, asset_class, strategy, trigger_level, decision_price,
                   exit_reason, fraction, decided_at, created_at
            FROM trade_signals
            WHERE signal_id = $1
            """,
            signal_id,
        )
        if row is None:
            log.warning("notified signal_id %s not found (post-commit race?)", signal_id)
            return
        await self._dispatch_row(dict(row))

    async def _dispatch_row(self, row: dict) -> None:
        """Run the handler for one signal, then advance dedup state and cursor.

        A handler that raises leaves the signal in the retry set (re-dispatched
        with backoff) and holds the PERSISTED cursor at or before
        it, even while later signals succeed and move the in-memory high-water
        on. At-least-once delivery; the handler's idempotency makes retries
        safe.
        """
        signal_id = row["signal_id"]
        if signal_id in self._recent:
            self._forget_failure(signal_id)
            return
        try:
            await self._handler(row)
        except Exception:
            self._note_failure(signal_id, row)
            return
        self._forget_failure(signal_id)
        self._mark_seen(signal_id)
        created_at = row["created_at"]
        if created_at is not None and created_at > self._cursor:
            self._cursor = created_at
        await self._persist_cursor()

    async def _persist_cursor(self) -> None:
        """Save the cursor, clamped to the oldest failed signal so a restart
        re-reads it. Only ever saves a later value than the last one saved."""
        target = self._cursor
        held = [r["created_at"] for r in self._failed.values()
                if r.get("created_at") is not None]
        if held:
            target = min(target, min(held))
        if self._on_cursor_advance is None or target <= self._persisted:
            return
        try:
            await self._on_cursor_advance(target)
        except Exception:
            log.exception("persisting listener cursor %s failed; will "
                          "retry on the next advance", target.isoformat())
            return
        self._persisted = target

    def _note_failure(self, signal_id: str, row: dict) -> None:
        n = self._attempts.get(signal_id, 0) + 1
        self._attempts[signal_id] = n
        self._failed[signal_id] = row
        delay = min(_RETRY_INTERVAL_S * 2 ** (n - 1), _RETRY_MAX_S)
        self._next_retry[signal_id] = time.monotonic() + delay
        if n < _STUCK_AFTER:
            log.exception("handler failed for signal %s (attempt %d); retrying in %.0fs",
                          signal_id, n, delay)
        elif (n - _STUCK_AFTER) % _STUCK_LOG_EVERY == 0:
            log.error("signal %s (%s %s %s) has failed %d times and is STUCK — still "
                      "retrying every %.0fs; the persisted cursor is held at it",
                      signal_id, row.get("action"), row.get("strategy"),
                      row.get("position_key"), n, delay, exc_info=True,
                      extra=alert(f"stuck:{signal_id}", every=3600))

    def _forget_failure(self, signal_id: str) -> None:
        self._failed.pop(signal_id, None)
        self._attempts.pop(signal_id, None)
        self._next_retry.pop(signal_id, None)

    def stuck_signals(self) -> int:
        """Signals that have failed at least _STUCK_AFTER times (for health)."""
        return sum(1 for n in self._attempts.values() if n >= _STUCK_AFTER)

    async def _maybe_retry_failed(self) -> None:
        """Re-dispatch failed signals whose backoff has elapsed, oldest
        first."""
        if not self._failed:
            return
        now = time.monotonic()
        due = [r for sid, r in self._failed.items()
               if self._next_retry.get(sid, 0) <= now]
        if not due:
            return
        due.sort(key=lambda r: (r.get("created_at") is None, r.get("created_at") or 0,
                                r["signal_id"]))
        log.info("retrying %d failed signal(s)", len(due))
        for row in due:
            await self._dispatch_row(row)

    def _mark_seen(self, signal_id: str) -> None:
        self._recent[signal_id] = None
        self._recent.move_to_end(signal_id)
        while len(self._recent) > _RECENT_CAPACITY:
            self._recent.popitem(last=False)