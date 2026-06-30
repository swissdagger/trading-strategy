"""
producer/async_writer.py — Batched, off-critical-path writer for the
research/audit tables (signals + chains propagation/termination).

WHY THIS EXISTS
The signal process's consume loop processes one bar at a time, and (after the
concurrency revert) the next Kafka bar is not pulled until the current bar's
work — including its DB writes — completes. With the DB remote from the VM,
every synchronous `await conn.execute(...)` on that path costs a full network
round-trip, in series, per write, per bar. That round-trip latency is the
dominant component of the present-moment lag, NOT raw write volume (the universe
is small). The fix is to get the writes that do NOT gate correctness off the
consume path.

WHAT IS SAFE TO DEFER (and what is NOT)
  * signals  (research ground truth)            -> DEFERRED here
  * chains propagation  (audit)                 -> DEFERRED here
  * chains termination  (audit)                 -> DEFERRED here
  * chains INIT                                 -> NOT here; stays SYNCHRONOUS.
        Migration 002 documents a write-before-emit ordering: the chain row
        must be durable before the chain_initiated event is consumed by the
        position manager, because trades.chain_id foreign-keys to chains. Init
        is one write per chain (rare), so keeping it synchronous costs almost
        nothing and preserves that guarantee for when the PM is switched on.
  * trade_signals (the PRODUCT)                 -> NOT here; stays SYNCHRONOUS
        in SignalWriter. The strategy's actual output must never be lost.

DURABILITY CONTRACT (explicitly chosen)
This accepts a BOUNDED LOSS WINDOW on the research/audit tables on a crash: rows
enqueued but not yet flushed are lost if the process dies. That is an accepted
trade for keeping the live path fast. The signals/chains writes are all
idempotent UPSERTs, so anything that IS re-derived later (e.g. the same chain
propagating again, or a warmup re-seed) reconciles cleanly; what is lost is only
the rows for events that never recur. trade_signals and chain_init are NOT
subject to this window.

BATCHING
Each enqueued write is appended to an in-memory buffer per write-kind. A single
drain task flushes on whichever comes first: the buffer reaching max_batch, or
flush_interval_s elapsing. Flushing coalesces same-kind writes into one
multi-row statement (executemany / multi-VALUES), turning N round-trips into 1.
With one symbol this mostly matters for the latency win (off-path); as the
universe grows it also caps round-trips per flush regardless of stream count.

ORDERING
Per chain_id, propagation and termination must apply in the order they were
produced (a terminate after a propagate must not be reordered before it). The
queue is a single FIFO and the drain preserves enqueue order within a flush, and
flushes are sequential, so per-chain order is preserved. Signals are independent
(idempotent on their own unique key) and order-insensitive.

BACKPRESSURE
The queue is bounded (max_queue). If the writer falls permanently behind (DB
down for a long time), enqueue does not block the consume loop — it drops the
oldest research rows and counts the drops (logged), because stalling the live
path to preserve audit rows would defeat the entire purpose. This is the bounded
window made concrete: under sustained DB unavailability the window is the queue
depth, not unbounded memory growth.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Optional

log = logging.getLogger("producer.async_writer")


class BatchedAuditWriter:
    """Owns a bounded queue of pending research/audit writes and a single drain
    task that flushes them to the DB in batches. Exposes persist_* methods whose
    SIGNATURES MATCH the synchronous persistence hooks exactly, so they are a
    drop-in swap at wiring time — the chain machine code is unchanged and stays
    oblivious to whether its writes are synchronous or deferred."""

    def __init__(
        self,
        pool,
        *,
        max_batch: int = 200,
        flush_interval_s: float = 0.25,
        max_queue: int = 50_000,
    ):
        self._pool = pool
        self._max_batch = max_batch
        self._flush_interval_s = flush_interval_s
        self._max_queue = max_queue

        # Single FIFO of (kind, payload_tuple). One queue (not one per kind) so
        # global enqueue order is preserved, which is what guarantees per-chain
        # propagate/terminate ordering. Kinds are separated at flush time.
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue)
        self._task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()
        self._dropped = 0   # count of rows shed under sustained backpressure

    # ----- lifecycle -----------------------------------------------------

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._drain_loop(), name="audit-writer")
            log.info("batched audit writer started (max_batch=%d, flush=%.0fms, max_queue=%d)",
                     self._max_batch, self._flush_interval_s * 1000, self._max_queue)

    async def stop(self) -> None:
        """Signal stop, drain whatever remains (best-effort, bounded by the
        normal flush path), and await the task. Called on clean shutdown so a
        graceful stop does NOT incur the loss window — only a crash does."""
        self._stopped.set()
        if self._task is not None:
            await self._task
            self._task = None
        log.info("batched audit writer stopped (lifetime dropped rows=%d)", self._dropped)

    # ----- enqueue API (signatures mirror the sync persist hooks) --------

    async def persist_signal(self, symbol: str, timeframe: str, ts: datetime,
                             signal: int, asset_class: str,
                             price: Optional[float]) -> None:
        self._offer("signal", (symbol, timeframe, ts, signal, asset_class, price))

    async def persist_chain_propagate(self, chain_id: str, new_level: int,
                                      active_timeframes: list, ts: datetime,
                                      new_step: dict) -> None:
        self._offer("propagate", (chain_id, new_level, active_timeframes, ts, new_step))

    async def persist_chain_terminate(self, chain_id: str, ts: datetime) -> None:
        self._offer("terminate", (chain_id, ts))

    def _offer(self, kind: str, payload: tuple) -> None:
        """Non-blocking enqueue. NEVER awaits / blocks the caller (the consume
        loop): if the queue is full (DB sustained-down), drop the OLDEST pending
        row to make room, so memory is bounded and the live path is never
        stalled to preserve an audit row. Dropping is counted + logged sparsely."""
        try:
            self._queue.put_nowait((kind, payload))
        except asyncio.QueueFull:
            try:
                self._queue.get_nowait()          # shed oldest
                self._dropped += 1
                if self._dropped % 1000 == 1:
                    log.warning(
                        "audit writer queue full — shedding research rows "
                        "(total dropped=%d). DB likely behind/unreachable.",
                        self._dropped,
                    )
                self._queue.put_nowait((kind, payload))
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                # Lost a race with the drain; the row is simply dropped. Counted
                # above already in the common path; this is the rare corner.
                self._dropped += 1

    # ----- drain + flush -------------------------------------------------

    async def _drain_loop(self) -> None:
        """Pull from the queue and flush in batches: flush when the buffer hits
        max_batch OR flush_interval_s passes with anything buffered. On stop,
        flush what remains and exit."""
        buf: list = []
        while True:
            timed_out = False
            try:
                item = await asyncio.wait_for(
                    self._queue.get(), timeout=self._flush_interval_s)
                buf.append(item)
                # Opportunistically pull everything immediately available so a
                # burst becomes one flush rather than many timer-sized ones.
                while len(buf) < self._max_batch:
                    try:
                        buf.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
            except asyncio.TimeoutError:
                timed_out = True

            if buf and (len(buf) >= self._max_batch or timed_out):
                await self._flush(buf)
                buf = []

            if self._stopped.is_set() and self._queue.empty():
                if buf:
                    await self._flush(buf)
                return

    async def _flush(self, items: list) -> None:
        """Group buffered writes by kind and emit one batched statement per kind,
        preserving enqueue order within each kind (so per-chain propagate/
        terminate ordering holds). A flush failure logs and DROPS the batch — it
        does not retry-forever (that would reintroduce unbounded growth) and does
        not raise (that would kill the drain task). The bounded-loss contract
        covers exactly this case."""
        signals: list = []
        propagates: list = []
        terminates: list = []
        for kind, payload in items:
            if kind == "signal":
                signals.append(payload)
            elif kind == "propagate":
                propagates.append(payload)
            elif kind == "terminate":
                terminates.append(payload)

        try:
            async with self._pool.acquire() as conn:
                # One transaction per flush: the whole batch lands atomically or
                # not at all, and it's a single round-trip's worth of protocol
                # overhead amortized across all rows.
                async with conn.transaction():
                    if signals:
                        await self._flush_signals(conn, signals)
                    if propagates:
                        await self._flush_propagates(conn, propagates)
                    if terminates:
                        await self._flush_terminates(conn, terminates)
        except Exception:
            log.exception(
                "audit flush failed — dropping %d signal, %d propagate, %d "
                "terminate rows (bounded-loss contract; live path unaffected)",
                len(signals), len(propagates), len(terminates),
            )

    @staticmethod
    async def _flush_signals(conn, rows: list) -> None:
        # Idempotent UPSERT, batched. executemany over the same prepared
        # statement; ON CONFLICT keeps replays/duplicates harmless.
        await conn.executemany(
            """
            INSERT INTO signals (symbol, timeframe, timestamp, signal, asset_class, price)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (symbol, timeframe, timestamp)
            DO UPDATE SET signal = EXCLUDED.signal, price = EXCLUDED.price
            """,
            rows,
        )

    @staticmethod
    async def _flush_propagates(conn, rows: list) -> None:
        # Per migration 008: a propagation is now a fixed-size chains metadata
        # UPDATE plus an append-only chain_steps INSERT — NOT a growing-JSONB
        # append. Both batched via executemany, order preserved, so multiple
        # propagations of the same chain in one flush apply in step order.
        # step_index = level - 1 (matches the init step at index 0).
        meta = [
            (new_level, active_timeframes, ts, chain_id)
            for (chain_id, new_level, active_timeframes, ts, new_step) in rows
        ]
        steps = [
            (chain_id, new_step["level"] - 1, new_step["level"],
             new_step["timeframe"],
             datetime.fromisoformat(new_step["timestamp"]),
             new_step.get("price"))
            for (chain_id, new_level, active_timeframes, ts, new_step) in rows
        ]
        await conn.executemany(
            """
            UPDATE chains
            SET propagation_levels = $1,
                active_timeframes  = $2,
                last_updated       = $3
            WHERE chain_id = $4
            """,
            meta,
        )
        await conn.executemany(
            """
            INSERT INTO chain_steps
                (chain_id, step_index, level, timeframe, step_time, price)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (chain_id, step_index) DO NOTHING
            """,
            steps,
        )

    @staticmethod
    async def _flush_terminates(conn, rows: list) -> None:
        await conn.executemany(
            "UPDATE chains SET active = false, terminated_at = $1, last_updated = $1 "
            "WHERE chain_id = $2",
            [(ts, chain_id) for (chain_id, ts) in rows],
        )