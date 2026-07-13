"""
producer/symbol_workers.py — per-symbol ordered worker pool.

THE PROBLEM. signal_process.run() awaits each bar to completion before pulling
the next from Kafka. Every forecast-eligible bar costs one BLOCKING Sumtyme call
(to_thread, up to forecast_timeout_s) plus its DB round-trips, all in series,
across the WHOLE universe. Per-bar cost exceeds the bar arrival rate, so consumer
lag grows without bound — the observed multi-day stall (38k messages behind, rows
landing with created_at ~39h after their bar timestamp).

The work is embarrassingly parallel ACROSS symbols and strictly serial WITHIN a
symbol. AAPL's forecast has nothing to do with BTCUSDT's; but two AAPL bars must
be processed in arrival order or the chain machine's timestamp-ordered
propagation (next_sig["timestamp"] >= chain["last_timestamp"]) silently
mis-propagates.

THE SHAPE. One bounded queue + one worker task PER SYMBOL. The consume loop does
nothing but decode a bar and hand it to that symbol's queue; N symbols then make
forward progress concurrently, each strictly in order.

This is the shape signal_process.run()'s own docstring prescribes, and it
explicitly warns against the alternative:

    "If cross-symbol throughput is needed, the correct shape is per-symbol
     ordered workers (one queue/task per symbol), not a blanket create_task per
     bar — ask before reintroducing concurrency here."

A blanket create_task per bar would let two bars for the SAME symbol run
concurrently, contending for the per-symbol lock and reaching the ChainView out
of order. That breaks propagation WITHOUT RAISING. This module is the sanctioned
alternative: the per-symbol lock in the router/runtime is preserved and still
load-bearing (the worker is what makes contention on it rare, not what removes
the need for it).

BACKPRESSURE. Queues are bounded. When a symbol's queue is full, we BLOCK the
consume loop rather than shed the bar: unlike the audit rows in async_writer,
a dropped BAR is a hole in the chain machine's input — the buffer/propagation
state is derived from a complete ordered bar sequence, so silently dropping one
corrupts chain state rather than merely losing an audit row. Blocking applies
real backpressure to Kafka (the fetch simply stops), which is the correct,
lossless response to "we cannot keep up". If it blocks persistently, the symbol
is genuinely too slow — that is a capacity signal to surface, not to paper over.

OFFSETS. Because work is now IN FLIGHT when the consume loop moves on, offsets
must NOT be auto-committed (an auto-commit would acknowledge bars still sitting
in a queue; a crash would drop them with no replay). The consume loop commits
only bars that have been fully PROCESSED — see signal_process.run(). This module
exposes the completion signal that makes that possible: each bar is enqueued with
its offset, and `done_offsets()` reports the offsets of bars whose processing has
finished.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Dict, Optional

log = logging.getLogger("producer.symbol_workers")

# Handler for one bar: the router's on_bar. Returns when the bar is fully
# processed (chains updated, events published, signals written).
BarHandler = Callable[[object], Awaitable[None]]


class SymbolWorkerPool:
    """Fans bars out to one ordered worker per symbol.

    Ordering guarantee: bars submitted for the SAME symbol are processed strictly
    in submission order (single consumer per FIFO queue, one bar at a time).
    Bars for DIFFERENT symbols proceed concurrently.
    """

    def __init__(
        self,
        handle_bar: BarHandler,
        *,
        max_queue_per_symbol: int = 1000,
    ):
        self._handle_bar = handle_bar
        self._max_queue = max_queue_per_symbol

        self._queues: Dict[str, asyncio.Queue] = {}
        self._workers: Dict[str, asyncio.Task] = {}
        self._stopping = False

        # Offsets of bars fully processed, awaiting commit by the consume loop.
        # Workers append here on completion; the consume loop drains it.
        self._done_offsets: list[int] = []
        # In-flight count, so drain() can wait for genuine quiescence.
        self._inflight = 0
        self._idle = asyncio.Event()
        self._idle.set()

    # ----- submission -----------------------------------------------------

    async def submit(self, symbol: str, bar, offset: int) -> None:
        """Hand one bar to its symbol's worker, creating the worker on first
        sight of the symbol. AWAITS if that symbol's queue is full — deliberate
        backpressure; see the module docstring on why a bar must never be shed."""
        if self._stopping:
            return
        q = self._queue_for(symbol)
        self._inflight += 1
        self._idle.clear()
        if q.full():
            # Visible, because a persistently full queue is the capacity signal
            # that this symbol cannot keep up — exactly what went unnoticed for a
            # day. Logged before we block, so the block itself is attributable.
            log.warning(
                "symbol %s queue FULL (%d) — applying backpressure to the bar "
                "consumer (this symbol is not keeping up)",
                symbol, self._max_queue,
            )
        await q.put((bar, offset))

    def _queue_for(self, symbol: str) -> asyncio.Queue:
        q = self._queues.get(symbol)
        if q is None:
            q = asyncio.Queue(maxsize=self._max_queue)
            self._queues[symbol] = q
            self._workers[symbol] = asyncio.create_task(
                self._worker(symbol, q), name=f"symbol-worker-{symbol}")
            log.info("started worker for symbol %s (queue max=%d)",
                     symbol, self._max_queue)
        return q

    # ----- the per-symbol worker -----------------------------------------

    async def _worker(self, symbol: str, q: asyncio.Queue) -> None:
        """Process this symbol's bars, ONE AT A TIME, in FIFO order. The
        one-at-a-time discipline is the ordering guarantee — do not add
        concurrency inside this loop."""
        while True:
            bar, offset = await q.get()
            try:
                await self._handle_bar(bar)
            except Exception:
                # The router already isolates machine + publish failures; this is
                # the final backstop. A bar that fails is DROPPED (logged), never
                # allowed to kill the worker — a dead worker would stall this
                # symbol forever while the rest of the system looked healthy.
                log.exception(
                    "worker failed processing bar symbol=%s tf=%s ts=%s — "
                    "dropping this bar (worker continues)",
                    symbol, getattr(bar, "timeframe", "?"),
                    getattr(bar, "timestamp", "?"),
                )
            finally:
                # The offset is reported done whether the bar SUCCEEDED or FAILED.
                # A bar that failed deterministically (bad payload, a bug) would
                # fail identically on replay, so withholding its offset would wedge
                # the commit point forever on a poison bar. Failures are loud in
                # the log; the commit point tracks CONSUMPTION, not success.
                self._done_offsets.append(offset)
                self._inflight -= 1
                if self._inflight == 0:
                    self._idle.set()
                q.task_done()

    # ----- offset handoff -------------------------------------------------

    def take_done_offsets(self) -> list[int]:
        """Drain and return the offsets of bars finished since the last call.
        The consume loop uses these to advance its commit point."""
        out = self._done_offsets
        self._done_offsets = []
        return out

    # ----- lifecycle ------------------------------------------------------

    async def drain(self, timeout: float = 30.0) -> None:
        """Wait for all queued + in-flight bars to finish. Called on clean
        shutdown so a graceful stop commits everything it actually processed
        instead of replaying it."""
        try:
            await asyncio.wait_for(self._idle.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            log.warning(
                "drain timed out after %.0fs with %d bars in flight — "
                "these will be replayed on restart (offsets uncommitted)",
                timeout, self._inflight,
            )

    async def stop(self, drain_timeout: float = 30.0) -> None:
        """Drain, then cancel the workers."""
        self._stopping = True
        await self.drain(timeout=drain_timeout)
        for task in self._workers.values():
            task.cancel()
        for task in self._workers.values():
            try:
                await task
            except asyncio.CancelledError:
                pass
        log.info("symbol worker pool stopped (%d workers)", len(self._workers))

    def worker_count(self) -> int:
        return len(self._workers)

    def queue_depths(self) -> Dict[str, int]:
        """Per-symbol backlog — the thing to graph. A depth that only grows is
        this bug recurring."""
        return {s: q.qsize() for s, q in self._queues.items()}