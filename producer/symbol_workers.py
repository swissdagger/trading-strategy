"""
producer/symbol_workers.py — per-symbol ORDERED pipeline: prepare -> forecast -> apply.

THE PROBLEM, ROUND ONE. signal_process.run() used to await each bar to
completion before pulling the next from Kafka — one blocking Sumtyme call per
bar, in series, across the WHOLE universe. Consumer lag grew without bound (the
observed multi-day stall). The fix was one ordered worker per symbol: symbols
proceed concurrently, each strictly serial.

THE PROBLEM, ROUND TWO. "Strictly serial within a symbol" means one symbol's
forecasts are serial too, so a symbol's throughput is 1 / wall-latency of a
single call (~8 s measured at N=1, ~16 s wall at N=6). A dense ladder — e.g.
[30s, 45s, 1m, 90s, 2m, 3m, 15m, 30m, 1h, 2h, 4h, 6h] — needs a forecast every
few seconds for ONE symbol. No symbol count fixes that; the per-symbol
serialisation itself is the ceiling.

WHAT ACTUALLY HAS TO BE SERIAL. Only the parts that touch machine state:

  * APPENDING to a (symbol, timeframe) buffer — buffer order IS Sumtyme's
    definition of "one step".
  * APPLYING a forecast — writing _latest_signals[idx] and running
    INIT/PROPAGATE/TERMINATE — because the state machine's only notion of
    "newer" is the last write to that cache.

The Sumtyme call between them reads a frozen snapshot and writes nothing. It
can overlap with anything. So the chain machine was split
(ServiceTwoChainMachine.prepare / forecast / apply) and this pool runs, per
symbol, a two-task pipeline:

    stager:   bar in -> prepare (under lock) -> spawn forecast task
                     -> push (routed, task) onto an apply FIFO
    applier:  pop FIFO HEAD -> await ITS task -> apply (under lock)
                     -> publish -> retire offset

The FIFO of futures is the whole ordering mechanism. The applier never looks
past the head: if bar 7's forecast finishes before bar 6's, bar 7 waits in the
FIFO with its result ready until 6 is applied. Prepare order == arrival order
(single stager, awaits each prepare before the next) and apply order == FIFO
order == prepare order. Forecast COMPLETION order is free, and irrelevant,
because every result is attached to the bar it came from, never to "the latest
bar". Cost: head-of-line latency. Benefit: strict order with concurrent compute.

BACKPRESSURE, unchanged in spirit. Both the input queue and the apply FIFO are
bounded; a full one BLOCKS the stage feeding it (never sheds — a dropped BAR
is a hole in the chain machine's input and corrupts state, unlike an audit
row). Full FIFO -> stager blocks -> input queue fills -> consume loop blocks ->
Kafka fetch stops. A persistently full queue is a capacity signal to surface,
not to paper over.

FAIRNESS, new. Serial processing gave it for free: a symbol could only ever
have one forecast in flight. Now a 6h boundary prepares a dozen bars for one
symbol at once, and without a cap they would grab every permit of the shared
ForecastGate and starve every other symbol's baseline. `max_inflight_per_symbol`
bounds how many of ONE symbol's forecasts may be in flight (below the global
gate); it binds only at boundaries, which is exactly when it matters.

OFFSETS, unchanged. Offsets are committed only at the contiguous low-water
mark of RETIRED bars (OffsetTracker in signal_process). A bar is retired here
only after apply() returns (or after prepare() says there is nothing to do, or
after a failure — the commit point tracks CONSUMPTION, not success, so a
poison bar cannot wedge it). In-flight bars are never retired, so a crash
replays them; the machine's monotonic guard makes the replay idempotent.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Dict, Optional

log = logging.getLogger("producer.symbol_workers")

# The three stages, as the router exposes them (MultiLadderRouter):
#   prepare(bar)   -> RoutedBar | None   (under symbol lock; None = nothing to do)
#   forecast(rb)   -> None               (no lock; concurrent-safe)
#   apply(rb)      -> None               (under symbol lock)
Prepare = Callable[[object], Awaitable[object]]
Forecast = Callable[[object], Awaitable[None]]
Apply = Callable[[object], Awaitable[None]]


class _SymbolPipeline:
    """The per-symbol state: two bounded queues, two tasks, one in-flight cap."""
    __slots__ = ("input", "fifo", "stager", "applier", "inflight_sem", "tasks")

    def __init__(self, max_queue: int, max_inflight: int):
        self.input: asyncio.Queue = asyncio.Queue(maxsize=max_queue)
        self.fifo: asyncio.Queue = asyncio.Queue(maxsize=max_queue)
        self.inflight_sem = asyncio.Semaphore(max_inflight)
        self.stager: Optional[asyncio.Task] = None
        self.applier: Optional[asyncio.Task] = None
        self.tasks: set[asyncio.Task] = set()   # live forecast tasks (for cancel)


class SymbolWorkerPool:
    """Fans bars out to one ordered prepare/forecast/apply pipeline per symbol.

    Ordering guarantee: for bars submitted for the SAME symbol, prepare() is
    called in submission order and apply() is called in submission order.
    Forecasts for one symbol may run concurrently (up to max_inflight_per_symbol)
    and complete in any order. Bars for DIFFERENT symbols proceed concurrently.
    """

    def __init__(
        self,
        prepare: Prepare,
        forecast: Forecast,
        apply: Apply,
        *,
        max_queue_per_symbol: int = 1000,
        max_inflight_per_symbol: int = 4,
    ):
        if max_inflight_per_symbol < 1:
            raise ValueError("max_inflight_per_symbol must be >= 1")
        self._prepare = prepare
        self._forecast = forecast
        self._apply = apply
        self._max_queue = max_queue_per_symbol
        self._max_inflight = max_inflight_per_symbol

        self._pipes: Dict[str, _SymbolPipeline] = {}
        self._stopping = False

        # Offsets of bars fully retired, awaiting commit by the consume loop.
        self._done_offsets: list[int] = []
        # Bars submitted but not yet retired, so drain() can wait for quiescence.
        self._inflight = 0
        self._idle = asyncio.Event()
        self._idle.set()

    # ----- submission -----------------------------------------------------

    async def submit(self, symbol: str, bar, offset: int) -> None:
        """Hand one bar to its symbol's pipeline, creating it on first sight of
        the symbol. AWAITS if that symbol's input queue is full — deliberate
        backpressure; see the module docstring on why a bar must never be shed."""
        if self._stopping:
            return
        pipe = self._pipe_for(symbol)
        self._inflight += 1
        self._idle.clear()
        if pipe.input.full():
            # Visible, because a persistently full queue is the capacity signal
            # that this symbol cannot keep up — exactly what went unnoticed for a
            # day. Logged before we block, so the block itself is attributable.
            log.warning(
                "symbol %s queue FULL (%d) — applying backpressure to the bar "
                "consumer (this symbol is not keeping up)",
                symbol, self._max_queue,
            )
        await pipe.input.put((bar, offset))

    def _pipe_for(self, symbol: str) -> _SymbolPipeline:
        pipe = self._pipes.get(symbol)
        if pipe is None:
            pipe = _SymbolPipeline(self._max_queue, self._max_inflight)
            self._pipes[symbol] = pipe
            pipe.stager = asyncio.create_task(
                self._stager(symbol, pipe), name=f"symbol-stager-{symbol}")
            pipe.applier = asyncio.create_task(
                self._applier(symbol, pipe), name=f"symbol-applier-{symbol}")
            log.info("started pipeline for symbol %s (queue max=%d, inflight max=%d)",
                     symbol, self._max_queue, self._max_inflight)
        return pipe

    # ----- stage 1 + 2: the stager ---------------------------------------

    async def _stager(self, symbol: str, pipe: _SymbolPipeline) -> None:
        """Take this symbol's bars ONE AT A TIME, in FIFO order: prepare each
        (appends to the buffer, under the lock), spawn its forecast, and push
        it onto the apply FIFO in the SAME order. The one-at-a-time discipline
        here is what makes buffer order == arrival order — do not add
        concurrency inside this loop."""
        while True:
            bar, offset = await pipe.input.get()
            try:
                try:
                    routed = await self._prepare(bar)
                except Exception:
                    # Router isolates per-machine failures; this is the backstop.
                    log.exception(
                        "prepare failed symbol=%s tf=%s ts=%s — dropping this bar",
                        symbol, getattr(bar, "timeframe", "?"),
                        getattr(bar, "timestamp", "?"),
                    )
                    routed = None

                if routed is None:
                    # Nothing to forecast (duplicate, not warm, unowned tf):
                    # retire NOW. The offset tracker only commits contiguous
                    # frontiers, so retiring ahead of an older in-flight bar
                    # is safe — it just waits for that bar.
                    self._retire(offset)
                    continue

                # Per-symbol in-flight cap (fairness across symbols at the
                # shared gate). Acquired BEFORE spawning, released by the task.
                await pipe.inflight_sem.acquire()
                task = asyncio.create_task(
                    self._run_forecast(symbol, routed, pipe),
                    name=f"forecast-{symbol}-{getattr(bar, 'timeframe', '?')}-"
                         f"{getattr(bar, 'timestamp', '?')}",
                )
                pipe.tasks.add(task)
                task.add_done_callback(pipe.tasks.discard)
                # Blocks if the applier is behind — backpressure, not shedding.
                await pipe.fifo.put((routed, task, offset))
            finally:
                pipe.input.task_done()

    async def _run_forecast(self, symbol: str, routed, pipe: _SymbolPipeline) -> None:
        try:
            await self._forecast(routed)
        except asyncio.CancelledError:
            raise
        except Exception:
            # The machine's forecast() swallows the expected failure modes
            # (timeout/SDK error/empty) itself; this is a genuine bug backstop.
            # The applier still runs apply(), which is a no-op for a pending
            # with no forecast, so the bar is retired and the pipeline moves on.
            log.exception("forecast task failed symbol=%s (bar will be applied as skipped)",
                          symbol)
        finally:
            pipe.inflight_sem.release()

    # ----- stage 3: the applier ------------------------------------------

    async def _applier(self, symbol: str, pipe: _SymbolPipeline) -> None:
        """Apply this symbol's bars in FIFO order. HEAD-OF-LINE by design: the
        next bar's forecast may already be done, but it is not applied until
        every earlier bar has been — that is the ordering guarantee."""
        while True:
            routed, task, offset = await pipe.fifo.get()
            try:
                try:
                    await task
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass   # already logged in _run_forecast
                try:
                    await self._apply(routed)
                except Exception:
                    # A bar whose apply fails is DROPPED (logged), never allowed
                    # to kill the applier — a dead applier would stall this
                    # symbol forever while the rest of the system looked healthy.
                    bar = getattr(routed, "bar", None)
                    log.exception(
                        "apply failed symbol=%s tf=%s ts=%s — dropping this bar "
                        "(pipeline continues)",
                        symbol, getattr(bar, "timeframe", "?"),
                        getattr(bar, "timestamp", "?"),
                    )
            finally:
                # Retired whether the bar SUCCEEDED or FAILED. A bar that failed
                # deterministically would fail identically on replay, so
                # withholding its offset would wedge the commit point forever on
                # a poison bar. Failures are loud in the log.
                self._retire(offset)
                pipe.fifo.task_done()

    def _retire(self, offset: int) -> None:
        self._done_offsets.append(offset)
        self._inflight -= 1
        if self._inflight == 0:
            self._idle.set()

    # ----- offset handoff -------------------------------------------------

    def take_done_offsets(self) -> list[int]:
        """Drain and return the offsets of bars retired since the last call.
        The consume loop uses these to advance its commit point."""
        out = self._done_offsets
        self._done_offsets = []
        return out

    # ----- lifecycle ------------------------------------------------------

    async def drain(self, timeout: float = 30.0) -> None:
        """Wait for all queued + in-flight bars to be applied. Called on clean
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
        """Drain, then cancel the pipelines (stager, applier, any forecast
        tasks still running)."""
        self._stopping = True
        await self.drain(timeout=drain_timeout)
        tasks: list[asyncio.Task] = []
        for pipe in self._pipes.values():
            for t in (pipe.stager, pipe.applier, *pipe.tasks):
                if t is not None:
                    t.cancel()
                    tasks.append(t)
        for t in tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
            except Exception:
                # The stage loops catch per-bar errors, so a task that ended
                # with one died outside them: say so rather than lose it.
                log.exception("symbol worker task %s ended with an error", t.get_name())
        log.info("symbol worker pool stopped (%d pipelines)", len(self._pipes))

    def worker_count(self) -> int:
        return len(self._pipes)

    def queue_depths(self) -> Dict[str, int]:
        """Per-symbol backlog (bars not yet applied) — the thing to graph. A
        depth that only grows is the capacity bug recurring."""
        return {s: p.input.qsize() + p.fifo.qsize() for s, p in self._pipes.items()}
