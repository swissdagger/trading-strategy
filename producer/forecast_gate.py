"""
producer/forecast_gate.py — global concurrency gate for the Sumtyme API.

WHY THIS EXISTS. The Sumtyme forecast call is the throughput bottleneck of the
entire pipeline, and its behaviour under concurrency is NON-OBVIOUS. Measured on
the live VM with a 5000-row window:

    N=1  ->  8.13s/call | 0.12 calls/sec
    N=4  ->  3.73s/call | 0.27 calls/sec
    N=6  ->  2.75s/call | 0.36 calls/sec   <-- PEAK
    N=8  ->  4.23s/call | 0.24 calls/sec   <-- COLLAPSES

Two things follow, and both matter:

1. It DOES parallelise — but SUBLINEARLY. Four concurrent calls take ~15s, not
   ~32s (serial) and not ~8s (perfect). So concurrency helps, up to a point.

2. Past the peak, throughput COLLAPSES. Too many in-flight calls queue up, blow
   the client timeout, and get discarded AFTER the server already spent the
   compute — we pay for work and throw the result away. That is strictly worse
   than never sending them. (Historically: 10 uncapped in-flight calls produced
   1145 timeouts and ZERO errors — the signature of queueing, not failure.)

The gate holds the in-flight count at the measured sweet spot. Waiting HERE is
free and cancellable; waiting in the API's queue is not.

CAVEAT ON THE N=8 COLLAPSE. It was partly an artifact of the default asyncio
thread pool: to_thread uses an executor sized min(32, cpu_count + 4) = 6 on a
2-vCPU box, so N=8 could never actually have 8 calls in flight. signal_process now
raises that pool (THREAD_POOL_SIZE) so the GATE, not the executor, is the binding
constraint. RE-BENCHMARK past 6 with the larger pool before assuming 6 is the true
ceiling — the real limit may be higher.

CAPACITY. Throughput is `concurrency / latency`, and this module measures both so
the number is never guessed. If demand (forecast-eligible bars/sec across all
symbols and rungs) exceeds capacity, the backlog grows WITHOUT BOUND — no
client-side change fixes that, and the levers become product ones: fewer symbols,
fewer forecasting rungs, more vCPUs, or a faster API tier.

WHY THE PER-SYMBOL WORKERS STILL EARN THEIR KEEP. The gate bounds only the Sumtyme
call. Everything else — buffer appends, dataframe construction, the chain state
machine, DB writes, Kafka publishing — still runs concurrently across symbols. Only
the genuinely contended resource is bounded.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Optional

log = logging.getLogger("producer.forecast_gate")


class _MemoEntry:
    """One memo slot: the exact window key, and either a settled value or the
    future of the call that will produce it."""
    __slots__ = ("key", "future", "value", "done")

    def __init__(self, key: tuple, future: Optional[asyncio.Future]):
        self.key = key
        self.future = future
        self.value = None
        self.done = False


class MemoClaim:
    """Result of ForecastMemo.claim(). See claim() for the three cases."""
    __slots__ = ("value", "future", "owner")

    def __init__(self, value, future: Optional[asyncio.Future], owner: bool):
        self.value = value
        self.future = future
        self.owner = owner


class ForecastMemo:
    """Cross-ladder forecast dedupe. ONE instance per process, shared by every
    chain machine (same discipline as ForecastGate).

    WHY. Sumtyme is DETERMINISTIC: an identical input window produces an
    identical signal, every time. Chain machines are per (symbol, ladder), and
    two ladders that share a timeframe (FC and CN share 1m/3m/5m/15m/1h on
    BTC-USD) each hold their own buffer for it and would each forecast the SAME
    (symbol, timeframe, window) — paying the pipeline's scarcest resource twice
    for one answer. The 1m rung alone is ~1440 calls/day/symbol, so without
    this memo a second ladder on the same rungs roughly DOUBLES Sumtyme demand;
    with it, the second ladder costs only its genuinely new rungs (CN's
    30s/45s/2m/30m).

    KEY: (asset_class, symbol, timeframe, first_bar_ts, last_bar_ts, n_bars).
    The first/last timestamps + length pin the exact window, not just its tail
    — so two machines whose buffers ever diverge (e.g. hydrated against
    different warmup snapshots) can never share a result that one of them
    didn't compute. A diverged window simply misses and forecasts normally.

    RETENTION: only the LATEST entry per (asset_class, symbol, timeframe) —
    a new bar replaces the previous one. The reuse window is "the other
    ladder's machine processing the same bar moments later", so history has no
    value, and memory stays bounded by the number of live (symbol, timeframe)
    pairs.

    CONCURRENCY: the two ladders sharing a timeframe no longer run one after
    the other — since the forecast/apply split (service2_machine.prepare /
    forecast / apply), a symbol's ladders prepare back-to-back under the lock
    and then forecast CONCURRENTLY. So a plain value cache would see both
    machines miss and both call the API. The memo therefore holds an IN-FLIGHT
    future per slot: the first claimant (the owner) computes, later claimants
    join its future. Single event loop, so the dict ops themselves need no lock.

    FAILURES ARE NEVER SHARED. The owner resolves the future only with a
    USABLE result; on any failure it calls fail(), which drops the entry and
    settles joiners with None so they call the API themselves. A failed call
    is thus never replayed to the other ladder — the same rule as before.
    """

    def __init__(self, *, stats_every: int = 500):
        # (asset_class, symbol, timeframe) -> _MemoEntry
        self._latest: dict[tuple, _MemoEntry] = {}
        self._hits = 0      # value already present, or joined an in-flight call
        self._misses = 0    # became the owner (an API call will be made)
        self._stats_every = stats_every

    @staticmethod
    def _slot(key: tuple) -> tuple:
        return key[:3]   # (asset_class, symbol, timeframe)

    # ----- the three-way claim used by the live machines ------------------

    def claim(self, key: tuple) -> "MemoClaim":
        """Claim this window. Exactly one of three outcomes:
          * value present  -> MemoClaim(value=forecast)           (hit)
          * in flight      -> MemoClaim(future=f, owner=False)    (join: await f)
          * nothing        -> MemoClaim(future=f, owner=True)     (you compute;
                              you MUST resolve() or fail() this key)
        Must be called from a running event loop."""
        slot = self._slot(key)
        entry = self._latest.get(slot)
        if entry is not None and entry.key == key:
            if entry.done:
                self._hits += 1
                self._maybe_log()
                return MemoClaim(value=entry.value, future=None, owner=False)
            self._hits += 1
            self._maybe_log()
            return MemoClaim(value=None, future=entry.future, owner=False)
        self._misses += 1
        self._maybe_log()
        fut = asyncio.get_running_loop().create_future()
        self._latest[slot] = _MemoEntry(key=key, future=fut)
        return MemoClaim(value=None, future=fut, owner=True)

    def resolve(self, key: tuple, forecast) -> None:
        """Owner (or a joiner re-putting the same value) publishes a USABLE
        forecast for this window. Idempotent."""
        slot = self._slot(key)
        entry = self._latest.get(slot)
        if entry is None or entry.key != key:
            entry = _MemoEntry(key=key, future=None)
            self._latest[slot] = entry
        entry.value = forecast
        entry.done = True
        if entry.future is not None and not entry.future.done():
            entry.future.set_result(forecast)

    def fail(self, key: tuple) -> None:
        """Owner reports that its call produced nothing usable. The entry is
        dropped (so the next bar, or a retry, starts clean) and any joiners are
        released with None so they make their own call. No-op if already
        resolved."""
        slot = self._slot(key)
        entry = self._latest.get(slot)
        if entry is None or entry.key != key or entry.done:
            return
        del self._latest[slot]
        if entry.future is not None and not entry.future.done():
            entry.future.set_result(None)

    # ----- plain value interface (standalone/test use; kept for compatibility) --

    def get(self, key: tuple):
        """The memoized forecast for exactly this window, or None. Does not
        join an in-flight call — use claim() for that."""
        entry = self._latest.get(self._slot(key))
        if entry is not None and entry.key == key and entry.done:
            self._hits += 1
            self._maybe_log()
            return entry.value
        self._misses += 1
        self._maybe_log()
        return None

    def put(self, key: tuple, forecast) -> None:
        """Store a USABLE forecast for this window, replacing the slot's
        previous window. Equivalent to resolve()."""
        self.resolve(key, forecast)

    def _maybe_log(self) -> None:
        total = self._hits + self._misses
        if total and total % self._stats_every == 0:
            self.log_stats()

    def log_stats(self) -> None:
        total = self._hits + self._misses
        rate = (self._hits / total * 100) if total else 0.0
        log.info(
            "forecast memo: %d lookups, %d hits (%.1f%%) — every hit is one "
            "Sumtyme call NOT made",
            total, self._hits, rate,
        )


class ForecastGate:
    """One shared permit-holder for Sumtyme calls, plus latency instrumentation.

    ONE instance per process, shared by every chain machine. Constructing one per
    machine would defeat the entire purpose — each would hold its own permit and
    all N would fire at once again.
    """

    def __init__(self, concurrency: int = 1, *, stats_every: int = 50):
        # Default 1 = strictly serial, matching Sumtyme's server-side behaviour.
        # >1 only makes sense if Sumtyme later offers real parallelism; raising it
        # against a serial upstream just recreates the queueing bug.
        self._sem = asyncio.Semaphore(concurrency)
        self.concurrency = concurrency

        self._stats_every = stats_every
        self._latencies: deque[float] = deque(maxlen=500)
        self._calls = 0
        self._timeouts = 0
        self._errors = 0
        # Time spent WAITING for the permit (i.e. queued behind other symbols).
        # High wait + low latency = we are demand-bound, not API-bound.
        self._waits: deque[float] = deque(maxlen=500)

    async def run(self, fn, *args, timeout_s: float, prepare=None, **kwargs):
        """Run one blocking forecast under the gate, in a worker thread.

        Raises asyncio.TimeoutError on timeout (the caller's existing skip-the-bar
        path), or whatever the SDK raises. The permit is ALWAYS released.

        `prepare`, if given, is a sync callable returning extra kwargs for `fn`;
        it runs AFTER the permit is acquired. The machine uses it to build the
        5001-row DataFrame only once the call is actually about to happen, so a
        bar queued behind other symbols holds a buffer snapshot (5000 refs), not
        a full frame. Under a backlog that is the difference between bounded and
        unbounded memory.

        NOTE on the timeout: it is measured from the moment the permit is
        acquired, NOT from when this coroutine was called. That is deliberate —
        time spent queued for the permit is our own backlog, not Sumtyme being
        slow, and counting it would make the timeout fire spuriously under load
        and skip bars that were never actually given a chance to forecast.
        """
        wait_start = time.perf_counter()
        async with self._sem:
            waited = time.perf_counter() - wait_start
            self._waits.append(waited)

            if prepare is not None:
                kwargs = {**kwargs, **prepare()}

            call_start = time.perf_counter()
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(fn, *args, **kwargs),
                    timeout=timeout_s,
                )
            except asyncio.TimeoutError:
                self._timeouts += 1
                self._record(time.perf_counter() - call_start)
                raise
            except Exception:
                self._errors += 1
                self._record(time.perf_counter() - call_start)
                raise
            self._record(time.perf_counter() - call_start)
            return result

    def _record(self, latency_s: float) -> None:
        self._latencies.append(latency_s)
        self._calls += 1
        if self._calls % self._stats_every == 0:
            self.log_stats()

    def log_stats(self) -> None:
        """Emit the numbers that decide whether this system is viable.

        Read it like this:
          * p50/p95 latency  = L, the single-call cost. The ceiling is 1/L.
          * capacity         = that ceiling, in bars/sec.
          * mean wait        = how long a symbol queues for the permit. If this is
                               large while latency is small, DEMAND exceeds
                               capacity and the backlog will grow forever — the
                               fix is fewer symbols/rungs or more Sumtyme
                               parallelism, NOT more client-side code.
        """
        if not self._latencies:
            return
        lat = sorted(self._latencies)
        n = len(lat)
        p50 = lat[n // 2]
        p95 = lat[min(n - 1, int(n * 0.95))]
        mean_wait = sum(self._waits) / len(self._waits) if self._waits else 0.0
        # Capacity = concurrency / latency, NOT 1 / latency. With N calls in
        # flight, N complete every ~p50 seconds. (This was 1/p50 when the gate was
        # built on the mistaken belief that Sumtyme serialised everything — that
        # formula understates true throughput by a factor of `concurrency`.)
        capacity = (self.concurrency / p50) if p50 > 0 else float("inf")
        log.info(
            "forecast gate: calls=%d timeouts=%d errors=%d | latency p50=%.2fs "
            "p95=%.2fs | mean permit wait=%.2fs | CAPACITY≈%.2f forecasts/sec "
            "(concurrency=%d)",
            self._calls, self._timeouts, self._errors, p50, p95, mean_wait,
            capacity, self.concurrency,
        )
        if mean_wait > 5 * p50 and p50 > 0:
            log.warning(
                "forecast gate: mean permit wait (%.1fs) is >5x call latency "
                "(%.1fs) — bars are queueing far longer than they take to "
                "process. DEMAND EXCEEDS CAPACITY: the backlog will grow without "
                "bound. Raise FORECAST_CONCURRENCY (re-benchmark first), or cut "
                "demand (fewer symbols / fewer forecasting rungs).",
                mean_wait, p50,
            )
        if self._timeouts > 0 and self._timeouts / max(1, self._calls) > 0.10:
            log.warning(
                "forecast gate: %.0f%% of calls TIMED OUT. With the gate held at "
                "concurrency=%d these are NOT queueing artifacts — Sumtyme itself "
                "is taking longer than the timeout. Raise SUMTYME_TIMEOUT_S or "
                "talk to Sumtyme.",
                100.0 * self._timeouts / self._calls, self.concurrency,
            )

    def snapshot(self) -> dict:
        """Current stats, for a metrics endpoint / health check."""
        lat = sorted(self._latencies)
        p50 = lat[len(lat) // 2] if lat else 0.0
        return {
            "calls": self._calls,
            "timeouts": self._timeouts,
            "errors": self._errors,
            "latency_p50_s": round(p50, 3),
            "capacity_per_s": round(
                self.concurrency / p50, 3) if p50 > 0 else None,
            "mean_permit_wait_s": round(
                sum(self._waits) / len(self._waits), 3) if self._waits else 0.0,
            "concurrency": self.concurrency,
        }