"""
producer/forecast_gate.py — global concurrency gate for the Sumtyme API.

WHY THIS EXISTS. Sumtyme processes concurrent requests SEQUENTIALLY server-side:
send it 10 calls at once and it runs them one after another, returning each as it
finishes. So client-side parallelism buys NOTHING — it just moves the queue from
our process into theirs.

Worse, it actively harms us. Once per-symbol workers were introduced, up to 10
forecasts were in flight at once. Each waits behind the others in Sumtyme's
internal queue, so the LAST one's latency is ~10x the single-call latency. Our
asyncio.wait_for then killed it at the timeout — after Sumtyme had ALREADY SPENT
the compute. We paid for work, discarded the result, and skipped the bar
(observed: 1145 forecast TIMEOUTs, zero errors — the signature of queueing, not
failure). Throughput was therefore WORSE than serial.

THE FIX. Hold a single global permit. Only one forecast is in flight at a time, so
the wait happens HERE, in our process, where it is cheap and cancellable, rather
than in Sumtyme's queue, where it burns capacity on results we will throw away.
A call that starts is a call that completes.

WHAT THIS IS NOT. It does not make forecasting faster — nothing client-side can,
because the upstream is serial. It stops us making it SLOWER, and it restores the
meaning of the timeout: with the gate, forecast_timeout_s measures Sumtyme's
actual compute time, not time spent queued behind our own other requests.

THE CEILING. Throughput is now exactly:

    max forecasts/sec = 1 / L        (L = single-call latency)

That is a property of the UPSTREAM, not of this code. If demand (forecast-eligible
bars/sec across all symbols and rungs) exceeds 1/L, the backlog grows without
bound no matter what we do here — see Little's Law, and see the capacity numbers
this module logs. The levers at that point are product ones: fewer symbols, fewer
forecasting rungs, or more parallelism from Sumtyme (higher tier / batch endpoint /
multiple keys). This module MEASURES that ceiling so the decision is data-driven.

WHY THE PER-SYMBOL WORKERS STILL EARN THEIR KEEP. The gate serialises only the
Sumtyme call. Everything else — buffer appends, dataframe construction, the chain
state machine, DB writes, Kafka publishing — still runs concurrently across
symbols. Only the genuinely serial resource is serialised.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Optional

log = logging.getLogger("producer.forecast_gate")


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

    async def run(self, fn, *args, timeout_s: float, **kwargs):
        """Run one blocking forecast under the gate, in a worker thread.

        Raises asyncio.TimeoutError on timeout (the caller's existing skip-the-bar
        path), or whatever the SDK raises. The permit is ALWAYS released.

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
        capacity = (1.0 / p50) if p50 > 0 else float("inf")
        log.info(
            "forecast gate: calls=%d timeouts=%d errors=%d | latency p50=%.2fs "
            "p95=%.2fs | mean permit wait=%.2fs | CAPACITY≈%.2f forecasts/sec "
            "(concurrency=%d)",
            self._calls, self._timeouts, self._errors, p50, p95, mean_wait,
            capacity, self.concurrency,
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
            "capacity_per_s": round(1.0 / p50, 3) if p50 > 0 else None,
            "mean_permit_wait_s": round(
                sum(self._waits) / len(self._waits), 3) if self._waits else 0.0,
            "concurrency": self.concurrency,
        }