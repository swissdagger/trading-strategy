"""
producer/service2_machine.py — service_2's chain state machine, extracted behind
the ChainMachine seam, instantiated once per (symbol, set).

This is the EXTRACTION of CausaView's service_2_sumtyme_bridge.process_forecast.
The INIT -> PROPAGATE -> TERMINATE logic is preserved verbatim in behavior; what
changes is structural, and exactly two things change in a way that MATTERS:

  1. ONE INSTANCE PER (symbol, set), not per asset class. Each instance owns its
     own signal cache, last-base-signal, active_chains, per-timeframe buffers, and
     lock — all scoped to one symbol on one ladder. In the original these were
     module-level dicts keyed by (asset_class, symbol); here they are instance
     fields, so two instances (Set A and Set B) for the same symbol are fully
     independent.

  2. THE LEVEL INDEX IS BOUND TO THE INSTANCE'S LADDER, not the asset class's.
     This is the crux of the two-ladder design. In the original, a timeframe's
     level index came from _timeframe_index(asset_class, timeframe) — one ladder
     per asset class. Here it is self.ladder.index(timeframe). The SAME timeframe
     has DIFFERENT indices in Set A vs Set B (1m is index 3 in A, index 1 in B),
     so each set's machine indexes the signal cache by its OWN ladder. Getting
     this wrong would write a bar's signal into the wrong cache slot and corrupt
     propagation — it is THE thing the extraction had to get right.

What is byte-faithful to the original (do not "improve" without re-validating
against service_2):
  * the WINDOW_SIZE buffer gate before forecasting,
  * the per-symbol asyncio.Lock acquired BEFORE snapshotting active_chains,
  * the asyncio.to_thread Sumtyme call (never block the loop),
  * the INIT condition (baseline signal changed direction),
  * the PROPAGATE/TERMINATE sweep, including the "next-expected level opposes" AND
    "current level flips" termination paths,
  * persisting signals + chains to the DB (research/audit tables want them),
  * hydrate-on-restart from the chains table.

What changes at the boundary: instead of emitting causal-events to Kafka, on_bar
RETURNS the events as ChainEvent objects. The FanoutRouter tags them with the set
and publishes. This keeps the machine oblivious to "sets" — it just runs a ladder.

The DB and Sumtyme client are injected (not module globals), so an instance is
self-contained and testable, and the signal process wires real handles in via the
MachineFactory.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

from producer.bar import OHLCPoint
from producer.chain_machine import ChainEvent, ChainMachine

from producer.forecast_gate import ForecastGate

log = logging.getLogger("producer.service2_machine")

WINDOW_SIZE = 5000   # same gate as service_2: no forecast until the buffer fills


def _epoch_ms_to_iso(ms: int) -> str:
    """OHLCPoint.timestamp is epoch milliseconds (int); the state machine compares
    timestamps with >= and parses them with datetime.fromisoformat, so the signal
    cache must hold ISO strings. Convert here, in UTC, at the one point the bar's
    time enters the cache. ISO-8601 strings are lexically ordered the same as
    chronological order (for a fixed offset), so the original's string >= compare
    semantics are preserved."""
    return datetime.fromtimestamp(ms / 1000.0, timezone.utc).isoformat()


# Injected collaborators — keep the machine free of module globals so each
# instance is self-contained and unit-testable.
#
# forecast_fn: wraps the blocking Sumtyme call. Given (ohlc_df, interval,
#   interval_unit) it returns the forecast dict {step_str: signal_value} (or an
#   Exception / falsy on failure). The caller is responsible for running it via
#   asyncio.to_thread — we await it here, so pass an async wrapper OR a sync fn we
#   wrap. We accept a SYNC callable and wrap it in to_thread ourselves, matching
#   the original's `await asyncio.to_thread(client.forecast_path, ...)`.
ForecastFn = Callable[..., Any]

# prepare_df_fn: the original prepare_dataframe_for_sumtyme(df, timeframe) ->
#   (api_ready_df, placeholder_dt). Injected so we reuse service_2's exact
#   dataframe prep (placeholder row, step column) without copying it.
PrepareDfFn = Callable[[Any, str], tuple]

# to_interval_fn: _to_sumtyme_interval(timeframe) -> (interval, unit). Injected to
#   reuse the exact hours->minutes mapping.
ToIntervalFn = Callable[[str], tuple]

# build_df_fn: builds the dataframe Sumtyme expects from a buffer (deque of tick
#   dicts). The original constructs this inline; injected here so we reuse it.
BuildDfFn = Callable[[deque], Any]

# DB hooks — async, injected. Mirror the original's INSERT/UPDATE sites.
PersistSignal = Callable[[str, str, datetime, int, str, Optional[float]], Awaitable[None]]
PersistChainInit = Callable[[dict], Awaitable[None]]
PersistChainPropagate = Callable[[str, int, list, datetime, dict], Awaitable[None]]
PersistChainTerminate = Callable[[str, datetime], Awaitable[None]]


class ServiceTwoChainMachine(ChainMachine):
    """One (symbol, set) chain machine. Behavior-faithful to service_2's
    process_forecast, with the level index bound to `ladder`."""

    def __init__(
        self,
        symbol: str,
        asset_class: str,
        ladder: List[str],
        *,
        forecast_fn: ForecastFn,
        prepare_df_fn: PrepareDfFn,
        to_interval_fn: ToIntervalFn,
        build_df_fn: BuildDfFn,
        persist_signal: PersistSignal,
        persist_chain_init: PersistChainInit,
        persist_chain_propagate: PersistChainPropagate,
        persist_chain_terminate: PersistChainTerminate,
        load_active_chains: Optional[Callable[[], Awaitable[List[dict]]]] = None,
        load_buffers: Optional[Callable[[], Awaitable[Dict[str, deque]]]] = None,
        # Restores _last_base_signal on hydrate (newest non-zero baseline signal
        # from the signals table). Optional so tests/standalone machines that
        # don't wire DB hydration keep working; without it the first non-zero
        # baseline signal after a restart always INITs, even if it continues the
        # pre-restart direction (a duplicate same-direction chain).
        load_last_base_signal: Optional[Callable[[], Awaitable[Optional[int]]]] = None,
        forecast_timeout_s: float = 30.0,
        forecast_gate: Optional["ForecastGate"] = None,
    ):
        self.symbol = symbol
        self.asset_class = asset_class
        self.ladder = ladder
        self.baseline_timeframe = ladder[0]

        self._forecast_fn = forecast_fn
        self._prepare_df = prepare_df_fn
        self._to_interval = to_interval_fn
        self._build_df = build_df_fn
        self._persist_signal = persist_signal
        self._persist_init = persist_chain_init
        self._persist_prop = persist_chain_propagate
        self._persist_term = persist_chain_terminate
        self._load_active_chains = load_active_chains
        self._load_buffers = load_buffers
        self._load_last_base_signal = load_last_base_signal
        self._forecast_timeout_s = forecast_timeout_s
        # SHARED across every machine in the process — see ForecastGate. Sumtyme
        # serialises concurrent calls server-side, so we hold a single global
        # permit and queue HERE (cheap, cancellable) instead of in their queue
        # (which burns their compute on results we then time out and discard).
        # If None, forecasts run ungated — correct for a standalone/test machine,
        # WRONG in the live multi-symbol process, where every machine must share
        # one gate instance. signal_process constructs exactly one and injects it.
        self._forecast_gate = forecast_gate

        # --- per-(symbol,set) state (was module-level, keyed by (ac,symbol)) ---
        self._lock = asyncio.Lock()
        # level_index -> latest signal record {signal, timeframe, timestamp, price}
        self._latest_signals: Dict[int, dict] = {}
        self._last_base_signal: Optional[int] = None
        # chain_id -> chain dict
        self._active_chains: Dict[str, dict] = {}
        # timeframe -> rolling buffer of tick dicts
        self._buffers: Dict[str, deque] = {}
        self._hydrated = False

    # ---------------------------------------------------------------
    def _level_index(self, timeframe: str) -> Optional[int]:
        """Position of `timeframe` in THIS instance's ladder (0-based), or None if
        not in the ladder. THE two-ladder-critical change: indexes by self.ladder,
        not by a global asset-class hierarchy."""
        try:
            return self.ladder.index(timeframe)
        except ValueError:
            return None

    async def hydrate(self) -> None:
        """Load active chains (and optionally buffers) for this (symbol, set) from
        the DB so chains survive restarts — service_2's hydrate-on-restart. No-op
        if no loaders were injected (e.g. in tests)."""
        if self._hydrated:
            return
        if self._load_buffers is not None:
            self._buffers = await self._load_buffers()
            # Buffer-fill visibility: the seeded length per timeframe, once per
            # machine. A rung at WINDOW_SIZE forecasts on its first live bar; a
            # rung at 0 is cold-start (either sub-minute, which we skip on
            # purpose, or a hydration miss worth investigating). This is the
            # single fastest signal for "warm buffer but no forecast?".
            log.info("hydrated buffers for %s ladder=%s: %s",
                     self.symbol, self.baseline_timeframe,
                     {tf: len(dq) for tf, dq in self._buffers.items()})
        if self._load_active_chains is not None:
            for chain in await self._load_active_chains():
                self._active_chains[chain["chain_id"]] = chain
            log.info("hydrated %d active chains for %s/%s",
                     len(self._active_chains), self.symbol, self.baseline_timeframe)
        if self._load_last_base_signal is not None:
            # Restore the "last non-zero baseline signal" invariant so the first
            # live baseline signal after a restart only INITs on a genuine
            # direction change, not unconditionally. A failure here must not
            # block hydration: falling back to None just reproduces the old
            # (duplicate-INIT-prone) behaviour, which is strictly survivable.
            try:
                self._last_base_signal = await self._load_last_base_signal()
                log.info("hydrated last base signal for %s/%s: %s",
                         self.symbol, self.baseline_timeframe,
                         self._last_base_signal)
            except Exception as e:
                log.warning(
                    "last-base-signal hydrate failed for %s/%s (continuing with "
                    "None; first baseline signal will INIT unconditionally): %r",
                    self.symbol, self.baseline_timeframe, e,
                )
        self._hydrated = True

    async def on_bar(self, bar: OHLCPoint) -> List[ChainEvent]:
        """Feed one closed bar. Append to its buffer; if the buffer is full, run
        the forecast and the state-machine sweep. Returns the resulting events.

        Faithful to process_forecast: forecast in a thread, persist the raw
        signal, update the level-indexed cache, then (under the lock) run
        INIT/PROPAGATE/TERMINATE. The only behavioral change is RETURNING events
        instead of emitting to Kafka."""
        idx = self._level_index(bar.timeframe)
        if idx is None:
            return []   # not in this ladder (router shouldn't send it, but guard)

        # Append to the per-timeframe buffer. (Gap-fill is handled upstream in the
        # original via _append_with_fill; if needed it can be injected, but the
        # core gate is the same: don't forecast until WINDOW_SIZE.)
        buf = self._buffers.setdefault(bar.timeframe, deque(maxlen=WINDOW_SIZE))
        buf.append(self._tick_dict(bar))

        # >= not == : an over-filled buffer from a race-y warmup must still
        # forecast (service_2's explicit inline note).
        if len(buf) < WINDOW_SIZE:
            return []

        df = self._build_df(buf)
        api_ready_df, placeholder_dt = self._prepare_df(df, bar.timeframe)
        interval, interval_unit = self._to_interval(bar.timeframe)

        # Blocking Sumtyme call off the event loop — never await it directly, and
        # BOUND it. Without a timeout, a hung forecast call wedges the consume loop
        # indefinitely: the to_thread worker blocks, the committed offset freezes,
        # and the DB silently stops advancing (observed: a multi-day stall).
        #
        # GATED. Sumtyme processes concurrent requests SEQUENTIALLY server-side, so
        # firing N forecasts at once does not parallelise — it queues them in
        # Sumtyme, and the last one waits ~N x the single-call latency, blows the
        # timeout, and gets discarded AFTER Sumtyme already spent the compute. That
        # is strictly worse than serial (observed: 1145 timeouts, ZERO errors — the
        # signature of queueing, not failure). The gate holds ONE global permit, so
        # the queue lives here, where waiting is free and cancellable.
        #
        # The gate also measures single-call latency L and logs the resulting
        # CAPACITY (1/L forecasts/sec). If demand exceeds that, no client-side
        # change can help — it is a product decision (fewer symbols/rungs, or more
        # parallelism from Sumtyme).
        #
        # NOTE: wait_for cancels the AWAIT, not the underlying thread. The SDK
        # exposes no request-timeout kwarg (signal_process logs a warning at
        # startup), so a genuinely hung socket still occupies a thread-pool worker
        # until the OS returns. Serialising via the gate bounds the damage: at most
        # `concurrency` such calls can ever be stuck at once, instead of one per
        # symbol.
        try:
            if self._forecast_gate is not None:
                forecast = await self._forecast_gate.run(
                    self._forecast_fn,
                    time_series_type="ohlc",
                    data_input=api_ready_df,
                    interval=interval,
                    interval_unit=interval_unit,
                    reasoning_mode="reactive",
                    timeframe_dependent=False,
                    timeout_s=self._forecast_timeout_s,
                )
            else:
                # Ungated path: standalone/test use only. In the live process every
                # machine is given the shared gate.
                forecast = await asyncio.wait_for(
                    asyncio.to_thread(
                        self._forecast_fn,
                        time_series_type="ohlc",
                        data_input=api_ready_df,
                        interval=interval,
                        interval_unit=interval_unit,
                        reasoning_mode="reactive",
                        timeframe_dependent=False,
                    ),
                    timeout=self._forecast_timeout_s,
                )
        except asyncio.TimeoutError:
            # The hang path. LOUD on purpose: this is the failure that silently
            # froze the pipeline for days. One per stuck bar; a burst of these
            # means Sumtyme is down/degraded, not a one-off.
            log.warning(
                "forecast TIMEOUT after %ss for %s/%s ts=%s — skipping bar "
                "(repeated timeouts = Sumtyme unreachable/degraded)",
                self._forecast_timeout_s, self.symbol, bar.timeframe, bar.timestamp,
            )
            return []
        except Exception:
            # The SDK raised (network, 4xx/5xx, bad payload). Previously a raised
            # exception here would unwind out of on_bar -> run() and stop the
            # consume loop entirely (silent DB freeze). Catch, log with traceback,
            # skip the bar — the loop keeps draining.
            log.exception(
                "forecast ERROR for %s/%s ts=%s — skipping bar",
                self.symbol, bar.timeframe, bar.timestamp,
            )
            return []

        # Sumtyme returned but the result is unusable. The SDK can hand back an
        # Exception object or a falsy/empty dict instead of raising; that used to
        # be a SILENT `return []`. Make it visible — a steady stream of these means
        # the forecast is failing in a way that doesn't raise, which you want to
        # see, not have hidden.
        if isinstance(forecast, Exception):
            log.warning(
                "forecast returned an Exception object for %s/%s ts=%s: %r — skipping bar",
                self.symbol, bar.timeframe, bar.timestamp, forecast,
            )
            return []
        if not forecast:
            log.warning(
                "forecast returned empty/falsy result (%r) for %s/%s ts=%s — skipping bar",
                forecast, self.symbol, bar.timeframe, bar.timestamp,
            )
            return []

        signal_value = list(forecast.values())[0]
        current_price = bar.close
        bar_iso = _epoch_ms_to_iso(bar.timestamp)

        # Success path is observable at DEBUG: a healthy pipeline emits a steady
        # stream of these; sudden silence (with bars still arriving) localises a
        # stall to *after* the forecast returns. Cheap heartbeat for the loop.
        log.debug("forecast OK %s/%s ts=%s -> signal=%s",
                  self.symbol, bar.timeframe, bar.timestamp, signal_value)

        # Persist the raw signal (research/audit). Failure here is recoverable —
        # the next bar's UPSERT re-writes it — so log-and-continue, not raise.
        try:
            await self._persist_signal(
                self.symbol, bar.timeframe, placeholder_dt,
                int(signal_value), self.asset_class, current_price,
            )
        except Exception as e:
            log.warning("signal persist failed %s/%s: %r",
                        self.symbol, bar.timeframe, e)

        # Update the level-indexed signal cache.
        self._latest_signals[idx] = {
            "signal": signal_value,
            "timeframe": bar.timeframe,
            "timestamp": bar_iso,
            "price": current_price,
        }

        if signal_value == 0:
            return []   # neutral signals are recorded but don't drive chain logic

        # Run the state machine under the per-symbol lock.
        async with self._lock:
            return await self._run_state_machine(idx)

    # ---------------------------------------------------------------
    async def _run_state_machine(self, incoming_idx: int) -> List[ChainEvent]:
        """INIT -> PROPAGATE/TERMINATE, returning events. Caller holds the lock.
        Faithful to process_forecast stages 1-3."""
        events: List[ChainEvent] = []

        # --- STAGE 1: CHAIN INITIATION (baseline signal changed direction) ---
        base_sig = self._latest_signals.get(0)
        if base_sig and base_sig["signal"] != 0:
            if self._last_base_signal is None or base_sig["signal"] != self._last_base_signal:
                chain_uuid = str(uuid.uuid4())
                first_step = {
                    "level": 1, "timeframe": base_sig["timeframe"],
                    "timestamp": base_sig["timestamp"], "price": base_sig["price"],
                }
                self._last_base_signal = base_sig["signal"]
                chain = {
                    "chain_id": chain_uuid, "symbol": self.symbol,
                    "asset_class": self.asset_class, "direction": base_sig["signal"],
                    "current_level": 0, "next_expected_level": 1,
                    "active_timeframes": [base_sig["timeframe"]],
                    "history": [first_step], "last_timestamp": base_sig["timestamp"],
                }
                self._active_chains[chain_uuid] = chain
                try:
                    await self._persist_init(chain)
                except Exception as e:
                    log.warning("chain init persist failed %s: %r", self.symbol, e)
                events.append(ChainEvent(
                    event_type="chain_initiated", chain_id=chain_uuid,
                    symbol=self.symbol, asset_class=self.asset_class,
                    direction=base_sig["signal"], propagation_level=1,
                    timeframe=base_sig["timeframe"], timestamp=base_sig["timestamp"],
                    price=base_sig["price"],
                ))

        # --- STAGE 2 & 3: PROPAGATION & TERMINATION ---
        for cid, chain in list(self._active_chains.items()):
            # (single-symbol/single-set instance, so no asset_class/symbol filter
            # needed — every chain here belongs to this stream by construction)
            while True:
                progress_made = False
                expected_idx = chain["next_expected_level"]
                curr_idx = chain["current_level"]

                next_sig = self._latest_signals.get(expected_idx)
                if next_sig and next_sig["timestamp"] >= chain["last_timestamp"]:
                    if next_sig["signal"] == chain["direction"]:
                        # PROPAGATE
                        chain["current_level"] = expected_idx
                        chain["next_expected_level"] = expected_idx + 1
                        chain["last_timestamp"] = next_sig["timestamp"]
                        if next_sig["timeframe"] not in chain["active_timeframes"]:
                            chain["active_timeframes"].append(next_sig["timeframe"])
                        new_step = {
                            "level": expected_idx + 1, "timeframe": next_sig["timeframe"],
                            "timestamp": next_sig["timestamp"], "price": next_sig["price"],
                        }
                        chain["history"].append(new_step)
                        try:
                            await self._persist_prop(
                                cid, expected_idx + 1, chain["active_timeframes"],
                                datetime.fromisoformat(next_sig["timestamp"]), new_step,
                            )
                        except Exception as e:
                            log.warning("chain propagate persist failed %s: %r", cid, e)
                        events.append(ChainEvent(
                            event_type="chain_propagated", chain_id=cid,
                            symbol=self.symbol, asset_class=self.asset_class,
                            direction=chain["direction"], propagation_level=expected_idx + 1,
                            timeframe=next_sig["timeframe"], timestamp=next_sig["timestamp"],
                            price=next_sig["price"],
                        ))
                        progress_made = True

                    elif next_sig["signal"] == -chain["direction"]:
                        # TERMINATE — opposing signal on the next-expected level
                        del self._active_chains[cid]
                        try:
                            await self._persist_term(
                                cid, datetime.fromisoformat(next_sig["timestamp"]))
                        except Exception as e:
                            log.warning("chain terminate persist failed %s: %r", cid, e)
                        events.append(ChainEvent(
                            event_type="chain_terminated", chain_id=cid,
                            symbol=self.symbol, asset_class=self.asset_class,
                            direction=chain["direction"], propagation_level=expected_idx + 1,
                            timeframe=next_sig["timeframe"], timestamp=next_sig["timestamp"],
                            price=next_sig["price"],
                        ))
                        break

                if not progress_made:
                    curr_sig = self._latest_signals.get(curr_idx)
                    if (curr_sig and curr_sig["timestamp"] >= chain["last_timestamp"]
                            and curr_sig["signal"] == -chain["direction"]):
                        # TERMINATE — current level flipped against the chain
                        del self._active_chains[cid]
                        try:
                            await self._persist_term(
                                cid, datetime.fromisoformat(curr_sig["timestamp"]))
                        except Exception as e:
                            log.warning("chain terminate persist failed %s: %r", cid, e)
                        events.append(ChainEvent(
                            event_type="chain_terminated", chain_id=cid,
                            symbol=self.symbol, asset_class=self.asset_class,
                            direction=chain["direction"], propagation_level=curr_idx + 1,
                            timeframe=curr_sig["timeframe"], timestamp=curr_sig["timestamp"],
                            price=curr_sig["price"],
                        ))
                        break

                if not progress_made:
                    break

        return events

    # ---------------------------------------------------------------
    def _tick_dict(self, bar: OHLCPoint) -> dict:
        """Buffer tick shape. Mirrors what service_2's buffers hold."""
        return {
            "symbol": bar.symbol, "timestamp": bar.timestamp,
            "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close,
            "timeframe": bar.timeframe, "asset_class": bar.asset_class,
        }