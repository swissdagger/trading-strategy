"""
producer/service2_machine.py — service_2's chain state machine, extracted behind
the ChainMachine seam, instantiated once per (symbol, set).

This is the EXTRACTION of CausaView's service_2_sumtyme_bridge.process_forecast.
The INIT -> PROPAGATE -> TERMINATE logic (the start condition, the one-rung
propagation loop, both termination paths) is the same as the original; the
deliberate differences are listed in README "State-machine sync" (ordering,
comparing on bar close, deterministic ids, input guard, storage). Structurally,
two things change in a way that MATTERS:

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
    "current level flips" termination paths (its comparison is on bar close —
    see README "State-machine sync"),
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
import hashlib
import logging
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, List, Optional

from producer.bar import OHLCPoint
from producer.chain_machine import ChainEvent, ChainMachine

from producer.forecast_gate import ForecastGate, ForecastMemo
from ops.alerts import alert

if TYPE_CHECKING:
    from producer.audit import WatermarkRegistry

log = logging.getLogger("producer.service2_machine")

WINDOW_SIZE = 5000   # same gate as service_2: no forecast until the buffer fills


@dataclass
class PendingForecast:
    """One bar that has been APPENDED but not yet APPLIED.

    Produced by `prepare()` (under the symbol lock), filled in by `forecast()`
    (no lock), consumed by `apply()` (under the symbol lock). It carries
    everything `apply()` needs, so `apply()` never reads the buffer — which is
    what lets bars N+1, N+2 be appended while bar N's forecast is in flight.

    `snapshot` is the buffer EXACTLY as it stood when this bar was appended:
    the window Sumtyme must see for this bar, frozen. Tick dicts are never
    mutated after creation, so sharing references is safe and the tuple is
    5000 pointers, not 5000 copies.
    """
    idx: int                         # level index of bar.timeframe in the ladder
    bar: OHLCPoint
    snapshot: tuple                  # tick dicts, len == len(buffer) at append
    memo_key: Optional[tuple]
    forecast: Any = None             # usable forecast dict after forecast(); None => skip
    placeholder_dt: Any = None       # the bar the forecast predicts (from prepare_df)
    memo_hit: bool = False


def _tf_seconds(timeframe: str) -> Optional[int]:
    """Seconds per bar for a '<n><unit>' timeframe (5s/1m/30m/6h/12h/1d), used
    only for the hydrate fill report's days-to-fill estimate. Unknown shapes
    return None (the report just omits the estimate) — deliberately forgiving,
    a log nicety must never be able to break hydration."""
    try:
        n, unit = int(timeframe[:-1]), timeframe[-1]
        return n * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    except (ValueError, KeyError, IndexError):
        return None


def _bar_close_ms(bar: OHLCPoint) -> int:
    """Epoch ms at which `bar` CLOSED: bar.timestamp (its open — bars are
    labelled by open time) + one timeframe. The moment the information in a
    signal computed from this bar first existed; the same instant
    service2_utils computes as placeholder_dt."""
    secs = _tf_seconds(bar.timeframe)
    if secs is None:
        raise ValueError(f"cannot parse timeframe {bar.timeframe!r}")
    return bar.timestamp + secs * 1000


# Sumtyme failures (timeout, error, unusable result) across EVERY machine in
# the process. One is noise; FORECAST_FAILURE_BURST within the window means
# Sumtyme is down or degraded and bars are being skipped — alert.
FORECAST_FAILURE_BURST = 5
FORECAST_FAILURE_WINDOW_S = 300.0
_forecast_failures: deque = deque()


def _note_forecast_failure(kind: str) -> None:
    now = time.monotonic()
    _forecast_failures.append(now)
    while _forecast_failures and now - _forecast_failures[0] > FORECAST_FAILURE_WINDOW_S:
        _forecast_failures.popleft()
    if len(_forecast_failures) >= FORECAST_FAILURE_BURST:
        log.error("Sumtyme failing: %d forecast failures in %.0f min (latest: %s) — bars "
                  "are being skipped", len(_forecast_failures),
                  FORECAST_FAILURE_WINDOW_S / 60, kind,
                  extra=alert("sumtyme-failing", every=900))


def _now_iso() -> str:
    """Wall clock, ISO UTC: a chain step's detected_at."""
    return datetime.now(timezone.utc).isoformat()


def _last_bar_close(chain: dict) -> str:
    """The bar close of the chain's newest step. Live chains carry it; a
    chain hydrated from rows written before chain_steps.bar_close existed has
    only the step's open label, from which the close follows (open + one
    timeframe)."""
    close = chain.get("last_bar_close")
    if close:
        return close
    last = chain["history"][-1]
    if last.get("bar_close"):
        return last["bar_close"]
    secs = _tf_seconds(last["timeframe"])
    opened = datetime.fromisoformat(last["timestamp"])
    return _epoch_ms_to_iso(int(opened.timestamp() * 1000) + (secs or 0) * 1000)


def _epoch_ms_to_iso(ms: int) -> str:
    """OHLCPoint.timestamp is epoch milliseconds (int); the state machine compares
    timestamps with >= and parses them with datetime.fromisoformat, so the signal
    cache must hold ISO strings. Convert here, in UTC, at the one point the bar's
    time enters the cache. ISO-8601 strings are lexically ordered the same as
    chronological order (for a fixed offset), so the original's string >= compare
    semantics are preserved."""
    return datetime.fromtimestamp(ms / 1000.0, timezone.utc).isoformat()


def _as_signal_candidate(loaded) -> Optional[tuple]:
    """Normalise whatever load_last_base_signal returned into (signal, iso_ts).

    The hook is expected to return (signal, iso_timestamp), but a bare signal
    int is accepted too — the contract is easy to get subtly wrong when wiring
    the persistence layer, and a mismatch used to surface as a TypeError deep in
    a max() key, which is a miserable way to learn about it.

    A bare int carries no timestamp to order by, so it ranks oldest: an active
    chain's origin, when there is one, comes from a synchronous write and is the
    more durable source anyway.
    """
    if loaded is None:
        return None
    if isinstance(loaded, (tuple, list)):
        if len(loaded) != 2:
            raise ValueError(
                f"load_last_base_signal returned {len(loaded)} values, "
                f"expected (signal, iso_timestamp)")
        return int(loaded[0]), str(loaded[1])
    return int(loaded), ""


def chain_id_for(symbol: str, asset_class: str, ladder_name: str,
                 direction: int, origin_timestamp: str) -> str:
    """The chain's identity, derived from the initiation that produced it.

    WHY THIS IS NOT uuid4. Bars are delivered at-least-once (see
    producer/offset_tracker.py) and `_last_base_signal` is process-local, so the
    SAME initiation can be processed more than once — on replay after a crash, or
    on the first baseline signal after a restart. With a random id, every one of
    those wrote a NEW chains row: `ON CONFLICT (chain_id) DO NOTHING` cannot fire
    on an id that was just minted, so the chains INSERT was the one write on this
    path with no idempotency at all. Worse, chain_id flows into position_key ->
    signal_id, so a duplicated chain read downstream as a brand-new position and
    could open a second trade against one real move.

    Deriving the id from the initiation makes a replayed init a no-op, exactly
    like chain_steps' (chain_id, step_index) and trade_signals' signal_id. Same
    "prefix + truncated sha256" shape as signal_writer._signal_id, for consistency.

    LADDER NAME IS PART OF THE KEY AND MUST STAY THERE. Ladders overlap: two
    ladders that share a baseline on the same symbols (as FC and the former PF
    ladder did at 1m on crypto) see one baseline signal legitimately initiate
    TWO chains — same symbol, direction, and origin_time, different ladders.
    Keying on the baseline timeframe instead of the ladder name would collapse
    them into one row and let two machines with different level numbering
    fight over it.
    """
    raw = f"chain1|{symbol}|{asset_class}|{ladder_name}|{direction}|{origin_timestamp}"
    return "chain1." + hashlib.sha256(raw.encode()).hexdigest()[:32]


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
        ladder_name: str,
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
        load_last_base_signal: Optional[
            Callable[[], Awaitable[Optional[tuple]]]] = None,
        forecast_timeout_s: float = 30.0,
        forecast_gate: Optional["ForecastGate"] = None,
        forecast_memo: Optional["ForecastMemo"] = None,
        optional_timeframes: Optional[set] = None,
        watermarks: Optional["WatermarkRegistry"] = None,
    ):
        self.symbol = symbol
        self.asset_class = asset_class
        self.ladder = ladder
        # The ladder's REGISTERED NAME (e.g. "FC", "ED"), not derivable from
        # the timeframe list: two ladders may share a baseline timeframe, so
        # the baseline does not identify a ladder. Part of the chain's
        # identity — see chain_id_for.
        self.ladder_name = ladder_name
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
        # SHARED across every machine in the process, like the gate and the memo.
        # None in tests/standalone; signal_process injects exactly one so the
        # audit endpoint can see every machine's progress.
        self._watermarks = watermarks
        self._forecast_timeout_s = forecast_timeout_s
        # SHARED across every machine in the process — see ForecastGate. Sumtyme
        # serialises concurrent calls server-side, so we hold a single global
        # permit and queue HERE (cheap, cancellable) instead of in their queue
        # (which burns their compute on results we then time out and discard).
        # If None, forecasts run ungated — correct for a standalone/test machine,
        # WRONG in the live multi-symbol process, where every machine must share
        # one gate instance. signal_process constructs exactly one and injects it.
        self._forecast_gate = forecast_gate
        # SHARED across every machine, like the gate. Sumtyme is deterministic,
        # so two ladders sharing a timeframe can share one forecast per bar —
        # see ForecastMemo. If None, every call goes to the API (standalone/test
        # posture); the live process constructs exactly one and injects it.
        self._forecast_memo = forecast_memo
        # Rungs whose empty/short buffer is EXPECTED and acceptable (e.g. DL's
        # 4h/6h rungs: 5000 bars of equity session time is years of history,
        # more than a young listing has). Downgrades the hydrate fill report
        # for these rungs from WARNING to INFO; affects LOGGING ONLY — an
        # unwarmed rung is inert either way (it never forecasts until its
        # buffer reaches WINDOW_SIZE, and chains below it are unaffected).
        self._optional_timeframes: set = optional_timeframes or set()

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
        # Did the last prepare() append its bar? Read by the router to decide
        # whether the bar goes to the bar store.
        self.last_prepare_appended = False
        self._older_drops = 0
        self._older_drop_logged = -1e9

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
            self._report_buffer_fill()
        if self._load_active_chains is not None:
            for chain in await self._load_active_chains():
                self._active_chains[chain["chain_id"]] = chain
            log.info("hydrated %d active chains for %s/%s",
                     len(self._active_chains), self.symbol, self.baseline_timeframe)
        await self._restore_last_base_signal()
        self._hydrated = True

    async def _restore_last_base_signal(self) -> None:
        """Rebuild the INIT guard from durable state. BEST EFFORT — never raises.

        WHY IT MUST NOT RAISE. hydrate() is called from
        MultiLadderRouter._machine_for on the bar path, so an exception here does
        not just skip the restore, it propagates into the symbol worker and every
        bar for this (symbol, ladder) is dropped. The whole downside of this
        method doing nothing is ONE redundant chain at the next baseline signal —
        the pre-fix behaviour. Trading that for a dead pipeline is never right, so
        failures are logged and swallowed.

        WHY THE GUARD NEEDS RESTORING. STAGE 1 initiates when the baseline signal
        DIFFERS from `_last_base_signal`, and None counts as different. Hydrate
        used to restore `_active_chains` but leave this at None, so the first
        baseline signal after every restart initiated a chain unconditionally —
        even with a live, same-direction chain just hydrated from the DB.

        The guard is exactly "the direction of the most recent non-zero baseline
        signal": it is only ever compared for inequality, and STAGE 1 sets it to
        the current baseline signal on every init.

        Two sources, newest wins, because neither alone is complete:
          * `signals` — every baseline signal, but written through the DEFERRED
            audit writer, so a crash can lose the last batch.
          * hydrated active chains — each one's origin IS a baseline signal in
            that chain's direction, and the init write is SYNCHRONOUS, so it
            survives a crash the signals row may not have.
        """
        try:
            await self._restore_last_base_signal_inner()
        except Exception as e:
            log.warning(
                "last base signal restore failed for %s/%s: %r — continuing "
                "without it; costs at most one redundant chain at the next "
                "baseline signal", self.symbol, self.ladder_name, e)

    async def _restore_last_base_signal_inner(self) -> None:
        candidates: List[tuple] = []
        if self._load_last_base_signal is not None:
            latest = _as_signal_candidate(await self._load_last_base_signal())
            if latest is not None:
                candidates.append(latest)          # (signal, iso_timestamp)
        for chain in self._active_chains.values():
            history = chain.get("history") or []
            if history:
                candidates.append((chain["direction"], history[0]["timestamp"]))
        if not candidates:
            return
        # ISO-8601 at a fixed offset sorts lexically as it sorts chronologically —
        # the same property the propagation sweep's string >= compares rely on.
        signal, ts = max(candidates, key=lambda c: c[1])
        self._last_base_signal = signal
        log.info("restored last base signal for %s/%s: %+d (from %s)",
                 self.symbol, self.ladder_name, signal, ts)

    def _report_buffer_fill(self) -> None:
        """Severity-graded startup report of buffer fill per rung, once per
        machine at hydrate. An unwarmed rung produces NO error at runtime — it
        is silently inert (never forecasts until WINDOW_SIZE), which is exactly
        why this report exists: it turns three silent behaviours into log lines.

          * baseline short  -> ERROR.  A chain can only initiate from index 0;
            no baseline means this symbol can never chain on this ladder AT ALL.
          * interior short  -> WARNING. The no-skip invariant means propagation
            is CAPPED at the level below the short rung until it fills; any
            strategy logic keyed to deeper levels never fires for this symbol.
          * short rung in optional_timeframes -> INFO. Expected-short by
            configuration (e.g. DL's 4h/6h; config.LADDER_OPTIONAL_TIMEFRAMES).

        The days-to-fill estimate assumes live bars arrive continuously — true
        for crypto, an underestimate for session-bound classes.
        """
        for i, tf in enumerate(self.ladder):
            have = len(self._buffers.get(tf, ()))
            if have >= WINDOW_SIZE:
                continue
            missing = WINDOW_SIZE - have
            secs = _tf_seconds(tf)
            eta = f" (~{missing * secs / 86400:.0f}d of live bars to fill)" if secs else ""
            if i == 0:
                log.error(
                    "buffer fill %s/%s rung %s (baseline): %d/%d — symbol is "
                    "INERT on this ladder (chains can never initiate) until "
                    "this buffer fills%s",
                    self.symbol, self.baseline_timeframe, tf, have, WINDOW_SIZE, eta)
            elif tf in self._optional_timeframes:
                log.info(
                    "buffer fill %s/%s rung %s: %d/%d — expected-short "
                    "(optional rung); level %d unreachable until filled%s",
                    self.symbol, self.baseline_timeframe, tf, have, WINDOW_SIZE,
                    i + 1, eta)
            else:
                log.warning(
                    "buffer fill %s/%s rung %s: %d/%d — propagation CAPPED at "
                    "level %d until this buffer fills; logic keyed to level "
                    ">= %d never fires for this symbol%s",
                    self.symbol, self.baseline_timeframe, tf, have, WINDOW_SIZE,
                    i, i + 1, eta)

    async def on_bar(self, bar: OHLCPoint) -> List[ChainEvent]:
        """Feed one closed bar, serially: prepare -> forecast -> apply.

        This is the ORIGINAL contract (ChainMachine.on_bar) and the standalone/
        test entry point. It is now a composition of the three stages below;
        the live router calls those stages separately so that the slow middle
        stage (the Sumtyme call) runs OUTSIDE the per-symbol lock and
        concurrently across bars, while the two stages that touch machine state
        still run under the lock, strictly in arrival order. See prepare(),
        forecast(), apply() — and producer/symbol_workers.py for the ordering
        mechanism.

        Behaviour-faithful to process_forecast: forecast in a thread, persist
        the raw signal, update the level-indexed cache, then (under the lock)
        run INIT/PROPAGATE/TERMINATE. Returns events instead of emitting."""
        pending = self.prepare(bar)
        if pending is None:
            return []
        await self.forecast(pending)
        return await self.apply(pending)

    # ---------------------------------------------------------------
    # STAGE 1 — prepare: append + snapshot. Caller holds the symbol lock.
    # ---------------------------------------------------------------
    def prepare(self, bar: OHLCPoint) -> Optional[PendingForecast]:
        """Append `bar` to its buffer and, if the buffer is full, return a
        PendingForecast carrying a frozen snapshot of that buffer.

        SYNCHRONOUS ON PURPOSE. There is no await between the monotonic guard
        and the append, so two bars for the same (symbol, timeframe) cannot
        interleave here even in principle: buffer order == the order prepare()
        was called in == arrival order. The caller (the router) invokes this
        under the symbol lock in arrival order, which is what makes the buffer
        — Sumtyme's definition of "one step" — correct.

        Returns None when there is nothing to forecast: timeframe not in this
        ladder, duplicate/out-of-order bar (dropped), or buffer not yet full.
        """
        self.last_prepare_appended = False
        idx = self._level_index(bar.timeframe)
        if idx is None:
            return None   # not in this ladder (router shouldn't send it, but guard)

        # Append to the per-timeframe buffer. (Gap-fill is handled upstream in the
        # original via _append_with_fill; if needed it can be injected, but the
        # core gate is the same: don't forecast until WINDOW_SIZE.)
        buf = self._buffers.setdefault(bar.timeframe, deque(maxlen=WINDOW_SIZE))

        # MONOTONIC GUARD. A bar at or before the newest buffered bar for this
        # timeframe is dropped, never appended. In step mode the buffer's row
        # order IS Sumtyme's definition of "one step", so a repeated or
        # out-of-order bar corrupts the series it reconstructs — and a repeat
        # would also re-run the forecast and re-emit the same chain events.
        # Repeats are expected (Kafka is at-least-once: a restart replays bars
        # since the last committed offset; the feed/warmup cursor overlap);
        # an OLDER bar means an upstream ordering bug, so it is louder.
        if buf and bar.timestamp <= buf[-1]["timestamp"]:
            newest = buf[-1]["timestamp"]
            if bar.timestamp == newest:
                log.debug("drop duplicate bar %s/%s ts=%s",
                          self.symbol, bar.timeframe, bar.timestamp)
            else:
                self._note_older_drop(bar, newest)
            return None
        buf.append(self._tick_dict(bar))
        self.last_prepare_appended = True

        # >= not == : an over-filled buffer from a race-y warmup must still
        # forecast (service_2's explicit inline note).
        if len(buf) < WINDOW_SIZE:
            return None

        # CROSS-LADDER MEMO KEY. Sumtyme is deterministic, so another machine
        # (a different ladder sharing this timeframe) forecasting this EXACT
        # window — same symbol, timeframe, first/last bar ts, length — gets our
        # result and skips the API call. Pinning first/last ts + length means
        # two machines whose buffers ever diverge simply miss. The claim itself
        # happens in forecast(), because that is where the in-flight future
        # lives (the ladders no longer run one after the other; see ForecastMemo).
        memo_key = None
        if self._forecast_memo is not None:
            memo_key = (
                self.asset_class, self.symbol, bar.timeframe,
                buf[0]["timestamp"], bar.timestamp, len(buf),
            )

        # SNAPSHOT NOW. By the time forecast() runs, later bars may have been
        # appended; the forecast for THIS bar must see the window as of THIS
        # append. The DataFrame itself is built later, after the gate permit
        # is acquired, so a bar queued for a permit holds 5000 references, not
        # a 5001-row frame (this is what bounds peak memory under backlog).
        return PendingForecast(idx=idx, bar=bar, snapshot=tuple(buf), memo_key=memo_key)

    def _note_older_drop(self, bar: OHLCPoint, newest: int) -> None:
        """An OLDER bar is expected in bulk after a restart (the consumer and
        the feed replay from before the hydrated buffer's newest bar), and
        otherwise means an upstream ordering bug. Counted, and logged at most
        once a minute per machine so a replay cannot flood the log."""
        self._older_drops += 1
        now = time.monotonic()
        if now - self._older_drop_logged >= 60.0:
            log.warning("dropped %d bar(s) not newer than the buffer for %s/%s "
                        "(latest: %s ts=%s, newest buffered %s) — replay after a "
                        "restart, or an upstream ordering bug",
                        self._older_drops, self.symbol, self.ladder_name,
                        bar.timeframe, bar.timestamp, newest)
            self._older_drops = 0
            self._older_drop_logged = now

    # ---------------------------------------------------------------
    # STAGE 2 — forecast: the slow part. NO lock held. Touches no machine state.
    # ---------------------------------------------------------------
    async def forecast(self, pending: PendingForecast) -> None:
        """Obtain the Sumtyme forecast for `pending`, via the memo if another
        ladder's machine already has (or is computing) it, else via the gate.

        Sets pending.forecast to a USABLE forecast dict, or leaves it None —
        a timeout, SDK error, Exception-object or empty result all mean "skip
        this bar", exactly as before. Never raises for those; apply() turns a
        None into a no-op. Reads only `pending` (and the shared memo/gate);
        never self._buffers / _latest_signals / _active_chains, so it is safe
        to run concurrently with prepare() and apply() of other bars.
        """
        bar = pending.bar
        memo = self._forecast_memo
        claim = None

        # The frame is needed on EVERY path (placeholder_dt comes from it, and
        # the signal is persisted against placeholder_dt). Built lazily so the
        # API path builds it only once the permit is held.
        frame: dict = {}

        def _frame():
            if not frame:
                df = self._build_df(pending.snapshot)
                frame["api_df"], frame["placeholder_dt"] = self._prepare_df(df, bar.timeframe)
            return frame

        forecast = None
        if memo is not None and pending.memo_key is not None:
            claim = memo.claim(pending.memo_key)
            if claim.value is not None:
                forecast = claim.value
                pending.memo_hit = True
                log.debug("forecast memo HIT %s/%s ts=%s — Sumtyme call skipped",
                          self.symbol, bar.timeframe, bar.timestamp)
            elif not claim.owner:
                # Another ladder's machine is forecasting this exact window
                # right now. Wait for it rather than paying for the same answer
                # twice. A None result means it FAILED — failures are never
                # shared (the memo's standing rule), so fall through and call
                # the API ourselves.
                forecast = await claim.future
                if forecast is not None:
                    pending.memo_hit = True
                    log.debug("forecast memo JOIN %s/%s ts=%s — Sumtyme call skipped",
                              self.symbol, bar.timeframe, bar.timestamp)

        try:
            if forecast is None:
                forecast = await self._call_api(pending, _frame)
            if forecast is not None:
                pending.forecast = forecast
                pending.placeholder_dt = _frame()["placeholder_dt"]
                if memo is not None and pending.memo_key is not None:
                    # Store for the other ladder's machine — only USABLE results
                    # reach this line, so a failure is never replayed cross-ladder.
                    # Resolving a hit/join is an idempotent same-value overwrite.
                    memo.resolve(pending.memo_key, forecast)
        finally:
            # OWNER CONTRACT: the owner of an in-flight claim must settle it on
            # every exit (including cancellation during shutdown), or a joiner
            # awaits forever. fail() is a no-op once resolved.
            if claim is not None and claim.owner and pending.forecast is None:
                memo.fail(pending.memo_key)

    async def _call_api(self, pending: PendingForecast, _frame) -> Any:
        """The gated, bounded, blocking Sumtyme call. Returns a usable forecast
        or None (logged). Factored out of forecast() so the memo logic above
        reads straight; the error handling is the original on_bar's, verbatim."""
        bar = pending.bar
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
        # signature of queueing, not failure). The gate holds the permits, so the
        # queue lives here, where waiting is free and cancellable.
        #
        # The gate also measures single-call latency L and logs the resulting
        # CAPACITY (concurrency/L forecasts/sec). If demand exceeds that, no
        # client-side change can help — it is a product decision (fewer
        # symbols/rungs, or more parallelism from Sumtyme).
        #
        # NOTE: wait_for cancels the AWAIT, not the underlying thread. The SDK
        # exposes no request-timeout kwarg (signal_process logs a warning at
        # startup), so a genuinely hung socket still occupies a thread-pool worker
        # until the OS returns. The gate bounds the damage: at most `concurrency`
        # such calls can ever be stuck at once.
        try:
            if self._forecast_gate is not None:
                forecast = await self._forecast_gate.run(
                    self._forecast_fn,
                    interval=interval,
                    interval_unit=interval_unit,
                    reasoning_mode="reactive",
                    timeframe_dependent=False,
                    time_series_type="ohlc",
                    timeout_s=self._forecast_timeout_s,
                    # Built AFTER the permit is acquired (see prepare()).
                    prepare=lambda: {"data_input": _frame()["api_df"]},
                )
            else:
                # Ungated path: standalone/test use only. In the live process every
                # machine is given the shared gate.
                forecast = await asyncio.wait_for(
                    asyncio.to_thread(
                        self._forecast_fn,
                        time_series_type="ohlc",
                        data_input=_frame()["api_df"],
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
            _note_forecast_failure("timeout")
            return None
        except asyncio.CancelledError:
            raise
        except Exception:
            # The SDK raised (network, 4xx/5xx, bad payload). Previously a raised
            # exception here would unwind out of on_bar -> run() and stop the
            # consume loop entirely (silent DB freeze). Catch, log with traceback,
            # skip the bar — the loop keeps draining.
            log.exception(
                "forecast ERROR for %s/%s ts=%s — skipping bar",
                self.symbol, bar.timeframe, bar.timestamp,
            )
            _note_forecast_failure("error")
            return None

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
            _note_forecast_failure("unusable result")
            return None
        if not forecast:
            log.warning(
                "forecast returned empty/falsy result (%r) for %s/%s ts=%s — skipping bar",
                forecast, self.symbol, bar.timeframe, bar.timestamp,
            )
            _note_forecast_failure("empty result")
            return None
        return forecast

    # ---------------------------------------------------------------
    # STAGE 3 — apply: cache + state machine. Caller holds the symbol lock.
    # ---------------------------------------------------------------
    async def apply(self, pending: PendingForecast) -> List[ChainEvent]:
        """Apply one forecast to machine state and run INIT/PROPAGATE/TERMINATE.

        MUST be called in the same order prepare() was called for this symbol
        (the router/worker pool guarantee this), because the state machine's
        only notion of "newer" is the last write to _latest_signals[idx]. A
        pending with forecast None (skipped bar) is a no-op: nothing is cached,
        no watermark moves, no events fire — the bar is simply absent, as before.
        """
        if pending.forecast is None:
            return []

        bar = pending.bar
        idx = pending.idx
        signal_value = list(pending.forecast.values())[0]
        current_price = bar.close
        # Two times per signal. `timestamp` is the bar's LABEL (its open, as
        # every vendor and the rest of this repo label bars) and is what chain
        # steps, origin_time and decided_at record. `bar_close` is when the
        # information existed, and is what the sweep COMPARES across rungs: a
        # higher rung may extend a chain only with information at least as new
        # as the chain's last step. Comparing opens would order a 1h bar
        # (11:00-12:00) an hour before a 30m bar (11:30-12:00) that closed at
        # the same moment, and refuse the extension.
        bar_iso = _epoch_ms_to_iso(bar.timestamp)
        close_iso = _epoch_ms_to_iso(_bar_close_ms(bar))

        # Success path is observable at DEBUG: a healthy pipeline emits a steady
        # stream of these; sudden silence (with bars still arriving) localises a
        # stall to *after* the forecast returns. Cheap heartbeat for the loop.
        log.debug("forecast OK %s/%s ts=%s -> signal=%s",
                  self.symbol, bar.timeframe, bar.timestamp, signal_value)

        # Persist the raw signal (research/audit). Failure here is recoverable —
        # the next bar's UPSERT re-writes it — so log-and-continue, not raise.
        try:
            await self._persist_signal(
                self.symbol, bar.timeframe, pending.placeholder_dt,
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
            "bar_close": close_iso,
            "price": current_price,
        }

        # PROCESSED-THROUGH WATERMARK. Recorded here — after the forecast landed
        # and the cache advanced — so it means "this machine consumed this bar",
        # not "a bar arrived". A neutral signal still counts: the machine did its
        # work and decided nothing, which is exactly the case an auditor cannot
        # distinguish from a dropped bar by watching chain events alone.
        #
        # Deliberately before the signal_value == 0 return, so silence in the
        # chain stream is still visible as forward progress.
        if self._watermarks is not None:
            self._watermarks.record(
                self.symbol, self.ladder_name, bar.timeframe, bar.timestamp)

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
                chain_id = chain_id_for(
                    self.symbol, self.asset_class, self.ladder_name,
                    base_sig["signal"], base_sig["timestamp"],
                )
                # Already live? Compare the chain's IDENTITY — direction plus
                # origin timestamp — not the id. Chains retained across migration
                # 019 kept their original uuid4, so a computed id will never match
                # theirs even when it is the same chain, and a replayed bar would
                # insert a second row for one that is already in memory. This is
                # the same tuple the hash is built from, so it subsumes an id
                # comparison and holds across both id generations.
                #
                # Re-initiating would fork the in-memory chain and have the two
                # copies race each other's chain_steps writes to a mutual
                # ON CONFLICT DO NOTHING. Advance the guard and fall through to
                # the propagation sweep, which is where a replayed bar belongs.
                already_live = any(
                    c["direction"] == base_sig["signal"]
                    and c["history"][0]["timestamp"] == base_sig["timestamp"]
                    for c in self._active_chains.values()
                )
                if already_live:
                    self._last_base_signal = base_sig["signal"]
                    log.debug("init skipped, chain already live: %s %s/%s",
                              chain_id, self.symbol, self.ladder_name)
                else:
                    first_step = {
                        "level": 1, "timeframe": base_sig["timeframe"],
                        "timestamp": base_sig["timestamp"],
                        "bar_close": base_sig["bar_close"],
                        "detected_at": _now_iso(),
                        "price": base_sig["price"],
                    }
                    self._last_base_signal = base_sig["signal"]
                    chain = {
                        "chain_id": chain_id, "symbol": self.symbol,
                        "asset_class": self.asset_class, "ladder": self.ladder_name,
                        "direction": base_sig["signal"],
                        "current_level": 0, "next_expected_level": 1,
                        "active_timeframes": [base_sig["timeframe"]],
                        "history": [first_step], "last_timestamp": base_sig["timestamp"],
                        "last_bar_close": base_sig["bar_close"],
                    }
                    self._active_chains[chain_id] = chain
                    try:
                        await self._persist_init(chain)
                    except Exception as e:
                        log.warning("chain init persist failed %s: %r", self.symbol, e)
                    events.append(ChainEvent(
                        event_type="chain_initiated", chain_id=chain_id,
                        symbol=self.symbol, asset_class=self.asset_class,
                        direction=base_sig["signal"], propagation_level=1,
                        timeframe=base_sig["timeframe"], timestamp=base_sig["timestamp"],
                        bar_close=base_sig["bar_close"],
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
                last_close = _last_bar_close(chain)
                if next_sig and next_sig["bar_close"] >= last_close:
                    if next_sig["signal"] == chain["direction"]:
                        # PROPAGATE
                        chain["current_level"] = expected_idx
                        chain["next_expected_level"] = expected_idx + 1
                        chain["last_timestamp"] = next_sig["timestamp"]
                        chain["last_bar_close"] = next_sig["bar_close"]
                        if next_sig["timeframe"] not in chain["active_timeframes"]:
                            chain["active_timeframes"].append(next_sig["timeframe"])
                        new_step = {
                            "level": expected_idx + 1, "timeframe": next_sig["timeframe"],
                            "timestamp": next_sig["timestamp"],
                            "bar_close": next_sig["bar_close"],
                            "detected_at": _now_iso(),
                            "price": next_sig["price"],
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
                            bar_close=next_sig["bar_close"],
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
                            bar_close=next_sig["bar_close"],
                            price=next_sig["price"],
                        ))
                        break

                if not progress_made:
                    curr_sig = self._latest_signals.get(curr_idx)
                    if (curr_sig and curr_sig["bar_close"] >= last_close
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
                            bar_close=curr_sig["bar_close"],
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