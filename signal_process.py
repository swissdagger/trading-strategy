"""
signal_process.py — The runnable signal process.

Assembles the producer + strategy into ONE process: consumes closed bars from
Kafka (raw-prices), runs both chain ladders per symbol via the FanoutRouter,
folds the resulting tagged events into the StrategyRuntime, and emits trade
signals to the trade_signals table. This is what you run NOW (the position
manager stays a separate, dark process).

PIPELINE (all in this process):

  Kafka raw-prices ─► bar ─► FanoutRouter.on_bar
                                 │ (per (symbol,set) ServiceTwoChainMachine)
                                 ▼ tagged chain events
                          publish:  ├─► Kafka chain-events (audit/other consumers)
                                    └─► StrategyRuntime.on_event (in-proc)
                                            │ four-step entry + OR-exit
                                            ▼
                                      trade_signals table

The FanoutRouter and StrategyRuntime SHARE one per-symbol lock map, so a symbol's
producer work (running its two ladders) and consumer work (strategy evaluation)
never interleave — one bar is fully processed before the next for that symbol.

Everything venue/DB/Sumtyme-specific is injected here at wiring time; the
components themselves stay free of globals. Replace the bar source (Kafka) or the
publish sink without touching the strategy or the chain machine.

CONFIG via env (see _load_config): DB URL, Sumtyme key, Kafka bootstrap, topics.
The chain-machine dataframe helpers (prepare_dataframe_for_sumtyme,
_to_sumtyme_interval, build-df) are imported from the extracted service_2 utils —
see the NOTE in _make_factory about supplying them.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import os
import signal as os_signal
import time
from typing import Optional

import asyncpg
from aiokafka import TopicPartition

from execution.observability import HealthServer, Metrics
from execution.signal_writer import SignalWriter, SignalWriterHooks
from producer.audit import (
    AuditCache,
    WatermarkRegistry,
    build_signal_health_document,
    make_chain_audit_refresher,
)
from producer.async_writer import BatchedAuditWriter
from producer.fanout_router_multi import MultiLadderRouter
from producer.forecast_gate import ForecastGate, ForecastMemo
from producer.offset_tracker import OffsetTracker
from producer.symbol_workers import SymbolWorkerPool
from feeds.base import timeframe_to_ms
from ops.alerts import alert, install_alerting, start_heartbeat
from producer.persistence import (
    make_insert_signal,
    make_load_active_chains,
    make_load_chain_view_seed,
    make_load_open_positions,
    # NOTE: only chain_init remains synchronous. persist_signal / chain_propagate
    # / chain_terminate are now served by BatchedAuditWriter (off the hot path);
    # their sync factories are intentionally no longer imported here.
    make_persist_chain_init,
    make_load_buffers,
    make_persist_live_bar,
    make_prune_bar_store,
    make_load_last_base_signal,
    make_load_strategy_open_entries,
    make_load_opposing_chain_since,
    make_load_chain_confirmed,
)
from producer.service2_machine import ServiceTwoChainMachine
from strategy.chain_view import ChainView, TaggedChainEvent
from strategy.ladder_registry import default_registry
from strategy.registry import StrategyRegistry, Strategy
from strategy.multi_runtime import MultiStrategyRuntime, RuntimeHooks, OpenPosition
from strategy.two_ladder_strategy import TwoLadderEntry, TwoLadderExit
from strategy.confirmed_15m_adapter import make_confirmed_15m
from strategy.chain_recorder import make_chain_recorder
from strategy.chain_rules import make_chain_rule
from strategy.prior_failure_adapter import make_prior_failure_scaleout

log = logging.getLogger("signal_process")


# ---------------------------------------------------------------------------
class Config:
    def __init__(self) -> None:
        self.db_url = _require("TRADING_DB_URL")
        self.sumtyme_key = _require("SUMTYME_API_KEY")
        self.kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
        # Dedicated bar topic on THIS system's Kafka — deliberately NOT named
        # 'raw-prices' so it can't be confused with CausaView's topic. The
        # trading system's feeds publish the two-ladder granularities here; this
        # stream is independent of CausaView entirely.
        self.bars_topic = os.environ.get("TRADING_BARS_TOPIC", "trading-bars")
        self.events_topic = os.environ.get("CHAIN_EVENTS_TOPIC", "chain-events")
        self.consumer_group = os.environ.get(
            "SIGNAL_CONSUMER_GROUP", "signal-process-group")
        # --- throughput / durability tunables (see run()) ---
        # Per-symbol queue depth. Bounded: a full queue backpressures the bar
        # consumer rather than shedding bars (a dropped bar is a HOLE in the chain
        # machine's input, unlike a shed audit row).
        self.symbol_queue_max = int(
            os.environ.get("SYMBOL_QUEUE_MAX", "1000"))
        # How many of ONE symbol's forecasts may be in flight at once (below
        # the global FORECAST_CONCURRENCY). Since the forecast/apply split a
        # symbol no longer serialises its own forecasts; this cap stops a
        # multi-rung boundary (12 bars for one symbol at a 6h close) from
        # grabbing every gate permit and starving other symbols' baselines.
        self.symbol_inflight_max = int(
            os.environ.get("SYMBOL_INFLIGHT_MAX", "4"))
        # How often the consume loop commits the contiguous low-water offset.
        self.commit_interval_s = float(
            os.environ.get("COMMIT_INTERVAL_S", "5.0"))
        # Wall-clock age (bar timestamp -> now) above which we warn. This is the
        # gauge that would have caught the multi-day stall on day one.
        self.lag_warn_s = float(os.environ.get("LAG_WARN_S", "300"))
        # --- Audit + health endpoint ---
        # Port the health/watermark/audit surface binds. Expose it to whoever is
        # auditing; it is read-only and carries no credentials or trade detail.
        self.health_port = int(os.environ.get("HEALTH_PORT", "8081"))
        # How often the chain-integrity counts are recomputed. NOT per request —
        # a scraping loop must not be able to load the DB. 60s is well inside the
        # window anyone auditing would care about, and the document reports its
        # own age so a stale view is never mistaken for a fresh one.
        self.audit_refresh_seconds = float(
            os.environ.get("AUDIT_REFRESH_S", "60"))
        # Client-side request timeout for the blocking Sumtyme SDK call.
        #
        # With the ForecastGate holding a single permit, this now measures what it
        # was always meant to: SUMTYME'S ACTUAL COMPUTE TIME. Before the gate, a
        # call could sit behind 9 others in Sumtyme's server-side queue, so the
        # timeout was mostly measuring OUR OWN pile-up and killed calls that had
        # barely started (1145 timeouts, zero errors). Now a call that starts is a
        # call that runs, so this can be a tight, meaningful bound.
        self.sumtyme_timeout_s = float(
            os.environ.get("SUMTYME_TIMEOUT_S", "20"))
        # How many Sumtyme calls may be in flight at once.
        #
        # MEASURED, not guessed (benchmark on the live VM, 5000-row window):
        #
        #     N=1 ->  8.13s/call | 0.12 calls/sec
        #     N=4 ->  3.73s/call | 0.27 calls/sec
        #     N=6 ->  2.75s/call | 0.36 calls/sec   <-- PEAK
        #     N=8 ->  4.23s/call | 0.24 calls/sec   <-- collapses
        #
        # Sumtyme DOES parallelise (4 concurrent calls take 15s, not 32s), but not
        # linearly, and throughput peaks around 6. The N=8 collapse was an artifact
        # of the default thread pool (min(32, cpu_count+4) = 6 on 2 vCPUs), which
        # is why thread_pool_size below is now raised — re-benchmark past 6 with a
        # bigger pool before assuming 6 is the true ceiling.
        #
        # Demand is ~0.25 forecast-eligible bars/sec (10 symbols x 5 rungs; the 1m
        # rung alone is ~62% of it). At 0.36 calls/sec there is ~44% headroom.
        # Set this too LOW and the backlog grows; too HIGH and calls queue, blow
        # SUMTYME_TIMEOUT_S, and get discarded AFTER Sumtyme already spent the
        # compute — strictly worse than not sending them.
        self.forecast_concurrency = int(
            os.environ.get("FORECAST_CONCURRENCY", "6"))
        # Worker threads for asyncio.to_thread (i.e. for the blocking Sumtyme SDK
        # call). MUST exceed forecast_concurrency, or the pool — not the gate —
        # silently becomes the real concurrency limit. These threads are blocked on
        # a socket, not burning CPU, so this is sized to the desired in-flight
        # count rather than to vCPUs.
        self.thread_pool_size = int(
            os.environ.get("THREAD_POOL_SIZE", "16"))


def _require(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"missing required env var {name}")
    return v


# ---------------------------------------------------------------------------
class SignalProcess:
    """Owns the assembled pipeline and its lifecycle."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.pool: Optional[asyncpg.Pool] = None
        self.consumer = None
        self.producer = None
        self.router: Optional[MultiLadderRouter] = None
        self.runtime: Optional[MultiStrategyRuntime] = None
        self.ladders = None            # LadderRegistry, built in start()
        self.strategies = None         # StrategyRegistry, built in start()
        self.resolver = None   # ClassResolver, built in start() from ASSET_CONFIGS
        self.audit_writer: Optional[BatchedAuditWriter] = None
        self.forecast_gate: Optional[ForecastGate] = None
        self.workers: Optional[SymbolWorkerPool] = None
        self.offsets = OffsetTracker()
        self._stop = asyncio.Event()

    async def start(self) -> None:
        # --- DB pool to the NEW trading project ---
        self.pool = await asyncpg.create_pool(self.cfg.db_url, min_size=2, max_size=12, statement_cache_size=0)

        # --- Asset-class resolver from ASSET_CONFIGS (single source of truth) ---
        from config import ASSET_CONFIGS
        from producer.universe import ClassResolver, build_symbol_class_map
        self.resolver = ClassResolver(build_symbol_class_map(ASSET_CONFIGS))

        # --- Sumtyme client (blocking SDK; the machine wraps calls in to_thread) ---
        import sumtyme  # imported here so the module loads without the SDK present
        # A CLIENT-SIDE request timeout is load-bearing, not belt-and-braces. The
        # machine bounds the forecast with asyncio.wait_for, but wait_for cancels
        # the AWAIT, not the thread: the to_thread worker stays blocked inside the
        # SDK's socket read until the OS returns. Repeated hangs therefore LEAK
        # worker threads (the machine's own NOTE says exactly this). Under the new
        # per-symbol concurrency there can be one forecast in flight per symbol, so
        # a degraded Sumtyme could leak a thread per symbol per bar. The SDK's
        # kwarg name is not guaranteed across versions — try the common ones and
        # fail LOUD if none is accepted, rather than silently running unbounded.
        client = None
        for kwarg in ("timeout", "request_timeout"):
            try:
                client = sumtyme.client(
                    apikey=self.cfg.sumtyme_key,
                    **{kwarg: self.cfg.sumtyme_timeout_s},
                )
                log.info("sumtyme client: %s=%ss", kwarg, self.cfg.sumtyme_timeout_s)
                break
            except TypeError:
                continue
        if client is None:
            client = sumtyme.client(apikey=self.cfg.sumtyme_key)
            log.warning(
                "sumtyme client accepts no timeout kwarg — forecasts are NOT "
                "bounded at the socket. asyncio.wait_for still skips the bar, but "
                "a hung call LEAKS its worker thread. Set the timeout on the SDK "
                "(session/transport) if it exposes one; otherwise watch thread count."
            )

        # --- Kafka consumer (bars in) + producer (tagged events out) ---
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
        import json as _json
        self.consumer = AIOKafkaConsumer(
            self.cfg.bars_topic,
            bootstrap_servers=self.cfg.kafka_bootstrap,
            group_id=self.cfg.consumer_group,
            value_deserializer=lambda b: _json.loads(b.decode()),
            # MANUAL COMMIT — mandatory now, not a preference. Bars are handed to
            # per-symbol workers and are still IN FLIGHT when the consume loop
            # moves on. Auto-commit would acknowledge offsets for bars sitting in
            # a queue; a crash would drop them with no replay, leaving permanent
            # holes in the chain machine's input. We commit only the contiguous
            # low-water mark of FINISHED bars (see run() / OffsetTracker).
            enable_auto_commit=False,
            # EARLIEST: a group with no committed offset (first start) reads
            # the whole topic. The bars after warmup's newest may already have
            # been published before this consumer joined; "latest" skipped
            # them, leaving a hole after the hydrated buffer. Everything older
            # than the buffer is dropped by the machines' monotonic guard.
            auto_offset_reset="earliest",
        )
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.cfg.kafka_bootstrap,
            value_serializer=lambda v: _json.dumps(v).encode(),
        )
        await self.consumer.start()
        await self.producer.start()

        # --- The ONE forecast gate, shared by every chain machine ---
        # Exactly one instance per process. Constructing one per machine would give
        # each its own permit and let all 10 fire at once — precisely the bug this
        # fixes. Sumtyme serialises concurrent calls, so we serialise them here,
        # where waiting is free instead of burning their compute on results we time
        # out and throw away.
        self.forecast_gate = ForecastGate(
            concurrency=self.cfg.forecast_concurrency)
        log.info(
            "forecast gate: concurrency=%d, timeout=%ss (Sumtyme serialises "
            "concurrent calls server-side, so concurrency>1 does NOT parallelise)",
            self.cfg.forecast_concurrency, self.cfg.sumtyme_timeout_s,
        )

        # --- The ONE forecast memo, shared by every chain machine ---
        # Same one-per-process discipline as the gate, opposite job: the gate
        # bounds how many Sumtyme calls run; the memo eliminates the ones that
        # don't need to run at all. Sumtyme is deterministic, so when two
        # ladders share a timeframe (FC and PF share 1m/3m/5m/15m/1h), the
        # second machine's forecast of the same (symbol, timeframe, window) is
        # a memo hit, not an API call — without this, adding PF would roughly
        # DOUBLE crypto's Sumtyme demand for identical answers.
        self.forecast_memo = ForecastMemo()

        # --- Audit surface: processed-through watermarks + chain integrity ---
        # Chain events are a SPARSE output — a machine can consume bars correctly
        # for hours and emit nothing because nothing propagated. So the chain
        # stream cannot distinguish "working, nothing to say" from "wedged" or
        # "dropping bars", and a raising hydrate once took down bar processing for
        # every symbol while the process stayed up and looked fine. The watermark
        # makes forward progress observable independently of whether anything fired.
        #
        # SHARED like the gate and the memo: one registry, injected into every
        # machine, so /watermarks shows the whole process.
        self.watermarks = WatermarkRegistry()
        # Chain-integrity counts, refreshed on a timer rather than per request, so
        # a scraping loop cannot put load on the DB and the HTTP thread never
        # touches asyncio.
        self.audit_cache = AuditCache()
        self.metrics = Metrics()
        self._audit_task: Optional[asyncio.Task] = None
        self._last_bar_monotonic = time.monotonic()
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._prune_task: Optional[asyncio.Task] = None
        self._health_server: Optional[HealthServer] = None

        # --- Batched audit writer: research/audit writes OFF the hot path ---
        # Adopting the writer exactly as its docstring prescribes (it was built,
        # documented, and left dormant for this moment). It takes the signals /
        # chain-propagate / chain-terminate round-trips off the per-bar path and
        # amortises them via executemany.
        #
        # HYBRID, deliberately: chain INIT stays SYNCHRONOUS (migration 002's
        # write-before-emit ordering — trades.chain_id foreign-keys to chains, so
        # the chain row must be durable before the event is consumed), and
        # trade_signals stays SYNCHRONOUS in SignalWriter (it is the PRODUCT; it
        # must never be inside a bounded-loss window).
        #
        # ACCEPTED TRADE: research/audit rows enqueued but unflushed are LOST on a
        # hard crash (bounded by queue depth), and land up to flush_interval_s
        # late. Both are explicitly sanctioned in the writer's DURABILITY CONTRACT.
        self.audit_writer = BatchedAuditWriter(self.pool)
        self.audit_writer.start()

        # --- Signal writer factory (per-strategy, tagged with the name) ---
        insert_signal = make_insert_signal(self.pool)

        def make_writer(strategy_name: str) -> SignalWriter:
            return SignalWriter(
                SignalWriterHooks(insert_signal=insert_signal),
                strategy=strategy_name,
            )

        # --- Ladder registry: built-in A/B plus any extras from config ---
        from config import EXTRA_LADDERS, STRATEGIES
        self.ladders = default_registry()
        for name, tfs in EXTRA_LADDERS.items():
            self.ladders.register(name, tfs)

        # --- Strategy registry: register factories, build enabled from config ---
        view = ChainView()   # the two-ladder shared view (A/B only)
        self.strategies = StrategyRegistry()
        self.strategies.register_factory(
            "two_ladder",
            lambda *, name, symbols, ladders, params: Strategy(
                name=name,
                entry=TwoLadderEntry(view, make_writer(name)),
                exit=TwoLadderExit(view, make_writer(name)),
                symbols=symbols, ladders=ladders,
            ),
        )
        self.strategies.register_factory(
            "confirmed_15m",
            lambda *, name, symbols, ladders, params: make_confirmed_15m(
                name=name, symbols=symbols, ladders=ladders, params=params,
                view_registry=self.ladders, writer_factory=make_writer,
            ),
        )
        # prior_failure_scaleout: same factory shape as confirmed_15m (own
        # ChainHistoryView on its own ladder). Registered UNCONDITIONALLY —
        # the config's `enabled` flag decides instantiation, and the factory
        # validates the schedule-fits-ladder invariant loudly at build time.
        self.strategies.register_factory(
            "prior_failure_scaleout",
            lambda *, name, symbols, ladders, params: make_prior_failure_scaleout(
                name=name, symbols=symbols, ladders=ladders, params=params,
                view_registry=self.ladders, writer_factory=make_writer,
            ),
        )
        # chain_recorder: record-only owner for a ladder evaluated as data (the
        # dense equity ladder DL until its strategy lands). Same factory shape.
        self.strategies.register_factory(
            "chain_recorder",
            lambda *, name, symbols, ladders, params: make_chain_recorder(
                name=name, symbols=symbols, ladders=ladders, params=params,
                view_registry=self.ladders, writer_factory=make_writer,
            ),
        )
        # The owner's frozen chain rules (strategy/rules.py via chain_rules.py).
        # Their volatility filters need 1m closes per asset class, from the
        # same feeds that serve the ladders: Polygon for equities, Coinbase
        # for crypto. One closes source per class, built lazily on first use
        # so a config without that class never needs its key here.
        self._closes_feeds: dict = {}
        self._vol_providers: dict = {}
        self._closes_sources: dict = {}

        def _closes_source(asset_class: str):
            if asset_class not in self._closes_sources:
                from strategy.vol_source import FeedClosesSource, VolStateProvider
                syms = list(ASSET_CONFIGS.get(asset_class, {}).get("symbols", []))
                if asset_class == "equity":
                    from feeds.polygon_stocks import PolygonStocksFeed
                    feed = PolygonStocksFeed(_require("POLYGON_API_KEY"), syms, ["1m"])
                elif asset_class == "crypto":
                    from feeds.coinbase_spot import CoinbaseSpotFeed
                    feed = CoinbaseSpotFeed(syms, ["1m"])
                else:
                    raise RuntimeError(f"no 1m closes source for asset class {asset_class!r}")
                self._closes_feeds[asset_class] = feed
                src = FeedClosesSource(feed)
                self._closes_sources[asset_class] = src
                self._vol_providers[asset_class] = VolStateProvider(src)
            return self._closes_sources[asset_class]

        def _vol_for(asset_class: str):
            _closes_source(asset_class)
            return self._vol_providers[asset_class]

        # The price a rule records for a clock-driven intent: the newest close
        # in the bar stream (fed from the consume loop), falling back to the
        # vol source's cache before the first bar for a symbol arrives.
        from strategy.vol_source import LiveCloses
        self._live_closes: dict = {}

        def _latest_close_for(asset_class: str):
            if asset_class not in self._live_closes:
                self._live_closes[asset_class] = LiveCloses(
                    fallback=_closes_source(asset_class).latest_close)
            return self._live_closes[asset_class].latest_close

        chain_rule_factory = lambda *, name, symbols, ladders, params: make_chain_rule(
            name=name, symbols=symbols, ladders=ladders, params=params,
            view_registry=self.ladders, writer_factory=make_writer,
            vol_provider_for=_vol_for, latest_close_for=_latest_close_for,
            load_open=make_load_strategy_open_entries(self.pool, name),
            load_opposing_since=make_load_opposing_chain_since(self.pool),
            load_confirmed=make_load_chain_confirmed(self.pool),
        )
        for key in ("rule1_v1", "rule1_new_literal_v1", "rule3_v1",
                    "rule3_new_literal_v1", "rule3_new_equiv_v1"):
            self.strategies.register_factory(key, chain_rule_factory)
        # All tracked symbols across the enabled strategies feed 'symbols: "*"'.
        all_symbols = _all_configured_symbols()
        self.strategies.build_from_config(STRATEGIES, all_symbols=all_symbols)

        # --- Multi-ladder router: one machine per (symbol, ladder) that a
        # strategy actually reads (compute-once, ownership-gated). The ownership
        # set is the (symbol, ladder) pairs enabled strategies declare; the router
        # intersects its timeframe-matched targets with it, so a symbol-scoped
        # strategy (e.g. two_ladder on AAPL only, confirmed_15m on BTC-USD only)
        # doesn't make the router compute A/B chains for BTC-USD just because
        # BTC-USD emits a shared timeframe (1m). Snapshot the set once at startup
        # (strategies are fixed for the process lifetime). ---
        owned = self.strategies.owned_symbol_ladders()
        self.router = MultiLadderRouter(
            registry=self.ladders,
            make_machine=self._make_factory(client),
            publish=self._publish,
            owns=lambda symbol, ladder: (symbol, ladder) in owned,
            persist_bar=make_persist_live_bar(self.pool),
        )
        # --- Multi-strategy runtime, sharing the router's lock map ---
        self.runtime = MultiStrategyRuntime(
            view=view,
            registry=self.strategies,
            hooks=self._runtime_hooks(),
            locks=self.router._locks,   # SHARE the router's lock map
        )
        await self.runtime.start()

        # --- Strategies with their own state/clock (Rule 1) ---
        # start(): restore open trades from trade_signals. tick(): the rule's
        # clock (stall exit, session-open drain), on a timer independent of
        # events — a symbol with no events still has to exit on time.
        self._strategy_tickers: list[asyncio.Task] = []
        for strat in self.strategies.active():
            comp = strat.entry
            if hasattr(comp, "start"):
                await comp.start()
            if hasattr(comp, "tick"):
                period = float((STRATEGIES.get(strat.name, {}).get("params") or {})
                               .get("tick_seconds", 15))
                self._strategy_tickers.append(asyncio.create_task(
                    self._tick_loop(comp, period), name=f"tick-{strat.name}"))

        # --- Audit + health endpoint ---
        # Started LAST, so a 200 from /health means the whole pipeline is up, not
        # just the process. Threaded (stdlib http.server on a daemon thread), NOT
        # async: a wedged event loop is precisely the failure this must report, so
        # the server must not share it.
        from config import BAR_STORE_KEEP_PER_STREAM, BAR_STORE_PRUNE_INTERVAL_S
        self._prune_task = asyncio.create_task(
            self._prune_loop(make_prune_bar_store(self.pool, BAR_STORE_KEEP_PER_STREAM),
                             BAR_STORE_PRUNE_INTERVAL_S),
            name="bar-store-prune")
        self._audit_task = asyncio.create_task(
            make_chain_audit_refresher(
                self.pool, self.audit_cache,
                interval_seconds=self.cfg.audit_refresh_seconds)(),
            name="chain-audit-refresh")
        self._health_server = HealthServer(
            lambda: build_signal_health_document(
                booted=True,
                watermarks=self.watermarks.snapshot(),
                audit=self.audit_cache.snapshot(),
                metrics_snapshot=self.metrics.snapshot()),
            self.metrics,
            port=self.cfg.health_port,
            extra_routes={
                "/watermarks": lambda: {"watermarks": self.watermarks.snapshot()},
                "/audit": self.audit_cache.snapshot,
            },
        )
        self._health_server.start()

        log.info("signal process started: strategies=%s ladders=%s bars<-%s events->%s",
                 [s.name for s in self.strategies.active()],
                 self.strategies.all_ladders(),
                 self.cfg.bars_topic, self.cfg.events_topic)

    def _make_factory(self, client):
        """Build the MachineFactory: (symbol, set) -> ServiceTwoChainMachine with
        real DB + Sumtyme hooks bound in.

        NOTE: the dataframe helpers (prepare_df, to_interval, build_df) are part of
        the extracted service_2 utilities. They must be supplied here from the
        extracted module. Until that utils extraction lands, these three are the
        remaining bind points — everything else is wired. They are pure functions
        (no state), so extracting them is mechanical.
        """
        from config import LADDER_OPTIONAL_TIMEFRAMES
        from producer.service2_utils import (  # extracted service_2 helpers
            prepare_dataframe_for_sumtyme,
            to_sumtyme_interval,
            build_sumtyme_df,
        )

        def factory(symbol: str, ladder_name: str, timeframes: list) -> ServiceTwoChainMachine:
            # Resolve the symbol's real asset class from ASSET_CONFIGS. The factory
            # is only ever called for symbols we've seen a bar for, and the run
            # loop skips unknown symbols before routing, so resolve() will succeed
            # here — but it raises clearly if a symbol slips through unmapped.
            asset_class = self.resolver.resolve(symbol)
            return ServiceTwoChainMachine(
                symbol=symbol, asset_class=asset_class,
                ladder=timeframes,
                # Part of the chain's deterministic identity, and the hydrate
                # discriminator. NOT derivable from `timeframes` — FC and PF
                # share the 1m baseline.
                ladder_name=ladder_name,
                forecast_fn=client.forecast_path,
                prepare_df_fn=prepare_dataframe_for_sumtyme,
                to_interval_fn=to_sumtyme_interval,
                build_df_fn=build_sumtyme_df,
                # DEFERRED (batched, off the hot path) — research/audit only.
                persist_signal=self.audit_writer.persist_signal,
                # SYNCHRONOUS — durability anchor. A chain row must be durable
                # before its chain_initiated event is acted on (trades.chain_id
                # FKs to chains). One write per chain, so this costs ~nothing.
                persist_chain_init=make_persist_chain_init(self.pool),
                # DEFERRED — audit. Signatures match the sync hooks exactly, so
                # this is a pure swap; the chain machine is unchanged and stays
                # oblivious to whether its writes are sync or deferred.
                persist_chain_propagate=self.audit_writer.persist_chain_propagate,
                persist_chain_terminate=self.audit_writer.persist_chain_terminate,
                # Filters on the ladder NAME (migration 026). The old baseline-
                # timeframe discriminator conflated FC and PF, whose baselines are
                # both 1m on the same crypto symbols: each machine adopted the
                # other's chains on restart and advanced them under its own level
                # numbering.
                load_active_chains=make_load_active_chains(
                    self.pool, symbol, asset_class, ladder_name),
                load_buffers=make_load_buffers(self.pool, symbol, timeframes),
                # Rebuilds the INIT guard so a restart doesn't mint a spurious
                # chain on the first baseline signal.
                load_last_base_signal=make_load_last_base_signal(
                    self.pool, symbol, timeframes[0]),
                # Was omitted, so the machine silently used its 30s DEFAULT and
                # SUMTYME_TIMEOUT_S had no effect on anything (the client
                # constructor rejected the kwarg, so it was applied nowhere at
                # all). Now it actually binds.
                forecast_timeout_s=self.cfg.sumtyme_timeout_s,
                # The SHARED gate — the SAME object for every machine, by design.
                # One permit total across the process, because Sumtyme serialises
                # concurrent calls anyway.
                forecast_gate=self.forecast_gate,
                # The SHARED memo — also the same object for every machine, so a
                # window forecast by one ladder's machine is a hit for another's.
                forecast_memo=self.forecast_memo,
                # Rungs whose short buffer is expected (config): downgrades the
                # hydrate fill report to INFO for them (e.g. PF's 12h).
                optional_timeframes=set(
                    LADDER_OPTIONAL_TIMEFRAMES.get(ladder_name, [])),
                # SHARED across every machine, like the gate and the memo:
                # the audit endpoint reads one registry to see the whole
                # process's progress.
                watermarks=self.watermarks,
            )
        return factory

    async def _prune_loop(self, prune, period: float) -> None:
        """Keep warmup_bars bounded: it gains every live bar (config:
        BAR_STORE_KEEP_PER_STREAM). First run one period after start, so a
        restart is not slowed by it."""
        while not self._stop.is_set():
            await asyncio.sleep(period)
            try:
                deleted = await prune()
                log.info("bar store pruned: %d row(s) deleted", deleted)
            except Exception:
                log.exception("bar store prune failed (retried next period)")

    async def _tick_loop(self, comp, period: float) -> None:
        while not self._stop.is_set():
            try:
                await comp.tick()
            except Exception:
                log.exception("strategy tick failed (continuing)")
            await asyncio.sleep(period)

    def _runtime_hooks(self):
        """Persistence for the multi-strategy open-position watch set.

        load_open_positions is DB-backed (reading trade_signals, now including
        the strategy column) so the open set survives restarts, per strategy.
        Positions are keyed by the NAMESPACED tracking_key (strategy/position_key)
        so two strategies' positions on one chain stay distinct.

        save/remove are deliberately NO-OPS: SignalWriter already durably
        persists every ENTER/EXIT to trade_signals at decision time (that's what
        load reads back). See the original single-strategy note — the reasoning
        is unchanged; only the shape (namespaced) differs."""
        from strategy.registry import namespaced_position_key

        load_open_rows = make_load_open_positions(self.pool)

        async def load_open():
            rows = await load_open_rows()
            out = []
            for r in rows:
                strategy = r.get("strategy", "two_ladder")
                base_pkey = r["position_key"]
                out.append(OpenPosition(
                    tracking_key=namespaced_position_key(strategy, base_pkey),
                    strategy_name=strategy,
                    position_key=base_pkey,
                    symbol=r["symbol"],
                    asset_class=r["asset_class"],
                    chain_prime_id=r["chain_id"],
                    trade_direction=1 if r["direction"] == "LONG" else -1,
                ))
            return out

        async def save_open(p):
            pass   # already durable: SignalWriter.emit_entry wrote this ENTER

        async def remove_open(k):
            pass   # already durable: SignalWriter.emit_exit wrote the EXIT

        return RuntimeHooks(
            load_open_positions=load_open,
            save_open_position=save_open,
            remove_open_position=remove_open,
        )

    async def _publish(self, raw: dict) -> None:
        """Router publish sink: send to Kafka chain-events (audit + any other
        consumers) AND feed the in-process multi-strategy runtime.

        The router produces RAW event dicts (carrying `ladder` and, for A/B,
        `set`). We forward the dict to Kafka and to the runtime. The runtime's
        _process_raw_locked is called DIRECTLY (not on_raw_event) because the
        router already holds this symbol's lock — and the router and runtime
        SHARE one lock map, so re-acquiring would deadlock on the non-reentrant
        asyncio.Lock."""
        # Kafka (fire to the events topic for durability / other consumers).
        try:
            await self.producer.send(self.cfg.events_topic, raw)
        except Exception:
            log.exception("failed to publish tagged event to Kafka (continuing)")

        # Feed the runtime under the lock the router already holds.
        await self.runtime._process_raw_locked(raw)

    async def run(self) -> None:
        """Consume bars and dispatch them to PER-SYMBOL ORDERED WORKERS.

        THE CONCURRENCY SHAPE (read this before changing it). Bars used to be
        awaited strictly sequentially here, which was load-bearing: the router
        takes the per-symbol lock, the runtime SHARES that lock map, and _publish
        calls runtime._process_raw_locked() assuming the router already holds the
        symbol's lock. The old docstring warned that a blanket create_task per bar
        would let two bars for the SAME symbol run at once — contending for the
        lock, reaching the ChainView out of order, and silently breaking the
        chain machine's timestamp-ordered propagation WITHOUT RAISING. That
        warning still stands, and it prescribed the fix:

            "the correct shape is per-symbol ordered workers (one queue/task per
             symbol), not a blanket create_task per bar"

        That is what this now is. Each symbol has ONE queue and ONE worker
        processing its bars strictly in order, so the per-symbol invariant is
        preserved exactly as before (the lock remains correct and load-bearing —
        the workers make contention on it rare, they do not remove the need for
        it). Different symbols proceed CONCURRENTLY, which is the entire point:
        the per-bar cost is dominated by ONE BLOCKING SUMTYME CALL, and those
        have no reason to serialise across symbols. With N symbols this is an
        ~N-fold throughput increase, which is what closes the lag.

        ROUND TWO (dense ladders). Per-symbol serial processing caps ONE
        symbol at 1 / wall-latency of a Sumtyme call, which a sub-minute ladder
        exceeds on its own. So the chain machine is now split into
        prepare (append + snapshot, under the lock) / forecast (no lock) /
        apply (cache + state machine, under the lock), and the pool runs each
        symbol as a stager + applier pipeline: prepares in arrival order,
        forecasts concurrently (capped by SYMBOL_INFLIGHT_MAX below the global
        gate), and applies STRICTLY in arrival order through a FIFO of
        futures. The ordering invariant the paragraph above protects is
        unchanged — it now lives in the apply stage — and _run_state_machine
        itself is untouched. See producer/symbol_workers.py.

        WHY THE LAG HAPPENED (for the next person): per-bar cost (one forecast +
        its DB round-trips, in series) exceeded the bar arrival rate across the
        universe, so consumer lag grew monotonically — 38k messages and ~39h
        behind, with signals rows landing at a created_at ~39h after their bar
        timestamp. It was never DB write throughput; the DB was always committing
        in milliseconds. It was this loop.

        OFFSETS. Because bars are IN FLIGHT when this loop moves on, offsets are
        committed MANUALLY, and only at the CONTIGUOUS LOW-WATER MARK of finished
        bars (OffsetTracker). Committing the highest finished offset would skip a
        slower in-flight bar below it and lose it permanently on a crash.
        Semantics are at-least-once: a crash replays bars above the commit point,
        which is safe because every write on this path is idempotent (signals
        UPSERT, chain_steps ON CONFLICT DO NOTHING, trade_signals ON CONFLICT on
        the deterministic signal_id).
        """
        from producer.bar import OHLCPoint
        assert self.consumer is not None and self.router is not None

        self.workers = SymbolWorkerPool(
            prepare=self.router.prepare,
            forecast=self.router.forecast,
            apply=self.router.apply,
            max_queue_per_symbol=self.cfg.symbol_queue_max,
            max_inflight_per_symbol=self.cfg.symbol_inflight_max,
        )
        last_commit = time.monotonic()

        try:
            async for msg in self.consumer:
                if self._stop.is_set():
                    break
                raw = msg.value
                if isinstance(raw, dict) and raw.get("type") == "status":
                    # Not a bar: nothing to process, but its offset must still be
                    # retired or it would wedge the contiguous commit point.
                    self.offsets.complete(msg.offset)
                    continue
                try:
                    bar = OHLCPoint(**raw)
                except Exception:
                    log.exception("bad bar payload, skipping: %r", raw)
                    self.offsets.complete(msg.offset)   # poison bar: don't wedge
                    continue

                if not self.resolver.known(bar.symbol):
                    log.debug("bar for unknown symbol %s, skipping", bar.symbol)
                    self.offsets.complete(msg.offset)
                    continue

                self._observe_lag(bar)
                closes = self._live_closes.get(bar.asset_class)
                if closes is not None:
                    closes.observe(bar)
                # Hand off. This AWAITS only if the symbol's queue is full
                # (backpressure); otherwise it returns immediately and the loop
                # goes straight back to Kafka. That is the whole speedup.
                await self.workers.submit(bar.symbol, bar, msg.offset)

                # Retire whatever finished while we were fetching, and commit the
                # contiguous frontier on a timer (not per bar — that would put a
                # network round-trip back on the hot path we just cleared).
                for done in self.workers.take_done_offsets():
                    self.offsets.complete(done)
                now = time.monotonic()
                if now - last_commit >= self.cfg.commit_interval_s:
                    await self._commit_offsets()
                    last_commit = now
        finally:
            await self.stop()

    def _observe_lag(self, bar) -> None:
        """Gauge: how far behind wall clock is the bar we are only now processing?

        This is the number that would have made a 39-hour stall obvious on day
        one instead of after a day of watching. Export it (Prometheus/statsd) —
        alert if it exceeds a few bar intervals. Sudden growth here, or growth in
        SymbolWorkerPool.queue_depths(), is this bug recurring.

        Age is measured from the bar's CLOSE (label + one timeframe): measured
        from its open label, every 15m/1h bar read as "behind" on arrival."""
        self._last_bar_monotonic = time.monotonic()
        age_s = time.time() - (bar.timestamp + timeframe_to_ms(bar.timeframe)) / 1000.0
        if age_s > self.cfg.lag_warn_s:
            log.warning(
                "PROCESSING LAG: bar %s/%s closed %.0fs (%.1fh) ago — the pipeline is "
                "behind. queue depths=%s",
                bar.symbol, bar.timeframe, age_s, age_s / 3600.0,
                self.workers.queue_depths() if self.workers else {},
                extra=alert(f"lag:{bar.symbol}"),
            )

    def bars_flowing(self, within_s: float = 300.0) -> bool:
        """A bar was consumed in the last within_s (for the heartbeat; the
        crypto rungs produce one every minute, 24/7)."""
        return time.monotonic() - self._last_bar_monotonic < within_s

    async def _commit_offsets(self) -> None:
        """Commit the contiguous low-water mark, if it advanced. Single-partition
        topic (bars are produced unkeyed to trading-bars), so one commit point."""
        offset = self.offsets.take_committable()
        if offset is None:
            return
        try:
            # Kafka commits the NEXT offset to read, i.e. last_processed + 1.
            await self.consumer.commit({
                TopicPartition(self.cfg.bars_topic, 0): offset + 1
            })
            log.debug("committed offset %d (pending out-of-order=%d)",
                      offset + 1, self.offsets.pending())
        except Exception:
            # A failed commit is survivable: the offset stays uncommitted and the
            # next commit (or a restart's replay) covers it. Never fatal.
            log.exception("offset commit failed (will retry next interval)")

    async def stop(self) -> None:
        """Graceful shutdown. ORDER IS LOAD-BEARING — each step depends on the
        previous one still having its resources:

          1. Drain the workers   — finish bars already accepted, so a clean stop
                                   does not replay work it actually completed.
          2. Commit offsets      — record what step 1 finished. MUST precede the
                                   consumer close (a closed consumer cannot commit).
          3. Flush audit writer  — its stop() drains the queue to the DB, so a
                                   GRACEFUL stop incurs NO bounded-loss window
                                   (only a crash does). MUST precede pool close.
          4. Close Kafka + pool  — everything above needs them alive.
        """
        self._stop.set()

        # Final capacity report: the numbers that decide whether the system is
        # viable (single-call latency L, and the resulting 1/L ceiling).
        if self.forecast_gate is not None:
            self.forecast_gate.log_stats()

        # Stop serving BEFORE draining: a health check during shutdown should
        # fail to connect (unambiguously "going away") rather than return a 200
        # describing a pipeline that is already tearing down.
        if self._health_server is not None:
            self._health_server.stop()
        if self._audit_task is not None:
            self._audit_task.cancel()
        if self._prune_task is not None:
            self._prune_task.cancel()
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
        for t in getattr(self, "_strategy_tickers", []):
            t.cancel()

        if self.workers is not None:
            await self.workers.drain()
            for done in self.workers.take_done_offsets():
                self.offsets.complete(done)
            await self.workers.stop()

        if self.consumer is not None:
            await self._commit_offsets()      # before the consumer closes
            await self.consumer.stop()

        if self.producer is not None:
            await self.producer.stop()

        if self.audit_writer is not None:
            await self.audit_writer.stop()    # flushes; needs the pool alive

        for feed in getattr(self, "_closes_feeds", {}).values():
            close = getattr(feed, "aclose", None)
            if close is not None:
                try:
                    await close()
                except Exception:
                    pass

        if self.pool is not None:
            await self.pool.close()

        log.info("signal process stopped cleanly")


def _all_configured_symbols() -> list:
    """Union of all symbols across ASSET_CONFIGS — the tracked universe, used to
    expand a strategy's 'symbols: "*"' to every tracked symbol."""
    from config import ASSET_CONFIGS
    out = []
    for cfg in ASSET_CONFIGS.values():
        for s in cfg.get("symbols", []):
            if s not in out:
                out.append(s)
    return out


def _tagged_to_wire(tev: TaggedChainEvent) -> dict:
    """TaggedChainEvent -> JSON dict for Kafka. Carries the 'set' tag so a Kafka
    consumer can reconstruct it via TaggedChainEvent.from_raw."""
    return {
        "set": tev.set.value, "event_type": tev.event_type,
        "chain_id": tev.chain_id, "symbol": tev.symbol,
        "asset_class": tev.asset_class, "direction": tev.direction,
        "metadata": {"propagation_level": tev.propagation_level},
        "propagation_level": tev.propagation_level, "timestamp": tev.timestamp,
    }


async def amain() -> None:
    logging.basicConfig(
        # Configurable so DEBUG (the `forecast OK` heartbeat, and the audit
        # writer's flush lines) can be turned on without a code change. Without
        # this you cannot distinguish "forecasting fine, conditions never met"
        # from "not forecasting at all" — two very different problems.
        level=getattr(
            logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    install_alerting("signal")
    cfg = Config()
    proc = SignalProcess(cfg)

    loop = asyncio.get_running_loop()

    # --- Size the thread pool for I/O-bound work ---
    # asyncio.to_thread() uses the loop's DEFAULT executor, which Python sizes
    # min(32, cpu_count + 4) — that is 6 on a 2-vCPU box. Every Sumtyme forecast
    # runs in one of those threads, so the default silently caps forecast
    # concurrency at 6 REGARDLESS of FORECAST_CONCURRENCY. Measured directly:
    #
    #     N=1 -> 0.12 calls/s   N=4 -> 0.27 calls/s
    #     N=6 -> 0.36 calls/s   N=8 -> 0.24 calls/s   <-- COLLAPSES past the cap
    #
    # The N=8 collapse is not Sumtyme saturating; it is two calls queueing for a
    # thread while six run. The cpu_count+4 heuristic is tuned for CPU-BOUND work
    # and is simply the wrong model here: these threads spend their whole life
    # BLOCKED ON A SOCKET waiting for Sumtyme, burning no CPU. Sizing them against
    # vCPUs makes no sense — size them against how many calls we want in flight.
    #
    # Kept comfortably above FORECAST_CONCURRENCY so the ForecastGate (not the
    # executor) is what actually bounds concurrency. The gate is the intentional
    # limit; the pool should never be the thing silently deciding it.
    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=cfg.thread_pool_size,
        thread_name_prefix="forecast",
    )
    loop.set_default_executor(executor)
    log.info(
        "thread pool: %d workers (default would be %d = min(32, cpu_count+4); "
        "forecasts are I/O-bound, so this is sized to the desired in-flight "
        "count, not to vCPUs)",
        cfg.thread_pool_size, min(32, (os.cpu_count() or 1) + 4),
    )

    # Graceful shutdown on SIGTERM/SIGINT so in-flight bars finish and Kafka
    # offsets/pool close cleanly.
    for sig in (os_signal.SIGTERM, os_signal.SIGINT):
        loop.add_signal_handler(sig, lambda: proc._stop.set())

    try:
        await proc.start()
        proc._heartbeat_task = start_heartbeat("HEARTBEAT_URL_SIGNAL", proc.bars_flowing)
        await proc.run()
    finally:
        # Don't wait on threads stuck in a hung SDK call — the SDK exposes no
        # request timeout, so a wedged socket would hang shutdown forever.
        executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    asyncio.run(amain())