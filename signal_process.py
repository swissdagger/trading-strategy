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
import logging
import os
import signal as os_signal
from typing import Optional

import asyncpg

from execution.signal_writer import SignalWriter, SignalWriterHooks
from producer.fanout_router_multi import MultiLadderRouter
from producer.persistence import (
    make_insert_signal,
    make_load_active_chains,
    make_load_chain_view_seed,
    make_load_open_positions,
    make_persist_chain_init,
    make_persist_chain_propagate,
    make_persist_chain_terminate,
    make_persist_signal,
    make_load_buffers,
)
from producer.service2_machine import ServiceTwoChainMachine
from strategy.chain_view import ChainView, TaggedChainEvent
from strategy.ladder_registry import default_registry
from strategy.registry import StrategyRegistry, Strategy
from strategy.multi_runtime import MultiStrategyRuntime, RuntimeHooks, OpenPosition
from strategy.two_ladder_strategy import TwoLadderEntry, TwoLadderExit
from strategy.four_chain_adapter import make_four_chain

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
        client = sumtyme.client(apikey=self.cfg.sumtyme_key)

        # --- Kafka consumer (bars in) + producer (tagged events out) ---
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
        import json as _json
        self.consumer = AIOKafkaConsumer(
            self.cfg.bars_topic,
            bootstrap_servers=self.cfg.kafka_bootstrap,
            group_id=self.cfg.consumer_group,
            value_deserializer=lambda b: _json.loads(b.decode()),
            enable_auto_commit=True,
            auto_offset_reset="latest",
        )
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.cfg.kafka_bootstrap,
            value_serializer=lambda v: _json.dumps(v).encode(),
        )
        await self.consumer.start()
        await self.producer.start()

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
            "four_chain",
            lambda *, name, symbols, ladders, params: make_four_chain(
                name=name, symbols=symbols, ladders=ladders, params=params,
                view_registry=self.ladders, writer_factory=make_writer,
            ),
        )
        # All tracked symbols across the enabled strategies feed 'symbols: "*"'.
        all_symbols = _all_configured_symbols()
        self.strategies.build_from_config(STRATEGIES, all_symbols=all_symbols)

        # --- Multi-ladder router: one machine per (symbol, ladder) across the
        # union of all enabled strategies' ladders (compute-once) ---
        self.router = MultiLadderRouter(
            registry=self.ladders,
            make_machine=self._make_factory(client),
            publish=self._publish,
        )
        # --- Multi-strategy runtime, sharing the router's lock map ---
        self.runtime = MultiStrategyRuntime(
            view=view,
            registry=self.strategies,
            hooks=self._runtime_hooks(),
            locks=self.router._locks,   # SHARE the router's lock map
        )
        await self.runtime.start()
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
                forecast_fn=client.forecast_path,
                prepare_df_fn=prepare_dataframe_for_sumtyme,
                to_interval_fn=to_sumtyme_interval,
                build_df_fn=build_sumtyme_df,
                persist_signal=make_persist_signal(self.pool),
                persist_chain_init=make_persist_chain_init(self.pool),
                persist_chain_propagate=make_persist_chain_propagate(self.pool),
                persist_chain_terminate=make_persist_chain_terminate(self.pool),
                load_active_chains=make_load_active_chains(
                    self.pool, symbol, asset_class, timeframes),
                load_buffers=make_load_buffers(self.pool, symbol, timeframes),
            )
        return factory

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
        """Consume bars and pump them through the router until stopped.

        Bars are awaited SEQUENTIALLY (not spawned as concurrent tasks). This is
        load-bearing, not an oversight: MultiLadderRouter.on_bar takes the
        per-symbol lock, and MultiStrategyRuntime shares that same lock map — the
        publish sink (_publish, below) calls runtime._process_raw_locked() on the
        assumption that the caller (the router, mid on_bar) already holds the
        symbol's lock. If bars were dispatched concurrently, two bars for the
        SAME symbol could run at once: their on_bar calls would contend for the
        lock while a third could read/mutate the ChainView out of order, and the
        timestamp-ordered propagation logic in the chain machine
        (next_sig["timestamp"] >= chain["last_timestamp"]) would no longer see
        bars in the order Kafka delivered them. That combination silently breaks
        chain propagation and the entry/exit decision logic without raising — the
        chains table still fills (it's lock-independent), but trade_signals does
        not. If cross-symbol throughput is needed, the correct shape is
        per-symbol ordered workers (one queue/task per symbol), not a blanket
        create_task per bar — ask before reintroducing concurrency here.
        """
        from producer.bar import OHLCPoint
        assert self.consumer is not None and self.router is not None

        try:
            async for msg in self.consumer:
                if self._stop.is_set():
                    break
                raw = msg.value
                if isinstance(raw, dict) and raw.get("type") == "status":
                    continue
                try:
                    bar = OHLCPoint(**raw)
                except Exception:
                    log.exception("bad bar payload, skipping: %r", raw)
                    continue

                if not self.resolver.known(bar.symbol):
                    log.debug("bar for unknown symbol %s, skipping", bar.symbol)
                    continue

                await self.router.on_bar(bar)
        finally:
            await self.stop()

    async def stop(self) -> None:
        self._stop.set()
        if self.consumer is not None:
            await self.consumer.stop()
        if self.producer is not None:
            await self.producer.stop()
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
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    cfg = Config()
    proc = SignalProcess(cfg)

    # Graceful shutdown on SIGTERM/SIGINT so in-flight bars finish and Kafka
    # offsets/pool close cleanly.
    loop = asyncio.get_running_loop()
    for sig in (os_signal.SIGTERM, os_signal.SIGINT):
        loop.add_signal_handler(sig, lambda: proc._stop.set())

    await proc.start()
    await proc.run()


if __name__ == "__main__":
    asyncio.run(amain())