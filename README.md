# Trading Backend

A headless, multi-strategy trading system. It ingests market-data bars, runs
Sumtyme causal-chain state machines over a configurable timeframe ladder, applies
one or more chain strategies, and emits **trade signals** to Postgres. A separate
executor process consumes those signals and places venue orders.

The signal side and the execution side are deliberately separate processes with
the database as the contract between them. A crash on either side cannot corrupt
the other, and the executor's real-money blast radius stays isolated.

---

## 1. Architecture

```
   feeds (Polygon / Binance)
        │  closed bars
        ▼
   Kafka: trading-bars
        │
        ▼
 ┌─────────────────── SIGNAL PROCESS ───────────────────┐
 │  per-symbol ordered workers                          │
 │      ▼                                               │
 │  MultiLadderRouter                                   │
 │      │  one ServiceTwoChainMachine per (symbol,      │
 │      │  ladder) — Sumtyme forecast -> chain events   │
 │      ▼                                               │
 │  tagged chain events ──► Kafka: chain-events (audit) │
 │      │ (also in-process, under the per-symbol lock)  │
 │      ▼                                               │
 │  MultiStrategyRuntime                                │
 │    ├─ two_ladder  (Set A / Set B ladders)            │
 │    └─ four_chain  (FC ladder)                        │
 │      │  ENTER / EXIT decisions                       │
 │      ▼                                               │
 │  SignalWriter ──► trade_signals (DB)                 │
 └──────────────────────────────────────────────────────┘
                     │
                     │  pg_notify('trade_signal')  +  catch-up sweep
                     ▼
 ┌────────────────── EXECUTOR PROCESS ──────────────────┐
 │  SignalListener ──► SignalExecutor                   │
 │      │  sizing, caps, kill switch, trade mode gate   │
 │      ▼                                               │
 │  GatedAdapter ──► IBKR / Binance                     │
 │      │                                               │
 │      ▼  trades / orders / fills / exit_state         │
 └──────────────────────────────────────────────────────┘
```

**Why the split.** The producer and strategy share one process because they are
coupled by the per-symbol lock and the in-memory chain views; splitting them
would mean serializing views over a wire for no benefit. The executor is a
separate process because it touches real money.

---

## 2. Layers

| Layer | Turns… | …into | Key modules |
|---|---|---|---|
| Feeds | venue APIs | closed bars on Kafka | `feed_process.py`, `polygon_*.py`, `binance_*.py`, `bar_aggregator.py` |
| Warmup | history | seeded buffers + cursors | `warmup_process.py`, `warmup_bars` table |
| Producer | bars | tagged chain events | `service2_machine.py`, `fanout_router_multi.py`, `ladder_registry.py` |
| Strategy | chain events | `trade_signals` rows | `multi_runtime.py`, `two_ladder_strategy.py`, `four_chain_*.py`, `signal_writer.py` |
| Execution | trade signals | venue orders | `signal_listener.py`, `signal_executor.py`, `sizing.py`, `gated_adapter.py`, adapters |
| Schema | — | the trading DB | `migrations/001…015` |

Dependencies point downward only. The executor never imports the strategy; both
sides share only the DB contract and the event shape.

---

## 3. Strategies

Strategies are registered in `STRATEGIES` (config) and built by
`strategy/registry.py`. Each has an **entry** and an **exit** component; the
runtime fans every chain event to every strategy subscribed to that ladder, with
per-strategy exception isolation so one failing strategy cannot stall the others.

**`two_ladder`** — runs two parallel ladders per symbol. Set B chains are
"primes"; a Set A chain reaching 2m triggers an entry, provided a same-direction
prime exists and no opposing chains are active. Exits are OR'd: an opposing Set B
chain reaching 2m is a STOP; the prime reaching the top of its ladder is a TARGET.

**`four_chain`** — a start-ordered initiation + confirmation pattern on the FC
ladder (`1m,3m,5m,15m,1h`). Requires two same-direction chains reaching 15m, two
distinct opposing failures in the correct start windows, and a confirmer reaching
3m after both — plus an independent open same-direction chain. Exits on an
opposing chain reaching 5m.

Positions are keyed `position_key = (chain_id, side)` and scoped by strategy, so
two strategies can hold independent positions on the same chain.

---

## 4. The `trade_signals` contract

Append-only ENTER/EXIT rows — the boundary between the signal side and any
consumer.

| Column | Notes |
|---|---|
| `signal_id` | deterministic (`sig2` scheme); `ON CONFLICT DO NOTHING` |
| `position_key` | `{chain_id}:{L\|S}` |
| `strategy` | `two_ladder` \| `four_chain` — **required by consumers** |
| `action` | `ENTER` \| `EXIT` |
| `direction`, `symbol`, `asset_class` | |
| `trigger_level`, `decision_price` | the level and price the strategy saw |
| `exit_reason` | `STOP` \| `TARGET` on EXIT, NULL on ENTER (CHECK-enforced) |
| `decided_at` | the deciding event's timestamp, not wall clock |

**Idempotency.** `signal_id` hashes `(strategy, position_key, action, discriminator)`
where the discriminator carries `decided_at`. Because `decided_at` is bar-derived,
a Kafka replay of the same decision reproduces the same id and dedupes, while a
genuine re-entry at a later bar gets a distinct id.

**Delivery.** Migration 009 adds an `AFTER INSERT` trigger firing
`pg_notify('trade_signal', signal_id)`. `SignalListener` holds a dedicated LISTEN
connection and runs a cursor-based catch-up sweep on reconnect, so a dropped
connection doesn't lose signals.

---

## 5. Processes

| Process | Command | Role |
|---|---|---|
| `warmup` | `python warmup_process.py` | one-shot; seeds `warmup_bars` + cursors, then exits |
| `feed` | `python feed_process.py` | venue APIs → `trading-bars` |
| `signal` | `python signal_process.py` | bars → chains → strategies → `trade_signals` |
| `executor` | `python run_executor.py` | `trade_signals` → venue orders |

`docker-compose.yml` covers the signal side (Kafka + warmup + feed + signal).
Warmup runs to completion before feed and signal start. The executor runs
separately — one process per venue (`EXECUTOR_VENUE=ibkr|binance`).

---

## 6. Configuration

Environment (`.env`):

| Var | Required by | Purpose |
|---|---|---|
| `TRADING_DB_URL` | all | Postgres DSN; must bypass RLS (service role/owner) |
| `SUMTYME_API_KEY` | signal | forecast API |
| `POLYGON_API_KEY` | feed | equity/fx data |
| `KAFKA_BOOTSTRAP` | all | defaults `localhost:9092`; compose sets `kafka:9092` |
| `EXECUTOR_VENUE` | executor | `ibkr` (default) or `binance` |
| `FORECAST_CONCURRENCY` | signal | in-flight Sumtyme calls (capacity ceiling) |
| `SUMTYME_TIMEOUT_S`, `SYMBOL_QUEUE_MAX`, `LAG_WARN_S` | signal | backpressure + lag alarms |

In `config.py`: `ASSET_CONFIGS` (symbols and timeframes per asset class),
`STRATEGIES` (which strategies are enabled, on which symbols and ladders),
`EXTRA_LADDERS` (non-A/B ladders such as `FC`), `VENUE_ENDPOINTS`,
`TRADE_MODE_DEFAULT`.

---

## 7. Safety controls

- **Trade mode** (`trade_enabled`, migration 012): `off` → orders no-op,
  `paper` → paper endpoint, `live` → real. Changed by a DB row; the executor
  LISTENs and swaps adapters without a restart.
- **Kill switch**: `halt_new` stops opening while letting exits run; `flatten`
  also closes everything. A DB row — faster than a redeploy.
- **Execution caps** (`execution_caps`, 015): per asset class and intent,
  hot-reloadable.
- **Boot reconcile**: connect → reconcile DB against venue → unexplained
  discrepancies HALT rather than auto-resolve.
- **Idempotency everywhere**: `signal_id`, `client_order_id`, `venue_exec_id`,
  and a unique `(chain_id, side)` on trades. Replays dedupe at the DB.
- **RLS**: migrations 003/004 leave the trading tables deny-all with no policies.
  Services connect as a role that bypasses RLS; there is no browser client.

---

## 8. Setup

```bash
# 1. Schema — apply in order, as a role that bypasses RLS
for f in migrations/0*.sql; do psql "$TRADING_DB_URL" -v ON_ERROR_STOP=1 -f "$f"; done

# 2. Config
cp .env.example .env        # fill in DB URL, Sumtyme + Polygon keys

# 3. Signal side
docker compose up -d --build

# 4. Executor (separate; starts in whatever mode trade_enabled says)
python run_executor.py
```

Verify: `warmup_bars` and `feed_cursors` populate, then `signals` and `chains`,
then `trade_signals`. Those tables are the pipeline's stage gates — if
`trade_signals` is empty, whichever earlier table is empty localizes the stall.

---

## 9. Verification

Two probes drive synthetic chain events through the **real** gates, writer,
insert, NOTIFY and listener — nothing is hand-inserted:

```bash
python write_path_probe.py    --i-understand-live-listeners   # two_ladder
python write_path_probe_fc.py --i-understand-live-listeners   # four_chain
```

Each one checks preconditions (migration 011's `strategy` column, 009's trigger,
INSERT privilege via a rolled-back transaction), asserts the gates do **not** fire
early, verifies the row contents, confirms `pg_notify`, and confirms the listener
dispatched with the right strategy label. They clean up after themselves
(`--keep-rows` to inspect).

`pg_notify` fans out to **every** listener, so run with the executor stopped or in
paper mode — hence the required acknowledgement flag. The default probe symbols
(`PROBE-TEST`, `PROBE-FC`) are unroutable by design.

---

## 10. Operational notes

- **Capacity is the binding constraint.** Throughput ≈ `FORECAST_CONCURRENCY ÷
  Sumtyme latency`. Demand is `symbols × forecast-eligible rungs × bars/sec`.
  If demand exceeds capacity the worker queues fill, backpressure stalls the
  consume loop, and one oversubscribed symbol delays every other symbol's signals
  (head-of-line blocking). Watch the lag gauge; fix by cutting rungs or symbols,
  not by tuning the client.
- **Sub-minute rungs are expensive.** A 5s rung is 720 forecasts/hour/symbol —
  it can consume more capacity than every other rung combined.
- **Restart hydration matters.** Machines restore buffers, active chains, and the
  last baseline signal; the two-ladder view reseeds from `chains`. Without the
  seed, chains already at the top of a ladder are invisible (they only terminate,
  never propagate), so their exits never fire.
- **Executor cursor.** `run_executor.py` starts its listener at "now", so signals
  written while the executor is down are not acted on. Persist the cursor if you
  need at-least-once execution across restarts.
- **State-machine sync.** The chain machine is copied from CausaView, not shared
  as a library. Changes must be applied in both places.
