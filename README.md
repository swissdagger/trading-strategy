# Trading Backend

A headless trading system for **Coinbase Advanced Trade spot, long-only,
crypto-only** ([ADR-001](docs/adr/001-coinbase-spot-long-only.md)). It ingests
Coinbase spot candles, runs Sumtyme causal-chain state machines over a timeframe
ladder, applies a chain strategy, and writes **trade signals** to Postgres. A
separate executor consumes those signals and trades them — on a simulated paper
account against the real market, or live.

The signal side and the execution side are deliberately separate processes with
the database as the contract between them. A crash on either side cannot corrupt
the other, and the executor's real-money blast radius stays isolated.

Operating it: [RUNBOOK.md](RUNBOOK.md).

---

## 1. Architecture

```
   Coinbase public candles (REST)        Polygon stocks (ws 1s aggs, REST fallback)
        │  closed, settled bars            │  12-rung dense ladder, 30s…6h
        │  (1m…1h; 3m built from 1m)       │  (seconds-rungs from trades, never /v2/aggs)
        ▼                                  ▼
   feed process ──► Kafka: trading-bars
        │
        ▼
 ┌─────────────────── SIGNAL PROCESS ───────────────────┐
 │  per-symbol ordered pipelines                        │
 │      ▼  prepare (lock) → forecast (no lock, ∥)       │
 │  MultiLadderRouter       → apply (lock, in order)    │
 │      │  one ServiceTwoChainMachine per (symbol,      │
 │      │  ladder) — Sumtyme forecast -> chain events   │
 │      ▼                                               │
 │  tagged chain events ──► Kafka: chain-events (audit) │
 │      │ (also in-process, under the per-symbol lock)  │
 │      ▼                                               │
 │  MultiStrategyRuntime ── confirmed_15m (FC ladder)   │
 │      │  ENTER / EXIT decisions                       │
 │      ▼                                               │
 │  SignalWriter ──► trade_signals (DB)                 │
 └──────────────────────────────────────────────────────┘
                     │
                     │  pg_notify('trade_signal') + catch-up from a persisted cursor
                     ▼
 ┌────────────────── EXECUTOR PROCESS ──────────────────┐
 │  SignalListener ──► SignalExecutor                   │
 │      │  stale-ENTER gate, side gate, kill switch,    │
 │      │  position cap, sizing, caps, escalation       │
 │      ├──► SHORT ──► shadow book (virtual fills)      │
 │      ▼                                               │
 │  GatedAdapter (trade mode)                           │
 │      ├─ off / paper ─► PaperCoinbaseAdapter          │
 │      │                  real reads, simulated fills  │
 │      └─ live ────────► CoinbaseAdapter (real orders) │
 │      ▼  trades / orders / fills / exit_state         │
 └──────────────────────────────────────────────────────┘
```

**Why the split.** The producer and strategy share one process because they are
coupled by the per-symbol lock and the in-memory chain views. The executor is a
separate process because it touches real money.

**Long-only.** The account is UK retail spot: no shorting. SHORT signals are
still produced; the executor never sends them to the venue and records them in
the **shadow book** (`shadow_trades`) as virtual round trips at the touch, net of
fees — the only short-side data available.

---

## 2. Layers

| Layer | Turns… | …into | Key modules |
|---|---|---|---|
| Feed | Coinbase candles | closed bars on Kafka | `feed_process.py`, `feeds/coinbase_spot.py`, `feeds/resample.py` |
| Warmup | history | seeded buffers + cursors | `warmup_process.py`, `warmup_bars` table |
| Producer | bars | tagged chain events | `service2_machine.py`, `fanout_router_multi.py`, `ladder_registry.py` |
| Strategy | chain events | `trade_signals` rows | `multi_runtime.py`, `confirmed_15m_*.py`, `signal_writer.py` |
| Execution | trade signals | venue orders | `signal_listener.py`, `signal_executor.py`, `sizing.py`, `shadow_book.py`, `gated_adapter.py`, `coinbase_adapter.py`, `paper_adapter.py` |
| Schema | — | the trading DB | `migrations/001…024` |

Dependencies point downward only. The executor never imports the strategy; both
sides share only the DB contract and the event shape.

---

## 3. Strategies

Strategies are registered in `STRATEGIES` (config) and built by
`strategy/registry.py`. Each has an **entry** and an **exit** component; the
runtime fans every chain event to every strategy subscribed to that ladder, with
per-strategy exception isolation.

**`confirmed_15m`** (enabled; formerly `four_chain`) — on the FC ladder
(`1m,3m,5m,15m,1h`). Enters when a chain propagates to 15m, provided an earlier
same-direction chain reached 15m first with an opposing 1m failure starting
between the two, and a finished same-direction chain that started earlier
reached 1h. Exits with TARGET when the entry chain reaches 1h, or STOP when an
open opposing chain reaches 5m. Full rules and open questions:
[`docs/strategies/confirmed_15m.md`](docs/strategies/confirmed_15m.md).

**The chain rules** (`strategy/rules.py`, the owner's frozen package, run verbatim
through `strategy/chain_rules.py`; each is its own pre-registered 200-trade
review and trades are never pooled across rules or ladders):

| Strategy | Asset | Ladder | Entry → confirm | Stall | Filters | Status |
|---|---|---|---|---|---|---|
| `rule1_v1` | equity | ED `1m,3m,5m,15m,30m,1h,2h,4h` | P2 = 3m → L3 = 5m | 60 min | vol_ratio ≤ 1.46; US regular session | enabled |
| `rule1_new_literal_v1` | equity | DL (30s base) | L2 = 45s → L3 = 1m | 60 min | same | enabled |
| `rule3_v1` | crypto | FC `1m,3m,5m,15m,1h` | P2 = 3m → L3 = 5m | 30 min | vol_ratio > 0.71, not squeeze-release; 24/7 | enabled |
| `rule3_new_literal_v1` | crypto | CN `30s,45s,1m,2m,3m,5m,15m,30m,1h` | L2 = 45s → L3 = 1m | 30 min | same | enabled, BTC-USD only |
| `rule3_new_equiv_v1` | crypto | CN | 3m → 5m (pinned by rung) | 30 min | same | enabled, BTC-USD only |

All are **shadow-only** (`SHADOW_ONLY_STRATEGIES`): the executor never sends
their signals to a venue in any trade mode; they fill in the shadow book at
`decision_price` with `SHADOW_FEE_BPS_BY_CLASS` per leg. The pre-registered
review is `docs/rule_review.sql`, at the assumed fee and at the spread measured
by the research process. Volatility filters read 1m closes from the feeds
(Polygon for equities, Coinbase for crypto; `strategy/vol_source.py`).

**`chain_recorder`** (enabled) — owns the dense equity ladder **DL**
(`30s,45s,1m,90s,2m,3m,15m,30m,1h,2h,4h,6h`) on the Polygon equities so its
chains and signals are computed and persisted; emits no trade signals. It is
the placeholder owner until the DL strategy lands (a config swap). The
executor routes any signal for an asset class its venue does not trade to the
shadow book, so an equity strategy on DL gets virtual round trips at
`decision_price` without a venue.

**Disabled, kept in the tree** (ADR-001 debt D1/D2): `two_ladder` (sub-minute
A/B ladders; no crypto source) and `prior_failure_scaleout` (scale-out exits;
expected to be replaced).

Positions are keyed `position_key = (chain_id, side)` and scoped by strategy, so
two strategies can hold independent positions on the same chain.

---

## 4. The `trade_signals` contract

Append-only ENTER/EXIT rows — the boundary between the signal side and any
consumer.

| Column | Notes |
|---|---|
| `signal_id` | deterministic; `ON CONFLICT DO NOTHING` |
| `position_key` | `{chain_id}:{L\|S}` |
| `strategy` | e.g. `confirmed_15m` — **required by consumers** |
| `action` | `ENTER` \| `EXIT` |
| `direction`, `symbol`, `asset_class` | symbol is the Coinbase product id (`BTC-USD`) |
| `trigger_level`, `decision_price` | the level and price the strategy saw |
| `exit_reason` | `STOP` \| `TARGET` (\| `SCALE` \| `TERMINAL`) on EXIT, NULL on ENTER |
| `decided_at` | the deciding bar's timestamp (market time, window start) |
| `created_at` | when the row was written (wall time) |

**Idempotency.** `signal_id` hashes `(strategy, position_key, action, discriminator)`,
so a replay of the same decision dedupes while a genuine re-entry gets a distinct id.

**Delivery.** Migration 009 fires `pg_notify('trade_signal', signal_id)` on
insert. `SignalListener` holds a dedicated LISTEN connection and, on every
(re)connect, sweeps from its cursor, which the executor persists
(`executor_cursor`), so signals written while it was down are replayed. Replayed
ENTERs older than `ENTER_MAX_AGE_SECONDS` (by `created_at`) are skipped; EXITs
are always honoured.

---

## 5. Processes

| Process | Command | Role |
|---|---|---|
| `warmup` | `python warmup_process.py` | one-shot; seeds `warmup_bars` + feed cursors, then exits |
| `feed` | `python feed_process.py` | Coinbase candles → `trading-bars` |
| `signal` | `python signal_process.py` | bars → chains → strategies → `trade_signals` |
| `executor` | `python run_executor.py` | `trade_signals` → paper or live orders |
| `research` | `python research_process.py` | `trade_signals` + Polygon → `research_*` tables; off the live path |

`docker-compose.yml` runs all of them (plus Kafka). The research process is
deliberately separate: it reads `trade_signals` via `pg_notify` and its own
cursor (the executor's mechanism) and Polygon, writes only `research_quotes`
(NBBO as of each equity signal), `research_bars_1m` (1-minute bars with volume,
VWAP and trade count for the equities plus SPY, QQQ, VIXY) and
`research_earnings` (CSV import; provider pluggable), and shares no lock,
queue, connection or thread with the feed, signal or executor processes.
`--backfill` captures quotes for signals written before it ran; `--verify`
checks the key's quote entitlement. Warmup runs to completion
before feed and signal start; the executor needs only the database.

---

## 6. Configuration

Environment (`.env`; every variable is listed in `.env.example`):

| Var | Required by | Purpose |
|---|---|---|
| `TRADING_DB_URL` | all | Postgres DSN; must bypass RLS (service role/owner) |
| `SUMTYME_API_KEY` | signal | forecast API |
| `POLYGON_API_KEY`, `POLYGON_WS_HOST` | feed, warmup, signal | equities (dense ladder; rule1's vol filter reads 1m closes); the ws host is the real-time entitlement host |
| `KAFKA_BOOTSTRAP` | feed, signal | defaults `localhost:9092`; compose sets `kafka:9092` |
| `COINBASE_API_KEY_NAME`, `COINBASE_API_PRIVATE_KEY`, `COINBASE_PORTFOLIO_UUID` | executor | ECDSA CDP key scoped to the bot portfolio; View + Trade, no Transfer |
| `FORECAST_CONCURRENCY` | signal | in-flight Sumtyme calls (capacity ceiling) |
| `SUMTYME_TIMEOUT_S`, `SYMBOL_QUEUE_MAX`, `SYMBOL_INFLIGHT_MAX`, `LAG_WARN_S` | signal | backpressure, per-symbol forecast fairness, lag alarms |

In `config.py`: `CRYPTO_BASES` × `QUOTE_CURRENCY` (the symbols), `ASSET_CONFIGS`
(timeframes), `STRATEGIES`, `EXTRA_LADDERS`, `VENUE_ENDPOINTS`, and the executor's
risk settings: `MAX_OPEN_POSITIONS`, `ENTER_MAX_AGE_SECONDS`,
`ORPHAN_BALANCE_ALLOWLIST`, `PAPER_STARTING_CASH`, `SHADOW_TAKER_FEE_BPS`.

---

## 7. Safety controls

- **Trade mode** (`trade_enabled.mode`, migrations 012/023; a fresh DB starts in
  `paper`): `off` → no orders; `paper` → simulated fills against the real market;
  `live` → real orders. off↔paper takes effect immediately; a paper↔live change
  blocks every write and restarts the executor so it reconciles against the new
  venue.
- **Kill switch** (`kill_switch`): blocks new entries. `halt_new` lets open
  positions run their normal exits; `flatten` also cancels working entries and
  closes every open position within a second (reason `KILL_SWITCH`, through the
  normal exit path and its escalation ladder).
- **Key checks at connect**: the key must be bound to the bot portfolio, must not
  be able to transfer funds, and (live) must be able to trade.
- **Boot reconcile**: venue positions vs. DB trades, **netted per symbol**;
  mismatches HALT the symbol's trades, and unknown non-dust balances refuse boot.
- **Position cap and sizing**: `MAX_OPEN_POSITIONS`; 5% of *available* balance per
  entry; venue minimums enforced before submit.
- **Execution caps** (`execution_caps`): marketable-limit caps per class and
  intent, with an exit escalation ladder that HALTs at the top. Hot-reloadable.
- **HALTs** are logged at ERROR and stay visible in `trades` (`state = 'HALTED'`,
  `halt_reason`); a refused boot is logged and retried every few minutes. There is
  no alerting.
- **Idempotency everywhere**: `signal_id`, `client_order_id` (deterministic `cv1`;
  Coinbase returns the existing order for a repeated id), `venue_exec_id`, and a
  unique `(chain_id, side)` on trades.
- **RLS**: the trading tables are deny-all with no policies. Services connect as a
  role that bypasses RLS; there is no browser client.

---

## 8. Setup

```bash
# 1. Schema — a FRESH database, all migrations in order, as a role that bypasses RLS
for f in migrations/0*.sql; do psql "$TRADING_DB_URL" -v ON_ERROR_STOP=1 -f "$f"; done

# 2. Config
cp .env.example .env        # DB URL, Sumtyme key, Coinbase key (see RUNBOOK §7b)

# 3. Check the Coinbase key read-only (no orders)
python verify_coinbase_readonly.py

# 4. Everything (warmup -> feed + signal; executor in paper mode)
docker compose up -d --build
```

Verify: `warmup_bars` and `feed_cursors` populate, then `signals` and `chains`,
then `trade_signals`, then `trades` / `paper_fills` (and `shadow_trades` for
shorts). Those tables are the pipeline's stage gates — whichever earlier table is
empty localizes a stall.

---

## 9. Verification

Two probes drive synthetic chain events through the **real** gates, writer,
insert, NOTIFY and listener — nothing is hand-inserted:

```bash
python write_path_probe.py    --i-understand-live-listeners   # two_ladder
python write_path_probe_fc.py --i-understand-live-listeners   # confirmed_15m
```

`pg_notify` fans out to **every** listener, so run them with the executor stopped
or in `off` — hence the required acknowledgement flag. The probe symbols are
unroutable by design.

---

## 10. Operational notes

- **Capacity is the binding constraint.** Throughput ≈ `FORECAST_CONCURRENCY ÷
  Sumtyme latency`; demand is `symbols × forecast-eligible rungs × bars/sec`. If
  demand exceeds capacity, worker queues fill and one oversubscribed symbol delays
  every other symbol's signals. Fix by cutting rungs or symbols (ADR-001 debt D12).
  A single symbol is no longer limited to one forecast at a time: the chain
  machine is split into prepare / forecast / apply, forecasts for one symbol
  overlap (up to `SYMBOL_INFLIGHT_MAX`), and bars are still *applied* strictly
  in arrival order (`producer/symbol_workers.py`).
- **Crypto seconds rungs.** The CN ladder's 30s/45s bars are built from
  Coinbase Exchange trades for `subminute_symbols` only (BTC-USD): a
  backward trade walk at warmup, a 15 s forward poll live, one source for
  both. ~5,570 extra forecasts/symbol/day; one symbol fits the measured
  capacity beside the ten-symbol FC ladder, a second needs the D12
  re-benchmark.
- **Equities capacity.** The DL ladder needs ~5,700 forecasts/symbol/day over
  the 04:00–20:00 ET session (~0.1/s while open): about three symbols at the
  measured 0.36 calls/s peak. The 30s/45s/90s rungs are built from trades
  (history: session walk; REST live: forward poll; ws: 1s aggregates) because
  Polygon's `/v2/aggs` second aggregates are T+1 stale. 2m/3m are built from
  1-minute bars locally. If the websocket cannot connect on a cold start the
  feed falls back to REST for the process lifetime (logged at ERROR).
- **Bar latency.** Coinbase revises a closed candle for up to ~36 s, so the feed
  emits a bar 60 s after its window closes. Thin books omit no-trade minutes; the
  gaps pass through.
- **Restart hydration.** Machines restore buffers, active chains and the last
  baseline signal, and drop any bar not newer than their buffer (Kafka replays).
- **State-machine sync.** The chain machine is copied from CausaView, not shared
  as a library, and differs by two changes: the monotonic bar guard, and the
  prepare/forecast/apply split of `on_bar` (ADR-001 debt D9). The
  INIT/PROPAGATE/TERMINATE sweep (`_run_state_machine`) is verbatim; apply
  changes to it in both places.
  in both places.
