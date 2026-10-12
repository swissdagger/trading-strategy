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
   Coinbase public candles (REST)
        │  closed, settled bars (1m…1h; 3m built from 1m)
        ▼
   feed process ──► Kafka: trading-bars
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

`docker-compose.yml` runs all of them (plus Kafka). Warmup runs to completion
before feed and signal start; the executor needs only the database.

---

## 6. Configuration

Environment (`.env`; every variable is listed in `.env.example`):

| Var | Required by | Purpose |
|---|---|---|
| `TRADING_DB_URL` | all | Postgres DSN; must bypass RLS (service role/owner) |
| `SUMTYME_API_KEY` | signal | forecast API |
| `KAFKA_BOOTSTRAP` | feed, signal | defaults `localhost:9092`; compose sets `kafka:9092` |
| `COINBASE_API_KEY_NAME`, `COINBASE_API_PRIVATE_KEY`, `COINBASE_PORTFOLIO_UUID` | executor | ECDSA CDP key scoped to the bot portfolio; View + Trade, no Transfer |
| `FORECAST_CONCURRENCY` | signal | in-flight Sumtyme calls (capacity ceiling) |
| `SUMTYME_TIMEOUT_S`, `SYMBOL_QUEUE_MAX`, `LAG_WARN_S` | signal | backpressure + lag alarms |

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
- **Bar latency.** Coinbase revises a closed candle for up to ~36 s, so the feed
  emits a bar 60 s after its window closes. Thin books omit no-trade minutes; the
  gaps pass through.
- **Restart hydration.** Machines restore buffers, active chains and the last
  baseline signal, and drop any bar not newer than their buffer (Kafka replays).
- **State-machine sync.** The chain machine is copied from CausaView, not shared
  as a library, and differs by one change (the monotonic bar guard). Apply changes
  in both places.
