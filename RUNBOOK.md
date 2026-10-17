# Trading System — Operator Runbook

How to deploy, run, watch and take live the Coinbase Advanced Trade spot,
long-only system ([ADR-001](docs/adr/001-coinbase-spot-long-only.md)). The
architecture, the layers and the `trade_signals` contract are in
[README.md](README.md); this document is what an operator does.

---

## 1. Deployment

One server runs everything under `docker compose`:

| Service | Restart | Role |
|---|---|---|
| `kafka` | always | single broker; `trading-bars` and `chain-events` topics |
| `warmup` | once | seeds `warmup_bars` + `feed_cursors`, then exits 0 |
| `feed` | unless-stopped | Coinbase public candles → `trading-bars` |
| `signal` | unless-stopped | bars → Sumtyme chains → `confirmed_15m` → `trade_signals` |
| `executor` | unless-stopped | `trade_signals` → paper or live orders |

`feed`, `signal` and `executor` start only after `warmup` has completed.

**Database.** A dedicated Postgres (Supabase project or plain Postgres), not
shared with CausaView. Every service connects as a role that bypasses RLS (the
service role or the owner); the trading tables are deny-all with no policies and
there is no browser client.

**Network.** Outbound HTTPS to `api.coinbase.com` (feed, warmup, executor), `api.exchange.coinbase.com` (feed, warmup: the trades source for the CN ladder's 30s/45s rungs) and to
the Sumtyme API (signal). Nothing else.

**First deployment, on a fresh database:**

```bash
for f in migrations/0*.sql; do psql "$TRADING_DB_URL" -v ON_ERROR_STOP=1 -f "$f"; done
cp .env.example .env              # fill in: DB URL, Sumtyme key, Coinbase key (§3)
python verify_coinbase_readonly.py    # must end with OK; places nothing
python verify_polygon_feed.py         # equities: must end with OK; reads only (§3b)
python verify_crypto_subminute.py     # CN ladder trades source: must end with OK; reads only (§3d)
docker compose up -d --build
```

Migration 023 puts a fresh database in **paper** mode, so the first start
trades the simulated account only. It never overrides a mode an operator has set.

**Upgrades:** apply any new migration first, then `docker compose up -d --build`.
The executor reconciles at every boot, so a restart is always safe.

---

## 2. Signal series provenance

The bars every chain is computed from are **Coinbase spot candles** (public
endpoint `/api/v3/brokerage/market/products/{product_id}/candles`). What an
operator needs to know:

- **Not comparable with the old Binance series.** Sumtyme is deterministic on the
  exact series it is given, and spot candles differ from perp klines bar by bar.
  Chain ids, levels and timings differ, so the research population has to be
  rebuilt on this series (ADR-001 debt D5).
- **Symbols are Coinbase product ids** (`BTC-USD`) = `config.CRYPTO_BASES` ×
  `config.QUOTE_CURRENCY`. Changing the quote currency changes every symbol and
  needs a fresh database.
- **Derived rungs.** 3m (and 4h/12h for the disabled PF ladder) are aggregated
  from 1m (1h/6h). They are not Coinbase granularities.
- **Gaps.** Coinbase returns no candle for an interval with no trades. The feed
  passes the gaps through and never fabricates flat bars. Thin books gap a lot
  at 1m (AAVE-USD: ~17% of minutes, mostly 21:00–01:00 UTC). Sumtyme runs in step
  mode, so a gap is one step, not a run of missing bars.
- **Latency.** Coinbase revises a closed candle for up to ~36 s (measured), so the
  feed emits each bar 60 s after its window closes (`SETTLE_MS`). A bar reaches
  the signal process ~60–90 s after its window ends. That is why
  `ENTER_MAX_AGE_SECONDS` is 2 minutes.
- **Outages.** On restart the feed catches up from its cursor, up to 100 h of 1m
  bars. **After a longer outage, re-run warmup** (`docker compose run --rm warmup`)
  before starting feed and signal.
- **Warmup check.** Every (symbol, timeframe) logs `seeded ... with N bars`, with
  N near 5000. A short count means the product's history is shorter than one
  Sumtyme window: that rung warms partially and fills from live bars.

---

## 3. Coinbase key and portfolio

1. In Coinbase, create a **dedicated portfolio** for the bot and move only the
   trading capital into it. Hold nothing else there.
2. In the Coinbase Developer Platform, create an API key **on that portfolio**:
   - signature algorithm **ECDSA** (this API rejects Ed25519);
   - permissions **View** and **Trade**;
   - **no Transfer**.
3. Put the key name, the PEM private key (one line, with `\n` escapes) and the
   portfolio UUID in `.env`.
4. Run `python verify_coinbase_readonly.py`. It prints:
   - the key's permissions and the fee tier;
   - equity and balances;
   - each product's tradability and limits;
   - open orders and recent fills.

   It makes no writes. It must end with `OK`.

### 3b. Polygon (equities, the DL ladder)

1. Put `POLYGON_API_KEY` in `.env`. `POLYGON_WS_HOST` defaults to
   `business.polygon.io`, the real-time entitlement host; the generic
   `socket.polygon.io` denies real-time for this account.
2. Run `python verify_polygon_feed.py` (optionally with tickers as arguments,
   or `--no-ws`). For each DL rung it fetches a small sample through the same
   source rule warmup will use and reports bars, span, **alignment** and
   spacing, then authenticates the websocket on the configured host. It makes
   no writes and touches neither Kafka nor the database. It must end with `OK`.
   - `FAIL … off the grid` on 15m/30m/hour rungs is ADR-001 debt D16: add that
     rung to `_PolygonAggsFeedBase.DERIVE_FROM_MINUTE` and re-run.
   - `FAIL ws: could not connect` means the feed process will fall back to
     REST polling at startup (logged at ERROR, once). Sub-minute rungs are then
     served by polling trades every ~15 s; the data is correct, only later.
   - Warnings about short samples on 4h/6h are expected (coarse rungs fill live).
3. The warmup for a seconds-rung walks back across sessions reading raw
   trades; a liquid name fills 30s/45s/90s from one or two sessions. Check
   `warmup_bars` per rung after the first run as for crypto.

### 3d. Crypto seconds rungs (the CN ladder)

`ASSET_CONFIGS["crypto"]["subminute_symbols"]` (BTC-USD) is the only symbol
whose 30s/45s bars are built, from the public Exchange trades endpoint
(`api.exchange.coinbase.com`; no key). `python verify_crypto_subminute.py`
reads a few pages, builds bars with the feed's code and reports alignment,
the trade rate and the page budget a 5000-bar warmup needs. Warmup walks
trades backward (bounded by `TRADES_MAX_PAGES`); a thin book gets a partial
buffer that fills live. Live is a 15 s poll of the same endpoint with a 2 s
settle. Adding a second symbol here is a capacity decision (ADR-001 D12),
not a config one.

### 3c. Research collection (optional, off the live path)

`research_process.py` needs only `TRADING_DB_URL` and `POLYGON_API_KEY`.
`python research_process.py --verify` reads one historical NBBO and one bars
window with the key (no writes); a `403` on quotes means the plan lacks
historical quotes and `research_quotes` will fill with `status = 'error'`
until it does (re-run `--backfill` afterwards). Earnings dates:
`python research_process.py --earnings-csv dates.csv` with
`symbol,report_date[,time_of_day]` (`bmo`/`amc`). Review queries: spread paid
per trade is `research_quotes` joined to `trade_signals` on `signal_id`.

`connect()` checks the same things at every executor boot. It refuses a key that:
- is not bound to the configured portfolio;
- can transfer funds;
- cannot trade (live mode only).

Paper mode needs the key too, because it reads the real touch, product rules and
fee tier.

---

## 4. Trade modes

`trade_enabled.mode` is the single switch. The executor LISTENs on it.

| Mode | Adapter | Orders |
|---|---|---|
| `off` | paper | none; signals are consumed and ENTERs refused at the gate |
| `paper` | `PaperCoinbaseAdapter` | simulated against the real book, in the paper ledger (`paper_*` tables) |
| `live` | `CoinbaseAdapter` | **real** |

```sql
UPDATE trade_enabled SET mode = 'paper', reason = 'operator: <why>';
```

- **off ↔ paper** takes effect immediately and keeps the same adapter.
- **paper ↔ live** changes the venue, which is never hot-swapped. On the
  notification the executor:
  1. blocks every write on the old adapter;
  2. stops;
  3. exits with code **3**.

  Compose restarts it, and boot reconciles against the new venue. The last log
  line before the restart is `executor stopped for a venue change`.

**The trade tables are shared by paper and live.** `trades`, `orders` and `fills`
carry no mode column. Two consequences:

- Drain paper positions before going live (§8 step 6). Otherwise boot finds DB
  trades with no matching Coinbase balance and HALTs them.
- Filter reviews by time: note the go-live timestamp.

**Paper fill model** (pessimistic, deliberately):
- A limit that crosses the touch fills **in full at its limit price**, not at the
  touch.
- A non-crossing limit rests and is re-checked against the touch on every poll.
- The fee is the account's **real taker tier** (`/transaction_summary`), refreshed
  every 6 h.
- No queue position, no partial fills, no market impact.

Paper results are therefore an upper bound on slippage cost, but say nothing
about fill rates in thin books. Judge those only from small live trades.

---

## 5. Monitoring

### Stage gates

Each table fills from the one before it. The first empty one is where the
pipeline stalls.

```
warmup_bars (advancing: the bar store) → signals → chains → trade_signals
    → trades / orders / fills (+ paper_fills in paper) and shadow_trades (shorts)
```

```sql
SELECT symbol, timeframe, to_timestamp(cursor_ms / 1000) FROM feed_cursors ORDER BY 1, 2;
SELECT max(timestamp) FROM signals;                       -- a few minutes old at most
SELECT action, count(*), max(created_at) FROM trade_signals GROUP BY 1;
SELECT state, count(*) FROM trades GROUP BY 1;
```

### Logs worth watching

| Process | Line | Meaning |
|---|---|---|
| signal | `PROCESSING LAG` | bars are older than `LAG_WARN_S`: Sumtyme capacity is short of demand (§7) |
| signal | `forecast TIMEOUT` | a bar was skipped. A burst means Sumtyme is down, degraded or over-subscribed |
| executor | `stale ENTER ... skipped` | a replayed or late ENTER was older than `ENTER_MAX_AGE_SECONDS` |
| executor | `persist_fill: ... no matching order` (ERROR) | a fill for a known trade could not be attributed. Investigate before the next boot |
| executor | `HALTED` (ERROR) | a trade needs a human (§6.2) |
| executor | `kill switch FLATTEN` | flatten is closing a position (§6.1) |
| executor | `cannot start` / `retrying in` | boot refused (§6.3) |

### Alerts (Slack)

Each process posts to Slack when `SLACK_WEBHOOK_URL` is set (a Slack
incoming-webhook URL; `.env`). An alert is a log line tagged for Slack, so the
text in Slack is the line in the log. Posting runs on its own thread and never
blocks trading; repeats of the same condition are limited to one per 10 min.

| Process | Alert | When |
|---|---|---|
| executor | `PAPER`/`LIVE trade OPENED` | a real entry's first fill (side, symbol, class, qty, avg price, strategy) |
| executor | `SHADOW trades opened (n)` | one message per `SHADOW_DIGEST_INTERVAL_S` (default 15 min) listing every shadow position opened in it (side, symbol, price and its source, fee, strategy); nothing for an empty interval |
| executor | `HALTED` | a trade needs a human (§6.2) |
| executor | `cannot start` / `executor failed` | boot refused, or a crash (§6.3) |
| executor | `STUCK` | a signal has failed 10 times; still retried, cursor held at it |
| executor | `kill switch ENGAGED` / `released` | any change, and at startup if already engaged |
| executor | `trade mode changed` | off / paper / live |
| executor | `Daily summary` | `DAILY_SUMMARY_HOUR_UTC` (default 07:00): trades opened/closed and P&L by strategy (real and shadow), open positions, HALTs, signals, forecasts |
| signal | `PROCESSING LAG` | a bar is applied more than `LAG_WARN_S` after it closed |
| signal | `Sumtyme failing` | 5 forecast failures within 5 min |
| signal | `write failed` | a chain rule could not write a signal (retried) |
| feed | `bars held` | a symbol's bars wait over 5 min for a lagging rung |
| feed | `errored` / `FALLING BACK to REST` | a feed crashed and relaunched; the websocket could not connect |

**Heartbeats.** A process that has died cannot alert. Each long-running process
pings a dead-man's switch while it is healthy: create one check per process
(e.g. on healthchecks.io, period 1 min, grace 5 min, with its Slack
integration) and set `HEARTBEAT_URL_EXECUTOR` (pings while its LISTEN
connection is up), `HEARTBEAT_URL_SIGNAL` (while a bar was consumed in the last
5 min) and `HEARTBEAT_URL_FEED` (while a bar was published in the last 5 min).
The service alerts when the pings stop: a crash, a wedged event loop, or a
stalled pipeline.

Check the HALTs as part of the routine too:

```sql
SELECT trade_id, symbol, halt_reason, last_updated FROM trades WHERE state = 'HALTED';
```

---

## 6. Incidents

### 6.1 Stop trading now

```sql
-- stop opening; open positions keep their normal exits
UPDATE kill_switch SET engaged = true, mode = 'halt_new',
       reason = '<why>', engaged_by = '<you>', engaged_at = now();

-- stop opening AND close everything
UPDATE kill_switch SET engaged = true, mode = 'flatten',
       reason = '<why>', engaged_by = '<you>', engaged_at = now();
```

Both modes block new ENTERs; the row is read on every ENTER, with no cache.

**`flatten`** is read every second by the executor's escalation loop. For each
live trade it runs the same close as a normal EXIT signal:
1. cancels a working entry first, so nothing keeps buying while it sells. If
   Coinbase has not confirmed the cancel, or trading is `off`, it retries rather
   than guess;
2. waits until every entry fill the venue reported has been recorded, so the
   close covers the whole position;
3. closes the remaining position with exit reason `KILL_SWITCH`, as a
   marketable limit on the normal escalation ladder. An exit that is still
   unfilled at the top of the ladder HALTs (§6.2) instead of selling at any price;
4. finalizes a trade whose entry never filled, with nothing to sell.

It leaves alone trades already closing on a STOP or TARGET (the ladder is
working them), HALTED trades, and shadow positions (they hold no capital).
Flatten places orders, so it needs `paper` or `live`: in `off` every order is
suppressed. Watch for `kill switch FLATTEN` in the log, then:

```sql
SELECT count(*) FROM trades WHERE state NOT IN ('CLOSED','REJECTED');   -- 0 when done
```

Release with `engaged = false`. Releasing mid-flatten stops starting new closes;
exits already working carry on.

### 6.2 A HALTED trade

A HALTED trade is one the executor refuses to act on. Triggers:
- boot reconcile found a DB/venue mismatch for the symbol;
- an exit climbed the escalation ladder to the top without filling.

The reason is in `trades.halt_reason` and in the executor log.

1. Compare the DB with Coinbase:
   ```sql
   SELECT * FROM trades WHERE state = 'HALTED';
   ```
   against the portfolio balance and the open orders.
2. Bring the venue to the state you want. Cancel stray orders; sell or keep the
   position by hand.
3. Make the DB match: set the trade `CLOSED` with the realized figures, or back
   to `OPEN` if the position is real and should be managed. Clear `halt_reason`
   in the same statement (a CHECK ties it to `HALTED`).
4. Restart the executor so boot reconciles again:
   `docker compose restart executor`.

Never edit the DB to "un-HALT" without first checking the venue.

### 6.3 Boot refused or crash-looping

On a refused or failed boot, the executor logs the reason, waits
`EXECUTOR_BOOT_RETRY_S` (default 300 s) and exits 1. Compose retries, so a broken
boot retries at most every few minutes. Common causes:

- **Orphan balance.** The portfolio holds a non-dust asset outside the traded
  universe with no DB trade explaining it. Sell or move it, or add the currency to
  `config.ORPHAN_BALANCE_ALLOWLIST` if it is meant to sit there.
- **Key check failed.** Wrong portfolio, Transfer permission, Ed25519 key, or
  (live) no Trade permission. Fix the key and re-run
  `verify_coinbase_readonly.py`.
- **DB unreachable or migrations missing.** Apply the migrations.

### 6.4 Reset the paper account

Only in `off` or `paper`, with no open paper trades (drain as in §8 step 6, or
accept that boot will HALT them):

```sql
TRUNCATE paper_fills, paper_orders, paper_balances;
```

The next executor start re-seeds `config.PAPER_STARTING_CASH`.

### 6.5 Long outage

| Outage | Action |
|---|---|
| executor, any length | restart. Missed EXITs replay from the persisted cursor; stale ENTERs are skipped |
| feed, up to 100 h | restart. The feed catches up from the newest stored bar (`warmup_bars`) |
| signal, any length | restart. Buffers hydrate from `warmup_bars`; Kafka replays the rest |
| feed, over 100 h | stop feed + signal, `docker compose run --rm warmup`, start them again |

---

## 7. Capacity

Sumtyme `forecast_path` takes seconds per call, and it is the bottleneck.

- **Throughput** ≈ `FORECAST_CONCURRENCY ÷ latency`.
- **Demand** = `symbols × forecasting rungs × bars per second`. The 1m rung
  dominates.

If demand exceeds throughput, per-symbol queues fill and one oversubscribed
symbol delays every other symbol's signals. Sustained `PROCESSING LAG` means
cutting symbols or rungs (ADR-001 debt D12). Raising concurrency past what the
API can serve only adds timeouts.

---

## 8. Paper review and the go-live gate

`docs/paper_review.sql` holds the review queries:
- realized P&L and fee drag per symbol;
- fill price vs. touch (slippage in bps);
- HALTs;
- shadow-book results for shorts;
- the paper account.

```bash
psql "$TRADING_DB_URL" -f docs/paper_review.sql
```

Go live only when every step passes, in order:

1. **Green suite.** `pytest` passes, including the Postgres integration tests
   (`QISS_TEST_DSN` set).
2. **Read-only verify.** `verify_coinbase_readonly.py` ends `OK` against the
   production key and portfolio.
3. **Paper for N weeks** (agree N up front; at least two). The pipeline has run
   continuously, with no unexplained gaps in `feed_cursors` and no sustained lag.
4. **Review** the paper period:
   - **Fee drag.** Net P&L after the real taker tier. A strategy that only wins
     before fees does not go live.
   - **Fill vs. touch.** Entry and exit slippage is within the caps in
     `execution_caps`.
   - **Reconcile.** Every HALT and refused boot in the period is explained, and there are no
     `no matching order` fill errors.
   - **Shadow book.** Read as context only: it is the short side the account
     cannot trade.
5. **Pre-flight.**
   - Fund the bot portfolio with the intended capital only.
   - Set `CRYPTO_BASES` to **one** symbol.
   - Confirm that 5% of available balance exceeds the product's minimum order
     size.
6. **Drain paper.** Engage the kill switch (`halt_new` to let positions exit on
   their own signals, or `flatten` to close them now) and wait until
   `SELECT count(*) FROM trades WHERE state NOT IN ('CLOSED','REJECTED')` is 0.
   Note the timestamp.
7. **Switch.** `UPDATE trade_enabled SET mode = 'live', reason = 'go-live'`,
   watch the executor restart (exit 3) and reconcile cleanly, then release the
   kill switch.
8. **Watch the first live round trip end to end**: order → fill → `fills` row →
   exit → CLOSED. Compare the realized fill with the paper model.
9. **Widen** one symbol at a time, re-reviewing fee drag and fills after each.

Going back to paper is the same switch in reverse: drain, set `mode = 'paper'`,
and expect a restart.

---

## 9. Invariants worth knowing

- **Idempotency** is enforced at the database, not only in memory: `signal_id`,
  `client_order_id` (deterministic `cv1`, and Coinbase returns the existing order
  for a repeated id), `venue_exec_id`, and a unique `(chain_id, side)` on trades.
  Replays and restarts dedupe.
- **Boot order:** connect → reconcile → diff → fill stream → subscribe. The
  executor never takes a signal before it knows what it holds.
- **An exit settles the entry first:** a final close (EXIT signal or flatten)
  cancels a still-working entry and waits for its reported fills before selling,
  so a late buy fill can't be left behind. A close waiting on that is retried
  every second and survives a restart.
- **Escalation:** an exit that does not fill widens its cap rung by rung. The top
  rung HALTs rather than selling at an unbounded price.
- **The chain machine is copied from CausaView**, not shared. It differs by the
  monotonic bar guard; apply any change in both places.
