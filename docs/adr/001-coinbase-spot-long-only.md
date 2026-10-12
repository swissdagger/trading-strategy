# ADR-001: Re-architect for Coinbase Advanced Trade spot, long-only, crypto-only

**Status:** Proposed
**Date:** 2026-10-12
**Deciders:** Repo owner
**Supersedes:** The multi-venue (IBKR + Binance USDⓈ-M) execution design described in `README.md` §1 and `RUNBOOK.md` §3

---

## Context

The system is a four-process trading backend (`warmup` → `feed` → `signal` → `executor`) that runs Sumtyme causal-chain state machines over timeframe ladders and executes the resulting `trade_signals` through a venue adapter. It was built for two venues (IBKR for equity/fx, Binance USDⓈ-M futures for crypto) and three asset classes.

The actual deployment target is now:

- **One venue:** Coinbase Advanced Trade, **spot only**. The account is UK retail. Under FCA COBS 22.6, UK retail clients cannot be sold crypto derivatives of any kind (perpetuals, dated futures, options, CFDs, spread bets), and margin borrowing is likewise unavailable to standard retail clients. Coinbase's August 2026 UK derivatives launch is restricted to Professional Clients; elective professional status is not available to the owner at present.
- **Crypto only** for now. Equities are intended later, after further research.
- **Paper, then live.**

This forces one strategic constraint that the current code does not express: **there is no short side.** Both enabled strategies (`four_chain`, `prior_failure_scaleout`) emit SHORT entries for negative chains. `prior_failure_scaleout` is additionally expected to be replaced (slippage), and the capacity question is deliberately deferred.

Findings from the code review that shape this decision:

1. The Sumtyme marshalling and chain machine are sound and follow every documented sharp edge (allowlisted payload, placeholder built from the same column list, NaN assert, hours→minutes, step mode, `to_thread` behind a semaphore with an explicitly sized executor, `>=` buffer gate, `ForecastMemo` dedupe). No change needed.
2. The executor's safety spine (deterministic `cv1` ids, boot reconcile that HALTs, kill switch and trade mode as DB rows, escalation ladder, `(chain_id, side)` uniqueness) is good and is kept unchanged.
3. `SignalExecutor._reconcile` and `boot._position_matches` compare the venue's *per-symbol* quantity against *each* open trade individually. Two open trades on one symbol HALT each other spuriously. This is latent on Binance one-way mode and certain on spot, where positions are derived from balances.
4. The "paper = different venue endpoint" assumption (IB port 4002, Binance testnet) has no Coinbase equivalent. The Advanced Trade sandbox exposes only Accounts and Orders endpoints with canned responses — no matching engine.
5. The signal feed is Binance USDⓈ-M klines: a live dependency on a venue the owner cannot trade on and which geo-blocks UK/US IPs; there is a perp-vs-spot basis between signal and fill.
6. Hygiene: `Caddyfile` referenced by compose does not exist; `gitignore`/`dockerignore` lack leading dots (so `__pycache__` is committed); no `.env.example`; `run_executor.py` docstring describes persistence hooks as unimplemented (they exist); `requirements.txt` includes the stale `asyncio` PyPI backport; the test suite is red (3 modules fail to import, 21 failures across 4 files — config drift and non-`cv1` test ids).
7. Import graph: `binance_futures.py` imports resampling helpers from `polygon_options.py`; `two_ladder` is not a leaf — `producer/ladders.py` derives the built-in A/B ladders from `strategy/chain_view.py`, and `signal_process.py`, `strategy/runtime.py`, `producer/fanout_router.py` and `write_path_probe.py` import it.

---

## Decision

Re-architect to **Coinbase Advanced Trade spot, long-only, crypto-only**, with the short side preserved as *data* (a shadow book) rather than discarded, and with the executor hardened for a balance-based spot venue before any venue code is written.

Concretely:

- SHORT entries are **routed to a shadow book** (virtual fills at the real touch, real fee tier) and never sized or sent to the venue.
- Positions are derived from **base-asset balances in a dedicated Coinbase portfolio** with a portfolio-scoped API key.
- Reconcile is **netted per symbol**.
- Paper trading is an **in-process simulated-fill adapter** wrapping the real adapter's reads, not a venue sandbox.
- IBKR, Binance execution, Polygon feeds, and the legacy executor are **deleted**; `two_ladder` and `prior_failure_scaleout` are **disabled via config and left in the tree** (see Consequences).
- The signal feed moves to **Coinbase candles** (Option 2a below), with the Binance alternative recorded as a fallback.
- The short-chain *information* (opposing-chain state as an exit/gate/risk-off modifier for the long book) is the agreed research direction for recovering short-side value; it is out of scope for this ADR but the shadow book is built to feed it.

---

## Options considered

### Long/short handling

#### Option A — Long-only spot + shadow book (chosen)

| Dimension | Assessment |
|---|---|
| Complexity | Low: a side gate in `_on_enter`, one table, one small module |
| Cost | Half the signal population is not traded |
| Scalability | Trivially; shadow book is write-only |
| Regulatory | Fully within UK retail spot permissions |

**Pros:** Zero execution risk on the untradeable side; produces the only short-side data available; nothing to unwind later.
**Cons:** The research population's edge was measured long+short; long-only edge is unknown until paper measures it.

#### Option B — Elective professional status → Coinbase derivatives (Deribit-powered gateway)

| Dimension | Assessment |
|---|---|
| Complexity | High: a second adapter against a different API (INTX/Deribit, not Advanced Trade), a second margin model, a second reconcile |
| Cost | Requires 2 of 3 FCA criteria (portfolio > €500k; ≥10 significant trades/quarter over 4 quarters; 1 year professional derivatives role); loses retail protections |
| Scalability | Full long/short |

**Rejected for now:** capital criterion not met. Noted that a live long-only track record satisfies the trade-frequency criterion over four quarters. Kept as the medium-term route.

#### Option C — DeFi synthetic short (borrow, sell spot, buy back, repay)

**Rejected:** a second trading system (on-chain execution, DEX slippage or bridging latency, variable borrow rate, liquidation monitoring, deep borrow markets on few majors). Not an extension of this executor.

#### Option D — Inventory model (hold baseline, sell down on short chains)

**Rejected:** a long book with a tilt; parks capital in beta between signals; its one honest form is Option A's research direction applied to existing positions.

#### Option E — Inverse ETNs / short ETPs

**Rejected:** FCA's October 2025 retail cETN permission covers unleveraged long products in LSE hours via a broker. Wrong instrument for a 24/7 strategy with a 3-minute entry rung.

### Signal feed

#### Option 2a — Coinbase candles (chosen)

| Dimension | Assessment |
|---|---|
| Complexity | One new `AssetFeed` (`feeds/coinbase_spot.py`); 3m derived from 1m via existing `bar_aggregator` |
| Cost | Sumtyme is deterministic on the exact series; chains will differ bar-by-bar from the research population (which is being redone anyway) |
| Risk | None jurisdictional; same venue as execution, zero basis |

#### Option 2b — Keep Binance USDⓈ-M klines

| Dimension | Assessment |
|---|---|
| Complexity | None; symbol map in the adapter |
| Cost | Live dependency on a geo-blocked venue the owner cannot trade; perp-vs-spot basis must be logged |
| Risk | Feed can disappear without notice |

Chosen 2a. If 2b is retained during transition, the `feeds/binance_*` deletion in Phase 1 is deferred and the basis is logged at every fill.

### Paper trading

#### Option P1 — Coinbase sandbox

**Rejected:** canned responses, Accounts/Orders endpoints only, no fills. Useful only for exercising auth and request shapes.

#### Option P2 — `PaperCoinbaseAdapter` wrapping the live adapter's reads (chosen)

Simulated writes, real touch, real fee tier, ledger in the DB. The entire executor above the adapter (sizing, pricing, reconcile, escalation, kill switch) runs unmodified.

---

## Trade-off analysis

- **Deleting vs. disabling dead code.** Deletion wins where the code is a leaf (IBKR, Binance execution, Polygon, legacy executor): it is what keeps the suite red and the dependency list long. Disabling wins where deletion is a refactor (`two_ladder` is woven into the ladder registry's built-ins; `prior_failure_scaleout` owns the executor's partial-exit machinery). Both are recorded as debt with a trigger for removal.
- **Netted reconcile vs. per-trade reconcile.** Netting loses the ability to attribute a mismatch to one trade on a symbol; it gains correctness on any venue where positions are per-asset. On mismatch, all trades on the symbol HALT — conservative, and consistent with the existing "never guess with real money" posture.
- **Polling fills vs. websocket user channel.** The user channel reports order-level cumulative state, not per-execution ids; the executor dedupes on `venue_exec_id`. Polling `/orders/historical/fills` gives per-fill `trade_id`, commission and `client_order_id` with the same at-least-once model the executor already assumes. ~2 s attribution latency accepted; websocket is a later trigger optimisation.
- **Pessimistic paper fills.** Filling at the limit price (worst case inside the cap) biases paper P&L down and cannot evaluate maker entries. Accepted for the first paper run: it answers "is taker fee drag acceptable?" which is the first question.
- **Taker entries kept.** Maker/post-only entries would cut fees but require an entry TTL and cancel path the executor lacks. Deferred until paper quantifies fee drag.

---

## Phased change list

Each phase is independently mergeable and leaves the system runnable.

### Phase 0 — Hygiene (no behaviour change)

- [x] Rename `gitignore` → `.gitignore`, `dockerignore` → `.dockerignore`; `git rm -r --cached` committed `__pycache__`.
- [x] Add `.env.example` covering every env var read by the four processes.
- [x] Remove the `caddy` service from `docker-compose.yml` (no external health consumer; `Caddyfile` absent). Port 8081 stays internal.
- [x] `requirements.txt`: remove `asyncio`; pin all versions; add `PyJWT`, `cryptography`.
- [x] Rename `execuctor_scale_probe.py` → `executor_scale_probe.py`; correct the stale `run_executor.py` docstring.
- [x] Tests: re-pin config-drift assertions (`test_fanout_router_multi`, `test_multi_runtime`, `test_four_chain_strategy`) to the Phase 1 config; convert `test_signal_executor` fake ids to `cv1` form; delete tests for Phase 1 deletions.
- [x] Add a GitHub Action running `pytest` on push. Green suite is the gate for Phase 3.

### Phase 1 — Prune to crypto-only

- [x] Delete `execution/ibkr_adapter.py`, `execution/binance_adapter.py`, `execution/_legacy_executor.py`, `execution/_legacy_exit_manager.py` and their tests.
- [x] `execution/adapter_factory.py`: keep `endpoint_kind_for_mode`; builders reduce to `build_coinbase_adapter` (Phase 4).
- [x] `run_executor.py`: `_ADAPTER_BUILDERS = {"coinbase": ...}`; `EXECUTOR_VENUE` default `coinbase`.
- [x] Create `feeds/resample.py` holding `_parse_timeframe` / `_resample_to_timeframe` (moved from `polygon_options.py`); repoint `binance_futures.py` and `bar_aggregator.py`.
- [x] Delete `feeds/polygon_stocks.py`, `polygon_forex.py`, `polygon_options.py`, `polygon_aggs.py`, `polygon_ws.py`, `contract_discovery.py`; remove equity/fx branches from `feed_process._build_feeds`; update `feeds/__init__.py`.
- [x] Keep `feeds/base.py` (incl. `is_market_open` / `exchange_calendars`) for future equities.
- [x] `config.py`: `ASSET_CONFIGS` → `crypto` only; remove `BNBUSDT`, `TRXUSDT`; `timeframes` → `["1m","3m","5m","15m","1h"]`; remove `two_ladder` from `STRATEGIES`; `prior_failure_scaleout.enabled = False`; `VENUE_ENDPOINTS` → `coinbase` only, credentials from env.
- [x] Reseed migration 015 caps for `crypto` only.

### Phase 2 — Feed → Coinbase candles

- [x] `feeds/coinbase_spot.py` implementing `AssetFeed`: `fetch_history` via public candles endpoint (paginated; granularities 1m/5m/15m/30m/1h/2h/6h/1d); `start_live` as REST polling at ~tf/2; native 5m/15m/1h, 3m derived from 1m via `bar_aggregator`.
- [x] Canonical symbol form becomes the Coinbase product id (`BTC-USD`) throughout (`universe.py`, chains, trades). Fresh DB; no migration of history.
- [x] Delete `feeds/binance_futures.py`, `feeds/binance_ws.py` and tests once the Coinbase feed is verified by a warmup run.
- [x] Record the series-provenance change in `RUNBOOK.md`.
- [x] Rename the `four_chain` strategy to **`confirmed_15m`** (owner's choice; `two_ladder` was taken by the old A/B strategy and is the DB default for the strategy column): registry key, `Confirmed15mEntry`/`Confirmed15mExit`, `confirmed_15m_strategy.py` / `confirmed_15m_adapter.py`, tests, config. The `FC` ladder name is unchanged (part of chain identity). Takes effect with the Phase 2 fresh DB.
- [x] Document the strategy rules: `docs/strategies/confirmed_15m.md`, written from the code (the original `trade_strategy_2.md` is not available), with a list of behaviours for the owner to confirm against the original intent.

### Phase 3 — Executor hardening (venue-independent; testable with existing fakes)

- [x] **Netted reconcile.** `SignalExecutor._reconcile` and `boot._position_matches`: group open DB trades by symbol, sum signed `open_qty`, diff once; on mismatch HALT all trades on that symbol. Epsilon = product `base_increment`.
- [x] **Side gate.** `ExecutionAdapter.supports_short: bool` class attribute. `_on_enter` routes SHORT to the shadow book before sizing/kill-switch when unsupported.
- [x] **Shadow book.** Migration 019 `shadow_trades (strategy, position_key, symbol, side, entry_signal_id UNIQUE, entry_px, entry_at, exit_signal_id, exit_px, exit_at, exit_reason, fee_bps, pnl_bps)`. New `execution/shadow_book.py`: ENTER fills at the touch on the side you would get (bid for a short, which sells to open; corrected from "ask" in the original draft), EXIT at the opposite touch, taker fee applied twice; runs under the per-symbol lock.
- [x] **Sizing.** `make_size_position` uses `equity.available`; `MAX_OPEN_POSITIONS` in config, checked under the lock; new `ExecutionAdapter.get_price_increment`; `limit_price` quantized to `quote_increment` toward the reference (buys floor, sells ceil); reject below `base_min_size` / `quote_min_size` pre-submit.
- [x] **Orphans HALT boot.** Unknown non-dust balances in the bot portfolio raise in `start()`; config allowlist of base assets to ignore.
- [x] **Persisted cursor.** Migration 020 `executor_cursor` singleton; `run_executor.py` loads it as `initial_cursor`; listener writes after each dispatch. ENTER max-age (default 2 × baseline tf) — stale ENTERs skipped, EXITs always honoured.
- [x] **HALT alerting.** Confirm the trigger path in migration 018 (email notifications); wire `_halt` and boot refusal into it. *(018 has no trigger; alerts go to a new outbox, migration 021 — see Implementation notes, PR 2.)*

### Phase 4 — `CoinbaseAdapter`

- [x] `execution/coinbase_client.py`: ES256 JWT per request (CDP key; `uri` claim) over `httpx.AsyncClient`.
- [x] `execution/coinbase_adapter.py` (`venue = "coinbase"`, `supported_classes = {"crypto"}`, `supports_short = False`):
  - `connect()`: signed `GET /accounts`; refuse if the key sees more than the bot portfolio.
  - Products cache from `GET /products/{id}`: `base_increment`, `quote_increment`, `base_min_size`, `quote_min_size`, `status`.
  - `get_touch`: `GET /best_bid_ask`.
  - `get_account_equity`: `available` = quote available balance; `total` = quote + Σ(base × mid).
  - `get_positions`: non-zero base balances (always positive).
  - `submit_order`: `POST /orders` with `limit_limit_gtc`; map HTTP-200 `success:false` + `error_response` → `REJECTED`. `client_order_id` passed through; verify venue length limit on first call, reuse deterministic compression if needed.
  - `cancel_order`: `POST /orders/batch_cancel` by venue id from the submit-time map, fallback to filtered open-orders lookup.
  - `amend_order`: `POST /orders/edit` (native).
  - Fill stream: poll `GET /orders/historical/fills` at ~2 s, cursored; `trade_id` → `venue_exec_id`, commission → `fee`, `client_order_id` for attribution.
  - `reconcile`: positions + `GET /orders/historical/batch?order_status=OPEN` + recent fills.
- [ ] Account: dedicated Coinbase portfolio for the bot; CDP key created on that portfolio; nothing else held in it.
- [ ] Read-only verification against the real portfolio with `trade_enabled = off`. *(Script ready: `verify_coinbase_readonly.py`; needs the owner's key.)*

### Phase 5 — `PaperCoinbaseAdapter`

- [x] Wraps a `CoinbaseAdapter` for reads (touch, products, fee tier from `GET /transaction_summary`); simulates writes: immediate full fill at `limit_price`, real taker fee.
- [x] Migration 022 `paper_ledger` (cash, positions) + `paper_fills` (renumbered: 021 is `executor_alerts`); paper reconcile exercises the real boot path.
- [x] `supports_short = False` so shadow shorts behave identically in paper and live.
- [x] Factory: `off` / `paper` → paper adapter; `live` → real adapter (unchanged mapping).

### Phase 6 — Ops and go-live

- [x] Add `executor` service to `docker-compose.yml`; seed `trade_enabled.mode = 'paper'`.
- [x] Rewrite `README.md` §1 and `RUNBOOK.md` for the Coinbase topology.
- [x] Go-live gate: green suite → read-only connect in `off` → paper N weeks → review (fee drag, fill-vs-touch, reconcile stability, shadow-short stats) → `live` on one symbol at minimum size → widen.

---

## Consequences

**Easier**
- One venue, one key family, one symbol namespace, one asset class: the adapter factory, feed builder, config and caps collapse to a single path.
- Reconcile is correct for balance-based venues and for multiple trades per symbol.
- The short side is measured instead of lost; the data feeds the agreed research direction (short-chain state as a long-book gate / exit / risk-off modifier).
- Paper and live share every line above the adapter.
- The suite becomes a real deploy gate.

**Harder**
- Research continuity: chains on Coinbase candles differ from the Binance-derived research population; the research is to be redone on the live series.
- Equities later will reintroduce a second venue, calendar-gated feeds and the ladder rework; the scaffolding is kept, but the integration is not free.
- A true short requires Option B (professional status) and a second adapter against a different API; nothing built here is reused for that write path.

**Debt register (remaining after Phase 6)**

| # | Item | Why it stays | Removal trigger |
|---|---|---|---|
| D1 | `two_ladder`, `chain_view` A/B ladders, `producer/ladders.py`, single-ladder `fanout_router.py`, `strategy/runtime.py`, `write_path_probe.py` | Woven into `ladder_registry` built-ins; deletion is a registry refactor | Before equities, when ladders are reworked |
| D2 | `prior_failure_scaleout`, PF ladder, executor SCALE/TERMINAL deficit model and tranche-strided `cv1` rungs | Tested, not dangerous, but the most intricate executor code exercised by nothing enabled | When the replacement strategy's exit model is decided; strip if it needs no partial exits |
| D3 | Paper fidelity: no partials, no latency, no queue position, fills at limit | Answers the first question (taker fee drag) | If maker entries are to be evaluated, add a maker/latency model |
| D4 | Fills by polling (~2 s); `fills_unattributable` may tick in the submit→ack window | At-least-once model already assumed by the executor | Add user-channel websocket as a latency trigger |
| D5 | Series provenance (2a: research on a different series; 2b: geo-blocked dependency) | Inherent to the feed decision | Research redone on the live series |
| D6 | Flat 5%-of-available sizing, count-capped; no vol scaling, no correlation awareness | Adequate for paper and minimum-size live | Before scaling capital |
| D7 | No entry TTL / cancel path | Dormant while entries are crossing limits | Required the moment post-only entries are tried |
| D8 | Shadow book is an upper bound (touch fill, no impact) | Only short-side data available | Treat P&L as "at most this"; refine with a slippage model if it drives Option B |
| D9 | `service2_machine.py` is a copy of CausaView's chain machine (sync obligation) | Diverges by one change: the monotonic bar guard in `on_bar` (PR 3); port it to CausaView or keep it in mind when syncing | Shared library extraction, if CausaView and this repo converge |
| D10 | Equities scaffolding (`is_market_open`, `exchange_calendars`, `asset_class` plumbing, by-class caps) | Deliberately retained | N/A — intentional |
| D11 | Single-broker Kafka on one box; Supabase over the network; `statement_cache_size=0` pgbouncer convention | Operational, not architectural | Revisit with the capacity work |
| D12 | Capacity sizing deferred (top-of-hour burst: ~70 simultaneous forecasts across 12 symbols × 6 rungs at `FORECAST_CONCURRENCY=6`) | Parked by decision | Benchmark `ForecastGate` p50/p95 before adding symbols or rungs |
| D13 | Orphan-balance allowlist is manual config | Dedicated portfolio makes orphans rare | Keep; document |

---

## Action items

1. [ ] PR 1 — Phases 0 and 1 together (mechanical; ends with a green suite and CI).
2. [ ] PR 2 — Phase 3 (executor hardening; fully testable with fakes; no venue credentials needed).
3. [ ] PR 3 — Phase 2 (Coinbase feed) and a warmup run to verify history depth per rung.
4. [ ] PR 4 — Phase 4 (`CoinbaseAdapter`) + read-only verification in `off` against the real portfolio.
5. [ ] PR 5 — Phase 5 (`PaperCoinbaseAdapter`) + Phase 6 compose/runbook; start the paper run.
6. [ ] Separate research ticket: short-chain state as a long-book gate / exit / risk-off modifier, fed by `shadow_trades` and `chains`.
7. [ ] Separate ticket: capacity benchmark (D12) before any universe or ladder growth.
8. [ ] Revisit Option B (professional status) when the trade-frequency criterion is met by the live record and capital allows.

---

## Implementation notes

### PR 1 (Phases 0–1)

- **`four_chain` entry trigger was unreachable (fixed).** `FourChainEntry.on_event` returned early unless `event_type == "level_reached"`, which no chain machine emits, so from the 2026-08-13 switch to the two-chain rules the strategy never entered (every four_chain row in `trade_signals` predates it). The trigger is now `chain_propagated` at 15m. The event-type check is kept on purpose: `chain_terminated` carries the level the chain died on, which can be 15m. `tests/test_four_chain_strategy.py` is rewritten from the old four-chain spec to the two-chain rules, including a test that a termination at 15m does not enter.
- Test failures that were *not* config drift: `test_fanout_router_multi` fakes lacked `ChainEvent.price`; `test_multi_runtime` fakes declared ladder `A` while the runtime now gates entries by declared ladder, and lacked the `decision_price` kwarg on exit; `test_signal_executor` scripted results carried placeholder client ids (now echo the submitted `cv1` id, as a venue does), and two tests asserted that unattributable fills are persisted with `trade_id = NULL`, whereas the executor deliberately counts them (`fills_unattributable`) and leaves them for reconcile because `fills.trade_id` is NOT NULL; `test_strategy_signals_integration` imported `make_insert_signal` from the wrong module.
- Symbols stay in Binance form (`BTCUSDT`) and the feed stays on Binance klines until Phase 2, so the signal side keeps running between PRs. `feeds/binance_ws.py` and `feeds/streaming_base.py` stay for the same reason.
- `build_coinbase_adapter` raises `NotImplementedError` until Phase 4; `run_executor.py` exits 1 with that message rather than trading through a missing venue.
- Migration 015 is edited in place (equity rows commented out) because Phase 2 starts from a fresh DB. On an existing DB, the equity rows are inert and can be deleted by hand.

### PR 2 (Phase 3)

- **Alerting: migration 018 has no trigger.** It is only the dedupe/cursor table of an email notifier that consumes `trade_signals`, and that notifier is not in this repo. HALTs and refused boots therefore write to a new outbox, `executor_alerts` (migration 021), whose trigger fires `pg_notify('executor_alert', alert_id)`. **Email delivery still needs the notifier to LISTEN on that channel (or sweep `delivered_at IS NULL`).** Alerts are best-effort: a failing insert never blocks the HALT.
- **Migration numbering:** 019 `shadow_trades`, 020 `executor_cursor`, 021 `executor_alerts`. Phase 5's paper ledger becomes 022.
- **Shadow fills:** a short ENTER fills at the **bid** (it sells), and its EXIT at the ask. The original draft said "ask for short", which would have recorded short entries at the wrong side of the book. SCALE tranches are ignored; the virtual position closes on the final EXIT. With no usable touch, the signal's `decision_price` is used and logged.
- **Ordering of entry gates:** replay check → stale-ENTER check → side gate (SHORT → shadow) → kill switch → open-position cap (under a cross-symbol entry lock) → sizing → pricing → venue minimums. Shadow entries are therefore not limited by the kill switch or the cap (they take no risk), but are age-gated, so the shadow book only records entries the long book would really have taken.
- **Reconcile tolerance** is one venue quantity increment (`get_step_size`, i.e. `base_increment`), falling back to the per-class epsilon. HALTED trades count toward the symbol's net (their quantity is still at the venue) and toward `MAX_OPEN_POSITIONS`.
- **Sizing** is 5% of `available`, falling back to `total` with a warning only if the venue reports no available balance. Venue minimums are enforced on entries only; exits are never refused pre-submit (a stranded position is worse than a venue rejection).
- **New adapter surface:** `supports_short` (default False — a venue must opt in), `get_price_increment` and `get_min_order_size` (both default None = unknown). Limits are rounded toward the touch: buys floor, sells ceil.
- **Bugs found and fixed along the way:** `GatedAdapter` was still abstract (missing `get_touch` / `get_account_equity` / `get_step_size`), so `run_executor.py` would have crashed on start; `run_executor.py` never started the escalation loop, so unfilled exits would never escalate or HALT; the three existing real-Postgres integration test modules used plain `@pytest.fixture` for async fixtures and errored whenever `QISS_TEST_DSN` was set.
- **CI** now runs the integration tests: a Postgres 16 service, all migrations applied, `QISS_TEST_DSN` set.
- `boot.run_boot_reconcile` is netted as specified, but nothing calls it: `SignalExecutor.start()` runs its own reconcile, which is the live path.

### PR 3 (Phase 2, feed)

- **Verified against the live API** (once `api.coinbase.com` was allowed):
  - Response shape as documented: newest first, string prices, the forming candle included, an empty list before a product's listing. The request cap is on the time span: up to 350 intervals is accepted (the extra candle trimmed), wider spans return HTTP 400.
  - **Real warmup** into a fresh DB with all migrations: 50/50 (symbol, timeframe) streams seeded with 5000 bars each, 50 feed cursors, 1,192 requests, no errors or rate limiting, 156 s. Every bar aligned to its timeframe with sane OHLC; BTC/ETH/SOL/XRP 1m have no gaps, so pagination drops nothing.
  - **Gaps are real Coinbase data:** thin books omit no-trade minutes (AAVE-USD 1m: 827 missing minutes in ~4 days, concentrated 21:00–01:00 UTC; DOGE 426; LINK 259), confirmed by re-querying the API. Every product also lacks 1h candles 2026-05-08 02:00–06:00 UTC (a venue-wide outage).
  - **Live polling run** (BTC-USD, AAVE-USD, 1m/3m, from cursors minutes stale): every window emitted in order, none skipped.
- **Found and fixed by the live checks:**
  - **Closed candles keep changing.** Over 148 one-minute windows on all ten books, 39% were revised after close (median 12 s, p90 25 s, max 36 s; thin books slowest, BTC/ETH/SOL/XRP/LTC within ~7 s). The settle delay went from 5 s to **60 s**, applied to history as well as live. Cost: a 1m bar reaches the signal process ~60–90 s after its window closes.
  - **Stale-cursor gap:** live polls read a fixed 6-window lookback, so a feed started more than 6 windows after its cursor silently skipped the windows between. Polls now read from the cursor forward (clamped at 100 h of 1m; longer gaps need a re-warmup). The Binance feed had the same flaw.
  - **Duplicate bar at startup:** the live cursor started AT warmup's newest bar instead of after it (contrary to the AssetFeed contract), so each stream's newest warmup bar was appended to the chain machine's buffer twice. Inherited from the Binance feed; fixed in the feed. The chain machine also gained its own guard (below).
  - **Executor stale-ENTER gate (Phase 3 bug):** it measured age from `decided_at`, which is the triggering bar's window START — 15+ minutes back for a 15m trigger — so it would have skipped every four_chain entry. It now measures from `trade_signals.created_at` (when the signal was written), which is what "replayed after downtime" means.
- **Completeness rule (live):** a window is emitted only once its end is `SETTLE_MS` (60 s) in the past, and a derived window (3m from 1m) only once its last base candle is published, or one base interval of grace has passed (that minute had no trades).
- **Derived rungs:** 3m from 1m for four_chain; 4h from 1h and 12h from 6h so the disabled PF ladder still has a feed. No synthetic gap-filling.
- **Symbols** are built from `config.CRYPTO_BASES` and `config.QUOTE_CURRENCY` (`USD`). All ten `-USD` products are `online` with trading enabled publicly; whether the account can trade them is confirmed in Phase 4 with a key.
- `transport='ws'` fails loudly at startup: there is no Coinbase websocket feed. `streaming_base.py` and `bar_aggregator.py` stay for one.
- **Duplicate bars reaching Sumtyme — fixed in the chain machine.** `ServiceTwoChainMachine.on_bar` now drops a bar whose timestamp is not newer than the newest buffered bar for its timeframe (repeats logged at DEBUG, out-of-order at WARNING). This covers Kafka at-least-once replay after a signal-process restart as well as any feed/warmup overlap, and stops a replay from re-running the forecast and re-emitting chain events. It is a deliberate divergence from CausaView's copy of the machine (debt D9), requested by the owner.
- **Rename and spec:** `four_chain` is now `confirmed_15m`; the rules are documented in `docs/strategies/confirmed_15m.md` from the code.

### PR 5 (Phase 4, Coinbase adapter)

- **Built from Coinbase's published OpenAPI spec** for the Advanced Trade REST API and its API-key authentication docs, and tested against a fake brokerage on that spec (it verifies every request's JWT signature and claims, and models order state, queued cancels, client-id idempotency and paginated fills). **Not yet run against the real portfolio:** that needs the owner's key; `verify_coinbase_readonly.py` is the read-only check (no writes).
- **Key requirements, enforced by `connect()`:** ECDSA signature algorithm (Coinbase rejects Ed25519 keys for this API; the client refuses one with a clear message), bound to `COINBASE_PORTFOLIO_UUID` (`/key_permissions`), View + Trade (Trade only required in live), and **no Transfer permission** (a key that can move funds is refused). Every account the key sees must belong to the bot portfolio.
- **Modes:** `live` → `CoinbaseAdapter`. `off` → the real adapter wrapped in `ReadOnlyAdapter`: connect, reconcile, balances, touches and fills work; submit/cancel/amend never reach the venue, independently of the mode gate. `paper` refuses to start until Phase 5. (An off→paper switch keeps the read-only adapter, so it can never place a real order.)
- **Deviations from the plan, from the spec:**
  - Limit prices round to the product's **`price_increment`** (falling back to `quote_increment`); it is the price tick. The two are equal on all ten traded products today.
  - **Fills carry no `client_order_id`.** The fill poller looks up each unknown `order_id` once (cached) to recover our id, so fills streamed before the submit acknowledgement are still attributable, and so are fills seen after a restart. This closes most of debt D4's gap.
  - `venue_exec_id` is the fill's **`entry_id`** (unique per fill entry), falling back to `trade_id`.
  - A cancel is queued, not instant (`CANCEL_QUEUED`): `cancel_order` waits (up to ~2 s) for a terminal status and reports `filled_size`, so the executor never issues a replacement exit while the old order can still fill.
  - Resubmitting a `client_order_id` returns the existing order (documented), which is the venue half of the executor's idempotency.
  - A transport failure or 5xx after retries **raises** from `submit_order` (the order may exist; the executor resolves it at reconcile); a 4xx or `success: false` is a REJECTED result with Coinbase's reason.

### PR 6 (Phases 5–6, paper trading and ops)

- **Paper fill model:** a limit that crosses the real touch fills **in full at its limit price** (not the touch), so paper slippage is the cap actually used, an upper bound; a non-crossing limit rests and is re-checked against the touch every poll (`fill_poll_s`). No queue position, partials or impact, so paper says nothing about fill rates in thin books. Fees use the account's real taker tier (`GET /transaction_summary`, cached 6 h, falling back to `SHADOW_TAKER_FEE_BPS`); the shadow book now uses the same tier per entry.
- **Ledger** (migration 022): `paper_balances`, `paper_orders` (keyed by `client_order_id`, so a repeat returns the same order, as on Coinbase), `paper_fills`. A fill's writes are one transaction. Seeded with `config.PAPER_STARTING_CASH` on first start; a restart resumes the same account and boot reconcile runs against it.
- **Factory:** `off` and `paper` → `PaperCoinbaseAdapter` over the real adapter wrapped in `ReadOnlyAdapter` (reads are real, and a real write is structurally impossible); `live` → `CoinbaseAdapter`.
- **Mode-change fix:** the old hot swap rebound only the launcher's adapter reference while `GatedAdapter` kept the old one, so live → paper would have kept placing **real** orders. A paper ↔ live change now wraps the gated adapter's inner adapter in `ReadOnlyAdapter` at once, stops the executor and exits 3; compose restarts it and boot reconciles against the new venue. off ↔ paper is a no-op (same adapter).
- **Paper seed** (migration 023): mode becomes `paper` only while `trade_enabled` still carries its initial reason and mode `off`, so it never overrides an operator.
- **Critical persistence bugs found by the new real-Postgres round-trip test** (paper entry → fill → exit through the real hooks, then the review queries):
  - `persist_fill` looked fills up by `orders.venue_order_id`, which nothing ever wrote, so **every fill was skipped as a duplicate** — no `fills` rows, no realized P&L, in paper or live. It now resolves by `client_order_id` first (the executor passes it), back-fills `orders.venue_order_id`, and logs ERROR for a known trade's fill it cannot map.
  - `get_exit_cap` received the JSONB escalation ladder as text (asyncpg has no JSON codec on the pool), so **every EXIT raised `TypeError`**. It now decodes it.
- **Ops:** `executor` service in `docker-compose.yml` (after warmup, `restart: unless-stopped`). A refused or failed boot sleeps `EXECUTOR_BOOT_RETRY_S` (300 s) before exiting 1, so a restart loop alerts at most every few minutes. `README.md` and `RUNBOOK.md` rewritten for the Coinbase topology; `RUNBOOK.md` §8 is the go-live gate; review queries in `docs/paper_review.sql`.
- **Shared trade tables:** `trades`/`orders`/`fills` carry no mode, so paper positions must be drained before going live (else boot HALTs them) and reviews filter by the go-live time. Documented in the runbook; a `mode` column is a candidate if this proves error-prone.
- **Still open:** the paper run itself. (Alert delivery and `flatten` were resolved after this PR; see below.)

### PR 7 (alerts removed, kill switch flatten, exit-entry fix)

- **Alerts removed (owner decision).** Nothing delivered from the `executor_alerts` outbox, and the owner chose not to build a notifier. Migration 024 drops the table, its trigger and function; the executor's `alert` hook, `make_alert` and the boot-failure alert are gone. HALTs are logged at ERROR (with strategy, symbol and side) and stay visible as `trades.state = 'HALTED'` + `halt_reason`; a refused boot is logged and retried every `EXECUTOR_BOOT_RETRY_S`. Migration 018 (`email_notifications`, the cursor table of an external trade-signal notifier) is untouched. `execution/boot.py` keeps its own alert callback parameter; that module is not on the live path.
- **Kill switch `flatten` implemented.** `make_kill_switch_flatten` reads the row (engaged AND mode = 'flatten'; a missing row does not flatten). `SignalExecutor.run_close_requests_once` (originally `run_flatten_once`), run every tick by the escalation loop, takes each live trade under its symbol lock and:
  1. cancels a working entry (PENDING/SUBMITTED, or OPEN with an unfilled remainder). It does not proceed when the cancel raises, is unconfirmed (WORKING/PARTIALLY_FILLED), or was suppressed by the trade gate (`off`). It retries next tick instead.
  2. waits until the entry fills the venue reported at cancel are recorded, so a late fill can't be left as an unmanaged position.
  3. closes the whole remainder with reason `KILL_SWITCH` via `_issue_exit` (marketable limit + the normal escalation ladder, which HALTs at the top). A working SCALE tranche is cancelled and superseded.
  4. finalizes CLOSED a trade whose entry never filled.
  It skips trades already CLOSING for good (the ladder owns them), HALTED trades, and shadow positions. `make_set_kill_switch` now accepts `flatten`.
- **Exit bug fixed: a normal EXIT now cancels a still-working entry first.** Before, an EXIT on a trade whose entry had not filled (or had filled only partly) closed what was recorded and left the buy order working. A trade with no fills was marked CLOSED outright. A late entry fill then became a position nobody managed, and the next boot refused it as an orphan or HALTed the symbol. Final EXITs and flatten now share `_request_close`, so the same entry-cancel safety applies to both. When the entry can't be settled at once, the close is queued in `_close_requests`, retried every tick by `run_close_requests_once` (which replaced `run_flatten_once`), and made durable through `trades.exit_reason`. `start()` re-queues it after a restart. A final EXIT that supersedes a working SCALE tranche cancels the tranche and returns the trade to OPEN before recording the final reason. An EXIT on a fully filled entry is unchanged and makes no extra venue call.

