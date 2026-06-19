# Trading System — Assembly & Runbook

A standalone, headless trading system forked from CausaView. It runs two parallel
chain "ladders" (Set A / Set B) per symbol, applies a two-ladder entry/exit
strategy, and **emits trade signals to a database**. A separate team's script
consumes those signals and manages positions today; this project's **own**
position manager is built but dark, to be switched on later.

This document is the assembly reference: how the pieces fit, what runs as what
process, what is wired vs. stubbed, and the exact remaining steps to first live
signal. It is **not** a description of a finished, running system — see
§7 (Status) for what is and isn't live-ready.

---

## 1. Layers

Three separable layers plus the schema. Separability is the point: the signal
layers run now; the position manager runs later, in its own process, dark until
switched on.

| Layer | Package | Turns… | …into | Status |
|---|---|---|---|---|
| Producer | `producer/` | market-data bars | tagged chain events | new layer done; chain machine is a seam (see §7) |
| Strategy / consumer | `strategy/` | tagged chain events | trade signals (DB rows) | **complete & verified** |
| Position manager | `execution/` | trade signals / chain events | real venue orders | spine done; adapters + assembly open |
| Schema | `migrations/` | — | the trading DB | **ready to apply** |

---

## 2. Project structure

```
trading/
├── migrations/                     # apply in order to a NEW Supabase project
│   ├── 001_trade_lifecycle.sql     # trades, orders, fills, exit_state, caps, kill_switch
│   ├── 002_signals_and_chains.sql  # signals + chains (research/audit) + FK from trades
│   ├── 003_lockdown_rls.sql        # RLS deny-all for the public API (service_role bypasses)
│   └── 004_trade_signals.sql       # the trade_signals cross-team contract table
│
├── producer/                       # bars -> tagged chain events  (the CausaView fork)
│   ├── ladders.py                  # Set A / Set B as data; timeframe -> which set(s)
│   ├── chain_machine.py            # ChainMachine INTERFACE (extraction seam for service_2)
│   └── fanout_router.py            # routes each bar to every ladder containing its tf; tags events
│
├── strategy/                       # tagged chain events -> trade signals  (the deliverable)
│   ├── chain_view.py               # TaggedChainEvent contract + per-set chain-state view
│   ├── two_ladder_strategy.py      # four-step entry + OR-exit logic
│   └── runtime.py                  # event ingress; ordering + per-symbol lock + open-position tracking
│
└── execution/                      # trade signals -> orders  (position manager; dark until switched on)
    ├── types.py                    # OrderRequest/Result, Fill, VenuePosition, enums
    ├── adapter.py                  # ExecutionAdapter ABC (one per venue)
    ├── client_order_id.py          # deterministic client-order-id keystone
    ├── boot.py                     # connect -> reconcile -> diff -> (proceed | HALT)
    ├── executor.py                 # live entry path; per-symbol lock; marketable-limit pricing
    ├── exit_manager.py             # OPEN->CLOSING->CLOSED; escalation ladder
    └── signal_writer.py            # decisions -> trade_signals rows (used by BOTH signal + PM sides)
```

Dependencies point downward only: `producer` and `strategy` both import the
shared event contract from `strategy/chain_view.py`; `strategy/signal_writer`
lives under `execution/` because both the signal process and the position manager
emit/consume signals. There are no upward imports (the executor never imports the
strategy).

---

## 3. Process topology — two now, three later

**Do not assemble this as one process.** The layers have different lifecycles and
(today) different deployment states. One process would chain the dark
position-manager to the live signal path and break the "someone else executes for
now" arrangement.

### Today — two processes

```
┌─────────────────────────── SIGNAL PROCESS ───────────────────────────┐
│  feeds (Polygon/Binance)                                             │
│      │ closed bars                                                   │
│      ▼                                                               │
│  FanoutRouter ──tagged events──► (Kafka: chain-events)              │
│      │ (also, in-process)                                            │
│      ▼                                                               │
│  StrategyRuntime ── ENTER/EXIT ──► trade_signals table              │
│   (ChainView + two-ladder strategy + SignalWriter)                  │
│                                                                      │
│  Producer and strategy share ONE process: they're coupled by the    │
│  per-symbol lock and the in-memory ChainView. Splitting them would  │
│  mean serializing the view over a wire for no benefit.              │
└──────────────────────────────────────────────────────────────────────┘
                                   │ trade_signals (DB)
                                   ▼
                    ┌──────────────────────────────┐
                    │  OTHER TEAM'S SCRIPT          │
                    │  (sizing + position mgmt)     │
                    └──────────────────────────────┘
```

### Later — switch on the third process

```
┌──────────── POSITION-MANAGER PROCESS (dark today) ────────────┐
│  boot.run_boot_reconcile()  → connect, reconcile, diff, HALT? │
│      │ (only if safe_to_proceed)                              │
│      ▼                                                         │
│  Executor (entries) + ExitManager (exits + escalation ladder) │
│      │                                                         │
│      ▼  ExecutionAdapter → IB Gateway / Binance               │
│  consumes: trade_signals table OR chain-events (see §5)       │
│  separate process so its real-money blast radius is isolated  │
│  — a crash here never touches signal production, and vice     │
│  versa.                                                        │
└───────────────────────────────────────────────────────────────┘
```

The **per-symbol lock map** matters only *within* a process. Across processes the
coordination is the **database** (signal process writes `trade_signals`; position
manager reads) and **Kafka** (both may consume chain-events). If the position
manager ever runs in the SAME process as the signal side (not recommended), pass
the same lock map into `FanoutRouter`, `StrategyRuntime`, `Executor`, and
`ExitManager` so producer/consumer/execution work for one symbol can't interleave.

---

## 4. Data flow & the stores

```
bars ─► FanoutRouter ─► tagged chain events ─► Kafka(chain-events)
                                              └► StrategyRuntime (in-proc)
                                                     │
                       chains / signals (audit) ◄────┤ (producer persists)
                                                     ▼
                                              trade_signals  ◄── the contract
                                                     │
                            ┌────────────────────────┴───────────┐
                            ▼                                     ▼
                     other team's script              position-manager process
                                                       (trades/orders/fills/exit_state)
```

- **`signals` / `chains`** (002): research + audit, written by the producer's
  chain machines. `chains.chain_id` is the FK target for `trades`.
- **`trade_signals`** (004): the cross-team product. Append-only ENTER/EXIT rows,
  keyed by `position_key = (chain_id, side)`. Deterministic `signal_id` → replays
  dedupe on both sides.
- **`trades/orders/fills/exit_state/execution_caps/kill_switch`** (001): the
  position manager's state. Untouched by the signal process.

---

## 5. The one open coupling decision

**When switched on, does the position manager consume `trade_signals` (DB) or
chain-events (Kafka)?**

- **trade_signals (recommended v1):** acts on the SAME distilled signals the other
  team gets — one strategy, two executors, perfectly consistent. Inherits
  DB-poll latency, which is fine while you're not latency-bound.
- **chain-events (Kafka):** lower latency, but re-derives entry/exit decisions in
  a SECOND place → the strategy logic now lives in two processes and can drift.

Decide before building the position-manager assembly. The rest of the PM spine is
agnostic to this choice; only its input adapter changes.

---

## 6. Deployment

- **New, separate Supabase project** (not a new schema in CausaView's project).
  Isolation of blast radius, billing, credentials. Apply `001→002→003→004` in the
  SQL editor as `service_role`. Services connect as `service_role` (or a direct
  Postgres connection string) — never the anon key; there is no browser client.
- **Separate server** from CausaView (already decided). The position-manager
  process additionally needs **IB Gateway + IBC** (a JRE keepalive) co-located —
  size the box for it; keep it off CausaView's 2-vCPU VM.
- **Kafka** on the trading box (kept for the replayable audit log + multi-consumer
  extensibility). Single broker; consumer groups per consumer.
- Env: Polygon/Binance keys, Supabase service-role key + DB URL, Kafka bootstrap,
  (later) IBKR credentials for the dedicated bot username.

---

## 7. Status — what's wired vs. open  (READ BEFORE RUNNING)

**Verified & complete**
- Schema (all four migrations parse; apply in order).
- Strategy/consumer path end-to-end: tagged event → view → four-step entry +
  OR-exit → `trade_signals`. Integration-tested across all branches.
- Producer fan-out + tagging: overlap bars fan to both ladders with correct
  per-set levels; output matches the consumer contract.
- Deterministic id keystones (client_order_id, signal_id) proven.
- Position-manager spine: boot/reconcile diff matrix, executor entry path +
  marketable-limit pricing, exit manager + escalation ladder.

**Open — required for first LIVE signal**
1. **Extract `service_2` chain machine behind `producer/chain_machine.ChainMachine`.**
   The seam is built and tested with a fake. Drop the real INIT/PROPAGATE/TERMINATE
   logic in via a `MachineFactory`; no router change needed. Preserve service_2's
   invariants (per-symbol lock, `>=` buffer check, `asyncio.to_thread` Sumtyme
   call, hydrate-on-restart).
2. **Bar granularity / entitlement.** Feeds must produce the seven timeframes
   `5s,10s,30s,1m,2m,5m,10m` (`producer.ladders.all_timeframes()`). Confirm the
   Polygon plan exposes clean 5s/10s aggregates; decide the Binance-side source
   for sub-minute crypto (klines bottom out at 1m). This is an operational/data
   decision, not code.
3. **`config.py::ASSET_CONFIGS`** must carry both ladders' timeframes per symbol
   (and `TIMEFRAMES_BY_CLASS` equivalents) so the feeds actually fetch them.
4. **Wire the signal process `main`:** feeds → FanoutRouter → StrategyRuntime,
   with the real `MachineFactory`, the `SignalWriter` insert hook (idempotent
   `ON CONFLICT (signal_id) DO NOTHING`), and the `RuntimeHooks` persistence.
5. **Agree the `trade_signals` shape with the consuming team.** It's yours to
   define but it's their read contract — hand it over, don't assume.

**Open — required to SWITCH ON the position manager (later)**
6. Concrete `BinanceAdapter` + `IBKRAdapter` behind `ExecutionAdapter`
   (fill-attribution: attach `client_order_id` to `Fill` where the venue gives it).
7. Position-manager `main`: boot → (if safe) executor + exit-manager tick loop +
   fill stream, consuming per §5.
8. Seed `execution_caps` (per asset_class, per intent) with cross/cap spread
   multiples + escalation ladders. Hot-reloadable; tune in early live trading.

---

## 8. Operational concerns

- **Kill switch** (`kill_switch` table): checked before every entry. `halt_new`
  (default) stops opening, lets open positions run their exit rules; `flatten`
  stops opening AND closes everything. A DB row — trip it faster than a redeploy.
- **Boot reconcile** (PM process): connect → reconcile → diff DB vs venue.
  Un-explained discrepancies → `HALTED` + alert; orphan positions → manual
  (default) and refuse to start trading. Never auto-resolve what it can't explain.
- **Escalation ladder** (exits): a capped protective exit that doesn't fill within
  its window widens the cap rung by rung; the top rung **HALTs + alerts** rather
  than market-ing out at an unbounded price. Loud-not-silent strand.
- **Exit-references-a-position gap** (cross-team): an EXIT names a `position_key`
  the consumer should have open. If their view disagrees, the row carries enough
  to DETECT and log it — but how they HANDLE it is a coordination agreement, not
  something the table fixes.
- **Idempotency everywhere:** `signal_id` (signals), `client_order_id` (orders),
  `venue_exec_id` (fills), `(chain_id, side)` unique (trades). Replays and
  restarts dedupe at the DB, not just in memory.
- **State-machine sync obligation:** the chain machine is copied from CausaView,
  not shared as a library. Changes must be applied in both places — leave the
  breadcrumb comment at both copies.

---

## 9. First-signal checklist (condensed)

1. Create new Supabase project; apply `001→002→003→004` as `service_role`.
2. Confirm 5s/10s bar availability; set `ASSET_CONFIGS` for both ladders.
3. Extract `service_2` behind `ChainMachine`; provide a `MachineFactory`.
4. Wire the signal-process `main` (feeds → router → runtime → signal writer).
5. Agree `trade_signals` shape with the consuming team.
6. Run the signal process; watch `trade_signals` populate; watch `chains`/`signals`
   for audit. Position manager stays dark.
```
```
