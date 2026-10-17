# ADR-002: Kafka on the live path for latency

**Status:** Accepted (rejected as proposed; edge-aligned polling adopted as the latency lever when needed)
**Date:** 2026-10-17
**Deciders:** Repo owner

## Context

The proposal was to add Kafka functionality to reduce latency on the live
path. Kafka already carries the feed → signal hop (`trading-bars`) and the
audit-only `chain-events` topic. The signal → executor hop is `pg_notify` with
a persisted cursor. Reading the code, the live path for a `confirmed_15m`
ENTER (trigger: a chain propagating to 15m) spends its time here:

| Stage | Source | Latency |
|---|---|---|
| Candle settle | `SETTLE_MS = 60_000` (`coinbase_spot.py`) | 60 s, fixed |
| 15m poll cadence | `interval = min(max(tf_ms // 2000, 8), 600)` → 15m: 450 s, 1h: 600 s | 0–450 s, mean ~225 s |
| Kafka feed → signal | `linger_ms=20`, single local broker | ~20–50 ms |
| Per-symbol ordering | a 15m bar waits behind the symbol's 1m/3m/5m forecasts | ~8 s |
| ForecastGate permit wait | concurrency 6; top-of-hour burst (~70 forecasts) | 0 s normally, up to ~3 min at :00 |
| `forecast_path` | 2.75 s at N=6, 8 s at N=1 | 3–8 s |
| chain_init persist → strategy → `trade_signals` insert | Supabase over the network | tens of ms |
| `pg_notify` → `SignalListener` | push | ms |
| Order submit + fill attribution | REST + 2 s fill poll (D4) | ~2 s |

Roughly 1.5 to 12 minutes per entry, of which Kafka is under 0.1 s. Two
constraints bound the options: the executor deliberately depends on the
database only (README §1), and the chain machine and strategy share one
per-symbol lock and must apply a symbol's bars in order.

## Decision

Do not add Kafka to the live path. Kafka is a durability and decoupling tool
and already does that job on the one hop where decoupling matters. The
latency is structural (candle settle, fixed poll cadence, forecast capacity)
and lives where messaging cannot reach it.

## Options considered

- **A. Kafka replaces `pg_notify` for signal → executor.** No latency gain
  (`pg_notify` is already push); adds a broker dependency to the money
  process and a second source of truth. Rejected.
- **B. Kafka between chain machine and strategy.** Adds a hop and breaks the
  shared-lock invariant. Rejected.
- **C. Tune the existing hop** (`linger_ms=0`). ~20 ms. Harmless, irrelevant.
- **D. Edge-aligned polling** in `CoinbaseSpotFeed.start_live`: poll each
  timeframe at window close + `SETTLE_MS` + ε, periodic poll as backstop.
  Removes 0–450 s on 15m and 0–600 s on 1h without changing the series
  Sumtyme sees. The lever to pull when latency matters. **Deferred** by the
  owner: not a concern at present.
- **E. Lower `SETTLE_MS`** per product (measured revisions: median 12 s, p90
  25 s, max 36 s). A bar revised after emission is a different series; only
  with a revision-rate measurement at the new settle, and only after D.
- **F. Coinbase websocket, self-built 1m bars from trades.** Removes settle
  and poll, but warmup (REST candles) and live (trade-built) would define a
  step differently. Only alongside a same-source warmup.
- **G. Re-benchmark the gate past N=6** with the raised thread pool (D12).
- **H. Websocket user channel for fills** (D4).

## Consequences

- The system keeps one Kafka hop and the DB as the signal/executor contract.
- A tempting scale-out — two signal processes on the symbol-keyed partitions —
  is a trap: each would run its own `ForecastGate` at 6 and oversubscribe
  Sumtyme (many timeouts, zero errors). Scale-out needs a shared concurrency
  budget, not more consumers.
- The latency work that followed went elsewhere: the per-symbol serialisation
  of forecasts was the real ceiling for a dense ladder (PR 8), not messaging.

## Action items

1. [ ] D, when entry latency on the crypto ladder starts to matter.
2. [ ] G (D12) before adding symbols or rungs.
3. [ ] H as a separate ticket.
