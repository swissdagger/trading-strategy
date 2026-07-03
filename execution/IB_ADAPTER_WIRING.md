# IB execution adapter + master on/off

Live order execution via Interactive Brokers, with a master enable/disable that
no-ops safely when off.

## Pieces

1. **`execution/ibkr_adapter.py`** — `IBKRAdapter`, a concrete `ExecutionAdapter`
   over IB Gateway/TWS using **`ib_async`** (the maintained successor to the
   archived `ib_insync`). Serves equity + fx. Implements the full ABC: session
   lifecycle with trading-session verification, marketable-limit submit,
   cancel, native amend, fill streaming, and reconcile.

2. **`execution/trade_enable.py`** — `TradeEnableState`, the master on/off state.
   Config default + live DB override, refreshed via NOTIFY + poll.

3. **`execution/gated_adapter.py`** — `GatedAdapter`, wraps any adapter and
   suppresses order placement when disabled, passing reads through.

4. **`migrations/010_trade_enabled.sql`** — the `trade_enabled` singleton (like
   `kill_switch`) plus a change-NOTIFY trigger. Apply after 009.

5. **`config.TRADE_ENABLED_DEFAULT`** — the boot/fallback state (default
   `False`).

## Why ib_async, not ib_insync

`ib_insync` was archived (read-only) in March 2024 after its author's passing
and no longer receives TWS-API-compatibility fixes. `ib_async`
(`ib-api-reloaded/ib_async`) is the actively maintained drop-in successor. For a
system moving real money, building on the maintained library is the correct
call. The `ExecutionAdapter` ABC's docstrings still say "ib_insync" — that's
just prose; the dependency choice lives in the concrete adapter.

## The on/off design

The switch is a **wrapper**, not `if enabled` scattered through the IB code:

```
executor → GatedAdapter → IBKRAdapter → IB Gateway
                ↑
         TradeEnableState (config default + live DB row)
```

- **Off:** `submit_order` returns `REJECTED` with a `GATED_` reason and touches
  no venue; `cancel_order`/`amend_order` return benign terminal results.
  `reconcile`/`get_positions`/`connect`/`start_fill_stream` still work, so you
  can observe the venue with trading off.
- **On:** everything passes through to IB.

This keeps the dangerous venue code free of enable-logic and puts the safety
switch in one auditable place. It composes with `kill_switch`: the executor
checks `kill_switch`, the gate checks `trade_enabled`, and **both** must allow an
order. The two are independent — `trade_enabled` is the ordinary power switch,
`kill_switch` the emergency stop.

## Flipping the switch live

```sql
-- Enable trading (takes effect within ~1 refresh, via NOTIFY):
UPDATE trade_enabled SET enabled = true, reason = 'go-live', set_by = 'ops'
 WHERE id = true;

-- Disable instantly:
UPDATE trade_enabled SET enabled = false, reason = 'pause' WHERE id = true;
```

No redeploy — same reason `kill_switch` is a row. The default boots **disabled**;
going live is a deliberate act.

## Wiring (in the executor process, when built)

```python
enable = TradeEnableState(dsn, pool, config_default=config.TRADE_ENABLED_DEFAULT)
await enable.start()

ib = IBKRAdapter(host="127.0.0.1", port=4001, account=BOT_ACCOUNT)
adapter = GatedAdapter(ib, enable)          # <-- executor uses this

# boot sequence (from the ABC):
await adapter.connect()                      # confirms trading session
snapshot = await adapter.reconcile()         # diff against DB (executor)
await adapter.start_fill_stream(on_fill)     # async fills -> executor
# ... then submit_order/cancel/amend as trades open and close.
```

`on_fill` is the executor's fill handler (dedupe on `venue_exec_id`, write
`fills`, advance quantities). This adapter only reports.

## Testing note

Adapter mapping logic (order direction, fill mapping, contract build, status
map) and the gate are unit-tested without a gateway. `TradeEnableState` is
integration-tested against real Postgres. The parts that need a **live IB
paper session** — the connect handshake, real order round-trips, live fill
streaming — must be shaken out against IB paper (port 4002) before going live.
Start with `TRADE_ENABLED_DEFAULT=False` and flip the row on only after the
paper reconcile looks right.
```
