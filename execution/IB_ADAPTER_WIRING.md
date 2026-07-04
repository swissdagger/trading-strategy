# IB execution adapter + master mode switch (off / paper / live)

Live order execution via Interactive Brokers, with a three-state master switch:
place no orders (off), place orders against the paper venue (paper, fake money),
or against the live venue (live, real money).

## Pieces

1. **`execution/ibkr_adapter.py`** — `IBKRAdapter` on **`ib_async`** (the
   maintained successor to the archived `ib_insync`). Serves equity + fx.
   Endpoint-agnostic: it takes host/port/account, so the SAME class connects to
   paper or live depending on the params it's given.
2. **`execution/trade_enable.py`** — `TradeEnableState`, the three-state mode
   (off/paper/live), config default + live DB override, NOTIFY + poll refresh,
   plus an `on_mode_change` reconnect hook.
3. **`execution/adapter_factory.py`** — builds the adapter for a mode, picking
   the paper vs live endpoint. LIVE -> live endpoint; PAPER and OFF -> paper
   endpoint (OFF never builds live).
4. **`execution/gated_adapter.py`** — no-ops orders when `is_enabled()` is false
   (i.e. mode == off); reads pass through.
5. **`migrations/010_trade_enabled.sql`** + **`012_trade_mode.sql`** — the
   singleton, the `mode` column, and the sync/notify triggers.
6. **`config.TRADE_MODE_DEFAULT`** + **`config.VENUE_ENDPOINTS`**.

## Why ib_async, not ib_insync

`ib_insync` was archived (read-only) in March 2024 after its author's passing
and no longer gets TWS-API fixes. `ib_async` is the maintained successor. The
ABC's docstrings say "ib_insync" — prose only; the dependency lives in the
concrete adapter.

## The three-state model

```
executor -> GatedAdapter -> IBKRAdapter -> IB Gateway (paper 4002 | live 4001)
                 ^                ^
         TradeEnableState   adapter_factory picks the endpoint by mode
```

- **off:** `submit_order` returns a `GATED_` reject, no venue contact. Reads
  still work.
- **paper:** orders go to the paper gateway (4002) + paper account — real order
  flow, real fills/reconcile, FAKE money. The shakeout mode.
- **live:** orders go to the live gateway (4001) — real money.

Both paper and live are "enabled" (`is_enabled()` true); only off gates orders.
This composes with `kill_switch`: executor checks kill_switch, the gate checks
mode != off, and BOTH must allow an order.

## Paper<->live is a RECONNECT, not just a flag

The venue is chosen at `connect()` time. Switching between paper and live
changes which gateway the adapter is connected to, so it needs a reconnect — not
a pure in-memory flip. `TradeEnableState` fires `on_mode_change(old, new)`; wire
it to rebuild + reconnect the adapter when the endpoint kind changes:

```python
from execution.adapter_factory import build_ibkr_adapter, endpoint_kind_for_mode

current = {"adapter": None}

async def on_mode_change(old, new):
    if endpoint_kind_for_mode(old) == endpoint_kind_for_mode(new):
        return  # off<->paper: same (paper) venue, no rebuild
    # venue changed (to/from live): rebuild + reconnect, gating orders in the gap
    new_raw, kind = build_ibkr_adapter(config.VENUE_ENDPOINTS["ibkr"], new)
    await new_raw.connect()
    current["adapter"] = new_raw   # swap; executor uses the gated wrapper over this

mode = TradeEnableState(dsn, pool,
    config_default=config.TRADE_MODE_DEFAULT, on_mode_change=on_mode_change)
await mode.start()

raw, kind = build_ibkr_adapter(config.VENUE_ENDPOINTS["ibkr"], mode.mode())
current["adapter"] = raw
adapter = GatedAdapter(raw, mode)     # executor uses this
```

`off<->paper` needs no rebuild (both paper). Anything to/from `live` rebuilds.
During the swap, the GatedAdapter keeps gating on `is_enabled()`, and the app
should quiesce order submission until the new connection is confirmed so no
order goes to the wrong venue in the gap.

## Flipping the switch live

```sql
-- Go to paper (shakeout):
UPDATE trade_enabled SET mode='paper', reason='shakeout', set_by='ops' WHERE id=true;
-- Go live (real money):
UPDATE trade_enabled SET mode='live', reason='go-live' WHERE id=true;
-- Stop:
UPDATE trade_enabled SET mode='off', reason='pause' WHERE id=true;
```

`enabled` is kept consistent automatically (a trigger derives it from mode), so
any legacy reader of `enabled` still works. Boots **off**; going to paper or
live is deliberate.

## Testing note

Adapter mapping/gating and the mode logic (including the reconnect callback) are
tested — the latter against real Postgres. The parts needing a live IB session
(connect handshake, order round-trips, live fills) must be shaken out against IB
**paper** (4002) first — which is exactly what `mode='paper'` is for. Boot with
`TRADE_MODE_DEFAULT='off'`, flip to `paper`, watch it, then `live`.