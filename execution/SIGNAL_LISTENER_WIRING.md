# Signal → executor hop (LISTEN/NOTIFY)

Low-latency, guaranteed delivery of `trade_signals` rows to a consumer (the IB
executor, later). Two layers: `NOTIFY` for speed, a cursor sweep for the
guarantee.

## Pieces

1. **`migrations/009_signal_notify.sql`** — an `AFTER INSERT` trigger on
   `trade_signals` that fires `pg_notify('trade_signal', signal_id)` in the
   insert's own transaction. Apply it after 008.

2. **`execution/signal_listener.py`** — `SignalListener`: holds a dedicated
   `LISTEN` connection, dispatches each notified `signal_id` to a handler, and
   runs a `created_at`-cursor catch-up sweep on every (re)connect.

## Producer side needs NO change

Because the `NOTIFY` lives in a database trigger, the existing writer path is
untouched:

```python
# signal_process.py — unchanged:
writer = SignalWriter(SignalWriterHooks(insert_signal=make_insert_signal(self.pool)))
```

Every insert into `trade_signals` — from this writer, a future position
manager, or a manual backfill — fires the notification automatically. The
trigger makes delivery a property of the table, not of any one caller.

Critically, the writer's idempotent `INSERT ... ON CONFLICT (signal_id) DO
NOTHING` interacts correctly with the trigger: a replayed emit that hits the
conflict inserts no row, so the `AFTER INSERT` trigger does **not** fire — a
duplicate emit never re-notifies the executor. (Verified in
`test_replayed_insert_does_not_renotify` against real Postgres.)

## Consumer side wiring (in the executor process, when built)

```python
listener = SignalListener(
    dsn=TRADING_DB_URL,          # same DSN the rest of the system uses
    pool=pool,                   # shared pool, for catch-up SELECTs only
    handler=handle_signal,       # your idempotent per-signal callback
    initial_cursor=resume_cursor # persisted created_at, or None for 'now'
)
asyncio.create_task(listener.run())   # runs until cancelled
```

`handle_signal(row: dict)` receives the full `trade_signals` row and does the
IB work. **It must be idempotent on `row['signal_id']`** — the listener
guarantees at-least-once delivery (a signal can arrive via both the fast path
and an overlapping sweep). This is the same discipline the cross-team contract
already requires; the deterministic `signal_id` exists precisely so a re-read
dedupes.

## Delivery guarantee

- **Fast path:** insert → trigger `NOTIFY` (same txn) → listener pushed the
  `signal_id` → point-lookup the row → dispatch. Sub-millisecond wake on a
  colocated DB.
- **Backstop:** `NOTIFY` is best-effort — a notification fired while the
  listener is disconnected is lost by Postgres. On reconnect, the catch-up
  sweep reads every row with `created_at >= cursor` and dispatches it. So a
  signal committed during any disconnect is still delivered, just not as fast.
- Connection drops are detected near-instantly via asyncpg's termination
  callback (not a slow poll), so reconnect + catch-up start promptly.

## Cursor / restart

`listener.cursor` is the `created_at` high-water of dispatched signals. Persist
it (e.g. periodically, or on shutdown) and pass it back as `initial_cursor` to
resume without replaying. `None` starts from 'now' (act only on signals from
startup forward); a tz-aware `datetime.min` replays the whole table.

## Latency vs. "fire to IB directly"

This path deliberately routes execution through the durable row rather than
calling IB from the decision point. The cost is one colocated-DB round-trip
(single-digit ms); the benefit is that every order IB receives corresponds to a
durable, audited, replayable signal — the `NOTIFY` cannot fire for a row that
didn't commit, and the row cannot commit without the `NOTIFY`. No dual-write
divergence, and a crashed executor resumes from the durable rows via its
cursor. See the discussion in the design notes.
