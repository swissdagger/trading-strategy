"""
write_path_probe.py — Deterministic end-to-end probe of the trade_signals write
path. Run from the repo root on the VM (needs the execution/, strategy/, and
producer/ packages importable, with the patched files deployed).

WHAT IT PROVES, in one run:

  PHASE 0  Preconditions: migration 011 applied (strategy column), migration
           009 applied (NOTIFY trigger present), and the connection role can
           INSERT into trade_signals (RLS/grants) — probed inside a rolled-back
           transaction so nothing fires or persists.
  PHASE 1  The DECISION path: synthetic chain events that legitimately satisfy
           the two-ladder entry gates are fed through the REAL
           MultiStrategyRuntime -> TwoLadderEntry -> SignalWriter ->
           make_insert_signal. Not a hand-inserted row: the real gates fire.
  PHASE 2  The DELIVERY path: the REAL SignalListener (dedicated LISTEN
           connection + catch-up sweep) receives both signals and invokes a
           collecting handler. A raw LISTEN connection independently confirms
           the pg_notify fired per-insert.
  PHASE 3  The EXIT path: an opposing Set B chain propagates to 2m, the
           runtime's exit sweep fires STOP on the tracked position, and the
           EXIT row + NOTIFY + dispatch are all verified.
  PHASE 4  Cleanup: probe rows are deleted (unless --keep-rows).

THE SCENARIO (two_ladder gates, verbatim from the strategy spec):
  1. Set B chain PRIME-1 initiates LONG               (step 1: prime exists)
  2. Set A chain TRIG-1 initiates LONG at level 1     (no trigger yet: < 2m)
  3. TRIG-1 propagates to level 5 (= 2m)              (step 2 trigger; step 2b
     no opposing chains anywhere; step 3 same-dir prime) => ENTER emitted,
     position keyed on PRIME-1:L, decision_price/trigger_level from this event.
  4. Set B chain OPP-1 initiates SHORT at level 1     (no exit yet: < B 2m)
  5. OPP-1 propagates to level 3 (= 2m in Set B)      => exit sweep fires STOP
     => EXIT emitted with this event's price/timestamp.

SAFETY — READ BEFORE RUNNING AGAINST A LIVE DB:
  pg_notify fans out to EVERY listener on the channel. If the production
  executor process is attached to this database, it WILL receive the probe's
  signals and attempt to handle them. Run with the executor stopped or in
  paper trade_mode, and use a symbol no venue adapter can trade (the default
  PROBE-TEST is deliberately unroutable). The probe refuses to run without
  --i-understand-live-listeners for exactly this reason.

Usage:
  TRADING_DB_URL=postgres://... python write_path_probe.py \
      --i-understand-live-listeners [--symbol PROBE-TEST] [--keep-rows]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg

from execution.signal_writer import SignalWriter, SignalWriterHooks
from execution.signal_listener import SignalListener
from producer.persistence import (
    make_insert_signal,
    make_load_open_positions,
)
from strategy.chain_view import ChainView
from strategy.multi_runtime import MultiStrategyRuntime, RuntimeHooks
from strategy.registry import Strategy, StrategyRegistry
from strategy.two_ladder_strategy import TwoLadderEntry, TwoLadderExit

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("write_path_probe")

STRATEGY_NAME = "two_ladder"          # matches the executor's routing default
NOTIFY_CHANNEL = "trade_signal"       # migration 009's channel


def _iso(base: datetime, seconds: int) -> str:
    """Deterministic, strictly-increasing ISO timestamps. Distinct decided_at
    per decision => distinct sig2 ids; identical across re-runs only if --base
    is pinned, which is exactly the replay-dedupe property being exercised."""
    return (base + timedelta(seconds=seconds)).isoformat()


def _event(*, set_: str, event_type: str, chain_id: str, symbol: str,
           direction: int, level: int, ts: str, price: float) -> dict:
    """A raw event in the exact shape the router's _tag publishes: 'set' and
    'ladder' both present (the runtime routes on either), level in metadata
    (from_raw's primary location) AND top-level (its fallback), decision_price
    included — the field whose absence once NULLed every row."""
    return {
        "set": set_,
        "ladder": set_,
        "event_type": event_type,
        "chain_id": chain_id,
        "symbol": symbol,
        "asset_class": "probe",
        "direction": direction,
        "propagation_level": level,
        "metadata": {"propagation_level": level},
        "timestamp": ts,
        "decision_price": price,
    }


# ---------------------------------------------------------------------------
# Phase 0 — preconditions (all failures here are DB-state, not code)
# ---------------------------------------------------------------------------

async def preflight(pool: asyncpg.Pool) -> None:
    cols = {
        r["column_name"]
        for r in await pool.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'trade_signals'"
        )
    }
    if not cols:
        raise SystemExit("PRECONDITION FAILED: trade_signals does not exist "
                         "(migrations 001-004 not applied?)")
    if "strategy" not in cols:
        raise SystemExit("PRECONDITION FAILED: trade_signals.strategy missing — "
                         "migration 011 not applied; every insert would fail")
    log.info("preflight OK: trade_signals columns present (%d cols)", len(cols))

    trig = await pool.fetchval(
        "SELECT COUNT(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
        "WHERE c.relname = 'trade_signals' AND NOT t.tgisinternal"
    )
    if not trig:
        raise SystemExit("PRECONDITION FAILED: no trigger on trade_signals — "
                         "migration 009 (NOTIFY) not applied; rows would land "
                         "but never push to listeners")
    log.info("preflight OK: NOTIFY trigger present")

    # Privilege probe INSIDE a rolled-back transaction: pg_notify only delivers
    # on COMMIT, so this exercises RLS/grants with zero side effects. A 42501
    # here is the deny-all RLS posture rejecting this role.
    sentinel = f"probe-priv.{uuid.uuid4().hex}"
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.execute(
                """
                INSERT INTO trade_signals
                  (signal_id, position_key, chain_id, direction, action, symbol,
                   asset_class, strategy, decided_at)
                VALUES ($1, 'p:L', 'p', 'LONG', 'ENTER', '_priv_probe',
                        'probe', $2, now())
                """,
                sentinel, STRATEGY_NAME,
            )
        except asyncpg.PostgresError as e:
            raise SystemExit(
                f"PRECONDITION FAILED: this role cannot INSERT into "
                f"trade_signals ({e.__class__.__name__}: {e}). Check RLS/role "
                f"— the writer's DSN must bypass RLS (service_role/owner)."
            )
        finally:
            await tr.rollback()
    log.info("preflight OK: INSERT privilege confirmed (rolled back, no notify)")


# ---------------------------------------------------------------------------
# Phase 1-3 — the traversal
# ---------------------------------------------------------------------------

async def run_probe(dsn: str, symbol: str, base_ts: datetime,
                    keep_rows: bool) -> int:
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4,
                                     statement_cache_size=0)
    failures: list[str] = []
    try:
        await preflight(pool)

        # -- independent NOTIFY witness: a raw LISTEN connection, so notify
        #    verification does not depend on the SignalListener implementation.
        notifies: list[str] = []
        raw_listen = await asyncpg.connect(dsn, statement_cache_size=0)
        await raw_listen.add_listener(
            NOTIFY_CHANNEL,
            lambda _c, _pid, _ch, payload: notifies.append(payload),
        )

        # -- the REAL delivery leg: SignalListener with a collecting handler.
        handled: dict[str, dict] = {}

        async def handler(row: dict) -> None:
            # Idempotent by construction (dict keyed on signal_id). Assert the
            # strategy column made it through the listener's SELECT (the bug
            # patched in signal_listener) rather than silently defaulting.
            if row.get("strategy") != STRATEGY_NAME:
                failures.append(
                    f"listener delivered row without strategy column: "
                    f"{row.get('signal_id')} -> {row.get('strategy')!r}")
            handled[row["signal_id"]] = row

        listener = SignalListener(
            dsn=dsn, pool=pool, handler=handler,
            initial_cursor=datetime.now(timezone.utc) - timedelta(seconds=5),
        )
        listener_task = asyncio.create_task(listener.run())

        # -- the REAL decision stack, wired exactly as signal_process wires it:
        #    real insert_signal, real writer, real components, real runtime.
        insert_signal = make_insert_signal(pool)
        writer = SignalWriter(
            SignalWriterHooks(insert_signal=insert_signal),
            strategy=STRATEGY_NAME,
        )
        view = ChainView()
        registry = StrategyRegistry()
        registry.register_factory(
            STRATEGY_NAME,
            lambda name, symbols, ladders, params, **_: Strategy(
                name=name,
                entry=TwoLadderEntry(view, writer),
                exit=TwoLadderExit(view, writer),
                symbols=symbols, ladders=ladders,
            ),
        )
        registry.build_from_config(
            {STRATEGY_NAME: {"enabled": True, "symbols": [symbol],
                             "ladders": ["A", "B"]}},
            all_symbols=[symbol],
        )

        async def _noop(*_a, **_k):  # save/remove: durable via trade_signals,
            return None              # same no-op contract as production

        runtime = MultiStrategyRuntime(
            view=view, registry=registry,
            hooks=RuntimeHooks(
                load_open_positions=make_load_open_positions(pool),
                save_open_position=_noop,
                remove_open_position=_noop,
            ),
        )
        await runtime.start()

        prime, trig, opp = (f"PRIME-{uuid.uuid4().hex[:8]}",
                            f"TRIG-{uuid.uuid4().hex[:8]}",
                            f"OPP-{uuid.uuid4().hex[:8]}")

        # PHASE 1: the entry scenario, one event at a time, asserting the gate
        # does NOT fire early (a probe that only checks the happy end can pass
        # while the gates are broken open).
        async def count_rows() -> int:
            return await pool.fetchval(
                "SELECT COUNT(*) FROM trade_signals WHERE symbol = $1", symbol)

        await runtime.on_raw_event(_event(
            set_="B", event_type="chain_initiated", chain_id=prime,
            symbol=symbol, direction=1, level=1, ts=_iso(base_ts, 0),
            price=100.00))
        await runtime.on_raw_event(_event(
            set_="A", event_type="chain_initiated", chain_id=trig,
            symbol=symbol, direction=1, level=1, ts=_iso(base_ts, 10),
            price=100.10))
        if await count_rows() != 0:
            failures.append("gate fired EARLY: a row exists before the 2m trigger")

        await runtime.on_raw_event(_event(
            set_="A", event_type="chain_propagated", chain_id=trig,
            symbol=symbol, direction=1, level=5, ts=_iso(base_ts, 20),
            price=100.25))   # Set A 2m => ENTER

        enter = await pool.fetchrow(
            "SELECT * FROM trade_signals WHERE symbol = $1 AND action = 'ENTER'",
            symbol)
        if enter is None:
            failures.append("ENTER row missing after a fully-gated trigger")
        else:
            expect = {
                "strategy": STRATEGY_NAME,
                "position_key": f"{prime}:L",
                "chain_id": prime,
                "direction": "LONG",
                "trigger_level": 5,
                "decision_price": 100.25,
            }
            for k, want in expect.items():
                got = enter[k]
                if (float(got) if k == "decision_price" else got) != want:
                    failures.append(f"ENTER.{k}: expected {want!r}, got {got!r}")
            if not enter["signal_id"].startswith("sig2."):
                failures.append(f"ENTER id not sig2 scheme: {enter['signal_id']}")
            log.info("PHASE 1 OK: ENTER %s written (pkey=%s price=%s)",
                     enter["signal_id"], enter["position_key"],
                     enter["decision_price"])

        # PHASE 1b: replay idempotency, live in the real path. Re-fire the
        # IDENTICAL trigger event (same decided_at): the sig2 id is identical,
        # ON CONFLICT swallows it, the runtime's tracking check prevents
        # double-tracking, and NO second ENTER row may appear. This is the
        # at-least-once Kafka contract the id scheme exists to satisfy.
        await runtime.on_raw_event(_event(
            set_="A", event_type="chain_propagated", chain_id=trig,
            symbol=symbol, direction=1, level=5, ts=_iso(base_ts, 20),
            price=100.25))
        n_after_replay = await pool.fetchval(
            "SELECT COUNT(*) FROM trade_signals WHERE symbol = $1 "
            "AND action = 'ENTER'", symbol)
        if n_after_replay != 1:
            failures.append(f"replay dedupe broken: {n_after_replay} ENTER rows "
                            f"after re-firing the identical trigger")
        else:
            log.info("PHASE 1b OK: identical replayed trigger deduped "
                     "(still exactly 1 ENTER row)")

        # PHASE 3: the exit scenario.
        await runtime.on_raw_event(_event(
            set_="B", event_type="chain_initiated", chain_id=opp,
            symbol=symbol, direction=-1, level=1, ts=_iso(base_ts, 30),
            price=100.15))
        if await count_rows() != 1:
            failures.append("exit fired EARLY: opposing chain below Set-B 2m")

        await runtime.on_raw_event(_event(
            set_="B", event_type="chain_propagated", chain_id=opp,
            symbol=symbol, direction=-1, level=3, ts=_iso(base_ts, 40),
            price=99.80))    # opposing Set B 2m => STOP

        exit_row = await pool.fetchrow(
            "SELECT * FROM trade_signals WHERE symbol = $1 AND action = 'EXIT'",
            symbol)
        if exit_row is None:
            failures.append("EXIT row missing after opposing Set B chain hit 2m")
        else:
            if exit_row["exit_reason"] != "STOP":
                failures.append(f"EXIT reason: expected STOP, got "
                                f"{exit_row['exit_reason']!r}")
            if float(exit_row["decision_price"]) != 99.80:
                failures.append(f"EXIT decision_price: expected 99.80, got "
                                f"{exit_row['decision_price']!r} (the sweep "
                                f"price plumbing regressed)")
            log.info("PHASE 3 OK: EXIT %s written (reason=%s price=%s)",
                     exit_row["signal_id"], exit_row["exit_reason"],
                     exit_row["decision_price"])

        # PHASE 2: delivery. Give the notify fan-out and the listener a moment.
        expected_ids = {r["signal_id"] for r in (enter, exit_row) if r}
        for _ in range(40):                      # up to ~4s
            if expected_ids <= set(notifies) and expected_ids <= set(handled):
                break
            await asyncio.sleep(0.1)
        missing_notify = expected_ids - set(notifies)
        missing_handled = expected_ids - set(handled)
        if missing_notify:
            failures.append(f"pg_notify never observed for: {missing_notify} "
                            f"(trigger present but not firing?)")
        else:
            log.info("PHASE 2 OK: pg_notify observed for both signals")
        if missing_handled:
            failures.append(f"SignalListener never dispatched: {missing_handled}")
        else:
            log.info("PHASE 2 OK: SignalListener dispatched both to the handler "
                     "(strategy column intact)")

        # Shutdown ordering matters: cancel the run task and REAP it before
        # stop() closes the dedicated connection, otherwise the drain loop
        # observes its socket dying and logs a spurious reconnect error.
        listener_task.cancel()
        try:
            await listener_task
        except (asyncio.CancelledError, Exception):
            pass
        await listener.stop()
        await raw_listen.close()

        # PHASE 4: cleanup.
        if keep_rows:
            log.info("PHASE 4: --keep-rows set; probe rows retained for "
                     "inspection (symbol=%s)", symbol)
        else:
            n = await pool.execute(
                "DELETE FROM trade_signals WHERE symbol = $1", symbol)
            log.info("PHASE 4: cleanup — %s", n)

        if failures:
            print("\n=== WRITE-PATH PROBE: FAIL ===")
            for f in failures:
                print(f"  ✗ {f}")
            return 1
        print("\n=== WRITE-PATH PROBE: PASS ===")
        print("  decision gates -> insert -> commit -> NOTIFY -> listener -> "
              "handler: all verified")
        print("  If the production executor was attached in paper mode, its log "
              "should show the same two signal_ids.")
        return 0
    finally:
        await pool.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--symbol", default="PROBE-TEST",
                    help="synthetic symbol; must not be a real tracked symbol")
    ap.add_argument("--keep-rows", action="store_true",
                    help="retain the probe's trade_signals rows for inspection")
    ap.add_argument("--base", default=None,
                    help="ISO base timestamp for decided_at values; pin it to "
                         "test sig2 replay-dedupe (a re-run with the same base "
                         "and --keep-rows must add ZERO new rows)")
    ap.add_argument("--i-understand-live-listeners", action="store_true",
                    help="required: acknowledges that any live executor on this "
                         "DB will receive the probe's NOTIFYs")
    args = ap.parse_args()

    dsn = os.environ.get("TRADING_DB_URL")
    if not dsn:
        sys.exit("TRADING_DB_URL is not set (use the exact DSN the signal "
                 "process uses — that is the point of the probe)")
    if not args.i_understand_live_listeners:
        sys.exit("Refusing to run: pg_notify fans out to every attached "
                 "listener, including a live executor. Stop it or put it in "
                 "paper mode, then re-run with --i-understand-live-listeners.")

    base_ts = (datetime.fromisoformat(args.base) if args.base
               else datetime.now(timezone.utc).replace(microsecond=0))
    rc = asyncio.run(run_probe(dsn, args.symbol, base_ts, args.keep_rows))
    sys.exit(rc)


if __name__ == "__main__":
    main()