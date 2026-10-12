"""
write_path_probe_fc.py — Deterministic end-to-end probe of the trade_signals
write path for the CONFIRMED_15M strategy.

Companion to write_path_probe.py (which covers two_ladder). confirmed_15m has a
completely different gate — a start-ordered initiation+confirmation pattern
across the [1m,3m,5m,15m,1h] ladder — so this builds a synthetic chain sequence
that legitimately satisfies it, driven through the REAL make_confirmed_15m factory,
the REAL Confirmed15mEntry/Exit, the REAL MultiStrategyRuntime, writer, insert, and
SignalListener. Nothing is hand-inserted; the actual gate fires.

Why a separate confirmed_15m probe matters (beyond "test the other strategy"):
  * It exercises Confirmed15mExitAdapter.maybe_emit_exit, whose MISSING
    decision_price parameter made every confirmed_15m exit raise TypeError — the
    patched adapter must accept and forward it. The two_ladder probe cannot
    catch this; only the confirmed_15m exit path does.
  * It exercises the strategy column round-trip under a NON-default strategy
    name ("confirmed_15m"): the listener SELECT must carry it, and the handler must
    NOT fall back to "two_ladder". A regression here silently mislabels every
    confirmed_15m trade.

THE PATTERN (verbatim from confirmed_15m_strategy's prose spec), by chain-START
time (first_seen == 1m initiation on this ladder):

  t=0   PRIME (dir +1) initiates, propagates up to 15m (level 4)
  t=10  FAIL1 (dir -1) initiates at 1m, terminates at level 1 (fails < 3m)
  t=20  INIT  (dir +1) initiates, propagates up to 15m (level 4)
  t=30  FAIL2 (dir -1) initiates at 1m, terminates at level 1 (distinct failure)
  t=40  OPEN  (dir +1) initiates, stays OPEN (the independent same-dir
              corroborator; must be a chain OTHER than the confirmer)
  t=50  CONF  (dir +1) initiates, propagates to 3m (level 2) -> TRIGGER
        conf.start (t=50) >= prime.15m and init.15m (both reached before t=50),
        and a distinct opposing failure exists in each of the two start windows
        [prime.start, init.start] and [init.start, conf.start] -> ENTER.

  EXIT: an opposing chain reaches 5m (level 3) -> STOP.

SAFETY: identical to the two_ladder probe — pg_notify fans out to every
listener; run with the executor stopped or in paper trade_mode, and keep the
unroutable default symbol. Requires --i-understand-live-listeners.

Usage:
  TRADING_DB_URL=postgres://... python write_path_probe_fc.py \
      --i-understand-live-listeners [--symbol PROBE-FC] [--keep-rows]
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
from producer.persistence import make_insert_signal, make_load_open_positions
from strategy.chain_view import ChainView
from strategy.ladder_registry import LadderRegistry
from strategy.multi_runtime import MultiStrategyRuntime, RuntimeHooks
from strategy.registry import StrategyRegistry
from strategy.confirmed_15m_adapter import make_confirmed_15m

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger("write_path_probe_fc")

STRATEGY_NAME = "confirmed_15m"
FC_LADDER = ["1m", "3m", "5m", "15m", "1h"]   # levels 1..5
NOTIFY_CHANNEL = "trade_signal"


def _iso(base: datetime, seconds: int) -> str:
    return (base + timedelta(seconds=seconds)).isoformat()


def _event(*, chain_id: str, event_type: str, symbol: str, direction: int,
           level: int, ts: str, price: float) -> dict:
    """Raw event in the router's published shape. confirmed_15m reads via
    _RawEventView (raw dict), ignoring the A/B 'set' tag, but the runtime routes
    on 'ladder'; we stamp ladder='FC' so this event is fanned to confirmed_15m and
    is NOT folded into the two-ladder ChainView."""
    return {
        "ladder": "FC",
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


async def preflight(pool: asyncpg.Pool) -> None:
    cols = {r["column_name"] for r in await pool.fetch(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'trade_signals'")}
    if "strategy" not in cols:
        raise SystemExit("PRECONDITION FAILED: trade_signals.strategy missing "
                         "(migration 011) — confirmed_15m rows would fail to insert")
    trig = await pool.fetchval(
        "SELECT COUNT(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
        "WHERE c.relname = 'trade_signals' AND NOT t.tgisinternal")
    if not trig:
        raise SystemExit("PRECONDITION FAILED: NOTIFY trigger absent (migration 009)")
    log.info("preflight OK: strategy column + NOTIFY trigger present")


async def run_probe(dsn: str, symbol: str, base_ts: datetime,
                    keep_rows: bool) -> int:
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4,
                                     statement_cache_size=0)
    failures: list[str] = []
    try:
        await preflight(pool)

        notifies: list[str] = []
        raw_listen = await asyncpg.connect(dsn, statement_cache_size=0)
        await raw_listen.add_listener(
            NOTIFY_CHANNEL,
            lambda _c, _p, _ch, payload: notifies.append(payload))

        handled: dict[str, dict] = {}

        async def handler(row: dict) -> None:
            # The confirmed_15m-specific assertion: the listener must carry the
            # strategy column, and it must read "confirmed_15m", NOT the
            # "two_ladder" fallback. This is the bug the two_ladder probe can't see.
            if row.get("strategy") != STRATEGY_NAME:
                failures.append(
                    f"strategy column wrong/absent through listener: "
                    f"{row.get('signal_id')} -> {row.get('strategy')!r} "
                    f"(expected {STRATEGY_NAME!r})")
            handled[row["signal_id"]] = row

        listener = SignalListener(
            dsn=dsn, pool=pool, handler=handler,
            initial_cursor=datetime.now(timezone.utc) - timedelta(seconds=5))
        listener_task = asyncio.create_task(listener.run())

        insert_signal = make_insert_signal(pool)

        def make_writer(strategy_name: str) -> SignalWriter:
            return SignalWriter(
                SignalWriterHooks(insert_signal=insert_signal),
                strategy=strategy_name)

        # Real FC ladder + factory wiring, exactly as signal_process builds it.
        ladders = LadderRegistry()
        ladders.register("FC", FC_LADDER)
        registry = StrategyRegistry()
        ladder_registry = ladders   # bind for the closure (factory kwarg is also 'ladders')

        def _fc_factory(*, name, symbols, ladders, params):
            return make_confirmed_15m(
                name=name, symbols=symbols, ladders=ladders, params=params,
                view_registry=ladder_registry, writer_factory=make_writer)

        registry.register_factory(STRATEGY_NAME, _fc_factory)
        registry.build_from_config(
            {STRATEGY_NAME: {"enabled": True, "symbols": [symbol],
                             "ladders": ["FC"]}},
            all_symbols=[symbol])

        async def _noop(*_a, **_k):
            return None

        # No two-ladder ChainView needed (FC uses its own history), but the
        # runtime requires a view object; pass an empty one — FC events carry
        # ladder='FC' so they are never folded into it.
        runtime = MultiStrategyRuntime(
            view=ChainView(), registry=registry,
            hooks=RuntimeHooks(
                load_open_positions=make_load_open_positions(pool),
                save_open_position=_noop, remove_open_position=_noop))
        await runtime.start()

        sfx = uuid.uuid4().hex[:6]
        PRIME, INIT, CONF = f"PRIME-{sfx}", f"INIT-{sfx}", f"CONF-{sfx}"
        FAIL1, FAIL2, OPEN = f"FAIL1-{sfx}", f"FAIL2-{sfx}", f"OPEN-{sfx}"
        d = 1

        async def feed(chain_id, event_type, level, t, price):
            await runtime.on_raw_event(_event(
                chain_id=chain_id, event_type=event_type, symbol=symbol,
                direction=(d if chain_id in (PRIME, INIT, CONF, OPEN) else -d),
                level=level, ts=_iso(base_ts, t), price=price))

        async def count(action):
            return await pool.fetchval(
                "SELECT COUNT(*) FROM trade_signals WHERE symbol=$1 AND action=$2",
                symbol, action)

        # --- Build the pattern. Each chain STARTS (initiate, level 1) at its
        #     window time, then propagates. first_seen == the initiate ts.
        # PRIME starts t=0, climbs to 15m by t=5.
        await feed(PRIME, "chain_initiated", 1, 0, 100.0)
        await feed(PRIME, "chain_propagated", 4, 5, 100.4)      # 15m
        # FAIL1 (opposing) starts t=10, terminates at level 1 (never reaches 3m).
        await feed(FAIL1, "chain_initiated", 1, 10, 100.3)
        await feed(FAIL1, "chain_terminated", 1, 11, 100.2)
        # INIT starts t=20, climbs to 15m by t=25.
        await feed(INIT, "chain_initiated", 1, 20, 100.5)
        await feed(INIT, "chain_propagated", 4, 25, 100.8)      # 15m
        # FAIL2 (opposing, distinct) starts t=30, terminates at level 1.
        await feed(FAIL2, "chain_initiated", 1, 30, 100.6)
        await feed(FAIL2, "chain_terminated", 1, 31, 100.5)
        # OPEN (same dir, independent corroborator) starts t=40, stays open.
        await feed(OPEN, "chain_initiated", 1, 40, 100.7)
        # No ENTER may exist yet — the confirmer hasn't triggered.
        if await count("ENTER") != 0:
            failures.append("gate fired EARLY: ENTER before the confirmer's 3m")
        # CONF starts t=50, reaches 3m at t=55 -> TRIGGER.
        await feed(CONF, "chain_initiated", 1, 50, 100.9)
        await feed(CONF, "chain_propagated", 2, 55, 101.25)     # 3m => ENTER

        enter = await pool.fetchrow(
            "SELECT * FROM trade_signals WHERE symbol=$1 AND action='ENTER'",
            symbol)
        if enter is None:
            failures.append("ENTER missing after a fully-formed confirmed_15m pattern")
        else:
            checks = {
                "strategy": STRATEGY_NAME,
                "position_key": f"{CONF}:L",   # prime == confirmer chain
                "chain_id": CONF,
                "direction": "LONG",
                "trigger_level": 2,
                "decision_price": 101.25,
            }
            for k, want in checks.items():
                got = enter[k]
                if (float(got) if k == "decision_price" else got) != want:
                    failures.append(f"ENTER.{k}: expected {want!r}, got {got!r}")
            log.info("PHASE 1 OK: confirmed_15m ENTER %s (pkey=%s price=%s)",
                     enter["signal_id"], enter["position_key"],
                     enter["decision_price"])

        # --- EXIT: an opposing chain reaches 5m (level 3) -> STOP. This is the
        #     path that exercises the patched exit adapter's decision_price.
        STOPPER = f"STOP-{sfx}"
        await runtime.on_raw_event(_event(
            chain_id=STOPPER, event_type="chain_initiated", symbol=symbol,
            direction=-d, level=1, ts=_iso(base_ts, 60), price=101.0))
        if await count("EXIT") != 0:
            failures.append("exit fired EARLY: opposing chain below 5m")
        await runtime.on_raw_event(_event(
            chain_id=STOPPER, event_type="chain_propagated", symbol=symbol,
            direction=-d, level=3, ts=_iso(base_ts, 65), price=100.5))  # 5m => STOP

        exit_row = await pool.fetchrow(
            "SELECT * FROM trade_signals WHERE symbol=$1 AND action='EXIT'",
            symbol)
        if exit_row is None:
            failures.append(
                "EXIT missing after opposing chain hit 5m — the confirmed_15m exit "
                "adapter likely rejected decision_price (the patched bug)")
        else:
            if exit_row["exit_reason"] != "STOP":
                failures.append(f"EXIT reason: expected STOP, got {exit_row['exit_reason']!r}")
            if float(exit_row["decision_price"]) != 100.5:
                failures.append(f"EXIT decision_price: expected 100.5, got "
                                f"{exit_row['decision_price']!r}")
            if exit_row["strategy"] != STRATEGY_NAME:
                failures.append(f"EXIT.strategy: expected confirmed_15m, got "
                                f"{exit_row['strategy']!r}")
            log.info("PHASE 3 OK: confirmed_15m EXIT %s (reason=%s price=%s)",
                     exit_row["signal_id"], exit_row["exit_reason"],
                     exit_row["decision_price"])

        # --- Delivery.
        expected = {r["signal_id"] for r in (enter, exit_row) if r}
        for _ in range(40):
            if expected <= set(notifies) and expected <= set(handled):
                break
            await asyncio.sleep(0.1)
        if expected - set(notifies):
            failures.append(f"pg_notify missing for: {expected - set(notifies)}")
        else:
            log.info("PHASE 2 OK: pg_notify observed for both confirmed_15m signals")
        if expected - set(handled):
            failures.append(f"listener never dispatched: {expected - set(handled)}")
        else:
            log.info("PHASE 2 OK: SignalListener dispatched both "
                     "(strategy column = confirmed_15m, not the two_ladder fallback)")

        listener_task.cancel()
        try:
            await listener_task
        except (asyncio.CancelledError, Exception):
            pass
        await listener.stop()
        await raw_listen.close()

        if keep_rows:
            log.info("PHASE 4: --keep-rows; retaining probe rows (symbol=%s)", symbol)
        else:
            log.info("PHASE 4: cleanup — %s",
                     await pool.execute(
                         "DELETE FROM trade_signals WHERE symbol=$1", symbol))

        if failures:
            print("\n=== CONFIRMED_15M WRITE-PATH PROBE: FAIL ===")
            for f in failures:
                print(f"  ✗ {f}")
            return 1
        print("\n=== CONFIRMED_15M WRITE-PATH PROBE: PASS ===")
        print("  pattern gates -> insert -> commit -> NOTIFY -> listener -> "
              "handler: all verified (incl. exit adapter decision_price + "
              "strategy=confirmed_15m round-trip)")
        return 0
    finally:
        await pool.close()


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--symbol", default="PROBE-FC",
                    help="synthetic symbol; must not be a real tracked symbol")
    ap.add_argument("--keep-rows", action="store_true")
    ap.add_argument("--base", default=None)
    ap.add_argument("--i-understand-live-listeners", action="store_true")
    args = ap.parse_args()

    dsn = os.environ.get("TRADING_DB_URL")
    if not dsn:
        sys.exit("TRADING_DB_URL is not set (use the exact DSN the signal "
                 "process uses)")
    if not args.i_understand_live_listeners:
        sys.exit("Refusing to run: pg_notify reaches every attached listener, "
                 "including a live executor. Stop it or use paper mode, then "
                 "re-run with --i-understand-live-listeners.")
    base_ts = (datetime.fromisoformat(args.base) if args.base
               else datetime.now(timezone.utc).replace(microsecond=0))
    sys.exit(asyncio.run(run_probe(dsn, args.symbol, base_ts, args.keep_rows)))


if __name__ == "__main__":
    main()