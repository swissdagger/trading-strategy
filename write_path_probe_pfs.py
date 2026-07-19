"""
write_path_probe_pfs.py — synthetic-event probe for prior_failure_scaleout.

Drives synthetic chain events through the REAL gate, view, exit logic, and
SignalWriter (with an in-memory insert hook standing in for the DB, honoring
ON CONFLICT semantics via signal_id), mimicking the runtime's contract:

  * every event folds via the entry adapter, then the exit sweep runs for
    each tracked position (fold-first, sweep-after — multi_runtime's order);
  * a position is UNTRACKED exactly when maybe_emit_exit returns its key.

Covers the spec's §8 cases 1-8 plus five hardening cases. Run from the repo
root:  python -m scripts.write_path_probe_pfs   (or python write_path_probe_pfs.py
with the repo on PYTHONPATH). Exits non-zero on any failure.

No network, no DB, no Sumtyme — pure write-path logic.
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from execution.signal_writer import SignalWriter, SignalWriterHooks
from strategy.ladder_registry import LadderRegistry
from strategy.prior_failure_adapter import make_prior_failure_scaleout
from strategy.registry import namespaced_position_key  # noqa: F401 (parity w/ runtime)

PF_TIMEFRAMES = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h"]
SCHEDULE = {1: 0.05, 2: 0.05, 3: 0.10, 4: 0.10, 5: 0.15, 6: 0.20, 7: 0.35}
T0 = datetime(2026, 7, 19, 12, 0, 0, tzinfo=timezone.utc)


def iso(minutes: float) -> str:
    return (T0 + timedelta(minutes=minutes)).isoformat()


class Harness:
    """One fresh strategy instance + in-memory signal store + the runtime's
    fold/sweep/untrack contract, per test case."""

    def __init__(self, schedule=None, window_hours=6):
        self.rows: list[dict] = []
        self._ids: set[str] = set()

        async def insert(row: dict) -> bool:
            # Honour the DB's ON CONFLICT (signal_id) DO NOTHING.
            if row["signal_id"] in self._ids:
                return False
            self._ids.add(row["signal_id"])
            self.rows.append(dict(row))
            return True

        registry = LadderRegistry()
        registry.register("PF", PF_TIMEFRAMES)
        self.strategy = make_prior_failure_scaleout(
            name="prior_failure_scaleout",
            symbols=["BTCUSDT"],
            ladders=["PF"],
            params={
                "entry_level": 2,
                "gate": {"failure_window_hours": window_hours,
                         "failure_exact_depth": 1},
                "exit": {"schedule": schedule or SCHEDULE,
                         "remainder_at_termination": True, "stop": None},
            },
            view_registry=registry,
            writer_factory=lambda name: SignalWriter(
                SignalWriterHooks(insert_signal=insert), strategy=name),
        )
        self.tracked: dict[str, SimpleNamespace] = {}   # pkey -> position

    async def event(self, chain_id: str, event_type: str, direction: int,
                    level: int, minutes: float, symbol="BTCUSDT"):
        """Fold one event (entry adapter), track any ENTER, then run the exit
        sweep for every tracked position — the runtime's per-event order."""
        ev = SimpleNamespace(
            symbol=symbol, asset_class="crypto", chain_id=chain_id,
            event_type=event_type, direction=direction,
            propagation_level=level, timestamp=iso(minutes),
            decision_price=100.0,
        )
        pkey = await self.strategy.entry.on_event(ev)
        if pkey is not None and pkey not in self.tracked:
            self.tracked[pkey] = SimpleNamespace(
                position_key=pkey,
                chain_prime_id=pkey.rsplit(":", 1)[0],
                trade_direction=1 if pkey.endswith(":L") else -1,
            )
        for key, pos in list(self.tracked.items()):
            exited = await self.strategy.exit.maybe_emit_exit(
                symbol=symbol, asset_class="crypto",
                chain_prime_id=pos.chain_prime_id,
                trade_direction=pos.trade_direction,
                decided_at=iso(minutes), decision_price=100.0,
            )
            if exited is not None:
                del self.tracked[key]     # the runtime untracks on a returned key

    # --- convenience drivers -------------------------------------------------
    async def failure(self, chain_id: str, direction: int, start_min: float,
                      depth: int = 1, terminate: bool = True):
        """A chain that initiates, (optionally) climbs to `depth`, and dies."""
        await self.event(chain_id, "chain_initiated", direction, 1, start_min)
        for lvl in range(2, depth + 1):
            await self.event(chain_id, "chain_propagated", direction, lvl, start_min + lvl)
        if terminate:
            await self.event(chain_id, "chain_terminated", direction, depth, start_min + depth + 1)

    async def climb(self, chain_id: str, direction: int, start_min: float,
                    to_level: int):
        await self.event(chain_id, "chain_initiated", direction, 1, start_min)
        for lvl in range(2, to_level + 1):
            await self.event(chain_id, "chain_propagated", direction, lvl, start_min + lvl)

    # --- row accessors -------------------------------------------------------
    def enters(self):
        return [r for r in self.rows if r["action"] == "ENTER"]

    def exits(self, reason=None):
        out = [r for r in self.rows if r["action"] == "EXIT"]
        return [r for r in out if r["exit_reason"] == reason] if reason else out


PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = ""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


async def main():
    # ------------------------------------------------------------------ §8.1
    h = Harness()
    await h.climb("C1", +1, start_min=0, to_level=3)
    check("1. gate does NOT fire without a prior opposing failure",
          not h.enters(), f"enters={h.enters()}")

    # ------------------------------------------------------------------ §8.2
    h = Harness()
    await h.failure("F", -1, start_min=0, depth=2)     # opposing, but reached L2
    await h.climb("C1", +1, start_min=10, to_level=2)
    check("2. gate does NOT fire when the prior opposing chain has depth >= 2",
          not h.enters(), f"enters={h.enters()}")

    # ------------------------------------------------------------------ §8.3
    h = Harness()
    await h.failure("F", -1, start_min=0, depth=1)
    await h.climb("C1", +1, start_min=6 * 60 + 5, to_level=2)   # failure start 6h05m prior
    check("3. gate does NOT fire when the failure is outside the window",
          not h.enters(), f"enters={h.enters()}")

    h = Harness()
    await h.failure("F", -1, start_min=0, depth=1)
    await h.climb("C1", +1, start_min=6 * 60, to_level=2)   # start EXACTLY 6h after
    check("3b. window bounds are INCLUSIVE: a failure exactly at start-6h fires",
          len(h.enters()) == 1, f"enters={h.enters()}")

    # ------------------------------------------------------------------ §8.4
    h = Harness()
    await h.failure("F", -1, start_min=0, depth=1)
    await h.climb("C1", +1, start_min=30, to_level=2)
    check("4. gate FIRES on exactly one qualifying failure",
          len(h.enters()) == 1 and h.enters()[0]["trigger_level"] == 2
          and h.enters()[0]["fraction"] == 1.0,
          f"enters={h.enters()}")

    # ---------------------------------------------------------------- §8.5+8
    h = Harness()
    await h.failure("F", -1, start_min=0, depth=1)
    await h.climb("C1", +1, start_min=30, to_level=9)   # full propagation to P9
    scales = h.exits("SCALE")
    ids = {r["signal_id"] for r in scales}
    check("5. seven tranches -> seven DISTINCT signal_ids",
          len(scales) == 7 and len(ids) == 7,
          f"n={len(scales)} distinct={len(ids)}")
    check("8. full propagation to P9: all seven tranches, NO remainder exit, "
          "position untracked",
          not h.exits("TERMINAL") and not h.tracked
          and [r["tranche" if "tranche" in r else "trigger_level"] for r in scales]
          and [r["trigger_level"] for r in scales] == [3, 4, 5, 6, 7, 8, 9]
          and sum(Decimal(str(r["fraction"])) for r in scales) == 1,
          f"terminal={h.exits('TERMINAL')} tracked={list(h.tracked)}")

    # ------------------------------------------------------------------ §8.6
    h = Harness()
    await h.failure("F", -1, start_min=0, depth=1)
    await h.climb("C1", +1, start_min=30, to_level=5)   # P5, then dies
    await h.event("C1", "chain_terminated", +1, 5, 40)
    scales = h.exits("SCALE")
    terms = h.exits("TERMINAL")
    banked = sum(Decimal(str(r["fraction"])) for r in scales)
    check("6. termination at P5: 0.20 banked (k=1..3), 0.80 TERMINAL, sum 1.0",
          [r["fraction"] for r in scales] == [0.05, 0.05, 0.10]
          and banked == Decimal("0.20")
          and len(terms) == 1 and terms[0]["fraction"] == 0.80
          and terms[0]["trigger_level"] == 5
          and banked + Decimal(str(terms[0]["fraction"])) == 1
          and not h.tracked,
          f"scales={[r['fraction'] for r in scales]} terms={[r['fraction'] for r in terms]}")

    # ------------------------------------------------------------------ §8.7
    h = Harness()
    await h.failure("F", -1, start_min=0, depth=1)
    await h.climb("C1", +1, start_min=30, to_level=3)   # P3, immediate death
    await h.event("C1", "chain_terminated", +1, 3, 35)
    scales, terms = h.exits("SCALE"), h.exits("TERMINAL")
    check("7. termination at P3: 0.05 banked, 0.95 TERMINAL",
          [r["fraction"] for r in scales] == [0.05]
          and len(terms) == 1 and terms[0]["fraction"] == 0.95
          and Decimal("0.05") + Decimal(str(terms[0]["fraction"])) == 1,
          f"scales={[r['fraction'] for r in scales]} terms={[r['fraction'] for r in terms]}")

    # -------------------------------------------------------------- hardening
    h = Harness()
    await h.event("F", "chain_initiated", -1, 1, 0)     # opposing L1, still ALIVE
    await h.climb("C1", +1, start_min=10, to_level=2)
    check("9. an ACTIVE opposing L1 chain is not a failure (gate closed)",
          not h.enters(), f"enters={h.enters()}")

    h = Harness()
    await h.failure("F", +1, start_min=0, depth=1)      # SAME-direction failure
    await h.climb("C1", +1, start_min=10, to_level=2)
    check("10. a same-direction failure does not gate", not h.enters())

    h = Harness()
    await h.failure("F", -1, start_min=0, depth=1)
    await h.climb("C1", +1, start_min=30, to_level=4)   # trigger re-fires at P3, P4
    check("11. one ENTER row despite deeper re-triggers (deterministic id dedupe)",
          len(h.enters()) == 1, f"n={len(h.enters())}")

    h = Harness()
    await h.failure("F", -1, start_min=0, depth=1)
    await h.climb("C1", +1, start_min=30, to_level=4)   # k=1,2 banked
    n_rows = len(h.rows)
    # Ten more sweeps with no peak change: an unrelated chain's events drive them.
    for i in range(10):
        await h.event("Z", "chain_initiated", -1, 1, 60 + i)
    check("12. re-sweeps with no new peak emit nothing (no duplicate tranches)",
          len(h.rows) == n_rows + 0,
          f"rows grew by {len(h.rows) - n_rows}")
    check("13. position stays TRACKED through partial banking (SCALE returns None)",
          len(h.tracked) == 1, f"tracked={list(h.tracked)}")

    # Factory refuses a schedule that overhangs the ladder (the P9 lesson).
    try:
        Harness(schedule={1: 0.5, 9: 0.5})   # entry 2 + offset 9 = P11 > P10
        check("14. factory refuses a schedule that overhangs the ladder", False,
              "no exception raised")
    except ValueError as e:
        check("14. factory refuses a schedule that overhangs the ladder",
              "does not fit ladder" in str(e), str(e))

    print(f"\n{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))