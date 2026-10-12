"""
Tests for the four_chain strategy (strategy/chain_history.py,
four_chain_strategy.py). Despite the registry name, the rules are the TWO-chain
pattern the strategy switched to on 2026-08-13:

ENTRY, evaluated when a chain ("Chain 2", direction d) PROPAGATES to 15m:
  1. Initiation: some earlier same-direction chain ("Chain 1") reached 15m,
     started at or before Chain 2, reached 15m no later than Chain 2 did, and an
     opposing chain FAILED (terminated without reaching 3m) with its start
     inside [Chain 1 start, Chain 2 start].
  2. Confirmation: some same-direction chain other than Chain 2 that started
     before Chain 2 is CLOSED and reached 1h (full propagation). Chain 1 itself
     qualifies.
  3. Enter on Chain 2.
EXIT: TARGET when Chain 2 reaches 1h; STOP when an OPEN opposing chain reaches 5m.

The negative cases matter most (a false entry is real money). Events go through
the history view and a fake writer, so we assert on what the strategy DECIDED
without a DB.
"""
from datetime import datetime, timedelta, timezone

import pytest

from strategy.chain_history import ChainHistoryView, _parse_ts
from strategy.four_chain_strategy import FourChainEntry, FourChainExit
from strategy.ladder_registry import LadderRegistry
from execution.types import ExitReason, TradeSide


FOUR_CHAIN_TF = ["1m", "3m", "5m", "15m", "1h"]   # levels 1..5
L = {"1m": 1, "3m": 2, "5m": 3, "15m": 4, "1h": 5}
SYM = "BTCUSDT"


def ladder():
    reg = LadderRegistry()
    return reg.register("FC", FOUR_CHAIN_TF)


class FakeWriter:
    """Records emit_entry/emit_exit calls; returns a position_key like the real
    writer."""
    def __init__(self):
        self.entries = []
        self.exits = []
        self.strategy = "four_chain"
    async def emit_entry(self, *, chain_id, side, symbol, asset_class, decided_at,
                         decision_price=None, trigger_level=None):
        pkey = f"{chain_id}:{'L' if side == TradeSide.LONG else 'S'}"
        self.entries.append({"chain_id": chain_id, "side": side, "symbol": symbol,
                             "pkey": pkey, "decided_at": decided_at,
                             "decision_price": decision_price,
                             "trigger_level": trigger_level})
        return pkey
    async def emit_exit(self, *, chain_id, side, symbol, asset_class, reason,
                        decided_at, decision_price=None, trigger_level=None):
        pkey = f"{chain_id}:{'L' if side == TradeSide.LONG else 'S'}"
        self.exits.append({"chain_id": chain_id, "reason": reason, "pkey": pkey})
        return pkey


# Time helper: base + N minutes as ISO.
BASE = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
def T(minutes) -> str:
    return (BASE + timedelta(minutes=minutes)).isoformat()


def feed(view, chain_id, direction, level, minutes, event_type="chain_propagated",
         symbol=SYM):
    """Fold one event into the history view."""
    view.apply(symbol=symbol, chain_id=chain_id, event_type=event_type,
               direction=direction, propagation_level=level, timestamp=T(minutes))


def climb(view, chain_id, direction, start, top, terminate_at=None):
    """A chain initiating on 1m at `start` and propagating one rung per minute
    up to level `top`; optionally terminating at minute `terminate_at`."""
    feed(view, chain_id, direction, L["1m"], start, "chain_initiated")
    for lvl in range(2, top + 1):
        feed(view, chain_id, direction, lvl, start + lvl - 1)
    if terminate_at is not None:
        feed(view, chain_id, direction, top, terminate_at, "chain_terminated")


def failure(view, chain_id, direction, start):
    """An opposing chain that initiates on 1m and dies there (never reaches 3m)."""
    feed(view, chain_id, direction, L["1m"], start, "chain_initiated")
    feed(view, chain_id, direction, L["1m"], start, "chain_terminated")


def setup():
    lad = ladder()
    view = ChainHistoryView(lad)
    writer = FakeWriter()
    return view, writer, FourChainEntry(view, lad, writer)


async def fire(view, entry, chain_id, direction, level, minutes,
               event_type="chain_propagated", decision_price=100.0):
    """Deliver one event the way FourChainEntryAdapter does: fold it into the
    view FIRST, then run the entry check on it."""
    feed(view, chain_id, direction, level, minutes, event_type)
    return await entry.on_event(
        symbol=SYM, asset_class="crypto", chain_id=chain_id,
        event_type=event_type, direction=direction, propagation_level=level,
        timestamp=T(minutes), decision_price=decision_price,
    )


def chain2_to_5m(view, d, start=10):
    """Chain 2 initiates at `start` and climbs to 5m; the test fires its 15m."""
    climb(view, "c2", d, start, L["5m"])


def valid_pattern(view, d=1):
    """The minimal valid two-chain pattern, ready for Chain 2's 15m trigger at
    t=13:
      c1 (d)  : 1m@0 .. 1h@4, terminated@6 — Chain 1 AND the closed full-prop
      cf (-d) : starts and fails on 1m @8 — inside [c1 start=0, c2 start=10]
      c2 (d)  : 1m@10 .. 5m@12; the trigger is its 15m @13
    """
    climb(view, "c1", d, 0, L["1h"], terminate_at=6)
    failure(view, "cf", -d, 8)
    chain2_to_5m(view, d)


# --------------------------------------------------------------------
# Entry — the valid pattern fires
# --------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("d,side,pkey", [(1, TradeSide.LONG, "c2:L"),
                                         (-1, TradeSide.SHORT, "c2:S")])
async def test_valid_pattern_enters_on_chain2(d, side, pkey):
    view, writer, entry = setup()
    valid_pattern(view, d)
    assert await fire(view, entry, "c2", d, L["15m"], 13) == pkey
    assert len(writer.entries) == 1
    e = writer.entries[0]
    assert e["chain_id"] == "c2" and e["side"] == side
    assert e["trigger_level"] == L["15m"]
    assert e["decided_at"] == T(13)
    assert e["decision_price"] == 100.0


@pytest.mark.asyncio
async def test_separate_chain1_and_confirmation_chain_enters():
    # Chain 1 need not be the full-prop chain: c0 closes at 1h, c1 stalls at
    # 15m (still open), the failure sits between c1 and c2.
    view, writer, entry = setup()
    climb(view, "c0", 1, 0, L["1h"], terminate_at=6)
    climb(view, "c1", 1, 7, L["15m"])
    failure(view, "cf", -1, 12)
    chain2_to_5m(view, 1, start=14)
    assert await fire(view, entry, "c2", 1, L["15m"], 17) == "c2:L"


@pytest.mark.asyncio
async def test_failure_starting_exactly_at_chain2_start_counts():
    # The failure window is inclusive at both ends.
    view, writer, entry = setup()
    climb(view, "c1", 1, 0, L["1h"], terminate_at=6)
    failure(view, "cf", -1, 10)
    chain2_to_5m(view, 1, start=10)
    assert await fire(view, entry, "c2", 1, L["15m"], 13) == "c2:L"


# --------------------------------------------------------------------
# Entry — trigger: only a chain_propagated event at 15m
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_terminated_at_15m_does_not_enter():
    # chain_terminated carries the level it died on, which can be 15m. A dying
    # chain must never trigger an entry.
    view, writer, entry = setup()
    valid_pattern(view, 1)
    assert await fire(view, entry, "c2", 1, L["15m"], 13,
                      event_type="chain_terminated") is None
    assert writer.entries == []


@pytest.mark.asyncio
@pytest.mark.parametrize("event_type", ["chain_initiated", "level_reached"])
async def test_other_event_types_at_15m_do_not_enter(event_type):
    # level_reached is the pre-fix trigger; nothing emits it.
    view, writer, entry = setup()
    valid_pattern(view, 1)
    assert await fire(view, entry, "c2", 1, L["15m"], 13,
                      event_type=event_type) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("level", [L["3m"], L["5m"], L["1h"]])
async def test_propagation_at_other_levels_does_not_enter(level):
    view, writer, entry = setup()
    valid_pattern(view, 1)
    assert await fire(view, entry, "c2", 1, level, 13) is None
    assert writer.entries == []


# --------------------------------------------------------------------
# Entry — initiation phase (Chain 1 + opposing failure)
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_entry_without_opposing_failure():
    view, writer, entry = setup()
    climb(view, "c1", 1, 0, L["1h"], terminate_at=6)
    chain2_to_5m(view, 1)
    assert await fire(view, entry, "c2", 1, L["15m"], 13) is None


@pytest.mark.asyncio
async def test_opposing_chain_that_reached_3m_is_not_a_failure():
    view, writer, entry = setup()
    climb(view, "c1", 1, 0, L["1h"], terminate_at=6)
    climb(view, "cf", -1, 7, L["3m"], terminate_at=9)
    chain2_to_5m(view, 1)
    assert await fire(view, entry, "c2", 1, L["15m"], 13) is None


@pytest.mark.asyncio
async def test_open_opposing_chain_at_1m_is_not_a_failure():
    # Alive at 1m has not failed yet — it may be about to run.
    view, writer, entry = setup()
    climb(view, "c1", 1, 0, L["1h"], terminate_at=6)
    feed(view, "cf", -1, L["1m"], 8, "chain_initiated")
    chain2_to_5m(view, 1)
    assert await fire(view, entry, "c2", 1, L["15m"], 13) is None


@pytest.mark.asyncio
async def test_same_direction_failure_does_not_count():
    view, writer, entry = setup()
    climb(view, "c1", 1, 0, L["1h"], terminate_at=6)
    failure(view, "cf", 1, 8)
    chain2_to_5m(view, 1)
    assert await fire(view, entry, "c2", 1, L["15m"], 13) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_start", [-5, 11])
async def test_failure_outside_window_does_not_count(fail_start):
    # Before every candidate Chain 1 started, or after Chain 2 started.
    view, writer, entry = setup()
    climb(view, "c1", 1, 0, L["1h"], terminate_at=6)
    failure(view, "cf", -1, fail_start)
    chain2_to_5m(view, 1)
    assert await fire(view, entry, "c2", 1, L["15m"], 13) is None


@pytest.mark.asyncio
async def test_no_entry_when_chain1_reached_15m_after_chain2():
    # Ordering rule: Chain 2's 15m must be at or after Chain 1's. Here the only
    # candidate's 15m timestamp is later than the trigger's.
    view, writer, entry = setup()
    feed(view, "c1", 1, L["1m"], 0, "chain_initiated")
    feed(view, "c1", 1, L["3m"], 1)
    feed(view, "c1", 1, L["5m"], 2)
    feed(view, "c1", 1, L["15m"], 20)
    feed(view, "c1", 1, L["1h"], 21)
    feed(view, "c1", 1, L["1h"], 22, "chain_terminated")
    failure(view, "cf", -1, 8)
    chain2_to_5m(view, 1)
    assert await fire(view, entry, "c2", 1, L["15m"], 13) is None


# --------------------------------------------------------------------
# Entry — confirmation phase (closed full-propagation chain)
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_no_entry_when_no_chain_reached_1h():
    view, writer, entry = setup()
    climb(view, "c1", 1, 0, L["15m"], terminate_at=6)   # tops out at 15m
    failure(view, "cf", -1, 8)
    chain2_to_5m(view, 1)
    assert await fire(view, entry, "c2", 1, L["15m"], 13) is None


@pytest.mark.asyncio
async def test_no_entry_when_full_prop_chain_still_open():
    view, writer, entry = setup()
    climb(view, "c1", 1, 0, L["1h"])                     # reached 1h, not closed
    failure(view, "cf", -1, 8)
    chain2_to_5m(view, 1)
    assert await fire(view, entry, "c2", 1, L["15m"], 13) is None


@pytest.mark.asyncio
async def test_opposing_full_prop_chain_does_not_confirm():
    view, writer, entry = setup()
    climb(view, "c1", 1, 0, L["15m"])                    # Chain 1, open at 15m
    climb(view, "cx", -1, 1, L["1h"], terminate_at=7)    # wrong direction
    failure(view, "cf", -1, 8)
    chain2_to_5m(view, 1)
    assert await fire(view, entry, "c2", 1, L["15m"], 13) is None


@pytest.mark.asyncio
async def test_full_prop_chain_starting_after_chain2_does_not_confirm():
    view, writer, entry = setup()
    climb(view, "c1", 1, 0, L["15m"])                    # Chain 1, open at 15m
    failure(view, "cf", -1, 8)
    chain2_to_5m(view, 1)
    climb(view, "c3", 1, 11, L["1h"], terminate_at=12)   # started after c2
    assert await fire(view, entry, "c2", 1, L["15m"], 13) is None


# --------------------------------------------------------------------
# Exit — TARGET and STOP
# --------------------------------------------------------------------

def test_exit_target_on_full_propagation():
    lad = ladder()
    view = ChainHistoryView(lad)
    writer = FakeWriter()
    exit_ = FourChainExit(view, lad, writer)
    # Prime chain c4 reaches 1h (level 5).
    feed(view, "c4", 1, 5, 20)   # 1h
    reason = exit_.evaluate(symbol="BTCUSDT", chain_prime_id="c4", trade_direction=1)
    assert reason == ExitReason.TARGET


def test_exit_stop_on_opposing_5m():
    lad = ladder()
    view = ChainHistoryView(lad)
    writer = FakeWriter()
    exit_ = FourChainExit(view, lad, writer)
    # An opposing chain reaches 5m (level 3).
    feed(view, "cx", -1, 3, 20)   # opposing at 5m
    reason = exit_.evaluate(symbol="BTCUSDT", chain_prime_id="c4", trade_direction=1)
    assert reason == ExitReason.STOP


def test_no_stop_when_opposing_5m_chain_already_terminated():
    lad = ladder()
    view = ChainHistoryView(lad)
    exit_ = FourChainExit(view, lad, FakeWriter())
    climb(view, "cx", -1, 20, L["5m"], terminate_at=25)
    assert exit_.evaluate(symbol=SYM, chain_prime_id="c2", trade_direction=1) is None


def test_no_exit_when_neither_condition():
    lad = ladder()
    view = ChainHistoryView(lad)
    writer = FakeWriter()
    exit_ = FourChainExit(view, lad, writer)
    # Prime only at 15m (not 1h); opposing only at 3m (not 5m).
    feed(view, "c4", 1, 4, 20)     # prime at 15m
    feed(view, "cx", -1, 2, 20)    # opposing at 3m only
    reason = exit_.evaluate(symbol="BTCUSDT", chain_prime_id="c4", trade_direction=1)
    assert reason is None


@pytest.mark.asyncio
async def test_exit_emits_signal():
    lad = ladder()
    view = ChainHistoryView(lad)
    writer = FakeWriter()
    exit_ = FourChainExit(view, lad, writer)
    feed(view, "c4", 1, 5, 20)   # 1h -> TARGET
    pkey = await exit_.maybe_emit_exit(
        symbol="BTCUSDT", asset_class="crypto", chain_prime_id="c4",
        trade_direction=1, decided_at=T(20),
    )
    assert pkey == "c4:L"
    assert writer.exits[0]["reason"] == ExitReason.TARGET


# --------------------------------------------------------------------
# History view — retention and failed-between
# --------------------------------------------------------------------

def test_history_retains_terminated_chains():
    lad = ladder()
    view = ChainHistoryView(lad)
    feed(view, "cf", -1, 1, 4, "chain_initiated")
    feed(view, "cf", -1, 1, 4, "chain_terminated")
    chains = view.chains("BTCUSDT")
    assert len(chains) == 1
    assert chains[0].terminated is True
    assert chains[0].peak_level == 1   # failed before 3m


def test_history_evicts_oldest_over_count_cap():
    # Retention is a per-symbol COUNT cap (no time window: four_chain's pattern
    # span is unbounded). Over the cap, the oldest by first_seen goes.
    lad = ladder()
    view = ChainHistoryView(lad, max_chains_per_symbol=2)
    feed(view, "old", 1, 4, 0)
    feed(view, "mid", 1, 4, 45)
    feed(view, "new", 1, 4, 90)
    ids = [c.chain_id for c in view.chains("BTCUSDT")]
    assert ids == ["mid", "new"]


def test_failed_between_detects_opposing_failure():
    lad = ladder()
    view = ChainHistoryView(lad)
    d = 1
    feed(view, "cf", -d, 1, 4, "chain_initiated")
    feed(view, "cf", -d, 1, 4, "chain_terminated")
    after = _parse_ts(T(3))
    before = _parse_ts(T(8))
    failed = view.failed_between("BTCUSDT", direction=d, after=after, before=before)
    assert len(failed) == 1
    assert failed[0].chain_id == "cf"
