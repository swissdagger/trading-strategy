"""
The strategy package's frozen rules (strategy/rules.py, verbatim) through
strategy/chain_rules.py. rules.py is validated in the owner's replay harness;
these pin the ADAPTER: config resolution per ladder, events + clock in,
trade_signals rows out, acks and retries, clock-driven pricing, restart
hydration + opposing-chain reconcile, the crypto filters, and the spec's own
unit cases (§5.2, §5.4).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from execution.types import ExitReason, TradeSide
from strategy import rules as R
from strategy.chain_rules import FROZEN, effective_config, exit_reason_for, make_chain_rule
from strategy.ladder_registry import LadderRegistry
from strategy.rules import VolState
from strategy.vol import vol_state_from_closes

import config as CFG

UTC = timezone.utc
IN_SESSION = datetime(2026, 10, 14, 15, 0, tzinfo=UTC)      # Wed 11:00 ET
OPEN = datetime(2026, 10, 14, 13, 30, tzinfo=UTC)
FRI_2030 = datetime(2026, 10, 16, 20, 30, tzinfo=UTC)       # Fri 16:30 ET, after the close
MON_OPEN = datetime(2026, 10, 19, 13, 30, tzinfo=UTC)       # Mon 09:30 EDT


class FakeWriter:
    def __init__(self):
        self.rows, self.fail_next = [], 0

    async def _emit(self, kind, kw):
        if self.fail_next:
            self.fail_next -= 1
            raise RuntimeError("db down")
        self.rows.append((kind, kw))
        return f"{kw['chain_id']}:{'L' if kw['side'] == TradeSide.LONG else 'S'}"

    async def emit_entry(self, **kw):
        return await self._emit("ENTER", kw)

    async def emit_exit(self, **kw):
        return await self._emit("EXIT", kw)


class FakeVol:
    def __init__(self, states):
        self.states, self.prefetched = states, []

    async def prefetch(self, symbol, now):
        self.prefetched.append((symbol, now))
        return self.states.get(symbol)

    def get(self, symbol, now):
        return self.states.get(symbol)


class Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


def ev(event_type, symbol, direction, chain_id, level, ts, price, asset_class="equity", ladder="ED"):
    from strategy.multi_runtime import _RawEventView
    return _RawEventView({"event_type": event_type, "symbol": symbol, "asset_class": asset_class,
                          "direction": direction, "chain_id": chain_id, "ladder": ladder,
                          "propagation_level": level, "metadata": {"propagation_level": level},
                          "timestamp": ts.isoformat(), "decision_price": price})


def registry():
    reg = LadderRegistry()
    for n in ("FC", "DL", "ED", "CN"):
        reg.register(n, CFG.EXTRA_LADDERS[n])
    return reg


def build(name="rule1_v1", rule="RULE1_EQUITY", ladder="ED", *, states=None, now=IN_SESSION,
          params=None, load_open=None, load_opposing=None, closes=None, symbols=("NVDA", "XOM", "SPCX")):
    writer = FakeWriter()
    vol = FakeVol(states if states is not None else {s: VolState(1.0, 0.5, 0.9) for s in symbols})
    clock = Clock(now)
    closes = closes or {"NVDA": 500.0, "XOM": 110.0, "SPCX": 40.0, "BTC-USD": 60000.0}
    p = {"rule": rule, **(params or {})}
    strat = make_chain_rule(name=name, symbols=list(symbols), ladders=[ladder], params=p,
                            view_registry=registry(), writer_factory=lambda n: writer,
                            vol_provider_for=lambda ac: vol, latest_close_for=lambda ac: closes.get,
                            load_open=load_open, load_opposing_since=load_opposing, clock=clock)
    return strat, writer, vol, clock


# ---------------------------------------------------------------------------
# Config resolution: the level numbers mean different rungs on each ladder
# ---------------------------------------------------------------------------

def test_frozen_configs_are_the_package_ones():
    assert FROZEN["RULE1_EQUITY"] is R.RULE1_EQUITY and R.RULE1_EQUITY.vol_ratio_max == 1.46
    assert R.RULE3_CRYPTO.vol_ratio_min == 0.71 and R.RULE3_CRYPTO.avoid_squeeze_release
    assert (R.RULE3_NEW_EQUIV.entry_level, R.RULE3_NEW_EQUIV.confirm_level) == (5, 6)


@pytest.mark.parametrize("rule,ladder,params,expect", [
    ("RULE1_EQUITY", "ED", {}, ("3m", "5m")),                        # the equity default ladder
    ("RULE1_EQUITY", "DL", {}, ("45s", "1m")),                       # the same numbers on DL
    ("RULE3_CRYPTO", "FC", {}, ("3m", "5m")),                        # crypto research ladder
    ("RULE3_NEW_LITERAL", "CN", {}, ("45s", "1m")),
    ("RULE3_NEW_EQUIV", "CN", {}, ("3m", "5m")),                     # levels 5/6 on the spec's ladder
    ("RULE3_NEW_EQUIV", "CN", {"entry_tf": "3m", "confirm_tf": "5m"}, ("3m", "5m")),
    ("RULE1_EQUITY", "DL", {"entry_tf": "3m", "confirm_tf": "15m"}, ("3m", "15m")),
])
def test_levels_resolve_to_rungs_per_ladder(rule, ladder, params, expect):
    reg = registry()
    lad = reg.get(ladder)
    cfg = effective_config(FROZEN[rule], name="x", ladder=lad, params=params)
    assert (lad.timeframes[cfg.entry_level - 1], lad.timeframes[cfg.confirm_level - 1]) == expect
    assert cfg.ladder == ladder and cfg.name == "x"
    # Everything else is the frozen config.
    base = FROZEN[rule]
    assert (cfg.stall_minutes, cfg.vol_ratio_max, cfg.vol_ratio_min, cfg.avoid_squeeze_release,
            cfg.asset_class, cfg.session) == (base.stall_minutes, base.vol_ratio_max,
                                              base.vol_ratio_min, base.avoid_squeeze_release,
                                              base.asset_class, base.session)


def test_rule3_new_equiv_on_dl_would_land_on_the_wrong_rungs_and_is_refused_without_pins():
    # DL has no 5m: levels 5/6 are 2m/3m there. Pinning by rung fails loudly.
    reg = registry()
    cfg = effective_config(FROZEN["RULE3_NEW_EQUIV"], name="x", ladder=reg.get("DL"), params={})
    assert (reg.get("DL").timeframes[cfg.entry_level - 1], reg.get("DL").timeframes[cfg.confirm_level - 1]) == ("2m", "3m")
    with pytest.raises(ValueError):
        effective_config(FROZEN["RULE3_NEW_EQUIV"], name="x", ladder=reg.get("DL"),
                         params={"entry_tf": "3m", "confirm_tf": "5m"})


def test_shipped_config_enables_the_three_runnable_rules_shadow_only():
    enabled = {k for k, v in CFG.STRATEGIES.items() if v["enabled"]}
    assert {"rule1_v1", "rule1_new_literal_v1", "rule3_v1"} <= enabled
    assert {"rule3_new_literal_v1", "rule3_new_equiv_v1"} <= enabled
    assert CFG.STRATEGIES["rule3_new_equiv_v1"]["symbols"] == CFG.ASSET_CONFIGS["crypto"]["subminute_symbols"]
    assert set(CFG.EXTRA_LADDERS["CN"]) <= set(CFG.ASSET_CONFIGS["crypto"]["timeframes"])
    assert CFG.EXTRA_LADDERS["ED"] == ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h"]
    assert CFG.STRATEGIES["rule1_v1"]["ladders"] == ["ED"]
    assert CFG.STRATEGIES["rule3_v1"]["ladders"] == ["FC"]
    assert "5m" in CFG.ASSET_CONFIGS["equity"]["timeframes"]
    for k in ("rule1_v1", "rule1_new_literal_v1", "rule3_v1", "rule3_new_literal_v1", "rule3_new_equiv_v1"):
        assert k in CFG.SHADOW_ONLY_STRATEGIES


def test_exit_reason_mapping():
    assert exit_reason_for("stall_no_L3_60m") == ExitReason.TIME
    assert exit_reason_for("stall_no_L6_30m") == ExitReason.TIME
    assert exit_reason_for("opposing_P1") == ExitReason.STOP


# ---------------------------------------------------------------------------
# Rule 1 on ED through the adapter (spec §5.2 cases)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_p2_in_session_writes_enter_and_acks():
    strat, writer, vol, _ = build()
    assert await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c1", 2, IN_SESSION, 501.5)) is None
    assert vol.prefetched == [("NVDA", IN_SESSION)]
    kind, kw = writer.rows[0]
    assert kind == "ENTER" and kw["trigger_level"] == 2 and kw["decision_price"] == 501.5
    assert kw["asset_class"] == "equity" and kw["side"] == TradeSide.LONG
    assert strat.entry.rule.trades["c1"].state == "open"
    # An entry-level event on a ladder the rule does not listen to is ignored by the rule.
    assert await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c2", 2, IN_SESSION, 500.0, ladder="DL")) is None
    assert "c2" in strat.entry.rule.trades        # the adapter retags with the rule's ladder


@pytest.mark.asyncio
async def test_stall_closes_at_exactly_stall_minutes_with_TIME():
    strat, writer, _, clock = build()
    await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c1", 2, IN_SESSION, 500.0))
    clock.now = IN_SESSION + timedelta(minutes=60)
    await strat.entry.tick()
    assert len(writer.rows) == 1                              # not yet: strictly after
    clock.now = IN_SESSION + timedelta(minutes=60, seconds=1)
    await strat.entry.tick()
    kind, kw = writer.rows[1]
    assert kind == "EXIT" and kw["reason"] == ExitReason.TIME and kw["decision_price"] == 500.0
    assert strat.entry.rule.trades["c1"].exit_reason == "stall_no_L3_60m"


@pytest.mark.asyncio
async def test_l3_inside_window_prevents_stall():
    strat, writer, _, clock = build()
    await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c1", 2, IN_SESSION, 500.0))
    clock.now = IN_SESSION + timedelta(minutes=30)
    await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c1", 3, clock.now, 502.0))
    clock.now = IN_SESSION + timedelta(hours=2)
    await strat.entry.tick()
    assert [k for k, _ in writer.rows] == ["ENTER"]


@pytest.mark.asyncio
async def test_opposing_p1_while_pending_entry_is_skipped_never_opened():
    pre = OPEN - timedelta(minutes=20)
    strat, writer, _, clock = build(now=pre)
    await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c1", 2, pre, 500.0))
    assert strat.entry.rule.trades["c1"].state == "pending_entry"
    clock.now = pre + timedelta(minutes=5)
    await strat.entry.on_event(ev("chain_initiated", "NVDA", -1, "c9", 1, clock.now, 499.0))
    assert strat.entry.rule.trades["c1"].state == "skipped"
    clock.now = OPEN
    await strat.entry.tick()
    assert writer.rows == []


@pytest.mark.asyncio
async def test_exit_firing_while_open_order_in_flight_closes_right_after_fill():
    # Simulate "in flight": the OPEN write fails (rule.reject -> pending_entry);
    # the opposing P1 arrives; the retried OPEN then immediately CLOSEs.
    strat, writer, _, clock = build()
    writer.fail_next = 1
    await strat.entry.on_event(ev("chain_propagated", "NVDA", 1, "c1", 2, IN_SESSION, 500.0))
    assert strat.entry.rule.trades["c1"].state == "pending_entry"
    clock.now = IN_SESSION + timedelta(seconds=5)
    await strat.entry.on_event(ev("chain_initiated", "NVDA", -1, "c9", 1, clock.now, 499.0))
    assert strat.entry.rule.trades["c1"].state == "skipped"       # never filled -> dropped
    assert writer.rows == []


@pytest.mark.asyncio
async def test_friday_after_close_p2_fills_at_monday_open(monkeypatch):
    # Spec §5.4. L3 confirmed so the stall does not drop it over the weekend.
    strat, writer, _, clock = build(now=FRI_2030)
    await strat.entry.on_event(ev("chain_propagated", "XOM", -1, "c1", 2, FRI_2030, 111.0))
    clock.now = FRI_2030 + timedelta(minutes=10)
    await strat.entry.on_event(ev("chain_propagated", "XOM", -1, "c1", 3, clock.now, 110.9))
    for when in (FRI_2030 + timedelta(hours=1), MON_OPEN - timedelta(days=1), MON_OPEN - timedelta(seconds=1)):
        clock.now = when
        await strat.entry.tick()
        assert writer.rows == []
    clock.now = MON_OPEN
    await strat.entry.tick()
    kind, kw = writer.rows[0]
    assert kind == "ENTER" and kw["side"] == TradeSide.SHORT and kw["decided_at"] == MON_OPEN.isoformat()
    assert kw["decision_price"] == 110.0                     # latest 1m close


@pytest.mark.asyncio
async def test_restart_restores_open_trade_and_reconciles_against_chains():
    opened = IN_SESSION - timedelta(hours=3)

    async def load_open():
        return [{"chain_id": "c1", "symbol": "NVDA", "direction": "LONG", "created_at": opened,
                 "position_key": "c1:L"},
                {"chain_id": "c2", "symbol": "XOM", "direction": "SHORT", "created_at": opened,
                 "position_key": "c2:S"}]
    asked = []

    async def opposing(symbol, direction, since):
        asked.append((symbol, direction, since))
        return symbol == "XOM"                                # an opposing chain started while down

    strat, writer, _, clock = build(load_open=load_open, load_opposing=opposing)
    await strat.entry.start()
    assert sorted(asked) == [("NVDA", -1, opened), ("XOM", 1, opened)]
    assert strat.entry.rule.trades["c1"].state == "open" and strat.entry.rule.trades["c1"].confirmed
    # The reconciled one was marked and, in session, drained at once.
    assert [(k, kw["chain_id"], kw["reason"]) for k, kw in writer.rows] == [("EXIT", "c2", ExitReason.STOP)]
    assert strat.entry.stats["reconciled_exits"] == 1


# ---------------------------------------------------------------------------
# Rule 3 (crypto) filters, 24/7, 30-min stall
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rule3_filters_and_stall():
    states = {
        "BTC-USD": VolState(ratio=0.9, level=0.5, short=0.8),     # accept
        "ETH-USD": VolState(ratio=0.71, level=0.5, short=0.8),    # ratio <= 0.71: skip
        "SOL-USD": VolState(ratio=1.2, level=0.10, short=1.3),    # squeeze-release: skip
        "XRP-USD": VolState(ratio=1.2, level=None, short=None),   # missing level/short: accept
        "ADA-USD": None,                                          # no history: skip
    }
    syms = tuple(states)
    strat, writer, _, clock = build("rule3_v1", "RULE3_CRYPTO", "FC", states=states,
                                    now=datetime(2026, 10, 17, 3, 0, tzinfo=UTC), symbols=syms)
    for s in syms:
        await strat.entry.on_event(ev("chain_propagated", s, 1, f"c-{s}", 2, clock.now, 1.0,
                                      asset_class="crypto", ladder="FC"))
    opened = sorted(kw["symbol"] for k, kw in writer.rows if k == "ENTER")
    assert opened == ["BTC-USD", "XRP-USD"]                     # 03:00 UTC Saturday: 24/7
    assert any("squeeze-release" in m for m in strat.entry.rule.log)
    clock.now += timedelta(minutes=31)
    await strat.entry.tick()
    exits = [(kw["symbol"], kw["reason"]) for k, kw in writer.rows if k == "EXIT"]
    assert sorted(exits) == [("BTC-USD", ExitReason.TIME), ("XRP-USD", ExitReason.TIME)]
    assert strat.entry.rule.trades["c-BTC-USD"].exit_reason == "stall_no_L3_30m"


def test_vol_state_research_function_sanity():
    idx = pd.date_range("2026-09-10", "2026-10-14 15:00", freq="min", tz="UTC")
    rng = np.random.default_rng(2)
    closes = pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 1e-3, len(idx)))), index=idx)
    st = vol_state_from_closes(closes, datetime(2026, 10, 14, 15, 7, tzinfo=UTC))
    assert st is not None and 0.7 < st.ratio < 1.3
    assert st.level is not None and 0.0 <= st.level <= 1.0
    assert st.short is not None and 0.3 < st.short < 3.0


# ---------------------------------------------------------------------------
# Router: detected_at on every tagged event
# ---------------------------------------------------------------------------

def test_tagged_events_carry_detected_at():
    from producer.chain_machine import ChainEvent
    from producer.fanout_router_multi import MultiLadderRouter
    ce = ChainEvent(event_type="chain_initiated", chain_id="c", symbol="NVDA", asset_class="equity",
                    direction=1, propagation_level=1, timeframe="1m", timestamp="2026-10-14T15:00:00+00:00",
                    price=1.0)
    raw = MultiLadderRouter._tag(None, ce, "ED")
    assert raw["ladder"] == "ED" and raw["detected_at"] is not None
    datetime.fromisoformat(raw["detected_at"])                 # parseable
