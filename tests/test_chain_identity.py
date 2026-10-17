"""
Unit tests for the duplicate-chain fix: deterministic chain identity, and the
INIT guard surviving a restart.

Both bugs were invisible in-process and only showed up as extra rows in `chains`,
so these pin the two properties that make a re-processed initiation a no-op:
  1. the same initiation always yields the same chain_id (and different ladders
     yield different ones);
  2. hydrate restores _last_base_signal, so the first baseline signal after a
     restart does not initiate next to a chain that is already live.

No DB and no Sumtyme: hydrate only touches the injected loaders, and STAGE 1 is
driven through the signal cache directly, which is what the machine's own
forecast path writes to.
"""
import pytest

from producer.service2_machine import ServiceTwoChainMachine, chain_id_for

FC_LADDER = ["1m", "3m", "5m", "15m", "1h"]
PF_LADDER = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h"]
ORIGIN = "2026-07-29T12:00:00+00:00"          # the 1m bar's open label
ORIGIN_CLOSE = "2026-07-29T12:01:00+00:00"    # its close


def _machine(ladder=FC_LADDER, ladder_name="FC", *,
             active_chains=None, last_base_signal=None):
    """A machine with every hook stubbed. The forecast path is never entered —
    these tests drive the state machine through the signal cache, as on_bar does
    once a forecast returns."""
    async def _noop(*args, **kwargs):
        return None

    return ServiceTwoChainMachine(
        symbol="BTCUSDT", asset_class="crypto", ladder=ladder,
        ladder_name=ladder_name,
        forecast_fn=lambda **kw: {"5001": 0},
        prepare_df_fn=lambda *a, **kw: (None, None),
        to_interval_fn=lambda tf: (1, "minute"),
        build_df_fn=lambda *a, **kw: None,
        persist_signal=_noop,
        persist_chain_init=_noop,
        persist_chain_propagate=_noop,
        persist_chain_terminate=_noop,
        load_active_chains=(lambda: _returns(active_chains or [])),
        load_last_base_signal=(lambda: _returns(last_base_signal)),
    )


def _returns(value):
    async def _coro():
        return value
    return _coro()


def _hydrated_chain(chain_id, direction, origin=ORIGIN, timeframe="1m"):
    """The dict shape make_load_active_chains hands back."""
    return {
        "chain_id": chain_id, "symbol": "BTCUSDT", "asset_class": "crypto",
        "ladder": "FC", "direction": direction,
        "current_level": 0, "next_expected_level": 1,
        "active_timeframes": [timeframe],
        "history": [{"level": 1, "timeframe": timeframe,
                     "timestamp": origin, "price": 100.0}],
        "last_timestamp": origin,
    }


# --- 1. identity ------------------------------------------------------------

def test_same_initiation_yields_same_id():
    # The property the ON CONFLICT (chain_id) DO NOTHING in persist_chain_init
    # depends on. With uuid4 this was false by construction.
    a = chain_id_for("BTCUSDT", "crypto", "FC", 1, ORIGIN)
    b = chain_id_for("BTCUSDT", "crypto", "FC", 1, ORIGIN)
    assert a == b


@pytest.mark.parametrize("field,value", [
    ("symbol", "ETHUSDT"),
    ("asset_class", "equity"),
    ("ladder_name", "PF"),
    ("direction", -1),
    ("origin", "2026-07-29T12:01:00+00:00"),
])
def test_each_identity_field_changes_the_id(field, value):
    base = dict(symbol="BTCUSDT", asset_class="crypto", ladder_name="FC",
                direction=1, origin=ORIGIN)
    variant = {**base, field: value}
    assert chain_id_for(*base.values()) != chain_id_for(*variant.values())


def test_overlapping_ladders_stay_distinct():
    # FC and PF share the 1m baseline and both run on crypto symbols, so ONE 1m
    # signal legitimately initiates two chains. They must not collapse into one
    # row — two machines with different level numbering would then share it.
    assert (chain_id_for("BTCUSDT", "crypto", "FC", 1, ORIGIN)
            != chain_id_for("BTCUSDT", "crypto", "PF", 1, ORIGIN))


# --- 2. the INIT guard across a restart -------------------------------------

@pytest.mark.asyncio
async def test_hydrate_restores_guard_from_signals():
    m = _machine(last_base_signal=(-1, ORIGIN))
    await m.hydrate()
    assert m._last_base_signal == -1


@pytest.mark.asyncio
async def test_hydrate_restores_guard_from_active_chain_when_signals_lag():
    # persist_signal is deferred/batched; persist_chain_init is synchronous. A
    # crash can therefore leave a chain durable whose signal row was lost, so the
    # newer of the two sources wins.
    chain = _hydrated_chain("chain1.abc", direction=1,
                            origin="2026-07-29T12:05:00+00:00")
    m = _machine(active_chains=[chain], last_base_signal=(-1, ORIGIN))
    await m.hydrate()
    assert m._last_base_signal == 1


@pytest.mark.asyncio
async def test_no_reinit_on_first_baseline_signal_after_restart():
    # THE regression. Pre-fix: hydrate left _last_base_signal at None, so this
    # baseline signal initiated a second chain beside the hydrated one.
    chain_id = chain_id_for("BTCUSDT", "crypto", "FC", 1, ORIGIN)
    m = _machine(active_chains=[_hydrated_chain(chain_id, direction=1)],
                 last_base_signal=(1, ORIGIN))
    await m.hydrate()

    m._latest_signals[0] = {"signal": 1, "timeframe": "1m",
                            "timestamp": ORIGIN, "bar_close": ORIGIN_CLOSE,
                            "price": 100.0}
    events = await m._run_state_machine(0)

    assert [e.event_type for e in events] == []
    assert list(m._active_chains) == [chain_id]


@pytest.mark.asyncio
async def test_replayed_initiation_does_not_fork_the_chain():
    # At-least-once delivery replays bars. The replay re-derives the same id, so
    # the already-live check short-circuits rather than creating a second dict
    # under the same id.
    m = _machine()
    m._latest_signals[0] = {"signal": 1, "timeframe": "1m",
                            "timestamp": ORIGIN, "bar_close": ORIGIN_CLOSE,
                            "price": 100.0}

    first = await m._run_state_machine(0)
    assert [e.event_type for e in first] == ["chain_initiated"]

    m._last_base_signal = None          # as a restart would leave it
    second = await m._run_state_machine(0)

    assert [e.event_type for e in second] == []
    assert len(m._active_chains) == 1


@pytest.mark.asyncio
async def test_no_reinit_beside_a_retained_legacy_chain():
    # Chains retained across migration 019 keep their uuid4. The already-live
    # check must recognise one by its identity (direction + origin), not by
    # matching a computed id it can never equal.
    legacy = _hydrated_chain("3956bf27-23c9-4bac-af42-12d22ef37ef9", direction=1)
    m = _machine(active_chains=[legacy], last_base_signal=None)
    await m.hydrate()
    m._last_base_signal = None          # worst case: guard restore found nothing

    m._latest_signals[0] = {"signal": 1, "timeframe": "1m",
                            "timestamp": ORIGIN, "bar_close": ORIGIN_CLOSE,
                            "price": 100.0}
    events = await m._run_state_machine(0)

    assert [e.event_type for e in events] == []
    assert list(m._active_chains) == ["3956bf27-23c9-4bac-af42-12d22ef37ef9"]


@pytest.mark.asyncio
async def test_direction_flip_still_initiates():
    # The guard must not over-suppress: a genuine baseline reversal is a new
    # chain, and its id differs because direction is part of the identity.
    m = _machine(last_base_signal=(1, ORIGIN))
    await m.hydrate()

    flip_ts = "2026-07-29T12:01:00+00:00"
    m._latest_signals[0] = {"signal": -1, "timeframe": "1m",
                            "timestamp": flip_ts, "bar_close": "2026-07-29T12:02:00+00:00",
                            "price": 99.0}
    events = await m._run_state_machine(0)

    assert [e.event_type for e in events] == ["chain_initiated"]
    assert events[0].chain_id == chain_id_for(
        "BTCUSDT", "crypto", "FC", -1, flip_ts)