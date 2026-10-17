"""
Tests for the ADR-001 Phase 3 executor hardening:

  * netted per-symbol reconcile
  * orphan venue balances refuse boot (dust and allowlist excepted)
  * side gate: SHORT never reaches a venue that can't short; it goes to the
    shadow book (virtual fills at the touch, fees on both legs)
  * open-position cap, stale-ENTER gate, price-increment rounding, venue minimums
  * sizing off available equity
  * the listener's cursor-advance callback (persisted cursor)

Reuses the fakes from test_signal_executor so the executor is driven exactly
as in its own tests.
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from test_signal_executor import FakeAdapter, enter_row, exit_row, make_hooks

from execution.gated_adapter import GatedAdapter
from execution.pricing import quantize_limit_price
from execution.shadow_book import ShadowBook, ShadowHooks, ShadowPosition, net_pnl_bps
from execution.signal_executor import OpenTrade, OrphanPositionError, SignalExecutor
from execution.signal_listener import SignalListener
from execution.sizing import make_size_position
from execution.types import (
    AccountEquity, MinOrderSize, OrderIntent, ReconcileResult, Touch, TradeSide,
    VenuePosition,
)


# --------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------

class SpotAdapter(FakeAdapter):
    """FakeAdapter plus the reads Phase 3 uses. Long-only by default."""
    venue = "fake-spot"
    supported_classes = {"equity"}
    supports_short = False

    def __init__(self, *, step=None, price_increment=None, mins=None):
        super().__init__()
        self.step = step
        self.price_increment = price_increment
        self.mins = mins

    async def get_step_size(self, symbol, asset_class):
        return self.step

    async def get_price_increment(self, symbol, asset_class):
        return self.price_increment

    async def get_min_order_size(self, symbol, asset_class):
        return self.mins


def positions(**qty_by_symbol):
    return ReconcileResult(
        positions=[VenuePosition(symbol=s, qty=Decimal(str(q)))
                   for s, q in qty_by_symbol.items()],
        open_orders=[], recent_fills=[])


def db_trade(trade_id, symbol="AAPL", qty="10", side=TradeSide.LONG, state="OPEN",
             chain=None):
    q = Decimal(qty)
    return OpenTrade(
        trade_id=trade_id, strategy="s", position_key=f"{chain or trade_id}:L",
        symbol=symbol, asset_class="equity", side=side, target_qty=q,
        filled_qty=q, state=state, entry_client_order_id=f"e-{trade_id}",
    )


class MemShadowStore:
    """In-memory shadow_trades."""
    def __init__(self, touch=Touch(Decimal("100"), Decimal("100.10"))):
        self.touch = touch
        self.rows = {}
        self.now = 1_000_000

    def hooks(self):
        async def get_touch(sym, ac):
            return self.touch
        async def insert_entry(row):
            k = (row["strategy"], row["position_key"])
            if k in self.rows:
                return False
            self.rows[k] = dict(row)
            return True
        async def close(row):
            k = (row["strategy"], row["position_key"])
            r = self.rows.get(k)
            if r is None or r.get("exit_at") is not None:
                return False
            r.update(row)
            return True
        async def load_open():
            return [ShadowPosition(
                strategy=r["strategy"], position_key=r["position_key"],
                symbol=r["symbol"], asset_class=r["asset_class"],
                side=TradeSide(r["side"]), entry_signal_id=r["entry_signal_id"],
                entry_px=r["entry_px"], fee_bps=r["fee_bps"])
                for r in self.rows.values() if r.get("exit_at") is None]
        return ShadowHooks(get_touch=get_touch, insert_entry=insert_entry,
                           close=close, load_open=load_open, now_ms=lambda: self.now)


async def booted(adapter=None, hooks=None, **kw):
    adapter = adapter or SpotAdapter()
    if hooks is None:
        hooks, _ = make_hooks()
    ex = SignalExecutor(adapter, hooks, **kw)
    await ex.start()
    return ex, adapter


# --------------------------------------------------------------------
# Netted reconcile
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_two_trades_one_symbol_netted_match_keeps_both():
    adapter = SpotAdapter()
    adapter.reconcile_result = positions(AAPL=15)
    hooks, state = make_hooks(open_trades=[db_trade("t1", qty="10"), db_trade("t2", qty="5")])
    ex, _ = await booted(adapter, hooks)
    assert {t.state for t in ex._open.values()} == {"OPEN"}
    assert state["states"] == {}


@pytest.mark.asyncio
async def test_netted_mismatch_halts_every_trade_on_symbol():
    adapter = SpotAdapter()
    adapter.reconcile_result = positions(AAPL=12)
    hooks, state = make_hooks(open_trades=[db_trade("t1", qty="10"), db_trade("t2", qty="5")])
    ex, _ = await booted(adapter, hooks)
    assert state["states"]["t1"][0] == "HALTED"
    assert state["states"]["t2"][0] == "HALTED"
    assert "db_net=15" in state["states"]["t1"][1] and "venue=12" in state["states"]["t1"][1]


@pytest.mark.asyncio
async def test_venue_flat_halts_with_flat_reason():
    adapter = SpotAdapter()
    adapter.reconcile_result = positions()
    hooks, state = make_hooks(open_trades=[db_trade("t1"), db_trade("t2")])
    await booted(adapter, hooks)
    assert all(state["states"][t][1].startswith("reconcile: DB open but venue flat")
               for t in ("t1", "t2"))


@pytest.mark.asyncio
async def test_other_symbols_unaffected_by_a_mismatch():
    adapter = SpotAdapter()
    adapter.reconcile_result = positions(AAPL=1, MSFT=10)
    hooks, state = make_hooks(open_trades=[db_trade("t1", "AAPL"), db_trade("t2", "MSFT")])
    await booted(adapter, hooks)
    assert state["states"]["t1"][0] == "HALTED"
    assert "t2" not in state["states"]


@pytest.mark.asyncio
async def test_halted_trade_quantity_counts_in_the_net():
    # The HALTED trade's 5 is still at the venue: venue 15 = 10 live + 5 halted.
    adapter = SpotAdapter()
    adapter.reconcile_result = positions(AAPL=15)
    hooks, state = make_hooks(open_trades=[
        db_trade("t1", qty="10"), db_trade("t2", qty="5", state="HALTED")])
    await booted(adapter, hooks)
    assert "t1" not in state["states"]


@pytest.mark.asyncio
@pytest.mark.parametrize("venue_qty,halts", [("10.00004", False), ("10.0002", True)])
async def test_tolerance_is_one_venue_quantity_increment(venue_qty, halts):
    adapter = SpotAdapter(step=Decimal("0.0001"))
    adapter.reconcile_result = positions(AAPL=venue_qty)
    hooks, state = make_hooks(open_trades=[db_trade("t1", qty="10")])
    await booted(adapter, hooks)
    assert ("t1" in state["states"]) is halts


# --------------------------------------------------------------------
# Orphan balances refuse boot
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_orphan_balance_refuses_boot():
    adapter = SpotAdapter()
    adapter.reconcile_result = positions(**{"ETH-USD": "2"})
    hooks, _ = make_hooks()
    ex = SignalExecutor(adapter, hooks)
    with pytest.raises(OrphanPositionError, match="ETH-USD"):
        await ex.start()
    assert not ex._booted
    assert not adapter.fill_stream_started


@pytest.mark.asyncio
@pytest.mark.parametrize("allow", [frozenset({"ETH"}), frozenset({"ETH-USD"})])
async def test_allowlisted_balance_does_not_refuse_boot(allow):
    adapter = SpotAdapter()
    adapter.reconcile_result = positions(**{"ETH-USD": "2"})
    ex, _ = await booted(adapter, orphan_allowlist=allow)
    assert ex._booted


@pytest.mark.asyncio
async def test_dust_below_min_order_size_does_not_refuse_boot():
    adapter = SpotAdapter(mins=MinOrderSize(base=Decimal("0.001")))
    adapter.reconcile_result = positions(**{"ETH-USD": "0.0004"})
    ex, _ = await booted(adapter)
    assert ex._booted


# --------------------------------------------------------------------
# Side gate + shadow book
# --------------------------------------------------------------------

async def shadow_executor(store=None, **kw):
    store = store or MemShadowStore()
    book = ShadowBook(store.hooks(), taker_fee_bps=Decimal("120"))
    await book.start()
    ex, adapter = await booted(shadow_book=book, **kw)
    return ex, adapter, store


@pytest.mark.asyncio
async def test_short_enter_goes_to_shadow_never_to_venue():
    hooks, state = make_hooks()
    ex, adapter, store = await shadow_executor(hooks=hooks)
    await ex.handle_signal(enter_row(direction="SHORT"))
    assert adapter.submitted == []
    assert state["trades"] == {}
    row = store.rows[("two_ladder", "chainX:S")]
    assert row["side"] == "SHORT"
    assert row["entry_px"] == Decimal("100")       # a short SELLS: the bid
    assert row["fee_bps"] == Decimal("120")


@pytest.mark.asyncio
async def test_short_exit_closes_shadow_at_ask_net_of_fees():
    ex, adapter, store = await shadow_executor()
    await ex.handle_signal(enter_row(direction="SHORT"))
    store.touch = Touch(Decimal("98"), Decimal("98.10"))
    await ex.handle_signal(exit_row(direction="SHORT"))
    row = store.rows[("two_ladder", "chainX:S")]
    assert row["exit_px"] == Decimal("98.10")      # a short BUYS back: the ask
    # (100 - 98.10) / 100 = 190 bps gross, minus 2 x 120 bps fees.
    assert row["pnl_bps"] == Decimal("-50.0000")
    assert row["exit_reason"] == "TARGET"
    assert adapter.submitted == []
    assert not ex.shadow_book.holds("two_ladder", "chainX:S")


@pytest.mark.asyncio
async def test_shadow_entry_ignores_kill_switch_and_position_cap():
    hooks, _ = make_hooks(kill=True)
    ex, _, store = await shadow_executor(hooks=hooks, max_open_positions=0)
    await ex.handle_signal(enter_row(direction="SHORT"))
    assert ("two_ladder", "chainX:S") in store.rows


@pytest.mark.asyncio
async def test_short_without_shadow_book_is_skipped():
    ex, adapter = await booted()
    await ex.handle_signal(enter_row(direction="SHORT"))
    assert adapter.submitted == []
    assert ex._open == {}


@pytest.mark.asyncio
async def test_venue_that_can_short_still_gets_the_order():
    adapter = SpotAdapter()
    adapter.supports_short = True
    ex, _ = await booted(adapter)
    await ex.handle_signal(enter_row(direction="SHORT"))
    assert len(adapter.submitted) == 1
    assert adapter.submitted[0].side == TradeSide.SHORT


@pytest.mark.asyncio
async def test_long_still_trades_with_shadow_book_present():
    ex, adapter, store = await shadow_executor()
    await ex.handle_signal(enter_row(direction="LONG"))
    assert len(adapter.submitted) == 1
    assert store.rows == {}


@pytest.mark.asyncio
async def test_shadow_replayed_enter_and_restart():
    store = MemShadowStore()
    ex, _, _ = await shadow_executor(store)
    await ex.handle_signal(enter_row(direction="SHORT"))
    await ex.handle_signal(enter_row(direction="SHORT"))   # replay
    assert len(store.rows) == 1
    # Restart: a fresh book reloads the open position and can close it.
    ex2, _, _ = await shadow_executor(store)
    assert ex2.shadow_book.holds("two_ladder", "chainX:S")
    await ex2.handle_signal(exit_row(direction="SHORT"))
    assert store.rows[("two_ladder", "chainX:S")]["exit_px"] == Decimal("100.10")


@pytest.mark.asyncio
async def test_shadow_falls_back_to_decision_price_without_touch():
    store = MemShadowStore(touch=None)
    ex, _, _ = await shadow_executor(store)
    await ex.handle_signal(enter_row(direction="SHORT"))
    assert store.rows[("two_ladder", "chainX:S")]["entry_px"] == Decimal("100.0")


@pytest.mark.asyncio
async def test_shadow_ignores_scale_tranches():
    ex, _, store = await shadow_executor()
    await ex.handle_signal(enter_row(direction="SHORT"))
    await ex.handle_signal(exit_row(direction="SHORT", reason="SCALE"))
    assert ex.shadow_book.holds("two_ladder", "chainX:S")
    assert "exit_px" not in store.rows[("two_ladder", "chainX:S")]


def test_net_pnl_bps_long_and_short():
    fee = Decimal("10")
    assert net_pnl_bps(TradeSide.LONG, Decimal("100"), Decimal("101"), fee) == Decimal("80.0000")
    assert net_pnl_bps(TradeSide.SHORT, Decimal("100"), Decimal("101"), fee) == Decimal("-120.0000")


def test_gated_adapter_mirrors_supports_short_and_is_concrete():
    inner = SpotAdapter()
    inner.supports_short = True
    gated = GatedAdapter(inner, enable_state=None)   # constructible = not abstract
    assert gated.supports_short is True


# --------------------------------------------------------------------
# Open-position cap
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_max_open_positions_refuses_entry_at_cap():
    ex, adapter = await booted(max_open_positions=1)
    await ex.handle_signal(enter_row(chain="c1", symbol="AAPL"))
    await ex.handle_signal(enter_row(chain="c2", symbol="MSFT"))
    assert [o.symbol for o in adapter.submitted] == ["AAPL"]


@pytest.mark.asyncio
async def test_slot_frees_when_position_closes():
    ex, adapter = await booted(max_open_positions=1)
    await ex.handle_signal(enter_row(chain="c1", symbol="AAPL"))
    await ex.handle_signal(exit_row(chain="c1", symbol="AAPL"))   # fills synchronously
    await ex.handle_signal(enter_row(chain="c2", symbol="MSFT"))
    assert [o.symbol for o in adapter.submitted] == ["AAPL", "AAPL", "MSFT"]


@pytest.mark.asyncio
async def test_halted_trade_occupies_a_slot():
    adapter = SpotAdapter()
    adapter.reconcile_result = positions(MSFT=10)
    hooks, _ = make_hooks(open_trades=[db_trade("t1", "MSFT", state="HALTED")])
    ex, _ = await booted(adapter, hooks, max_open_positions=1)
    await ex.handle_signal(enter_row(chain="c1", symbol="AAPL"))
    assert adapter.submitted == []


# --------------------------------------------------------------------
# Stale ENTER gate
# --------------------------------------------------------------------

def _at(ms_ago, now_ms=1_000_000):
    return datetime.fromtimestamp((now_ms - ms_ago) / 1000, tz=timezone.utc)


@pytest.mark.asyncio
@pytest.mark.parametrize("age_ms,enters", [(119_000, True), (121_000, False)])
async def test_enter_older_than_max_age_is_skipped(age_ms, enters):
    ex, adapter = await booted(enter_max_age_ms=120_000)
    row = enter_row()
    row["created_at"] = _at(age_ms)
    await ex.handle_signal(row)
    assert bool(adapter.submitted) is enters


@pytest.mark.asyncio
async def test_old_decided_at_does_not_make_a_fresh_signal_stale():
    # A 15m-triggered signal: decided_at is the bar's window start, 16 minutes
    # back, but the row was written just now. It must be acted on.
    ex, adapter = await booted(enter_max_age_ms=120_000)
    row = enter_row()
    row["decided_at"] = _at(16 * 60_000)
    row["created_at"] = _at(1_000)
    await ex.handle_signal(row)
    assert len(adapter.submitted) == 1


@pytest.mark.asyncio
async def test_iso_string_created_at_is_understood():
    ex, adapter = await booted(enter_max_age_ms=120_000)
    row = enter_row()
    row["created_at"] = _at(600_000).isoformat()
    await ex.handle_signal(row)
    assert adapter.submitted == []


@pytest.mark.asyncio
async def test_stale_exit_is_still_honoured():
    ex, adapter = await booted(enter_max_age_ms=120_000)
    await ex.handle_signal(enter_row())
    row = exit_row()
    row["created_at"] = _at(3_600_000)
    await ex.handle_signal(row)
    assert [o.intent for o in adapter.submitted] == [OrderIntent.ENTRY, OrderIntent.EXIT]


@pytest.mark.asyncio
async def test_stale_short_does_not_reach_the_shadow_book():
    ex, _, store = await shadow_executor(enter_max_age_ms=120_000)
    row = enter_row(direction="SHORT")
    row["created_at"] = _at(600_000)
    await ex.handle_signal(row)
    assert store.rows == {}


# --------------------------------------------------------------------
# Price increment and venue minimums
# --------------------------------------------------------------------

@pytest.mark.parametrize("side,intent,expected", [
    (TradeSide.LONG, OrderIntent.ENTRY, Decimal("100.12")),   # buy: floor
    (TradeSide.LONG, OrderIntent.EXIT, Decimal("100.13")),    # sell: ceil
    (TradeSide.SHORT, OrderIntent.ENTRY, Decimal("100.13")),  # sell: ceil
    (TradeSide.SHORT, OrderIntent.EXIT, Decimal("100.12")),   # buy: floor
])
def test_quantize_limit_rounds_toward_the_touch(side, intent, expected):
    assert quantize_limit_price(Decimal("100.1234"), side, intent, Decimal("0.01")) == expected


def test_quantize_limit_without_increment_is_unchanged():
    assert quantize_limit_price(Decimal("1.2345"), TradeSide.LONG, OrderIntent.ENTRY, None) == Decimal("1.2345")


@pytest.mark.asyncio
async def test_entry_limit_is_quantized_to_price_increment():
    # touch 100 / 100.10 with the test caps: crossed buy limit = 100.20.
    hooks, _ = make_hooks(touch=Touch(Decimal("100.003"), Decimal("100.107")))
    ex, adapter = await booted(SpotAdapter(price_increment=Decimal("0.05")), hooks)
    await ex.handle_signal(enter_row())
    limit = adapter.submitted[0].limit_price
    assert limit % Decimal("0.05") == 0
    assert limit >= Decimal("100.107")   # floored, but never below the ask


@pytest.mark.asyncio
async def test_exit_limit_is_quantized_to_price_increment():
    ex, adapter = await booted(SpotAdapter(price_increment=Decimal("0.05")))
    await ex.handle_signal(enter_row())
    await ex.handle_signal(exit_row())
    assert adapter.submitted[1].limit_price % Decimal("0.05") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("mins,enters", [
    (MinOrderSize(base=Decimal("11")), False),     # size 10 < 11 base
    (MinOrderSize(quote=Decimal("5000")), False),  # 10 x ~100 < 5000 notional
    (MinOrderSize(base=Decimal("1"), quote=Decimal("10")), True),
])
async def test_entry_below_venue_minimum_is_refused(mins, enters):
    hooks, state = make_hooks()
    ex, adapter = await booted(SpotAdapter(mins=mins), hooks)
    await ex.handle_signal(enter_row())
    assert bool(adapter.submitted) is enters
    assert bool(state["trades"]) is enters


# --------------------------------------------------------------------
# Sizing off available equity
# --------------------------------------------------------------------

class SizingAdapter:
    def __init__(self, equity):
        self.equity = equity
    async def get_account_equity(self):
        return self.equity
    async def get_touch(self, symbol, asset_class):
        return Touch(Decimal("99"), Decimal("101"))
    async def get_step_size(self, symbol, asset_class):
        return None


@pytest.mark.asyncio
async def test_sizing_uses_available_not_total():
    eq = AccountEquity(total=Decimal("100000"), available=Decimal("20000"), currency="USD")
    size = make_size_position(lambda: SizingAdapter(eq))
    # 5% of 20,000 available / mid 100 = 10.
    assert await size("BTC-USD", "crypto", TradeSide.LONG, None) == Decimal("10")


@pytest.mark.asyncio
async def test_sizing_zero_available_places_nothing():
    eq = AccountEquity(total=Decimal("100000"), available=Decimal("0"), currency="USD")
    size = make_size_position(lambda: SizingAdapter(eq))
    assert await size("BTC-USD", "crypto", TradeSide.LONG, None) == Decimal("0")


# --------------------------------------------------------------------
# Listener cursor persistence
# --------------------------------------------------------------------

def _listener(handler, on_advance):
    return SignalListener(dsn="unused", pool=None, handler=handler,
                          initial_cursor=datetime(2026, 1, 1, tzinfo=timezone.utc),
                          on_cursor_advance=on_advance)


@pytest.mark.asyncio
async def test_cursor_advance_is_persisted_after_dispatch():
    saved = []
    async def handler(row): pass
    async def save(ts): saved.append(ts)
    lst = _listener(handler, save)
    t = datetime(2026, 1, 2, tzinfo=timezone.utc)
    await lst._dispatch_row({"signal_id": "s1", "created_at": t})
    assert saved == [t] and lst.cursor == t
    # An older row (catch-up boundary) does not move or re-save the cursor.
    await lst._dispatch_row({"signal_id": "s0", "created_at": t - timedelta(hours=1)})
    assert saved == [t]


@pytest.mark.asyncio
async def test_failed_handler_does_not_advance_or_persist():
    saved = []
    async def handler(row): raise RuntimeError("boom")
    async def save(ts): saved.append(ts)
    lst = _listener(handler, save)
    await lst._dispatch_row({"signal_id": "s1",
                             "created_at": datetime(2026, 1, 2, tzinfo=timezone.utc)})
    assert saved == []


@pytest.mark.asyncio
async def test_failed_cursor_save_does_not_break_delivery():
    handled = []
    async def handler(row): handled.append(row["signal_id"])
    async def save(ts): raise ConnectionError("db down")
    lst = _listener(handler, save)
    t = datetime(2026, 1, 2, tzinfo=timezone.utc)
    await lst._dispatch_row({"signal_id": "s1", "created_at": t})
    assert handled == ["s1"] and lst.cursor == t


# --------------------------------------------------------------------
# Asset-class gate (PR 9): a class the venue does not trade is shadowed
# --------------------------------------------------------------------
# The fixtures' SpotAdapter declares supported_classes = {"equity"} and the
# row helpers default to equity, so "unsupported" here is a CRYPTO row on
# that adapter — the mirror image of production (equity rows on Coinbase).

def _crypto(row: dict) -> dict:
    return {**row, "symbol": "BTC-USD", "asset_class": "crypto"}


@pytest.mark.asyncio
async def test_unsupported_asset_class_long_enter_goes_to_shadow_never_to_venue():
    hooks, state = make_hooks()
    ex, adapter, store = await shadow_executor(hooks=hooks)
    await ex.handle_signal(_crypto(enter_row(direction="LONG")))
    assert adapter.submitted == []                  # never sized, never sent
    assert state["trades"] == {}
    row = store.rows[("two_ladder", "chainX:L")]
    assert row["side"] == "LONG" and row["symbol"] == "BTC-USD"


@pytest.mark.asyncio
async def test_unsupported_asset_class_exit_closes_the_shadow_position():
    ex, adapter, store = await shadow_executor()
    await ex.handle_signal(_crypto(enter_row(direction="LONG")))
    await ex.handle_signal(_crypto(exit_row(direction="LONG")))
    row = store.rows[("two_ladder", "chainX:L")]
    assert row["exit_px"] is not None
    assert adapter.submitted == []
    assert not ex.shadow_book.holds("two_ladder", "chainX:L")


@pytest.mark.asyncio
async def test_unsupported_asset_class_without_shadow_book_is_skipped():
    ex, adapter = await booted()
    await ex.handle_signal(_crypto(enter_row(direction="LONG")))
    assert adapter.submitted == []


@pytest.mark.asyncio
async def test_supported_asset_class_is_not_shadowed():
    # Control: the same LONG on the adapter's own class reaches the venue.
    ex, adapter, store = await shadow_executor()
    await ex.handle_signal(enter_row(direction="LONG"))
    assert adapter.submitted != []
    assert ("two_ladder", "chainX:L") not in store.rows


# --------------------------------------------------------------------
# Shadow-only strategies (PR 12): never a venue order, in any mode
# --------------------------------------------------------------------

@pytest.mark.asyncio
async def test_shadow_only_strategy_long_on_supported_class_is_shadowed():
    ex, adapter, store = await shadow_executor(shadow_only_strategies=frozenset({"two_ladder"}))
    await ex.handle_signal(enter_row(direction="LONG"))           # equity on the equity adapter
    assert adapter.submitted == []
    assert ("two_ladder", "chainX:L") in store.rows


@pytest.mark.asyncio
async def test_non_shadow_only_strategy_still_reaches_venue():
    ex, adapter, store = await shadow_executor(shadow_only_strategies=frozenset({"rule3_v1"}))
    await ex.handle_signal(enter_row(direction="LONG"))
    assert adapter.submitted != [] and ("two_ladder", "chainX:L") not in store.rows


@pytest.mark.asyncio
async def test_shadow_fee_per_asset_class_override():
    from decimal import Decimal
    from execution.shadow_book import ShadowBook
    store = MemShadowStore()
    book = ShadowBook(store.hooks(), taker_fee_bps=Decimal("120"), fee_bps_by_class={"equity": 5})
    assert await book._fee_bps("equity") == Decimal("5")
    assert await book._fee_bps("crypto") == Decimal("120")
    assert await book._fee_bps(None) == Decimal("120")


# --- SignalWriter: whole-position exits only, strategy required ---------------

def test_signal_writer_requires_a_strategy():
    from execution.signal_writer import SignalWriter, SignalWriterHooks
    hooks = SignalWriterHooks(insert_signal=None)
    with pytest.raises(TypeError):
        SignalWriter(hooks)
    with pytest.raises(ValueError):
        SignalWriter(hooks, "")


@pytest.mark.asyncio
async def test_signal_writer_exit_is_whole_position_and_refuses_scale():
    from execution.signal_writer import SignalWriter, SignalWriterHooks, _signal_id
    from execution.types import ExitReason
    rows = []
    async def insert(row):
        rows.append(row)
        return True
    w = SignalWriter(SignalWriterHooks(insert_signal=insert), strategy="s1")
    kw = dict(chain_id="c1", side=TradeSide.LONG, symbol="ETH-USD",
              asset_class="crypto", decided_at="2026-01-01T00:00:00+00:00")
    await w.emit_exit(reason=ExitReason.TERMINAL, **kw)
    (row,) = rows
    assert row["fraction"] == 1.0 and row["exit_reason"] == "TERMINAL"
    # Same id scheme as before: the reason alone is the discriminator.
    assert row["signal_id"] == _signal_id("s1", "c1:L", "EXIT", "TERMINAL")
    with pytest.raises(ValueError):
        await w.emit_exit(reason=ExitReason.SCALE, **kw)
    assert len(rows) == 1
