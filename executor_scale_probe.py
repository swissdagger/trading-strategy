"""Step-4 verification: the partial-exit (scale-out) executor machinery,
driven end-to-end through handle_signal with a fake adapter + fake hooks."""
import asyncio
from decimal import Decimal
from types import SimpleNamespace

from execution.signal_executor import (
    SignalExecutor, ExecutorHooks, OpenTrade, ExitCap, EscalationRung, _esc,
)
from execution.pricing import CapConfig
from execution import client_order_id as coid
from execution.types import (
    Fill, OrderIntent, OrderResult, OrderStatus, Touch, TradeSide,
)

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  PASS  {name}")
    else:
        FAIL += 1; print(f"  FAIL  {name}  {detail}")


class FakeAdapter:
    def __init__(self):
        self.orders = []        # every OrderRequest submitted
        self.cancels = []       # every client_order_id cancelled
        self._exec_seq = 0

    def _fill(self, order, qty):
        self._exec_seq += 1
        return Fill(venue_exec_id=f"E{self._exec_seq}", qty=qty,
                    price=Decimal("100"), intent=order.intent,
                    filled_at_ms=0, client_order_id=order.client_order_id)

    async def submit_order(self, order):
        self.orders.append(order)
        if order.intent == OrderIntent.ENTRY:
            # Entries fill synchronously in full (marketable limit).
            return OrderResult(client_order_id=order.client_order_id,
                               status=OrderStatus.FILLED, venue_order_id=f"V{len(self.orders)}",
                               filled_qty=order.qty, fills=[self._fill(order, order.qty)])
        # Exits ack WORKING; tests deliver fills via the stream.
        return OrderResult(client_order_id=order.client_order_id,
                           status=OrderStatus.WORKING, venue_order_id=f"V{len(self.orders)}")

    async def cancel_order(self, client_order_id):
        self.cancels.append(client_order_id)
        return OrderResult(client_order_id=client_order_id, status=OrderStatus.CANCELLED)


class Env:
    def __init__(self, quantize=None):
        self.adapter = FakeAdapter()
        self.persisted = []          # snapshots of persist_trade calls
        self.states = []             # (trade_id, state) history
        self.exit_states = []        # (trade_id, rung, deadline) history
        self.now = [1_000_000]
        seen_fills = set()

        async def persist_trade(t):
            self.persisted.append((t.trade_id, t.state, dict(t.scale_banked),
                                   t.exit_reason, t.filled_qty, t.exit_filled_qty))
        async def update_state(tid, state, reason): self.states.append((tid, state))
        async def persist_order(row): pass
        async def persist_fill(row):
            if row["venue_exec_id"] in seen_fills: return False
            seen_fills.add(row["venue_exec_id"]); return True
        async def advance(cid, q, s): pass
        async def load_open(): return []
        async def get_touch(sym, ac): return Touch(bid=Decimal("99.99"), ask=Decimal("100.01"))
        async def get_cap(ac, intent):
            return CapConfig(cross_spread_mult=Decimal(1), cap_spread_mult=Decimal(2),
                             cap_abs_bps=Decimal(50))
        async def size(sym, ac, side, px): return Decimal(100)
        async def get_exit_cap(ac):
            return ExitCap(
                base=CapConfig(cross_spread_mult=Decimal("1.5"),
                               cap_spread_mult=Decimal(3), cap_abs_bps=Decimal(50)),
                ladder=(EscalationRung(Decimal(4), 10), EscalationRung(Decimal(6), 15)),
            )
        async def persist_exit_state(tid, rung, dl): self.exit_states.append((tid, rung, dl))
        async def ks(): return False

        self.hooks = ExecutorHooks(
            persist_trade=persist_trade, update_trade_state=update_state,
            persist_order=persist_order, persist_fill=persist_fill,
            advance_order_fill=advance, load_open_trades=load_open,
            get_touch=get_touch, get_cap=get_cap, size_position=size,
            get_exit_cap=get_exit_cap, persist_exit_state=persist_exit_state,
            kill_switch_blocks=ks, now_ms=lambda: self.now[0],
            quantize_exit_qty=quantize,
        )
        self.ex = SignalExecutor(adapter=self.adapter, hooks=self.hooks)
        self.ex._booted = True

    # -- drivers ------------------------------------------------------------
    async def enter(self, pkey="chain1:L"):
        await self.ex.handle_signal({
            "signal_id": f"sig-enter-{pkey}", "position_key": pkey,
            "chain_id": pkey.split(":")[0], "direction": "LONG", "action": "ENTER",
            "symbol": "BTC-USD", "asset_class": "crypto",
            "strategy": "prior_failure_scaleout", "trigger_level": 2,
            "decision_price": 100.0, "exit_reason": None, "fraction": 1.0,
            "decided_at": "t"})
        return ("prior_failure_scaleout", pkey)

    async def scale(self, level, fraction, pkey="chain1:L"):
        await self.ex.handle_signal({
            "signal_id": f"sig-scale-{pkey}-{level}", "position_key": pkey,
            "chain_id": pkey.split(":")[0], "direction": "LONG", "action": "EXIT",
            "symbol": "BTC-USD", "asset_class": "crypto",
            "strategy": "prior_failure_scaleout", "trigger_level": level,
            "decision_price": 100.0, "exit_reason": "SCALE", "fraction": fraction,
            "decided_at": "t"})

    async def close(self, reason="TERMINAL", fraction=1.0, pkey="chain1:L"):
        await self.ex.handle_signal({
            "signal_id": f"sig-{reason}-{pkey}", "position_key": pkey,
            "chain_id": pkey.split(":")[0], "direction": "LONG", "action": "EXIT",
            "symbol": "BTC-USD", "asset_class": "crypto",
            "strategy": "prior_failure_scaleout", "trigger_level": None,
            "decision_price": 100.0, "exit_reason": reason, "fraction": fraction,
            "decided_at": "t"})

    async def fill_last_exit(self, qty=None):
        order = self.exit_orders()[-1]
        f = self.adapter._fill(order, qty if qty is not None else order.qty)
        await self.ex._on_fill(f)

    def exit_orders(self):
        return [o for o in self.adapter.orders if o.intent == OrderIntent.EXIT]

    def trade(self, key):
        return self.ex._open.get(key)


async def main():
    # T1 — legacy whole-position exit: byte-compatible block-0 id, CLOSED.
    e = Env(); key = await e.enter()
    await e.close(reason="STOP")
    o = e.exit_orders()[0]
    check("T1 legacy STOP: full qty, block-0 rung id, CLOSED on fill",
          o.qty == Decimal(100) and coid.parse(o.client_order_id).rung == 0
          and (await e.fill_last_exit() or e.trade(key) is None),
          f"qty={o.qty} id={o.client_order_id}")

    # T2 — one tranche: 5% order in block 300, back to OPEN on fill.
    e = Env(); key = await e.enter()
    await e.scale(3, 0.05)
    o = e.exit_orders()[0]
    ok_order = o.qty == Decimal("5.00") and coid.parse(o.client_order_id).rung == 300
    await e.fill_last_exit()
    t = e.trade(key)
    check("T2 single tranche: qty 5 @rung 300; fill -> OPEN, exit_filled=5",
          ok_order and t is not None and t.state == "OPEN"
          and t.exit_filled_qty == Decimal(5),
          f"order={o.qty}/{o.client_order_id} state={t.state if t else None}")

    # T3 — coalescing: tranches arriving while one works fold into ONE next order.
    e = Env(); key = await e.enter()
    await e.scale(3, 0.05)                  # order 1 working (5)
    await e.scale(4, 0.05)                  # banks only
    await e.scale(5, 0.10)                  # banks only
    n_while_working = len(e.exit_orders())
    await e.fill_last_exit()                # tranche 1 fills -> continuation
    orders = e.exit_orders()
    o2 = orders[-1]
    check("T3 same-tick tranches coalesce: 1 order while working, then ONE for 15 @rung 500",
          n_while_working == 1 and len(orders) == 2
          and o2.qty == Decimal("15.00")
          and coid.parse(o2.client_order_id).rung == 500,
          f"n={len(orders)} qty={o2.qty} id={o2.client_order_id}")
    await e.fill_last_exit()
    t = e.trade(key)
    check("T3b after coalesced fill: OPEN with exit_filled=20",
          t.state == "OPEN" and t.exit_filled_qty == Decimal(20))

    # T4 — TERMINAL closes the remainder in block 0 and finalizes CLOSED.
    await e.close(reason="TERMINAL", fraction=0.80)
    o3 = e.exit_orders()[-1]
    ok = o3.qty == Decimal(80) and coid.parse(o3.client_order_id).rung == 0
    await e.fill_last_exit()
    check("T4 TERMINAL: remainder 80 @block 0; CLOSED",
          ok and e.trade(key) is None, f"qty={o3.qty} id={o3.client_order_id}")

    # T5 — replayed tranche is a no-op.
    e = Env(); key = await e.enter()
    await e.scale(3, 0.05); await e.fill_last_exit()
    n = len(e.exit_orders())
    await e.scale(3, 0.05)                  # replay (same trigger_level)
    check("T5 replayed SCALE (same level): no new order, banked unchanged",
          len(e.exit_orders()) == n and e.trade(key).scale_banked == {3: Decimal("0.05")})

    # T6 — tranche escalation: cancel prior rung, reissue at rung+1.
    e = Env(); key = await e.enter()
    await e.scale(3, 0.05)                  # working, deadline now+10s
    e.now[0] += 11_000
    await e.ex.run_escalation_once()
    o = e.exit_orders()[-1]
    check("T6 tranche escalation: cancels X.300, reissues X.301 for same deficit",
          e.adapter.cancels == [coid.build(e.trade(key).trade_id, OrderIntent.EXIT, 300)]
          and coid.parse(o.client_order_id).rung == 301 and o.qty == Decimal("5.00")
          and _esc(301) == 1,
          f"cancels={e.adapter.cancels} id={o.client_order_id}")

    # T7 — below-lot accumulation via the quantize hook.
    async def quantize(sym, ac, qty):
        return qty if qty >= Decimal(6) else Decimal(0)
    e = Env(quantize=quantize); key = await e.enter()
    await e.scale(3, 0.05)                  # deficit 5 < min 6 -> no order
    n_after_first = len(e.exit_orders())
    await e.scale(4, 0.05)                  # deficit 10 -> one order for 10
    o = e.exit_orders()[-1]
    check("T7 below-lot tranche accumulates into the next (no order, then 10 @rung 400)",
          n_after_first == 0 and len(e.exit_orders()) == 1
          and o.qty == Decimal("10.00") and coid.parse(o.client_order_id).rung == 400
          and e.trade(key).state == "CLOSING",
          f"n1={n_after_first} qty={o.qty}")

    # T8 — a final close supersedes a working tranche: cancel + full remainder.
    e = Env(); key = await e.enter()
    await e.scale(3, 0.05)                  # tranche working
    tid = e.trade(key).trade_id
    await e.close(reason="STOP")
    o = e.exit_orders()[-1]
    ok = (e.adapter.cancels == [coid.build(tid, OrderIntent.EXIT, 300)]
          and o.qty == Decimal(100) and coid.parse(o.client_order_id).rung == 0)
    await e.fill_last_exit()
    check("T8 STOP supersedes working tranche: cancel + full close @block 0 -> CLOSED",
          ok and e.trade(key) is None, f"cancels={e.adapter.cancels} qty={o.qty}")

    # T9 — all seven tranches -> CLOSED with no TERMINAL (and heavy coalescing).
    e = Env(); key = await e.enter()
    sched = {3: 0.05, 4: 0.05, 5: 0.10, 6: 0.10, 7: 0.15, 8: 0.20, 9: 0.35}
    for lvl, frac in sched.items():
        await e.scale(lvl, frac)
    await e.fill_last_exit()                # first order (5) fills
    await e.fill_last_exit()                # coalesced remainder (95) fills
    check("T9 seven tranches, two orders (5 then 95), CLOSED without TERMINAL",
          e.trade(key) is None and len(e.exit_orders()) == 2
          and [o.qty for o in e.exit_orders()] == [Decimal("5.00"), Decimal("95.00")],
          f"orders={[str(o.qty) for o in e.exit_orders()]}")

    # T10 — restart resume: a reloaded OPEN trade with banked state + partial
    # fills advances the correct deficit.
    e = Env()
    t = OpenTrade(trade_id="0" * 32, strategy="prior_failure_scaleout",
                  position_key="chainR:L", symbol="BTC-USD", asset_class="crypto",
                  side=TradeSide.LONG, target_qty=Decimal(100),
                  filled_qty=Decimal(100), state="OPEN",
                  entry_client_order_id=coid.build("0" * 32, OrderIntent.ENTRY, 0),
                  exit_filled_qty=Decimal(5))
    t.scale_banked = {3: Decimal("0.05"), 4: Decimal("0.05")}
    key = ("prior_failure_scaleout", "chainR:L")
    e.ex._open[key] = t
    await e.ex._advance_scale(t, key)
    o = e.exit_orders()[-1]
    check("T10 restart: banked {3,4}, exited 5 -> one order for the 5 deficit @rung 400",
          o.qty == Decimal("5.00") and coid.parse(o.client_order_id).rung == 400)

    # T11 — persist_trade snapshots carry the banked map (durable before issue).
    e = Env(); key = await e.enter()
    await e.scale(3, 0.05)
    banked_snaps = [b for (_, _, b, _, _, _) in e.persisted if b]
    check("T11 scale_banked persisted on accept (before the order goes out)",
          banked_snaps and banked_snaps[0] == {3: Decimal("0.05")},
          f"snaps={banked_snaps[:1]}")

    # T12 — over-banked producer bug clamps at the whole position.
    e = Env(); key = await e.enter()
    await e.scale(3, 0.9)
    await e.fill_last_exit()                # 90 exits
    await e.scale(4, 0.9)                   # sum 1.8 -> clamp: deficit 10
    o = e.exit_orders()[-1]
    await e.fill_last_exit()
    check("T12 over-banked sum clamps to position: second order 10, then CLOSED",
          o.qty == Decimal("10.0") and e.trade(key) is None
          and e.ex.metrics.counters.get("exits_scale_overbanked", 0) >= 1
          if hasattr(e.ex, "metrics") else o.qty == Decimal("10.0") and e.trade(key) is None,
          f"qty={o.qty}")

    print(f"\n{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))