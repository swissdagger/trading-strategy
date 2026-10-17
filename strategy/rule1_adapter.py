"""
strategy/rule1_adapter.py — runs the owner's Rule 1 (strategy/rule1.py) as a
registry strategy on the dense equity ladder, WITHOUT modifying rule1.py.

What rule1.py is: pure logic with its own trade state machine. It consumes
chain events + a clock and returns OPEN/CLOSE intents for "a broker"; it
expects acks (confirm_open / reject / confirm_close). It never reads the wall
clock, so the same object runs live and in replay.

What this adapter does, and only this:

  1. EVENTS. The runtime hands each DL event (symbol-locked) to on_event().
     The adapter passes the raw dict to Rule1.on_chain_event(ev, now) with
     now = wall clock — the rule's own timing rule (every time used is when
     the event reached us, never the chain timestamp). Before a P2 event it
     awaits VolRatioProvider.prefetch so the rule's SYNC vol_ratio_fn is a
     cache hit.

  2. CLOCK. tick(now) calls Rule1.on_clock(now); the signal process runs it
     every `tick_seconds` (15 s). This is what fires the 1h stall exit and
     drains intents queued to the next regular session.

  3. INTENTS → trade_signals. OPEN → ENTER (trigger_level = the rule's P2),
     CLOSE → EXIT with the rule's reason mapped onto the contract's enum:
       stall_no_L3_1h → TIME, opposing_P1 → STOP
     (the rule's own reason string stays on the Trade and in the log).
     decision_price: the event's price for event-driven intents; for a
     clock-driven intent (queued entry at the open, stall exit) the latest
     1m close the vol source holds — there is no chain event to price it.
     decided_at: the event's timestamp, or now for a clock intent.

  4. ACKS. Equities have no venue here: the executor routes them to the
     shadow book, which fills at decision_price immediately. So a successful
     ENTER write is confirm_open(now) and a successful EXIT write is
     confirm_close(now); a failed write is reject(), which the rule retries
     on its next drain. The rule's "broker rejected on a holiday" branch is
     therefore never exercised — the shadow book fills on holidays too (D17).

  5. OWN STATE. The rule keeps all position state; the runtime's open-set
     tracking is bypassed (on_event returns None, the exit component is a
     no-op) because Rule1._drain emits intents for ANY symbol, not just the
     one whose event is being processed, and the runtime can only track one
     position per on_event. Restart hydration is therefore the adapter's:
     start() rebuilds Rule1.trades from open rule1 ENTERs in trade_signals.

  6. LEVELS. rule1.py is written against "P2 / level 3". On DL those are
     45s and 1m. `entry_level` (default 2 = the rule as frozen) shifts the
     level numbering the rule sees by (entry_level − 2) so the rule's "2"
     can be mapped onto a higher rung without editing the rule.

Concurrency: one asyncio.Lock per adapter around every call into the rule.
on_event already runs under the symbol lock (router) and then takes this
lock; tick takes only this lock and never a symbol lock, so there is no
lock-order cycle.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict, List, Optional

from execution.types import ExitReason, TradeSide
from strategy.rule1 import STALL_WINDOW, Intent, Rule1, Trade
from strategy.vol_source import VolRatioProvider

log = logging.getLogger("strategy.rule1")

REASON_MAP: Dict[str, ExitReason] = {
    "stall_no_L3_1h": ExitReason.TIME,
    "opposing_P1": ExitReason.STOP,
}

Clock = Callable[[], datetime]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Rule1Entry:
    """EntryComponent. Also owns the clock tick and the restart hydration;
    the registry's Strategy only knows it as `entry`."""

    def __init__(
        self,
        name: str,
        rule: Rule1,
        vol: VolRatioProvider,
        writer,
        *,
        latest_close: Callable[[str], Optional[float]],
        load_open: Optional[Callable[[], Awaitable[List[dict]]]] = None,
        clock: Clock = _utc_now,
        entry_level: int = 2,
    ):
        self._name = name
        self.rule = rule
        self._vol = vol
        self._writer = writer
        self._latest_close = latest_close
        self._load_open = load_open
        self._clock = clock
        self._offset = int(entry_level) - 2
        self._lock = asyncio.Lock()
        self.stats = {"opens": 0, "closes": 0, "write_failures": 0}

    # ----------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """Rebuild the rule's trades from open rule1 ENTERs so stall and
        opposing-P1 exits keep working across a restart."""
        if self._load_open is None:
            return
        now = self._clock()
        rows = await self._load_open()
        n = 0
        for r in rows:
            cid = r["chain_id"]
            if cid in self.rule.trades:
                continue
            opened = r["created_at"]
            if opened.tzinfo is None:
                opened = opened.replace(tzinfo=timezone.utc)
            t = Trade(
                chain_id=cid, symbol=r["symbol"],
                direction=1 if r["direction"] == "LONG" else -1,
                p2_at=opened, vol_ratio=float("nan"), state="open", entry_at=opened,
                # Older than the stall window and still open ⇒ L3 was seen
                # (or the stall exit fired already and we lost the row — the
                # opposing-P1 exit still closes it). Younger ⇒ the stall
                # clock keeps running from the real P2 time.
                l3_seen=(now - opened) > STALL_WINDOW,
            )
            self.rule.trades[cid] = t
            n += 1
        log.info("rule1 %s: restored %d open trade(s)", self._name, n)

    # -------------------------------------------------------------- events
    async def on_event(self, ev) -> Optional[str]:
        raw = getattr(ev, "_raw", None)
        if raw is None:
            raw = {k: getattr(ev, k, None) for k in (
                "event_type", "symbol", "asset_class", "direction", "chain_id",
                "propagation_level", "timestamp", "decision_price")}
            raw["metadata"] = {"propagation_level": raw.get("propagation_level")}
        if raw.get("asset_class") != "equity":
            return None
        now = self._clock()
        ev_for_rule = self._shift_levels(raw)

        if (raw.get("event_type") == "chain_propagated"
                and int(ev_for_rule["metadata"]["propagation_level"]) == 2):
            # The rule's vol_ratio_fn is sync; make this hour's ratio a cache hit.
            await self._vol.prefetch(raw["symbol"], now)

        async with self._lock:
            intents = self.rule.on_chain_event(ev_for_rule, now)
            await self._execute(intents, now,
                                event_symbol=raw["symbol"],
                                decided_at=str(raw.get("timestamp") or now.isoformat()),
                                event_price=raw.get("decision_price"),
                                trigger_level=raw.get("propagation_level"))
        return None   # the rule tracks its own positions (module docstring, 5)

    async def tick(self, now: Optional[datetime] = None) -> None:
        now = now or self._clock()
        async with self._lock:
            intents = self.rule.on_clock(now)
            await self._execute(intents, now, event_symbol=None,
                                decided_at=now.isoformat(), event_price=None,
                                trigger_level=None)

    # ------------------------------------------------------------- helpers
    def _shift_levels(self, raw: dict) -> dict:
        if self._offset == 0:
            return raw
        md = dict(raw.get("metadata") or {})
        lvl = md.get("propagation_level", raw.get("propagation_level"))
        if lvl is not None:
            md["propagation_level"] = int(lvl) - self._offset
        out = dict(raw)
        out["metadata"] = md
        if "propagation_level" in out and out["propagation_level"] is not None:
            out["propagation_level"] = int(out["propagation_level"]) - self._offset
        return out

    def _price_for(self, intent: Intent, event_symbol: Optional[str],
                   event_price: Optional[float]) -> Optional[float]:
        if event_symbol == intent.trade.symbol and event_price is not None:
            return float(event_price)
        return self._latest_close(intent.trade.symbol)

    async def _execute(self, intents: List[Intent], now: datetime, *,
                       event_symbol: Optional[str], decided_at: str,
                       event_price: Optional[float], trigger_level) -> None:
        for intent in intents:
            t = intent.trade
            side = TradeSide.LONG if t.direction > 0 else TradeSide.SHORT
            price = self._price_for(intent, event_symbol, event_price)
            try:
                if intent.action == "OPEN":
                    await self._writer.emit_entry(
                        chain_id=t.chain_id, side=side, symbol=t.symbol,
                        asset_class="equity", decided_at=decided_at,
                        decision_price=price,
                        trigger_level=2 + self._offset,
                    )
                    self.rule.confirm_open(t.chain_id, now)
                    self.stats["opens"] += 1
                    log.info("rule1 ENTER %s %s chain=%s vol_ratio=%.2f price=%s",
                             t.symbol, side.value, t.chain_id, t.vol_ratio, price)
                else:
                    reason = REASON_MAP.get(t.exit_reason or "", ExitReason.STOP)
                    await self._writer.emit_exit(
                        chain_id=t.chain_id, side=side, symbol=t.symbol,
                        asset_class="equity", reason=reason, decided_at=decided_at,
                        decision_price=price,
                        trigger_level=int(trigger_level) if trigger_level is not None else None,
                    )
                    self.rule.confirm_close(t.chain_id, now)
                    self.stats["closes"] += 1
                    log.info("rule1 EXIT %s %s chain=%s reason=%s (%s) price=%s",
                             t.symbol, side.value, t.chain_id, t.exit_reason,
                             reason.value, price)
            except Exception:
                # The rule retries the intent on its next drain (event or tick).
                self.stats["write_failures"] += 1
                self.rule.reject(t.chain_id, intent.action)
                log.exception("rule1 %s write failed for %s chain=%s — will retry",
                              intent.action, t.symbol, t.chain_id)


class Rule1Exit:
    """ExitComponent: exits are decided and written by Rule1 via the entry
    component's intents, so the runtime's per-event exit sweep is a no-op."""

    async def maybe_emit_exit(self, **_) -> Optional[str]:
        return None


def make_rule1(*, name, symbols, ladders, params, view_registry, writer_factory,
               vol_provider: VolRatioProvider,
               latest_close: Callable[[str], Optional[float]],
               load_open=None, clock: Clock = _utc_now):
    """Strategy factory. params: entry_level (default 2 — the rule as frozen)."""
    from strategy.registry import Strategy

    if len(ladders) != 1:
        raise ValueError(f"rule1 strategy {name!r} must declare exactly one ladder, got {ladders}")
    ladder = view_registry.get(ladders[0])
    entry_level = int(params.get("entry_level", 2))
    if not 1 <= entry_level < len(ladder.timeframes):
        raise ValueError(f"rule1 entry_level={entry_level} outside ladder {ladder.timeframes}")
    rule = Rule1(vol_ratio_fn=vol_provider.get)
    entry = Rule1Entry(name, rule, vol_provider, writer_factory(name),
                       latest_close=latest_close, load_open=load_open,
                       clock=clock, entry_level=entry_level)
    log.info("rule1 %s: entry at ladder level %d (%s), stall level %d (%s); "
             "vol filter and stall window are rule1.py's constants",
             name, entry_level, ladder.timeframes[entry_level - 1],
             entry_level + 1, ladder.timeframes[entry_level])
    return Strategy(name=name, entry=entry, exit=Rule1Exit(), symbols=symbols, ladders=ladders)
