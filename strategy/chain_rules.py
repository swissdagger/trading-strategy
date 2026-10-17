"""
strategy/chain_rules.py — runs the owner's frozen chain-following rules
(strategy/rules.py: Rule + RuleConfig, four configurations) as registry
strategies, without modifying rules.py.

rules.py is pure logic with its own trade state machine: events + a clock in,
OPEN/CLOSE intents out, broker acks back (confirm_open / reject /
confirm_close). It never reads the wall clock, so the same engine ran the
owner's replay validation (216/216 Rule 1 trades) and runs live here.

The adapter, and only this:

  1. CONFIG. `params["rule"]` names a frozen config in rules.py
     (RULE1_EQUITY, RULE3_CRYPTO, RULE3_NEW_LITERAL, RULE3_NEW_EQUIV). The
     effective config is that one with `name` = the strategy's registry name
     and `ladder` = the ladder the strategy declares, because this repo names
     ladders FC/ED/DL/CN where the package said default/new. Entry/confirm
     levels are the frozen ones unless `params["entry_tf"]` /
     `params["confirm_tf"]` name rungs explicitly; either way the resolved
     (level, rung) pairs are logged at start, since a level number means a
     different rung on each ladder (the whole reason the package has four
     rules and the pre-registration says never to pool them).

  2. EVENTS. The runtime hands each declared-ladder event (symbol-locked) to
     on_event(). The adapter passes the raw dict with `ladder` rewritten to
     the config's tag, and now = wall clock — the rules' timing rule. Before
     an entry-level event it awaits VolStateProvider.prefetch so the rule's
     SYNC vol_fn is a cache hit.

  3. CLOCK. tick() -> Rule.on_clock(now), every tick_seconds from the signal
     process: the stall exit and the session-open drain.

  4. INTENTS -> trade_signals. OPEN -> ENTER (trigger_level = entry level);
     CLOSE -> EXIT with the rule's reason mapped onto the contract's enum:
     stall_no_L*_*m -> TIME, opposing_P1 -> STOP (the rule's own string stays
     on the Trade and in the log). decision_price: the event's price for an
     event-driven intent; for a clock-driven one, the newest close the signal
     process has seen in the bar stream (strategy/vol_source.LiveCloses).
     decided_at: the event timestamp (the deciding bar's open label), or now;
     decided_bar_close: that bar's close for an event-driven intent, else None.

  5. ACKS. These strategies are shadow-only (config.SHADOW_ONLY_STRATEGIES
     and the executor's class gate): the shadow book fills at decision_price
     immediately, so a successful write is confirm_open/confirm_close(now)
     and a failed write is reject(), which the rule retries on its next
     drain. The package's PaperBroker (fill at the NEXT 1m price minus a
     half-spread) is not replicated; the real spread at each signal is in
     research_quotes and the review query applies it (docs/rule_review.sql).

  6. OWN STATE. The rule keeps all position state; the runtime's open-set
     tracking is bypassed (on_event returns None, exit component is a
     no-op), because Rule._drain emits for ANY symbol of the rule.
     start() rebuilds Rule.trades from open ENTERs in trade_signals and
     reconciles each against `chains`/`chain_steps`: `confirmed` is whether
     the chain reached the confirm level inside the stall window, and an
     opposing chain initiated on the rule's ladder after the entry (wall
     time, as the rule measures) marks the trade for exit (the package's
     load_open_trades does the same). Rule.log lines are logged and cleared
     after every call.

Concurrency: one asyncio.Lock per adapter around every call into the rule.
on_event runs under the symbol lock and then takes this lock; tick takes only
this lock and never a symbol lock — no cycle.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional

from execution.types import ExitReason, TradeSide
from ops.alerts import alert
from strategy import rules as R
from strategy.rules import Intent, Rule, RuleConfig, Trade, VolState

log = logging.getLogger("strategy.chain_rules")

FROZEN = {
    "RULE1_EQUITY": R.RULE1_EQUITY,
    "RULE3_CRYPTO": R.RULE3_CRYPTO,
    "RULE3_NEW_LITERAL": R.RULE3_NEW_LITERAL,
    "RULE3_NEW_EQUIV": R.RULE3_NEW_EQUIV,
}

Clock = Callable[[], datetime]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def exit_reason_for(rule_reason: Optional[str]) -> ExitReason:
    if rule_reason and rule_reason.startswith("stall_"):
        return ExitReason.TIME
    return ExitReason.STOP          # opposing_P1 (and anything unexpected: protective)


def effective_config(base: RuleConfig, *, name: str, ladder, params: dict) -> RuleConfig:
    """The frozen config, retagged for this repo's ladder naming, with rung
    overrides resolved against the actual ladder. Raises if a level does not
    exist on the ladder."""
    entry = int(params.get("entry_level", base.entry_level))
    confirm = int(params.get("confirm_level", base.confirm_level))
    if "entry_tf" in params:
        entry = ladder.level_of(params["entry_tf"])
    if "confirm_tf" in params:
        confirm = ladder.level_of(params["confirm_tf"])
    n = len(ladder.timeframes)
    if not (1 <= entry < confirm <= n):
        raise ValueError(f"{name}: entry level {entry} / confirm level {confirm} do not fit "
                         f"ladder {ladder.name} {ladder.timeframes}")
    return replace(base, name=name, ladder=ladder.name, entry_level=entry, confirm_level=confirm)


class ChainRuleEntry:
    def __init__(self, name: str, rule: Rule, vol, writer, *,
                 asset_class: str, latest_close: Callable[[str], Optional[float]],
                 load_open=None, load_opposing_since=None, load_confirmed=None,
                 symbols=(), clock: Clock = _utc_now):
        self._name = name
        self.rule = rule
        self._vol = vol
        self._writer = writer
        self._asset_class = asset_class
        self._latest_close = latest_close
        self._load_open = load_open
        self._load_opposing = load_opposing_since
        self._load_confirmed = load_confirmed
        self._symbols = [sym for sym in symbols if sym != "*"]
        self._clock = clock
        self._lock = asyncio.Lock()
        self._warm_task: Optional[asyncio.Task] = None
        self.stats = {"opens": 0, "closes": 0, "write_failures": 0, "reconciled_exits": 0}

    # ------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        # Seed the vol history (32 days of 1m closes per symbol) in the
        # background now, rather than on the first entry-level event, where it
        # ran under that symbol's lock and stalled its chain processing.
        if self._symbols and self._warm_task is None:
            self._warm_task = asyncio.create_task(self._warm_vol(),
                                                  name=f"vol-warm-{self._name}")
        if self._load_open is None:
            return
        now = self._clock()
        n = 0
        for r in await self._load_open():
            cid = r["chain_id"]
            if cid in self.rule.trades:
                continue
            opened = r["created_at"]
            if opened.tzinfo is None:
                opened = opened.replace(tzinfo=timezone.utc)
            stall = timedelta(minutes=self.rule.cfg.stall_minutes)
            confirmed = None
            if self._load_confirmed is not None:
                # Did the chain reach the confirm level inside the stall
                # window? The rule's own flag was lost with the process.
                try:
                    confirmed = await self._load_confirmed(
                        cid, self.rule.cfg.confirm_level, opened + stall)
                except Exception:
                    log.exception("%s: confirm reconcile failed for %s", self._name, cid)
            if confirmed is None:
                # No record to go by: older than the stall window and still
                # open => treat as confirmed (the opposing-P1 exit still closes
                # it); younger => the live stall clock decides.
                confirmed = now - opened > stall
            t = Trade(self.rule.cfg.name, cid, r["symbol"],
                      1 if r["direction"] == "LONG" else -1, opened,
                      VolState(ratio=None), state="open", entry_at=opened,
                      confirmed=confirmed)
            self.rule.trades[cid] = t
            n += 1
            # Reconcile: an opposing chain may have started while we were down.
            if self._load_opposing is not None:
                try:
                    if await self._load_opposing(t.symbol, -t.direction, opened,
                                                 self.rule.cfg.ladder):
                        self.rule._mark_exit(t, "opposing_P1", now)
                        self.stats["reconciled_exits"] += 1
                except Exception:
                    log.exception("%s: opposing-chain reconcile failed for %s", self._name, cid)
        log.info("%s: restored %d open trade(s), %d marked for exit by reconcile",
                 self._name, n, self.stats["reconciled_exits"])
        if n:
            async with self._lock:
                await self._execute(self.rule._drain(now), now, event_symbol=None,
                                    decided_at=now.isoformat(), event_price=None, trigger_level=None)

    async def _warm_vol(self) -> None:
        for sym in self._symbols:
            try:
                await self._vol.prefetch(sym, self._clock())
            except Exception:
                log.exception("%s: vol warm-up failed for %s (retried on the first "
                              "entry-level event)", self._name, sym)

    def _flush_rule_log(self) -> None:
        """The rule records why it skipped an entry (vol filter, squeeze, no
        history) in Rule.log, a list nothing read. Log and drop each line."""
        for line in self.rule.log:
            log.info("%s: %s", self._name, line)
        self.rule.log.clear()

    # -------------------------------------------------------------- events
    async def on_event(self, ev) -> Optional[str]:
        raw = getattr(ev, "_raw", None)
        if raw is None:
            raw = {k: getattr(ev, k, None) for k in (
                "event_type", "symbol", "asset_class", "direction", "chain_id",
                "propagation_level", "timestamp", "decision_price", "bar_close")}
            raw["metadata"] = {"propagation_level": raw.get("propagation_level")}
        if raw.get("asset_class") != self._asset_class:
            return None
        now = self._clock()
        ev_for_rule = {**raw, "ladder": self.rule.cfg.ladder}
        lvl = (raw.get("metadata") or {}).get("propagation_level", raw.get("propagation_level"))
        if raw.get("event_type") == "chain_propagated" and lvl is not None \
                and int(lvl) == self.rule.cfg.entry_level:
            await self._vol.prefetch(raw["symbol"], now)
        async with self._lock:
            intents = self.rule.on_chain_event(ev_for_rule, now)
            self._flush_rule_log()
            await self._execute(intents, now, event_symbol=raw["symbol"],
                                decided_at=str(raw.get("timestamp") or now.isoformat()),
                                event_price=raw.get("decision_price"), trigger_level=lvl,
                                decided_bar_close=raw.get("bar_close") if raw.get("timestamp") else None)
        return None

    async def tick(self, now: Optional[datetime] = None) -> None:
        now = now or self._clock()
        async with self._lock:
            intents = self.rule.on_clock(now)
            self._flush_rule_log()
            await self._execute(intents, now, event_symbol=None, decided_at=now.isoformat(),
                                event_price=None, trigger_level=None)

    # ------------------------------------------------------------- helpers
    def _price_for(self, intent: Intent, event_symbol, event_price) -> Optional[float]:
        if event_symbol == intent.trade.symbol and event_price is not None:
            return float(event_price)
        return self._latest_close(intent.trade.symbol)

    async def _execute(self, intents: List[Intent], now: datetime, *, event_symbol,
                       decided_at: str, event_price, trigger_level,
                       decided_bar_close: Optional[str] = None) -> None:
        for intent in intents:
            t = intent.trade
            side = TradeSide.LONG if t.direction > 0 else TradeSide.SHORT
            price = self._price_for(intent, event_symbol, event_price)
            try:
                if intent.action == "OPEN":
                    await self._writer.emit_entry(
                        chain_id=t.chain_id, side=side, symbol=t.symbol,
                        asset_class=self._asset_class, decided_at=decided_at,
                        decision_price=price, trigger_level=self.rule.cfg.entry_level,
                        decided_bar_close=decided_bar_close)
                    self.rule.confirm_open(t.chain_id, now)
                    self.stats["opens"] += 1
                    log.info("%s ENTER %s %s chain=%s vol_ratio=%s price=%s", self._name, t.symbol,
                             side.value, t.chain_id, t.vol.ratio, price)
                else:
                    reason = exit_reason_for(t.exit_reason)
                    await self._writer.emit_exit(
                        chain_id=t.chain_id, side=side, symbol=t.symbol,
                        asset_class=self._asset_class, reason=reason, decided_at=decided_at,
                        decision_price=price,
                        trigger_level=int(trigger_level) if trigger_level is not None else None,
                        decided_bar_close=decided_bar_close)
                    self.rule.confirm_close(t.chain_id, now)
                    self.stats["closes"] += 1
                    log.info("%s EXIT %s %s chain=%s reason=%s (%s) price=%s", self._name,
                             t.symbol, side.value, t.chain_id, t.exit_reason, reason.value, price)
            except Exception:
                self.stats["write_failures"] += 1
                self.rule.reject(t.chain_id, intent.action)
                log.exception("%s %s write failed for %s chain=%s — will retry",
                              self._name, intent.action, t.symbol, t.chain_id,
                              extra=alert(f"rule-write:{self._name}"))


class ChainRuleExit:
    async def maybe_emit_exit(self, **_) -> Optional[str]:
        return None


def make_chain_rule(*, name, symbols, ladders, params, view_registry, writer_factory,
                    vol_provider_for: Callable[[str], object],
                    latest_close_for: Callable[[str], Callable[[str], Optional[float]]],
                    load_open=None, load_opposing_since=None, load_confirmed=None,
                    clock: Clock = _utc_now):
    """Strategy factory. params: rule (frozen config name, required), and
    optional entry_tf / confirm_tf (rung names) or entry_level / confirm_level."""
    from strategy.registry import Strategy

    if len(ladders) != 1:
        raise ValueError(f"{name}: must declare exactly one ladder, got {ladders}")
    key = params.get("rule")
    if key not in FROZEN:
        raise ValueError(f"{name}: params.rule must be one of {sorted(FROZEN)}, got {key!r}")
    ladder = view_registry.get(ladders[0])
    cfg = effective_config(FROZEN[key], name=name, ladder=ladder, params=params)
    vol = vol_provider_for(cfg.asset_class)
    rule = Rule(cfg, vol_fn=vol.get)
    entry = ChainRuleEntry(name, rule, vol, writer_factory(name),
                           asset_class=cfg.asset_class,
                           latest_close=latest_close_for(cfg.asset_class),
                           load_open=load_open, load_opposing_since=load_opposing_since,
                           load_confirmed=load_confirmed, symbols=symbols or (),
                           clock=clock)
    log.info("%s: %s on ladder %s — entry L%d (%s), confirm L%d (%s), stall %d min, "
             "vol_ratio_max=%s vol_ratio_min=%s squeeze=%s",
             name, key, ladder.name, cfg.entry_level, ladder.timeframes[cfg.entry_level - 1],
             cfg.confirm_level, ladder.timeframes[cfg.confirm_level - 1], cfg.stall_minutes,
             cfg.vol_ratio_max, cfg.vol_ratio_min, cfg.avoid_squeeze_release)
    return Strategy(name=name, entry=entry, exit=ChainRuleExit(), symbols=symbols, ladders=ladders)
