"""Generic chain-following rule engine. Rule 1 (equity) and Rule 3 (crypto) are configurations.

Pattern: enter when a chain reaches ENTRY_LEVEL; if it has not reached CONFIRM_LEVEL within
STALL_MINUTES, close; otherwise hold until the first NEW chain in the opposite direction on that
symbol. No take-profit, no stop-loss. Ladder-agnostic: levels are whatever the live ladder makes
them (adding 30s/45s/2m rungs changes what "P2" means, not this code).

All times are when an event REACHED us ("now"), never the chain/history timestamp (section 18).
Pure logic, no I/O; replayable (see README validation).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Callable, Dict, List, Optional
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")


def us_regular_session(now: datetime) -> bool:
    t = now.astimezone(NY)
    return t.weekday() < 5 and time(9, 30) <= t.time() < time(16, 0)


def always(now: datetime) -> bool:
    return True


@dataclass(frozen=True)
class RuleConfig:
    name: str
    asset_class: str
    entry_level: int = 2
    confirm_level: int = 3
    stall_minutes: int = 60
    session: Callable[[datetime], bool] = always
    vol_ratio_max: Optional[float] = None      # skip entry if vol_ratio > this   (equity: 1.46)
    vol_ratio_min: Optional[float] = None      # skip entry if vol_ratio <= this  (crypto: 0.71)
    avoid_squeeze_release: bool = False        # skip if vol_level <= 0.181 and vol_short > 1 (crypto Rule 2)
    symbols: Optional[frozenset] = None        # None = every symbol of the asset class
    ladder: str = "default"                    # which chain ladder's events this rule listens to


# Frozen rules (findings_addendum.md sections 20-23 and the crypto pre-registration)
RULE1_EQUITY = RuleConfig("rule1_v1", "equity", 2, 3, 60, us_regular_session, vol_ratio_max=1.46)
RULE3_CRYPTO = RuleConfig("rule3_v1", "crypto", 2, 3, 30, always, vol_ratio_min=0.71, avoid_squeeze_release=True)

# NEW ladder {30s, 45s, 1m, 2m, 3m, 5m, 15m, ...}: level numbers shift, so two separate rules.
#   literal : P2 = 45s rung confirmed, P3 = 1m rung  (the rule as you stated it, on the new ladder)
#   equiv   : enter at the 3m rung (level 5), confirm at 5m (level 6) = the backtested old-ladder rule
RULE3_NEW_LITERAL = RuleConfig("rule3_new_literal_v1", "crypto", 2, 3, 30, always, vol_ratio_min=0.71,
                               avoid_squeeze_release=True, ladder="new")
RULE3_NEW_EQUIV = RuleConfig("rule3_new_equiv_v1", "crypto", 5, 6, 30, always, vol_ratio_min=0.71,
                             avoid_squeeze_release=True, ladder="new")


@dataclass
class VolState:
    ratio: Optional[float]          # 24h vol / 14-day average
    level: Optional[float] = None   # percentile of 24h vol within its last 30 days
    short: Optional[float] = None   # 4h vol / 24h vol


@dataclass
class Trade:
    rule: str
    chain_id: str
    symbol: str
    direction: int
    entry_signal_at: datetime
    vol: VolState
    state: str = "pending_entry"    # pending_entry -> opening -> open -> pending_exit -> closing -> closed | skipped
    confirmed: bool = False
    exit_reason: Optional[str] = None
    exit_signal_at: Optional[datetime] = None
    entry_at: Optional[datetime] = None
    exit_at: Optional[datetime] = None


@dataclass
class Intent:
    action: str
    trade: Trade


@dataclass
class Rule:
    cfg: RuleConfig
    vol_fn: Callable[[str, datetime], Optional[VolState]]
    trades: Dict[str, Trade] = field(default_factory=dict)
    log: List[str] = field(default_factory=list)

    # ------------------------------------------------------------------ filters
    def _accept(self, sym: str, vol: Optional[VolState], now: datetime) -> bool:
        c = self.cfg
        if c.symbols is not None and sym not in c.symbols:
            return False
        if vol is None or vol.ratio is None:
            self.log.append(f"{now} {sym} skipped: no vol history"); return False
        if c.vol_ratio_max is not None and vol.ratio > c.vol_ratio_max:
            self.log.append(f"{now} {sym} skipped: vol_ratio {vol.ratio:.2f} > {c.vol_ratio_max}"); return False
        if c.vol_ratio_min is not None and vol.ratio <= c.vol_ratio_min:
            self.log.append(f"{now} {sym} skipped: vol_ratio {vol.ratio:.2f} <= {c.vol_ratio_min}"); return False
        if c.avoid_squeeze_release and vol.level is not None and vol.short is not None \
                and vol.level <= 0.181 and vol.short > 1:
            self.log.append(f"{now} {sym} skipped: squeeze-release (level {vol.level:.2f}, short {vol.short:.2f})"); return False
        return True

    # ------------------------------------------------------------------- events
    def on_chain_event(self, ev: dict, now: datetime) -> List[Intent]:
        if ev.get("asset_class") != self.cfg.asset_class or ev.get("ladder", "default") != self.cfg.ladder:
            return []
        et, sym, d, cid = ev["event_type"], ev["symbol"], int(ev["direction"]), ev.get("chain_id")
        if et == "chain_initiated":
            for t in self._live(sym):
                if t.direction == -d and t.exit_reason is None:
                    self._mark_exit(t, "opposing_P1", now)
        elif et == "chain_propagated":
            lvl = int(ev["metadata"]["propagation_level"])
            if lvl == self.cfg.entry_level and cid not in self.trades:
                vol = self.vol_fn(sym, now)
                if self._accept(sym, vol, now):
                    self.trades[cid] = Trade(self.cfg.name, cid, sym, d, now, vol)
            elif lvl >= self.cfg.confirm_level and cid in self.trades:
                t = self.trades[cid]
                if not t.confirmed and now <= t.entry_signal_at + timedelta(minutes=self.cfg.stall_minutes):
                    t.confirmed = True
        return self._drain(now)

    def on_clock(self, now: datetime) -> List[Intent]:
        deadline = timedelta(minutes=self.cfg.stall_minutes)
        for t in self.trades.values():
            if (t.state in ("pending_entry", "opening", "open") and t.exit_reason is None
                    and not t.confirmed and now > t.entry_signal_at + deadline):
                self._mark_exit(t, f"stall_no_L{self.cfg.confirm_level}_{self.cfg.stall_minutes}m", now)
        return self._drain(now)

    # --------------------------------------------------------------- broker acks
    def confirm_open(self, cid: str, at: datetime) -> None:
        t = self.trades[cid]; t.entry_at = at
        t.state = "pending_exit" if t.exit_reason else "open"

    def confirm_close(self, cid: str, at: datetime) -> None:
        t = self.trades[cid]; t.state, t.exit_at = "closed", at

    def reject(self, cid: str, action: str) -> None:
        self.trades[cid].state = "pending_entry" if action == "OPEN" else "pending_exit"

    # ------------------------------------------------------------------ helpers
    def _live(self, sym):
        return [t for t in self.trades.values() if t.symbol == sym and t.state in ("pending_entry", "opening", "open", "pending_exit")]

    def _mark_exit(self, t: Trade, reason: str, now: datetime) -> None:
        t.exit_reason, t.exit_signal_at = reason, now
        if t.state == "pending_entry":
            t.state = "skipped"; self.log.append(f"{now} {t.symbol} entry dropped before fill: {reason}")
        elif t.state == "open":
            t.state = "pending_exit"

    def _drain(self, now: datetime) -> List[Intent]:
        if not self.cfg.session(now):
            return []
        out = []
        for t in self.trades.values():
            if t.state == "pending_entry":
                out.append(Intent("OPEN", t)); t.state = "opening"
            elif t.state == "pending_exit":
                out.append(Intent("CLOSE", t)); t.state = "closing"
        return out
