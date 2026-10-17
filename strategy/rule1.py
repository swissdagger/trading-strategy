"""Rule 1 - equity P2 with volatility filter, 1h stall exit, opposing-P1 failsafe.

Pure logic, no I/O. The service feeds it events and the clock; it returns intents
(OPEN / CLOSE) for a broker to execute. Because it never reads the wall clock itself,
the same class runs live and in historical replay (see replay_validate.py).

Rule (frozen; see findings_addendum.md sections 20-23):
  universe : asset_class == "equity"
  entry    : chain reaches propagation level 2 (P2), in the chain's direction
  filter   : vol_ratio <= 1.46, where vol_ratio = 24h realised vol / its 14-day average
             (computed from 1m bars up to the last COMPLETE hour before the P2 event)
  exit A   : chain has not reached level 3 within 60 min of the P2 event  -> close
  exit B   : first NEW chain in the opposite direction on that symbol     -> close
  no take-profit, no stop-loss
  execution: CFDs trade in the regular US session only. Signals outside it are queued to the
             next open; a queued entry is dropped if its exit condition fired while waiting.

Timing rule (section 18): every time used here is when the EVENT REACHED US ("now"),
never the chain/history timestamp, which is one timeframe earlier.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Callable, Dict, List, Optional
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
SESSION_OPEN, SESSION_CLOSE = time(9, 30), time(16, 0)

VOL_RATIO_MAX = 1.46
STALL_WINDOW = timedelta(minutes=60)


def in_session(now: datetime) -> bool:
    """Regular US equity session. Exchange holidays are NOT modelled: on a holiday the
    broker rejects the order and the intent stays pending until the next real session."""
    t = now.astimezone(NY)
    return t.weekday() < 5 and SESSION_OPEN <= t.time() < SESSION_CLOSE


@dataclass
class Trade:
    chain_id: str
    symbol: str
    direction: int                     # +1 long, -1 short
    p2_at: datetime                    # when the P2 event reached us
    vol_ratio: float
    state: str = "pending_entry"       # pending_entry -> open -> pending_exit -> closed | skipped
    l3_seen: bool = False
    exit_reason: Optional[str] = None
    exit_signal_at: Optional[datetime] = None
    entry_at: Optional[datetime] = None
    exit_at: Optional[datetime] = None


@dataclass
class Intent:
    action: str                        # "OPEN" or "CLOSE"
    trade: Trade


@dataclass
class Rule1:
    vol_ratio_fn: Callable[[str, datetime], Optional[float]]
    trades: Dict[str, Trade] = field(default_factory=dict)      # by chain_id
    log: List[str] = field(default_factory=list)

    # ---------------------------------------------------------------- events
    def on_chain_event(self, ev: dict, now: datetime) -> List[Intent]:
        """ev: a causal-events payload (event_type, symbol, asset_class, direction,
        chain_id, metadata.propagation_level)."""
        if ev.get("asset_class") != "equity":
            return []
        et, sym, d = ev["event_type"], ev["symbol"], int(ev["direction"])
        out: List[Intent] = []

        if et == "chain_initiated":                       # failsafe for opposite trades
            for t in self._live(sym):
                if t.direction == -d and t.exit_reason is None:
                    self._mark_exit(t, "opposing_P1", now)
            out += self._drain(now)

        elif et == "chain_propagated":
            lvl = int(ev["metadata"]["propagation_level"])
            cid = ev["chain_id"]
            if lvl == 2 and cid not in self.trades:
                vr = self.vol_ratio_fn(sym, now)
                if vr is None:
                    self.log.append(f"{now} {sym} P2 skipped: no vol history")
                elif vr > VOL_RATIO_MAX:
                    self.log.append(f"{now} {sym} P2 skipped: vol_ratio {vr:.2f} > {VOL_RATIO_MAX}")
                else:
                    self.trades[cid] = Trade(cid, sym, d, now, vr)
            elif lvl >= 3 and cid in self.trades:
                t = self.trades[cid]
                if not t.l3_seen and now <= t.p2_at + STALL_WINDOW:
                    t.l3_seen = True
            out += self._drain(now)
        return out

    def on_clock(self, now: datetime) -> List[Intent]:
        """Call regularly (e.g. every 15 s live; every bar in replay)."""
        for t in self.trades.values():
            if (t.state in ("pending_entry", "opening", "open") and t.exit_reason is None
                    and not t.l3_seen and now > t.p2_at + STALL_WINDOW):
                self._mark_exit(t, "stall_no_L3_1h", now)
        return self._drain(now)

    # ------------------------------------------------------------ broker acks
    def confirm_open(self, chain_id: str, at: datetime) -> None:
        t = self.trades[chain_id]
        t.entry_at = at
        # an exit may have fired while the order was in flight
        t.state = "pending_exit" if t.exit_reason else "open"

    def reject(self, chain_id: str, action: str) -> None:
        """Broker refused (e.g. exchange holiday): put the intent back to retry later."""
        t = self.trades[chain_id]
        t.state = "pending_entry" if action == "OPEN" else "pending_exit"

    def confirm_close(self, chain_id: str, at: datetime) -> None:
        t = self.trades[chain_id]
        t.state, t.exit_at = "closed", at

    # ---------------------------------------------------------------- helpers
    def _live(self, sym: str):
        return [t for t in self.trades.values() if t.symbol == sym and t.state in ("pending_entry", "opening", "open", "pending_exit")]

    def _mark_exit(self, t: Trade, reason: str, now: datetime) -> None:
        t.exit_reason, t.exit_signal_at = reason, now
        if t.state == "pending_entry":                      # never got filled: drop it
            t.state = "skipped"
            self.log.append(f"{now} {t.symbol} entry dropped before fill: {reason}")
        elif t.state == "open":
            t.state = "pending_exit"
        # state "opening": confirm_open() sees exit_reason and turns it into pending_exit

    def _drain(self, now: datetime) -> List[Intent]:
        if not in_session(now):
            return []
        out = []
        for t in self.trades.values():
            if t.state == "pending_entry":
                out.append(Intent("OPEN", t)); t.state = "opening"
            elif t.state == "pending_exit":
                out.append(Intent("CLOSE", t)); t.state = "closing"
        return out
