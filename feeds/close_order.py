"""
feeds/close_order.py — merge a symbol's rungs into CLOSE-TIME order.

The chain machines apply bars in arrival order, so the order a feed publishes
a symbol's bars in decides which chains propagate and which terminate. A feed
that polls each rung on its own schedule publishes them in poll order; this
merge holds a symbol's bars until no rung can still produce one that closes
earlier, then releases them sorted by (close time, timeframe length) — the
shorter rung first on a tie.

THE RELEASE RULE. For each (symbol, rung) the feed reports NEXT_UNREAD: the
close of the earliest window of that rung it has NOT yet read (every window
closing before it has been read: emitted, or decided to have no bar). Any
bar the rung has yet to produce sorts at or after (NEXT_UNREAD, rung length).
So a held bar is released once its (close, length) is below that key for
every rung of its symbol. A rung read just after 13:00 has NEXT_UNREAD 14:00
(1h) — a 1m bar closing at 13:05 need not wait for the next 1h read.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

from feeds.base import timeframe_to_ms
from ops.alerts import alert
from producer.bar import OHLCPoint

logger = logging.getLogger(__name__)

# A symbol whose bars have waited longer than this for a lagging rung is
# logged (once per minute) naming the rung.
HOLD_WARN_MS = 5 * 60_000


def close_ms(p: OHLCPoint) -> int:
    """When bar `p` closed: its label (open time) + one timeframe."""
    return p.timestamp + timeframe_to_ms(p.timeframe)


def order_key(p: OHLCPoint) -> Tuple[int, int]:
    """Close time, then timeframe LENGTH (not the label string: '15m' < '1m'
    as strings)."""
    return close_ms(p), timeframe_to_ms(p.timeframe)


def next_window_end(after_ms: int, tf: str) -> int:
    """The first epoch-aligned window end of `tf` strictly after `after_ms`."""
    tf_ms = timeframe_to_ms(tf)
    return (after_ms // tf_ms + 1) * tf_ms


class CloseOrderMerge:
    """Per-symbol merge of every rung's bars into close-time order.

    streams: {symbol: [timeframes]}. next_unread: {(symbol, timeframe): ms}
    to start from — the close of the first window the feed will read (its
    live cursor's window end)."""

    def __init__(self, streams: Dict[str, List[str]], next_unread: Dict[tuple, int]):
        self._streams = {s: list(tfs) for s, tfs in streams.items() if tfs}
        self._next_unread = dict(next_unread)
        self._pending: Dict[str, List[OHLCPoint]] = {s: [] for s in self._streams}
        self._warned_at: Dict[str, int] = {}

    def offer(self, symbol: str, tf: str, points: List[OHLCPoint],
              next_unread_ms: Optional[int]) -> None:
        """Hand in a rung's new bars and, if the read was complete, the close
        of the first window it has not read (None: the read failed, the rung
        is no further along)."""
        self._pending.setdefault(symbol, []).extend(points)
        if next_unread_ms is not None:
            key = (symbol, tf)
            have = self._next_unread.get(key)
            self._next_unread[key] = next_unread_ms if have is None else max(have, next_unread_ms)

    def _cutoff(self, symbol: str) -> Tuple[int, int]:
        return min((self._next_unread[(symbol, tf)], timeframe_to_ms(tf))
                   for tf in self._streams[symbol])

    def release(self, now_ms: Optional[int] = None) -> List[OHLCPoint]:
        out: List[OHLCPoint] = []
        for sym in self._streams:
            pending = self._pending.get(sym)
            if not pending:
                continue
            cutoff = self._cutoff(sym)
            ready, held = [], []
            for p in pending:
                (ready if order_key(p) < cutoff else held).append(p)
            ready.sort(key=order_key)
            out.extend(ready)
            self._pending[sym] = held
            if held and now_ms is not None:
                oldest = min(close_ms(p) for p in held)
                if now_ms - oldest > HOLD_WARN_MS and \
                        now_ms - self._warned_at.get(sym, 0) > 60_000:
                    self._warned_at[sym] = now_ms
                    lagging = [tf for tf in self._streams[sym]
                               if self._next_unread[(sym, tf)] <= oldest]
                    logger.warning(
                        "%s: %d bar(s) held %.0fs for rung(s) %s not yet read past "
                        "their close — bars for this symbol are not reaching the "
                        "signal process", sym, len(held), (now_ms - oldest) / 1000, lagging,
                        extra=alert(f"feed-hold:{sym}"))
        return out
