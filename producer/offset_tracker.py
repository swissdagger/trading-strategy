"""
producer/offset_tracker.py — safe commit point for out-of-order bar completion.

WHY THIS EXISTS. With per-symbol workers, bars complete OUT OF ORDER: offset 105
(BTC-USD, no forecast, returns instantly) can finish long before offset 100
(AAPL, mid-forecast). The consume loop must still commit a single monotonically
advancing offset for the partition.

Committing the HIGHEST done offset would be a data-loss bug: committing 105 while
100 is still in flight means a crash loses bar 100 forever — it is never
redelivered, and the chain machine has a permanent hole in its input.

The only safe commit point is the CONTIGUOUS LOW-WATER MARK: the highest offset N
such that every offset up to and including N is done. Everything above N stays
uncommitted and is replayed on restart.

DELIVERY SEMANTICS: at-least-once. On a crash, bars between the commit point and
the in-flight frontier are redelivered. That is SAFE here and by design — every
write on the path is idempotent (signals UPSERT on (symbol,timeframe,timestamp);
chain_steps ON CONFLICT DO NOTHING; trade_signals ON CONFLICT on the deterministic
signal_id). Replaying a bar re-derives the same rows. The alternative
(at-most-once, via committing early) trades a benign re-write for silent bar loss,
which corrupts chain state. We take at-least-once every time.
"""
from __future__ import annotations

import logging
from typing import Optional

log = logging.getLogger("producer.offset_tracker")


class OffsetTracker:
    """Tracks completed offsets for ONE partition and yields the contiguous
    low-water mark that is safe to commit."""

    def __init__(self) -> None:
        # Offsets finished but not yet contiguous with the commit point.
        self._done: set[int] = set()
        # Highest offset such that everything <= it is done. None until the first
        # contiguous run forms.
        self._committable: Optional[int] = None
        # The next offset we expect to be able to commit — i.e. the frontier.
        self._next_expected: Optional[int] = None

    def start_at(self, first_offset: int) -> None:
        """Anchor the frontier at the first offset actually consumed. Called once,
        on the first bar, because a consumer starting at auto_offset_reset=latest
        (or resuming from a committed cursor) does not begin at 0."""
        if self._next_expected is None:
            self._next_expected = first_offset

    def complete(self, offset: int) -> None:
        """Mark one offset as fully processed."""
        if self._next_expected is None:
            self.start_at(offset)
        self._done.add(offset)
        # Advance the contiguous frontier as far as the completed set allows.
        while self._next_expected in self._done:
            self._done.discard(self._next_expected)
            self._committable = self._next_expected
            self._next_expected += 1

    def committable(self) -> Optional[int]:
        """The highest offset safe to commit, or None if no contiguous run has
        formed yet (i.e. the very first bar is still in flight)."""
        return self._committable

    def take_committable(self) -> Optional[int]:
        """Return the commit point and clear it, so the caller only issues a
        commit when it has actually advanced."""
        c = self._committable
        self._committable = None
        return c

    def pending(self) -> int:
        """Bars finished but stuck behind an in-flight lower offset. A number that
        grows without bound means one symbol is wedged and holding the commit
        point hostage — worth alerting on."""
        return len(self._done)