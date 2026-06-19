"""
feeds/base.py — The AssetFeed interface.

Every data source implements this ABC. Adding a third source later
means writing one new file that inherits AssetFeed and adding one
entry to ASSET_CONFIGS — Services 2 and 3 know nothing about which
feeds exist.

The interface is deliberately small. Anything source-specific
(Polygon's `O:` prefix, Binance's USDⓈ-M Futures endpoint, contract
discovery for options) lives behind the concrete class. The signature
shape is fixed by the plan; the rationale for each method is below.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Awaitable, Callable, Dict, List, Set

from producer.bar import OHLCPoint


# Type alias for the live-stream callback. Every concrete feed pumps
# OHLC batches into this callback when a polling cycle yields new
# candles. Service 1 wires it to "persist to prices + publish to
# raw-prices Kafka topic" — the feed has no knowledge of Kafka.
LiveDataCallback = Callable[[List[OHLCPoint]], Awaitable[None]]


# ---------------------------------------------------------------
# Session-hours helper. Used by feeds whose underlying markets have
# closed hours, to avoid wasting rate-limit budget on calls that
# will return empty windows during overnight/weekend gaps.
#
# Phase 2 wired options into this; Phase 7 added the equity cash
# session (shares the options branch) and reinstated fx (weekly
# Sun 22 UTC → Fri 22 UTC window). Crypto (24/7) never calls this.
# If a future feed needs another session model, add a branch rather
# than copying the logic into the feed itself.
#
# All times in UTC. Caller passes `now_utc=datetime.now(timezone.utc)`.
# ---------------------------------------------------------------
def is_market_open(asset_class: str, now_utc: datetime) -> bool:
    """Return True if the given asset class is in its trading session
    at `now_utc`. Conservative for the feed callers — when in doubt,
    return True and let the API decide.
    """
    if asset_class in ("option", "equity"):
        # US options AND US cash equities: 13:30–20:00 UTC weekdays.
        # This is the regular-hours window for OPRA-listed options and
        # the NYSE/NASDAQ cash session. After-hours trading exists for
        # both but isn't aggregated the same way and we're not
        # subscribed to it.
        #
        # Deliberately ONE branch for both classes so they can never
        # drift: the equity session is definitionally the same cash
        # session the options trade against. Note this inherits the
        # existing fixed-UTC simplification (no DST shift, no holiday
        # calendar) — if that's ever fixed, it fixes both at once.
        return (
            now_utc.weekday() < 5  # Mon=0 ... Fri=4
            and dtime(13, 30) <= now_utc.time() < dtime(20, 0)
        )
    if asset_class == "fx":
        # Forex: continuous 22:00 UTC Sunday → 22:00 UTC Friday.
        # The check here is "not the weekend gap." Mon=0..Sun=6;
        # the closed window is Fri 22:00 → Sun 22:00.
        weekday = now_utc.weekday()
        t = now_utc.time()
        if weekday == 5:  # Saturday — closed all day
            return False
        if weekday == 4 and t >= dtime(22, 0):  # Friday after close
            return False
        if weekday == 6 and t < dtime(22, 0):  # Sunday before open
            return False
        return True
    # Crypto and anything we don't know about: assume always open.
    return True


def session_bounds_for(
    asset_class: str, t_utc_ms: int,
) -> "tuple[int, int] | None":
    """For a given UTC timestamp in ms, return the (start_ms, end_ms)
    bounds of the trading session that contains it, or None if the
    asset wasn't trading at that time.

    For options:
      - Within a regular-hours weekday session → return that day's
        13:30..20:00 UTC bounds.
      - Outside session (overnight, weekend, holiday) → None.

    For crypto:
      - Always returns a sentinel (0, very_large) since crypto has
        no session boundaries. Callers that distinguish intra-session
        from inter-session gaps will always treat crypto gaps as
        intra-session (i.e. all fillable). In practice Binance fills
        no-trade minutes itself, so the feed never produces gaps for
        crypto and this branch is rarely hit.

    For equity:
      - Same daily 13:30..20:00 UTC bounds as options (one shared
        branch — see is_market_open).

    For fx (Phase 7 — reinstated):
      - Returns the WEEKLY session bounds: the Sun 22:00 UTC →
        Fri 22:00 UTC window containing `t_utc_ms`, or None inside
        the weekend gap (Fri 22:00 → Sun 22:00). This is the "return
        the full weekly window" option the Phase 6 comment deferred:
        quiet intra-week windows on sparse pairs (e.g. USDKRW at 15s)
        get the same flat-fill treatment as an illiquid option's
        no-trade minutes, while the weekend remains a step boundary —
        the market genuinely wasn't open, so fabricating bars across
        it would corrupt the manifold.

    Use:
      session = session_bounds_for("option", bar_ts_ms)
      if session is None:
          # bar is outside any session — shouldn't happen in practice
          # for data we just fetched, but defensive callers handle it
          pass
      else:
          start_ms, end_ms = session
          # Now we can fill windows up to but not including end_ms.
    """
    if asset_class in ("option", "equity"):
        # Reconstruct the date in UTC and the session bounds on that
        # date. Caller is responsible for not passing weekend or
        # overnight timestamps — but we still check and return None
        # rather than producing garbage bounds. One branch for both
        # classes — same cash session, same drift-prevention rationale
        # as is_market_open above.
        dt = datetime.fromtimestamp(t_utc_ms / 1000.0, timezone.utc)
        if dt.weekday() >= 5:  # Sat or Sun
            return None
        session_start = dt.replace(hour=13, minute=30, second=0, microsecond=0)
        session_end = dt.replace(hour=20, minute=0, second=0, microsecond=0)
        if dt < session_start or dt >= session_end:
            return None
        return (
            int(session_start.timestamp() * 1000),
            int(session_end.timestamp() * 1000),
        )
    if asset_class == "crypto":
        # No session boundaries. Use sentinel so callers' arithmetic
        # works uniformly (start_ms <= bar_ts < end_ms is always true).
        return (0, 2**63 - 1)
    if asset_class == "fx":
        # Weekly session: Sun 22:00 UTC → Fri 22:00 UTC. Timestamps in
        # the weekend gap return None (no session contains them).
        #
        # The arithmetic mirrors is_market_open's fx branch — keep the
        # two in agreement: is_market_open(fx, t) is True exactly when
        # session_bounds_for(fx, t) is not None.
        dt = datetime.fromtimestamp(t_utc_ms / 1000.0, timezone.utc)
        weekday, t = dt.weekday(), dt.time()
        if weekday == 5:  # Saturday — closed all day
            return None
        if weekday == 4 and t >= dtime(22, 0):  # Friday after close
            return None
        if weekday == 6 and t < dtime(22, 0):  # Sunday before open
            return None
        # In-session. Find the containing week's Sunday-22:00 open.
        # days_since_sunday: Mon=1 ... Fri=5, Sun=0. (A Sunday here is
        # necessarily >= 22:00 — the pre-open case returned above —
        # so "0 days back" is correct for Sunday evenings.)
        days_since_sunday = (weekday + 1) % 7
        open_dt = (dt - timedelta(days=days_since_sunday)).replace(
            hour=22, minute=0, second=0, microsecond=0,
        )
        close_dt = open_dt + timedelta(days=5)  # Friday 22:00
        return (
            int(open_dt.timestamp() * 1000),
            int(close_dt.timestamp() * 1000),
        )
    # unknown — defer.
    return None


_TF_UNIT_MS = {"s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}


def timeframe_to_ms(timeframe: str) -> int:
    """Parse a timeframe label ('5m', '1h') to milliseconds.

    Shared so Service 2's fill site and discovery's coverage check
    agree on bar spacing. Unknown unit falls back to minutes — same
    defensive default Service 2's local parser used before this moved
    here.
    """
    return int(timeframe[:-1]) * _TF_UNIT_MS.get(timeframe[-1].lower(), 60_000)


def count_synthetic_for_gap(
    asset_class: str,
    timeframe: str,
    prev_ts_ms: int,
    new_ts_ms: int,
) -> int:
    """Return how many synthetic flat bars the within-session fill
    inserts between two consecutive real bars of one stream.

    This is THE shared definition of the gap arithmetic. Two callers:

      - Service 2's `_gap_fill_ticks` builds the actual synthetic tick
        dicts to append to the buffer; its loop is bounded by this
        same arithmetic.
      - `contract_discovery._measure_buffer_coverage` sums this across
        a candidate contract's trial history to PREDICT the buffer's
        synthetic fraction before the contract is ever selected — so
        discovery rejects a contract whose buffer would be mostly
        fabricated, instead of finding out at buffer-load time.

    Because both go through this one function, the prediction and the
    reality can't drift. Earlier Phase 6.1 gates (volume, raw bar
    count) were proxies that correlated with buffer health but didn't
    measure it; this measures it.

    Rules (identical to `_gap_fill_ticks`'s, by construction):
      - Gap <= one timeframe step: no fill, 0.
      - Within prev's session: fill every step in the gap.
      - Gap crosses out of prev's session: fill only up to the
        session boundary; the inter-session portion passes through
        unfilled (the next session's first bar is the next step).
      - session_bounds_for returns None (no well-defined session,
        e.g. fx): no fill, 0.
    """
    tf_ms = timeframe_to_ms(timeframe)
    if new_ts_ms - prev_ts_ms <= tf_ms:
        return 0

    session = session_bounds_for(asset_class, prev_ts_ms)
    if session is None:
        return 0
    _, prev_session_end_ms = session

    # Fill from prev_ts + tf_ms up to (exclusive) min(new_ts, session_end).
    fill_until = min(new_ts_ms, prev_session_end_ms)
    if fill_until <= prev_ts_ms + tf_ms:
        return 0
    # Number of steps strictly before fill_until, starting at prev+tf_ms.
    return (fill_until - prev_ts_ms - tf_ms + tf_ms - 1) // tf_ms


class AssetFeed(ABC):
    """One feed per data source. Concrete subclasses set `asset_class`
    and `name` as class attributes.

    Lifecycle: Service 1 calls `fetch_history` repeatedly during the
    warmup phase (one call per (symbol, timeframe) that needs seeding),
    then calls `start_live` once and lets it run for the process
    lifetime. `start_live` is expected to never return under normal
    operation; cancellation is handled by the caller.

    Each feed owns its own RateLimiter and any source-specific state
    (HTTP client, auth headers, partial-bar dedupe cursors). Feeds do
    NOT share rate budget — Polygon and Binance have independent
    HTTP quotas. (This was an explicit user direction during Phase 1
    design.)
    """

    # Subclasses MUST set these as class attributes.
    asset_class: str = ""   # 'fx' | 'crypto' | 'option' | 'equity' — matches types.ts AssetClass
    name: str = ""          # 'polygon_forex' | 'polygon_options' | 'polygon_stocks' | 'binance_futures'

    @abstractmethod
    async def fetch_history(
        self,
        symbol: str,
        timeframe: str,
        points_needed: int,
    ) -> List[OHLCPoint]:
        """Fetch up to `points_needed` historical candles for the given
        (symbol, timeframe). Returned points carry this feed's
        `asset_class` so the seeder doesn't have to remember which
        feed produced them.

        Implementations are responsible for their own pagination,
        rate-limiting, and 429/418 retry handling.
        """

    @abstractmethod
    async def start_live(
        self,
        symbols: List[str],
        timeframes: List[str],
        initial_cursors: Dict[str, Dict[str, int]],
        callback: LiveDataCallback,
    ) -> None:
        """Begin streaming live candles. Runs until cancelled.

        `initial_cursors[symbol][timeframe]` is the latest already-
        persisted timestamp (ms) for that pair — the feed should fetch
        only candles strictly newer than this to avoid re-inserting
        data that warmup already seeded.

        Calls `callback` whenever a new batch of candles arrives. The
        callback shape (list of OHLCPoint) is identical across all
        feeds so Service 1's persister is feed-agnostic.
        """

    @abstractmethod
    def supported_timeframes(self) -> Set[str]:
        """Which canonical timeframes (e.g. '1m', '5m', '12h') this
        feed can produce. Used by config validation to refuse
        configurations that ask for unsupported pairs (e.g. '10m' on
        Binance Futures, which doesn't offer it).
        """

    @abstractmethod
    def to_native_symbol(self, symbol: str) -> str:
        """Convert the canonical symbol to whatever string the source
        API expects. Polygon forex needs `C:EURUSD`; Polygon options
        already includes the `O:` prefix in the canonical name;
        Binance is pass-through. Returns the native form; canonical
        form is what's stored in the DB and emitted to Kafka.
        """