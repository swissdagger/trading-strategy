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
from functools import lru_cache
from typing import Awaitable, Callable, Dict, List, Set
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd

from producer.bar import OHLCPoint


# Type alias for the live-stream callback. Every concrete feed pumps
# OHLC batches into this callback when a polling cycle yields new
# candles. Service 1 wires it to "persist to prices + publish to
# raw-prices Kafka topic" — the feed has no knowledge of Kafka.
LiveDataCallback = Callable[[List[OHLCPoint]], Awaitable[None]]


# ---------------------------------------------------------------
# US equity/options market calendar (DST + holidays + half-days).
#
# Replaces the previous fixed-UTC 13:30–20:00 hardcode, which was only
# correct during US daylight time (it was an hour off all winter) and
# knew nothing about holidays or early closes. exchange_calendars'
# XNYS calendar is the source of truth for WHICH days trade and the
# DST-correct regular open/close (incl. half-day early closes).
#
# IMPORTANT — what XNYS does and does NOT provide:
#   • Provides: trading-day calendar, holidays, half-days, and the
#     DST-correct REGULAR session (09:30–16:00 ET).
#   • Does NOT provide: pre-market / after-hours. The library models
#     the regular session only. Equity extended hours are therefore
#     composed here as a fixed Eastern-time envelope (PRE_OPEN_ET..
#     POST_CLOSE_ET) around any day XNYS marks as a session — the
#     envelope is DST-correct because it's anchored in America/New_York
#     and converted to UTC, and it's holiday-correct because it only
#     applies on XNYS session days.
#
# Options use the regular session directly (no extended hours — OPRA
# options trade 09:30–16:00 ET only). Equity uses the extended envelope.
# ---------------------------------------------------------------
_NY_TZ = ZoneInfo("America/New_York")

# US equity extended-hours envelope, fixed in Eastern time:
#   pre-market 04:00 ET → after-hours close 20:00 ET.
# Applied only on XNYS session days. On half-days the regular session
# truncates (handled by the calendar) but after-hours still runs to
# 20:00 ET, per product decision.
_EQUITY_PRE_OPEN_ET = dtime(4, 0)
_EQUITY_POST_CLOSE_ET = dtime(20, 0)

# Bound the calendar's start so module load doesn't build decades of
# sessions. The right edge is auto-set ~1 year ahead by the library;
# see _xnys_is_session for the out-of-bounds guard that keeps a
# long-lived process from crashing once "now" passes that edge.
_XNYS_CALENDAR_START = "2015-01-01"
_XNYS = xcals.get_calendar("XNYS", start=pd.Timestamp(_XNYS_CALENDAR_START))


@lru_cache(maxsize=4096)
def _xnys_is_session(session_date_iso: str) -> bool:
    """True if `session_date_iso` (an ISO date string, ET calendar
    date) is an XNYS trading day. Cached — the per-tick callers hit the
    same handful of dates repeatedly.

    Guards DateOutOfBounds: the calendar only spans to ~1 year past
    construction, and is_session() RAISES (not returns False) beyond
    that. Rather than let a long-running process crash on every tick
    once it passes the edge, we fall back to a conservative weekday
    check — matching the long-standing "when in doubt, return True and
    let the API decide" posture. A restart rebuilds the calendar with a
    fresh horizon, so this fallback is a safety net, not the steady
    state.
    """
    try:
        return bool(_XNYS.is_session(pd.Timestamp(session_date_iso)))
    except Exception:
        # Out of the calendar's range (or unparseable) — assume a
        # weekday is a trading day. Holidays in the far future are
        # missed by this fallback, but a crash would be far worse, and
        # the service restart cadence keeps us off this path normally.
        d = datetime.fromisoformat(session_date_iso)
        return d.weekday() < 5


def _equity_extended_envelope_utc(session_iso: str) -> "tuple[int, int]":
    """(start_ms, end_ms) of the equity extended-hours envelope for an
    XNYS session date, in UTC. 04:00 ET → 20:00 ET on that ET date,
    DST-correct via zoneinfo. Caller must have already confirmed the
    date is a session via _xnys_is_session.
    """
    d = datetime.fromisoformat(session_iso).date()
    start_et = datetime.combine(d, _EQUITY_PRE_OPEN_ET, tzinfo=_NY_TZ)
    end_et = datetime.combine(d, _EQUITY_POST_CLOSE_ET, tzinfo=_NY_TZ)
    return (
        int(start_et.astimezone(timezone.utc).timestamp() * 1000),
        int(end_et.astimezone(timezone.utc).timestamp() * 1000),
    )


def _option_regular_bounds_utc(now_utc: datetime) -> "tuple[int, int] | None":
    """(start_ms, end_ms) of the XNYS REGULAR session containing
    now_utc, or None. DST- and half-day-correct (straight from the
    calendar). Used by the option path, which has no extended hours.

    The regular session never crosses UTC midnight in either DST state
    (summer 13:30–20:00Z, winter 14:30–21:00Z), so anchoring on the UTC
    date of now_utc is safe here — unlike the equity envelope, whose
    winter after-hours spills past 00:00Z and must anchor on the ET
    date.
    """
    session_iso = now_utc.astimezone(timezone.utc).date().isoformat()
    if not _xnys_is_session(session_iso):
        return None
    try:
        o = _XNYS.session_open(pd.Timestamp(session_iso))
        c = _XNYS.session_close(pd.Timestamp(session_iso))
    except Exception:
        # Out-of-range: fall back to None (closed) rather than crash.
        # The conservative-open fallback lives in _xnys_is_session; for
        # precise bounds we'd rather under-report than fabricate a
        # session window, since bounds feed the forward-fill.
        return None
    start_ms = int(o.timestamp() * 1000)
    end_ms = int(c.timestamp() * 1000)
    now_ms = int(now_utc.timestamp() * 1000)
    if start_ms <= now_ms < end_ms:
        return (start_ms, end_ms)
    return None


# ---------------------------------------------------------------
# Session-hours helper. Used by feeds whose underlying markets have
# closed hours, to avoid wasting rate-limit budget on calls that
# will return empty windows during overnight/weekend gaps.
#
# Phase 2 wired options into this; Phase 7 added the equity cash
# session and reinstated fx (weekly Sun 22 UTC → Fri 22 UTC window).
# Phase 7.x replaced the equity/options fixed-UTC hardcode with the
# XNYS exchange calendar (DST + holidays + half-days) and split the
# two classes: EQUITY now spans extended hours (04:00–20:00 ET),
# OPTIONS stay regular-only (09:30–16:00 ET). Crypto (24/7) never
# calls this. fx keeps its 24×5 arithmetic (no exchange calendar).
#
# All times in UTC. Caller passes `now_utc=datetime.now(timezone.utc)`.
# ---------------------------------------------------------------
def is_market_open(asset_class: str, now_utc: datetime) -> bool:
    """Return True if the given asset class is in its trading session
    at `now_utc`. Conservative for the feed callers — when in doubt,
    return True and let the API decide.
    """
    if asset_class == "equity":
        # US cash equities INCLUDING extended hours: 04:00–20:00 ET on
        # any XNYS trading day, DST- and holiday-correct. The envelope
        # is anchored on the ET calendar date (NOT the UTC date): winter
        # after-hours runs to 01:00 UTC the next calendar day, so a bar
        # at 00:30 UTC belongs to the PREVIOUS ET date's session.
        et = now_utc.astimezone(_NY_TZ)
        session_iso = et.date().isoformat()
        if not _xnys_is_session(session_iso):
            return False
        return _EQUITY_PRE_OPEN_ET <= et.timetz().replace(tzinfo=None) < _EQUITY_POST_CLOSE_ET

    if asset_class == "option":
        # OPRA options: REGULAR session only (09:30–16:00 ET), no
        # extended hours. Sourced from the XNYS calendar so it's DST-
        # and half-day-correct (the old fixed 13:30–20:00 UTC was an
        # hour off all winter and ignored early closes).
        return _option_regular_bounds_utc(now_utc) is not None

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
    if asset_class == "equity":
        # Equity extended-hours envelope: 04:00–20:00 ET on an XNYS
        # trading day, returned in UTC ms. Anchored on the ET calendar
        # date of the bar — NOT its UTC date — because winter after-
        # hours spills past 00:00 UTC into the next UTC day while still
        # belonging to the prior ET session. Overnight (20:00→04:00 ET),
        # weekends, and holidays return None, so the forward-fill treats
        # them as step boundaries and never fabricates bars across a
        # genuine close.
        dt = datetime.fromtimestamp(t_utc_ms / 1000.0, timezone.utc)
        et = dt.astimezone(_NY_TZ)
        session_iso = et.date().isoformat()
        if not _xnys_is_session(session_iso):
            return None
        env = _equity_extended_envelope_utc(session_iso)
        if env[0] <= t_utc_ms < env[1]:
            return env
        return None
    if asset_class == "option":
        # Option REGULAR session via the XNYS calendar (DST + half-day
        # correct). No extended hours. Regular session never crosses
        # UTC midnight, so UTC-date anchoring inside
        # _option_regular_bounds_utc is safe.
        dt = datetime.fromtimestamp(t_utc_ms / 1000.0, timezone.utc)
        return _option_regular_bounds_utc(dt)
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