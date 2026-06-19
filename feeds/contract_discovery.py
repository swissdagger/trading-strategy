"""
feeds/contract_discovery.py — continuous-tracking options discovery.

Bridges the gap between a high-level intent ("track 15 of our most-
liquid US equity options") and the specific OCC tickers that Polygon's
aggregates endpoint expects ("O:AAPL250620C00200000"). Runs:

  1. On Service 1 startup (fills the slot table if empty).
  2. Daily, at OPTION_REPLACEMENT_CRON_HOUR_UTC (default 04:00 UTC).
     Detects expired contracts, frees their slots, and pre-discovers
     replacements for contracts within OPTION_REPLACEMENT_OVERLAP_DAYS
     of expiry (default 7 days).

Selection model:

  - Pool: ASSET_CONFIGS["option"]["underlyings"] — the curated list
    of underlyings we care about.
  - For each underlying, pick its top candidate (most-liquid contract
    inside the DTE window).
  - Globally rank those underlying-winners by composite z-score:
        oi + volume + spread_tightness + dte_adequacy
    each axis normalised across the candidates evaluated this cycle.
  - Take the top 15 distinct underlyings; one contract per underlying.

Continuous-tracking lifecycle (replaces the old "deactivate when
illiquid" model):

  active=TRUE,  expired_at IS NULL  → currently tracked
  active=FALSE, expired_at IS NOT   → expired naturally; slot freed
                                       NULL  for replacement
  active=FALSE, expired_at IS NULL  → admin-deactivated (rare)

Contracts are tracked irrespective of bar coverage, falling OI, or
thin volume during their life. The structural signal is the product;
liquidity is what governs whether the contract enters the universe,
not whether it stays. Once in, it stays until expiry.

There is no bar-coverage gate. Contracts with sparse history are
acceptable — Sumtyme runs on whatever real bars exist (plus carry-
forward synth to fill the deque to WINDOW_SIZE). Earlier designs
rejected contracts under MIN_REAL_BARS_FOR_TRACKING; that rule is
gone. Sweep deactivation in Service 1 is also gone.

Replacement timing — soft cap with overlap:

  When an active contract's expiry is within
  OPTION_REPLACEMENT_OVERLAP_DAYS, the daily cron pre-discovers a
  replacement on the same underlying and starts seeding it. Briefly
  the system tracks 16 contracts (the incumbent + replacement); when
  the incumbent expires, only the replacement remains. The overlap
  prevents a tracking gap and gives the replacement a head start at
  filling its buffer with real (not carry-forward) bars.

If no replacement contract on the same underlying passes the DTE
floor (rare — underlyings always have a forward-month chain), the
slot is reassigned to the next-best underlying in the global ranking
that isn't already tracked.

Phase 2 standalone test (scripts/test_polygon_options.py) still
exercises the snapshot + OCC parsing pieces, but the public entry
point now is `reconcile_options_universe(...)` rather than
`refresh_options_contracts(...)`.
"""
from __future__ import annotations

import asyncio
import logging
import math
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

import asyncpg
import httpx

from producer.bar import RateLimiter

logger = logging.getLogger(__name__)

# Universe target. Total active option contracts the system tracks,
# distinct by underlying. Discovery picks the top OPTION_UNIVERSE_TARGET
# underlyings each cycle, ranked by composite z-score. Tunable; the
# pool in ASSET_CONFIGS["option"]["underlyings"] should be at least
# OPTION_UNIVERSE_TARGET + headroom (a few extra) so a temporarily
# inactive underlying — e.g. a takeover halting trading — doesn't
# starve the slot count.
OPTION_UNIVERSE_TARGET = 15

# Pre-filters on individual contracts. These apply BEFORE the composite
# z-score; a contract failing either is dropped entirely from ranking.
#
# MIN_OPEN_INTEREST: a true minimum, not a quality threshold. Contracts
# under 1000 OI typically have a single anomalous print as their
# volume — no real market. Doesn't gate quality; gates noise.
MIN_OPEN_INTEREST = 100

# DTE window. Below the minimum a contract has too little remaining
# life to be useful; above the maximum it's almost certainly a LEAP
# (long-dated, illiquid for most names). The composite z-score gives
# a small bonus to mid-window DTE, but anything outside [MIN, MAX] is
# rejected outright.
MIN_DTE_DAYS = 14
MAX_DTE_DAYS = 90

# Soft-overlap window. When an active contract's expiry is within this
# many days, the daily cron starts pre-discovering a replacement on
# the same underlying so the new contract's buffer can begin filling
# with real ticks while the incumbent is still tracked. Brief overlap
# of 1-2 contracts per underlying during this window is acceptable.
OPTION_REPLACEMENT_OVERLAP_DAYS = 7

# Daily replacement-discovery cron hour, UTC. Runs once per day at
# this hour. Picked to fall well after US market close (21:00 UTC
# typical) and before any major Asian open, so the cron doesn't
# compete with live ingest for Polygon rate-limit budget.
OPTION_REPLACEMENT_CRON_HOUR_UTC = 4

# Internal — composite z-score axis weights. Hand-tuned so that no
# single axis dominates. OI and volume carry the most signal (both
# proxies for genuine market interest); spread tightness penalises
# wide quotes; DTE adequacy gives a modest bonus to contracts in the
# meat of the [MIN, MAX] window so a contract about to fall out of
# the window in 2 days isn't preferred over a 45-DTE one with similar
# OI/volume. Weights sum to 1.0 for readability — they're applied to
# z-scores, so absolute scale doesn't matter; relative does.
_Z_WEIGHTS = {
    "oi":     0.40,
    "volume": 0.30,
    "spread": 0.15,
    "dte":    0.15,
}

# Phase 6.1 — snapshot page-fetch retry policy.
#
# Polygon intermittently returns a truncated response body on the
# larger options chains (SPY, QQQ, MSFT, AMZN), which makes
# resp.json() throw "Expecting ',' delimiter". It's transient — a
# retry of the same page almost always succeeds. _fetch_snapshot
# retries a failed page this many times (linear backoff) before
# giving up on the whole underlying. The key invariant: it must
# NEVER return a partial chain — a corrupt subset would feed garbage
# into the per-underlying selection.
_SNAPSHOT_PAGE_MAX_ATTEMPTS = 4
_SNAPSHOT_RETRY_BACKOFF = 1.5  # seconds, multiplied by attempt number


# OCC option symbol format (Polygon's variant — same layout as
# the OCC standard but prefixed with `O:`):
#
#   O:AAPL250620C00200000
#   │ │   │    │ │
#   │ │   │    │ └ strike × 1000, 8-digit zero-padded → $200.00
#   │ │   │    └ contract type: C(all) | P(ut)
#   │ │   └ expiry YYMMDD → 2025-06-20
#   │ └ underlying root (variable length, alphabetic)
#   └ asset class prefix
#
# Underlying root is 1-6 chars per OCC, alphabetic, no spaces/dots.
# Tickers with corporate-action variants (BRK.B → BRKB or similar)
# are normalized in OCC space; we accept whatever Polygon returns.
_OCC_PATTERN = re.compile(
    r"^O:(?P<underlying>[A-Z]{1,6})"
    r"(?P<expiry>\d{6})"
    r"(?P<contract_type>[CP])"
    r"(?P<strike>\d{8})$"
)


class ParsedOption(NamedTuple):
    """Structured decomposition of an OCC ticker. Field names match
    the `option_contracts` table columns so this can be unpacked
    directly into an INSERT.
    """
    symbol:        str       # 'O:AAPL250620C00200000' (full original)
    underlying:    str       # 'AAPL'
    expiry:        date      # date(2025, 6, 20)
    strike:        float     # 200.00
    contract_type: str       # 'call' | 'put'


def parse_occ(ticker: str) -> Optional[ParsedOption]:
    """Decompose a Polygon OCC ticker into its component parts.
    Returns None if the ticker doesn't match the expected format —
    callers should log and skip rather than crash, since Polygon
    occasionally returns slightly malformed tickers for delisted
    contracts.
    """
    m = _OCC_PATTERN.match(ticker)
    if not m:
        return None

    yy = int(m["expiry"][:2])
    mm = int(m["expiry"][2:4])
    dd = int(m["expiry"][4:6])
    # OCC's 2-digit year: per industry convention, 00-49 → 2000s,
    # 50-99 → 1900s. In practice no listed contracts have 19xx
    # expiries — they all rolled off decades ago — so 00-49 → 2000s
    # is what matters. The 1900s branch is here for symmetry and
    # to avoid accidentally booking a contract expiring in 1986.
    year = 2000 + yy if yy < 50 else 1900 + yy
    try:
        expiry = date(year, mm, dd)
    except ValueError:
        return None  # invalid date — malformed ticker

    strike = int(m["strike"]) / 1000.0
    contract_type = "call" if m["contract_type"] == "C" else "put"

    return ParsedOption(
        symbol=ticker,
        underlying=m["underlying"],
        expiry=expiry,
        strike=strike,
        contract_type=contract_type,
    )


# ---------------------------------------------------------------
# Snapshot fetching
# ---------------------------------------------------------------
async def _fetch_snapshot(
    client: httpx.AsyncClient,
    api_key: str,
    underlying: str,
    rate_limiter: RateLimiter,
    max_pages: int = 200,
) -> Optional[List[Dict[str, Any]]]:
    """Fetch the full options chain snapshot for one underlying.

    Polygon paginates with `next_url`. We follow until exhausted so
    `_pick_top_contract` sees the entire chain.

    Returns None (not a partial list) if any page can't be fetched
    after retries — see the _SNAPSHOT_PAGE_MAX_ATTEMPTS comment. A
    partial chain must never reach _pick_top_contract: it would make
    a confident-looking selection from a corrupt subset.

    Page counts in practice (at limit=250):
      - AAPL: ~15 pages (~3500 contracts)
      - SPY:  ~100 pages (~25000 contracts — most-traded options on
              the planet, full chain across strikes and expiries)
      - Mid-cap stocks: typically <10 pages

    `max_pages=200` is a safety cap, not a target. Allows headroom
    for very deep chains (SPX, the index-options counterpart, is
    even larger than SPY). If a real underlying ever truly needs
    >200 pages, the cap should be raised consciously; an early hit
    indicates either a chain that doesn't merit single-contract
    tracking, or a regression in `next_url` handling.

    The `max_pages` guard exists because an early version of this
    function deadlocked into an infinite loop when httpx silently
    dropped the cursor parameter from `next_url` (it was merging the
    dict `params` argument over the URL's existing query string,
    leaving only apiKey). The result was hundreds of bare calls to
    the base endpoint, each returning page 1 over and over. The
    fix below (passing `next_url` as a raw string, no dict params)
    addresses that — but the guard is here as defense in depth.
    """
    # First request: clean URL + dict params (standard httpx pattern).
    url = f"https://api.polygon.io/v3/snapshot/options/{underlying}"
    first_request_params: Optional[Dict[str, Any]] = {"apiKey": api_key, "limit": 250}
    use_dict_params = True
    results: List[Dict[str, Any]] = []
    seen_urls: set = set()
    pages = 0

    while url:
        # Safety: if pagination ever loops back to a URL we've seen
        # (Polygon misbehaving, or a future bug in next_url handling),
        # break instead of spinning forever.
        if url in seen_urls:
            logger.warning(
                f"Snapshot pagination revisited {url!r} for {underlying}; "
                f"stopping early at {len(results)} results"
            )
            break
        seen_urls.add(url)

        if pages >= max_pages:
            logger.warning(
                f"Snapshot pagination for {underlying} hit max_pages={max_pages}; "
                f"stopping with {len(results)} results"
            )
            break
        pages += 1

        page_attempts = 0
        while True:
            page_attempts += 1
            try:
                async with rate_limiter:
                    if use_dict_params:
                        resp = await client.get(url, params=first_request_params, timeout=30.0)
                    else:
                        # next_url already has its cursor params baked in. Do NOT
                        # pass a `params=` dict — httpx will silently overwrite
                        # the URL's existing query string, which is what caused
                        # the original infinite loop. Append apiKey manually
                        # since Polygon's next_url omits it.
                        sep = "&" if "?" in url else "?"
                        resp = await client.get(f"{url}{sep}apiKey={api_key}", timeout=30.0)

                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        backoff = float(retry_after) if retry_after else 2.0
                    except (ValueError, TypeError):
                        backoff = 2.0
                    logger.warning(f"429 on snapshot for {underlying}; backing off {backoff}s")
                    await asyncio.sleep(backoff)
                    page_attempts -= 1  # 429 isn't a "real" attempt; don't burn the retry budget
                    continue

                if resp.status_code in (401, 403):
                    logger.error(
                        f"{resp.status_code} from Polygon snapshot for {underlying}. "
                        f"Confirm POLYGON_API_KEY has options access."
                    )
                    return None

                resp.raise_for_status()
                # resp.json() is the line that throws on a truncated body
                # ("Expecting ',' delimiter ..."). Polygon intermittently
                # returns a cut-off response on the larger chains (SPY, QQQ,
                # MSFT, AMZN all hit it). It's almost always transient — a
                # retry of the same page usually succeeds.
                data = resp.json()
                break  # page fetched + parsed cleanly

            except Exception as e:
                if page_attempts < _SNAPSHOT_PAGE_MAX_ATTEMPTS:
                    backoff = _SNAPSHOT_RETRY_BACKOFF * page_attempts
                    logger.warning(
                        f"Snapshot page fetch failed for {underlying} "
                        f"(attempt {page_attempts}/{_SNAPSHOT_PAGE_MAX_ATTEMPTS}, "
                        f"{type(e).__name__}); retrying in {backoff}s"
                    )
                    await asyncio.sleep(backoff)
                    continue
                # Out of retries. Do NOT return the partial `results` —
                # a truncated chain feeds _pick_top_contract a corrupt
                # subset and it makes a confident pick from bad data
                # (this is exactly how the Phase 6.1 discovery run
                # selected garbage contracts). Fail the whole underlying
                # loudly; the next 24h cycle retries from scratch.
                logger.error(
                    f"Snapshot fetch for {underlying} failed after "
                    f"{_SNAPSHOT_PAGE_MAX_ATTEMPTS} attempts on page {pages} "
                    f"({type(e).__name__}: {e}). Skipping underlying this "
                    f"cycle — NOT selecting from a partial chain."
                )
                return None

        results.extend(data.get("results", []))

        next_url = data.get("next_url")
        if next_url:
            url = next_url
            use_dict_params = False  # next_url has its own params baked in
        else:
            url = None

    logger.info(f"Snapshot for {underlying}: {len(results)} contracts in {pages} pages")
    return results


# ---------------------------------------------------------------
# Per-contract scoring inputs
# ---------------------------------------------------------------
def _extract_scoring_inputs(
    contract: Dict[str, Any],
    today: date,
) -> Optional[Dict[str, float]]:
    """Pull the four ranking axes out of a Polygon snapshot contract.

    Returns None if the contract is unusable — wrong DTE window,
    sub-threshold OI, missing strike/expiry, etc. The pre-filters
    happen here so the caller's selection loop stays simple.

    Spread tightness is `1 / (ask - bid)`, capped (so a zero spread —
    common on illiquid contracts where bid == ask == 0 — doesn't
    produce inf and dominate the z-score). If bid/ask are missing or
    inverted, spread_tightness is treated as 0 (worst).

    DTE adequacy is the distance from the middle of the [MIN, MAX]
    window: contracts at the center get the highest score, contracts
    at either edge get the lowest. This nudges away from contracts
    about to fall out of the window in days while still allowing them
    to win on raw OI/volume if dominant.
    """
    details = contract.get("details") or {}

    # OI floor + extract.
    oi = float(contract.get("open_interest", 0) or 0)
    if oi < MIN_OPEN_INTEREST:
        return None

    # Expiry / DTE window.
    expiry_str = details.get("expiration_date", "")
    try:
        expiry = date.fromisoformat(expiry_str)
    except (ValueError, TypeError):
        return None
    dte = (expiry - today).days
    if dte < MIN_DTE_DAYS or dte > MAX_DTE_DAYS:
        return None

    # Volume (today's). Snapshot returns 0 for contracts that didn't
    # trade; that's a real signal — keep it.
    volume = float((contract.get("day") or {}).get("volume", 0) or 0)

    # Spread tightness from last_quote (bid/ask). Bid/ask can be
    # absent entirely on quiet contracts; treat as worst.
    quote = contract.get("last_quote") or {}
    bid = float(quote.get("bid") or 0)
    ask = float(quote.get("ask") or 0)
    if bid <= 0 or ask <= 0 or ask <= bid:
        spread_tightness = 0.0
    else:
        # Cap at 1/0.01 == 100 to prevent vanishingly-thin spreads
        # from blowing up the z-score. Anything tighter than $0.01
        # gets the same treatment as $0.01.
        raw = 1.0 / max(ask - bid, 0.01)
        spread_tightness = raw

    # DTE adequacy: triangular peak at midpoint of [MIN, MAX].
    midpoint = (MIN_DTE_DAYS + MAX_DTE_DAYS) / 2.0
    half_width = (MAX_DTE_DAYS - MIN_DTE_DAYS) / 2.0
    dte_adequacy = 1.0 - abs(dte - midpoint) / half_width
    # Guard: if MAX == MIN somehow (config bug), force 0.0.
    if half_width == 0:
        dte_adequacy = 0.0

    return {
        "oi":     oi,
        "volume": volume,
        "spread": spread_tightness,
        "dte":    dte_adequacy,
    }


def _pick_best_per_underlying(
    chain: Optional[List[Dict[str, Any]]],
    underlying: str,
    today: date,
) -> Optional[Dict[str, Any]]:
    """For one underlying's chain, return the single best contract
    (by composite z-score WITHIN this underlying's eligible set) or
    None if no contract in the chain is eligible.

    This is a within-underlying pass; the returned contract still has
    to compete against other underlyings' winners in the global pass.
    The z-score is computed locally because comparing axes across an
    underlying with 50 eligible contracts vs. one with 3 isn't apples-
    to-apples; we want THE best representative of each underlying,
    not "the most-confident pick from any underlying."

    Returns the raw snapshot dict (so the caller can hand it to
    parse_occ + DB write code unchanged), with `_score_inputs` stuck
    on for the global pass.
    """
    if chain is None:
        logger.warning(f"No usable snapshot for {underlying} — skipping")
        return None
    if not chain:
        logger.warning(f"Empty options chain for {underlying}")
        return None

    eligible: List[Tuple[Dict[str, float], Dict[str, Any]]] = []
    for contract in chain:
        inputs = _extract_scoring_inputs(contract, today)
        if inputs is None:
            continue
        eligible.append((inputs, contract))

    if not eligible:
        logger.info(
            f"{underlying}: no contracts pass pre-filters "
            f"(OI>={MIN_OPEN_INTEREST}, DTE in [{MIN_DTE_DAYS},{MAX_DTE_DAYS}])"
        )
        return None

    # Local z-score across eligible contracts in THIS underlying only.
    score = _composite_zscore([inputs for inputs, _ in eligible])
    ranked = sorted(
        zip(score, eligible),
        key=lambda p: -p[0],
    )
    best_score, (best_inputs, best_contract) = ranked[0]

    # Stash the inputs on the contract dict so the global pass doesn't
    # have to re-extract. Don't stash the local score — that's
    # underlying-relative; the global pass computes its own.
    best_contract["_score_inputs"] = best_inputs

    occ = (best_contract.get("details") or {}).get("ticker", "?")
    logger.info(
        f"{underlying}: best candidate {occ} "
        f"(OI={int(best_inputs['oi'])}, vol={int(best_inputs['volume'])}, "
        f"spread_tightness={best_inputs['spread']:.3f}, "
        f"dte_adequacy={best_inputs['dte']:.3f}) "
        f"from {len(eligible)} eligible"
    )
    return best_contract


def _composite_zscore(
    inputs_list: List[Dict[str, float]],
) -> List[float]:
    """Compute the composite z-score for a list of contracts.

    Each of the four axes (oi, volume, spread, dte) is z-scored
    independently across the input list, then combined with the
    weights in _Z_WEIGHTS. Returns a list of floats, one per input
    in the same order.

    Edge case: if every contract has identical value on an axis
    (stdev == 0), that axis contributes 0 to everyone — equivalent
    to "this axis adds no information here." A single-contract list
    gets a composite of 0 (z-scores are all 0 by definition with one
    sample, but we'd hit a divide-by-zero, so we just return [0.0]).
    """
    n = len(inputs_list)
    if n == 0:
        return []
    if n == 1:
        return [0.0]

    # Per-axis mean/stdev.
    def _stats(axis: str) -> Tuple[float, float]:
        vals = [d[axis] for d in inputs_list]
        mean = sum(vals) / n
        var = sum((v - mean) ** 2 for v in vals) / n
        return mean, math.sqrt(var)

    stats = {axis: _stats(axis) for axis in _Z_WEIGHTS}

    scores: List[float] = []
    for d in inputs_list:
        s = 0.0
        for axis, weight in _Z_WEIGHTS.items():
            mean, stdev = stats[axis]
            if stdev == 0:
                continue  # this axis contributes nothing here
            z = (d[axis] - mean) / stdev
            s += weight * z
        scores.append(s)
    return scores


# ---------------------------------------------------------------
# DB writes
# ---------------------------------------------------------------
async def _upsert_contract(pool: asyncpg.Pool, parsed: ParsedOption) -> None:
    """Insert or update an `option_contracts` row, marking it active.

    The Phase 0 schema's PRIMARY KEY is `symbol`, so ON CONFLICT is
    by-symbol. We refresh `last_seen` on every discovery cycle so
    operators can spot contracts that haven't been picked recently.
    """
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO option_contracts (
                symbol, underlying, expiry, strike, contract_type,
                active, last_seen
            )
            VALUES ($1, $2, $3, $4, $5, true, now())
            ON CONFLICT (symbol) DO UPDATE SET
                active    = true,
                last_seen = now()
        """,
            parsed.symbol, parsed.underlying, parsed.expiry,
            parsed.strike, parsed.contract_type,
        )


async def _deactivate_other_contracts(
    pool: asyncpg.Pool,
    underlying: str,
    keep_symbol: Optional[str],
) -> None:
    """Mark contracts for `underlying` as inactive. Admin-override
    helper — NOT called by the regular continuous-tracking flow.

    The continuous-tracking model only marks a contract inactive when
    it expires (expire_and_replace sets active=FALSE + expired_at).
    This helper exists for ad-hoc admin operations:
      - keep_symbol=None: deactivate all of an underlying's contracts
        (e.g. an underlying becomes untradable mid-life).
      - keep_symbol=X: deactivate all but X (e.g. an admin manually
        forces a specific replacement, overriding the global ranker).

    Leaves `expired_at` NULL on the deactivated rows — that's how the
    daily cron's expiry sweep distinguishes "natural expiry" from
    "admin override." Admin-deactivated slots are NOT automatically
    refilled by reconcile.

    Row not deleted — its asset_class='option' rows in `prices` and
    `signals` and any chains in `chains` stay intact for historical
    reference.
    """
    async with pool.acquire() as conn:
        if keep_symbol is None:
            # Skip case — deactivate everything for this underlying.
            await conn.execute("""
                UPDATE option_contracts
                SET active = false
                WHERE underlying = $1
                  AND active = true
            """, underlying)
        else:
            await conn.execute("""
                UPDATE option_contracts
                SET active = false
                WHERE underlying = $1
                  AND symbol != $2
                  AND active = true
            """, underlying, keep_symbol)


# ---------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------
async def _gather_candidates(
    pool_underlyings: List[str],
    api_key: str,
    rate_limiter: RateLimiter,
    today: date,
) -> List[Tuple[Dict[str, Any], str]]:
    """Walk the underlying pool, pick the best candidate per
    underlying, return a list of (contract_dict, underlying) tuples.

    Sequential within one underlying (snapshot pagination is the rate-
    limited part — RateLimiter handles bursts). The cost is one
    snapshot fetch per underlying — ~30 underlyings × ~few seconds
    each = 1-2 minutes worst case, well within the daily-cron budget.
    """
    candidates: List[Tuple[Dict[str, Any], str]] = []
    async with httpx.AsyncClient() as client:
        for underlying in pool_underlyings:
            logger.info(f"Scanning chain for {underlying}...")
            chain = await _fetch_snapshot(
                client, api_key, underlying, rate_limiter,
            )
            best = _pick_best_per_underlying(chain, underlying, today)
            if best is not None:
                candidates.append((best, underlying))
    logger.info(
        f"Collected {len(candidates)} per-underlying candidates "
        f"from {len(pool_underlyings)} underlyings"
    )
    return candidates


async def _existing_active_contracts(
    pool: asyncpg.Pool,
) -> Dict[str, List[Dict[str, Any]]]:
    """Return {underlying: [rows]} for currently-tracked contracts
    (active=TRUE, expired_at IS NULL). The new lifecycle: at most
    one active row per underlying, except briefly during a soft-
    overlap window where the incumbent and its pre-discovered
    replacement both run (so the value is a list of size 1 or 2).
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol, underlying, expiry
            FROM option_contracts
            WHERE active = TRUE AND expired_at IS NULL
            ORDER BY underlying, expiry DESC
        """)
    by_underlying: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        u = r["underlying"]
        by_underlying.setdefault(u, []).append(dict(r))
    return by_underlying


async def _ensure_pinned_active(
    pool: asyncpg.Pool,
    pinned_underlyings: List[str],
    api_key: str,
    rate_limiter: RateLimiter,
) -> List[str]:
    """For each underlying in `pinned_underlyings`, ensure an active
    contract exists. If not, discover the best per-underlying contract
    and upsert it.

    Pinned underlyings get their slot regardless of global rank — they
    don't compete with the rest of the pool. This is the entry point
    that enforces the pin: it doesn't ask whether CRM's contract would
    win the global ranking, only whether CRM has SOME active contract
    that satisfies the per-underlying pre-filters (OI floor, DTE
    window, etc.).

    Returns the list of symbols newly written.

    Idempotent: re-running with a pinned underlying that's already
    active is a no-op. The function reads which underlyings have
    active contracts and only discovers for the ones missing.
    """
    if not pinned_underlyings:
        return []

    by_underlying = await _existing_active_contracts(pool)
    missing = [u for u in pinned_underlyings if u not in by_underlying]

    if not missing:
        logger.info(
            f"Pinned: all {len(pinned_underlyings)} pinned underlying(s) "
            f"already have an active contract"
        )
        return []

    logger.info(
        f"Pinned: {len(missing)} pinned underlying(s) without an active "
        f"contract — discovering: {missing}"
    )
    today = date.today()
    written: List[str] = []
    async with httpx.AsyncClient() as client:
        for underlying in missing:
            chain = await _fetch_snapshot(
                client, api_key, underlying, rate_limiter,
            )
            best = _pick_best_per_underlying(chain, underlying, today)
            if best is None:
                logger.warning(
                    f"Pinned: no eligible contract for {underlying} "
                    f"(pre-filters failed: OI<{MIN_OPEN_INTEREST} or "
                    f"DTE outside [{MIN_DTE_DAYS}, {MAX_DTE_DAYS}]). "
                    f"Slot stays empty; next cron will retry."
                )
                continue
            occ = (best.get("details") or {}).get("ticker")
            parsed = parse_occ(occ) if occ else None
            if parsed is None:
                logger.warning(
                    f"Pinned: {underlying} best candidate has "
                    f"unparseable OCC ticker {occ!r}"
                )
                continue
            await _upsert_contract(pool, parsed)
            inp = best["_score_inputs"]
            logger.info(
                f"  → pinned {parsed.symbol} (underlying={underlying}, "
                f"OI={int(inp['oi'])}, vol={int(inp['volume'])}, "
                f"expiry={parsed.expiry})"
            )
            written.append(parsed.symbol)
    return written


async def reconcile_options_universe(
    pool: asyncpg.Pool,
    api_key: str,
    pool_underlyings: List[str],
    rate_limiter: Optional[RateLimiter] = None,
    pinned_underlyings: Optional[List[str]] = None,
) -> List[str]:
    """Fill empty slots up to OPTION_UNIVERSE_TARGET (15) by globally
    ranking the best contract from each non-pinned underlying in the
    pool, AND ensure every pinned underlying has an active contract.

    Two-step model:
      1. Pinned underlyings (from `pinned_underlyings`, typically
         OPTION_PINNED_UNDERLYINGS from config) get their slot
         FIRST, unconditionally. Best contract on the underlying by
         per-underlying composite z-score; no global competition.
      2. Then the global ranker fills OPTION_UNIVERSE_TARGET slots
         from the remaining (non-pinned) pool underlyings. Pinned
         underlyings are excluded from this ranking pass — they
         already have a slot.

    Steady-state universe size:
      len(pinned_underlyings) + OPTION_UNIVERSE_TARGET

    Returns the list of symbols newly written (both pinned and
    ranker-picked). Does NOT deactivate anything — expiry detection
    and admin overrides are the only paths that mark a contract
    inactive in the new model.

    Idempotent: running twice produces the same result on the second
    run (no new writes) since pinned slots and global slots are full.
    """
    if rate_limiter is None:
        rate_limiter = RateLimiter(requests_per_second=5.0, max_concurrent=5)
    pinned_underlyings = list(pinned_underlyings or [])

    today = date.today()

    # ----- Step 1: ensure pinned underlyings have active contracts.
    pinned_written = await _ensure_pinned_active(
        pool, pinned_underlyings, api_key, rate_limiter,
    )

    # ----- Step 2: global-ranker fill for non-pinned underlyings.
    # Inventory existing tracked contracts (now includes anything we
    # just wrote in step 1). Underlyings already represented are
    # excluded from the ranker.
    by_underlying = await _existing_active_contracts(pool)
    existing_underlyings = set(by_underlying.keys())

    # Ranker only fills OPTION_UNIVERSE_TARGET slots, NOT including the
    # pinned underlyings' slots. So the "existing count" for slot math
    # subtracts out the pinned underlyings — they don't consume from
    # the ranker's budget.
    existing_non_pinned = existing_underlyings - set(pinned_underlyings)
    free_slots = OPTION_UNIVERSE_TARGET - len(existing_non_pinned)

    if free_slots <= 0:
        total_active = len(existing_underlyings)
        logger.info(
            f"Reconcile: {len(existing_non_pinned)} ranker-tracked + "
            f"{len(pinned_underlyings)} pinned underlying(s) "
            f"({total_active} total). Ranker has no free slots."
        )
        return pinned_written

    # Candidates from non-pinned underlyings not yet represented.
    eligible_pool = [
        u for u in pool_underlyings
        if u not in existing_underlyings and u not in pinned_underlyings
    ]
    logger.info(
        f"Reconcile: {free_slots} ranker slot(s) free, "
        f"scanning {len(eligible_pool)} candidate underlyings"
    )
    candidates = await _gather_candidates(
        eligible_pool, api_key, rate_limiter, today,
    )
    if not candidates:
        logger.warning(
            "Reconcile: no eligible ranker candidates from the pool. "
            "Slots remain empty until the next cron."
        )
        return pinned_written

    # Global ranking across the underlying-winners.
    inputs_list = [c["_score_inputs"] for c, _ in candidates]
    global_scores = _composite_zscore(inputs_list)
    ranked = sorted(
        zip(global_scores, candidates),
        key=lambda p: -p[0],
    )

    # Take the top `free_slots`, by distinct underlying (our per-
    # underlying step already enforced distinctness, so this is just
    # the slot count).
    picked = ranked[:free_slots]

    ranker_written: List[str] = []
    for global_score, (contract, underlying) in picked:
        details = contract.get("details") or {}
        occ_ticker = details.get("ticker")
        if not occ_ticker:
            logger.warning(
                f"{underlying}: best candidate has no ticker; skipping slot"
            )
            continue
        parsed = parse_occ(occ_ticker)
        if parsed is None:
            logger.warning(
                f"{underlying}: could not parse OCC ticker {occ_ticker!r}; "
                f"skipping slot"
            )
            continue
        await _upsert_contract(pool, parsed)
        inp = contract["_score_inputs"]
        logger.info(
            f"  → {parsed.symbol} (underlying={underlying}, "
            f"global_z={global_score:.3f}, OI={int(inp['oi'])}, "
            f"vol={int(inp['volume'])}, expiry={parsed.expiry})"
        )
        ranker_written.append(parsed.symbol)

    total_underlyings = len(existing_underlyings) + len(ranker_written)
    logger.info(
        f"Reconcile: wrote {len(pinned_written)} pinned + "
        f"{len(ranker_written)} ranker contract(s); universe now has "
        f"{total_underlyings} distinct tracked underlyings "
        f"(target {OPTION_UNIVERSE_TARGET} + {len(pinned_underlyings)} pinned)"
    )
    return pinned_written + ranker_written


async def expire_and_replace(
    pool: asyncpg.Pool,
    api_key: str,
    pool_underlyings: List[str],
    rate_limiter: Optional[RateLimiter] = None,
    pinned_underlyings: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Daily-cron entry point. Three steps:

      1. Mark expired contracts: any active row whose expiry < today
         gets active=FALSE, expired_at=now(). Slot frees.
      2. Pre-discover replacements for active contracts whose expiry
         falls within OPTION_REPLACEMENT_OVERLAP_DAYS. Discovery here
         is per-underlying — find the next-best contract on the same
         underlying that isn't the one about to expire. Result: a
         brief overlap where the incumbent and replacement both run.
         This step is IDENTICAL for pinned and ranker-picked
         contracts — both are rolled per-underlying.
      3. Reconcile to fill any other free slots. Passes
         `pinned_underlyings` so the reconcile step preserves the pin
         invariant: pinned underlyings get their slot before the
         global ranker runs.

    Returns a small summary dict for logging.
    """
    if rate_limiter is None:
        rate_limiter = RateLimiter(requests_per_second=5.0, max_concurrent=5)
    pinned_underlyings = list(pinned_underlyings or [])

    today = date.today()
    overlap_cutoff = today + timedelta(days=OPTION_REPLACEMENT_OVERLAP_DAYS)

    # Step 1 — mark expired. Use a single UPDATE for atomicity and to
    # avoid the read-modify-write race with discovery running on a
    # separate task.
    async with pool.acquire() as conn:
        expired_rows = await conn.fetch("""
            UPDATE option_contracts
            SET active = FALSE,
                expired_at = NOW()
            WHERE active = TRUE
              AND expired_at IS NULL
              AND expiry < $1
            RETURNING symbol, underlying, expiry
        """, today)
    if expired_rows:
        logger.info(
            f"Expiry sweep: marked {len(expired_rows)} contract(s) expired"
        )
        for r in expired_rows:
            logger.info(
                f"  expired: {r['symbol']} "
                f"(underlying={r['underlying']}, expiry={r['expiry']})"
            )

    # Step 2 — pre-discover overlap replacements. Read fresh after
    # the expiry sweep so the underlyings we look at are still active.
    async with pool.acquire() as conn:
        near_expiry = await conn.fetch("""
            SELECT symbol, underlying, expiry
            FROM option_contracts
            WHERE active = TRUE
              AND expired_at IS NULL
              AND expiry <= $1
            ORDER BY expiry
        """, overlap_cutoff)
    overlap_underlyings = {r["underlying"]: dict(r) for r in near_expiry}

    overlap_written: List[str] = []
    if overlap_underlyings:
        logger.info(
            f"Overlap window: {len(overlap_underlyings)} contract(s) within "
            f"{OPTION_REPLACEMENT_OVERLAP_DAYS} days of expiry; "
            f"pre-discovering replacements"
        )
        # Per-underlying replacement discovery. We need the chain, the
        # per-underlying best candidate (excluding the incumbent symbol
        # so we don't pre-discover the same contract), and an upsert.
        async with httpx.AsyncClient() as client:
            for underlying, incumbent in overlap_underlyings.items():
                chain = await _fetch_snapshot(
                    client, api_key, underlying, rate_limiter,
                )
                if chain is None:
                    logger.warning(
                        f"Overlap: could not fetch chain for {underlying}; "
                        f"slot will free naturally on expiry"
                    )
                    continue
                # Exclude the incumbent symbol (otherwise we'd rediscover
                # the contract we're trying to replace).
                incumbent_occ = incumbent["symbol"]
                filtered = [
                    c for c in chain
                    if (c.get("details") or {}).get("ticker") != incumbent_occ
                ]
                best = _pick_best_per_underlying(filtered, underlying, today)
                if best is None:
                    logger.info(
                        f"Overlap: no eligible replacement for {underlying}; "
                        f"slot will free naturally on expiry"
                    )
                    continue
                occ = (best.get("details") or {}).get("ticker")
                parsed = parse_occ(occ) if occ else None
                if parsed is None:
                    logger.warning(
                        f"Overlap: replacement OCC unparseable for "
                        f"{underlying}: {occ!r}"
                    )
                    continue
                await _upsert_contract(pool, parsed)
                logger.info(
                    f"  → overlap replacement {parsed.symbol} "
                    f"(underlying={underlying}, incumbent={incumbent_occ}, "
                    f"new expiry={parsed.expiry})"
                )
                overlap_written.append(parsed.symbol)

    # Step 3 — fill remaining slots by global reconcile. This handles
    # underlyings whose incumbent expired in step 1 without a step-2
    # overlap replacement (rare — usually means the overlap window was
    # skipped because no eligible replacement existed), and any
    # otherwise-free slots from prior cycles. Pinned underlyings
    # get re-ensured here too: if a pinned underlying's contract
    # expired and no overlap replacement was discovered, this is
    # where it gets re-pinned.
    reconciled = await reconcile_options_universe(
        pool, api_key, pool_underlyings, rate_limiter,
        pinned_underlyings=pinned_underlyings,
    )

    return {
        "expired":     [dict(r) for r in expired_rows],
        "overlap":     overlap_written,
        "reconciled":  reconciled,
    }