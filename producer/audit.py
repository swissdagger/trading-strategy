"""
producer/audit.py — processed-through watermarks and a chain-integrity audit
surface for the signal process.

WHY THIS EXISTS. Chain events are a sparse output: a machine can consume bars
correctly for hours and emit nothing, because nothing propagated. Watching the
chain stream therefore cannot distinguish "processing normally, nothing to say"
from "wedged" or "dropping bars" — and a dropped bar is not hypothetical here
(a raising hydrate once took down bar processing for every symbol, silently,
while the process stayed up). The watermark makes forward progress observable
independently of whether anything fired.

The audit document exists for a DIFFERENT reader: a downstream team deciding
whether to trust trade_signals. Their confusion is specific and reasonable — the
chains table contains rows that look like duplicates. Some are, some are not, and
the difference is invisible without knowing that each ladder on a symbol runs its
own chain machine (FC and CN on BTC-USD, DL and ED on equities), so two ladders
can initiate at the same bar. So the document does not just assert health; it
publishes the counts that let someone verify the claim themselves:

  * live chains per ladder, and legitimate cross-ladder pairs, named as such
  * true duplicates, which must be zero because uq_chains_active_initiation
    makes them unwritable — a non-zero here means the index is missing
  * chains whose recorded shape contradicts their own ladder (the corruption
    from the pre-019 hydrate bug)
  * how many live chains carry deterministic ids vs retained legacy uuid4s

THREE PIECES, DELIBERATELY SPLIT:

  * WatermarkRegistry — pure in-memory, thread-safe. Machines call record() on
    the bar path (a dict write under a lock, no I/O). The HTTP thread calls
    snapshot(). No DB, so a wedged pool cannot hide a wedged loop.

  * AuditCache — holds the last DB-derived integrity view plus when it was
    computed. Refreshed by an async task on the event loop; served from the HTTP
    thread. The queries never run per-request, so a scraping loop cannot put load
    on the DB, and the HTTP thread never touches asyncio.

  * build_signal_health_document — the /health verdict, pure and testable.

Serving is execution/observability.HealthServer, extended with an extra_routes
map. Threaded rather than async on purpose: a wedged event loop is precisely the
failure the endpoint must report, so the server must not share it.
"""
from __future__ import annotations

import logging
import re
import threading
import time
from typing import Callable, Dict, Optional, Tuple

log = logging.getLogger("producer.audit")

# A watermark older than this means a machine has stopped consuming. Sized for
# the COARSEST rung in a ladder: DL's 6h bar legitimately updates four times a
# day, so a blanket bound would flap. The verdict is computed per timeframe against
# its own cadence — see build_signal_health_document.
STALENESS_MULTIPLE = 3.0

# Timeframe -> seconds. Mirrors service2_machine._tf_seconds; duplicated rather
# than imported so the audit surface has no dependency on the machine module
# (and so a machine-side refactor cannot silently change the health verdict).
_TF_SECONDS = {
    "5s": 5, "10s": 10, "30s": 30,
    "1m": 60, "2m": 120, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "12h": 43200,
}


class WatermarkRegistry:
    """Per-(symbol, ladder, timeframe) high-water mark of consumed bars.

    Thread-safe: record() is called from the event loop, snapshot() from the
    HTTP thread. Monotonic — a late or replayed bar never moves a watermark
    backwards, so the value always means "everything through here was seen",
    which is the only reading an auditor can safely make of it.
    """

    def __init__(self, now: Callable[[], float] = time.time) -> None:
        self._now = now
        self._lock = threading.Lock()
        # (symbol, ladder, timeframe) -> (bar_ts_ms, wall_clock_seen)
        self._marks: Dict[Tuple[str, str, str], Tuple[int, float]] = {}

    def record(self, symbol: str, ladder: str, timeframe: str,
               bar_ts_ms: int) -> None:
        key = (symbol, ladder, timeframe)
        wall = self._now()
        with self._lock:
            existing = self._marks.get(key)
            # Monotonic in BAR time, but wall time always advances: a replayed
            # bar is still evidence the machine is alive right now, even though
            # it does not advance progress.
            if existing is not None and existing[0] >= bar_ts_ms:
                self._marks[key] = (existing[0], wall)
            else:
                self._marks[key] = (bar_ts_ms, wall)

    def snapshot(self) -> list:
        """One dict per (symbol, ladder, timeframe), JSON-ready."""
        with self._lock:
            marks = dict(self._marks)
        now = self._now()
        out = []
        for (symbol, ladder, timeframe), (ts_ms, wall) in sorted(marks.items()):
            out.append({
                "symbol": symbol,
                "ladder": ladder,
                "timeframe": timeframe,
                "processed_through_ms": ts_ms,
                "processed_through": _iso(ts_ms),
                "seconds_since_last_bar": round(now - wall, 3),
                "stale": _is_stale(timeframe, now - wall),
            })
        return out


class AuditCache:
    """Last computed chain-integrity view, refreshed off the event loop.

    Served as-is to HTTP callers. Holds the computation's age so a consumer can
    tell a fresh verdict from one frozen by a failing refresher — a cache that
    silently serves hours-old counts would be worse than no endpoint at all for
    someone auditing before trusting signals.
    """

    def __init__(self, now: Callable[[], float] = time.time) -> None:
        self._now = now
        self._lock = threading.Lock()
        self._doc: Optional[dict] = None
        self._computed_at: Optional[float] = None
        self._last_error: Optional[str] = None

    def put(self, doc: dict) -> None:
        with self._lock:
            self._doc = doc
            self._computed_at = self._now()
            self._last_error = None

    def put_error(self, err: str) -> None:
        """Record a failed refresh WITHOUT discarding the last good document —
        stale-with-a-reason beats empty."""
        with self._lock:
            self._last_error = err

    def snapshot(self) -> dict:
        with self._lock:
            doc, computed_at, err = self._doc, self._computed_at, self._last_error
        age = None if computed_at is None else round(self._now() - computed_at, 3)
        return {
            "computed_at_age_seconds": age,
            "last_refresh_error": err,
            "chains": doc,
        }


def _iso(ts_ms: int) -> str:
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts_ms / 1000.0, timezone.utc).isoformat()


def _is_stale(timeframe: str, age_seconds: float) -> bool:
    """Stale relative to the timeframe's OWN cadence. A 12h rung updating every
    12h is healthy; a 5s rung silent for a minute is not."""
    period = _TF_SECONDS.get(timeframe)
    if period is None:
        return False        # unknown cadence: do not manufacture a verdict
    return age_seconds > period * STALENESS_MULTIPLE


def build_signal_health_document(*, booted: bool, watermarks: list,
                                 audit: dict, metrics_snapshot: dict) -> dict:
    """The /health verdict. Pure and side-effect-free so it is unit-testable.

    Healthy iff booted, at least one watermark exists, and NO watermark is stale
    against its own timeframe's cadence. A machine that has stopped consuming is
    the failure this endpoint exists to catch, so one stale rung flips the
    verdict rather than being averaged away — an auditor reading 200 must be able
    to treat it as "every rung is current".

    Chain-integrity problems do NOT flip the verdict. They are a data-quality
    question about history, not a liveness question about now, and conflating
    them would leave the endpoint stuck at 503 over rows that are already
    retired. They are reported in the document and left to the reader.
    """
    stale = [w for w in watermarks if w["stale"]]
    healthy = booted and bool(watermarks) and not stale

    reasons = []
    if not booted:
        reasons.append("not booted")
    if not watermarks:
        reasons.append("no bars processed yet")
    for w in stale:
        reasons.append(
            f"{w['symbol']}/{w['ladder']}/{w['timeframe']} stale "
            f"({w['seconds_since_last_bar']}s since last bar)")

    return {
        "healthy": healthy,
        "booted": booted,
        "machines_reporting": len({(w["symbol"], w["ladder"]) for w in watermarks}),
        "watermarks_total": len(watermarks),
        "watermarks_stale": len(stale),
        "reasons": reasons[:20],      # bounded: one wedged symbol can stale many rungs
        "chain_audit": audit,
        "metrics": metrics_snapshot,
    }


# ---------------------------------------------------------------------------
# The DB-derived integrity view.
# ---------------------------------------------------------------------------

def _shape_predicate(ladders: Dict[str, list]) -> str:
    """SQL that is true for a chain whose rungs are not a prefix of its own
    configured ladder (config.EXTRA_LADDERS). Names and timeframes come from
    config, not input; they are still checked before being inlined."""
    terms = []
    for name, tfs in sorted(ladders.items()):
        if not re.fullmatch(r"[A-Za-z0-9_]+", name) or not all(
                re.fullmatch(r"[0-9]+[smhd]", tf) for tf in tfs):
            raise ValueError(f"unexpected ladder spec {name!r}: {tfs!r}")
        arr = "ARRAY[" + ",".join(f"'{tf}'" for tf in tfs) + "]"
        terms.append(f"(ladder = '{name}' AND active_timeframes "
                     f"<> ({arr})[1:cardinality(active_timeframes)])")
    return "\n           OR ".join(terms) or "false"


def _audit_sql(ladders: Dict[str, list]) -> str:
    """One round trip. Every count is a claim an auditor can re-derive from
    `chains` with the same predicate, which is the point — the endpoint
    publishes evidence, not reassurance. A chain on a ladder no longer
    configured is counted (active_unconfigured_ladder): no machine will ever
    advance or terminate it."""
    names = ",".join(f"'{n}'" for n in sorted(ladders)) or "NULL"
    return f"""
SELECT
    count(*)                                                    AS active_chains,
    count(*) FILTER (WHERE ladder IS NULL OR ladder NOT IN ({names}))
                                                                AS active_unconfigured_ladder,
    count(*) FILTER (WHERE chain_id LIKE 'chain1.%%')           AS deterministic_ids,
    count(*) FILTER (WHERE chain_id NOT LIKE 'chain1.%%')       AS legacy_uuid_ids,
    count(*) FILTER (
        WHERE propagation_levels <> cardinality(active_timeframes)
           OR {_shape_predicate(ladders)}
    )                                                           AS shape_violations
FROM chains
WHERE active
"""


_BY_LADDER_SQL = """
SELECT coalesce(ladder, '?') AS ladder, count(*) AS n
FROM chains
WHERE active
GROUP BY 1
"""

# Two live chains at one origin on DIFFERENT ladders: expected and correct. Each
# ladder on a symbol runs its own machine, so two can initiate at the same bar
# open; they are distinct chains with their own level numbering. This is the
# count the downstream team is reading as "duplicates".
_PAIRS_SQL = """
SELECT count(*) AS legitimate_cross_ladder_pairs
FROM (
    SELECT symbol, direction, origin_time
    FROM chains
    WHERE active
    GROUP BY 1,2,3
    HAVING count(DISTINCT ladder) > 1
) g
"""

# Two live chains at one origin on the SAME ladder: a real duplicate. Must be 0 —
# uq_chains_active_initiation makes it unwritable. Non-zero means the index is
# absent, so the query doubles as a check that the backstop is actually in place.
_DUPES_SQL = """
SELECT coalesce(sum(n - 1), 0) AS same_ladder_duplicates
FROM (
    SELECT count(*) AS n
    FROM chains
    WHERE active
    GROUP BY symbol, ladder, direction, origin_time
    HAVING count(*) > 1
) g
"""

_INDEX_SQL = """
SELECT count(*) AS n
FROM pg_indexes
WHERE tablename = 'chains' AND indexname = 'uq_chains_active_initiation'
"""


def make_chain_audit_refresher(pool, cache: AuditCache,
                               interval_seconds: float = 60.0,
                               ladders: Optional[Dict[str, list]] = None):
    """Async task: recompute the integrity view on a fixed interval.

    Not per-request. A scraping loop must not be able to put load on the DB, and
    the HTTP thread must not have to reach into asyncio to serve. Failures are
    recorded on the cache and the loop continues — a refresher that dies on one
    bad query would freeze the document at a stale value with no indication why.
    """
    import asyncio

    if ladders is None:
        from config import EXTRA_LADDERS as ladders
    audit_sql = _audit_sql(ladders)

    async def refresh_once() -> dict:
        async with pool.acquire() as conn:
            main = await conn.fetchrow(audit_sql)
            by_ladder = await conn.fetch(_BY_LADDER_SQL)
            pairs = await conn.fetchrow(_PAIRS_SQL)
            dupes = await conn.fetchrow(_DUPES_SQL)
            idx = await conn.fetchrow(_INDEX_SQL)
        doc = {k: int(v) for k, v in dict(main).items()}
        doc["active_by_ladder"] = {r["ladder"]: int(r["n"]) for r in by_ladder}
        doc["legitimate_cross_ladder_pairs"] = int(pairs["legitimate_cross_ladder_pairs"])
        doc["same_ladder_duplicates"] = int(dupes["same_ladder_duplicates"])
        doc["dedupe_index_present"] = bool(int(idx["n"]))
        doc["notes"] = {
            "legitimate_cross_ladder_pairs":
                "Live chains on DIFFERENT ladders sharing symbol, direction and "
                "origin_time. Each ladder on a symbol runs its own chain machine "
                "(FC and CN on BTC-USD, DL and ED on equities), so two can "
                "initiate at the same bar. They are distinct chains, not "
                "duplicates, each evaluated by its own strategies. Non-zero is "
                "expected.",
            "active_unconfigured_ladder":
                "Live chains on a ladder no longer in config.EXTRA_LADDERS (e.g. "
                "the deleted PF ladder). No machine advances or terminates them; "
                "they are history, not live state.",
            "same_ladder_duplicates":
                "Two live chains on the SAME ladder at one origin. These are real "
                "duplicates. Expected 0: uq_chains_active_initiation makes them "
                "unwritable. Non-zero means that index is missing — check "
                "dedupe_index_present.",
            "legacy_uuid_ids":
                "Chains created before deterministic ids keep their original uuid4. "
                "They are valid; only their ids predate deterministic chain_id. "
                "This count drains to 0 as they terminate.",
            "shape_violations":
                "Live chains whose propagation_levels and active_timeframes "
                "contradict their own ladder's rungs — damage from the pre-019 "
                "cross-ladder hydrate bug. Expected 0 after 019.",
        }
        return doc

    async def loop() -> None:
        while True:
            try:
                cache.put(await refresh_once())
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("chain audit refresh failed: %r", e)
                cache.put_error(repr(e))
            await asyncio.sleep(interval_seconds)

    return loop