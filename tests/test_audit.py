"""
Unit tests for the audit surface: processed-through watermarks, the /health
verdict, and the audit cache's staleness reporting.

No DB, no HTTP — WatermarkRegistry and build_signal_health_document are pure, and
the DB-derived integrity view is injected as a plain dict.
"""
import pytest

from producer.audit import (
    AuditCache,
    WatermarkRegistry,
    build_signal_health_document,
)


def _clock(start=1000.0):
    box = [start]
    return box, (lambda: box[0])


# --- watermarks -------------------------------------------------------------

def test_records_per_symbol_ladder_timeframe():
    wm = WatermarkRegistry()
    wm.record("BTCUSDT", "FC", "1m", 1_000_000)
    wm.record("BTCUSDT", "PF", "1m", 1_000_000)   # same bar, other ladder
    snap = wm.snapshot()
    assert len(snap) == 2
    assert {w["ladder"] for w in snap} == {"FC", "PF"}


def test_watermark_never_moves_backwards():
    # Bars are delivered at-least-once. A replayed bar must not make the
    # watermark claim less progress than was actually made — the value has to
    # mean "everything through here was seen".
    wm = WatermarkRegistry()
    wm.record("BTCUSDT", "FC", "1m", 1_000_000)
    wm.record("BTCUSDT", "FC", "1m", 500_000)
    assert wm.snapshot()[0]["processed_through_ms"] == 1_000_000


def test_replay_still_refreshes_liveness():
    # A replayed bar does not advance progress but IS evidence the machine is
    # alive, so wall-clock age resets even when bar time does not.
    box, now = _clock()
    wm = WatermarkRegistry(now=now)
    wm.record("BTCUSDT", "FC", "1m", 1_000_000)
    box[0] += 100
    wm.record("BTCUSDT", "FC", "1m", 500_000)
    snap = wm.snapshot()[0]
    assert snap["processed_through_ms"] == 1_000_000
    assert snap["seconds_since_last_bar"] == 0.0


def test_staleness_is_relative_to_the_timeframe():
    # A 12h rung updating twice a day is healthy; a 1m rung silent for 200s is
    # not. A blanket bound would either flap on 12h or never fire on 1m.
    box, now = _clock()
    wm = WatermarkRegistry(now=now)
    wm.record("BTCUSDT", "PF", "1m", 1_000_000)
    wm.record("BTCUSDT", "PF", "12h", 1_000_000)
    box[0] += 200

    by_tf = {w["timeframe"]: w for w in wm.snapshot()}
    assert by_tf["1m"]["stale"] is True
    assert by_tf["12h"]["stale"] is False


def test_unknown_timeframe_is_not_judged():
    box, now = _clock()
    wm = WatermarkRegistry(now=now)
    wm.record("BTCUSDT", "FC", "7m", 1_000_000)
    box[0] += 100_000
    assert wm.snapshot()[0]["stale"] is False


# --- health verdict ---------------------------------------------------------

def test_healthy_when_everything_current():
    wm = WatermarkRegistry()
    wm.record("BTCUSDT", "FC", "1m", 1_000_000)
    doc = build_signal_health_document(
        booted=True, watermarks=wm.snapshot(), audit={}, metrics_snapshot={})
    assert doc["healthy"] is True
    assert doc["reasons"] == []


def test_one_stale_rung_fails_the_whole_verdict():
    # Not averaged away: a 200 has to mean EVERY rung is current, or an auditor
    # cannot use the endpoint to conclude anything.
    box, now = _clock()
    wm = WatermarkRegistry(now=now)
    wm.record("BTCUSDT", "FC", "1m", 1_000_000)
    wm.record("ETHUSDT", "FC", "1m", 1_000_000)
    box[0] += 200
    wm.record("BTCUSDT", "FC", "1m", 1_100_000)   # only this one recovers

    doc = build_signal_health_document(
        booted=True, watermarks=wm.snapshot(), audit={}, metrics_snapshot={})
    assert doc["healthy"] is False
    assert doc["watermarks_stale"] == 1
    assert "ETHUSDT/FC/1m" in doc["reasons"][0]


def test_unhealthy_before_any_bar():
    doc = build_signal_health_document(
        booted=True, watermarks=[], audit={}, metrics_snapshot={})
    assert doc["healthy"] is False
    assert "no bars processed yet" in doc["reasons"]


def test_chain_integrity_does_not_flip_the_verdict():
    # Data quality about history is not liveness about now. Conflating them would
    # pin the endpoint at 503 over rows that are already retired.
    wm = WatermarkRegistry()
    wm.record("BTCUSDT", "FC", "1m", 1_000_000)
    doc = build_signal_health_document(
        booted=True, watermarks=wm.snapshot(),
        audit={"chains": {"shape_violations": 6, "same_ladder_duplicates": 2}},
        metrics_snapshot={})
    assert doc["healthy"] is True
    assert doc["chain_audit"]["chains"]["shape_violations"] == 6


def test_reasons_are_bounded():
    # One wedged symbol can stale every rung it owns; the document must stay
    # readable and bounded in size.
    box, now = _clock()
    wm = WatermarkRegistry(now=now)
    for i in range(50):
        wm.record(f"SYM{i}USDT", "FC", "1m", 1_000_000)
    box[0] += 500
    doc = build_signal_health_document(
        booted=True, watermarks=wm.snapshot(), audit={}, metrics_snapshot={})
    assert doc["watermarks_stale"] == 50
    assert len(doc["reasons"]) == 20


# --- audit cache ------------------------------------------------------------

def test_cache_reports_its_own_age():
    box, now = _clock()
    cache = AuditCache(now=now)
    cache.put({"active_chains": 79})
    box[0] += 30
    assert cache.snapshot()["computed_at_age_seconds"] == 30.0


def test_failed_refresh_keeps_last_good_document():
    # Serving nothing would be worse than serving stale — but the reader must be
    # told, or a frozen document reads as a fresh one.
    cache = AuditCache()
    cache.put({"active_chains": 79})
    cache.put_error("asyncpg.PostgresError('boom')")
    snap = cache.snapshot()
    assert snap["chains"] == {"active_chains": 79}
    assert "boom" in snap["last_refresh_error"]


def test_empty_cache_is_explicit():
    snap = AuditCache().snapshot()
    assert snap["chains"] is None
    assert snap["computed_at_age_seconds"] is None


# --- the DB integrity view (real Postgres) ----------------------------------

import asyncio
import os
import uuid

DSN = os.environ.get("QISS_TEST_DSN")


async def _refresh(pool, ladders):
    from producer.audit import make_chain_audit_refresher
    cache = AuditCache()
    task = asyncio.create_task(
        make_chain_audit_refresher(pool, cache, interval_seconds=60, ladders=ladders)())
    try:
        for _ in range(200):
            snap = cache.snapshot()
            if snap["chains"] is not None or snap["last_refresh_error"]:
                break
            await asyncio.sleep(0.01)
    finally:
        task.cancel()
    assert snap["last_refresh_error"] is None, snap["last_refresh_error"]
    return snap["chains"]


@pytest.mark.skipif(not DSN, reason="QISS_TEST_DSN not set; needs real Postgres")
@pytest.mark.asyncio
async def test_chain_audit_checks_every_configured_ladder_and_flags_unconfigured_ones():
    import asyncpg
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2, statement_cache_size=0)
    ladders = {"FC": ["1m", "3m", "5m", "15m", "1h"], "CN": ["30s", "45s", "1m"]}
    sym = f"AUDIT-{uuid.uuid4().hex[:6]}"
    rows = [  # (ladder, active_timeframes)
        ("FC", ["1m", "3m"]),          # fine
        ("CN", ["30s", "1m"]),         # skips 45s: a shape violation on CN
        ("PF", ["1m"]),                # deleted ladder: no machine will ever end it
    ]
    try:
        before = await _refresh(pool, ladders)
        for i, (ladder, tfs) in enumerate(rows):
            await pool.execute(
                "INSERT INTO chains (chain_id, symbol, asset_class, direction, origin_time, "
                "propagation_levels, active_timeframes, ladder) "
                "VALUES ($1, $2, 'crypto', 1, now() - make_interval(mins => $3), $4, $5, $6)",
                f"chain1.{sym}.{i}", sym, i, len(tfs), tfs, ladder)
        after = await _refresh(pool, ladders)
    finally:
        await pool.execute("DELETE FROM chains WHERE symbol = $1", sym)
        await pool.close()
    assert after["active_chains"] - before["active_chains"] == 3
    assert after["shape_violations"] - before["shape_violations"] == 1
    assert after["active_unconfigured_ladder"] - before["active_unconfigured_ladder"] == 1
    assert after["active_by_ladder"]["PF"] - before["active_by_ladder"].get("PF", 0) == 1
