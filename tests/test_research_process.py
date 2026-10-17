"""
Research collection (PR 11): quotes as-of each signal, 1m bars with
volume/VWAP/count, earnings CSV import, backfill, and the migration's upserts.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from urllib.parse import parse_qs

import httpx
import pytest
import pytest_asyncio

from research.bars import BarCollector
from research.earnings import import_csv, parse_earnings_csv
from research.polygon_research import QUOTE_LOOKBACK, PolygonResearchClient
from research.quotes import QuoteCapture

UTC = timezone.utc
MIN = 60_000


class FakePolygon:
    """/v3/quotes with a list of (t_ns, bid, ask, bs, as); /v2/aggs 1-minute with v/vw/n."""
    def __init__(self, quotes=None, bars=None, deny=False):
        self.quotes = sorted(quotes or [])
        self.bars = bars or []
        self.deny = deny
        self.requests = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(str(request.url))
        if self.deny:
            return httpx.Response(403, json={"status": "NOT_AUTHORIZED"})
        path = request.url.path
        q = parse_qs(request.url.query.decode())
        if "/v3/quotes/" in path:
            lte = int(q["timestamp.lte"][0]); gte = int(q["timestamp.gte"][0])
            rows = [r for r in self.quotes if gte <= r[0] <= lte]
            rows.sort(reverse=True)
            rows = rows[: int(q.get("limit", ["1"])[0])]
            return httpx.Response(200, json={"results": [
                {"sip_timestamp": t, "bid_price": b, "ask_price": a, "bid_size": bs, "ask_size": asz}
                for (t, b, a, bs, asz) in rows]})
        if "/v2/aggs/" in path:
            parts = path.split("/")
            lo, hi = int(parts[8]), int(parts[9])
            seen, rows = set(), []
            for b in self.bars:                 # first occurrence wins (newest listed first)
                if lo <= b["t"] <= hi and b["t"] not in seen:
                    seen.add(b["t"]); rows.append(b)
            rows.sort(key=lambda b: b["t"])
            return httpx.Response(200, json={"results": rows})
        return httpx.Response(404, json={})


def client_for(fake: FakePolygon) -> PolygonResearchClient:
    return PolygonResearchClient("k", client=httpx.AsyncClient(
        transport=httpx.MockTransport(fake.handler)))


class MemStore:
    def __init__(self):
        self.quotes = {}
        self.bars = {}
        self.earnings = {}

    async def upsert_quote(self, row, quote, status):
        self.quotes[row["signal_id"]] = (row, quote, status)

    async def upsert_bars(self, bars):
        bars = list(bars)
        for b in bars:
            self.bars[(b.symbol, b.timestamp_ms)] = b
        return len(bars)

    async def latest_ms(self, symbol):
        ts = [t for (s, t) in self.bars if s == symbol]
        return max(ts) if ts else None

    async def upsert_earnings(self, rows):
        rows = list(rows)
        for (s, d, t, src) in rows:
            self.earnings[(s, d)] = (t, src)
        return len(rows)


def signal_row(sid="sig1", symbol="NVDA", at=None, action="ENTER", asset_class="equity", price=500.0):
    return {"signal_id": sid, "symbol": symbol, "action": action, "strategy": "rule1",
            "created_at": at or datetime(2026, 10, 14, 15, 0, tzinfo=UTC),
            "decision_price": price, "asset_class": asset_class}


# ---------------------------------------------------------------------------
# Quotes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_quote_as_of_signal_time_is_the_last_quote_at_or_before_it():
    at = datetime(2026, 10, 14, 15, 0, 0, tzinfo=UTC)
    ns = lambda dt: int(dt.timestamp() * 1e9)
    fake = FakePolygon(quotes=[
        (ns(at - timedelta(seconds=5)), 499.9, 500.1, 3, 2),   # the one we want
        (ns(at - timedelta(seconds=9)), 499.0, 501.0, 1, 1),
        (ns(at + timedelta(seconds=1)), 510.0, 511.0, 1, 1),   # after the signal: never
    ])
    store = MemStore()
    cap = QuoteCapture(client_for(fake), store.upsert_quote)
    await cap.handle(signal_row(at=at))
    row, q, status = store.quotes["sig1"]
    assert status == "ok" and q.bid == 499.9 and q.ask == 500.1
    assert q.quote_ts == at - timedelta(seconds=5)
    assert abs(q.spread_bps - (0.2 / 500.0 * 1e4)) < 1e-6
    assert cap.stats["ok"] == 1
    # Idempotent: a replayed row rewrites the same key.
    await cap.handle(signal_row(at=at))
    assert len(store.quotes) == 1


@pytest.mark.asyncio
async def test_no_quote_in_lookback_is_recorded_as_no_quote_not_retried():
    at = datetime(2026, 10, 14, 15, 0, tzinfo=UTC)
    stale = int((at - QUOTE_LOOKBACK - timedelta(minutes=1)).timestamp() * 1e9)
    fake = FakePolygon(quotes=[(stale, 1.0, 2.0, 1, 1)])
    store = MemStore()
    cap = QuoteCapture(client_for(fake), store.upsert_quote)
    await cap.handle(signal_row(at=at))
    _, q, status = store.quotes["sig1"]
    assert q is None and status == "no_quote"


@pytest.mark.asyncio
async def test_entitlement_denied_records_error_and_does_not_raise():
    store = MemStore()
    cap = QuoteCapture(client_for(FakePolygon(deny=True)), store.upsert_quote)
    await cap.handle(signal_row())
    assert store.quotes["sig1"][2] == "error" and cap.stats["error"] == 1


@pytest.mark.asyncio
async def test_non_equity_signals_are_skipped_without_a_request():
    fake = FakePolygon()
    store = MemStore()
    cap = QuoteCapture(client_for(fake), store.upsert_quote)
    await cap.handle(signal_row(symbol="BTC-USD", asset_class="crypto"))
    assert store.quotes == {} and fake.requests == [] and cap.stats["skipped"] == 1


# ---------------------------------------------------------------------------
# Bars
# ---------------------------------------------------------------------------

def bar(t, c, v=1000.0, vw=None, n=10):
    return {"t": t, "o": c, "h": c + 0.5, "l": c - 0.5, "c": c, "v": v, "vw": vw or c, "n": n}


@pytest.mark.asyncio
async def test_bars_first_pass_uses_initial_lookback_then_overlap_and_upserts_revisions():
    now = 1_700_000_000_000 // MIN * MIN
    bars = [bar(now - i * MIN, 100.0 + i) for i in range(0, 3000)]   # ~50 h of minutes
    fake = FakePolygon(bars=bars)
    store = MemStore()
    clock = {"now": now}
    col = BarCollector(client_for(fake), ["NVDA", "SPY"], store.upsert_bars, store.latest_ms,
                       overlap_min=60, initial_lookback_min=24 * 60, now_ms=lambda: clock["now"])
    n1 = await col.run_once()
    assert n1 == 2 * (24 * 60 + 1)                     # inclusive window, both symbols
    b = store.bars[("NVDA", now)]
    assert b.volume == 1000.0 and b.vwap == 100.0 and b.trade_count == 10

    # 15 minutes later: 15 new bars, and a REVISED volume on an old one.
    fake.bars = [bar(now + i * MIN, 200.0 + i) for i in range(1, 16)] + \
                [bar(now, 100.0, v=5000.0)] + bars
    clock["now"] = now + 15 * MIN
    n2 = await col.run_once()
    assert n2 == 2 * (60 + 15 + 1)                     # overlap + new, both symbols
    assert store.bars[("NVDA", now)].volume == 5000.0  # revision absorbed
    assert store.bars[("NVDA", now + 15 * MIN)].close == 215.0
    # Every request was for 1-minute aggregates.
    assert all("/range/1/minute/" in u for u in fake.requests)


@pytest.mark.asyncio
async def test_bars_pass_isolates_a_failing_symbol():
    now = 1_700_000_000_000 // MIN * MIN
    fake = FakePolygon(bars=[bar(now, 1.0)])
    store = MemStore()

    async def latest(sym):
        if sym == "BAD":
            raise RuntimeError("db")
        return None
    col = BarCollector(client_for(fake), ["BAD", "SPY"], store.upsert_bars, latest,
                       now_ms=lambda: now)
    n = await col.run_once()
    assert n == 1 and col.stats["errors"] == 1 and ("SPY", now) in store.bars


# ---------------------------------------------------------------------------
# Earnings CSV
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_earnings_csv_parses_header_comments_and_time_of_day(tmp_path):
    p = tmp_path / "e.csv"
    p.write_text("symbol,report_date,time_of_day\n# NVDA fiscal Q3\nNVDA,2026-11-19,amc\n"
                 "xom,2026-10-31\nSPCX,2026-11-05,BMO\n")
    rows = parse_earnings_csv(p)
    assert rows == [("NVDA", date(2026, 11, 19), "amc"), ("XOM", date(2026, 10, 31), "unknown"),
                    ("SPCX", date(2026, 11, 5), "bmo")]
    store = MemStore()
    assert await import_csv(p, store.upsert_earnings) == 3
    assert store.earnings[("NVDA", date(2026, 11, 19))] == ("amc", "csv:e.csv")
    bad = tmp_path / "bad.csv"
    bad.write_text("NVDA,2026-11-19,noon\n")
    with pytest.raises(ValueError):
        parse_earnings_csv(bad)


# ---------------------------------------------------------------------------
# Backfill loop terminates when only 'error' rows remain
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backfill_terminates_on_persistent_errors(monkeypatch):
    import research_process as rp
    store = MemStore()
    cap = QuoteCapture(client_for(FakePolygon(deny=True)), store.upsert_quote)

    async def load(limit=500):
        return [signal_row(sid="s1"), signal_row(sid="s2")]   # always "unquoted"
    proc = rp.ResearchProcess.__new__(rp.ResearchProcess)
    proc.pool = object()
    proc.quotes = cap
    monkeypatch.setattr(rp, "make_load_signals_without_quotes", lambda pool: load)
    done = await proc.backfill()
    assert done == 2 and cap.stats["error"] == 2


# ---------------------------------------------------------------------------
# Migration 025 upserts against real Postgres (skipped without a DSN)
# ---------------------------------------------------------------------------

DSN = os.environ.get("QISS_TEST_DSN")


@pytest_asyncio.fixture
async def pool():
    import asyncpg
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=2, statement_cache_size=0)
    await p.execute("TRUNCATE research_quotes, research_bars_1m, research_earnings, research_cursor")
    # Other integration tests leave trade_signals rows; this test counts them.
    await p.execute("TRUNCATE trade_signals CASCADE")
    yield p
    await p.close()


@pytest.mark.skipif(not DSN, reason="QISS_TEST_DSN not set; needs real Postgres")
@pytest.mark.asyncio
async def test_persistence_round_trip(pool):
    from research.persistence import (make_latest_bar_ms, make_load_cursor,
                                      make_load_signals_without_quotes, make_save_cursor,
                                      make_upsert_bars, make_upsert_earnings, make_upsert_quote)
    from research.polygon_research import Bar1m, Quote
    from producer.persistence import make_insert_signal

    at = datetime(2026, 10, 14, 15, 0, tzinfo=UTC)
    insert = make_insert_signal(pool)
    await insert({"signal_id": "rq-1", "position_key": "c:L", "chain_id": "c", "direction": "LONG",
                  "action": "ENTER", "symbol": "NVDA", "asset_class": "equity", "strategy": "rule1",
                  "trigger_level": 2, "decision_price": 500.0, "exit_reason": None,
                  "decided_at": at.isoformat()})
    todo = await make_load_signals_without_quotes(pool)()
    assert [r["signal_id"] for r in todo] == ["rq-1"]
    row = todo[0]
    q = Quote("NVDA", at - timedelta(seconds=1), 499.9, 500.1, 3, 2)
    await make_upsert_quote(pool)(row, q, "ok")
    assert await make_load_signals_without_quotes(pool)() == []
    await make_upsert_quote(pool)(row, None, "error")
    assert [r["signal_id"] for r in await make_load_signals_without_quotes(pool)()] == ["rq-1"]

    up = make_upsert_bars(pool)
    assert await up([Bar1m("SPY", 1000 * MIN, 1, 2, 0.5, 1.5, 10.0, 1.4, 3)]) == 1
    assert await up([Bar1m("SPY", 1000 * MIN, 1, 2, 0.5, 1.5, 99.0, 1.4, 3)]) == 1
    assert await make_latest_bar_ms(pool)("SPY") == 1000 * MIN
    assert await pool.fetchval("SELECT volume FROM research_bars_1m WHERE symbol='SPY'") == 99.0

    assert await make_upsert_earnings(pool)([("NVDA", date(2026, 11, 19), "amc", "csv:t")]) == 1
    assert await make_load_cursor(pool)() is None
    await make_save_cursor(pool)(at)
    await make_save_cursor(pool)(at - timedelta(days=1))          # monotonic
    assert await make_load_cursor(pool)() == at
