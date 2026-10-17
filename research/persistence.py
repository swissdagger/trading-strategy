"""research/persistence.py — the research_* tables (migration 025)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, List, Optional

import asyncpg

from research.polygon_research import Bar1m, Quote


def make_upsert_quote(pool: asyncpg.Pool):
    async def upsert_quote(row: dict, quote: Optional[Quote], status: str) -> None:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO research_quotes
                    (signal_id, symbol, action, strategy, signal_at, quote_ts,
                     bid, ask, bid_size, ask_size, spread_bps, decision_price, status)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                ON CONFLICT (signal_id) DO UPDATE SET
                    quote_ts = EXCLUDED.quote_ts, bid = EXCLUDED.bid, ask = EXCLUDED.ask,
                    bid_size = EXCLUDED.bid_size, ask_size = EXCLUDED.ask_size,
                    spread_bps = EXCLUDED.spread_bps, status = EXCLUDED.status,
                    captured_at = now()
                """,
                row["signal_id"], row["symbol"], row["action"], row["strategy"],
                row["created_at"],
                quote.quote_ts if quote else None,
                quote.bid if quote else None, quote.ask if quote else None,
                quote.bid_size if quote else None, quote.ask_size if quote else None,
                quote.spread_bps if quote else None,
                float(row["decision_price"]) if row.get("decision_price") is not None else None,
                status,
            )
    return upsert_quote


def make_load_signals_without_quotes(pool: asyncpg.Pool):
    """Backfill source: equity trade_signals rows with no research_quotes row,
    oldest first. 'error' rows are retried; 'no_quote'/'ok' are final."""
    async def load(limit: int = 500) -> List[dict]:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT s.signal_id, s.symbol, s.action, s.strategy, s.created_at,
                       s.decision_price, s.asset_class
                FROM trade_signals s
                LEFT JOIN research_quotes q ON q.signal_id = s.signal_id
                WHERE s.asset_class = 'equity'
                  AND (q.signal_id IS NULL OR q.status = 'error')
                ORDER BY s.created_at
                LIMIT $1
                """, limit)
        return [dict(r) for r in rows]
    return load


def make_upsert_bars(pool: asyncpg.Pool):
    async def upsert_bars(bars: Iterable[Bar1m]) -> int:
        bars = list(bars)
        if not bars:
            return 0
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO research_bars_1m
                    (symbol, timestamp_ms, open, high, low, close, volume, vwap, trade_count, fetched_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
                ON CONFLICT (symbol, timestamp_ms) DO UPDATE SET
                    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                    close = EXCLUDED.close, volume = EXCLUDED.volume, vwap = EXCLUDED.vwap,
                    trade_count = EXCLUDED.trade_count, fetched_at = now()
                """,
                [(b.symbol, b.timestamp_ms, b.open, b.high, b.low, b.close,
                  b.volume, b.vwap, b.trade_count) for b in bars])
        return len(bars)
    return upsert_bars


def make_latest_bar_ms(pool: asyncpg.Pool):
    async def latest(symbol: str) -> Optional[int]:
        async with pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT max(timestamp_ms) FROM research_bars_1m WHERE symbol = $1", symbol)
    return latest


def make_upsert_earnings(pool: asyncpg.Pool):
    async def upsert(rows: Iterable[tuple]) -> int:
        """rows: (symbol, report_date, time_of_day, source)."""
        rows = list(rows)
        if not rows:
            return 0
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO research_earnings (symbol, report_date, time_of_day, source, fetched_at)
                VALUES ($1,$2,$3,$4, now())
                ON CONFLICT (symbol, report_date) DO UPDATE SET
                    time_of_day = EXCLUDED.time_of_day, source = EXCLUDED.source, fetched_at = now()
                """, rows)
        return len(rows)
    return upsert


def make_load_cursor(pool: asyncpg.Pool):
    async def load() -> Optional[datetime]:
        async with pool.acquire() as conn:
            return await conn.fetchval("SELECT cursor_at FROM research_cursor WHERE id = true")
    return load


def make_save_cursor(pool: asyncpg.Pool):
    async def save(cursor_at: datetime) -> None:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO research_cursor (id, cursor_at, updated_at)
                VALUES (true, $1, now())
                ON CONFLICT (id) DO UPDATE SET
                    cursor_at = GREATEST(research_cursor.cursor_at, EXCLUDED.cursor_at),
                    updated_at = now()
                """, cursor_at)
    return save
