"""
producer/persistence.py — Real DB persistence for the signal process.

Concrete implementations of the hooks the chain machine and signal writer take as
injected callables. Kept here (not inline in main) so the SQL lives in one place
and the wiring in main stays declarative. All SQL targets the NEW trading
Supabase project's schema (migrations 002 signals/chains, 004 trade_signals) —
NOT CausaView's DB.

asyncpg is assumed (same driver as CausaView's service_2). A single shared pool
is passed in; these are thin async functions over it.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional
from collections import deque


# ---- chain-machine persistence (signals + chains tables, migration 002) ----

def make_persist_signal(pool):
    """signals UPSERT. Idempotent on (symbol, timeframe, timestamp) — a replay or
    restart re-writing the same signal is a no-op-equivalent (updates to the same
    value). Mirrors service_2's signals INSERT ... ON CONFLICT."""
    async def persist_signal(symbol: str, timeframe: str, ts: datetime,
                             signal: int, asset_class: str,
                             price: Optional[float]) -> None:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO signals (symbol, timeframe, timestamp, signal, asset_class, price)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (symbol, timeframe, timestamp)
                DO UPDATE SET signal = EXCLUDED.signal, price = EXCLUDED.price
                """,
                symbol, timeframe, ts, signal, asset_class, price,
            )
    return persist_signal


def make_persist_chain_init(pool):
    """chains INSERT at initiation. Mirrors service_2's chains INSERT, against the
    002 schema (chain_id PK, history JSONB, origin_price)."""
    async def persist_chain_init(chain: dict) -> None:
        first = chain["history"][0]
        origin_time = datetime.fromisoformat(first["timestamp"])
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO chains
                    (chain_id, symbol, asset_class, direction, origin_time,
                     origin_price, propagation_levels, active_timeframes,
                     history, active, last_updated)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, true, $5)
                ON CONFLICT (chain_id) DO NOTHING
                """,
                chain["chain_id"], chain["symbol"], chain["asset_class"],
                chain["direction"], origin_time, first.get("price"),
                1, chain["active_timeframes"], json.dumps(chain["history"]),
            )
    return persist_chain_init


def make_persist_chain_propagate(pool):
    """chains UPDATE on propagation: advance level, append the step to history
    JSONB. Mirrors service_2's UPDATE ... history || $::jsonb."""
    async def persist_chain_propagate(chain_id: str, new_level: int,
                                      active_timeframes: list, ts: datetime,
                                      new_step: dict) -> None:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE chains
                SET propagation_levels = $1,
                    active_timeframes  = $2,
                    last_updated       = $3,
                    history            = history || $4::jsonb
                WHERE chain_id = $5
                """,
                new_level, active_timeframes, ts,
                f"[{json.dumps(new_step)}]", chain_id,
            )
    return persist_chain_propagate


def make_persist_chain_terminate(pool):
    """chains UPDATE on termination: active=false + terminated_at."""
    async def persist_chain_terminate(chain_id: str, ts: datetime) -> None:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE chains SET active = false, terminated_at = $1, last_updated = $1 "
                "WHERE chain_id = $2",
                ts, chain_id,
            )
    return persist_chain_terminate


def make_load_active_chains(pool, symbol: str, asset_class: str, ladder: list):
    """Hydrate active chains for one (symbol, set) on restart. Filters by symbol +
    asset_class AND by the chain's timeframes being within this set's ladder, so a
    Set A machine doesn't adopt a Set B chain (they share the chains table but
    belong to different ladders). The ladder membership of the chain's baseline
    timeframe is the discriminator."""
    async def load_active_chains() -> list:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT chain_id, symbol, asset_class, direction, origin_time,
                       origin_price, propagation_levels, active_timeframes, history
                FROM chains
                WHERE active = true AND symbol = $1 AND asset_class = $2
                """,
                symbol, asset_class,
            )
        out = []
        for r in rows:
            history = r["history"] if isinstance(r["history"], list) else json.loads(r["history"])
            # Only adopt chains whose baseline timeframe is this set's baseline —
            # i.e. chains that belong to THIS ladder.
            if not history:
                continue
            if history[0]["timeframe"] != ladder[0]:
                continue
            # Reconstruct the in-memory chain dict shape the machine expects.
            last = history[-1]
            out.append({
                "chain_id": r["chain_id"], "symbol": r["symbol"],
                "asset_class": r["asset_class"], "direction": r["direction"],
                "current_level": len(history) - 1,
                "next_expected_level": len(history),
                "active_timeframes": list(r["active_timeframes"]),
                "history": history, "last_timestamp": last["timestamp"],
            })
        return out
    return load_active_chains


# ---- signal-writer persistence (trade_signals table, migration 004) --------

def make_insert_signal(pool):
    """trade_signals INSERT, idempotent on signal_id (the deterministic PK).
    Returns True if newly inserted, False if it already existed (a replay) — which
    is what SignalWriter uses to log emit vs idempotent-skip."""
    async def insert_signal(row: dict) -> bool:
        async with pool.acquire() as conn:
            result = await conn.execute(
                """
                INSERT INTO trade_signals
                    (signal_id, position_key, chain_id, direction, action,
                     symbol, asset_class, trigger_level, decision_price,
                     exit_reason, decided_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                ON CONFLICT (signal_id) DO NOTHING
                """,
                row["signal_id"], row["position_key"], row["chain_id"],
                row["direction"], row["action"], row["symbol"],
                row["asset_class"], row.get("trigger_level"),
                row.get("decision_price"), row.get("exit_reason"),
                row["decided_at"],
            )
        # asyncpg returns 'INSERT 0 1' on insert, 'INSERT 0 0' on conflict-skip.
        return result.endswith(" 1")
    return insert_signal


# ---- feed-cursor handoff (feed_cursors table, migration 005) ----------------

async def write_feed_cursors(pool, cursors: dict) -> None:
    """Persist warmup's high-water marks. cursors[symbol][timeframe] = epoch ms.
    Called by the signal process AFTER warmup. Upsert, last-write-wins, so a later
    warmup (a restart that fetched fresher history) advances the cursor. The feed
    process reads these at its next startup."""
    rows = [
        (symbol, tf, ts)
        for symbol, tfs in cursors.items()
        for tf, ts in tfs.items()
    ]
    if not rows:
        return
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO feed_cursors (symbol, timeframe, cursor_ms, updated_at)
            VALUES ($1, $2, $3, now())
            ON CONFLICT (symbol, timeframe)
            DO UPDATE SET cursor_ms = EXCLUDED.cursor_ms, updated_at = now()
            """,
            rows,
        )


async def read_feed_cursors(pool) -> dict:
    """Load all feed cursors as cursors[symbol][timeframe] = epoch ms. Called by
    the FEED process at startup; passed to start_live as initial_cursors. Empty
    map on first-ever boot (feed then starts from 'now')."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT symbol, timeframe, cursor_ms FROM feed_cursors")
    out: dict = {}
    for r in rows:
        out.setdefault(r["symbol"], {})[r["timeframe"]] = r["cursor_ms"]
    return out

# ---- warmup buffer-seed handoff (warmup_bars table, migration 006) ----------
#
# The cold-start counterpart to feed_cursors. warmup_process.py back-fills
# history and calls make_persist_warmup_bars to land it; the signal process
# passes make_load_buffers into the machine factory so hydrate() pre-fills
# each (symbol, timeframe) deque to WINDOW_SIZE before any live bar arrives.
#
# Buffer tick shape MUST match ServiceTwoChainMachine._tick_dict exactly
# (symbol, timestamp, open, high, low, close, timeframe, asset_class) — the
# machine appends these dicts directly, so a field mismatch would silently
# corrupt the Sumtyme input. The single source of truth for that shape is
# _tick_dict; this loader reproduces it.


# Kept in sync with service2_machine.WINDOW_SIZE. Imported lazily inside the
# loader (not at module top) so persistence.py stays importable without the
# machine module — same lazy-import posture as the rest of the signal wiring.


def make_persist_warmup_bars(pool):
    """Return an async (symbol, asset_class, timeframe, points) -> None that
    upserts a fetched history slice into warmup_bars. `points` is the
    List[OHLCPoint] fetch_history returns (ascending by timestamp).

    Idempotent on the (symbol, timeframe, timestamp_ms) PK: re-running warmup
    re-UPSERTs the same rows rather than duplicating, so a crash mid-warmup is
    safe to retry.

    NOTE on growth: this never prunes. A stream re-seeded daily accumulates
    rows older than the newest WINDOW_SIZE, which the loader's LIMIT ignores
    but which still occupy space. If that matters operationally, add a periodic
    DELETE keeping only the newest WINDOW_SIZE per (symbol, timeframe); it's
    deliberately omitted here to keep the write path single-statement.
    """
    async def persist_warmup_bars(
        symbol: str, asset_class: str, timeframe: str, points: list
    ) -> None:
        if not points:
            return
        rows = [
            (symbol, timeframe, p.timestamp,
             p.open, p.high, p.low, p.close, asset_class)
            for p in points
        ]
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO warmup_bars
                    (symbol, timeframe, timestamp_ms,
                     open, high, low, close, asset_class)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (symbol, timeframe, timestamp_ms)
                DO UPDATE SET
                    open = EXCLUDED.open, high = EXCLUDED.high,
                    low = EXCLUDED.low, close = EXCLUDED.close,
                    asset_class = EXCLUDED.asset_class, updated_at = now()
                """,
                rows,
            )
    return persist_warmup_bars


def make_load_buffers(pool, symbol: str, ladder: list):
    """Hydrate this (symbol, set)'s per-timeframe buffers from warmup_bars on
    startup — the cold-start counterpart to make_load_active_chains.

    Returns an async () -> Dict[str, deque] sized to WINDOW_SIZE, holding the
    newest WINDOW_SIZE bars per timeframe in _tick_dict shape, ascending by
    timestamp (deque append order = chronological, as on_bar expects).

    Scope: only the timeframes in THIS set's ladder are loaded, mirroring
    make_load_active_chains' per-set filtering — a Set A machine doesn't
    hydrate Set B's buffers. A timeframe with no warmup rows simply yields no
    entry; the machine creates an empty deque for it on first live bar and
    accumulates the slow way (correct, just not pre-seeded).
    """
    from producer.service2_machine import WINDOW_SIZE

    async def load_buffers() -> dict:
        buffers: dict = {}
        async with pool.acquire() as conn:
            for tf in ladder:
                # Newest WINDOW_SIZE rows for this stream, then reverse to
                # ascending so deque append order is chronological. The
                # DESC+LIMIT uses warmup_bars_stream_ts_idx; reversing in
                # Python avoids a second sort over at most WINDOW_SIZE rows.
                rows = await conn.fetch(
                    """
                    SELECT symbol, timestamp_ms, open, high, low, close,
                           timeframe, asset_class
                    FROM warmup_bars
                    WHERE symbol = $1 AND timeframe = $2
                    ORDER BY timestamp_ms DESC
                    LIMIT $3
                    """,
                    symbol, tf, WINDOW_SIZE,
                )
                if not rows:
                    continue
                buf = deque(maxlen=WINDOW_SIZE)
                for r in reversed(rows):  # DESC -> ascending
                    # EXACT _tick_dict shape — keep these keys identical to
                    # ServiceTwoChainMachine._tick_dict.
                    buf.append({
                        "symbol": r["symbol"], "timestamp": r["timestamp_ms"],
                        "open": r["open"], "high": r["high"],
                        "low": r["low"], "close": r["close"],
                        "timeframe": r["timeframe"], "asset_class": r["asset_class"],
                    })
                buffers[tf] = buf
        return load_buffers_log(symbol, buffers)
    return load_buffers


def load_buffers_log(symbol: str, buffers: dict) -> dict:
    """Tiny helper: log what hydrate restored, return buffers unchanged.
    Separated so the loader body stays a pure data path."""
    import logging
    if buffers:
        sizes = ", ".join(f"{tf}:{len(b)}" for tf, b in buffers.items())
        logging.getLogger("producer.persistence").info(
            "warmup: hydrated buffers for %s (%s)", symbol, sizes
        )
    return buffers