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

def make_load_chain_view_seed(pool):
    """Load EVERY active chain (all symbols, both sets) as ChainView seed rows,
    for consumer-side hydrate-on-restart (runtime.start -> ChainView.seed_active).
 
    Returns a list of dicts, one per active chain, each carrying exactly what
    ChainView.seed_active needs:
 
        {"symbol", "set", "chain_id", "direction", "peak_level"}
 
      * set        — "A" | "B", classified from the chain's BASELINE timeframe
                     (active_timeframes[0]), per migration 008: we classify from
                     active_timeframes rather than history[0] so this needs no
                     per-step data at all. A chain whose baseline tf belongs to
                     neither set's ladder baseline is skipped (defensive — it
                     belongs to some other ladder, e.g. the four-chain FC ladder,
                     which the two-ladder ChainView must not adopt).
      * direction  — -1 | +1, straight from the column.
      * peak_level — propagation_levels, the highest level reached. ChainView
                     stores peak (not current) level and only ascends it, so
                     propagation_levels is the right source.
 
    Only active chains are read (partial index ix_chains_active). Unlike
    make_load_active_chains this is NOT filtered by symbol/asset_class — the
    ChainView is global across all symbols, so one query hydrates everything.
    """
    from strategy.chain_view import SET_A_LADDER, SET_B_LADDER, ChainSet
 
    # baseline timeframe -> ChainSet. Baseline is ladder[0].
    _SET_FOR_BASELINE = {
        SET_A_LADDER[0]: ChainSet.A,   # "5s" -> A
        SET_B_LADDER[0]: ChainSet.B,   # "30s" -> B
    }
 
    async def load_chain_view_seed() -> list:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT chain_id, symbol, direction,
                       propagation_levels, active_timeframes
                FROM chains
                WHERE active = true
                """
            )
        out = []
        for r in rows:
            tfs = list(r["active_timeframes"]) if r["active_timeframes"] else []
            if not tfs:
                # No timeframes recorded — cannot classify a set; skip defensively.
                continue
            set_ = _SET_FOR_BASELINE.get(tfs[0])
            if set_ is None:
                # Baseline tf isn't an A/B baseline — chain belongs to another
                # ladder (e.g. four-chain). The two-ladder ChainView must not
                # adopt it.
                continue
            out.append({
                "symbol": r["symbol"],
                "set": set_.value,             # "A" | "B" — seed_active wraps in ChainSet
                "chain_id": r["chain_id"],
                "direction": int(r["direction"]),
                "peak_level": int(r["propagation_levels"]),
            })
        return out
 
    return load_chain_view_seed

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
                     symbol, asset_class, strategy, trigger_level, decision_price,
                     exit_reason, decided_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                ON CONFLICT (signal_id) DO NOTHING
                """,
                row["signal_id"], row["position_key"], row["chain_id"],
                row["direction"], row["action"], row["symbol"],
                row["asset_class"], row.get("strategy", "two_ladder"),
                row.get("trigger_level"),
                row.get("decision_price"), row.get("exit_reason"),
                row["decided_at"],
            )
        # asyncpg returns 'INSERT 0 1' on insert, 'INSERT 0 0' on conflict-skip.
        return result.endswith(" 1")
    return insert_signal

def make_load_open_positions(pool):
    """Load positions currently OPEN per the trade_signals log: an ENTER with no
    matching EXIT for the same (strategy, position_key). Used by the signal
    process to rebuild the runtime's open-position watch set on restart so exit
    evaluation resumes for positions entered before the restart.
 
    Returns list[dict] with keys: position_key, symbol, asset_class, chain_id,
    direction ('LONG'|'SHORT'), strategy — exactly what
    signal_process._runtime_hooks().load_open() maps into OpenPosition.
 
    Matching is on (strategy, position_key) together (migrations 004/011): a
    position_key is unique only within a strategy, so an EXIT closes only its
    OWN strategy's ENTER.
 
    For each still-open (strategy, position_key) we return the ENTER row's
    identity fields (symbol/asset_class/chain_id/direction are fixed at ENTER;
    an EXIT only references the key). If the same key was entered, exited, and
    re-entered, the NOT EXISTS correlated on decided_at ordering below treats it
    as open iff the LATEST action is an ENTER — see the subquery note.
    """
    async def load_open_positions() -> list:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                -- The latest action per (strategy, position_key). A position is
                -- OPEN iff its most recent decision is an ENTER (no EXIT has
                -- superseded it). DISTINCT ON picks the newest row per key by
                -- decided_at (then created_at as a tiebreak for equal decided_at).
                SELECT position_key, symbol, asset_class, chain_id,
                       direction::text AS direction, strategy
                FROM (
                    SELECT DISTINCT ON (strategy, position_key)
                           strategy, position_key, symbol, asset_class,
                           chain_id, direction, action
                    FROM trade_signals
                    ORDER BY strategy, position_key,
                             decided_at DESC, created_at DESC
                ) latest
                WHERE action = 'ENTER'
                """
            )
        # asyncpg Records are mapping-like; the caller uses r["..."] and
        # r.get("strategy", ...). Convert to plain dicts so .get works.
        return [dict(r) for r in rows]
 
    return load_open_positions

# ---- warmup-bars persistence (warmup_bars table, migration 006) -------------

def make_persist_warmup_bars(pool):
    """warmup_bars UPSERT for a batch of seeded bars. Idempotent on the
    (symbol, timeframe, timestamp_ms) PK: re-running warmup, or a later warmup that
    fetched fresher history, re-writes the same bars in place rather than erroring
    or duplicating — the same last-write-wins posture as the feed_cursors handoff.
    One (symbol, timeframe) seed is up to WINDOW_SIZE (5000) bars, so the batch
    goes through executemany in a single acquired connection rather than a
    round-trip per bar.

    points is a list[OHLCPoint] (producer/bar.py): each has .timestamp (epoch ms,
    int) and .open/.high/.low/.close (float). asset_class is stored on every row
    (denormalized, as the schema defines it) so the buffer loader can filter by
    class without a join."""
    async def persist_warmup_bars(symbol: str, asset_class: str, timeframe: str,
                                  points) -> None:
        if not points:
            return
        rows = [
            (symbol, timeframe, p.timestamp, p.open, p.high, p.low, p.close,
             asset_class)
            for p in points
        ]
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO warmup_bars
                    (symbol, timeframe, timestamp_ms, open, high, low, close,
                     asset_class, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
                ON CONFLICT (symbol, timeframe, timestamp_ms)
                DO UPDATE SET
                    open        = EXCLUDED.open,
                    high        = EXCLUDED.high,
                    low         = EXCLUDED.low,
                    close       = EXCLUDED.close,
                    asset_class = EXCLUDED.asset_class,
                    updated_at  = now()
                """,
                rows,
            )
    return persist_warmup_bars


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