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
import logging
from datetime import datetime
from typing import Optional
from collections import deque

log = logging.getLogger("producer.persistence")


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
    """chains INSERT at initiation, PLUS the initiation step (step_index 0) into
    chain_steps — both in ONE transaction so the FK-ordered chain row and its
    first step are durable together before the chain_initiated event is emitted
    (migration 002's write-before-emit). chains.history is NO LONGER written
    per-step (migration 008): the authoritative per-step record is chain_steps.
    We still seed history with the init step here so a reader that inspects the
    column directly sees a sane value, but hydrate reconstructs from chain_steps,
    not from this column.

    step_index is derived as level - 1: init is level 1 -> step_index 0; the Nth
    propagation is level N+1 -> step_index N. Steps are appended in order with no
    dedup, and terminate adds no step, so levels stay contiguous and this mapping
    is exact (it matches the old len(history)-based indexing byte-for-byte)."""
    async def persist_chain_init(chain: dict) -> None:
        first = chain["history"][0]
        origin_time = datetime.fromisoformat(first["timestamp"])
        async with pool.acquire() as conn:
            async with conn.transaction():
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
                # The initiation step (step_index 0, level 1). Idempotent on the
                # (chain_id, step_index) PK so a replayed init is a no-op.
                await conn.execute(
                    """
                    INSERT INTO chain_steps
                        (chain_id, step_index, level, timeframe, step_time, price)
                    VALUES ($1, 0, 1, $2, $3, $4)
                    ON CONFLICT (chain_id, step_index) DO NOTHING
                    """,
                    chain["chain_id"], first["timeframe"], origin_time,
                    first.get("price"),
                )
    return persist_chain_init


def make_persist_chain_propagate(pool):
    """chains UPDATE on propagation + an append-only chain_steps INSERT
    (migration 008). The growing-JSONB append (history || $::jsonb) is GONE — it
    rewrote the whole column on every step, the write-amplification this fixes.
    Now: a fixed-size UPDATE of the chain's scalar metadata (level, timeframes,
    last_updated) and a plain INSERT of the new step. history is not touched.

    step_index = new_level - 1 (the Nth propagation lands at level N+1 ->
    step_index N), matching the init step at index 0. Both statements run in one
    transaction so a step and its chain-level metadata advance together.

    NOTE: this synchronous version is the reference implementation; the live hot
    path routes propagation through the batched async writer, which performs the
    same two writes per step in bulk. They must stay behaviorally identical."""
    async def persist_chain_propagate(chain_id: str, new_level: int,
                                      active_timeframes: list, ts: datetime,
                                      new_step: dict) -> None:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE chains
                    SET propagation_levels = $1,
                        active_timeframes  = $2,
                        last_updated       = $3
                    WHERE chain_id = $4
                    """,
                    new_level, active_timeframes, ts, chain_id,
                )
                await conn.execute(
                    """
                    INSERT INTO chain_steps
                        (chain_id, step_index, level, timeframe, step_time, price)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (chain_id, step_index) DO NOTHING
                    """,
                    chain_id, new_step["level"] - 1, new_step["level"],
                    new_step["timeframe"],
                    datetime.fromisoformat(new_step["timestamp"]),
                    new_step.get("price"),
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
    asset_class AND by the chain's baseline timeframe being this set's baseline,
    so a Set A machine doesn't adopt a Set B chain (they share the chains table
    but belong to different ladders).

    history is RECONSTRUCTED from chain_steps (migration 008), not read from the
    chains.history JSONB (which is no longer maintained per-step). The
    reconstruction is byte-equivalent to the old in-memory history: one entry per
    step, ordered by step_index, no dedup — so len(history) and history[-1] keep
    the exact meaning the machine's current_level / next_expected_level /
    last_timestamp derivations rely on."""
    async def load_active_chains() -> list:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT chain_id, symbol, asset_class, direction, origin_time,
                       origin_price, propagation_levels, active_timeframes
                FROM chains
                WHERE active = true AND symbol = $1 AND asset_class = $2
                """,
                symbol, asset_class,
            )
            out = []
            for r in rows:
                # The baseline timeframe is active_timeframes[0] (SQL index 1);
                # only adopt chains whose baseline is THIS set's baseline.
                atfs = list(r["active_timeframes"])
                if not atfs or atfs[0] != ladder[0]:
                    continue
                # Reconstruct ordered history from chain_steps.
                step_rows = await conn.fetch(
                    """
                    SELECT level, timeframe, step_time, price
                    FROM chain_steps
                    WHERE chain_id = $1
                    ORDER BY step_index
                    """,
                    r["chain_id"],
                )
                if not step_rows:
                    # A chain row with no steps shouldn't happen (init writes
                    # step 0 in the same txn), but guard rather than crash hydrate.
                    log.warning("chain %s has no chain_steps; skipping hydrate", r["chain_id"])
                    continue
                history = [
                    {
                        "level": s["level"], "timeframe": s["timeframe"],
                        "timestamp": s["step_time"].isoformat(),
                        "price": s["price"],
                    }
                    for s in step_rows
                ]
                last = history[-1]
                out.append({
                    "chain_id": r["chain_id"], "symbol": r["symbol"],
                    "asset_class": r["asset_class"], "direction": r["direction"],
                    "current_level": len(history) - 1,
                    "next_expected_level": len(history),
                    "active_timeframes": atfs,
                    "history": history, "last_timestamp": last["timestamp"],
                })
        return out
    return load_active_chains


def make_load_chain_view_seed(pool):
    """Load every currently-active chain, across ALL symbols and both sets, to
    hydrate the STRATEGY's ChainView at startup — the consumer-side counterpart
    to make_load_active_chains (which hydrates the PRODUCER machine's own
    _active_chains). These are two different pieces of state that must BOTH
    survive a restart, and only one of them did before this:

      * the machine's _active_chains lets propagation/termination CONTINUE
        correctly for a chain (already hydrated, per-symbol-per-set, via
        make_load_active_chains);
      * the strategy's ChainView lets entry/exit DECISIONS see a chain that's
        already active, even if it never emits another event — e.g. a Set B
        chain already at its ladder's top (10m) can only ever terminate from
        here, it will never propagate again, so it never reaches the view
        through the normal event path.

    Without this, every restart silently erases the strategy's knowledge of
    every chain that existed before it (chain_prime candidates especially),
    even though those chains are still active in the DB and in the machine —
    entries would only fire for chains that happen to get a FRESH event after
    the restart, which active long-propagated chains, by definition, don't.

    Returns a flat list across all symbols (ChainView is one shared registry,
    not scoped per symbol) of dicts:
        {symbol, set: 'A'|'B', chain_id, direction, peak_level}
    peak_level is taken directly from propagation_levels — chains only ascend,
    so the persisted depth IS the peak the view needs; no need to replay
    history. The set is derived from the chain's baseline timeframe, which is
    active_timeframes[0] (the first timeframe the chain initiated at) —
    unambiguous (Set A's baseline '5s' and Set B's baseline '30s' don't overlap,
    even though their ladders share 30s/1m/2m higher up). This reads NO per-step
    data, so it is independent of the history-column removal in migration 008.
    """
    from producer.ladders import BASELINE

    baseline_to_set = {tf: s.value for s, tf in BASELINE.items()}

    async def load_seed() -> list:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT symbol, chain_id, direction, propagation_levels,
                       active_timeframes
                FROM chains
                WHERE active = true
                """
            )
        out = []
        for r in rows:
            atfs = list(r["active_timeframes"])
            if not atfs:
                continue
            set_value = baseline_to_set.get(atfs[0])
            if set_value is None:
                continue   # baseline timeframe matches neither ladder — shouldn't happen
            out.append({
                "symbol": r["symbol"], "set": set_value,
                "chain_id": r["chain_id"], "direction": r["direction"],
                "peak_level": r["propagation_levels"],
            })
        return out
    return load_seed


def make_load_open_positions(pool):
    """Reconstruct the strategy's open-position set from trade_signals at
    startup — the persistence path for StrategyRuntime's RuntimeHooks.
    load_open_positions.

    WHY THIS NEEDS NO NEW TABLE: trade_signals (migration 004) is already the
    durable, idempotent log of every ENTER/EXIT decision — SignalWriter writes
    it AT THE MOMENT the decision is made, not as a separate "persist the open
    set" step. An ENTER row defines a position_key; an EXIT row closes it. So
    "open positions" is just an anti-join: every ENTER whose position_key has
    no corresponding EXIT row. Maintaining a second, separate store for this
    (as a prior in-memory version of the hooks did) is both redundant and
    actively misleading — it can drift from or simply not survive what
    trade_signals already durably knows.

    chain_id on the ENTER row IS chain_prime_id: the entry logic keys
    emit_entry's chain_id on prime.chain_id (see TwoLadderEntry.on_event), so
    no separate lookup is needed to recover it.

    Returns plain dicts (not OpenPosition — that type lives in strategy.runtime,
    and this module is producer-side; the caller converts), one per currently
    open position:
        {position_key, chain_id, direction: 'LONG'|'SHORT', symbol, asset_class}
    """
    async def load_open_positions() -> list:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT e.position_key, e.chain_id, e.direction,
                       e.symbol, e.asset_class
                FROM trade_signals e
                WHERE e.action = 'ENTER'
                  AND NOT EXISTS (
                      SELECT 1 FROM trade_signals x
                      WHERE x.position_key = e.position_key
                        AND x.action = 'EXIT'
                  )
                """
            )
        return [
            {
                "position_key": r["position_key"], "chain_id": r["chain_id"],
                "direction": r["direction"], "symbol": r["symbol"],
                "asset_class": r["asset_class"],
            }
            for r in rows
        ]
    return load_open_positions


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