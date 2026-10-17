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

import logging
import time
from datetime import datetime
from decimal import Decimal
from typing import Optional, Union
from collections import deque

log = logging.getLogger("producer.persistence")

def _as_timestamptz(value: Union[str, datetime]) -> datetime:
    """Coerce a decision timestamp to a datetime for a timestamptz bind.

    The chain machine carries decision times as ISO-8601 STRINGS end-to-end
    (service2_machine._epoch_ms_to_iso -> ChainEvent.timestamp -> the raw event
    dict -> SignalWriter's `decided_at`), because the state machine compares them
    lexically. asyncpg does NOT coerce str -> timestamptz: binding the raw string
    raises DataError on EVERY emit, and — being uncaught on the emit path — that
    exception unwound all the way out to the Kafka consume loop and killed the
    pipeline. It is why no trade signal was ever written.

    Every other writer in this module already converts at the bind boundary (see
    persist_chain_init / persist_chain_propagate below, both of which call
    datetime.fromisoformat before binding). This is that same conversion, applied
    at the one boundary that was missing it — the type contract belongs here, at
    the DB edge, so a future caller of insert_signal cannot reintroduce the crash.

    Accepts a datetime unchanged, so a caller already holding one (or a future
    non-string producer) needs no special-casing.
    """
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


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
    """chains INSERT at initiation + the initiation step (step_index = 0) into
    chain_steps.
 
    Per migration 008, chain_steps is the authoritative per-step record and
    chains.history is no longer written per-step (it defaults to '[]' and is
    kept only so nothing reading the column breaks). The initiation step is
    step_index = 0 (level 1); subsequent propagations are written by
    persist_chain_propagate at step_index = level - 1. The chains row and its
    step-0 row are written in ONE transaction so a chain never exists without
    its initiation step (hydrate reconstructs current_level from the step
    count, so a missing step-0 would under-report the level by one).
 
    Per migration 019 the row also carries `ladder`, the registered ladder name.
    ON CONFLICT (chain_id) DO NOTHING has been here since 002 but was dead code:
    chain_id was a fresh uuid4 per call, so the conflict could never fire and a
    replayed initiation wrote a duplicate row. chain_id is now derived from the
    initiation (service2_machine.chain_id_for), which makes this clause do the
    job it always claimed to — a replayed init is a genuine no-op, like
    chain_steps' (chain_id, step_index).
    """
    async def persist_chain_init(chain: dict) -> None:
        first = chain["history"][0]
        origin_time = datetime.fromisoformat(first["timestamp"])
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO chains
                        (chain_id, symbol, asset_class, ladder, direction,
                         origin_time, origin_price, propagation_levels,
                         active_timeframes, history, active, last_updated)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, '[]'::jsonb, true, $6)
                    ON CONFLICT (chain_id) DO NOTHING
                    """,
                    chain["chain_id"], chain["symbol"], chain["asset_class"],
                    chain["ladder"], chain["direction"], origin_time,
                    first.get("price"), 1, chain["active_timeframes"],
                )
                await conn.execute(
                    """
                    INSERT INTO chain_steps
                        (chain_id, step_index, level, timeframe, step_time, price)
                    VALUES ($1, 0, $2, $3, $4, $5)
                    ON CONFLICT (chain_id, step_index) DO NOTHING
                    """,
                    chain["chain_id"], first["level"], first["timeframe"],
                    origin_time, first.get("price"),
                )
    return persist_chain_init

def make_persist_chain_propagate(pool):
    """chains metadata UPDATE on propagation + an append-only chain_steps INSERT.

    Per migration 008 this replaces the old `history = history || $::jsonb`
    growing-JSONB append (which rewrote the whole column on every step — write
    amplification worsening with chain depth). The chains row now takes a
    FIXED-SIZE UPDATE (level/timeframes/timestamp only), and the per-step record
    is a plain append-only INSERT into chain_steps at step_index = level - 1
    (the initiation step at index 0 is written by persist_chain_init). Both in
    one transaction so the metadata and its step land together. ON CONFLICT DO
    NOTHING on the step makes a replayed propagation idempotent, mirroring the
    idempotency the old upsert had.
    """
    async def persist_chain_propagate(chain_id: str, new_level: int,
                                      active_timeframes: list, ts: datetime,
                                      new_step: dict) -> None:
        step_time = datetime.fromisoformat(new_step["timestamp"])
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
                    new_step["timeframe"], step_time, new_step.get("price"),
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

def make_load_last_base_signal(pool, symbol: str, baseline_timeframe: str):
    """The most recent non-zero signal at a ladder's baseline timeframe, as
    (signal, iso_timestamp), or None if there is none.
 
    This reconstructs the machine's INIT guard across a restart. STAGE 1
    initiates when the baseline signal differs from _last_base_signal, and None
    counts as different — so before this existed, the first baseline signal after
    every restart initiated a chain unconditionally, next to the still-active
    chain hydrate had just loaded.
 
    The guard is only ever compared for inequality and is set to the current
    baseline signal at every init, which makes it exactly "the direction of the
    most recent non-zero baseline signal" — what this query returns.
 
    Cold path (once per machine at hydrate). uq_signals_stream
    (symbol, timeframe, timestamp) serves it as a backward index scan that stops
    at the first non-zero row, so the filter does not force a wide scan even
    though `signals` is the highest-volume table here.
 
    asset_class is deliberately not in the predicate: symbol already determines
    it, and adding it would put a column outside uq_signals_stream in the way of
    a query whose whole point is to be a cheap index scan.
    """
    async def load_last_base_signal() -> Optional[tuple]:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT signal, timestamp
                FROM signals
                WHERE symbol = $1 AND timeframe = $2 AND signal <> 0
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                symbol, baseline_timeframe,
            )
        if row is None:
            return None
        return int(row["signal"]), row["timestamp"].isoformat()
    return load_last_base_signal

def make_load_active_chains(pool, symbol: str, asset_class: str,
                            ladder_name: str):
    """Hydrate active chains for one (symbol, ladder) on restart, so chain state
    survives a restart.
 
    THE DISCRIMINATOR IS THE LADDER NAME (migration 019). It used to be the
    chain's baseline timeframe (history[0].timeframe == ladder[0]), which is not
    unique across ladders: FC and PF both baseline at 1m and both run on
    crypto_symbols(), so each machine adopted the other's chains on restart and
    advanced them under its own level numbering — 1h is level 5 in FC and level 6
    in PF. Ownership is now explicit and filtered in SQL.
 
    No legacy branch: migration 019 retired every chain that predates the
    `ladder` column, and ck_chains_ladder_required makes the column mandatory on
    anything live, so a NULL ladder cannot reach this query.
 
    Per migration 008 the per-step history is reconstructed from chain_steps
    (authoritative, append-only) rather than the chains.history column. One
    indexed SELECT per chain ordered by step_index yields the same ordered list
    of {level, timeframe, timestamp, price} dicts the machine expects, so
    current_level (= len - 1), next_expected_level (= len), and last_timestamp
    derive exactly as before. Ordering is by step_index, NOT step_time: distinct
    timeframes can share a bar timestamp, so the explicit index is the only
    stable order (matching how the old JSONB array was appended)."""
    async def load_active_chains() -> list:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT chain_id, symbol, asset_class, ladder, direction,
                       origin_time, origin_price, propagation_levels,
                       active_timeframes
                FROM chains
                WHERE active = true AND symbol = $1 AND asset_class = $2
                  AND ladder = $3
                """,
                symbol, asset_class, ladder_name,
            )
            # Reconstruct each chain's ordered step history from chain_steps.
            step_rows = await conn.fetch(
                """
                SELECT cs.chain_id, cs.step_index, cs.level, cs.timeframe,
                       cs.step_time, cs.price
                FROM chain_steps cs
                JOIN chains c ON c.chain_id = cs.chain_id
                WHERE c.active = true AND c.symbol = $1 AND c.asset_class = $2
                  AND c.ladder = $3
                ORDER BY cs.chain_id, cs.step_index
                """,
                symbol, asset_class, ladder_name,
            )
        # Group steps by chain_id, preserving step_index order (query is ORDER BY).
        history_by_chain: dict = {}
        for s in step_rows:
            history_by_chain.setdefault(s["chain_id"], []).append({
                "level": s["level"],
                "timeframe": s["timeframe"],
                # step_time is timestamptz; the machine compares/parses ISO
                # strings, so emit isoformat exactly as the live path stored it.
                "timestamp": s["step_time"].isoformat(),
                "price": s["price"],
            })
        out = []
        for r in rows:
            history = history_by_chain.get(r["chain_id"], [])
            # A chain with no steps cannot be reconstructed (current_level and
            # next_expected_level derive from the step count). persist_chain_init
            # writes the row and its step-0 in ONE transaction, so this should be
            # unreachable — log it rather than silently dropping the chain.
            if not history:
                log.warning("skipping chain %s: no chain_steps rows (expected "
                            "at least step 0 from persist_chain_init)",
                            r["chain_id"])
                continue
            # Reconstruct the in-memory chain dict shape the machine expects.
            last = history[-1]
            out.append({
                "chain_id": r["chain_id"], "symbol": r["symbol"],
                "asset_class": r["asset_class"], "ladder": r["ladder"],
                "direction": r["direction"],
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
                     belongs to some other ladder, e.g. the confirmed_15m FC ladder,
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
                # ladder (e.g. confirmed_15m). The two-ladder ChainView must not
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
        # decided_at arrives as an ISO STRING from the chain machine. asyncpg will
        # not coerce it to the timestamptz column — binding it raw raises DataError
        # on every emit. Do not remove this. See _as_timestamptz.
        decided_at = _as_timestamptz(row["decided_at"])
        # Visibility: log the ATTEMPT before the write, so a row that raises inside
        # the INSERT (a schema/enum/NULL violation) is still traceable to its
        # signal_id — the log line exists even if the write never returns. Kept at
        # INFO because trade_signals writes are rare (one per decision) and are the
        # PRODUCT: every one is worth a line while proving the path.
        log.info(
            "insert_signal ATTEMPT signal_id=%s action=%s %s/%s strategy=%s "
            "decided_at=%s",
            row["signal_id"], row["action"], row["symbol"],
            row["position_key"], row.get("strategy", "two_ladder"),
            decided_at.isoformat(),
        )
        t0 = time.perf_counter()
        try:
            async with pool.acquire() as conn:
                result = await conn.execute(
                    """
                    INSERT INTO trade_signals
                        (signal_id, position_key, chain_id, direction, action,
                         symbol, asset_class, strategy, trigger_level, decision_price,
                         exit_reason, fraction, decided_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                    ON CONFLICT (signal_id) DO NOTHING
                    """,
                    row["signal_id"], row["position_key"], row["chain_id"],
                    row["direction"], row["action"], row["symbol"],
                    row["asset_class"], row.get("strategy", "two_ladder"),
                    row.get("trigger_level"),
                    row.get("decision_price"), row.get("exit_reason"),
                    # fraction (migration 016): portion of the position this
                    # signal concerns; 1.0 = whole position, the pre-016
                    # semantics and the default for writers that don't set it.
                    # Bound via Decimal(str(...)) so the NUMERIC(5,4) column
                    # receives the schedule's exact decimal (0.05, 0.35, ...),
                    # not a binary-float approximation — the consumer's
                    # cumulative-fraction arithmetic relies on exactness.
                    Decimal(str(row.get("fraction", 1.0))),
                    decided_at,
                )
        except Exception:
            # An exception here is exactly the failure mode that once killed the
            # pipeline silently. Log it WITH the signal_id and re-raise unchanged —
            # the caller's error handling is unchanged, we've only made the failure
            # visible. Do not swallow: swallowing would hide a broken write behind
            # a "no rows" symptom, which is what we're trying to eliminate.
            elapsed_ms = (time.perf_counter() - t0) * 1000
            log.exception(
                "insert_signal FAILED signal_id=%s action=%s %s after %.1fms — "
                "the trade_signals write raised; row NOT persisted",
                row["signal_id"], row["action"], row["symbol"], elapsed_ms,
            )
            raise
        elapsed_ms = (time.perf_counter() - t0) * 1000
        # asyncpg returns 'INSERT 0 1' on insert, 'INSERT 0 0' on conflict-skip.
        newly = result.endswith(" 1")
        if newly:
            log.info(
                "insert_signal OK signal_id=%s action=%s %s — ROW WRITTEN (%.1fms)",
                row["signal_id"], row["action"], row["symbol"], elapsed_ms,
            )
        else:
            # Not an error: the deterministic signal_id already existed (a replay or
            # a re-read of the same decision). Logged at INFO so an idempotent skip
            # is DISTINGUISHABLE from "nothing happened" while proving the path —
            # under normal steady-state this could drop to DEBUG.
            log.info(
                "insert_signal SKIP signal_id=%s action=%s %s — already present "
                "(idempotent, %.1fms)",
                row["signal_id"], row["action"], row["symbol"], elapsed_ms,
            )
        return newly
    return insert_signal

def make_load_strategy_open_entries(pool, strategy: str):
    """Open ENTERs for ONE strategy, with created_at — for a strategy that
    keeps its own position state (Rule 1) and must restore the time its
    position opened (the stall clock runs from it). Same open-position rule
    as make_load_open_positions: an ENTER with no closing (non-SCALE) EXIT."""
    async def load() -> list:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT e.position_key, e.symbol, e.chain_id,
                       e.direction::text AS direction, e.created_at
                FROM trade_signals e
                WHERE e.strategy = $1 AND e.action = 'ENTER'
                  AND NOT EXISTS (
                      SELECT 1 FROM trade_signals x
                      WHERE x.strategy = e.strategy
                        AND x.position_key = e.position_key
                        AND x.action = 'EXIT'
                        AND x.exit_reason <> 'SCALE'
                  )
                ORDER BY e.created_at
                """, strategy)
        return [dict(r) for r in rows]
    return load


def make_load_opposing_chain_since(pool):
    """Restart reconcile for the chain rules: did a chain in `direction` on
    `symbol` initiate on the rule's own `ladder` after `since`? The live rule
    exits only on an opposing initiation on its ladder, and its clock is when
    an event REACHED the process — so this compares the initiation step's
    recorded_at (wall time), not the chain's market-time origin."""
    async def load(symbol: str, direction: int, since, ladder: str) -> bool:
        async with pool.acquire() as conn:
            return bool(await conn.fetchval(
                """
                SELECT 1 FROM chains c
                JOIN chain_steps s ON s.chain_id = c.chain_id AND s.step_index = 0
                WHERE c.symbol = $1 AND c.direction = $2 AND c.ladder = $4
                  AND s.recorded_at > $3
                LIMIT 1
                """, symbol, int(direction), since, ladder))
    return load


def make_load_chain_confirmed(pool):
    """Restart reconcile for the chain rules: did `chain_id` reach `level`
    by wall time `by` (entry + stall window)? A trade restored from
    trade_signals has lost its in-memory `confirmed` flag; without this, a
    trade that confirmed before a restart inside its stall window was
    stalled out."""
    async def load(chain_id: str, level: int, by) -> bool:
        async with pool.acquire() as conn:
            return bool(await conn.fetchval(
                """
                SELECT 1 FROM chain_steps
                WHERE chain_id = $1 AND level >= $2 AND recorded_at <= $3
                LIMIT 1
                """, chain_id, int(level), by))
    return load


def make_load_open_positions(pool):
    """Load positions currently OPEN per the trade_signals log. Used by the
    signal process to rebuild the runtime's open-position watch set on restart
    so exit evaluation resumes for positions entered before the restart.

    Returns list[dict] with keys: position_key, symbol, asset_class, chain_id,
    direction ('LONG'|'SHORT'), strategy — exactly what
    signal_process._runtime_hooks().load_open() maps into OpenPosition.

    Matching is on (strategy, position_key) together (migrations 004/011): a
    position_key is unique only within a strategy, so an EXIT closes only its
    OWN strategy's ENTER.

    OPENNESS UNDER PARTIAL EXITS (migration 016). The pre-016 rule — open iff
    the latest action is an ENTER — is WRONG for a scale-out position: its
    first SCALE tranche is an EXIT row, which would evict the position from
    the watch set on restart, stranding the remaining tranches and the
    TERMINAL (the consumer would hold a position we stopped watching — the
    exact reconciliation gap 004 exists to prevent). The rule is now:

        OPEN iff an ENTER exists AND
          * no CLOSING exit exists — any EXIT whose reason is not 'SCALE'
            (STOP/TARGET/TIME/MANUAL/KILL_SWITCH/TERMINAL all close), AND
          * cumulative SCALE fraction < 1.0 (all-tranches-banked closes the
            position with no TERMINAL ever written; fraction is NUMERIC so
            the sum is exact, and per-tranche signal_ids mean no row is ever
            double-counted).

    For whole-exit strategies (two_ladder, confirmed_15m) every exit is
    non-SCALE, so this is behaviourally identical to the old rule. Re-entry
    on a (strategy, position_key) cannot occur — the deterministic ENTER
    signal_id admits one ENTER per key, ever — so the old latest-row ordering
    dance is unnecessary.
    """
    async def load_open_positions() -> list:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT e.position_key, e.symbol, e.asset_class, e.chain_id,
                       e.direction::text AS direction, e.strategy
                FROM trade_signals e
                WHERE e.action = 'ENTER'
                  AND NOT EXISTS (
                      SELECT 1 FROM trade_signals x
                      WHERE x.strategy = e.strategy
                        AND x.position_key = e.position_key
                        AND x.action = 'EXIT'
                        AND x.exit_reason <> 'SCALE'
                  )
                  AND COALESCE((
                      SELECT SUM(x.fraction) FROM trade_signals x
                      WHERE x.strategy = e.strategy
                        AND x.position_key = e.position_key
                        AND x.action = 'EXIT'
                        AND x.exit_reason = 'SCALE'
                  ), 0) < 1.0
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


def make_persist_live_bar(pool):
    """BAR STORE write for one live bar the signal process appended: the same
    warmup_bars table warmup seeds, so make_load_buffers hydrates a restarted
    machine up to where the last run got. Without it, buffers came back as
    warmup left them while Kafka resumed after the last committed offset, and
    every bar in between vanished — spliced out of the series Sumtyme reads
    as consecutive steps.

    DO NOTHING on conflict, unlike warmup's upsert: the buffer keeps the FIRST
    version of a bar (a replay with revised values is dropped as a duplicate),
    so the store must too."""
    async def persist_live_bar(bar) -> None:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO warmup_bars
                    (symbol, timeframe, timestamp_ms, open, high, low, close,
                     asset_class, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
                ON CONFLICT (symbol, timeframe, timestamp_ms) DO NOTHING
                """,
                bar.symbol, bar.timeframe, bar.timestamp, bar.open, bar.high,
                bar.low, bar.close, bar.asset_class,
            )
    return persist_live_bar


async def read_bar_store_cursors(pool) -> dict:
    """The newest stored bar per (symbol, timeframe), as
    cursors[symbol][timeframe] = epoch ms — the feed's resume point: everything
    at or before it is already in the signal process's buffers."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT symbol, timeframe, max(timestamp_ms) AS ts FROM warmup_bars "
            "GROUP BY symbol, timeframe")
    out: dict = {}
    for r in rows:
        out.setdefault(r["symbol"], {})[r["timeframe"]] = r["ts"]
    return out


def make_load_buffers(pool, symbol: str, timeframes: list):
    """Hydrate one machine's per-timeframe buffers from warmup_bars (migration
    006) so forecasting can start immediately on restart instead of waiting
    days-to-weeks for coarse buffers to refill from the live stream.
 
    Returns a zero-arg async loader producing {timeframe: deque(maxlen=WINDOW_SIZE)}
    of tick dicts shaped exactly like ServiceTwoChainMachine._tick_dict. Only the
    timeframes in `timeframes` are loaded (this machine's ladder); a timeframe
    with no seeded rows yields an empty deque (it simply fills live).
 
    Newest WINDOW_SIZE rows per (symbol, timeframe) are read via the
    timestamp_ms DESC index, then reversed to ascending so the deque is
    chronological (oldest at the left, newest at the right) — the order on_bar's
    append + maxlen eviction assumes.
    """
    from producer.service2_machine import WINDOW_SIZE
 
    async def load_buffers() -> dict:
        buffers: dict = {}
        async with pool.acquire() as conn:
            for tf in timeframes:
                rows = await conn.fetch(
                    """
                    SELECT symbol, timestamp_ms, open, high, low, close,
                           asset_class, timeframe
                    FROM warmup_bars
                    WHERE symbol = $1 AND timeframe = $2
                    ORDER BY timestamp_ms DESC
                    LIMIT $3
                    """,
                    symbol, tf, WINDOW_SIZE,
                )
                # rows are newest-first; reverse to oldest-first for the deque.
                dq = deque(maxlen=WINDOW_SIZE)
                for r in reversed(rows):
                    dq.append({
                        "symbol": r["symbol"],
                        "timestamp": r["timestamp_ms"],   # epoch ms, matches _tick_dict
                        "open": r["open"],
                        "high": r["high"],
                        "low": r["low"],
                        "close": r["close"],
                        "timeframe": r["timeframe"],
                        "asset_class": r["asset_class"],
                    })
                buffers[tf] = dq
        return buffers
 
    return load_buffers


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