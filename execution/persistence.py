"""
execution/persistence.py — concrete ExecutorHooks persistence over the 001
trade-lifecycle schema.

The mirror of producer/persistence.py, for the execution side. Each public
factory is make_<name>(pool) and returns an async closure matching one callable
in ExecutorHooks (execution/signal_executor.py). Keeping the SQL here (not inline
in run_executor.py) means the schema contract lives in one place and the launcher
stays declarative — the same split the producer side uses.

All SQL targets the 001 schema (trades, orders, fills, exit_state,
execution_caps, kill_switch). asyncpg, one shared pool, thin async functions
over it — same conventions as producer/persistence.py (statement_cache_size=0 is
set on the pool by the caller for Supabase/pgbouncer).

------------------------------------------------------------------------------
DECIMAL / NUMERIC. The 001 schema stores quantities as numeric(38,18) and prices
as double precision. asyncpg maps numeric<->Decimal and double precision<->float.
The executor works in Decimal for quantities and mixes Decimal/float for prices
(see CapConfig, which crosses float->Decimal once at construction). These
closures therefore:
  * pass Decimal quantities straight through to numeric columns,
  * cast price floats with float() at the double precision boundary,
  * return Decimal for quantity fields read back (asyncpg already does this).

ENUMS. trade_state, order_status, order_intent, trade_side, exit_reason are PG
enums; asyncpg accepts the matching str value (e.g. "OPEN", "ENTRY", "LONG"),
which is exactly what the executor's *.value gives. No codec registration needed.
------------------------------------------------------------------------------

STRATEGY / POSITION_KEY (resolved by migration 014):

OpenTrade carries `strategy` and `position_key`. position_key is fully
recoverable from (chain_id, side) ("{chain_id}:{L|S}", the exact rule in
signal_writer.position_key), so we reconstruct it. `strategy` is now a real
column on `trades` (migration 014, mirroring 011's column on trade_signals):
persist_trade writes it, load_open_trades reads it. Apply 014 before running
this module against a multi-strategy deployment — without it, load_open_trades'
SELECT of the strategy column fails, and (worse, if you removed the SELECT)
two strategies holding a position on the same (chain_id, side) would collide on
one in-memory key. DEFAULT_STRATEGY below only documents the column's default.
"""
from __future__ import annotations

import json
import logging
from decimal import Decimal
from typing import Optional

import asyncpg

from execution.signal_executor import (
    ExecutorHooks,
    OpenTrade,
    Touch,
    ExitCap,
    EscalationRung,
)
from execution.pricing import CapConfig
from execution.types import OrderIntent, TradeSide

log = logging.getLogger("executor.persistence")

# The `trades.strategy` column default (migration 014), mirroring 011's default
# on trade_signals. load_open_trades reads the real column; this constant is kept
# only as documentation of what a row with no explicit strategy resolves to.
DEFAULT_STRATEGY = "two_ladder"


def _position_key(chain_id: str, side: str) -> str:
    """Reproduce signal_writer.position_key exactly: chain + directional suffix.
    side is the trade_side enum's str value ('LONG'|'SHORT')."""
    return f"{chain_id}:{'L' if side == 'LONG' else 'S'}"


# ---------------------------------------------------------------------------
# Persistence — writes to the 001 schema
# ---------------------------------------------------------------------------

def make_persist_trade(pool: asyncpg.Pool):
    """UPSERT a trade row from an OpenTrade. Idempotent on the (chain_id, side)
    unique constraint: a replay re-writing the same trade updates in place rather
    than opening a second position (the DB backstop the schema was built for).

    NOTE OpenTrade does not carry chain_id directly; position_key encodes it
    ("{chain_id}:{L|S}"). We split it back out to satisfy the NOT NULL chain_id
    column and the (chain_id, side) key. entry_client_order_id, entry_target_qty,
    and state are the other NOT NULLs OpenTrade can supply."""
    async def persist_trade(t: OpenTrade) -> None:
        # position_key is "{chain_id}:{L|S}" — chain_id may itself contain no
        # ':' (it's a uuid string), so rsplit once is safe.
        chain_id = t.position_key.rsplit(":", 1)[0]
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO trades
                    (trade_id, chain_id, symbol, asset_class, side, state,
                     entry_client_order_id, entry_target_qty, entry_filled_qty,
                     exit_filled_qty, strategy, scale_banked, exit_reason,
                     last_updated)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                        $12::jsonb, $13, now())
                ON CONFLICT (chain_id, side) DO UPDATE SET
                    state             = EXCLUDED.state,
                    entry_filled_qty  = EXCLUDED.entry_filled_qty,
                    exit_filled_qty   = EXCLUDED.exit_filled_qty,
                    scale_banked      = EXCLUDED.scale_banked,
                    exit_reason       = EXCLUDED.exit_reason,
                    last_updated      = now()
                """,
                t.trade_id, chain_id, t.symbol, t.asset_class, t.side.value,
                t.state, t.entry_client_order_id, t.target_qty, t.filled_qty,
                t.exit_filled_qty, t.strategy,
                # scale_banked: {trigger_level -> fraction}, both as STRINGS so
                # the fractions stay exact decimals through JSON (a JSON number
                # would round-trip through binary float). Empty for whole-exit
                # strategies. exit_reason: the reason driving the current/last
                # exit lifecycle (distinguishes closing-a-tranche from
                # closing-for-good across a restart — migration 017).
                json.dumps({str(k): str(v) for k, v in t.scale_banked.items()}),
                t.exit_reason,
            )
    return persist_trade


def make_update_trade_state(pool: asyncpg.Pool):
    """Set a trade's state, and its halt_reason. The 001 ck_trades_halt_reason
    CHECK requires halt_reason present iff state='HALTED', so we write reason as
    given for HALTED and NULL it otherwise — never leave a stale halt_reason on a
    recovered trade. Also stamps closed_at when moving to a terminal state."""
    async def update_trade_state(trade_id: str, state: str,
                                 halt_reason: Optional[str]) -> None:
        reason = halt_reason if state == "HALTED" else None
        terminal = state in ("CLOSED", "REJECTED")
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE trades
                   SET state        = $2,
                       halt_reason  = $3,
                       closed_at    = CASE WHEN $4 AND closed_at IS NULL
                                          THEN now() ELSE closed_at END,
                       last_updated = now()
                 WHERE trade_id = $1
                """,
                trade_id, state, reason, terminal,
            )
    return update_trade_state


def make_persist_order(pool: asyncpg.Pool):
    """UPSERT an order row from the executor's _order_row dict. Idempotent on the
    orders.client_order_id UNIQUE constraint (the deterministic crash-recovery
    key), so a re-submit of the same order updates status rather than duplicating.

    The dict shape is exactly _order_row(...) in signal_executor.py:
      client_order_id, trade_id, symbol(*), asset_class(*), intent, side, qty,
      limit_price, venue_reference, cap_applied, status, exit_reason,
      escalation_rung.
    (*) symbol/asset_class are on the dict for the venue call but are NOT columns
    on `orders` (they live on the parent trade) — we ignore them here. qty maps
    to target_qty; filled_qty defaults to 0 and is advanced by advance_order_fill.
    """
    async def persist_order(row: dict) -> None:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO orders
                    (trade_id, intent, side, status, client_order_id,
                     limit_price, venue_reference, cap_applied,
                     exit_reason, escalation_rung, target_qty, last_updated)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
                ON CONFLICT (client_order_id) DO UPDATE SET
                    status          = EXCLUDED.status,
                    venue_reference = EXCLUDED.venue_reference,
                    cap_applied     = EXCLUDED.cap_applied,
                    last_updated    = now()
                """,
                row["trade_id"], row["intent"], row["side"], row["status"],
                row["client_order_id"],
                float(row["limit_price"]),
                None if row.get("venue_reference") is None else float(row["venue_reference"]),
                None if row.get("cap_applied") is None else float(row["cap_applied"]),
                row.get("exit_reason"), row.get("escalation_rung", 0),
                row["qty"],
            )
    return persist_order


def make_persist_fill(pool: asyncpg.Pool):
    """INSERT a fill row from the executor's _fill_row dict. Returns False if the
    venue_exec_id already exists (the fills UNIQUE constraint — a re-delivered or
    reconcile-re-read execution), True if it was newly inserted. The executor
    relies on this bool to avoid double-counting into filled_qty.

    The fill row must resolve to a real order_id and trade_id (both NOT NULL FKs
    on `fills`). The parent order is found by client_order_id — written on the
    orders row at submit, and carried on every fill row the executor builds —
    falling back to venue_order_id. (Looking up by venue_order_id alone, as
    before, never matched: nothing wrote orders.venue_order_id, so EVERY fill was
    skipped and reported as a duplicate, and the executor never advanced a
    trade's filled quantity.) The venue's order id is recorded on the order row
    the first time a fill reveals it.

    A fill that cannot be mapped cannot satisfy the FK: we skip it and return
    False. For a fill of a known trade that is a real fault (logged ERROR); for
    the reconcile sweep's foreign fills it is expected.

    filled_at_ms (epoch ms) maps to the timestamptz filled_at via to_timestamp.
    """
    async def persist_fill(row: dict) -> bool:
        cid = row.get("client_order_id")
        venue_order_id = row.get("venue_order_id")
        trade_id = row.get("trade_id")
        async with pool.acquire() as conn:
            order_id = None
            if cid is not None:
                order_id = await conn.fetchval(
                    "SELECT order_id FROM orders WHERE client_order_id = $1", cid)
            if order_id is None and venue_order_id is not None:
                order_id = await conn.fetchval(
                    "SELECT order_id FROM orders WHERE venue_order_id = $1",
                    venue_order_id,
                )
            if order_id is None or trade_id is None:
                if trade_id is not None:
                    log.error("persist_fill: fill %s of trade %s has no matching order "
                              "(client_order_id=%s venue_order_id=%s) — NOT recorded",
                              row.get("venue_exec_id"), trade_id, cid, venue_order_id)
                else:
                    log.info("persist_fill: unattributed fill venue_exec_id=%s "
                             "venue_order_id=%s — skipped",
                             row.get("venue_exec_id"), venue_order_id)
                return False
            if venue_order_id is not None:
                await conn.execute(
                    "UPDATE orders SET venue_order_id = $2 "
                    "WHERE order_id = $1 AND venue_order_id IS NULL",
                    order_id, venue_order_id)
            # INSERT; ON CONFLICT on the venue_exec_id UNIQUE makes the dup a
            # no-op. xmax=0 trick: RETURNING tells us whether a row was inserted.
            inserted = await conn.fetchval(
                """
                INSERT INTO fills
                    (order_id, trade_id, intent, qty, price, venue_exec_id,
                     fee, fee_currency, filled_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, to_timestamp($9 / 1000.0))
                ON CONFLICT (venue_exec_id) DO NOTHING
                RETURNING true
                """,
                order_id, trade_id, row["intent"], row["qty"],
                float(row["price"]), row["venue_exec_id"],
                float(row.get("fee", 0)), row.get("fee_currency"),
                row["filled_at_ms"],
            )
            return bool(inserted)
    return persist_fill


def make_advance_order_fill(pool: asyncpg.Pool):
    """Advance an order's filled_qty and status by client_order_id. Sets
    terminal_at when the status is terminal (FILLED/CANCELLED/REJECTED). The
    ck_orders_filled_le_target CHECK guards against over-fill — a violation
    raises, which is the correct loud failure (the venue told us we filled more
    than we ordered)."""
    async def advance_order_fill(client_order_id: str, filled_qty: Decimal,
                                 status: str) -> None:
        terminal = status in ("FILLED", "CANCELLED", "REJECTED")
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE orders
                   SET filled_qty  = $2,
                       status      = $3,
                       terminal_at = CASE WHEN $4 AND terminal_at IS NULL
                                         THEN now() ELSE terminal_at END,
                       last_updated = now()
                 WHERE client_order_id = $1
                """,
                client_order_id, filled_qty, status, terminal,
            )
    return advance_order_fill


def make_load_open_trades(pool: asyncpg.Pool):
    """Load all non-terminal trades as OpenTrade handles for boot reconcile.
    Scans the ix_trades_active partial index (state NOT IN CLOSED/REJECTED).

    position_key is reconstructed from (chain_id, side); strategy is read from
    the trades.strategy column (migration 014). Before 014, this fell back to a
    default — see the module-level STRATEGY/POSITION_KEY CAVEAT history.

    Escalation-ladder state (escalation_rung, escalation_deadline) is LEFT JOINed
    from exit_state so a CLOSING trade re-adopted on restart resumes at the rung
    and deadline it was already working, NOT from a blank slate. Without this the
    escalation driver sees exit_deadline_ms=None on a healthy working exit and
    HALTs it (and would re-escalate from rung 0). A trade with no exit_state row
    (never reached OPEN) yields NULLs, leaving the OpenTrade defaults untouched.
    escalation_deadline is timestamptz; we convert it back to epoch ms to match
    the in-memory exit_deadline_ms the escalation driver compares against now_ms().
    """
    async def load_open_trades() -> list[OpenTrade]:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT t.trade_id, t.chain_id, t.symbol, t.asset_class, t.side,
                       t.state, t.entry_client_order_id, t.entry_target_qty,
                       t.entry_filled_qty, t.exit_filled_qty, t.strategy,
                       t.scale_banked::text AS scale_banked_json, t.exit_reason,
                       es.escalation_rung,
                       (EXTRACT(EPOCH FROM es.escalation_deadline) * 1000)::bigint
                           AS escalation_deadline_ms
                  FROM trades t
                  LEFT JOIN exit_state es ON es.trade_id = t.trade_id
                 WHERE t.state NOT IN ('CLOSED', 'REJECTED')
                """
            )
        out: list[OpenTrade] = []
        for r in rows:
            side = TradeSide(r["side"])
            deadline_ms = r["escalation_deadline_ms"]
            out.append(OpenTrade(
                trade_id=str(r["trade_id"]),
                strategy=r["strategy"],
                position_key=_position_key(r["chain_id"], r["side"]),
                symbol=r["symbol"],
                asset_class=r["asset_class"],
                side=side,
                target_qty=r["entry_target_qty"],
                filled_qty=r["entry_filled_qty"],
                state=r["state"],
                entry_client_order_id=r["entry_client_order_id"],
                exit_filled_qty=r["exit_filled_qty"],
                # NULL when no exit_state row exists yet; keep the field default (0).
                exit_rung=r["escalation_rung"] if r["escalation_rung"] is not None else 0,
                exit_deadline_ms=int(deadline_ms) if deadline_ms is not None else None,
                # Scale-out state (migration 017): banked tranches back to
                # {int level -> Decimal fraction} (stored as strings for
                # exactness), and the driving exit reason — together these let
                # a restarted executor tell a CLOSING tranche (returns to OPEN)
                # from a CLOSING final (finalizes CLOSED), and recompute the
                # scale deficit from durable state alone.
                scale_banked={int(k): Decimal(v)
                              for k, v in json.loads(r["scale_banked_json"] or "{}").items()},
                exit_reason=r["exit_reason"],
            ))
        return out
    return load_open_trades


# ---------------------------------------------------------------------------
# Venue / market reads
# ---------------------------------------------------------------------------

def make_get_touch(pool: asyncpg.Pool):
    """DEPRECATED / no-op. The touch is VENUE state, not DB state — the 001
    schema has no quotes table (see its header: "no prices table here"). This
    factory exists only so build_executor_hooks can populate the field; the
    launcher OVERWRITES hooks.get_touch with the live adapter's get_touch
    (execution/adapter.ExecutionAdapter.get_touch, implemented on IBKRAdapter via
    reqTickers and BinanceAdapter via bookTicker). If it is ever left as-is it
    returns None, which the pricer treats as "no reference" and skips the order —
    a safe no-op, never a stale price."""
    async def get_touch(symbol: str, asset_class: str) -> Optional[Touch]:
        log.warning("get_touch(%s): DB no-op reached — launcher should have wired "
                    "the adapter's get_touch. Returning None.", symbol)
        return None
    return get_touch


def make_get_cap(pool: asyncpg.Pool):
    """Load a marketable-limit CapConfig for (asset_class, intent) from the
    hot-reloadable execution_caps table. Read at order time (not cached) so an
    operator can widen a cap live without a redeploy — the table's whole purpose.
    Raises if the row is missing or disabled: pricing an order with no cap is not
    a safe default."""
    async def get_cap(asset_class: str, intent: OrderIntent) -> CapConfig:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT cross_spread_mult, cap_spread_mult, cap_abs_bps
                  FROM execution_caps
                 WHERE asset_class = $1 AND intent = $2 AND enabled = true
                """,
                asset_class, intent.value,
            )
        if row is None:
            raise RuntimeError(
                f"no enabled execution_caps row for ({asset_class}, {intent.value})"
            )
        return CapConfig(
            cross_spread_mult=row["cross_spread_mult"],
            cap_spread_mult=row["cap_spread_mult"],
            cap_abs_bps=row["cap_abs_bps"],
        )
    return get_cap


def make_size_position(pool: asyncpg.Pool):
    """DEPRECATED / not DB-backed. Sizing is a RISK POLICY that reads live venue
    equity + touch, not the DB. The real implementation is
    execution/sizing.make_size_position (5% of account equity per position),
    which the launcher wires into hooks.size_position over the adapter. This
    factory only populates the field; if it is ever left as-is it raises, so an
    un-wired sizing hook fails loudly rather than trading a placeholder size."""
    async def size_position(symbol: str, asset_class: str, side: TradeSide,
                            decision_price: Optional[float]) -> Decimal:
        raise NotImplementedError(
            "size_position not wired: the launcher must set hooks.size_position "
            "from execution/sizing.make_size_position (reads adapter equity+touch)."
        )
    return size_position


# ---------------------------------------------------------------------------
# Escalation ladder (EXIT driving)
# ---------------------------------------------------------------------------

def make_get_exit_cap(pool: asyncpg.Pool):
    """Load the EXIT ExitCap (base CapConfig + escalation ladder) for an asset
    class from execution_caps. The ladder is the ordered JSONB array of
    {widen_to_spread_mult, window_seconds} rungs; the final rung's implicit
    action is HALT (the executor's ExitCap.has_rung_beyond enforces that)."""
    async def get_exit_cap(asset_class: str) -> ExitCap:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT cross_spread_mult, cap_spread_mult, cap_abs_bps,
                       escalation_ladder
                  FROM execution_caps
                 WHERE asset_class = $1 AND intent = 'EXIT' AND enabled = true
                """,
                asset_class,
            )
        if row is None:
            raise RuntimeError(f"no enabled EXIT execution_caps row for {asset_class}")
        base = CapConfig(
            cross_spread_mult=row["cross_spread_mult"],
            cap_spread_mult=row["cap_spread_mult"],
            cap_abs_bps=row["cap_abs_bps"],
        )
        # asyncpg returns JSONB as TEXT unless a json codec is registered on
        # the connection (none is, here), so parse it. (Iterating the raw
        # string, as before, raised on every EXIT: no exit could be priced.)
        ladder_raw = row["escalation_ladder"] or []
        if isinstance(ladder_raw, str):
            ladder_raw = json.loads(ladder_raw)
        ladder = tuple(
            EscalationRung(
                widen_to_spread_mult=Decimal(str(r["widen_to_spread_mult"])),
                window_seconds=int(r["window_seconds"]),
            )
            for r in ladder_raw
        )
        return ExitCap(base=base, ladder=ladder)
    return get_exit_cap


def make_persist_exit_state(pool: asyncpg.Pool):
    """Persist the escalation rung + deadline for an in-flight protective exit,
    so resume-management can re-adopt it on restart. UPSERTs the 1:1 exit_state
    row (created when a trade first becomes OPEN). deadline_ms (epoch ms) maps to
    the timestamptz escalation_deadline; None clears it.

    exit_state.exit_rule_id is NOT NULL; a bare escalation update must not clobber
    or omit it, so we only INSERT a fresh row with a placeholder rule id if none
    exists yet, and otherwise UPDATE just the escalation columns. A trade that
    reached OPEN should already have its exit_state row written by the exit
    component; this keeps the escalation write from depending on that ordering."""
    async def persist_exit_state(trade_id: str, rung: int,
                                 deadline_ms: Optional[int]) -> None:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO exit_state
                    (trade_id, exit_rule_id, escalation_rung, escalation_deadline,
                     last_updated)
                VALUES ($1, 'escalation', $2,
                        CASE WHEN $3::bigint IS NULL THEN NULL
                             ELSE to_timestamp($3 / 1000.0) END,
                        now())
                ON CONFLICT (trade_id) DO UPDATE SET
                    escalation_rung     = EXCLUDED.escalation_rung,
                    escalation_deadline = EXCLUDED.escalation_deadline,
                    last_updated        = now()
                """,
                trade_id, rung, deadline_ms,
            )
    return persist_exit_state


# ---------------------------------------------------------------------------
# Global switches
# ---------------------------------------------------------------------------

def make_kill_switch_blocks(pool: asyncpg.Pool):
    """True if the global kill switch forbids opening new positions. Reads the
    singleton kill_switch row on EVERY call (no cache) — the switch must take
    effect faster than a redeploy, which means faster than any cache TTL.

    Engaged in EITHER mode blocks new entries. The policy is HALT-NEW-ONLY: when
    engaged we stop opening but leave open positions to their normal protective
    exits (the exit path never consults this). The 'flatten' mode (auto-close
    everything) is NOT implemented; if it is ever set in the DB we still block new
    entries (correct) but log a warning, so no operator relies on an auto-flatten
    that doesn't exist."""
    async def kill_switch_blocks() -> bool:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT engaged, mode FROM kill_switch WHERE id = true"
            )
        # Missing row => treat as blocked (fail safe): if we can't confirm the
        # switch is off, don't open new risk.
        if row is None:
            log.warning("kill_switch row missing — failing safe (blocking new entries)")
            return True
        engaged = bool(row["engaged"])
        if engaged and row["mode"] == "flatten":
            log.warning("kill_switch mode='flatten' is NOT implemented — behaving as "
                        "'halt_new' (blocking new entries; open positions run their "
                        "normal exits). Do not rely on auto-flatten.")
        return engaged
    return kill_switch_blocks


def make_set_kill_switch(pool: asyncpg.Pool):
    """Engage or disengage the global kill switch, with an audit trail.

    This is the operator-facing control (wire it to an admin endpoint / CLI). It
    is the sanctioned alternative to hand-editing the row in SQL: it records who
    engaged it, why, and when, which matters for a post-incident review of an
    emergency halt. Idempotent — setting the same state twice just refreshes the
    audit fields.

    mode is accepted for forward-compatibility but only 'halt_new' is honored by
    the executor; passing 'flatten' is rejected here rather than silently stored,
    so the DB can't hold a mode the system won't act on."""
    async def set_kill_switch(engaged: bool, *, reason: Optional[str] = None,
                              engaged_by: Optional[str] = None,
                              mode: str = "halt_new") -> None:
        if mode != "halt_new":
            raise ValueError(
                f"unsupported kill_switch mode {mode!r}: only 'halt_new' is "
                f"implemented (flatten/auto-exit is deliberately not built)")
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE kill_switch
                   SET engaged     = $1,
                       mode        = $2,
                       reason      = $3,
                       engaged_by  = $4,
                       engaged_at  = CASE WHEN $1 THEN now() ELSE NULL END,
                       updated_at  = now()
                 WHERE id = true
                """,
                engaged, mode, reason, engaged_by,
            )
        log.warning("kill_switch %s by %s (reason=%s)",
                    "ENGAGED" if engaged else "DISENGAGED",
                    engaged_by or "unknown", reason or "-")
    return set_kill_switch


def make_kill_switch_status(pool: asyncpg.Pool):
    """Read the full kill-switch row for operators/health (engaged, mode, reason,
    who, when). Returns None only if the singleton row is missing."""
    async def kill_switch_status() -> Optional[dict]:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT engaged, mode, reason, engaged_by, engaged_at, updated_at "
                "FROM kill_switch WHERE id = true"
            )
        return dict(row) if row is not None else None
    return kill_switch_status


# ---------------------------------------------------------------------------
# Shadow book (migration 019) — virtual fills for signals the venue can't take
# ---------------------------------------------------------------------------

def make_shadow_hooks(pool: asyncpg.Pool, get_touch):
    """ShadowHooks over shadow_trades. get_touch is the live-adapter touch
    closure the launcher already builds (not DB-backed)."""
    from execution.shadow_book import ShadowHooks, ShadowPosition

    async def insert_entry(row: dict) -> bool:
        async with pool.acquire() as conn:
            res = await conn.execute(
                """
                INSERT INTO shadow_trades
                    (strategy, position_key, symbol, asset_class, side,
                     entry_signal_id, entry_px, entry_at, fee_bps)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT DO NOTHING
                """,
                row["strategy"], row["position_key"], row["symbol"],
                row["asset_class"], row["side"], row["entry_signal_id"],
                row["entry_px"], row["entry_at"], row["fee_bps"],
            )
        return res.endswith(" 1")

    async def close(row: dict) -> bool:
        async with pool.acquire() as conn:
            res = await conn.execute(
                """
                UPDATE shadow_trades
                   SET exit_signal_id = $3, exit_px = $4, exit_at = $5,
                       exit_reason = $6, pnl_bps = $7
                 WHERE strategy = $1 AND position_key = $2 AND exit_at IS NULL
                """,
                row["strategy"], row["position_key"], row["exit_signal_id"],
                row["exit_px"], row["exit_at"], row["exit_reason"], row["pnl_bps"],
            )
        return res.endswith(" 1")

    async def load_open() -> list:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT strategy, position_key, symbol, asset_class, side,
                       entry_signal_id, entry_px, fee_bps
                  FROM shadow_trades
                 WHERE exit_at IS NULL
                """
            )
        return [
            ShadowPosition(
                strategy=r["strategy"], position_key=r["position_key"],
                symbol=r["symbol"], asset_class=r["asset_class"],
                side=TradeSide(r["side"]), entry_signal_id=r["entry_signal_id"],
                entry_px=r["entry_px"], fee_bps=r["fee_bps"],
            )
            for r in rows
        ]

    return ShadowHooks(get_touch=get_touch, insert_entry=insert_entry,
                       close=close, load_open=load_open)


# ---------------------------------------------------------------------------
# Listener cursor (migration 020)
# ---------------------------------------------------------------------------

def make_load_executor_cursor(pool: asyncpg.Pool):
    """The persisted listener cursor, or None if the executor never ran."""
    async def load_executor_cursor():
        async with pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT cursor_at FROM executor_cursor WHERE id = true")
    return load_executor_cursor


def make_save_executor_cursor(pool: asyncpg.Pool):
    """Upsert the listener cursor. Monotonic (GREATEST): it can never move
    backwards, even if two writes land out of order."""
    async def save_executor_cursor(cursor_at) -> None:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO executor_cursor (id, cursor_at, updated_at)
                VALUES (true, $1, now())
                ON CONFLICT (id) DO UPDATE SET
                    cursor_at  = GREATEST(executor_cursor.cursor_at, EXCLUDED.cursor_at),
                    updated_at = now()
                """,
                cursor_at,
            )
    return save_executor_cursor


# ---------------------------------------------------------------------------
# Alerts (migration 021)
# ---------------------------------------------------------------------------

def make_alert(pool: asyncpg.Pool):
    """(kind, detail) -> insert an executor_alerts row; its trigger fires
    pg_notify('executor_alert'). The executor calls this best-effort."""
    async def alert(kind: str, detail: dict) -> None:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO executor_alerts (kind, trade_id, symbol, detail)
                VALUES ($1, $2, $3, $4::jsonb)
                """,
                kind, detail.get("trade_id"), detail.get("symbol"),
                json.dumps(detail, default=str),
            )
    return alert


# ---------------------------------------------------------------------------
# Paper ledger (migration 022) — the simulated account behind paper mode
# ---------------------------------------------------------------------------

def make_paper_ledger_hooks(pool: asyncpg.Pool):
    """PaperLedgerHooks over paper_balances / paper_orders / paper_fills. A
    fill's writes (fill row, order, balance deltas) run in one transaction."""
    from datetime import datetime, timezone
    from execution.paper_adapter import PaperLedgerHooks, PaperOrder, PaperState
    from execution.types import Fill, OrderIntent

    def _dt(ms: int):
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

    async def load(fills_since_ms: int) -> PaperState:
        async with pool.acquire() as conn:
            bals = await conn.fetch("SELECT currency, qty FROM paper_balances")
            orders = await conn.fetch(
                """SELECT client_order_id, venue_order_id, symbol, side, intent, qty,
                          limit_price, status, filled_qty,
                          (EXTRACT(EPOCH FROM created_at) * 1000)::bigint AS created_ms
                     FROM paper_orders
                    WHERE status = 'OPEN' OR created_at >= $1""",
                _dt(fills_since_ms))
            fills = await conn.fetch(
                """SELECT fill_id, client_order_id, venue_order_id, intent, qty, price,
                          fee, fee_currency,
                          (EXTRACT(EPOCH FROM filled_at) * 1000)::bigint AS filled_ms
                     FROM paper_fills WHERE filled_at >= $1 ORDER BY filled_at""",
                _dt(fills_since_ms))
        return PaperState(
            balances={r["currency"]: r["qty"] for r in bals},
            orders={r["client_order_id"]: PaperOrder(
                client_order_id=r["client_order_id"], venue_order_id=r["venue_order_id"],
                symbol=r["symbol"], side=r["side"], intent=r["intent"], qty=r["qty"],
                limit_price=r["limit_price"], status=r["status"],
                filled_qty=r["filled_qty"], created_ms=r["created_ms"]) for r in orders},
            fills=[Fill(venue_exec_id=r["fill_id"], qty=r["qty"], price=r["price"],
                        intent=OrderIntent(r["intent"]), filled_at_ms=r["filled_ms"],
                        fee=r["fee"], fee_currency=r["fee_currency"],
                        venue_order_id=r["venue_order_id"],
                        client_order_id=r["client_order_id"]) for r in fills],
        )

    async def seed(balances) -> None:
        async with pool.acquire() as conn:
            async with conn.transaction():
                if await conn.fetchval("SELECT count(*) FROM paper_balances"):
                    return
                for cur, qty in balances.items():
                    await conn.execute(
                        "INSERT INTO paper_balances (currency, qty) VALUES ($1, $2)", cur, qty)

    async def _upsert_order(conn, o) -> None:
        await conn.execute(
            """INSERT INTO paper_orders
                   (client_order_id, venue_order_id, symbol, side, intent, qty,
                    limit_price, status, filled_qty)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
               ON CONFLICT (client_order_id) DO UPDATE SET
                   limit_price = EXCLUDED.limit_price,
                   status      = EXCLUDED.status,
                   filled_qty  = EXCLUDED.filled_qty,
                   updated_at  = now()""",
            o.client_order_id, o.venue_order_id, o.symbol, o.side, o.intent, o.qty,
            o.limit_price, o.status, o.filled_qty)

    async def save_order(o) -> None:
        async with pool.acquire() as conn:
            await _upsert_order(conn, o)

    async def apply_fill(fill, symbol: str, o, deltas) -> None:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await _upsert_order(conn, o)
                await conn.execute(
                    """INSERT INTO paper_fills
                           (fill_id, client_order_id, venue_order_id, symbol, intent, qty,
                            price, fee, fee_currency, filled_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)""",
                    fill.venue_exec_id, fill.client_order_id, fill.venue_order_id, symbol,
                    fill.intent.value, fill.qty, fill.price, fill.fee, fill.fee_currency,
                    _dt(fill.filled_at_ms))
                for cur, d in deltas.items():
                    await conn.execute(
                        """INSERT INTO paper_balances (currency, qty) VALUES ($1, $2)
                           ON CONFLICT (currency) DO UPDATE SET
                               qty = paper_balances.qty + EXCLUDED.qty, updated_at = now()""",
                        cur, d)

    return PaperLedgerHooks(load=load, seed=seed, save_order=save_order,
                            apply_fill=apply_fill)


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------

def build_executor_hooks(pool: asyncpg.Pool) -> ExecutorHooks:
    """Assemble the full ExecutorHooks from the factories above. This is what
    run_executor.build_executor_hooks should return.

    Two members are intentionally NOT DB-backed and will fail loudly until wired
    to the venue adapter / risk policy: get_touch (needs the live book, not the
    DB) and size_position (needs account equity + risk config). See "LEFT TO DO".
    now_ms is left unset — ExecutorHooks defaults it to the wall clock.
    """
    return ExecutorHooks(
        persist_trade=make_persist_trade(pool),
        update_trade_state=make_update_trade_state(pool),
        persist_order=make_persist_order(pool),
        persist_fill=make_persist_fill(pool),
        advance_order_fill=make_advance_order_fill(pool),
        load_open_trades=make_load_open_trades(pool),
        get_touch=make_get_touch(pool),          # <- rewire to adapter (see below)
        get_cap=make_get_cap(pool),
        size_position=make_size_position(pool),  # <- implement sizing policy
        get_exit_cap=make_get_exit_cap(pool),
        persist_exit_state=make_persist_exit_state(pool),
        kill_switch_blocks=make_kill_switch_blocks(pool),
        alert=make_alert(pool),
    )


# ==========================================================================
# LEFT TO DO (status after the adapter get_touch/equity + sizing work):
#
# 1. get_touch  — RESOLVED. Added to the adapter ABC and implemented on both
#    venues (IBKRAdapter via reqTickers; BinanceAdapter via bookTicker). The
#    launcher wires hooks.get_touch to the live adapter, so this module's
#    make_get_touch no-op is never used at runtime.
#
# 2. size_position — RESOLVED. Implemented in execution/sizing.py (5% of account
#    equity per position), wired by the launcher over the adapter's
#    get_account_equity + get_touch, and floored to the venue step via
#    adapter.get_step_size (IB contract details / Binance LOT_SIZE). No separate
#    step map to maintain.
#
# 3. trades.strategy — RESOLVED by migration 014 (apply it). persist_trade now
#    writes t.strategy; load_open_trades reads the column. Nothing further needed
#    beyond applying the migration.
#
# 4. persist_fill order mapping — assumes orders.venue_order_id is populated on
#    ack (it is, per the adapter contract). Fills that arrive before their order
#    row's venue_order_id is written will be skipped and re-swept on reconcile;
#    confirm the adapter writes venue_order_id promptly on ack for your venue.
# ==========================================================================