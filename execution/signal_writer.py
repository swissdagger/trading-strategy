"""
execution/signal_writer.py — Turns entry/exit DECISIONS into trade-signal rows.

This is the bridge that makes the entry/exit LOGIC into the trading-signal
PRODUCT. EntryPolicy/ExitRule decide; this writes those decisions to the
trade_signals table (004) as discrete, deterministic, idempotent trade-intent
rows. A separate team's script consumes them now; this project's own position
manager will consume the same rows when switched on.

POSITION IDENTITY. The strategy holds multiple concurrent positions per symbol,
so a signal names a POSITION, not a symbol. position_key is derived from
(chain_id, direction): an ENTER defines it, an EXIT references it. Both consumers
track open positions by this key.

DETERMINISTIC, IDEMPOTENT. signal_id is reproducible from the decision, so a
replayed emit (our restart) or a re-read (their poll) dedupes. Same discipline
as the client-order-id keystone, applied to the cross-team signal contract. The
table PK on signal_id is the backstop; the deterministic id is what makes the
"already emitted?" check reliable in the first place.

SEPARATION. This writer has NOTHING to do with position management — no fills,
no sizing, no execution. It records what the strategy decided. It can run with
the position manager switched entirely OFF (the current world: emit signals,
someone else executes), or alongside it later (emit signals AND manage our own
positions from the same decisions).
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from execution.types import ExitReason, TradeSide

log = logging.getLogger("executor.signals")


def position_key(chain_id: str, side: TradeSide) -> str:
    """Stable, unique identity for a position: (chain_id, direction). The chain
    is directional once initiated, so (chain_id, side) names exactly one
    position. ENTER defines it; EXIT references it. Same basis as the trade key
    in the position-management schema, so the two systems agree on what a
    position IS."""
    return f"{chain_id}:{'L' if side == TradeSide.LONG else 'S'}"


def _signal_id(strategy: str, position_key_: str, action: str, discriminator: str) -> str:
    """Deterministic signal id. Same logical decision -> same id.

    Includes `strategy`: two strategies can decide on the same chain (same
    position_key + action), and their signals MUST be distinct rows, not
    deduped into one.

    SCHEME v2 (sig2): the discriminator now carries `decided_at` (the deciding
    event's timestamp) for BOTH actions — decided_at alone for ENTER,
    "{reason}|{decided_at}" for EXIT. Why:

      * The v1 discriminator was empty for ENTER ("one per position"), but the
        strategies genuinely permit ENTER -> EXIT -> re-ENTER cycles on the
        same (chain_id, side): two_ladder can re-trigger on a still-live prime
        after a STOP's opposing chain terminates, and four_chain re-triggers on
        every propagation >= 3m of the same conf chain. Under v1 the second
        ENTER hashed to the SAME id and was silently swallowed by ON CONFLICT
        DO NOTHING — no row, no NOTIFY, executor never re-entered — while the
        runtime re-tracked the position in memory. (load_open_positions
        explicitly anticipates re-entry; v1 made that history unrepresentable.)

      * decided_at is the EVENT's timestamp (derived from the bar), not wall
        clock — so a Kafka replay of the same decision reproduces the same
        decided_at and the same id, preserving replay idempotency exactly.
        A genuine re-entry decided on a LATER event gets a distinct id.

      * Same reasoning for EXIT: enter/exit/re-enter/exit-again with the same
        reason must yield two EXIT rows, not one.

    DEPLOY NOTE: rows written under v1 keep their sig1.* ids. A post-deploy
    replay of a PRE-deploy decision generates a fresh sig2.* id and therefore a
    duplicate row for that one decision — deploy at a committed-offset boundary
    (or accept the bounded one-time duplicate window; consumers dedupe by
    signal_id, so a duplicate row is acted on at most once more, idempotently,
    by the executor's DB/venue-level dedupe).
    """
    raw = f"sig2|{strategy}|{position_key_}|{action}|{discriminator}"
    return "sig2." + hashlib.sha256(raw.encode()).hexdigest()[:32]


@dataclass
class SignalWriterHooks:
    # Insert a trade_signals row. MUST be idempotent on signal_id (the table PK
    # does this via ON CONFLICT DO NOTHING at the writer) so a replayed emit is a
    # no-op, never a duplicate signal the consumer might double-act on.
    insert_signal: Callable[[dict], Awaitable[bool]]   # returns True if newly inserted


class SignalWriter:
    """Persists entry/exit decisions as trade_signals rows.

    Carries the STRATEGY name that produced these decisions, folded into each
    signal_id (so two strategies don't collide on the PK) and written to the
    trade_signals.strategy column (migration 011). Defaults to 'two_ladder' so a
    single-strategy deployment and the pre-multi-strategy history are unchanged.
    In the multi-strategy runtime, each strategy gets its own SignalWriter bound
    to its name.
    """

    def __init__(self, hooks: SignalWriterHooks, strategy: str = "two_ladder"):
        self.hooks = hooks
        self.strategy = strategy

    async def emit_entry(
        self,
        *,
        chain_id: str,
        side: TradeSide,
        symbol: str,
        asset_class: str,
        decided_at: str,            # ISO timestamp from the triggering signal
        decision_price: Optional[float] = None,
        trigger_level: Optional[int] = None,
    ) -> str:
        """Emit an ENTER signal: announces a new position and defines its key.
        Returns the position_key the consumer will track this position by (and
        which a later exit must reference)."""
        pkey = position_key(chain_id, side)
        # decided_at in the discriminator: distinguishes a genuine re-ENTER of
        # the same position (later event, new id) from a replay of THIS decision
        # (same event timestamp, same id -> dedupes). See _signal_id.
        sig_id = _signal_id(self.strategy, pkey, "ENTER", decided_at)
        row = {
            "signal_id": sig_id,
            "position_key": pkey,
            "chain_id": chain_id,
            "direction": "LONG" if side == TradeSide.LONG else "SHORT",
            "action": "ENTER",
            "symbol": symbol,
            "asset_class": asset_class,
            "strategy": self.strategy,
            "trigger_level": trigger_level,
            "decision_price": decision_price,
            "exit_reason": None,
            "decided_at": decided_at,
        }
        newly = await self.hooks.insert_signal(row)
        if newly:
            log.info("ENTER signal %s position=%s %s @level=%s",
                     sig_id, pkey, symbol, trigger_level)
        else:
            log.debug("ENTER signal %s already emitted (idempotent skip)", sig_id)
        return pkey

    async def emit_exit(
        self,
        *,
        chain_id: str,
        side: TradeSide,
        symbol: str,
        asset_class: str,
        reason: ExitReason,
        decided_at: str,
        decision_price: Optional[float] = None,
        trigger_level: Optional[int] = None,
    ) -> str:
        """Emit an EXIT signal: references an existing position by key and asks
        to close it. The consumer matches position_key to the open position it
        tracks; if it has no such open position, it logs a reconciliation gap
        (the table carries enough to detect it — see 004's note). Returns the
        position_key."""
        pkey = position_key(chain_id, side)
        # reason AND decided_at: a re-entered position exiting for the same
        # reason as its previous life must produce a new row. See _signal_id.
        sig_id = _signal_id(self.strategy, pkey, "EXIT", f"{reason.value}|{decided_at}")
        row = {
            "signal_id": sig_id,
            "position_key": pkey,
            "chain_id": chain_id,
            "direction": "LONG" if side == TradeSide.LONG else "SHORT",
            "action": "EXIT",
            "symbol": symbol,
            "asset_class": asset_class,
            "strategy": self.strategy,
            "trigger_level": trigger_level,
            "decision_price": decision_price,
            "exit_reason": reason.value,
            "decided_at": decided_at,
        }
        newly = await self.hooks.insert_signal(row)
        if newly:
            log.info("EXIT signal %s position=%s %s reason=%s",
                     sig_id, pkey, symbol, reason.value)
        else:
            log.debug("EXIT signal %s already emitted (idempotent skip)", sig_id)
        return pkey