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


def _signal_id(position_key_: str, action: str, discriminator: str) -> str:
    """Deterministic signal id. Same logical decision -> same id.

    discriminator distinguishes signals that share (position_key, action) but are
    genuinely different decisions — for an ENTER there is only ever one per
    position so the discriminator is empty; for an EXIT it is the exit reason (a
    position is exited once, but carrying the reason keeps the id meaningful and
    guards against a future where re-exit semantics change). Hashed to a compact,
    table-safe id; the inputs are all reproducible so the output is too.
    """
    raw = f"sig1|{position_key_}|{action}|{discriminator}"
    return "sig1." + hashlib.sha256(raw.encode()).hexdigest()[:32]


@dataclass
class SignalWriterHooks:
    # Insert a trade_signals row. MUST be idempotent on signal_id (the table PK
    # does this via ON CONFLICT DO NOTHING at the writer) so a replayed emit is a
    # no-op, never a duplicate signal the consumer might double-act on.
    insert_signal: Callable[[dict], Awaitable[bool]]   # returns True if newly inserted


class SignalWriter:
    """Persists entry/exit decisions as trade_signals rows."""

    def __init__(self, hooks: SignalWriterHooks):
        self.hooks = hooks

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
        sig_id = _signal_id(pkey, "ENTER", "")
        row = {
            "signal_id": sig_id,
            "position_key": pkey,
            "chain_id": chain_id,
            "direction": "LONG" if side == TradeSide.LONG else "SHORT",
            "action": "ENTER",
            "symbol": symbol,
            "asset_class": asset_class,
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
        sig_id = _signal_id(pkey, "EXIT", reason.value)
        row = {
            "signal_id": sig_id,
            "position_key": pkey,
            "chain_id": chain_id,
            "direction": "LONG" if side == TradeSide.LONG else "SHORT",
            "action": "EXIT",
            "symbol": symbol,
            "asset_class": asset_class,
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
