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

    Now includes `strategy`: two strategies can decide on the same chain (same
    position_key + action), and their signals MUST be distinct rows, not
    deduped into one. Folding strategy into the hash makes the id distinct per
    strategy while staying deterministic (same strategy + decision -> same id,
    so replays still dedupe within a strategy). Pre-multi-strategy ids used
    strategy 'two_ladder'; that value reproduces the original ids for that
    strategy, so its historical dedupe behaviour is unchanged.

    discriminator distinguishes signals sharing (strategy, position_key, action)
    — empty for ENTER (one per position), the exit reason for EXIT. (Historical
    SCALE rows used "SCALE:k", one per tranche; nothing emits them any more.)
    """
    raw = f"sig1|{strategy}|{position_key_}|{action}|{discriminator}"
    return "sig1." + hashlib.sha256(raw.encode()).hexdigest()[:32]


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
    trade_signals.strategy column (migration 011). Required: each strategy gets
    its own SignalWriter bound to its name.
    """

    def __init__(self, hooks: SignalWriterHooks, strategy: str):
        if not strategy:
            raise ValueError("SignalWriter needs the strategy name")
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
        decided_bar_close: Optional[str] = None,   # ISO; the deciding bar's close
    ) -> str:
        """Emit an ENTER signal: announces a new position and defines its key.
        Returns the position_key the consumer will track this position by (and
        which a later exit must reference)."""
        pkey = position_key(chain_id, side)
        sig_id = _signal_id(self.strategy, pkey, "ENTER", "")
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
            # Every signal concerns the WHOLE position (migration 016's
            # column; a CHECK enforces it for ENTER). Carried explicitly so the
            # row shape is uniform across actions for the insert path.
            "fraction": 1.0,
            "decided_at": decided_at,
            "decided_bar_close": decided_bar_close,
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
        decided_bar_close: Optional[str] = None,
    ) -> str:
        """Emit an EXIT signal: references an existing position by key and asks
        to close the WHOLE of it. The consumer matches position_key to the open
        position it tracks; if it has no such open position, it logs a
        reconciliation gap (the table carries enough to detect it — see 004's
        note). Returns the position_key.

        SCALE (a partial-exit tranche) is no longer supported and is refused.
        """
        if reason == ExitReason.SCALE:
            raise ValueError("SCALE (partial) exits are no longer supported")
        pkey = position_key(chain_id, side)
        sig_id = _signal_id(self.strategy, pkey, "EXIT", reason.value)
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
            "fraction": 1.0,
            "decided_at": decided_at,
            "decided_bar_close": decided_bar_close,
        }
        newly = await self.hooks.insert_signal(row)
        if newly:
            log.info("EXIT signal %s position=%s %s reason=%s",
                     sig_id, pkey, symbol, reason.value)
        else:
            log.debug("EXIT signal %s already emitted (idempotent skip)", sig_id)
        return pkey