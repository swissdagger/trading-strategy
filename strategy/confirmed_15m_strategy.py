"""
strategy/confirmed_15m_strategy.py — the confirmed_15m entry/exit rules.

Spec: docs/strategies/confirmed_15m.md (the rules as implemented here). The
strategy was called four_chain until ADR-001 Phase 2; its current two-chain
rules date from 2026-08-13, when it replaced the earlier four-chain pattern.
"""
from __future__ import annotations

import logging
from collections import Counter
from typing import Optional

from execution.signal_writer import SignalWriter
from execution.types import ExitReason, TradeSide
from strategy.chain_history import ChainHistoryView, ChainLifecycle
from strategy.ladder_registry import Ladder

log = logging.getLogger("strategy.confirmed_15m")

_FUNNEL_SUMMARY_EVERY = 25


def _side(direction: int) -> TradeSide:
    return TradeSide.LONG if direction == 1 else TradeSide.SHORT


class Confirmed15mEntry:
    """Entry: a chain propagating to 15m (Chain 2), preceded by an
    initiation pattern (Chain 1 + an opposing failure between them) and a
    closed same-direction chain that reached 1h. See the spec for details."""

    def __init__(self, view: ChainHistoryView, ladder: Ladder, writer: SignalWriter):
        self.view = view
        self.ladder = ladder
        self.writer = writer
        self._L_15M = ladder.level_of("15m")
        self._L_1H = ladder.level_of("1h")
        self._funnel: Counter[str] = Counter()

    async def on_event(
        self, *, symbol: str, asset_class: str, chain_id: str, event_type: str,
        direction: int, propagation_level: int, timestamp: str,
        decision_price: Optional[float] = None,
    ) -> Optional[str]:
        
        # --- ENTRY TRIGGER ---
        # Chain 2 reaching 15m is a chain_propagated event at the 15m level
        # (propagation ascends one rung at a time, so this fires once per
        # chain). The event-type check is load-bearing: chain_terminated also
        # carries a level — the rung it died on — and can be 15m.
        if propagation_level != self._L_15M or event_type != "chain_propagated":
            return None

        self._funnel["trigger"] += 1
        d = direction
        chain_2 = self.view.chain(symbol, chain_id)
        
        if chain_2 is None:
            return self._veto(symbol, "chain2_not_folded")

        # --- INITIATION PHASE ---
        chain_1 = self._find_chain_1(symbol, d, chain_2)
        if chain_1 is None:
            return self._veto(symbol, "no_chain_1_pattern")
            
        self._funnel["init_pattern_ok"] += 1

        # --- CONFIRMATION PHASE ---
        active_ids = {active.chain_id for active in self.view.active_chains(symbol, direction=d)}
        has_full_prop_closed = False
        
        for r in self.view.chains(symbol):
            if r.direction == d and r.chain_id != chain_id:
                is_closed = r.chain_id not in active_ids
                is_preceding = r.first_seen < chain_2.first_seen
                if is_closed and is_preceding and r.reached_level(self._L_1H):
                    has_full_prop_closed = True
                    break

        if not has_full_prop_closed:
            return self._veto(symbol, "no_closed_full_prop")

        # All gates passed -> IMMEDIATELY ENTER.
        self._funnel["entered"] += 1
        
        pkey = await self.writer.emit_entry(
            chain_id=chain_id,  # This is Chain 2
            side=_side(d),
            symbol=symbol,
            asset_class=asset_class,
            decided_at=timestamp,
            decision_price=decision_price,
            trigger_level=propagation_level,
        )
        
        log.info(
            "confirmed_15m ENTER %s dir=%d chain_2=%s chain_1=%s",
            symbol, d, chain_id, chain_1.chain_id,
        )
        return pkey

    def _find_chain_1(
        self, symbol: str, direction: int, chain_2: ChainLifecycle,
    ) -> Optional[ChainLifecycle]:
        """Finds Chain 1 and verifies the opposing failure between them."""
        chain_2_15m_ts = chain_2.reached_at_ts(self._L_15M)
        
        cands = self.view.initiators_by_start(
            symbol, direction=direction, exclude_chain_id=chain_2.chain_id,
            before_start=chain_2.first_seen,
        )

        for chain_1 in reversed(cands):  
            chain_1_15m_ts = chain_1.reached_at_ts(self._L_15M)
            
            if chain_1_15m_ts is None:
                continue
                
            if chain_2_15m_ts < chain_1_15m_ts:
                continue
            
            fails = self.view.failed_between(
                symbol, direction=direction,
                after=chain_1.first_seen, before=chain_2.first_seen,
            )
            
            if fails: 
                return chain_1
                
        return None

    def _veto(self, symbol: str, stage: str) -> None:
        self._funnel[f"veto:{stage}"] += 1
        log.debug("confirmed_15m VETO %s at %s", symbol, stage)
        triggers = self._funnel["trigger"]
        if triggers % _FUNNEL_SUMMARY_EVERY == 0:
            vetoes = {
                k.split(":", 1)[1]: v
                for k, v in sorted(self._funnel.items())
                if k.startswith("veto:")
            }
            log.info(
                "confirmed_15m FUNNEL: triggers=%d init_ok=%d entered=%d vetoes=%s",
                triggers, self._funnel["init_pattern_ok"],
                self._funnel["entered"], vetoes,
            )
        return None


class Confirmed15mExit:
    """Exit for a position on Chain 2: TARGET when Chain 2 reaches 1h, STOP
    when an open opposing chain reaches 5m."""

    def __init__(self, view: ChainHistoryView, ladder: Ladder, writer: SignalWriter):
        self.view = view
        self.ladder = ladder
        self.writer = writer
        self._L_5M = ladder.level_of("5m")
        self._L_1H = ladder.level_of("1h")

    def evaluate(
        self, *, symbol: str, chain_prime_id: str, trade_direction: int,
    ) -> Optional[ExitReason]:
        
        chain_2 = self.view.chain(symbol, chain_prime_id)
        if chain_2 is not None and chain_2.reached_level(self._L_1H):
            return ExitReason.TARGET

        opposing = -trade_direction
        for r in self.view.active_chains(symbol, direction=opposing):
            if r.reached_level(self._L_5M):
                return ExitReason.STOP

        return None

    async def maybe_emit_exit(
        self, *, symbol: str, asset_class: str, chain_prime_id: str,
        trade_direction: int, decided_at: str,
        decision_price: Optional[float] = None,
    ) -> Optional[str]:
        
        reason = self.evaluate(
            symbol=symbol, chain_prime_id=chain_prime_id, 
            trade_direction=trade_direction,
        )
        if reason is None:
            return None
            
        pkey = await self.writer.emit_exit(
            chain_id=chain_prime_id,
            side=_side(trade_direction),
            symbol=symbol,
            asset_class=asset_class,
            reason=reason,
            decided_at=decided_at,
            decision_price=decision_price,
        )
        log.info("confirmed_15m EXIT %s chain_2=%s reason=%s", symbol, chain_prime_id, reason.value)
        return pkey