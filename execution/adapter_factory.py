"""
execution/adapter_factory.py — build the venue adapter for a given trade mode.

The mode selects which adapter to construct and where it connects. This factory
is that selection, kept in one place so the mode->endpoint mapping is explicit
and auditable (getting it wrong means real orders when you meant paper).

## Endpoint kinds

Every mode maps to an endpoint KIND:
  TradeMode.PAPER -> 'paper' (fake money)
  TradeMode.LIVE  -> 'live'  (real money)
  TradeMode.OFF   -> 'paper' — never build LIVE for OFF. Even if a stray order
                     slipped through the GatedAdapter's OFF gate, it would hit
                     paper, never live.

ADR-001: the only venue is Coinbase Advanced Trade spot. Coinbase has no paper
venue endpoint (its sandbox returns canned responses and has no matching
engine), so 'paper' will mean the in-process PaperCoinbaseAdapter wrapping the
live adapter's READS (Phase 5), and 'live' the real CoinbaseAdapter (Phase 4).
The kind mapping below is unchanged by that; only what each kind builds is.

## Reconnect on mode change

When TradeEnableState fires on_mode_change(old, new) and the endpoint kind
differs between old and new (e.g. paper->live), the app rebuilds the adapter via
this factory for the new mode and reconnects. off<->paper doesn't change the
kind (both paper) so no rebuild is needed; anything involving live does.
"""
from __future__ import annotations

import logging

from execution.trade_enable import TradeMode

log = logging.getLogger("executor.adapter_factory")


def endpoint_kind_for_mode(mode: TradeMode) -> str:
    """The endpoint kind ('paper'|'live') a mode maps to, WITHOUT building an
    adapter — so the reconnect logic can decide whether a mode change actually
    changes the venue (and thus needs a rebuild) cheaply.

    off -> paper, paper -> paper, live -> live. So off<->paper is a no-venue-
    change transition; anything to/from live changes the venue.
    """
    return "live" if mode == TradeMode.LIVE else "paper"


def build_coinbase_adapter(venue_cfg: dict, mode: TradeMode):
    """Build the Coinbase adapter for `mode`. Returns (adapter, endpoint_kind).

    Not implemented until ADR-001 Phase 4 (CoinbaseAdapter) / Phase 5
    (PaperCoinbaseAdapter). Raises NotImplementedError so run_executor refuses
    to start with a clear message rather than trading through a missing venue.
    """
    kind = endpoint_kind_for_mode(mode)
    raise NotImplementedError(
        f"Coinbase adapter (mode={mode.value}, endpoint={kind}) is not built yet — "
        "it lands in ADR-001 Phase 4 (live) / Phase 5 (paper). The executor cannot "
        "start until then; the signal side (warmup/feed/signal) is unaffected."
    )
