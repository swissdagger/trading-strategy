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
OFF builds the real adapter wrapped READ-ONLY (no write can reach the venue).
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

      LIVE  -> CoinbaseAdapter (key must be able to trade).
      OFF   -> the real CoinbaseAdapter for READS (connect, reconcile, balances,
               touches, fills) wrapped in ReadOnlyAdapter, so no write can reach
               the venue whatever the mode gate does. The key need not trade.
      PAPER -> PaperCoinbaseAdapter, ADR-001 Phase 5. Until then this raises
               NotImplementedError and the executor refuses to start in paper.

    venue_cfg is config.VENUE_ENDPOINTS["coinbase"]: api_key_name,
    api_private_key, portfolio_uuid, base_url, quote_currency.
    """
    kind = endpoint_kind_for_mode(mode)
    if mode == TradeMode.PAPER:
        raise NotImplementedError(
            "Coinbase paper adapter (PaperCoinbaseAdapter) lands in ADR-001 Phase 5. "
            "Run in 'off' (read-only against the real portfolio) or 'live'.")
    from execution.coinbase_adapter import CoinbaseAdapter
    from execution.coinbase_client import DEFAULT_BASE_URL, CoinbaseClient
    from execution.read_only_adapter import ReadOnlyAdapter

    client = CoinbaseClient(
        venue_cfg.get("api_key_name", ""), venue_cfg.get("api_private_key", ""),
        base_url=venue_cfg.get("base_url") or DEFAULT_BASE_URL,
    )
    adapter = CoinbaseAdapter(
        client,
        portfolio_uuid=venue_cfg.get("portfolio_uuid", ""),
        quote_currency=venue_cfg.get("quote_currency", "USD"),
        require_trade=(mode == TradeMode.LIVE),
    )
    if mode == TradeMode.OFF:
        log.info("built Coinbase adapter READ-ONLY for mode=off")
        return ReadOnlyAdapter(adapter), kind
    log.warning("built Coinbase adapter LIVE (real orders) for mode=%s", mode.value)
    return adapter, kind
