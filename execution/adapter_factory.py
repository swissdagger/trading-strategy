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
engine), so 'paper' is the in-process PaperCoinbaseAdapter over the real
adapter's READS (wrapped read-only), and 'live' is the real CoinbaseAdapter.
OFF builds the paper adapter too, so a stray order in OFF could only ever be a
simulated one.

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


def build_coinbase_market(venue_cfg: dict, *, require_trade: bool):
    """The real CoinbaseAdapter from config.VENUE_ENDPOINTS["coinbase"]
    (api_key_name, api_private_key, portfolio_uuid, base_url, quote_currency)."""
    from execution.coinbase_adapter import CoinbaseAdapter
    from execution.coinbase_client import DEFAULT_BASE_URL, CoinbaseClient

    client = CoinbaseClient(
        venue_cfg.get("api_key_name", ""), venue_cfg.get("api_private_key", ""),
        base_url=venue_cfg.get("base_url") or DEFAULT_BASE_URL,
    )
    return CoinbaseAdapter(
        client,
        portfolio_uuid=venue_cfg.get("portfolio_uuid", ""),
        quote_currency=venue_cfg.get("quote_currency", "USD"),
        require_trade=require_trade,
    )


def build_coinbase_adapter(venue_cfg: dict, mode: TradeMode, *, paper_ledger=None,
                           paper_starting_cash=None, fallback_fee_bps=None):
    """Build the Coinbase adapter for `mode`. Returns (adapter, endpoint_kind).

      LIVE        -> CoinbaseAdapter: real orders (the key must be able to trade).
      OFF, PAPER  -> PaperCoinbaseAdapter over the real adapter wrapped READ-ONLY:
                     real touch, product rules and fee tier; simulated fills in
                     the paper ledger. No write can reach the venue. OFF places
                     nothing at all (the GatedAdapter refuses orders); off<->paper
                     is the same adapter, so no rebuild.

    paper_ledger: PaperLedgerHooks (execution/persistence.make_paper_ledger_hooks);
    required for OFF and PAPER.
    """
    from execution.read_only_adapter import ReadOnlyAdapter

    kind = endpoint_kind_for_mode(mode)
    if mode == TradeMode.LIVE:
        adapter = build_coinbase_market(venue_cfg, require_trade=True)
        log.warning("built Coinbase adapter LIVE (real orders)")
        return adapter, kind
    if paper_ledger is None:
        raise ValueError(f"mode={mode.value} needs a paper ledger (PaperLedgerHooks)")
    from decimal import Decimal
    from execution.paper_adapter import PaperCoinbaseAdapter

    market = ReadOnlyAdapter(build_coinbase_market(venue_cfg, require_trade=False))
    kwargs = {}
    if paper_starting_cash is not None:
        kwargs["starting_cash"] = Decimal(str(paper_starting_cash))
    if fallback_fee_bps is not None:
        kwargs["fallback_fee_bps"] = Decimal(str(fallback_fee_bps))
    adapter = PaperCoinbaseAdapter(market, paper_ledger,
                                   quote_currency=venue_cfg.get("quote_currency", "USD"),
                                   **kwargs)
    log.info("built Coinbase PAPER adapter for mode=%s (real reads, simulated fills)",
             mode.value)
    return adapter, kind
