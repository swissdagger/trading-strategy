"""
execution/adapter_factory.py — build the venue adapter for a given trade mode.

Paper vs live in QISS means a DIFFERENT VENUE ENDPOINT (IB gateway port 4002 +
paper account, or Binance testnet) — not a gate on top of a live connection. So
the mode selects which adapter to construct and where it connects. This factory
is that selection, kept in one place so the mode->endpoint mapping is explicit
and auditable (getting it wrong means real orders when you meant paper).

## Endpoint config

Per venue, config carries a PAPER and a LIVE endpoint block. The factory picks
by mode:
  TradeMode.PAPER -> the venue's paper endpoint (fake money)
  TradeMode.LIVE  -> the venue's live endpoint (real money)
  TradeMode.OFF   -> no venue connection is needed for order placement, but the
                     app may still want a read-only connection to observe; by
                     default OFF builds the PAPER adapter (safe: even if a stray
                     order slipped through the GatedAdapter's OFF gate, it would
                     hit paper, never live). Never build LIVE for OFF.

## Reconnect on mode change

When TradeEnableState fires on_mode_change(old, new) and the venue endpoint
differs between old and new (e.g. paper->live), the app rebuilds the adapter via
this factory for the new mode and reconnects. off<->paper doesn't change the
endpoint (both paper) so no rebuild is needed; anything involving live does.
"""
from __future__ import annotations

import logging

from execution.ibkr_adapter import IBKRAdapter
from execution.binance_adapter import BinanceAdapter
from execution.trade_enable import TradeMode

log = logging.getLogger("executor.adapter_factory")


def _ibkr_from_endpoint(ep: dict) -> IBKRAdapter:
    return IBKRAdapter(
        host=ep.get("host", "127.0.0.1"),
        port=ep["port"],
        client_id=ep.get("client_id", 1),
        account=ep.get("account", ""),
        connect_timeout=ep.get("connect_timeout", 15.0),
        default_exchange=ep.get("default_exchange", "SMART"),
        default_currency=ep.get("default_currency", "USD"),
    )


def _endpoint_for_mode(venue_cfg: dict, mode: TradeMode) -> tuple[str, dict]:
    """Return (endpoint_kind, endpoint_cfg) for a mode. endpoint_kind is
    'paper' or 'live' — the thing that actually determines the venue, so the
    caller can tell whether a mode change requires a rebuild (kind changed)."""
    if mode == TradeMode.LIVE:
        return "live", venue_cfg["live"]
    # PAPER and OFF both use the paper endpoint — OFF never connects live.
    return "paper", venue_cfg["paper"]


def build_ibkr_adapter(venue_cfg: dict, mode: TradeMode) -> tuple[IBKRAdapter, str]:
    """Build the IB adapter for `mode`. Returns (adapter, endpoint_kind).

    venue_cfg shape:
        {
          "paper": {"host":..., "port": 4002, "account": "DU...", ...},
          "live":  {"host":..., "port": 4001, "account": "U...",  ...},
        }
    """
    kind, ep = _endpoint_for_mode(venue_cfg, mode)
    adapter = _ibkr_from_endpoint(ep)
    log.info("built IBKR adapter for mode=%s endpoint=%s (port=%s)",
             mode.value, kind, ep.get("port"))
    return adapter, kind


def endpoint_kind_for_mode(mode: TradeMode) -> str:
    """The endpoint kind ('paper'|'live') a mode maps to, WITHOUT building an
    adapter — so the reconnect logic can decide whether a mode change actually
    changes the venue (and thus needs a rebuild) cheaply.

    off -> paper, paper -> paper, live -> live. So off<->paper is a no-venue-
    change transition; anything to/from live changes the venue.
    """
    return "live" if mode == TradeMode.LIVE else "paper"


def _binance_from_endpoint(ep: dict) -> BinanceAdapter:
    return BinanceAdapter(
        api_key=ep["api_key"],
        api_secret=ep["api_secret"],
        base_url=ep.get("base_url", "https://fapi.binance.com"),
        recv_window_ms=ep.get("recv_window_ms", 5000),
        request_timeout=ep.get("request_timeout", 10.0),
    )


def build_binance_adapter(venue_cfg: dict, mode: TradeMode) -> tuple[BinanceAdapter, str]:
    """Build the Binance adapter for `mode`. Returns (adapter, endpoint_kind).

    PAPER/OFF -> the paper endpoint (Binance testnet, base_url
    https://demo-fapi.binance.com); LIVE -> the live endpoint (production fapi).
    Same paper-never-live safety as IB: OFF builds paper, never live.

    venue_cfg shape:
        {
          "paper": {"api_key":..., "api_secret":..., "base_url": "https://demo-fapi.binance.com"},
          "live":  {"api_key":..., "api_secret":..., "base_url": "https://fapi.binance.com"},
        }
    """
    kind, ep = _endpoint_for_mode(venue_cfg, mode)
    adapter = _binance_from_endpoint(ep)
    log.info("built Binance adapter for mode=%s endpoint=%s (base=%s)",
             mode.value, kind, ep.get("base_url"))
    return adapter, kind