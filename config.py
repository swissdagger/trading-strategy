"""
config.py — The trading system's universe and ladder configuration.

This is the trading repo's OWN config (not CausaView's). It is the single source
of truth for:
  * which symbols are tracked, grouped by asset class (ASSET_CONFIGS),
  * which strategies run and on which ladders (STRATEGIES, EXTRA_LADDERS), and
  * the execution venue's connection settings (VENUE_ENDPOINTS).

ADR-001 (docs/adr/001-coinbase-spot-long-only.md): crypto-only, long-only, one
venue (Coinbase Advanced Trade spot). The equity/fx classes are removed here; the
asset_class plumbing (universe.py, feeds/base.is_market_open, by-class caps) is
deliberately kept for future equities.

producer/universe.py reads ASSET_CONFIGS to resolve symbol -> asset_class. A
symbol must appear under exactly ONE class (universe.build_symbol_class_map
raises if a symbol is configured under two).
"""
from __future__ import annotations

import os


# Per-asset-class universe + the points/baseline the feeds and machine need.
# 'symbols'    — the tracked universe for this class.
# 'timeframes' — the bar granularities the feed must produce (= union of the
#   enabled strategies' ladders).
# 'points'     — history depth to seed per (symbol, timeframe). WINDOW_SIZE-aligned.
# 'baseline_timeframe' — informational; the actual baseline is the first entry of
#   each ladder.
ASSET_CONFIGS = {
    "crypto": {
        # Binance USDⓈ-M symbol form until the feed moves to Coinbase candles
        # (ADR-001 Phase 2), when the canonical form becomes the Coinbase
        # product id (BTC-USD). BNBUSDT and TRXUSDT are dropped per ADR-001
        # (not part of the Coinbase spot universe).
        "symbols": [
            "BTCUSDT",
            "ETHUSDT",
            "SOLUSDT",
            "XRPUSDT",
            "DOGEUSDT",
            "ADAUSDT",
            "LINKUSDT",
            "LTCUSDT",
            "NEARUSDT",
            "AAVEUSDT",
        ],
        # four_chain's ladder [1m,3m,5m,15m,1h] — the only enabled strategy.
        # prior_failure_scaleout's PF rungs (30m..12h) are not fetched while it
        # is disabled; re-add them here (and re-run warmup) before enabling it.
        #
        # Served by CONTINUOUS REST polling (BinanceFuturesFeed.start_live runs
        # a per-timeframe poll loop at ~tf/2 cadence, closed-window-only,
        # relaunched on error by feed_process). transport is 'rest' NOT 'ws':
        # the Binance futures WEBSOCKET (fstream) is unreachable from cloud
        # egress IPs — it connects but delivers zero frames. REST (fapi) is
        # reachable and is all this ladder needs (no sub-minute rungs).
        "timeframes": ["1m", "3m", "5m", "15m", "1h"],
        "points": 5000,
        "baseline_timeframe": "1m",
        "transport": "rest",
    },
}


def all_tracked_symbols() -> list[str]:
    """Every symbol across all classes — convenience for feed startup / health."""
    out: list[str] = []
    for cfg in ASSET_CONFIGS.values():
        out.extend(cfg.get("symbols", []))
    return out


def crypto_symbols() -> list[str]:
    """Every tracked CRYPTO symbol.

    four_chain runs only on crypto (its [1m,3m,5m,15m,1h] ladder is served by
    REST polling; see the crypto ASSET_CONFIGS note). Deriving the
    strategy's universe from ASSET_CONFIGS here — rather than a second hardcoded
    list in STRATEGIES — keeps the traded set in lockstep with the FETCHED set:
    a coin added to ASSET_CONFIGS['crypto'] auto-enrolls in four_chain, and a
    symbol can never be traded without also being fed.
    """
    return list(ASSET_CONFIGS.get("crypto", {}).get("symbols", []))


# Valid live-data transports. "rest" = the polling feeds; "ws" = the
# websocket feeds. Kept as a module constant so feed_process and config
# validation agree on the allowed set.
VALID_TRANSPORTS = ("rest", "ws")

# Asset classes that actually HAVE a websocket feed today. A class not
# listed here must not be configured with transport="ws" — the feed
# builder would have nothing to instantiate.
WS_CAPABLE_CLASSES = ("crypto",)


def transport_for(asset_class: str) -> str:
    """The configured live-data transport for a class: 'ws' or 'rest'.

    Defaults to 'rest' when the key is absent (existing configs unchanged).
    Raises on an unknown transport VALUE. Whether a class actually HAS a ws
    feed is a feed-construction concern, checked in feed_process._build_feeds
    (which raises RuntimeError, the codebase's startup-misconfig convention) —
    kept out of here so this stays a pure config-value accessor.
    """
    cfg = ASSET_CONFIGS.get(asset_class, {})
    transport = cfg.get("transport", "rest")
    if transport not in VALID_TRANSPORTS:
        raise ValueError(
            f"{asset_class}: unknown transport {transport!r}; "
            f"expected one of {VALID_TRANSPORTS}"
        )
    return transport

# ---------------------------------------------------------------------------
# Master trade MODE default (execution/trade_enable.py). Three states:
#   "off"   — place no orders.
#   "paper" — place orders against the PAPER venue (fake money): the shakeout.
#   "live"  — place orders against the LIVE venue (real money).
# The trade_enabled DB row's `mode` (migration 012) is the LIVE override that
# wins once running; this is only the boot/fallback mode. Default "off" — go to
# paper or live deliberately, never by accident.
# ---------------------------------------------------------------------------
TRADE_MODE_DEFAULT = "off"

# Execution venue settings, keyed by EXECUTOR_VENUE (run_executor.py). ADR-001:
# Coinbase Advanced Trade spot is the only venue. There is ONE block, not a
# paper/live pair: Coinbase has no paper venue (the sandbox has no matching
# engine), so paper mode wraps the real adapter's READS and simulates writes
# in-process (Phase 5). Both modes therefore use the same key and endpoint.
#
# Credentials come from the environment, never literals (see .env.example):
#   COINBASE_API_KEY_NAME     — CDP key name, organizations/{org}/apiKeys/{id}
#   COINBASE_API_PRIVATE_KEY  — the key's EC private key (PEM, ES256)
#   COINBASE_PORTFOLIO_UUID   — the dedicated bot portfolio the key is scoped to
VENUE_ENDPOINTS = {
    "coinbase": {
        "api_key_name": os.getenv("COINBASE_API_KEY_NAME", ""),
        "api_private_key": os.getenv("COINBASE_API_PRIVATE_KEY", ""),
        "portfolio_uuid": os.getenv("COINBASE_PORTFOLIO_UUID", ""),
        "base_url": os.getenv(
            "COINBASE_BASE_URL", "https://api.coinbase.com/api/v3/brokerage"),
    },
}

# ---------------------------------------------------------------------------
# STRATEGIES — the multi-strategy registry config (strategy/registry.py).
# Each entry: enabled, the symbols it trades ('*' = all tracked), the LADDERS
# it reads chains from (strategy/ladder_registry.py), and opaque params. A
# disabled strategy is never instantiated. The union of all enabled strategies'
# ladders is what the system computes (compute-once); shared ladders are
# computed once. Add a strategy: register its factory + add an entry here.
# ---------------------------------------------------------------------------
STRATEGIES = {
    "four_chain": {
        "enabled": True,              # ladder is streamed + warmed (crypto config)
        # Crypto-only: resolved just below the dict to the current crypto
        # universe so new crypto symbols auto-join.
        "symbols": [],                # set to crypto_symbols() below
        "ladders": ["FC"],            # the [1m,3m,5m,15m,1h] ladder below
        # No time-based retention (four_chain's pattern span is unbounded); the
        # ChainHistoryView is bounded only by a per-symbol count cap. Override it
        # here with 'max_chains_per_symbol' if a symbol ever approaches the
        # default; omitted => the view's default cap.
        "params": {},
    },
    # prior_failure_scaleout — the loose-gate sibling of four_chain (same
    # opposing-failure ingredient, no corroboration requirements; ~3x the
    # frequency for a similar edge per the research). Entry: a chain reaching
    # P2 with a recent prior opposing L1-only failure on the same symbol.
    # Exit: seven scale-out tranches banked as the chain propagates, weighted
    # toward the deep levels; any remainder closes at chain termination. NO
    # stop loss — deliberate; giveback/trailing stops failed as a class in the
    # research, the chain's own termination is the stop.
    #
    # DISABLED by ADR-001: expected to be replaced (slippage), and its PF rungs
    # above 1h are no longer fetched (ASSET_CONFIGS). Code stays in the tree
    # (debt D2). The original rollout gates still apply if it is revived:
    # (1) migration 016 applied, (2) the consuming team confirms
    # fraction/SCALE/TERMINAL support, (3) the strategy factory is registered
    # (rollout step 3), (4) the executor's partial-exit support lands (step 4). The executor carries an
    # interlock that refuses partial exits until (4), so enabling early fails
    # loud, not wrong — but don't.  Enable in PAPER mode first.
    #
    # NOTE on first enable: the gate needs prior-chain history. The PF
    # machines start producing chains only once this strategy is enabled
    # (owned_symbol_ladders gates computation), so for ~failure_window_hours
    # after first enable the gate cannot fire — expected warm-in, not a bug.
    # Re-run warmup_process just before enabling so the PF buffers hydrate
    # fresh (a stale warmup leaves a gap between its last bar and live).
    "prior_failure_scaleout": {
        "enabled": False,
        # Crypto-only, resolved below to the current crypto universe
        # (mirrors four_chain).
        "symbols": [],
        "ladders": ["PF"],            # [1m..12h] below; entry P2=3m, schedule tops at P9=6h
        "params": {
            "entry_level": 2,          # P2 (3m on PF). Schedule keys are OFFSETS from this.
            "gate": {
                # Admit iff a prior opposing chain on the same symbol FAILED
                # (exact depth 1 — initiated and died without propagating; a
                # chain that reached L3 then terminated is NOT a failure) with
                # its START inside the window before this chain's start.
                "failure_window_hours": 6,   # research default, never swept
                "failure_exact_depth": 1,
            },
            "exit": {
                # Fraction of ORIGINAL size banked at each level PAST entry
                # (offset -> fraction; sums to 1.0). Offsets, not absolute
                # levels: entry at P2 puts offset 7 at P9 = 6h. Back-loaded on
                # purpose — the deep levels carry the magnitude.
                "schedule": {1: 0.05, 2: 0.05, 3: 0.10, 4: 0.10,
                             5: 0.15, 6: 0.20, 7: 0.35},
                "remainder_at_termination": True,
                "stop": None,          # deliberate — see above; do not add one
            },
            "sizing": "flat",          # v1; per_symbol_base_rate is a follow-up
        },
    },
    # Example of a future strategy on different ladders (disabled):
    # "mean_revert": {
    #     "enabled": False,
    #     "symbols": ["EURUSD"],
    #     "ladders": ["C"],           # e.g. a 1m/5m/1h ladder registered at startup
    #     "params": {"lookback": 20},
    # },
}

# two_ladder is not configured at all (ADR-001): its sub-minute A/B ladders have
# no crypto source, and its code stays only because the built-in A/B ladders are
# woven into the ladder registry (debt D1). The signal process still registers
# its factory; with no STRATEGIES entry it is never instantiated.
STRATEGIES["four_chain"]["symbols"] = crypto_symbols()
STRATEGIES["prior_failure_scaleout"]["symbols"] = crypto_symbols()

# Extra ladders to register beyond the built-in A and B (name -> timeframes).
# A strategy referencing a ladder name not here (and not A/B) fails at startup.
EXTRA_LADDERS = {
    "FC": ["1m", "3m", "5m", "15m", "1h"],   # four_chain strategy's ladder
    # prior_failure_scaleout's ladder: the research's 10-level hierarchy.
    # Levels: 1m=P1 .. 6h=P9, 12h=P10. Entry at P2 (3m); the scale-out
    # schedule's last tranche is offset 7 = P9 = 6h, so 12h sits ABOVE the
    # schedule — kept in the ladder so live chain records stay depth-comparable
    # with the research population, at a cost of 2 forecasts/day/symbol.
    # PF shares 1m/3m/5m/15m/1h with FC; the ForecastMemo dedupes those
    # (Sumtyme is deterministic), so PF's marginal Sumtyme demand is only its
    # five new rungs (~70 calls/day/symbol), not a doubling of the shared ones.
    "PF": ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h"],
}

# Rungs whose short/unwarmed buffer is EXPECTED and acceptable, per ladder
# (ladder name -> timeframes). Downgrades the machine's hydrate fill report
# from WARNING to INFO for those rungs — logging only; an unwarmed rung is
# inert either way. PF's 12h needs ~6.9y of venue history (BTCUSDT only), and
# nothing in prior_failure_scaleout depends on it (schedule tops at 6h).
LADDER_OPTIONAL_TIMEFRAMES = {
    "PF": ["12h"],
}