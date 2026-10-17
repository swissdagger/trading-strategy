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


# The traded universe: base assets, all quoted in QUOTE_CURRENCY. The product
# id is f"{base}-{QUOTE_CURRENCY}" (BTC-USD). BNB and TRX were dropped in
# ADR-001 Phase 1. Changing the quote (e.g. to GBP or USDC) changes every
# symbol, so it means a fresh DB and a new warmup — and the account must be
# able to trade that order book.
CRYPTO_BASES = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "LINK", "LTC", "NEAR", "AAVE",
]
QUOTE_CURRENCY = "USD"

# Per-asset-class universe + the points/baseline the feeds and machine need.
# 'symbols'    — the tracked universe for this class.
# 'timeframes' — the bar granularities the feed must produce (= union of the
#   enabled strategies' ladders).
# 'points'     — history depth to seed per (symbol, timeframe). WINDOW_SIZE-aligned.
# 'baseline_timeframe' — informational; the actual baseline is the first entry of
#   each ladder.
ASSET_CONFIGS = {
    "crypto": {
        # Coinbase product ids (ADR-001 Phase 2): the canonical symbol
        # everywhere — Kafka, chains, trades, trade_signals. Built from
        # CRYPTO_BASES and QUOTE_CURRENCY below.
        "symbols": [],
        # confirmed_15m's ladder [1m,3m,5m,15m,1h] — the only enabled strategy.
        # prior_failure_scaleout's PF rungs (30m..12h) are not fetched while it
        # is disabled; re-add them here (and re-run warmup) before enabling it.
        #
        # Served by CONTINUOUS REST polling of Coinbase's public candles
        # endpoint (CoinbaseSpotFeed.start_live: one poll loop per timeframe
        # at ~tf/2, complete windows only, relaunched on error by
        # feed_process). 3m has no Coinbase granularity and is derived from
        # 1m. No websocket feed exists for Coinbase yet.
        # FC ∪ CN. The seconds rungs (30s/45s) are built from trades and
        # served ONLY for subminute_symbols (warmup and live); 2m/30m come
        # from candles for every symbol but are owned only by CN strategies.
        "timeframes": ["30s", "45s", "1m", "2m", "3m", "5m", "15m", "30m", "1h"],
        "points": 5000,
        "baseline_timeframe": "1m",
        "transport": "rest",
        # CAPACITY. CN adds ~5,570 forecasts/symbol/day beyond FC (30s 2,880 +
        # 45s 1,920 + 2m 720 + 30m 48; the rest is memo-shared with FC) ≈
        # 0.064/s per symbol, on top of FC's ~0.27/s for ten symbols, against
        # the measured 0.36/s peak. ONE symbol fits; a second needs the D12
        # re-benchmark first. This list is the CN universe.
        "subminute_symbols": ["BTC-USD"],
    },
    # US equities via Polygon (PR 9). The DENSE LADDER (EXTRA_LADDERS["DL"]):
    # a 30s baseline with two more seconds-rungs, so the chain machine sees
    # directional change earlier than the 1m crypto ladder can. Research
    # direction, not a trading universe: the executor has no equity venue and
    # routes equity signals to the shadow book (execution/signal_executor).
    #
    # CAPACITY (do this arithmetic before adding a symbol). Over the extended
    # 04:00–20:00 ET session the ladder needs ~5,700 forecasts/symbol/day,
    # ~0.1/s/symbol while the market is open. At the measured 0.36 calls/s
    # peak that is ~3 symbols. The forecast/apply split (PR 8) is what makes
    # even ONE symbol fit: serial per-symbol processing capped a symbol at
    # one forecast per ~8 s, and 30s+45s+1m+90s+2m+3m need one every ~6 s.
    #
    # SESSION: feeds/base.is_market_open('equity') is the extended envelope,
    # per product decision. Warmup's trade walk and the live feed use that
    # same definition — in step mode the bar sequence IS the step, so the two
    # must not differ.
    #
    # SOURCES (feeds/polygon_aggs.py): 30s/45s/90s from raw trades (history:
    # session walk; REST live: forward poll; ws live: 1s aggregates), 2m/3m
    # from 1-minute bars re-bucketed locally, the rest from Polygon's own
    # buckets. Transport 'ws' with automatic fallback to 'rest' if the socket
    # cannot be established on a cold start (feed_process._run_feed).
    "equity": {
        "symbols": ["SPCX", "XOM", "NVDA"],
        # Union of the DL ladder and the ED ladder (rule1_v1's research
        # ladder adds 5m); the memo dedupes the rungs they share.
        "timeframes": ["30s", "45s", "1m", "90s", "2m", "3m", "5m",
                       "15m", "30m", "1h", "2h", "4h", "6h"],
        "points": 5000,
        "baseline_timeframe": "30s",
        "transport": "ws",
    },
}


ASSET_CONFIGS["crypto"]["symbols"] = [f"{b}-{QUOTE_CURRENCY}" for b in CRYPTO_BASES]


def all_tracked_symbols() -> list[str]:
    """Every symbol across all classes — convenience for feed startup / health."""
    out: list[str] = []
    for cfg in ASSET_CONFIGS.values():
        out.extend(cfg.get("symbols", []))
    return out


def serves(asset_class: str, symbol: str, timeframe: str) -> bool:
    """Whether a (symbol, timeframe) stream exists for this class. Seconds
    rungs are served only for the class's `subminute_symbols` (if set)."""
    cfg = ASSET_CONFIGS.get(asset_class, {})
    if timeframe not in cfg.get("timeframes", []):
        return False
    if timeframe.endswith("s"):
        sub = cfg.get("subminute_symbols")
        return sub is None or symbol in sub
    return True


def equity_symbols() -> list[str]:
    """The configured equity universe (Polygon tickers), for strategy symbol
    lists. Empty when no equities are configured."""
    return list(ASSET_CONFIGS.get("equity", {}).get("symbols", []))


# Research collection (research_process.py, migration 025). The traded
# equities plus market references and the VIX proxy — bars with volume/VWAP/
# trade count are collected for all of these; NBBO quotes only for symbols
# that produce trade_signals. Collecting bars costs no Sumtyme capacity, so
# this list can be wider than the ladder universe.
RESEARCH_EXTRA_SYMBOLS = ["SPY", "QQQ", "VIXY"]


def research_symbols() -> list[str]:
    out = list(ASSET_CONFIGS.get("equity", {}).get("symbols", []))
    for s in RESEARCH_EXTRA_SYMBOLS:
        if s not in out:
            out.append(s)
    return out


def crypto_symbols() -> list[str]:
    """Every tracked CRYPTO symbol.

    confirmed_15m runs only on crypto (its [1m,3m,5m,15m,1h] ladder is served by
    REST polling of Coinbase candles; see the crypto ASSET_CONFIGS note). Deriving the
    strategy's universe from ASSET_CONFIGS here — rather than a second hardcoded
    list in STRATEGIES — keeps the traded set in lockstep with the FETCHED set:
    a coin added to ASSET_CONFIGS['crypto'] auto-enrolls in confirmed_15m, and a
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
#   COINBASE_API_PRIVATE_KEY  — the key's EC private key (PEM, ES256). The key
#                               must be created with the ECDSA algorithm:
#                               Coinbase rejects Ed25519 keys for this API.
#   COINBASE_PORTFOLIO_UUID   — the dedicated bot portfolio the key is scoped to
# The key must have View (+ Trade for live) and must NOT have Transfer:
# CoinbaseAdapter.connect() refuses a key that can move funds.
VENUE_ENDPOINTS = {
    "coinbase": {
        "api_key_name": os.getenv("COINBASE_API_KEY_NAME", ""),
        "api_private_key": os.getenv("COINBASE_API_PRIVATE_KEY", ""),
        "portfolio_uuid": os.getenv("COINBASE_PORTFOLIO_UUID", ""),
        "base_url": os.getenv(
            "COINBASE_BASE_URL", "https://api.coinbase.com/api/v3/brokerage"),
        # Positions are "{BASE}-{QUOTE}" base-asset balances; equity is in this.
        "quote_currency": QUOTE_CURRENCY,
    },
}

# ---------------------------------------------------------------------------
# Executor risk and safety settings (ADR-001 Phase 3). Read by run_executor.py
# and injected into the executor; execution/ never imports config directly.
# ---------------------------------------------------------------------------

# Hard cap on concurrently open positions across all symbols and strategies
# (non-terminal trades, HALTED included: a halted trade may still hold capital).
# Checked under a lock before every ENTER. Placeholder — tune in paper.
MAX_OPEN_POSITIONS = 5

# An ENTER signal written longer ago than this (trade_signals.created_at ->
# now) is skipped, never acted on: after executor downtime the catch-up sweep
# replays history and a stale entry would buy at a price the strategy never
# saw. Measured from created_at (when the signal was written), NOT decided_at
# (the triggering bar's window start, already 15+ minutes back for a 15m
# trigger). EXITs are ALWAYS honoured regardless of age. Default 2 x the 1m
# baseline timeframe.
ENTER_MAX_AGE_SECONDS = 2 * 60

# Venue balances the executor ignores when checking for orphans at boot: base
# assets (e.g. "ETH") or full symbols (e.g. "ETH-USD"). Any OTHER non-dust
# balance in the bot portfolio with no open trade refuses boot. The dedicated
# portfolio should make this rarely needed (ADR-001 debt D13).
ORPHAN_BALANCE_ALLOWLIST: frozenset = frozenset()

# FALLBACK taker fee, in basis points per side, for the shadow book's virtual
# fills and paper fills. Both normally use the account's live fee tier
# (GET /transaction_summary); this applies only when that can't be read.
# Coinbase Advanced Trade's lowest volume tier taker rate at the time of writing.
SHADOW_TAKER_FEE_BPS = 120

# Paper mode (ADR-001 Phase 5): the simulated account's starting cash, in
# QUOTE_CURRENCY, seeded into paper_balances on the executor's first paper
# start. Truncate the paper_* tables to reset the account.
PAPER_STARTING_CASH = 10_000

# ---------------------------------------------------------------------------
# STRATEGIES — the multi-strategy registry config (strategy/registry.py).
# Each entry: enabled, the symbols it trades ('*' = all tracked), the LADDERS
# it reads chains from (strategy/ladder_registry.py), and opaque params. A
# disabled strategy is never instantiated. The union of all enabled strategies'
# ladders is what the system computes (compute-once); shared ladders are
# computed once. Add a strategy: register its factory + add an entry here.
# ---------------------------------------------------------------------------
STRATEGIES = {
    "confirmed_15m": {
        "enabled": True,              # ladder is streamed + warmed (crypto config)
        # Crypto-only: resolved just below the dict to the current crypto
        # universe so new crypto symbols auto-join.
        "symbols": [],                # set to crypto_symbols() below
        "ladders": ["FC"],            # the [1m,3m,5m,15m,1h] ladder below
        # No time-based retention (confirmed_15m's pattern span is unbounded); the
        # ChainHistoryView is bounded only by a per-symbol count cap. Override it
        # here with 'max_chains_per_symbol' if a symbol ever approaches the
        # default; omitted => the view's default cap.
        "params": {},
    },
    # prior_failure_scaleout — the loose-gate sibling of confirmed_15m (same
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
        # (mirrors confirmed_15m).
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
STRATEGIES["confirmed_15m"]["symbols"] = crypto_symbols()
STRATEGIES["prior_failure_scaleout"]["symbols"] = crypto_symbols()

# chain_recorder — a record-only OWNER for the dense equity ladder. The router
# only computes chains for (symbol, ladder) pairs some enabled strategy owns,
# so a ladder being evaluated as data still needs an owner; this one emits no
# trade_signals (chains and signals rows are the product). Replace it with the
# real DL strategy when that lands: same 'symbols'/'ladders', different key.
STRATEGIES["chain_recorder"] = {
    "enabled": True,              # kept on beside rule1: depth counters on /health, no signals
    "symbols": equity_symbols(),
    "ladders": ["DL"],
    "params": {},
}

# ---------------------------------------------------------------------------
# The owner's frozen chain-following rules (strategy/rules.py, run verbatim
# through strategy/chain_rules.py). Each is its OWN pre-registered 200-trade
# review; the package is explicit that trades are never pooled across ladders
# or rules, which is why a rule on two ladders is two strategies.
#
# LEVELS ARE LADDER-RELATIVE. "P2" on the research equity ladder (ED) is 3m;
# on the crypto ladder (FC) it is 3m; on the dense ladder (DL) it is 45s. The
# adapter logs the resolved (level, rung) pairs at start. `entry_tf` /
# `confirm_tf` pin a rule to rungs by name when a level number would land
# somewhere else on a different ladder.
#
# All of these are SHADOW-ONLY (SHADOW_ONLY_STRATEGIES below): the executor
# never sends their signals to a venue, in paper or live mode. Crypto LONGs
# would otherwise reach Coinbase.
# ---------------------------------------------------------------------------
STRATEGIES["rule1_v1"] = {                       # the research rule, on the research ladder
    "enabled": True,
    "symbols": equity_symbols(),
    "ladders": ["ED"],
    "params": {"rule": "RULE1_EQUITY", "tick_seconds": 15},     # P2 = 3m, L3 = 5m
}
STRATEGIES["rule1_new_literal_v1"] = {           # the same rule's level numbers on the dense ladder
    "enabled": True,
    "symbols": equity_symbols(),
    "ladders": ["DL"],
    "params": {"rule": "RULE1_EQUITY", "tick_seconds": 15},     # P2 = 45s, L3 = 1m
}
STRATEGIES["rule3_v1"] = {                       # crypto, pre-registered 2026-10-17
    "enabled": True,
    "symbols": crypto_symbols(),
    "ladders": ["FC"],                           # FC's P2 = 3m, L3 = 5m: the research ladder's
    "params": {"rule": "RULE3_CRYPTO", "tick_seconds": 15},
}
# The two new-ladder crypto rules, on the CN universe only (the seconds rungs
# are built from trades for ASSET_CONFIGS["crypto"]["subminute_symbols"]).
STRATEGIES["rule3_new_literal_v1"] = {
    "enabled": True,
    "symbols": list(ASSET_CONFIGS["crypto"]["subminute_symbols"]),
    "ladders": ["CN"],
    "params": {"rule": "RULE3_NEW_LITERAL", "tick_seconds": 15},  # P2 = 45s, L3 = 1m
}
STRATEGIES["rule3_new_equiv_v1"] = {
    "enabled": True,
    "symbols": list(ASSET_CONFIGS["crypto"]["subminute_symbols"]),
    "ladders": ["CN"],
    "params": {"rule": "RULE3_NEW_EQUIV", "entry_tf": "3m", "confirm_tf": "5m",
               "tick_seconds": 15},                               # pinned by rung, not level
}

# Strategies whose signals the executor must NEVER route to a venue, in any
# trade mode: they go to the shadow book (virtual fills at decision_price).
# The chain rules are paper-only by their own spec.
SHADOW_ONLY_STRATEGIES = frozenset(
    # FAIL CLOSED: every chain-rule strategy (a `params.rule` entry), enabled
    # or not, is shadow-only by construction, so adding one cannot send its
    # signals to a venue by forgetting to list it here.
    {name for name, cfg in STRATEGIES.items() if "rule" in (cfg.get("params") or {})}
    | {"rule1_v1", "rule1_new_literal_v1", "rule3_v1",
       "rule3_new_literal_v1", "rule3_new_equiv_v1"}
)

# Shadow-book fee per leg (bps) by asset class, overriding the venue's fee
# tier where the venue does not trade the class. Equity CFD half-spread 0.05%
# per side is the package's paper assumption; set it from the real ticket
# (research_quotes has the measured spread per signal).
SHADOW_FEE_BPS_BY_CLASS = {"equity": 5}

# Extra ladders to register beyond the built-in A and B (name -> timeframes).
# A strategy referencing a ladder name not here (and not A/B) fails at startup.
EXTRA_LADDERS = {
    "FC": ["1m", "3m", "5m", "15m", "1h"],   # confirmed_15m strategy's ladder
    # prior_failure_scaleout's ladder: the research's 10-level hierarchy.
    # Levels: 1m=P1 .. 6h=P9, 12h=P10. Entry at P2 (3m); the scale-out
    # schedule's last tranche is offset 7 = P9 = 6h, so 12h sits ABOVE the
    # schedule — kept in the ladder so live chain records stay depth-comparable
    # with the research population, at a cost of 2 forecasts/day/symbol.
    # PF shares 1m/3m/5m/15m/1h with FC; the ForecastMemo dedupes those
    # (Sumtyme is deterministic), so PF's marginal Sumtyme demand is only its
    # five new rungs (~70 calls/day/symbol), not a doubling of the shared ones.
    "PF": ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h"],
    # DL — the dense equity ladder (ASSET_CONFIGS["equity"]). 12 rungs; a
    # chain climbs one rung at a time (no skipping), so 3m→15m is one step
    # like any other. Levels: 30s=L1 .. 6h=L12. Shares 1m/3m/15m/1h LABELS
    # with FC, but the memo keys on asset class, so equity and crypto windows
    # never collide; ownership (chain_recorder on equities, confirmed_15m on
    # crypto) keeps the router from building DL machines for crypto symbols.
    "DL": ["30s", "45s", "1m", "90s", "2m", "3m",
           "15m", "30m", "1h", "2h", "4h", "6h"],
    # ED — the equity DEFAULT ladder (1m base), on which rule1_v1's 216
    # research trades were validated: P2 = 3m, L3 = 5m — the same rungs as
    # the crypto research ladder's P2/L3. Every rung but 5m is in DL, so its
    # marginal Sumtyme demand is ~288 forecasts/symbol/day.
    "ED": ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h"],
    # CN — the crypto NEW ladder from the strategy package (30s base, 5m rung,
    # no 90s): the ladder rule3_new_literal/equiv are defined against. The
    # seconds rungs are built from Coinbase trades (feeds/coinbase_spot.py).
    "CN": ["30s", "45s", "1m", "2m", "3m", "5m", "15m", "30m", "1h"],
}

# Rungs whose short/unwarmed buffer is EXPECTED and acceptable, per ladder
# (ladder name -> timeframes). Downgrades the machine's hydrate fill report
# from WARNING to INFO for those rungs — logging only; an unwarmed rung is
# inert either way. PF's 12h needs ~6.9y of venue history (BTC-USD only), and
# nothing in prior_failure_scaleout depends on it (schedule tops at 6h).
LADDER_OPTIONAL_TIMEFRAMES = {
    "PF": ["12h"],
    # 5000 × 6h of EQUITY session time is years of history; a young listing
    # cannot warm these and they fill live. Logging only.
    "DL": ["4h", "6h"],
    "ED": ["4h"],
}