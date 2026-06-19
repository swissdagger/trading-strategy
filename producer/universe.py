"""
producer/universe.py — symbol -> asset_class resolution from ASSET_CONFIGS.

The chain machine stamps asset_class on every event, signal, and chain, so each
machine must be built with the CORRECT class for its symbol. The single source of
truth is config.ASSET_CONFIGS (the same dict the runbook requires carry both
ladders' timeframes per class). This module turns that dict into a symbol -> class
lookup so nothing downstream hardcodes a class.

Ladders are UNIVERSAL across asset classes (Set A / Set B use the same timeframes
for every class — confirmed design decision), so the class affects only the TAG
carried through the pipeline, never the routing or level numbering. That keeps
ladders.py class-agnostic; only this resolver is class-aware.
"""
from __future__ import annotations

from typing import Dict


def build_symbol_class_map(asset_configs: dict) -> Dict[str, str]:
    """Flatten ASSET_CONFIGS into {symbol -> asset_class}.

    ASSET_CONFIGS is keyed by asset class, each value carrying a 'symbols' list
    (the same shape CausaView's config uses). A symbol appearing under two classes
    is a config error — raise loudly rather than silently pick one, because a
    mis-tagged symbol would corrupt every event it produces.
    """
    out: Dict[str, str] = {}
    for asset_class, cfg in asset_configs.items():
        for symbol in cfg.get("symbols", []):
            if symbol in out and out[symbol] != asset_class:
                raise ValueError(
                    f"symbol {symbol!r} configured under both {out[symbol]!r} and "
                    f"{asset_class!r} — a symbol must belong to exactly one class")
            out[symbol] = asset_class
    return out


class ClassResolver:
    """Resolves a symbol to its asset class, with a clear failure for unknown
    symbols. Built once at startup from ASSET_CONFIGS and passed to the factory."""

    def __init__(self, symbol_class_map: Dict[str, str]):
        self._map = symbol_class_map

    def resolve(self, symbol: str) -> str:
        cls = self._map.get(symbol)
        if cls is None:
            # An unknown symbol means a bar arrived for something not in our
            # universe. The chain machine cannot tag it correctly, so the caller
            # must decide to skip it — surfaced as a clear error, not a silent
            # empty-string class.
            raise KeyError(f"symbol {symbol!r} not in any asset class in ASSET_CONFIGS")
        return cls

    def known(self, symbol: str) -> bool:
        return symbol in self._map
