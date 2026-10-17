"""
execution/client_order_id.py — The deterministic client-order-id scheme.

This is small but it is the KEYSTONE of the whole crash-recovery and idempotency
story. Every order sent to a venue carries a client order id (IBKR orderRef;
Binance newClientOrderId) that is DERIVED FROM THE TRADE, never random. Because
it is deterministic and parseable:

  * A crash between "submit" and "ack" is recoverable: on restart we know exactly
    what id to ask the venue about ("did THIS order land?"), instead of blindly
    re-sending and risking a double-fill.
  * Replays/duplicates dedupe: re-submitting the same logical order produces the
    SAME id, so the venue's own client-id dedupe rejects the second one (belt),
    and our DB unique constraint on orders.client_order_id rejects it too
    (braces).
  * Boot reconcile attributes venue fills to the right trade by PARSING the id
    back into (trade_id, intent, rung), exactly rather than best-effort.

FORMAT (single line, venue-safe charset only):

    cv1.<trade_short>.<intent>.<rung>

  * "cv1"        — scheme tag + version. Lets us evolve the format later and
                   still parse old ids. If we ever change the layout, bump to
                   cv2 and keep cv1 parsing for in-flight recovery.
  * trade_short  — the trades.trade_id (a uuid) with dashes stripped. Uuids are
                   hex+dashes; stripping dashes keeps it within venue length
                   limits and the venue-safe charset. Full 32 hex chars — NOT
                   truncated, because a truncated uuid could collide and a
                   collision here means cross-trade order confusion with real
                   money. (See length budget note below.)
  * intent       — "E" (entry) or "X" (exit). One position has exactly one entry
                   but may have several exits over the escalation ladder.
  * rung         — for entries: always 0. For exits, the escalation rung:
                   0 for the first attempt, then 1, 2, ... for each re-issue at
                   a wider cap. Each rung is a DISTINCT venue order with a
                   DISTINCT id (required — re-issuing a cancelled exit at a
                   wider cap is a NEW order, and must not collide with the
                   cancelled one's id).
                   HISTORICAL: scale-out tranche orders (migration 017, since
                   removed) used strided rungs, tranche_block * 100 +
                   escalation (e.g. 300, 301). Such ids may still be in the
                   orders table or on the venue; they parse unchanged (rung is
                   any non-negative integer) and attribute to their trade.

Uniqueness contract: (trade_id, intent, rung) is globally unique by
construction, so the id is too. Entry: rung is always 0 (there is only ever one
entry order per trade — re-submitting a never-landed entry reuses id E.0 ON
PURPOSE, so the venue dedupes if it secretly landed). Exit: rung increments, so
each ladder step is a fresh id.

LENGTH BUDGET: tag(3) + dots(3) + trade_short(32) + intent(1) + rung(<=3 typ.)
= ~42 chars. IBKR orderRef allows well over this; Binance newClientOrderId
allows 36 chars for some endpoints and more for others — if a target venue's
limit is tighter than our id, that is a per-adapter concern: the adapter may
hash-compress the trade_short into its own space WHILE preserving determinism
(same input -> same compressed id) and keep a local map back. The CANONICAL id
stored in orders.client_order_id is this full form; any venue-side compression
is the adapter's private business and must round-trip deterministically.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

from execution.types import OrderIntent

_SCHEME_TAG = "cv1"

# Venue-safe: alnum and dots only. No slashes, spaces, or punctuation that any
# venue might reject or interpret.
_INTENT_TO_CODE = {OrderIntent.ENTRY: "E", OrderIntent.EXIT: "X"}
_CODE_TO_INTENT = {v: k for k, v in _INTENT_TO_CODE.items()}

# Strict parse pattern — anchored, fixed shape, so a malformed or foreign id is
# rejected loudly rather than silently mis-parsed.
_PATTERN = re.compile(
    r"^cv1\.([0-9a-f]{32})\.([EX])\.(\d+)$"
)


@dataclass(frozen=True)
class ParsedOrderId:
    trade_id: str          # canonical uuid form, dashes restored
    intent: OrderIntent
    rung: int


def build(trade_id: str, intent: OrderIntent, rung: int = 0) -> str:
    """Construct the deterministic client order id for (trade_id, intent, rung).

    Same inputs ALWAYS produce the same id — this is what makes re-submission
    idempotent and crash-recovery possible. trade_id is a uuid (with or without
    dashes accepted); we normalize to 32 lowercase hex chars internally.

    Entries: pass rung=0 (the only entry per trade). Exits: pass the escalation
    rung (0 for first attempt, 1, 2, ... for successive re-issues at wider caps).
    """
    short = _normalize_uuid(trade_id)
    if rung < 0:
        raise ValueError(f"rung must be >= 0, got {rung}")
    code = _INTENT_TO_CODE[intent]
    return f"{_SCHEME_TAG}.{short}.{code}.{rung}"


def parse(client_order_id: str) -> ParsedOrderId:
    """Parse a client order id back into (trade_id, intent, rung).

    Raises ValueError on anything that is not a well-formed cv1 id — including
    ids from another system sharing the venue account. Callers in boot reconcile
    use this to attribute venue orders/fills to the right trade; a foreign id
    raising here is correct (it is NOT one of ours, so it must not be attributed
    to one of our trades).
    """
    m = _PATTERN.match(client_order_id)
    if not m:
        raise ValueError(f"not a valid cv1 client_order_id: {client_order_id!r}")
    short, code, rung_s = m.group(1), m.group(2), m.group(3)
    return ParsedOrderId(
        trade_id=_restore_uuid(short),
        intent=_CODE_TO_INTENT[code],
        rung=int(rung_s),
    )


def is_ours(client_order_id: str) -> bool:
    """True if this id is one of ours (parses as a cv1 id). Cheap guard for
    reconcile loops scanning a venue account that may also carry orders from
    other systems or manual trading — we touch only ids we recognize."""
    return bool(_PATTERN.match(client_order_id))


# --- internals -------------------------------------------------------------

def _normalize_uuid(trade_id: str) -> str:
    """uuid (dashed or not) -> 32 lowercase hex chars. Validates it really is a
    uuid-shaped value so a bad trade_id can't silently produce a malformed id."""
    stripped = trade_id.replace("-", "").lower()
    if len(stripped) != 32 or not re.fullmatch(r"[0-9a-f]{32}", stripped):
        raise ValueError(f"trade_id is not a uuid: {trade_id!r}")
    return stripped


def _restore_uuid(short: str) -> str:
    """32 hex chars -> canonical dashed uuid (8-4-4-4-12)."""
    return f"{short[0:8]}-{short[8:12]}-{short[12:16]}-{short[16:20]}-{short[20:32]}"