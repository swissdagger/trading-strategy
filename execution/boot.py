"""
execution/boot.py — The executor's boot-and-reconcile sequence.

This is the highest-stakes code in the system. It runs ONCE at startup, before
the executor consumes a single chain event, and its job is to make the DB's
picture of the world agree with the venue's picture of the world — or, where it
cannot safely do so, to refuse to trade (HALT) and alert a human.

THE BOOT-ORDERING INVARIANT (from execution/adapter.py), made concrete here:

    1. connect()           — block until a trading-enabled session is live
    2. reconcile()         — snapshot the venue's truth
    3. diff & resolve      — this module; un-resolvable -> HALTED + alert
    4. check kill switch
    5. ONLY THEN           — subscribe to causal-events and begin acting

Steps 1-4 are deliberately "boring and slow." Subscribing to events (step 5) is
the LAST thing, never the first — a replayed event must not trigger an order
before we have confirmed what we already hold.

RESOLUTION PRINCIPLE (agreed): when in doubt, HALT and alert. Never auto-resolve
a discrepancy we cannot explain. The deterministic client_order_id makes the
venue authoritative for "did my order land", so PENDING/SUBMITTED/CLOSING cases
auto-resolve cleanly by querying the venue. The cases that HALT are the ones
where we held (or hold) something we cannot explain.

TWO TUNABLES (config, changeable without code edits):
  * orphan_position_policy: what to do when the venue holds a position we have
    NO trade for. Default 'manual' (alert + refuse to act). 'auto_flatten' is
    only safe if this account is EXCLUSIVELY this system's — never with a shared
    or manually-hedged account.
  * recent_fills_lookback_seconds: how far back reconcile fetches venue fills to
    explain a missed close. Default 24h. Too short -> legitimate closes look
    like unexplained HALTs; too long -> slow/noisy reconcile.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Awaitable, Callable, Dict, List, Optional

from execution import client_order_id as coid
from execution.adapter import ExecutionAdapter
from execution.types import (
    Fill,
    OrderIntent,
    OrderStatus,
    ReconcileResult,
    TradeSide,
    VenueOrder,
    VenuePosition,
)

log = logging.getLogger("executor.boot")


# Alert sink: wired by the executor to whatever raises a human-visible alert
# (Service-4-equivalent notification, PagerDuty, etc.). Boot NEVER swallows a
# HALT silently — every HALT routes here.
AlertCallback = Callable[[str, dict], Awaitable[None]]


@dataclass(frozen=True)
class BootConfig:
    orphan_position_policy: str = "manual"        # 'manual' | 'auto_flatten'
    recent_fills_lookback_seconds: int = 86_400   # 24h
    # Quantity tolerance for "position matches": venues report sizes with their
    # own rounding; treat |db - venue| <= epsilon as equal rather than HALTing on
    # dust. Per-asset-class precision should set this; a tiny default here.
    qty_match_epsilon: Decimal = Decimal("1e-9")


# ---------------------------------------------------------------------------
# DB-side view. The executor passes in the current non-terminal trades (and the
# helpers to mutate them). boot.py is pure orchestration over these — it does
# not own the DB connection, so it stays testable and the write-authority stays
# with the executor's persistence layer.
# ---------------------------------------------------------------------------
@dataclass
class DbTrade:
    trade_id: str
    chain_id: str
    symbol: str
    asset_class: str
    side: TradeSide
    state: str                       # trade_state enum value
    entry_client_order_id: str
    entry_target_qty: Decimal
    entry_filled_qty: Decimal
    exit_filled_qty: Decimal

    @property
    def open_qty(self) -> Decimal:
        """Signed position we believe we hold: + for LONG, - for SHORT."""
        net = self.entry_filled_qty - self.exit_filled_qty
        return net if self.side == TradeSide.LONG else -net


# Persistence hooks the executor supplies. Keeping them as injected callables
# (rather than importing a DB module here) keeps boot.py free of DB-driver
# specifics and unit-testable with fakes.
@dataclass
class BootHooks:
    load_nonterminal_trades: Callable[[], Awaitable[List[DbTrade]]]
    write_fill: Callable[[Fill, str], Awaitable[None]]            # (fill, trade_id)
    set_state: Callable[[str, str, Optional[str]], Awaitable[None]]  # (trade_id, state, halt_reason)
    resubmit_entry: Callable[[DbTrade], Awaitable[None]]
    resume_exit: Callable[[DbTrade], Awaitable[None]]             # re-arm escalation/exit rule
    reissue_exit: Callable[[DbTrade], Awaitable[None]]            # close order vanished, re-place


async def run_boot_reconcile(
    adapter: ExecutionAdapter,
    hooks: BootHooks,
    alert: AlertCallback,
    config: BootConfig = BootConfig(),
) -> bool:
    """Execute the full boot sequence. Returns True if the executor may proceed
    to subscribe to events, False if boot ended in a state where a human must
    intervene before trading (any HALT raised). The executor treats False as
    "stay halted, do not subscribe."

    NOTE: returning True does not mean zero discrepancies — it means every
    discrepancy was either resolved or safely parked as a HALTED trade that does
    not block the rest of the system. Orphan positions and unexplained losses
    raise alerts; whether they block startup entirely is the executor's call via
    the return value semantics below (we return False only on orphan-with-manual,
    the one class that means "the account is in a state we don't model").
    """
    # ---- Step 1: connect (blocks until trading-enabled session confirmed) ----
    log.info("boot: connecting to venue %s ...", adapter.venue)
    await adapter.connect()
    log.info("boot: venue %s session confirmed trading-enabled", adapter.venue)

    # ---- Step 2: snapshot venue truth ----
    snap: ReconcileResult = await adapter.reconcile()
    positions_by_symbol = {p.symbol: p for p in snap.positions}
    orders_by_client_id: Dict[str, VenueOrder] = {
        o.client_order_id: o for o in snap.open_orders if o.client_order_id
    }
    fills_by_client_id = _index_fills_by_order(snap.recent_fills, snap.open_orders)

    # ---- Step 3: diff each non-terminal DB trade against venue truth ----
    db_trades = await hooks.load_nonterminal_trades()
    matched_symbols: set[str] = set()
    safe_to_proceed = True

    for t in db_trades:
        matched_symbols.add(t.symbol)
        try:
            await _resolve_trade(
                t, adapter, hooks, alert, config,
                positions_by_symbol, orders_by_client_id, fills_by_client_id,
            )
        except Exception as e:  # never let one trade's resolution abort the rest
            # An exception DURING resolution is itself a halt-worthy unknown.
            reason = f"reconcile_exception: {e!r}"
            log.exception("boot: resolution failed for trade %s", t.trade_id)
            await hooks.set_state(t.trade_id, "HALTED", reason)
            await alert("trade_halted_on_boot",
                        {"trade_id": t.trade_id, "reason": reason})

    # ---- Step 3b: orphan positions — venue holds something we have no trade for ----
    for sym, pos in positions_by_symbol.items():
        if abs(pos.qty) <= config.qty_match_epsilon:
            continue  # flat, not an orphan
        if sym in matched_symbols:
            continue  # accounted for by a DB trade above
        # A position with no corresponding trade. The scariest discrepancy:
        # capital committed that our system does not know it owns. NEVER
        # auto-act under 'manual'. Under 'auto_flatten' (exclusive-account only)
        # the executor's own flatten path handles it — boot still alerts.
        await alert("orphan_position",
                    {"symbol": sym, "qty": str(pos.qty),
                     "policy": config.orphan_position_policy})
        log.error("boot: ORPHAN position %s qty=%s policy=%s",
                  sym, pos.qty, config.orphan_position_policy)
        if config.orphan_position_policy == "manual":
            # We do not model the account's current state -> do not trade.
            safe_to_proceed = False
        # 'auto_flatten' intentionally does NOT block proceed; the executor
        # flattens the orphan as a KILL_SWITCH-style exit outside boot.

    if not safe_to_proceed:
        log.error("boot: NOT proceeding to event subscription — manual "
                  "intervention required (orphan position under 'manual').")
    else:
        log.info("boot: reconcile complete, safe to proceed.")
    return safe_to_proceed


async def _resolve_trade(
    t: DbTrade,
    adapter: ExecutionAdapter,
    hooks: BootHooks,
    alert: AlertCallback,
    config: BootConfig,
    positions_by_symbol: Dict[str, VenuePosition],
    orders_by_client_id: Dict[str, VenueOrder],
    fills_by_client_id: Dict[str, List[Fill]],
) -> None:
    """Resolve ONE DB trade against the venue snapshot, per the agreed diff
    matrix. Each branch either advances the trade to a consistent state or
    HALTs it with an explicit reason.
    """
    pos = positions_by_symbol.get(t.symbol)
    has_position = pos is not None and abs(pos.qty) > config.qty_match_epsilon

    # ----- PENDING / SUBMITTED: did the ENTRY order land? Ask the venue. -----
    if t.state in ("PENDING", "SUBMITTED"):
        vorder = orders_by_client_id.get(t.entry_client_order_id)
        vfills = fills_by_client_id.get(t.entry_client_order_id, [])

        if vfills:
            # Order (partly) filled while we were down. Record fills; OPEN.
            for f in vfills:
                await hooks.write_fill(f, t.trade_id)
            await hooks.set_state(t.trade_id, "OPEN", None)
            await hooks.resume_exit(t)   # exit rule takes over the live position
            log.info("boot: %s %s entry filled during downtime -> OPEN",
                     t.symbol, t.side.value)
            return
        if vorder and vorder.status in (OrderStatus.WORKING,
                                        OrderStatus.PARTIALLY_FILLED):
            # Still working at the venue. Stay SUBMITTED; live fill stream will
            # advance it. (PARTIALLY_FILLED with fills not in recent window is
            # rare; the fill stream reconciles it.)
            await hooks.set_state(t.trade_id, "SUBMITTED", None)
            return
        if vorder and vorder.status in (OrderStatus.CANCELLED,
                                        OrderStatus.REJECTED):
            await hooks.set_state(t.trade_id, "REJECTED",
                                  None if vorder.status == OrderStatus.CANCELLED
                                  else "entry_rejected_at_venue")
            # CANCELLED entry with no fills => no position; terminal as REJECTED-
            # equivalent (no capital committed). REJECTED reason recorded above.
            return
        # Venue has never heard of this order id and no fills exist: it never
        # landed. Safe to re-submit the entry fresh (same deterministic id, so
        # if it DID land and we just can't see it, the venue dedupes).
        await hooks.resubmit_entry(t)
        log.info("boot: %s %s entry never landed -> resubmit", t.symbol, t.side.value)
        return

    # ----- OPEN: do we still hold the position we think we do? -----
    if t.state == "OPEN":
        if has_position and _position_matches(t, pos, config):
            # Clean re-adoption. Resume-management: exit rule resumes governing.
            await hooks.resume_exit(t)
            log.info("boot: %s %s OPEN position re-adopted", t.symbol, t.side.value)
            return
        if not has_position:
            # We held it; venue shows nothing. Can we explain the close?
            exit_fills = _explaining_exit_fills(t, fills_by_client_id)
            if exit_fills:
                for f in exit_fills:
                    await hooks.write_fill(f, t.trade_id)
                await hooks.set_state(t.trade_id, "CLOSED", None)
                log.info("boot: %s %s closed during downtime (explained) -> CLOSED",
                         t.symbol, t.side.value)
                return
            # Unexplained disappearance of a held position. HALT.
            reason = "open_position_vanished_unexplained"
            await hooks.set_state(t.trade_id, "HALTED", reason)
            await alert("trade_halted_on_boot",
                        {"trade_id": t.trade_id, "symbol": t.symbol, "reason": reason})
            log.error("boot: %s OPEN but venue flat, no explaining fill -> HALTED",
                      t.symbol)
            return
        # Position exists but size/side disagrees with our record. HALT — we do
        # not silently adopt a position we can't reconcile to our quantity.
        reason = (f"open_position_mismatch db_open={t.open_qty} "
                  f"venue={pos.qty if pos else None}")
        await hooks.set_state(t.trade_id, "HALTED", reason)
        await alert("trade_halted_on_boot",
                    {"trade_id": t.trade_id, "symbol": t.symbol, "reason": reason})
        log.error("boot: %s OPEN position mismatch -> HALTED", t.symbol)
        return

    # ----- CLOSING: did the exit complete? Is the position actually gone? -----
    if t.state == "CLOSING":
        if not has_position:
            # Position is flat — exit succeeded. Record any explaining fills.
            exit_fills = _explaining_exit_fills(t, fills_by_client_id)
            for f in exit_fills:
                await hooks.write_fill(f, t.trade_id)
            await hooks.set_state(t.trade_id, "CLOSED", None)
            log.info("boot: %s %s CLOSING -> flat -> CLOSED", t.symbol, t.side.value)
            return
        # Still holding. Is the close order still working?
        # (We look for ANY of this trade's exit orders still live at the venue.)
        live_exit = _live_exit_order(t, orders_by_client_id)
        if live_exit:
            # Close order still working — resume the escalation ladder/exit rule.
            await hooks.resume_exit(t)
            log.info("boot: %s %s CLOSING, close order still working -> resume",
                     t.symbol, t.side.value)
            return
        # Holding, intent is CLOSING, but no live close order — it vanished
        # (cancelled by venue, or never landed). Intent is ONE-WAY: re-issue the
        # exit. This is NOT a re-decision; we committed to flat and we see it
        # through.
        await hooks.reissue_exit(t)
        log.info("boot: %s %s CLOSING, close order gone -> reissue exit",
                 t.symbol, t.side.value)
        return

    # Any other state (CLOSED/REJECTED/HALTED) is terminal/parked and should not
    # appear in load_nonterminal_trades; if it does, leave it untouched.
    log.warning("boot: trade %s in unexpected state %s during reconcile",
                t.trade_id, t.state)


# ---------------------------------------------------------------------------
# Small pure helpers. Kept here and side-effect-free so the diff logic above
# reads as the matrix it is.
# ---------------------------------------------------------------------------
def _position_matches(t: DbTrade, pos: VenuePosition, cfg: BootConfig) -> bool:
    """True if the venue position agrees with our open_qty in sign and size
    (within epsilon)."""
    return abs(pos.qty - t.open_qty) <= cfg.qty_match_epsilon


def _index_fills_by_order(
    fills: List[Fill], orders: List[VenueOrder],
) -> Dict[str, List[Fill]]:
    """Group recent venue fills by the client_order_id of the order they belong
    to. The venue reports fills against venue_order_ids; we map back through the
    open-orders list. Fills whose order is not in the snapshot are still usable
    by venue_exec_id at the executor, but for boot we index what we can map.
    """
    venue_to_client = {o.venue_order_id: o.client_order_id for o in orders}
    out: Dict[str, List[Fill]] = {}
    for f in fills:
        # Prefer the client_order_id the adapter stamps on the fill; fall back to
        # the venue_order_id -> client_order_id bridge from the open-orders
        # snapshot. Either way the index key is OUR deterministic client id, which
        # is what the attribution helpers below parse to reach the trade. A fill
        # we can map to neither is left out of this per-order index (the executor
        # still dedupes it by venue_exec_id).
        cid = f.client_order_id
        if cid is None and f.venue_order_id is not None:
            cid = venue_to_client.get(f.venue_order_id)
        if cid:
            out.setdefault(cid, []).append(f)
    return out


def _is_exit_order_of(client_order_id: Optional[str], t: DbTrade) -> bool:
    """True iff `client_order_id` is one of ours (parses as cv1) AND belongs to
    this exact trade's EXIT ladder. This is what makes attribution EXACT rather
    than best-effort: the cv1 id encodes (trade_id, intent, rung), so we match on
    trade_id — never on symbol alone, which would sweep in another trade's fills
    on the same instrument. A foreign or unparseable id is not ours -> False."""
    if not client_order_id or not coid.is_ours(client_order_id):
        return False
    parsed = coid.parse(client_order_id)
    return parsed.trade_id == t.trade_id and parsed.intent == OrderIntent.EXIT


def _explaining_exit_fills(
    t: DbTrade, fills_by_client_id: Dict[str, List[Fill]],
) -> List[Fill]:
    """Exit fills (within the lookback window) that account for THIS trade's
    position having closed while we were down. Attributed strictly by parsing the
    cv1 client_order_id back to this trade's exit orders — a fill under any other
    trade's id (even on the same symbol) is NOT returned, so we never credit one
    trade's close with another's fills."""
    out: List[Fill] = []
    for cid, fs in fills_by_client_id.items():
        if _is_exit_order_of(cid, t):
            out.extend(fs)
    return out


def _live_exit_order(
    t: DbTrade, orders_by_client_id: Dict[str, VenueOrder],
) -> Optional[VenueOrder]:
    """This trade's still-working EXIT order, if any. Matched by parsing the
    deterministic cv1 client id back to (trade_id, EXIT) — NOT by symbol+status,
    which on a shared account would return a different trade's exit order on the
    same instrument and mask a genuinely-vanished close."""
    for cid, o in orders_by_client_id.items():
        if (_is_exit_order_of(cid, t)
                and o.status in (OrderStatus.WORKING, OrderStatus.PARTIALLY_FILLED)):
            return o
    return None