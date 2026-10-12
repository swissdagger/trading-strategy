# confirmed_15m — strategy rules (as implemented)

**Code:** `strategy/confirmed_15m_strategy.py` (rules), `strategy/confirmed_15m_adapter.py` (wiring), `strategy/chain_history.py` (the history view the rules query).
**Tests:** `tests/test_confirmed_15m_strategy.py`.
**Config:** `config.STRATEGIES["confirmed_15m"]`, ladder `FC`.

This document describes what the code **does**, written from the code. The original specification (`trade_strategy_2.md`, cited by the code since 2026-08-13) is not in the repository; where the implementation has a consequence that may not match the original intent, it is listed under [Behaviours to confirm](#behaviours-to-confirm).

**History:** called `four_chain` until ADR-001 Phase 2. It implemented a four-chain pattern (3m trigger) until 2026-08-13, when the rules below replaced it. From then until the Phase 2 fix the entry never fired, because it waited for an event type (`level_reached`) that nothing emits.

---

## Ladder

| Level | 1 | 2 | 3 | 4 | 5 |
|---|---|---|---|---|---|
| Timeframe | 1m | 3m | 5m | 15m | 1h |

Universe: every configured crypto symbol (`config.crypto_symbols()`). Bars are Coinbase spot candles; chains come from the FC-ladder chain machine.

## Chain history

Every FC-ladder event for a symbol is folded into a per-symbol history **before** the entry check runs. For each chain it keeps:

- **direction** (+1 / −1) and **start** (`first_seen`: the time of the first event seen, which is the chain's 1m initiation);
- **peak level** and the time each level was **first** reached;
- whether it has **terminated** (a terminated chain is kept, never aged out by time).

A `chain_terminated` event marks the chain terminated without changing its peak. Memory is bounded only by a count cap: above 2,000 chains per symbol the oldest by start are dropped.

A **failure** is an *opposing* chain that **terminated without reaching 3m** (peak level 1: it initiated on 1m and died). A chain still alive at 1m is not a failure.

## Entry

Evaluated on every FC-ladder event. All steps must pass; otherwise no entry (each veto is counted, and a funnel summary is logged every 25 triggers).

1. **Trigger.** A `chain_propagated` event at level 4 (15m). The propagating chain is **Chain 2**; its direction `d` is the trade direction. Termination events never trigger, even at level 4 (a `chain_terminated` event carries the level the chain died on).

2. **Initiation — find Chain 1.** Candidates are chains that:
   - have direction `d` and are not Chain 2;
   - reached 15m;
   - started at or before Chain 2 started.

   They are checked **newest start first**. The first candidate that passes all of the following is Chain 1:
   - it has a recorded 15m time;
   - Chain 2 reached 15m **at or after** the candidate did;
   - at least one **failure** (opposing, terminated at 1m) **started** within `[candidate start, Chain 2 start]`, both ends inclusive.

   No candidate passes → veto `no_chain_1_pattern`.

3. **Confirmation.** At least one chain must exist that:
   - has direction `d` and is not Chain 2;
   - has **terminated**;
   - **started strictly before** Chain 2;
   - reached **1h** (full propagation).

   None → veto `no_closed_full_prop`.

4. **Enter** immediately: an ENTER signal on **Chain 2's id**, LONG if `d = +1`, SHORT if `d = −1`.
   - `decision_price` = the triggering event's price (the 15m bar's close);
   - `decided_at` = the triggering event's timestamp, which is the 15m **bar's window start** (market time, not wall time);
   - `trigger_level` = 4;
   - position key = `{chain2_id}:L` or `{chain2_id}:S`.

## Exit

Evaluated for every open position on a symbol, on every event for that symbol. The first rule that matches emits the EXIT:

1. **TARGET** — Chain 2 has reached 1h.
2. **STOP** — any **open** (not terminated) **opposing** chain has reached 5m.

The EXIT carries the triggering event's price and timestamp. There is no time stop, no price stop and no partial exit.

## Execution (ADR-001)

- The account is UK retail spot: **SHORT entries are never sent to the venue.** The executor routes them to the shadow book (`shadow_trades`), which records a virtual round trip at the touch, net of taker fees on both legs.
- LONG entries are sized at 5% of available equity, subject to `MAX_OPEN_POSITIONS`, the kill switch and venue minimums.
- The executor skips an ENTER **written** (`trade_signals.created_at`) more than `ENTER_MAX_AGE_SECONDS` ago. It is deliberately not measured from `decided_at`, which for this strategy is always 15+ minutes back.

## Behaviours to confirm

These follow from the code as written. They may or may not match the original specification.

1. **Chain 1 and the confirmation chain are usually the same chain.** A chain that reached 1h passed 15m earlier than Chain 2 did, so it is normally also a valid Chain 1 whenever a failure started after it began. In practice the pattern reduces to: *a finished same-direction chain that reached 1h, then an opposing chain dying at 1m, then Chain 2 reaching 15m.* If the spec required two distinct chains, the code does not enforce it.
2. **No time bound on the confirmation chain.** Any finished full-propagation chain from anywhere in the retained history (up to 2,000 chains per symbol) qualifies, however old.
3. **The failure only has to start inside the window.** When it terminated does not matter.
4. **Chain 1 is chosen newest-start first**, so a more recent qualifying chain is preferred over an older one.
5. **TARGET depends on Chain 2 still being in the history.** If Chain 2 is evicted by the count cap, only STOP can close the position.
6. **STOP looks at open opposing chains only.** An opposing chain that reached 5m and has already terminated does not stop the trade.
7. **Exits are evaluated on any event for the symbol**, not only on Chain 2's events, so an exit can fire on an unrelated chain's bar.
