-- ============================================================================
-- 001_trade_lifecycle.sql
--
-- The trading system's core schema. This is the DURABLE CONTRACT that the
-- executor, the exit component, and the boot-time reconcile path all read and
-- write. It lives in a SEPARATE Supabase project from CausaView — this system
-- learns about chains only from the causal-events Kafka stream, never by
-- reading CausaView's DB. There is intentionally no `chains`, `signals`, or
-- `prices` table here; chain identity arrives as an opaque string on the wire.
--
-- Design decisions encoded here (settled in design discussion):
--   * Exit is UNTIED from chain termination — a separate exit component owns
--     OPEN -> CLOSING. chain_terminated is just one possible input to that
--     component, never a direct edge to a close order.
--   * RESUME-MANAGEMENT on restart — reconcile re-adopts OPEN positions and
--     the exit component resumes governing them, so every column the exit rule
--     needs to resume must be persisted (see exit_state).
--   * NO RE-DECISION — once CLOSING, intent is one-way (we WILL get flat). The
--     working close order's PRICE may be amended, but CLOSING never returns to
--     OPEN. This removes an un-disambiguable crash-window state.
--   * ROBUST PARTIAL FILLS — fills are individual rows, never a single
--     fill_price column. Trade-level entry/exit prices are COMPUTED as
--     quantity-weighted averages over fills. Any nonzero filled qty means the
--     position is live and the exit component owns it.
--   * MARKETABLE LIMITS BOTH SIDES with per-class, per-side basis caps, priced
--     off the VENUE's current touch (IBKR snapshot for equity/fx; Binance live
--     book for crypto). Caps are hot-reloadable config, not baked in.
--   * ESCALATION LADDER on protective exits — a capped exit that doesn't fill
--     within its window escalates (widen cap, ... , finally HALT+alert) rather
--     than silently stranding or blindly market-ing out.
--
-- Conventions mirror CausaView's schema: chain_id is a uuid-as-text (it is
-- str(uuid.uuid4()) on the producer side), prices are double precision,
-- timestamps are timestamptz, asset_class is text matching types.ts
-- AssetClass ('crypto' | 'equity' | 'fx'), JSONB for evolving substructure.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- ENUM: trade_state
--
-- The lifecycle. Each value answers "what is unambiguously true right now",
-- which is exactly what resume-management reads on boot. See the state diagram
-- in the design notes. The two "in-flight at venue" states (PENDING, CLOSING)
-- are the dangerous ones on restart — resolved by querying the venue with the
-- deterministic client_order_id.
-- ----------------------------------------------------------------------------
CREATE TYPE trade_state AS ENUM (
    'PENDING',    -- entry decided, order not yet confirmed at venue. AMBIGUOUS
                  -- on restart: query venue by client_order_id to learn if it
                  -- landed (-> SUBMITTED) or never did (-> retry / REJECTED).
    'SUBMITTED',  -- entry order working at venue, acked, not (fully) filled.
    'OPEN',       -- entry (partly or fully) filled; position is LIVE. The exit
                  -- component owns it. Note: OPEN means "we hold a position",
                  -- NOT "entry is complete" — a partial entry can still have a
                  -- working remainder (see entry_filled_qty vs entry_target_qty).
    'CLOSING',    -- exit decided, close order working. Intent is one-way. On
                  -- restart: confirm the close at the venue or re-issue it.
    'CLOSED',     -- position flat, exit filled. Terminal. Carries realized P&L.
    'REJECTED',   -- venue refused the ENTRY order (margin, bad contract, market
                  -- closed). Terminal, and DISTINCT from CLOSED: no position was
                  -- ever held, so exit logic / position reconcile must skip it.
    'HALTED'      -- reconcile (or an exit-ladder escalation) could not safely
                  -- resolve this trade. A human must look. The executor refuses
                  -- to act on a HALTED trade. This is the real-money "I don't
                  -- understand this, stop" state — preferred over a clever guess.
);


-- ----------------------------------------------------------------------------
-- ENUM: trade_side
-- Direction of the POSITION. Mirrors ChainDirection (-1 | 1, never 0 — a chain
-- is directional once initiated) but named in trading terms.
-- ----------------------------------------------------------------------------
CREATE TYPE trade_side AS ENUM ('LONG', 'SHORT');


-- ----------------------------------------------------------------------------
-- ENUM: order_intent
-- What an order row is trying to do. ENTRY acquires the position; EXIT reduces
-- or flattens it. Used on the orders table so reconcile can tell, for a given
-- working order at the venue, whether it belongs to the PENDING/SUBMITTED entry
-- or the CLOSING exit.
-- ----------------------------------------------------------------------------
CREATE TYPE order_intent AS ENUM ('ENTRY', 'EXIT');


-- ----------------------------------------------------------------------------
-- ENUM: order_status
-- Lifecycle of a single venue order. Distinct from trade_state: one trade can
-- have several orders over its life (entry, then exit, plus any escalation
-- re-issues). PARTIALLY_FILLED is a first-class status because partials are
-- modeled, not treated as exceptions.
-- ----------------------------------------------------------------------------
CREATE TYPE order_status AS ENUM (
    'WORKING',           -- live at venue, no fills yet
    'PARTIALLY_FILLED',  -- some quantity executed, remainder still working
    'FILLED',            -- fully executed
    'CANCELLED',         -- cancelled (by us, e.g. escalation re-price, or venue)
    'REJECTED'           -- venue refused this specific order
);


-- ----------------------------------------------------------------------------
-- ENUM: exit_reason
-- WHY an exit fired. Flows from the exit component -> execution adapter because
-- the reason selects the cap AND the escalation window: a protective STOP wants
-- a looser cap and a short, escalating window (get flat); a TARGET wants a
-- resting-ish marketable limit, no urgency, partials welcome. MANUAL and
-- KILL_SWITCH are operator-initiated flattens.
-- ----------------------------------------------------------------------------
CREATE TYPE exit_reason AS ENUM (
    'STOP',         -- risk-driven: position went against us past threshold.
    'TARGET',       -- profit-taking at a favorable level. No urgency.
    'TIME',         -- time-based exit (max holding period).
    'MANUAL',       -- operator flattened this trade.
    'KILL_SWITCH'   -- global halt flattened everything.
);


-- ----------------------------------------------------------------------------
-- TABLE: trades
--
-- One row per trade. The trade key is (chain_id, side): a single chain in a
-- single direction can open at most one position. This is enforced as a UNIQUE
-- constraint so that a Kafka replay, a Service-2-equivalent restart re-hydration,
-- or a duplicate event CANNOT open a second position even if every in-memory
-- guard fails. In-memory dedupe is not enough with real money — the DB is the
-- backstop. (Mirrors the deterministic-event-ID dedupe the frontend already
-- relies on, applied to capital.)
-- ----------------------------------------------------------------------------
CREATE TABLE trades (
    trade_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    -- --- Chain provenance (the "why did this open" audit trail) ---
    -- chain_id is opaque here: it is the str(uuid4) that arrived on the
    -- causal-events stream. We do NOT foreign-key it (the chains table lives in
    -- the other DB); we store it for audit and for the (chain_id, side) key.
    chain_id          text         NOT NULL,
    symbol            text         NOT NULL,
    asset_class       text         NOT NULL,   -- 'crypto' | 'equity' | 'fx'
    side              trade_side   NOT NULL,

    -- Propagation level at which we entered (the entry trigger is still being
    -- finalized; storing the level keeps the decision auditable regardless of
    -- which level we end up triggering on).
    entry_trigger_level  integer,

    -- --- State ---
    state             trade_state  NOT NULL DEFAULT 'PENDING',

    -- --- Deterministic client order id for the ENTRY ---
    -- Derived from the trade key, NOT random. This is the bridge across a crash:
    -- a PENDING trade on restart is resolved by asking the venue about THIS id.
    -- One per trade for the entry; exit/escalation orders derive their own ids
    -- (see orders.client_order_id) but all descend deterministically from this.
    entry_client_order_id  text     NOT NULL,

    -- --- Quantities (partial-fill aware) ---
    -- target is what we intend to hold; filled is the running sum from `fills`
    -- (kept here as a denormalized cache for fast reads, authoritative source is
    -- the fills table). open_qty = entry_filled - exit_filled; when it hits 0
    -- from a nonzero peak, the trade is flat.
    entry_target_qty  numeric(38, 18) NOT NULL,
    entry_filled_qty  numeric(38, 18) NOT NULL DEFAULT 0,
    exit_filled_qty   numeric(38, 18) NOT NULL DEFAULT 0,

    -- --- Decision vs fill prices (BASIS MEASUREMENT) ---
    -- decision_price is the price our SIGNAL saw (Polygon bar from the chain
    -- event, or Binance book for crypto) at entry. The realized entry/exit fill
    -- prices are COMPUTED from `fills` (qty-weighted avg), never stored here as
    -- single values — that would lie under partial fills. We persist the
    -- decision price so basis = realized_entry_fill - decision_price is a number
    -- we watch per trade, not a worry.
    entry_decision_price   double precision,
    -- Optional venue reference read at order entry (the IBKR snapshot, costs ~1c)
    -- used to price the marketable limit. Stored for post-hoc basis forensics.
    entry_venue_reference  double precision,

    -- --- HALTED forensics ---
    -- When state = HALTED, this says why, in machine-greppable form, so alerts
    -- and the operator can triage without reading logs. NULL unless HALTED.
    halt_reason       text,

    -- --- Timestamps ---
    created_at        timestamptz  NOT NULL DEFAULT now(),
    opened_at         timestamptz,   -- first entry fill
    closed_at         timestamptz,   -- flat (terminal CLOSED) or terminal REJECTED
    last_updated      timestamptz  NOT NULL DEFAULT now(),

    -- The idempotency backstop. A given chain, in a given direction, opens at
    -- most one trade — ever. Re-entry after a close is a NEW chain_id, so this
    -- does not block legitimate re-entries (those carry a different chain_id).
    CONSTRAINT uq_trades_chain_side UNIQUE (chain_id, side),

    -- A halt_reason is present exactly when HALTED. Keeps the forensic field
    -- honest — no silent halts, no stale halt_reason on a recovered trade.
    CONSTRAINT ck_trades_halt_reason
        CHECK ((state = 'HALTED') = (halt_reason IS NOT NULL)),

    -- Exit can never exceed entry: you can't close more than you hold.
    CONSTRAINT ck_trades_exit_le_entry
        CHECK (exit_filled_qty <= entry_filled_qty)
);

-- Hot path: reconcile-on-boot and the exit component both scan "what is not
-- terminal". Partial index keeps it tiny — terminal trades (the vast majority
-- over time) are excluded.
CREATE INDEX ix_trades_active
    ON trades (state)
    WHERE state NOT IN ('CLOSED', 'REJECTED');

-- Look up a trade by its entry client order id during reconcile (venue tells us
-- a client id filled; we map back to the trade).
CREATE UNIQUE INDEX ix_trades_entry_client_order_id
    ON trades (entry_client_order_id);

CREATE INDEX ix_trades_symbol      ON trades (symbol);
CREATE INDEX ix_trades_chain_id    ON trades (chain_id);


-- ----------------------------------------------------------------------------
-- TABLE: orders
--
-- One row per ORDER SENT TO THE VENUE. A trade has at least one (the entry) and
-- usually more (the exit, plus any escalation re-issues, each a fresh order with
-- its own deterministic client_order_id). Distinct from `fills`: an order can
-- produce zero, one, or many fills. Distinct from `trades.state`: this tracks a
-- single order's life, not the position's.
--
-- This table is what reconcile diffs against the venue's open-orders list. The
-- deterministic client_order_id is the join key across a crash.
-- ----------------------------------------------------------------------------
CREATE TABLE orders (
    order_id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    trade_id          uuid NOT NULL REFERENCES trades(trade_id),

    intent            order_intent NOT NULL,   -- ENTRY | EXIT
    side              trade_side   NOT NULL,    -- buy/sell direction of THIS order
    status            order_status NOT NULL DEFAULT 'WORKING',

    -- Deterministic, venue-submitted. IBKR: orderRef/clientId; Binance:
    -- newClientOrderId. Globally unique — this is THE crash-recovery key.
    client_order_id   text         NOT NULL,
    -- The venue's own id, learned on ack. Null in the crash-window between
    -- submit and ack — which is exactly why we cannot rely on it for recovery
    -- and must use client_order_id instead.
    venue_order_id    text,

    -- --- Marketable-limit pricing (the order-flow contract) ---
    -- All our orders are marketable (crossing) limits with a basis cap. We store
    -- the cap we used and the reference we priced against, so a fill that lands
    -- at the cap edge is explainable after the fact.
    limit_price       double precision NOT NULL,  -- the actual limit sent
    venue_reference   double precision,            -- touch we priced off of
    cap_applied       double precision,            -- the basis cap distance used

    -- For EXIT orders: why, and where on the escalation ladder this order sits.
    -- NULL for ENTRY orders. exit_reason selects cap + window; escalation_rung
    -- counts re-issues (0 = first attempt, 1 = first widening, ...).
    exit_reason       exit_reason,
    escalation_rung   integer      NOT NULL DEFAULT 0,

    target_qty        numeric(38, 18) NOT NULL,
    filled_qty        numeric(38, 18) NOT NULL DEFAULT 0,

    created_at        timestamptz  NOT NULL DEFAULT now(),
    acked_at          timestamptz,   -- venue acknowledged (venue_order_id known)
    terminal_at       timestamptz,   -- FILLED / CANCELLED / REJECTED reached
    last_updated      timestamptz  NOT NULL DEFAULT now(),

    CONSTRAINT uq_orders_client_order_id UNIQUE (client_order_id),
    CONSTRAINT ck_orders_filled_le_target CHECK (filled_qty <= target_qty),
    -- exit_reason present iff this is an EXIT order.
    CONSTRAINT ck_orders_exit_reason
        CHECK ((intent = 'EXIT') = (exit_reason IS NOT NULL))
);

CREATE INDEX ix_orders_trade_id ON orders (trade_id);
-- Reconcile scans non-terminal orders to diff against the venue.
CREATE INDEX ix_orders_open
    ON orders (status)
    WHERE status IN ('WORKING', 'PARTIALLY_FILLED');


-- ----------------------------------------------------------------------------
-- TABLE: fills
--
-- One row per EXECUTION reported by the venue. THIS is the authoritative record
-- of what actually happened to your capital. Trade-level entry/exit prices are
-- computed from here (qty-weighted average), never stored as scalars on trades —
-- because under partial fills a single fill_price is a lie.
--
-- A fill belongs to an order (which belongs to a trade). We denormalize trade_id
-- for fast per-trade aggregation without a join through orders.
-- ----------------------------------------------------------------------------
CREATE TABLE fills (
    fill_id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id          uuid NOT NULL REFERENCES orders(order_id),
    trade_id          uuid NOT NULL REFERENCES trades(trade_id),

    intent            order_intent NOT NULL,   -- ENTRY | EXIT (denormalized)
    qty               numeric(38, 18)  NOT NULL,
    price             double precision NOT NULL,
    -- Venue's own execution id. UNIQUE so the same execution, if reported twice
    -- (venue resend, reconcile re-read), is idempotently deduped — never
    -- double-counted into filled_qty.
    venue_exec_id     text         NOT NULL,

    -- Fees/commissions for honest P&L and honest basis. Stored per-fill because
    -- venues report them per-execution.
    fee               double precision NOT NULL DEFAULT 0,
    fee_currency      text,

    filled_at         timestamptz  NOT NULL,
    recorded_at       timestamptz  NOT NULL DEFAULT now(),

    CONSTRAINT uq_fills_venue_exec_id UNIQUE (venue_exec_id),
    CONSTRAINT ck_fills_qty_positive CHECK (qty > 0)
);

CREATE INDEX ix_fills_trade_id ON fills (trade_id);
CREATE INDEX ix_fills_order_id ON fills (order_id);


-- ----------------------------------------------------------------------------
-- TABLE: exit_state
--
-- The exit component's PRIVATE running state, one row per trade. Kept separate
-- from `trades` so write-authority is clean: the executor writes trades
-- (entry side), the exit component writes exit_state, and they never contend —
-- the same separation-of-owners discipline the chain state machine uses with
-- its per-symbol lock.
--
-- This is what RESUME-MANAGEMENT reads on restart to re-adopt an OPEN position
-- WITHOUT the exit rule restarting from a blank slate. If the exit rule trails,
-- the trailing anchor lives here; if it has fixed levels, they live here.
-- Everything the exit rule needs to resume must persist here.
--
-- 1:1 with trades, created when a trade first becomes OPEN.
-- ----------------------------------------------------------------------------
CREATE TABLE exit_state (
    trade_id          uuid PRIMARY KEY REFERENCES trades(trade_id),

    -- Which exit rule governs this trade (lets the rule set evolve without
    -- losing the meaning of an in-flight trade's persisted state).
    exit_rule_id      text         NOT NULL,

    -- Fixed levels, if the rule uses them. Priced in the instrument's units.
    stop_price        double precision,
    target_price      double precision,
    time_exit_at      timestamptz,

    -- Trailing state, if the rule trails. peak_favorable is the best excursion
    -- seen so far (high-water for LONG, low-water for SHORT); the trailing stop
    -- is derived from it. Persisting it is what makes resume-management faithful:
    -- without it, a trail restarts from current price after a crash and behaves
    -- differently than before.
    peak_favorable_price  double precision,

    -- --- Escalation ladder state (for an in-flight protective exit) ---
    -- When a protective exit is working and may need to escalate, this tracks
    -- where on the ladder we are and when the current rung's window expires.
    -- The exit component, on each tick/timer, checks: if state=CLOSING and
    -- now() > escalation_deadline and not filled -> advance the rung (widen cap
    -- / re-issue) or, at the top of the ladder, HALT + alert.
    escalation_rung       integer      NOT NULL DEFAULT 0,
    escalation_deadline   timestamptz,

    -- Free-form rule scratch space for anything the above doesn't capture, so a
    -- new exit rule doesn't require a migration to store its working state.
    rule_state        jsonb        NOT NULL DEFAULT '{}'::jsonb,

    created_at        timestamptz  NOT NULL DEFAULT now(),
    last_updated      timestamptz  NOT NULL DEFAULT now()
);


-- ----------------------------------------------------------------------------
-- TABLE: execution_caps
--
-- The marketable-limit basis caps and escalation-ladder config, per asset class
-- and per side. HOT-RELOADABLE — the executor/exit component read this at order
-- time, not from a baked-in constant — because the first weeks of live trading
-- are about discovering whether caps are right, and you must not need a redeploy
-- between "my exits keep missing in fast markets" and widening the exit cap.
--
-- Caps are expressed as a SPREAD MULTIPLE (cross up to N x current spread) so
-- they self-scale across the three liquidity regimes (one-tick large-cap vs
-- twenty-pip thin cross) where a fixed tick/bps cap would mis-fit. An absolute
-- floor/ceiling in bps is also provided as a hard backstop.
--
-- The escalation ladder is stored as an ordered JSONB array of rungs; each rung
-- widens the cap and sets a window. The final rung's action ('halt') is what
-- converts an unfillable protective exit into HALTED+alert instead of an
-- unbounded market order.
-- ----------------------------------------------------------------------------
CREATE TABLE execution_caps (
    asset_class       text         NOT NULL,   -- 'crypto' | 'equity' | 'fx'
    intent            order_intent NOT NULL,   -- ENTRY | EXIT

    -- Marketable-limit aggression: how far THROUGH the touch we price (as a
    -- spread multiple), and the cap on how far beyond the touch a fill is
    -- tolerable ("not horrible"). entry caps tight, exit caps looser.
    cross_spread_mult     double precision NOT NULL,  -- how aggressively we cross
    cap_spread_mult       double precision NOT NULL,  -- max tolerable beyond touch
    cap_abs_bps           double precision,            -- hard bps backstop, optional

    -- Escalation ladder (EXIT rows, esp. STOP). Ordered array of:
    --   {"widen_to_spread_mult": <num>, "window_seconds": <int>}
    -- with an implicit final action of HALT+alert after the last rung. ENTRY
    -- rows leave this null/empty — entries don't escalate, they just miss.
    escalation_ladder     jsonb        NOT NULL DEFAULT '[]'::jsonb,

    enabled           boolean      NOT NULL DEFAULT true,
    updated_at        timestamptz  NOT NULL DEFAULT now(),

    PRIMARY KEY (asset_class, intent)
);


-- ----------------------------------------------------------------------------
-- TABLE: kill_switch
--
-- Single-row global halt. The executor checks this before EVERY order. When
-- engaged, it consumes events but places NO new entries (and optionally
-- flattens — that's an operator choice at engage time, recorded in mode).
-- A DB row, not an env var, so it can be tripped instantly without a redeploy —
-- you must be able to stop trading faster than you can ship code.
-- ----------------------------------------------------------------------------
CREATE TABLE kill_switch (
    id                boolean PRIMARY KEY DEFAULT true,   -- single row guard
    engaged           boolean      NOT NULL DEFAULT false,
    -- 'halt_new'  = stop opening, leave open positions to their exit rules
    -- 'flatten'   = stop opening AND flatten everything via KILL_SWITCH exits
    mode              text         NOT NULL DEFAULT 'halt_new',
    reason            text,
    engaged_by        text,
    engaged_at        timestamptz,
    updated_at        timestamptz  NOT NULL DEFAULT now(),

    CONSTRAINT ck_kill_switch_singleton CHECK (id = true)
);

INSERT INTO kill_switch (id, engaged, mode) VALUES (true, false, 'halt_new')
    ON CONFLICT (id) DO NOTHING;
