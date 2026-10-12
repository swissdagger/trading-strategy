-- ============================================================================
-- 015_seed_execution_caps.sql
--
-- Seeds execution_caps so the executor's pricing path has caps to read. Nothing
-- in migrations 001–014 ever populates this table, and make_get_cap /
-- make_get_exit_cap RAISE on a missing (asset_class, intent) row — so without
-- these rows the executor cannot price a single order once it starts. This
-- migration removes that gap and makes it survive a DB wipe + replay.
--
-- ⚠️  THE VALUES BELOW ARE STARTING PLACEHOLDERS, NOT TUNED PARAMETERS.
-- execution_caps is deliberately hot-reloadable (read at order time, not baked
-- in) precisely because the right caps are discovered during the first weeks of
-- paper trading, per instrument. Treat these as a conservative starting point:
-- run paper, watch realized fill price vs. the touch, and UPDATE these rows
-- (no redeploy needed) — widen the EXIT caps if protective exits miss in fast
-- markets, tighten if you're paying away more than necessary.
--
-- UNITS (all multipliers are in units of the current bid-ask SPREAD; see
-- pricing.price_marketable_limit):
--   cross_spread_mult — how far THROUGH the touch we place the limit so it fills
--                       immediately (1.0 = one full spread past the touch).
--   cap_spread_mult   — worst price tolerated, as a spread multiple. The limit
--                       is the TIGHTER of (crossed) and (this cap edge).
--   cap_abs_bps       — hard absolute ceiling in basis points, independent of
--                       spread, guarding a pathologically wide spread. NULL = none.
--   escalation_ladder — EXIT only. Ordered JSONB array of
--                       {"widen_to_spread_mult": N, "window_seconds": S}. After
--                       the last rung the executor HALTs + alerts rather than
--                       chasing a fill forever. ENTRY rows use [] (no escalation
--                       — an entry that misses is just re-signalled).
--
-- DESIGN POSTURE: entries are TIGHT (missing is cheap — you re-signal); exits,
-- especially protective ones, are LOOSER and ESCALATE (staying stuck in a
-- position is worse than paying a little more to get flat).
--
-- COVERAGE: one ENTRY and one EXIT row per asset class the executor will trade.
-- ADR-001: crypto only (Coinbase Advanced Trade spot). equity and fx are kept
-- commented-out as reference values for when equities return. Edited in place
-- rather than superseded because ADR-001 Phase 2 starts from a fresh DB; on an
-- existing DB that already holds equity rows, they are inert (no equity symbols
-- are configured) and can be deleted by hand.
--
-- Idempotent: ON CONFLICT (asset_class, intent) DO UPDATE, so replaying this
-- migration re-asserts the seed values without erroring on an existing row.
-- (If you've hand-tuned a row in paper, re-running this WILL overwrite it back
-- to these placeholders — tune via your own UPDATEs, or edit this file, rather
-- than relying on a replay to preserve tuning.)
-- ============================================================================

INSERT INTO execution_caps
    (asset_class, intent, cross_spread_mult, cap_spread_mult, cap_abs_bps,
     escalation_ladder, enabled)
VALUES
    -- ---- crypto (Coinbase spot) ---------------------------------------------
    -- Crypto spreads are usually tight on majors but blow out fast in volatility,
    -- so the absolute bps backstop matters here.
    ('crypto', 'ENTRY', 1.0, 2.0, 20, '[]'::jsonb, true),
    ('crypto', 'EXIT',  1.5, 3.0, 50,
        '[{"widen_to_spread_mult": 4.0, "window_seconds": 10},
          {"widen_to_spread_mult": 6.0, "window_seconds": 15}]'::jsonb,
        true)

    -- ---- equity — uncomment when equities are enabled in config.py ---------
    -- Liquid US equities are often one tick wide; the spread-multiple approach
    -- keeps entries tight there while still self-scaling on wider names.
    -- ,('equity', 'ENTRY', 1.0, 2.0, 15, '[]'::jsonb, true),
    -- ('equity', 'EXIT',  1.5, 3.0, 40,
    --     '[{"widen_to_spread_mult": 4.0, "window_seconds": 15},
    --       {"widen_to_spread_mult": 6.0, "window_seconds": 20}]'::jsonb,
    --     true)

    -- ---- fx — uncomment when fx is enabled in config.py ---------------------
    -- ,('fx', 'ENTRY', 1.0, 2.0, 8, '[]'::jsonb, true),
    -- ('fx', 'EXIT',  1.5, 3.0, 25,
    --     '[{"widen_to_spread_mult": 4.0, "window_seconds": 15},
    --       {"widen_to_spread_mult": 6.0, "window_seconds": 20}]'::jsonb,
    --     true)

ON CONFLICT (asset_class, intent) DO UPDATE SET
    cross_spread_mult = EXCLUDED.cross_spread_mult,
    cap_spread_mult   = EXCLUDED.cap_spread_mult,
    cap_abs_bps       = EXCLUDED.cap_abs_bps,
    escalation_ladder = EXCLUDED.escalation_ladder,
    enabled           = EXCLUDED.enabled,
    updated_at        = now();

-- Verify after applying:
--   SELECT asset_class, intent, cross_spread_mult, cap_spread_mult, cap_abs_bps,
--          jsonb_array_length(escalation_ladder) AS rungs, enabled
--   FROM execution_caps ORDER BY asset_class, intent;
-- Expect: one ENTRY (0 rungs) and one EXIT (>=1 rung) per traded class, all enabled.
