-- ============================================================================
-- 003_lockdown_rls.sql
--
-- This is a HEADLESS backend. Nothing should ever reach these tables through
-- Supabase's auto-generated public PostgREST / anon / realtime API. The trading
-- services connect as service_role (or the Postgres superuser via a direct
-- connection string), and service_role BYPASSES RLS — so enabling RLS with NO
-- policies makes the public API deny-all while leaving the backend completely
-- unaffected.
--
-- This is a LOCKDOWN, not an access model. We are not writing policies to grant
-- access; we are enabling RLS precisely to grant NOTHING through the public
-- path. Supabase exposes every public-schema table through PostgREST by
-- default, and the anon key is designed to be shippable in client code — so a
-- leaked anon key against an RLS-off table would expose the entire trade
-- history, positions, and P&L. RLS-deny-all closes that door; the service-role
-- backend sails past it.
--
-- Apply after 001 and 002.
--
-- NOTE: if the trading services are ever switched to a DEDICATED non-superuser
-- Postgres role (least-privilege, better than using service_role for
-- everything), RLS WOULD then apply to that role and this file must be
-- accompanied by explicit GRANTs / policies for it. For v1 (service_role),
-- deny-all is the correct, simplest posture.
-- ============================================================================

ALTER TABLE trades          ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders          ENABLE ROW LEVEL SECURITY;
ALTER TABLE fills           ENABLE ROW LEVEL SECURITY;
ALTER TABLE exit_state      ENABLE ROW LEVEL SECURITY;
ALTER TABLE execution_caps  ENABLE ROW LEVEL SECURITY;
ALTER TABLE kill_switch     ENABLE ROW LEVEL SECURITY;
ALTER TABLE signals         ENABLE ROW LEVEL SECURITY;
ALTER TABLE chains          ENABLE ROW LEVEL SECURITY;

-- No policies are created, so anon/authenticated get zero rows on every table.
-- service_role bypasses RLS and is unaffected.

-- Defense in depth: revoke the table privileges PostgREST relies on, so the
-- tables aren't even exposed through the public API surface in the first place.
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM anon, authenticated;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM anon, authenticated;
