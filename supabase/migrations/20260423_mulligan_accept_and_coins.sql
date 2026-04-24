-- =====================================================================
-- Migration: 20260423_mulligan_accept_and_coins.sql
-- 1. Add mulligan_streak_at_loss column to players (tracks streak before loss)
-- 2. Create accept_mulligan() RPC — immediately accepts mulligan, no admin review
-- 3. Patch self_apply_pick_result to set mulligan_streak_at_loss + deduct coin
-- 4. Patch apply_missed_pick to deduct coin for no-pick
-- =====================================================================

-- ── 1. Add missing column ──────────────────────────────────────────
ALTER TABLE public.players
  ADD COLUMN IF NOT EXISTS mulligan_streak_at_loss integer DEFAULT 0;

-- ── 2. accept_mulligan RPC ─────────────────────────────────────────
-- Called by the client from the No Hit overlay when the user taps
-- "Use Mulligan". Validates eligibility server-side then immediately
-- restores streak and marks the mulligan as consumed.
CREATE OR REPLACE FUNCTION public.accept_mulligan(p_streak integer DEFAULT 0)
RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_username text;
  v_player   players%ROWTYPE;
  v_restored integer;
BEGIN
  -- Resolve username from JWT (same pattern as other BSH RPCs)
  v_username := (auth.jwt() ->> 'username');
  IF v_username IS NULL OR v_username = '' THEN
    RETURN json_build_object('ok', false, 'error', 'not_authenticated');
  END IF;

  SELECT * INTO v_player FROM players WHERE username = v_username;
  IF NOT FOUND THEN
    RETURN json_build_object('ok', false, 'error', 'player_not_found');
  END IF;

  -- Guard: must be eligible and not already used
  IF NOT COALESCE(v_player.mulligan_eligible, false) THEN
    RETURN json_build_object('ok', false, 'error', 'not_eligible');
  END IF;
  IF COALESCE(v_player.mulligan_used, false) THEN
    RETURN json_build_object('ok', false, 'error', 'already_used');
  END IF;

  -- Determine the streak to restore:
  -- Prefer the stored mulligan_streak_at_loss; fall back to client-supplied p_streak.
  -- Clamp to 10–29 to prevent abuse.
  v_restored := COALESCE(
    NULLIF(v_player.mulligan_streak_at_loss, 0),
    p_streak
  );
  -- Validate the streak was in the eligible window
  IF v_restored < 10 OR v_restored > 29 THEN
    RETURN json_build_object('ok', false, 'error', 'not_eligible');
  END IF;

  -- Apply: restore streak, mark mulligan consumed
  UPDATE players SET
    streak            = v_restored,
    mulligan_used     = true,
    mulligan_eligible = false,
    status            = CASE WHEN coins > 0 THEN 'active' ELSE 'locked' END
  WHERE username = v_username;

  RETURN json_build_object('ok', true, 'streak', v_restored);
END;
$$;

GRANT EXECUTE ON FUNCTION public.accept_mulligan(integer) TO authenticated;

-- ── 3. Ensure self_apply_pick_result sets mulligan_streak_at_loss ──
-- (Add this UPDATE inside the no_hit branch of self_apply_pick_result)
-- NOTE: Amend the existing self_apply_pick_result function to store the
-- streak-before-loss in mulligan_streak_at_loss when it resets the streak.
-- Example addition inside the no_hit branch:
--
--   UPDATE players SET
--     mulligan_streak_at_loss = CASE WHEN mulligan_eligible THEN streak ELSE mulligan_streak_at_loss END,
--     streak = 0,
--     coins  = GREATEST(0, coins - 1),   -- deduct 1 coin for no_hit (min 0)
--     ...
--   WHERE username = v_username;
--
-- ── 4. Ensure apply_missed_pick deducts 1 coin for no-pick ─────────
-- Add inside apply_missed_pick:
--   coins = GREATEST(0, coins - 1)
--
-- Both RPCs' exact bodies depend on your current implementation.
-- Apply GREATEST(0, coins - 1) wherever they UPDATE the players row.
