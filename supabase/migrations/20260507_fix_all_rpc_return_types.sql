-- =============================================================
-- Migration: 20260507_fix_all_rpc_return_types.sql
-- Root cause: RPCs returned void (→ null) or SETOF <table> (→ array).
-- Both make resp.ok undefined → every client check fails.
-- Fix: DROP + recreate each function with RETURNS json + {ok,...} shape.
-- =============================================================

-- ── DROP all affected functions ────────────────────────────────────
DROP FUNCTION IF EXISTS public.place_pick(text,text,text,text,text,boolean,integer);
DROP FUNCTION IF EXISTS public.get_my_profile(text,text);
DROP FUNCTION IF EXISTS public.self_clear_stale_pick(text,text);
DROP FUNCTION IF EXISTS public.self_change_pending_pick(text,text,text,text,text);
DROP FUNCTION IF EXISTS public.self_apply_pick_result(text,text,text,text);
DROP FUNCTION IF EXISTS public.apply_missed_pick(text,text,date);
DROP FUNCTION IF EXISTS public.accept_mulligan(text,text,integer);
DROP FUNCTION IF EXISTS public.send_inbox_message(text,text,text,text);
DROP FUNCTION IF EXISTS public.submit_topup(text,text,text,numeric,integer);
DROP FUNCTION IF EXISTS public.submit_topup(text,text,text,integer,integer);
DROP FUNCTION IF EXISTS public.admin_update_player(text,text,text,jsonb);
DROP FUNCTION IF EXISTS public.admin_set_pick_result_for_today(text,text,text,text);
DROP FUNCTION IF EXISTS public.admin_delete_player(text,text,text);
DROP FUNCTION IF EXISTS public.admin_create_player(text,text,text,text,integer,text);
DROP FUNCTION IF EXISTS public.admin_cancel_topup(text,text,text);
DROP FUNCTION IF EXISTS public.admin_mark_topup_paid(text,text,text,text,integer,text);
DROP FUNCTION IF EXISTS public.admin_send_inbox(text,text,text,text);
DROP FUNCTION IF EXISTS public.admin_assign_pick(text,text,text,text,text,text);

-- ── place_pick ─────────────────────────────────────────────────────
CREATE FUNCTION public.place_pick(
  p_username text, p_token text,
  p_batter_name text, p_batter_id text, p_game_pk text,
  p_is_bonus boolean DEFAULT false, p_day_number integer DEFAULT 1
) RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_player players; v_pick pick_history; v_today date;
BEGIN
  SELECT * INTO v_player FROM players WHERE username = p_username AND session_token = p_token;
  IF NOT FOUND THEN RETURN json_build_object('ok',false,'error','invalid_session'); END IF;
  IF v_player.status = 'locked' THEN RETURN json_build_object('ok',false,'error','account_locked'); END IF;
  IF COALESCE(v_player.coins,0) <= 0 AND COALESCE(v_player.streak,0) <= 0 THEN
    RETURN json_build_object('ok',false,'error','no_coins');
  END IF;
  v_today := (NOW() AT TIME ZONE 'America/Los_Angeles')::date;
  BEGIN
    INSERT INTO pick_history (player_username,pick_date,batter_name,batter_id,game_pk,is_bonus,day_number,result)
    VALUES (p_username, v_today, p_batter_name,
            COALESCE(NULLIF(p_batter_id,'')::integer,0),
            COALESCE(NULLIF(p_game_pk,'')::integer,0),
            COALESCE(p_is_bonus,false), COALESCE(p_day_number,1), 'pending')
    RETURNING * INTO v_pick;
  EXCEPTION WHEN unique_violation THEN
    RETURN json_build_object('ok',false,'error','duplicate_pick');
  END;
  IF NOT COALESCE(p_is_bonus,false) THEN
    UPDATE players SET
      today_pick=p_batter_name,
      today_pick_id=COALESCE(NULLIF(p_batter_id,'')::integer,0),
      today_pick_gamepk=COALESCE(NULLIF(p_game_pk,'')::integer,0),
      pick_locked_at=NOW()
    WHERE username=p_username;
  END IF;
  RETURN json_build_object('ok',true,'pick',row_to_json(v_pick));
END; $$;

-- ── get_my_profile ─────────────────────────────────────────────────
CREATE FUNCTION public.get_my_profile(p_username text, p_token text)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE v_player players;
BEGIN
  SELECT * INTO v_player FROM players WHERE username=p_username AND session_token=p_token;
  IF NOT FOUND THEN RETURN json_build_object('ok',false,'error','invalid_session'); END IF;
  RETURN json_build_object('ok',true,'player',row_to_json(v_player));
END; $$;

-- ── self_clear_stale_pick ─────────────────────────────────────────
CREATE FUNCTION public.self_clear_stale_pick(p_username text, p_token text)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE v_player players; v_today date; v_lock_date date;
BEGIN
  SELECT * INTO v_player FROM players WHERE username=p_username AND session_token=p_token;
  IF NOT FOUND THEN RETURN json_build_object('ok',false,'error','invalid_session'); END IF;
  IF v_player.pick_locked_at IS NULL THEN RETURN json_build_object('ok',true,'stale',false); END IF;
  v_today := (NOW() AT TIME ZONE 'America/Los_Angeles')::date;
  v_lock_date := (v_player.pick_locked_at AT TIME ZONE 'America/Los_Angeles')::date;
  IF v_lock_date = v_today THEN RETURN json_build_object('ok',true,'stale',false); END IF;
  UPDATE players SET today_pick=NULL,today_pick_id=NULL,today_pick_gamepk=NULL,pick_locked_at=NULL
  WHERE username=p_username;
  RETURN json_build_object('ok',true,'stale',true);
END; $$;

-- ── self_change_pending_pick ──────────────────────────────────────
CREATE FUNCTION public.self_change_pending_pick(
  p_username text, p_token text,
  p_batter_name text DEFAULT NULL, p_batter_id text DEFAULT NULL, p_game_pk text DEFAULT NULL
) RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE v_today date;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM players WHERE username=p_username AND session_token=p_token) THEN
    RETURN json_build_object('ok',false,'error','invalid_session');
  END IF;
  v_today := (NOW() AT TIME ZONE 'America/Los_Angeles')::date;
  DELETE FROM pick_history
  WHERE player_username=p_username AND pick_date=v_today AND is_bonus=false AND result='pending';
  UPDATE players SET today_pick=NULL,today_pick_id=NULL,today_pick_gamepk=NULL,pick_locked_at=NULL
  WHERE username=p_username;
  RETURN json_build_object('ok',true);
END; $$;

-- ── self_apply_pick_result ────────────────────────────────────────
CREATE FUNCTION public.self_apply_pick_result(
  p_username text, p_token text, p_pick_id text, p_result text
) RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_player players; v_pick pick_history;
  v_new_streak integer; v_new_coins integer; v_mul_elig boolean;
BEGIN
  SELECT * INTO v_player FROM players WHERE username=p_username AND session_token=p_token;
  IF NOT FOUND THEN RETURN json_build_object('ok',false,'error','invalid_session'); END IF;
  SELECT * INTO v_pick FROM pick_history WHERE id=p_pick_id::integer AND player_username=p_username;
  IF NOT FOUND THEN RETURN json_build_object('ok',false,'error','not_your_pick'); END IF;
  IF v_pick.result <> 'pending' THEN RETURN json_build_object('ok',false,'error','already_resolved'); END IF;

  UPDATE pick_history SET result=p_result WHERE id=v_pick.id;

  IF p_result = 'hit' THEN
    v_new_streak := COALESCE(v_player.streak,0) + 1;
    IF v_pick.is_bonus THEN
      UPDATE players SET streak=v_new_streak WHERE username=p_username;
    ELSE
      UPDATE players SET
        streak=v_new_streak, today_pick=NULL, today_pick_id=NULL,
        today_pick_gamepk=NULL, pick_locked_at=NULL,
        mulligan_used = CASE WHEN COALESCE(streak,0)=0 THEN false ELSE mulligan_used END
      WHERE username=p_username;
    END IF;
    RETURN json_build_object('ok',true,'result','hit','streak',v_new_streak,
      'coins',v_player.coins,'mulligan_eligible',v_player.mulligan_eligible);
  END IF;

  IF p_result = 'no_hit' THEN
    v_new_coins := GREATEST(0, COALESCE(v_player.coins,0) - 1);
    v_mul_elig  := NOT COALESCE(v_player.mulligan_used,false)
                   AND COALESCE(v_player.streak,0) >= 10
                   AND COALESCE(v_player.streak,0) <= 29;
    UPDATE players SET
      streak=0, coins=v_new_coins,
      today_pick=NULL, today_pick_id=NULL, today_pick_gamepk=NULL, pick_locked_at=NULL,
      mulligan_eligible=v_mul_elig,
      mulligan_streak_at_loss=COALESCE(v_player.streak,0),
      mulligan_used=false,
      status=CASE WHEN v_new_coins>0 OR v_mul_elig THEN 'active' ELSE 'locked' END
    WHERE username=p_username;
    IF COALESCE(v_player.streak,0) > 0 THEN
      BEGIN
        INSERT INTO leaderboard_archive(nickname,max_streak)
        VALUES(COALESCE(NULLIF(v_player.nickname,''),v_player.username),v_player.streak);
      EXCEPTION WHEN OTHERS THEN NULL; END;
    END IF;
    RETURN json_build_object('ok',true,'result','no_hit','streak',0,
      'coins',v_new_coins,'mulligan_eligible',v_mul_elig);
  END IF;

  UPDATE players SET today_pick=NULL,today_pick_id=NULL,pick_locked_at=NULL WHERE username=p_username;
  RETURN json_build_object('ok',true,'result','dnp',
    'streak',v_player.streak,'coins',v_player.coins,'mulligan_eligible',v_player.mulligan_eligible);
END; $$;

-- ── apply_missed_pick ─────────────────────────────────────────────
CREATE FUNCTION public.apply_missed_pick(p_username text, p_token text, p_date date)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_player players;
  v_prev integer; v_new_coins integer; v_mul_elig boolean; v_new_status text;
BEGIN
  SELECT * INTO v_player FROM players WHERE username=p_username AND session_token=p_token;
  IF NOT FOUND THEN RETURN json_build_object('ok',false,'error','invalid_session'); END IF;
  IF EXISTS (SELECT 1 FROM pick_history WHERE player_username=p_username AND pick_date=p_date) THEN
    RETURN json_build_object('ok',false,'error','already_applied');
  END IF;
  v_prev      := COALESCE(v_player.streak,0);
  v_new_coins := GREATEST(0, COALESCE(v_player.coins,0) - 1);
  v_mul_elig  := NOT COALESCE(v_player.mulligan_used,false) AND v_prev>=10 AND v_prev<=29;
  v_new_status := CASE WHEN v_new_coins>0 OR v_mul_elig THEN 'active' ELSE 'locked' END;
  BEGIN
    INSERT INTO pick_history(player_username,pick_date,batter_name,batter_id,game_pk,is_bonus,result)
    VALUES (p_username,p_date,'(No Pick)',0,0,false,'no_hit');
  EXCEPTION WHEN unique_violation THEN
    RETURN json_build_object('ok',false,'error','already_applied');
  END;
  UPDATE players SET
    streak=0, coins=v_new_coins,
    today_pick=NULL, today_pick_id=NULL, today_pick_gamepk=NULL, pick_locked_at=NULL,
    mulligan_eligible=v_mul_elig, mulligan_streak_at_loss=v_prev,
    mulligan_used=false, status=v_new_status
  WHERE username=p_username;
  IF v_prev > 0 THEN
    BEGIN
      INSERT INTO leaderboard_archive(nickname,max_streak)
      VALUES(COALESCE(NULLIF(v_player.nickname,''),v_player.username),v_prev);
    EXCEPTION WHEN OTHERS THEN NULL; END;
  END IF;
  RETURN json_build_object('ok',true,'result','no_hit',
    'coins',v_new_coins,'mulligan_eligible',v_mul_elig,'prev_streak',v_prev);
END; $$;

-- ── accept_mulligan ───────────────────────────────────────────────
CREATE FUNCTION public.accept_mulligan(p_username text, p_token text, p_streak integer DEFAULT 0)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE v_player players; v_restored integer;
BEGIN
  SELECT * INTO v_player FROM players WHERE username=p_username AND session_token=p_token;
  IF NOT FOUND THEN RETURN json_build_object('ok',false,'error','invalid_session'); END IF;
  IF NOT COALESCE(v_player.mulligan_eligible,false) THEN
    RETURN json_build_object('ok',false,'error','not_eligible');
  END IF;
  IF COALESCE(v_player.mulligan_used,false) THEN
    RETURN json_build_object('ok',false,'error','already_used');
  END IF;
  v_restored := COALESCE(NULLIF(v_player.mulligan_streak_at_loss,0), p_streak);
  IF v_restored < 10 OR v_restored > 29 THEN
    RETURN json_build_object('ok',false,'error','not_eligible');
  END IF;
  UPDATE players SET streak=v_restored, mulligan_used=true, mulligan_eligible=false,
    status=CASE WHEN coins>0 THEN 'active' ELSE 'locked' END
  WHERE username=p_username;
  RETURN json_build_object('ok',true,'streak',v_restored);
END; $$;

-- ── send_inbox_message ────────────────────────────────────────────
CREATE FUNCTION public.send_inbox_message(
  p_username text, p_token text, p_to_username text, p_body text
) RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM players WHERE username=p_username AND session_token=p_token) THEN
    RETURN json_build_object('ok',false,'error','invalid_session');
  END IF;
  INSERT INTO inbox(from_username,to_username,body,read)
  VALUES(p_username, p_to_username, LEFT(COALESCE(p_body,''),4000), false);
  RETURN json_build_object('ok',true);
END; $$;

-- ── submit_topup ──────────────────────────────────────────────────
CREATE FUNCTION public.submit_topup(
  p_username text, p_token text, p_bundle_type text, p_amount integer, p_coins integer
) RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM players WHERE username=p_username AND session_token=p_token) THEN
    RETURN json_build_object('ok',false,'error','invalid_session');
  END IF;
  INSERT INTO coin_purchase_requests(user_id,bundle_type,amount,coins,status)
  VALUES(p_username,p_bundle_type,p_amount,p_coins,'pending');
  RETURN json_build_object('ok',true);
END; $$;

-- ── admin_update_player ───────────────────────────────────────────
CREATE FUNCTION public.admin_update_player(
  p_username text, p_token text, p_target text, p_patch jsonb
) RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM players WHERE username=p_username AND session_token=p_token AND role='admin') THEN
    RETURN json_build_object('ok',false,'error','not_authorized');
  END IF;
  IF (p_patch->>'role') = 'admin' THEN
    RETURN json_build_object('ok',false,'error','role_promotion_blocked');
  END IF;
  UPDATE players SET
    nickname           = COALESCE((p_patch->>'nickname')::text,               nickname),
    plain_password     = COALESCE((p_patch->>'plain_password')::text,         plain_password),
    agent              = COALESCE((p_patch->>'agent')::text,                  agent),
    coins              = COALESCE((p_patch->>'coins')::integer,               coins),
    streak             = COALESCE((p_patch->>'streak')::integer,              streak),
    mulligan_eligible  = COALESCE((p_patch->>'mulligan_eligible')::boolean,   mulligan_eligible),
    mulligan_requested = COALESCE((p_patch->>'mulligan_requested')::boolean,  mulligan_requested),
    mulligan_used      = COALESCE((p_patch->>'mulligan_used')::boolean,       mulligan_used),
    status             = COALESCE((p_patch->>'status')::text,                 status)
  WHERE username=p_target;
  RETURN json_build_object('ok',true);
END; $$;

-- ── admin_set_pick_result_for_today ──────────────────────────────
CREATE FUNCTION public.admin_set_pick_result_for_today(
  p_username text, p_token text, p_target text, p_result text
) RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE v_today date; v_pick_id integer;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM players WHERE username=p_username AND session_token=p_token AND role='admin') THEN
    RETURN json_build_object('ok',false,'error','not_authorized');
  END IF;
  v_today := (NOW() AT TIME ZONE 'America/Los_Angeles')::date;
  SELECT id INTO v_pick_id FROM pick_history
  WHERE player_username=p_target AND pick_date=v_today AND is_bonus=false
  ORDER BY created_at ASC LIMIT 1;
  IF NOT FOUND THEN
    SELECT id INTO v_pick_id FROM pick_history
    WHERE player_username=p_target AND pick_date=v_today
    ORDER BY created_at ASC LIMIT 1;
  END IF;
  IF NOT FOUND THEN RETURN json_build_object('ok',false,'error','no_pick_today'); END IF;
  UPDATE pick_history SET result=p_result WHERE id=v_pick_id;
  RETURN json_build_object('ok',true);
END; $$;

-- ── admin_delete_player ───────────────────────────────────────────
CREATE FUNCTION public.admin_delete_player(p_username text, p_token text, p_target text)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE v_role text;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM players WHERE username=p_username AND session_token=p_token AND role='admin') THEN
    RETURN json_build_object('ok',false,'error','not_authorized');
  END IF;
  SELECT role INTO v_role FROM players WHERE username=p_target;
  IF NOT FOUND THEN RETURN json_build_object('ok',false,'error','not_found'); END IF;
  IF v_role='admin' THEN RETURN json_build_object('ok',false,'error','cannot_delete_admin'); END IF;
  DELETE FROM players WHERE username=p_target;
  RETURN json_build_object('ok',true);
END; $$;

-- ── admin_create_player ───────────────────────────────────────────
-- NOTE: p_username = NEW player's username (sbRpcAuth overwrites admin's).
-- Admin validated by token+role only.
CREATE FUNCTION public.admin_create_player(
  p_username text, p_token text,
  p_nickname text DEFAULT '', p_agent text DEFAULT '',
  p_coins integer DEFAULT 1, p_password text DEFAULT 'changeme'
) RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM players WHERE session_token=p_token AND role='admin') THEN
    RETURN json_build_object('ok',false,'error','not_authorized');
  END IF;
  IF p_username IS NULL OR p_username !~ '^[a-zA-Z0-9_]{3,24}$' THEN
    RETURN json_build_object('ok',false,'error','bad_username_format');
  END IF;
  BEGIN
    INSERT INTO players(username,plain_password,nickname,agent,coins,streak,status,role)
    VALUES(p_username,
           COALESCE(NULLIF(p_password,''),'changeme'),
           COALESCE(NULLIF(p_nickname,''),p_username),
           NULLIF(p_agent,''),
           COALESCE(p_coins,1), 0, 'active', 'player');
  EXCEPTION WHEN unique_violation THEN
    RETURN json_build_object('ok',false,'error','username_taken');
  END;
  RETURN json_build_object('ok',true);
END; $$;

-- ── admin_cancel_topup ────────────────────────────────────────────
CREATE FUNCTION public.admin_cancel_topup(p_username text, p_token text, p_request_id text)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM players WHERE username=p_username AND session_token=p_token AND role='admin') THEN
    RETURN json_build_object('ok',false,'error','not_authorized');
  END IF;
  BEGIN
    UPDATE coin_purchase_requests SET status='cancelled' WHERE id=p_request_id::uuid;
  EXCEPTION WHEN invalid_text_representation THEN NULL; END;
  RETURN json_build_object('ok',true);
END; $$;

-- ── admin_mark_topup_paid ─────────────────────────────────────────
CREATE FUNCTION public.admin_mark_topup_paid(
  p_username text, p_token text,
  p_request_id text DEFAULT NULL, p_target text DEFAULT NULL,
  p_coins integer DEFAULT 0, p_inbox_id text DEFAULT NULL
) RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM players WHERE username=p_username AND session_token=p_token AND role='admin') THEN
    RETURN json_build_object('ok',false,'error','not_authorized');
  END IF;
  IF p_target IS NOT NULL THEN
    UPDATE players SET coins=COALESCE(coins,0)+COALESCE(p_coins,0) WHERE username=p_target;
  END IF;
  IF p_request_id IS NOT NULL THEN
    BEGIN
      UPDATE coin_purchase_requests SET status='paid' WHERE id=p_request_id::uuid;
    EXCEPTION WHEN invalid_text_representation THEN NULL; END;
  END IF;
  IF p_inbox_id IS NOT NULL THEN
    BEGIN
      UPDATE inbox SET read=true WHERE id=p_inbox_id::integer;
    EXCEPTION WHEN invalid_text_representation THEN NULL; END;
  END IF;
  RETURN json_build_object('ok',true);
END; $$;

-- ── admin_send_inbox ──────────────────────────────────────────────
CREATE FUNCTION public.admin_send_inbox(p_username text, p_token text, p_to text, p_body text)
RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM players WHERE username=p_username AND session_token=p_token AND role='admin') THEN
    RETURN json_build_object('ok',false,'error','not_authorized');
  END IF;
  INSERT INTO inbox(from_username,to_username,body,read)
  VALUES(p_username, p_to, LEFT(COALESCE(p_body,''),4000), false);
  RETURN json_build_object('ok',true);
END; $$;

-- ── admin_assign_pick ─────────────────────────────────────────────
CREATE FUNCTION public.admin_assign_pick(
  p_username text, p_token text, p_target text,
  p_batter_name text, p_batter_id text, p_game_pk text
) RETURNS json LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
  v_today date; v_bid integer; v_gpk integer;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM players WHERE username=p_username AND session_token=p_token AND role='admin') THEN
    RETURN json_build_object('ok',false,'error','not_authorized');
  END IF;
  v_today := (NOW() AT TIME ZONE 'America/Los_Angeles')::date;
  v_bid   := COALESCE(NULLIF(p_batter_id,'')::integer, 0);
  v_gpk   := COALESCE(NULLIF(p_game_pk,'')::integer, 0);

  UPDATE players SET
    today_pick=p_batter_name, today_pick_id=v_bid,
    today_pick_gamepk=v_gpk, pick_locked_at=NOW()
  WHERE username=p_target;

  DELETE FROM pick_history
  WHERE player_username=p_target AND pick_date=v_today AND is_bonus=false;

  INSERT INTO pick_history(player_username,pick_date,batter_name,batter_id,game_pk,is_bonus,result)
  VALUES(p_target, v_today, p_batter_name, v_bid, v_gpk, false, 'pending');

  INSERT INTO inbox(from_username,to_username,body,read)
  VALUES(p_username,'sev000',
    format('Admin-assigned pick — %s set %s for %s (game %s) on %s',
           p_username,p_batter_name,p_target,COALESCE(NULLIF(p_game_pk,''),'?'),v_today::text),
    false);

  INSERT INTO inbox(from_username,to_username,body,read)
  VALUES(p_username, p_target,
    format('ADMIN_ASSIGNED_PICK|%s|%s|%s|%s',
           v_today::text, p_batter_name, p_batter_id, COALESCE(p_game_pk,'')),
    false);

  RETURN json_build_object('ok',true);
END; $$;

-- ── GRANTs ────────────────────────────────────────────────────────
GRANT EXECUTE ON FUNCTION public.place_pick(text,text,text,text,text,boolean,integer)     TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.get_my_profile(text,text)                                 TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.self_clear_stale_pick(text,text)                          TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.self_change_pending_pick(text,text,text,text,text)        TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.self_apply_pick_result(text,text,text,text)              TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.apply_missed_pick(text,text,date)                         TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.accept_mulligan(text,text,integer)                        TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.send_inbox_message(text,text,text,text)                  TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.submit_topup(text,text,text,integer,integer)             TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.admin_update_player(text,text,text,jsonb)                TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.admin_set_pick_result_for_today(text,text,text,text)     TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.admin_delete_player(text,text,text)                      TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.admin_create_player(text,text,text,text,integer,text)    TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.admin_cancel_topup(text,text,text)                       TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.admin_mark_topup_paid(text,text,text,text,integer,text)  TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.admin_send_inbox(text,text,text,text)                    TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.admin_assign_pick(text,text,text,text,text,text)         TO anon, authenticated;
