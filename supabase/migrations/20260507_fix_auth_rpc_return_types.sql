-- =============================================================
-- Migration: 20260507_fix_auth_rpc_return_types.sql
-- Fix: login_player, restore_player, logout_player all returned
-- SETOF players / void instead of json.
-- The client checks resp.ok — undefined on an array — so every
-- login showed "Wrong credentials" even with correct credentials.
-- Replace all three with RETURNS json and the {ok, player} shape
-- the client expects.
-- =============================================================

DROP FUNCTION IF EXISTS public.login_player(text, text);
DROP FUNCTION IF EXISTS public.restore_player(text, text);
DROP FUNCTION IF EXISTS public.logout_player(text, text);

-- login_player: returns {ok:true, player:{...}} or {ok:false, error:'bad_credentials'}
CREATE FUNCTION public.login_player(p_username text, p_password text)
RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_token TEXT;
  v_row players;
BEGIN
  SELECT * INTO v_row FROM players
    WHERE username = p_username AND plain_password = p_password;
  IF NOT FOUND THEN
    RETURN json_build_object('ok', false, 'error', 'bad_credentials');
  END IF;
  v_token := encode(gen_random_bytes(24), 'hex');
  UPDATE players SET session_token = v_token WHERE username = p_username;
  v_row.session_token := v_token;
  RETURN json_build_object('ok', true, 'player', row_to_json(v_row));
END;
$$;

-- restore_player: returns {ok:true, player:{...}} or {ok:false, error:'invalid_session'}
CREATE FUNCTION public.restore_player(p_username text, p_token text)
RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_row players;
BEGIN
  SELECT * INTO v_row FROM players
    WHERE username = p_username AND session_token = p_token;
  IF NOT FOUND THEN
    RETURN json_build_object('ok', false, 'error', 'invalid_session');
  END IF;
  RETURN json_build_object('ok', true, 'player', row_to_json(v_row));
END;
$$;

-- logout_player: returns {ok:true}
CREATE FUNCTION public.logout_player(p_username text, p_token text)
RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  UPDATE players SET session_token = NULL
    WHERE username = p_username AND session_token = p_token;
  RETURN json_build_object('ok', true);
END;
$$;

GRANT EXECUTE ON FUNCTION public.login_player(text,text)  TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.restore_player(text,text) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.logout_player(text,text) TO anon, authenticated;
