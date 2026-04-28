// /api/nightly-process.js
// Vercel Cron Job — runs daily at 11:00 UTC (04:00 AM PT / 07:00 AM ET)
//
// Phase 1 — Pending pick resolution:
//   For every pick_history row with result='pending' on yesterday's PT date,
//   hit the MLB live-feed API to determine hit/no_hit. Resolve the pick and
//   update the player's streak/coins accordingly. Idempotent — skips already-
//   resolved picks.
//
// Phase 2 — Missed pick sweep:
//   Any active (non-admin) player with no pick_history row for yesterday gets
//   the standard no-pick penalty: streak reset, -1 coin, mulligan window opens
//   if eligible. A synthetic pick_history row (batter_name='(No Pick)') serves
//   as the idempotency guard.
//
// Env vars required:
//   SUPABASE_URL              — Supabase project URL
//   SUPABASE_SERVICE_ROLE_KEY — Service role key (bypasses RLS)
//   BSH_CRON_SECRET           — Must match Authorization: Bearer <value>
'use strict';

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://heykwxkyvbzffkhgrqgf.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const MLB_LIVE    = 'https://statsapi.mlb.com/api/v1.1/game';
const MLB_TIMEOUT = 12_000;

function todayPT() {
  return new Date().toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
}
function yesterdayPT() {
  const base = todayPT();
  const d = new Date(base + 'T12:00:00Z');
  d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().slice(0, 10);
}

function sbHeaders(extras) {
  return { 'apikey': SUPABASE_KEY, 'Authorization': `Bearer ${SUPABASE_KEY}`, 'Content-Type': 'application/json', ...extras };
}
async function sbGet(table, qs) {
  const url = `${SUPABASE_URL}/rest/v1/${table}${qs ? '?' + qs : ''}`;
  const r = await fetch(url, { headers: sbHeaders() });
  if (!r.ok) { const body = await r.text(); throw new Error(`GET ${table} ${r.status}: ${body}`); }
  return r.json();
}
async function sbPatch(table, qs, body) {
  const url = `${SUPABASE_URL}/rest/v1/${table}?${qs}`;
  const r = await fetch(url, { method: 'PATCH', headers: sbHeaders({ 'Prefer': 'return=minimal' }), body: JSON.stringify(body) });
  if (!r.ok) { const text = await r.text(); throw new Error(`PATCH ${table} ${r.status}: ${text}`); }
}
async function sbPost(table, body) {
  const url = `${SUPABASE_URL}/rest/v1/${table}`;
  const r = await fetch(url, { method: 'POST', headers: sbHeaders({ 'Prefer': 'return=minimal' }), body: JSON.stringify(body) });
  if (!r.ok) {
    const text = await r.text();
    const err = new Error(`POST ${table} ${r.status}: ${text}`);
    if (r.status === 409 || /unique|duplicate/i.test(text)) { err.isDuplicate = true; }
    throw err;
  }
}

async function getHitResult(gamePk, batterId) {
  if (!gamePk || String(gamePk) === '0') return 'pending';
  let data;
  try {
    const controller = new AbortController();
    const tid = setTimeout(() => controller.abort(), MLB_TIMEOUT);
    const resp = await fetch(`${MLB_LIVE}/${gamePk}/feed/live`, { signal: controller.signal });
    clearTimeout(tid);
    if (!resp.ok) { console.warn(`[mlb] feed/${gamePk} returned ${resp.status}`); return 'pending'; }
    data = await resp.json();
  } catch (e) { console.warn(`[mlb] feed/${gamePk} fetch error: ${e.message}`); return 'pending'; }
  const liveGamePk = data?.gamePk ?? data?.gameData?.game?.pk;
  if (liveGamePk != null && String(liveGamePk) !== String(gamePk)) return 'pending';
  const abs = data?.gameData?.status?.abstractGameState;
  const feedIsFinal = abs === 'Final';
  const feedIsLive  = abs === 'Live';
  if (!feedIsFinal && !feedIsLive) return 'pending';
  const teams = data?.liveData?.boxscore?.teams;
  if (!teams) return 'pending';
  const key   = `ID${batterId}`;
  const entry = teams.home?.players?.[key] || teams.away?.players?.[key];
  if (!entry) return feedIsFinal ? 'no_hit' : 'pending';
  const rawHits = entry?.stats?.batting?.hits;
  if (rawHits == null) return feedIsFinal ? 'no_hit' : 'pending';
  const hits = parseInt(rawHits, 10);
  if (Number.isNaN(hits)) return 'pending';
  return hits >= 1 ? 'hit' : feedIsFinal ? 'no_hit' : 'pending';
}

module.exports = async function handler(req, res) {
  const cronSecret = process.env.BSH_CRON_SECRET;
  if (cronSecret) {
    const auth = req.headers['authorization'] || '';
    if (auth !== `Bearer ${cronSecret}`) {
      console.warn('[nightly-process] Rejected unauthorized request');
      return res.status(401).json({ error: 'Unauthorized' });
    }
  }
  if (!SUPABASE_KEY) {
    console.error('[nightly-process] SUPABASE_SERVICE_ROLE_KEY is not set');
    return res.status(500).json({ error: 'Server misconfigured: missing SUPABASE_SERVICE_ROLE_KEY' });
  }

  const yesterday = yesterdayPT();
  console.log(`[nightly-process] Starting run for ${yesterday}`);
  const p1 = { processed: 0, hits: 0, misses: 0, dnps: 0, errors: [] };
  const p2 = { swept: 0, skipped: 0, errors: [] };

  // PHASE 1
  try {
    const pendingPicks = await sbGet('pick_history',
      `pick_date=eq.${yesterday}&result=eq.pending&select=id,player_username,batter_name,batter_id,game_pk,is_bonus`);
    console.log(`[phase1] ${pendingPicks.length} pending pick(s) for ${yesterday}`);
    for (const pick of pendingPicks) {
      try {
        const [fresh] = await sbGet('pick_history', `id=eq.${pick.id}&select=id,result`);
        if (!fresh || fresh.result !== 'pending') continue;
        const result = await getHitResult(pick.game_pk, pick.batter_id);
        const [player] = await sbGet('players',
          `username=eq.${encodeURIComponent(pick.player_username)}&select=username,nickname,streak,coins,mulligan_used,mulligan_streak_at_loss`);
        if (!player) { p1.errors.push(`player not found: ${pick.player_username}`); continue; }
        const prevStreak = player.streak || 0;
        if (result === 'pending') {
          // DNP: game not final — treat as miss
          p1.dnps++;
          const newCoins = Math.max(0, (player.coins || 0) - 1);
          const mulEligible = !player.mulligan_used && prevStreak >= 10 && prevStreak <= 29;
          await sbPatch('pick_history', `id=eq.${pick.id}`, { result: 'no_hit' });
          await sbPatch('players', `username=eq.${encodeURIComponent(pick.player_username)}`, {
            streak: 0, coins: newCoins, status: newCoins > 0 || mulEligible ? 'active' : 'locked',
            mulligan_eligible: mulEligible, mulligan_used: false,
            mulligan_streak_at_loss: mulEligible ? prevStreak : (player.mulligan_streak_at_loss || 0),
            today_pick: null, today_pick_id: null, today_pick_gamepk: null, pick_locked_at: null,
          });
          if (prevStreak > 0) await sbPost('leaderboard_archive', { nickname: player.nickname || player.username, max_streak: prevStreak }).catch(() => {});
          continue;
        }
        await sbPatch('pick_history', `id=eq.${pick.id}`, { result });
        p1.processed++;
        if (result === 'hit') {
          const newStreak = prevStreak + 1;
          const patch = pick.is_bonus ? { streak: newStreak } : { streak: newStreak, today_pick: null, today_pick_id: null, today_pick_gamepk: null, pick_locked_at: null };
          if (prevStreak === 0) patch.mulligan_used = false;
          await sbPatch('players', `username=eq.${encodeURIComponent(pick.player_username)}`, patch);
          p1.hits++;
          console.log(`[phase1] HIT — ${pick.player_username} streak ${prevStreak}→${newStreak}`);
        } else {
          const newCoins = Math.max(0, (player.coins || 0) - 1);
          const mulEligible = !player.mulligan_used && prevStreak >= 10 && prevStreak <= 29;
          const patch = {
            streak: 0, coins: newCoins, status: newCoins > 0 || mulEligible ? 'active' : 'locked',
            mulligan_eligible: mulEligible, mulligan_used: false,
            mulligan_streak_at_loss: mulEligible ? prevStreak : (player.mulligan_streak_at_loss || 0),
          };
          if (!pick.is_bonus) { patch.today_pick = null; patch.today_pick_id = null; patch.today_pick_gamepk = null; patch.pick_locked_at = null; }
          await sbPatch('players', `username=eq.${encodeURIComponent(pick.player_username)}`, patch);
          if (prevStreak > 0) await sbPost('leaderboard_archive', { nickname: player.nickname || player.username, max_streak: prevStreak }).catch(() => {});
          p1.misses++;
          console.log(`[phase1] NO HIT — ${pick.player_username} streak ${prevStreak}→0, coins→${newCoins}`);
        }
      } catch (e) { p1.errors.push(`pick ${pick.id} (${pick.player_username}): ${e.message}`); }
    }
  } catch (e) { p1.errors.push(`fatal: ${e.message}`); }

  // PHASE 2
  try {
    const [activePlayers, yesterdayPickRows] = await Promise.all([
      sbGet('players', 'role=neq.admin&status=eq.active&select=username,nickname,streak,coins,mulligan_used,mulligan_streak_at_loss'),
      sbGet('pick_history', `pick_date=eq.${yesterday}&select=player_username`),
    ]);
    const pickedSet = new Set(yesterdayPickRows.map(r => r.player_username));
    const missed = activePlayers.filter(p => !pickedSet.has(p.username));
    console.log(`[phase2] ${activePlayers.length} active, ${pickedSet.size} picked, ${missed.length} missed`);
    for (const player of missed) {
      try {
        const prevStreak = player.streak || 0;
        const newCoins = Math.max(0, (player.coins || 0) - 1);
        const mulEligible = !player.mulligan_used && prevStreak >= 10 && prevStreak <= 29;
        await sbPost('pick_history', {
          player_username: player.username, pick_date: yesterday,
          batter_name: '(No Pick)', batter_id: '0', game_pk: '0', is_bonus: false, result: 'no_hit',
        });
        await sbPatch('players', `username=eq.${encodeURIComponent(player.username)}`, {
          streak: 0, coins: newCoins, status: newCoins > 0 || mulEligible ? 'active' : 'locked',
          today_pick: null, today_pick_id: null, today_pick_gamepk: null, pick_locked_at: null,
          mulligan_eligible: mulEligible, mulligan_used: false,
          mulligan_streak_at_loss: mulEligible ? prevStreak : (player.mulligan_streak_at_loss || 0),
        });
        if (prevStreak > 0) await sbPost('leaderboard_archive', { nickname: player.nickname || player.username, max_streak: prevStreak }).catch(() => {});
        p2.swept++;
        console.log(`[phase2] SWEPT — ${player.username} streak ${prevStreak}→0, coins→${newCoins}`);
      } catch (e) {
        if (e.isDuplicate) { p2.skipped++; }
        else { p2.errors.push(`${player.username}: ${e.message}`); }
      }
    }
  } catch (e) { p2.errors.push(`fatal: ${e.message}`); }

  console.log(`[nightly-process] done phase1=${JSON.stringify(p1)} phase2=${JSON.stringify(p2)}`);
  return res.status(200).json({
    ok: true, date: yesterday,
    phase1: { processed: p1.processed, hits: p1.hits, misses: p1.misses, dnps: p1.dnps },
    phase2: { swept: p2.swept },
  });
};
