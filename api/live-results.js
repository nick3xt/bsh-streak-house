// v1.1 build trigger
// /api/live-results.js
// Vercel Cron Job â runs hourly 17:00â05:00 UTC (10 AM â 10 PM PT)
//
// Resolves TODAY's pending picks mid-game as soon as a hit is detected
// on the MLB live feed. Designed to run alongside /api/nightly-process:
//   â¢ HIT confirmed  â mark pick 'hit', increment streak immediately
//   â¢ NO HIT + Final â mark pick 'no_hit', reset streak
//     (coin/mulligan settlement deferred to nightly at 04:00 PT)
//   â¢ Still in-progress â leave pending (next hourly run will catch it)
//
// Auth: same BSH_CRON_SECRET pattern as nightly-process.js
//
// Env vars required:
//   SUPABASE_URL              â Supabase project URL
//   SUPABASE_SERVICE_ROLE_KEY â Service role key (bypasses RLS)
//   BSH_CRON_SECRET           â Must match Authorization: Bearer <value>
'use strict';

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://heykwxkyvbzffkhgrqgf.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const MLB_LIVE = 'https://statsapi.mlb.com/api/v1.1/game';
const MLB_TIMEOUT = 12_000;

function todayPT() {
  return new Date().toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
}

function sbHeaders(extras = {}) {
  return {
    'apikey': SUPABASE_KEY,
    'Authorization': `Bearer ${SUPABASE_KEY}`,
    'Content-Type': 'application/json',
    ...extras,
  };
}

async function sbGet(table, qs) {
  const url = `${SUPABASE_URL}/rest/v1/${table}${qs ? '?' + qs : ''}`;
  const r = await fetch(url, { headers: sbHeaders() });
  if (!r.ok) { const body = await r.text(); throw new Error(`GET ${table} ${r.status}: ${body}`); }
  return r.json();
}

async function sbPatch(table, qs, body) {
  const url = `${SUPABASE_URL}/rest/v1/${table}?${qs}`;
  const r = await fetch(url, {
    method: 'PATCH',
    headers: sbHeaders({ 'Prefer': 'return=minimal' }),
    body: JSON.stringify(body),
  });
  if (!r.ok) { const text = await r.text(); throw new Error(`PATCH ${table} ${r.status}: ${text}`); }
}

// Identical hit-detection logic to nightly-process.js:
//   'hit'    â batter has â¥1 hit (game Live or Final)
//   'no_hit' â batter has 0 hits AND game is Final
//   'pending' â game not yet started, postponed, or API error
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
  } catch (e) {
    console.warn(`[mlb] feed/${gamePk} fetch error: ${e.message}`);
    return 'pending';
  }

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
  // Auth: must present BSH_CRON_SECRET (Vercel injects it for cron callers)
  const cronSecret = process.env.BSH_CRON_SECRET;
  if (cronSecret) {
    const auth = req.headers['authorization'] || '';
    if (auth !== `Bearer ${cronSecret}`) {
      console.warn('[live-results] Rejected unauthorized request');
      return res.status(401).json({ error: 'Unauthorized' });
    }
  }

  if (!SUPABASE_KEY) {
    console.error('[live-results] SUPABASE_SERVICE_ROLE_KEY is not set');
    return res.status(500).json({ error: 'Server misconfigured: missing SUPABASE_SERVICE_ROLE_KEY' });
  }

  const today = todayPT();
  console.log(`[live-results] Starting run for ${today}`);

  const summary = {
    date: today,
    hits: 0,
    no_hits: 0,
    still_pending: 0,
    skipped: 0,
    errors: [],
  };

  let pendingPicks;
  try {
    pendingPicks = await sbGet(
      'pick_history',
      `pick_date=eq.${today}&result=eq.pending&select=id,player_username,batter_name,batter_id,game_pk,is_bonus`
    );
  } catch (e) {
    console.error(`[live-results] Failed to fetch pending picks: ${e.message}`);
    return res.status(500).json({ error: e.message });
  }

  console.log(`[live-results] ${pendingPicks.length} pending pick(s) for ${today}`);

  for (const pick of pendingPicks) {
    try {
      // Re-read to guard against a concurrent nightly run having resolved it
      const [fresh] = await sbGet('pick_history', `id=eq.${pick.id}&select=id,result`);
      if (!fresh || fresh.result !== 'pending') {
        console.log(`[live-results] pick ${pick.id} already resolved â skipping`);
        summary.skipped++;
        continue;
      }

      const result = await getHitResult(pick.game_pk, pick.batter_id);
      console.log(`[live-results] pick ${pick.id} (${pick.batter_name}) â ${result}`);

      if (result === 'pending') {
        summary.still_pending++;
        continue;
      }

      // Fetch current player stats for correct streak math
      const [player] = await sbGet(
        'players',
        `username=eq.${encodeURIComponent(pick.player_username)}&select=username,streak,coins,mulligan_used,mulligan_streak_at_loss`
      );
      if (!player) throw new Error(`player not found: ${pick.player_username}`);

      const prevStreak = player.streak || 0;

      // Mark pick resolved
      await sbPatch('pick_history', `id=eq.${pick.id}`, { result });

      if (result === 'hit') {
        const newStreak = prevStreak + 1;
        const patch = pick.is_bonus
          ? { streak: newStreak }
          : { streak: newStreak, today_pick: null, today_pick_id: null, today_pick_gamepk: null, pick_locked_at: null };
        if (prevStreak === 0) patch.mulligan_used = false;
        await sbPatch('players', `username=eq.${encodeURIComponent(pick.player_username)}`, patch);
        summary.hits++;
    // [BSH Push] Notify user of HIT result — fire-and-forget
    fetch('https://42streakhouse.com/api/send-push', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title: '\u2705 HIT! ' + pick.batter_name,
        body: 'Your streak continues! Day ' + newStreak + ' in the books.',
        url: 'https://42streakhouse.com',
        userId: pick.player_username
      })
    }).catch(e => console.warn('[push] HIT error:', e.message));
        console.log(`[live-results] HIT â ${pick.player_username} streak ${prevStreak} â ${newStreak}`);

      } else {
        // no_hit â game is Final. Reset streak now so players see the result
        // immediately. Coin deduction + mulligan settlement runs via nightly-process
        // at 04:00 PT using the same idempotency guard (pick no longer pending).
        // NOTE: nightly will NOT re-process this pick since result != 'pending'.
        // Full no-hit penalty (coins, mulligan) is applied here to ensure it runs.
        const newCoins    = Math.max(0, (player.coins || 0) - 1);
        const mulEligible = !player.mulligan_used && prevStreak >= 10 && prevStreak <= 29;
        const patch = {
          streak: 0,
          coins: newCoins,
          status: newCoins > 0 || mulEligible ? 'active' : 'locked',
          mulligan_eligible: mulEligible,
          mulligan_used: false,
          mulligan_streak_at_loss: mulEligible ? prevStreak : (player.mulligan_streak_at_loss || 0),
        };
        if (!pick.is_bonus) {
          patch.today_pick        = null;
          patch.today_pick_id     = null;
          patch.today_pick_gamepk = null;
          patch.pick_locked_at    = null;
        }
        await sbPatch('players', `username=eq.${encodeURIComponent(pick.player_username)}`, patch);
        summary.no_hits++;
    // [BSH Push] Notify user of NO_HIT result — fire-and-forget
    fetch('https://42streakhouse.com/api/send-push', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title: '\u274c No hit for ' + pick.batter_name,
        body: 'Streak resets. Pick a new player tomorrow.',
        url: 'https://42streakhouse.com',
        userId: pick.player_username
      })
    }).catch(e => console.warn('[push] NO_HIT error:', e.message));
        console.log(`[live-results] NO HIT â ${pick.player_username} streak ${prevStreak} â 0, coins â ${newCoins}`);
      }
    } catch (e) {
      const msg = `pick ${pick.id} (${pick.player_username}): ${e.message}`;
      console.error(`[live-results] Error â ${msg}`);
      summary.errors.push(msg);
    }
  }

  console.log(`[live-results] Done: ${JSON.stringify(summary)}`);
  return res.status(200).json({ ok: true, ...summary });
};
