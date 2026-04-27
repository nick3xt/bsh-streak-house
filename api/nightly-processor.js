// /api/nightly-processor.js
// Vercel Cron Job — runs daily at 08:00 UTC (04:00 ET / 01:00 PT)
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
// Optional:
//   CRON_SECRET               — Vercel auto-injects; blocks non-cron callers

'use strict';

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://heykwxkyvbzffkhgrqgf.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const MLB_LIVE     = 'https://statsapi.mlb.com/api/v1.1/game';
const MLB_TIMEOUT  = 12_000; // ms per MLB API call

// ── Date helpers ────────────────────────────────────────────────────────────

/** Returns the current date in PT as YYYY-MM-DD. */
function todayPT() {
  return new Date().toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
}

/** Returns yesterday's date in PT as YYYY-MM-DD. */
function yesterdayPT() {
  const base = todayPT();
  const d = new Date(base + 'T12:00:00Z'); // anchor at noon UTC to survive DST
  d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().slice(0, 10);
}

// ── Supabase REST helpers ────────────────────────────────────────────────────

function sbHeaders(extras) {
  return {
    'apikey':        SUPABASE_KEY,
    'Authorization': `Bearer ${SUPABASE_KEY}`,
    'Content-Type':  'application/json',
    ...extras,
  };
}

async function sbGet(table, qs) {
  const url = `${SUPABASE_URL}/rest/v1/${table}${qs ? '?' + qs : ''}`;
  const r = await fetch(url, { headers: sbHeaders() });
  if (!r.ok) {
    const body = await r.text();
    throw new Error(`GET ${table} ${r.status}: ${body}`);
  }
  return r.json();
}

async function sbPatch(table, qs, body) {
  const url = `${SUPABASE_URL}/rest/v1/${table}?${qs}`;
  const r = await fetch(url, {
    method:  'PATCH',
    headers: sbHeaders({ 'Prefer': 'return=minimal' }),
    body:    JSON.stringify(body),
  });
  if (!r.ok) {
    const text = await r.text();
    throw new Error(`PATCH ${table} ${r.status}: ${text}`);
  }
}

async function sbPost(table, body) {
  const url = `${SUPABASE_URL}/rest/v1/${table}`;
  const r = await fetch(url, {
    method:  'POST',
    headers: sbHeaders({ 'Prefer': 'return=minimal' }),
    body:    JSON.stringify(body),
  });
  if (!r.ok) {
    const text = await r.text();
    const err = new Error(`POST ${table} ${r.status}: ${text}`);
    if (r.status === 409 || /unique|duplicate/i.test(text)) {
      err.isDuplicate = true;
    }
    throw err;
  }
}

// ── MLB hit detection ────────────────────────────────────────────────────────
//
// Mirrors getHitResult() in index.html exactly:
//   • Hit fires as soon as the game is Live OR Final.
//   • No-hit only confirmed when the game is Final (batter may not have
//     batted yet mid-game).
//   • Any uncertainty (postponed, preview, API error) → 'pending'.
//
// Returns: 'hit' | 'no_hit' | 'pending'

async function getHitResult(gamePk, batterId) {
  if (!gamePk || String(gamePk) === '0') return 'pending';

  let data;
  try {
    const controller = new AbortController();
    const tid = setTimeout(() => controller.abort(), MLB_TIMEOUT);
    const resp = await fetch(`${MLB_LIVE}/${gamePk}/feed/live`, {
      signal: controller.signal,
    });
    clearTimeout(tid);
    if (!resp.ok) {
      console.warn(`[mlb] feed/${gamePk} returned ${resp.status}`);
      return 'pending';
    }
    data = await resp.json();
  } catch (e) {
    console.warn(`[mlb] feed/${gamePk} fetch error: ${e.message}`);
    return 'pending';
  }

  // Verify the feed payload is for the gamePk we asked for
  const liveGamePk = data?.gamePk ?? data?.gameData?.game?.pk;
  if (liveGamePk != null && String(liveGamePk) !== String(gamePk)) return 'pending';

  const abs        = data?.gameData?.status?.abstractGameState;
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
  if (hits >= 1) return 'hit';
  return feedIsFinal ? 'no_hit' : 'pending';
}

// ── Handler ──────────────────────────────────────────────────────────────────

module.exports = async function handler(req, res) {
  // Vercel Cron injects Authorization: Bearer <CRON_SECRET>.
  // Block any non-cron caller when the secret is configured.
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret) {
    const auth = req.headers['authorization'] || '';
    if (auth !== `Bearer ${cronSecret}`) {
      console.warn('[nightly-processor] Rejected unauthorized request');
      return res.status(401).json({ error: 'Unauthorized' });
    }
  }

  if (!SUPABASE_KEY) {
    console.error('[nightly-processor] SUPABASE_SERVICE_ROLE_KEY is not set');
    return res.status(500).json({ error: 'Server misconfigured: missing SUPABASE_SERVICE_ROLE_KEY' });
  }

  const yesterday = yesterdayPT();
  console.log(`[nightly-processor] Starting run for ${yesterday}`);

  const summary = {
    date:   yesterday,
    phase1: { total: 0, resolved_hit: 0, resolved_no_hit: 0, left_pending: 0, already_resolved: 0, errors: [] },
    phase2: { swept: 0, skipped: 0, errors: [] },
  };

  // ════════════════════════════════════════════════════════════════════════════
  // PHASE 1 — Resolve pending picks from yesterday
  // ════════════════════════════════════════════════════════════════════════════
  try {
    const pendingPicks = await sbGet(
      'pick_history',
      `pick_date=eq.${yesterday}&result=eq.pending&select=id,player_username,batter_name,batter_id,game_pk,is_bonus`
    );
    console.log(`[phase1] ${pendingPicks.length} pending pick(s) for ${yesterday}`);
    summary.phase1.total = pendingPicks.length;

    for (const pick of pendingPicks) {
      try {
        // Re-read to confirm still pending — another process may have resolved
        const [fresh] = await sbGet('pick_history', `id=eq.${pick.id}&select=id,result`);
        if (!fresh || fresh.result !== 'pending') {
          console.log(`[phase1] pick ${pick.id} already resolved, skipping`);
          summary.phase1.already_resolved++;
          continue;
        }

        const result = await getHitResult(pick.game_pk, pick.batter_id);
        if (result === 'pending') {
          console.log(`[phase1] pick ${pick.id} (${pick.batter_name}) game not yet final — leaving pending`);
          summary.phase1.left_pending++;
          continue;
        }

        // Fetch current player stats for correct coin/streak math
        const [player] = await sbGet(
          'players',
          `username=eq.${encodeURIComponent(pick.player_username)}&select=username,nickname,streak,coins,mulligan_used,mulligan_streak_at_loss`
        );
        if (!player) {
          const msg = `player not found: ${pick.player_username}`;
          console.error(`[phase1] ${msg}`);
          summary.phase1.errors.push(msg);
          continue;
        }

        const prevStreak = player.streak || 0;

        // ── Mark pick resolved (idempotent — we already confirmed pending above)
        await sbPatch('pick_history', `id=eq.${pick.id}`, { result });

        if (result === 'hit') {
          const newStreak = prevStreak + 1;
          const patch = pick.is_bonus
            ? { streak: newStreak }
            : { streak: newStreak, today_pick: null, today_pick_id: null, today_pick_gamepk: null, pick_locked_at: null };
          // Fresh streak starting: clear mulligan_used so a new streak can
          // earn a fresh mulligan (mirrors client-side JIMMY bug fix).
          if (prevStreak === 0) patch.mulligan_used = false;
          await sbPatch('players', `username=eq.${encodeURIComponent(pick.player_username)}`, patch);
          summary.phase1.resolved_hit++;
          console.log(`[phase1] HIT  — ${pick.player_username} streak ${prevStreak} → ${newStreak}`);

        } else {
          // no_hit
          const newCoins    = Math.max(0, (player.coins || 0) - 1);
          const mulEligible = !player.mulligan_used && prevStreak >= 10 && prevStreak <= 29;
          const newStatus   = newCoins > 0 || mulEligible ? 'active' : 'locked';
          // Store the streak-before-loss so accept_mulligan can restore it
          const mulStreakAtLoss = mulEligible
            ? prevStreak
            : (player.mulligan_streak_at_loss || 0);

          const patch = {
            streak: 0,
            coins:  newCoins,
            status: newStatus,
            mulligan_eligible:     mulEligible,
            mulligan_used:         false,
            mulligan_streak_at_loss: mulStreakAtLoss,
          };
          if (!pick.is_bonus) {
            patch.today_pick        = null;
            patch.today_pick_id     = null;
            patch.today_pick_gamepk = null;
            patch.pick_locked_at    = null;
          }
          await sbPatch('players', `username=eq.${encodeURIComponent(pick.player_username)}`, patch);

          // Archive peak streak for leaderboard
          if (prevStreak > 0) {
            await sbPost('leaderboard_archive', {
              nickname:   player.nickname || player.username,
              max_streak: prevStreak,
            }).catch(e => console.warn(`[phase1] leaderboard_archive failed for ${pick.player_username}: ${e.message}`));
          }
          summary.phase1.resolved_no_hit++;
          console.log(`[phase1] NO HIT — ${pick.player_username} streak ${prevStreak} → 0, coins → ${newCoins}`);
        }

      } catch (e) {
        const msg = `pick ${pick.id} (${pick.player_username}): ${e.message}`;
        console.error(`[phase1] Error — ${msg}`);
        summary.phase1.errors.push(msg);
        // Continue to the next pick — do not corrupt other players' data
      }
    }
  } catch (e) {
    // Fatal Phase 1 error (e.g. Supabase unreachable). Log and fall through
    // to Phase 2 so at least the missed-pick sweep runs.
    const msg = `fatal query error: ${e.message}`;
    console.error(`[phase1] ${msg}`);
    summary.phase1.errors.push(msg);
  }

  // ════════════════════════════════════════════════════════════════════════════
  // PHASE 2 — Missed pick sweep
  // ════════════════════════════════════════════════════════════════════════════
  try {
    // Fetch all active non-admin players and all pick_history rows for yesterday
    // in two parallel queries to minimize round-trip time.
    const [activePlayers, yesterdayPickRows] = await Promise.all([
      sbGet('players',
        'role=neq.admin&status=eq.active&select=username,nickname,streak,coins,mulligan_used,mulligan_streak_at_loss'),
      sbGet('pick_history',
        `pick_date=eq.${yesterday}&select=player_username`),
    ]);

    const pickedSet = new Set(yesterdayPickRows.map(r => r.player_username));
    const missed    = activePlayers.filter(p => !pickedSet.has(p.username));
    console.log(`[phase2] ${activePlayers.length} active players, ${pickedSet.size} picked, ${missed.length} missed`);

    for (const player of missed) {
      try {
        const prevStreak  = player.streak || 0;
        const newCoins    = Math.max(0, (player.coins || 0) - 1);
        const mulEligible = !player.mulligan_used && prevStreak >= 10 && prevStreak <= 29;
        const newStatus   = newCoins > 0 || mulEligible ? 'active' : 'locked';
        const mulStreakAtLoss = mulEligible
          ? prevStreak
          : (player.mulligan_streak_at_loss || 0);

        // Insert synthetic row first — uniqueness conflict = already swept
        await sbPost('pick_history', {
          player_username: player.username,
          pick_date:       yesterday,
          batter_name:     '(No Pick)',
          batter_id:       '0',
          game_pk:         '0',
          is_bonus:        false,
          result:          'no_hit',
        });

        await sbPatch('players', `username=eq.${encodeURIComponent(player.username)}`, {
          streak:  0,
          coins:   newCoins,
          status:  newStatus,
          today_pick:        null,
          today_pick_id:     null,
          today_pick_gamepk: null,
          pick_locked_at:    null,
          mulligan_eligible:       mulEligible,
          mulligan_used:           false,
          mulligan_streak_at_loss: mulStreakAtLoss,
        });

        if (prevStreak > 0) {
          await sbPost('leaderboard_archive', {
            nickname:   player.nickname || player.username,
            max_streak: prevStreak,
          }).catch(e => console.warn(`[phase2] leaderboard_archive failed for ${player.username}: ${e.message}`));
        }

        summary.phase2.swept++;
        console.log(`[phase2] SWEPT — ${player.username} streak ${prevStreak} → 0, coins → ${newCoins}`);

      } catch (e) {
        if (e.isDuplicate) {
          // Synthetic row already exists — this player was already swept
          summary.phase2.skipped++;
          console.log(`[phase2] ${player.username} already swept (duplicate row)`);
        } else {
          const msg = `${player.username}: ${e.message}`;
          console.error(`[phase2] Error — ${msg}`);
          summary.phase2.errors.push(msg);
        }
      }
    }
  } catch (e) {
    const msg = `fatal query error: ${e.message}`;
    console.error(`[phase2] ${msg}`);
    summary.phase2.errors.push(msg);
  }

  console.log(`[nightly-processor] Finished: ${JSON.stringify(summary)}`);
  return res.status(200).json({ ok: true, ...summary });
};
