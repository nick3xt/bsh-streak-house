'use strict';
// api/subscribe.js — stores Web Push subscriptions in Supabase (no npm deps)
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

function sbHeaders() {
  return {
    apikey: SUPABASE_KEY,
    Authorization: `Bearer ${SUPABASE_KEY}`,
    'Content-Type': 'application/json',
  };
}

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { endpoint, keys, userId } = req.body || {};
  if (!endpoint || !keys || !keys.p256dh || !keys.auth) {
    return res.status(400).json({ error: 'Invalid subscription object' });
  }

  // Upsert on endpoint conflict
  const r = await fetch(`${SUPABASE_URL}/rest/v1/push_subscriptions`, {
    method: 'POST',
    headers: {
      ...sbHeaders(),
      Prefer: 'resolution=merge-duplicates,return=minimal',
    },
    body: JSON.stringify({
      endpoint,
      p256dh: keys.p256dh,
      auth:   keys.auth,
      user_id: userId || null,
    }),
  });

  if (!r.ok) {
    const body = await r.text();
    console.error('[subscribe] Supabase error:', r.status, body);
    return res.status(500).json({ error: body });
  }

  return res.status(201).json({ ok: true });
};
