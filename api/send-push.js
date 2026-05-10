'use strict';
const webpush = require('web-push');
const { createClient } = require('@supabase/supabase-js');

const VAPID_PUBLIC  = 'BCZ30nDna_ZDjDNzpBdBVmjjD9aPI2LEaEA33KUpN1h-cEiArWYJBL6fvlyCJKKiOdt0xh8VNiVS0i5f8eMqzt0';
const VAPID_PRIVATE = '52Asy1FGidsIP08lopn_gszBgxwd2ek5jOZjgBbpziM';

webpush.setVapidDetails(
  'mailto:admin@42streakhouse.com',
  VAPID_PUBLIC,
  VAPID_PRIVATE
);

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { title, body, url, userId } = req.body || {};

  const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_KEY
  );

  let query = supabase.from('push_subscriptions').select('*');
  if (userId) query = query.eq('user_id', userId);

  const { data: subs, error } = await query;
  if (error) {
    console.error('[send-push] Supabase error:', error.message);
    return res.status(500).json({ error: error.message });
  }

  if (!subs || !subs.length) {
    return res.status(200).json({ ok: true, sent: 0, note: 'no subscriptions' });
  }

  const payload = JSON.stringify({ title, body, url });

  const results = await Promise.allSettled(
    subs.map(async (sub) => {
      try {
        await webpush.sendNotification(
          { endpoint: sub.endpoint, keys: { p256dh: sub.p256dh, auth: sub.auth } },
          payload
        );
      } catch (e) {
        if (e.statusCode === 410 || e.statusCode === 404) {
          await supabase.from('push_subscriptions').delete().eq('endpoint', sub.endpoint);
          console.log('[send-push] Removed expired subscription:', sub.endpoint.slice(0, 60));
        }
        throw e;
      }
    })
  );

  const sent  = results.filter(r => r.status === 'fulfilled').length;
  const failed = results.filter(r => r.status === 'rejected').length;
  console.log('[send-push] sent=' + sent + ' failed=' + failed);
  return res.status(200).json({ ok: true, sent, failed });
};
