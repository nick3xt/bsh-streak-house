'use strict';
// api/send-push.js — sends Web Push notifications using Node.js built-ins only.
// Implements VAPID (RFC 8292) + aes128gcm payload encryption (RFC 8291 / RFC 8188).
// No npm dependencies required.

const nodeCrypto = require('crypto');
const { subtle }  = nodeCrypto.webcrypto;

// ── VAPID keys (pre-generated P-256 key pair) ──────────────────────────────
const VAPID_PUB   = 'BCZ30nDna_ZDjDNzpBdBVmjjD9aPI2LEaEA33KUpN1h-cEiArWYJBL6fvlyCJKKiOdt0xh8VNiVS0i5f8eMqzt0';
const VAPID_PRIV  = '52Asy1FGidsIP08lopn_gszBgxwd2ek5jOZjgBbpziM';
const VAPID_EMAIL = 'mailto:admin@42streakhouse.com';

// ── Supabase ───────────────────────────────────────────────────────────────
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

function sbHeaders() {
  return { apikey: SUPABASE_KEY, Authorization: `Bearer ${SUPABASE_KEY}` };
}

// ── Base64url helpers ───────────────────────────────────────────────────────
function b64u(buf)  { return Buffer.from(buf).toString('base64url'); }
function unb64u(s)  { return Buffer.from(s, 'base64url'); }

// ── HKDF (SHA-256) ─────────────────────────────────────────────────────────
async function hkdfExtract(salt, ikm) {
  const k = await subtle.importKey('raw', salt, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  return Buffer.from(await subtle.sign('HMAC', k, ikm));
}

async function hkdfExpand(prk, info, len) {
  const k = await subtle.importKey('raw', prk, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  let T = Buffer.alloc(0), out = Buffer.alloc(len), pos = 0, i = 1;
  while (pos < len) {
    T   = Buffer.from(await subtle.sign('HMAC', k, Buffer.concat([T, info, Buffer.from([i++])])));
    T.copy(out, pos, 0, Math.min(T.length, len - pos));
    pos += T.length;
  }
  return out;
}

// ── VAPID JWT (ES256) ───────────────────────────────────────────────────────
async function createVapidJwt(endpoint) {
  const { origin } = new URL(endpoint);
  const exp = Math.floor(Date.now() / 1000) + 43200; // 12 h

  const header  = b64u(JSON.stringify({ typ: 'JWT', alg: 'ES256' }));
  const payload = b64u(JSON.stringify({ aud: origin, exp, sub: VAPID_EMAIL }));
  const unsigned = `${header}.${payload}`;

  // Reconstruct EC public key x,y from uncompressed point (04 || x || y)
  const pubBuf = unb64u(VAPID_PUB);
  const x = b64u(pubBuf.slice(1, 33));
  const y = b64u(pubBuf.slice(33, 65));

  const privKey = await subtle.importKey(
    'jwk',
    { kty: 'EC', crv: 'P-256', d: VAPID_PRIV, x, y },
    { name: 'ECDSA', namedCurve: 'P-256' },
    false, ['sign']
  );

  // subtle.sign returns IEEE P1363 format (r||s, 64 bytes) — correct for ES256 JWT
  const sig = await subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, privKey, Buffer.from(unsigned));
  return `${unsigned}.${b64u(sig)}`;
}

// ── Web Push payload encryption (RFC 8291 + RFC 8188 aes128gcm) ────────────
async function encryptPayload(p256dh, auth, plaintext) {
  const authBuf       = unb64u(auth);
  const receiverPubBuf = unb64u(p256dh);

  // Import receiver's (UA) public key
  const receiverPubKey = await subtle.importKey(
    'raw', receiverPubBuf, { name: 'ECDH', namedCurve: 'P-256' }, false, []
  );

  // Ephemeral sender (AS) ECDH key pair
  const { privateKey: senderPriv, publicKey: senderPub } = await subtle.generateKey(
    { name: 'ECDH', namedCurve: 'P-256' }, true, ['deriveBits']
  );
  const senderPubBuf = Buffer.from(await subtle.exportKey('raw', senderPub)); // 65 bytes

  // ECDH shared secret
  const ecdhBits   = await subtle.deriveBits({ name: 'ECDH', public: receiverPubKey }, senderPriv, 256);
  const ecdhSecret  = Buffer.from(ecdhBits);

  const salt = nodeCrypto.randomBytes(16);

  // RFC 8291 §3.4 key derivation
  // PRK_key  = HKDF-Extract(auth_secret, ecdh_secret)
  const prkKey = await hkdfExtract(authBuf, ecdhSecret);

  // key_info = "WebPush: info\x00" || ua_public (65 B) || as_public (65 B)
  const keyInfo = Buffer.concat([
    Buffer.from('WebPush: info\x00'),
    receiverPubBuf,
    senderPubBuf,
  ]);
  // IKM = HKDF-Expand(PRK_key, key_info, 32)
  const ikm = await hkdfExpand(prkKey, keyInfo, 32);

  // PRK = HKDF-Extract(salt, IKM)
  const prk = await hkdfExtract(salt, ikm);

  const cek   = await hkdfExpand(prk, Buffer.from('Content-Encoding: aes128gcm\x00'), 16);
  const nonce = await hkdfExpand(prk, Buffer.from('Content-Encoding: nonce\x00'),     12);

  // AES-128-GCM encrypt — append record delimiter 0x02 (no padding)
  const cekKey   = await subtle.importKey('raw', cek, { name: 'AES-GCM' }, false, ['encrypt']);
  const padded   = Buffer.concat([Buffer.from(plaintext), Buffer.from([0x02])]);
  const encrypted = Buffer.from(
    await subtle.encrypt({ name: 'AES-GCM', iv: nonce, tagLength: 128 }, cekKey, padded)
  );

  // RFC 8188 content-coding header: salt(16) + rs(4 BE) + idlen(1) + keyid(65)
  const rsHdr = Buffer.alloc(4);
  rsHdr.writeUInt32BE(4096, 0);
  const encHeader = Buffer.concat([salt, rsHdr, Buffer.from([senderPubBuf.length]), senderPubBuf]);

  return Buffer.concat([encHeader, encrypted]);
}

// ── Send one notification ───────────────────────────────────────────────────
async function sendPush(sub, data) {
  const { endpoint, p256dh, auth } = sub;
  const [jwt, body] = await Promise.all([
    createVapidJwt(endpoint),
    encryptPayload(p256dh, auth, JSON.stringify(data)),
  ]);

  const r = await fetch(endpoint, {
    method: 'POST',
    headers: {
      Authorization:      `vapid t=${jwt}, k=${VAPID_PUB}`,
      'Content-Type':     'application/octet-stream',
      'Content-Encoding': 'aes128gcm',
      TTL:                '86400',
    },
    body,
  });

  if (r.status === 410 || r.status === 404) {
    // Subscription expired — delete it
    await fetch(
      `${SUPABASE_URL}/rest/v1/push_subscriptions?endpoint=eq.${encodeURIComponent(endpoint)}`,
      { method: 'DELETE', headers: sbHeaders() }
    ).catch(() => {});
    console.log('[send-push] Removed expired sub:', endpoint.slice(0, 60));
  }

  return r.status;
}

// ── Handler ─────────────────────────────────────────────────────────────────
module.exports = async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { title, body, url, userId } = req.body || {};

  let qs = 'select=endpoint,p256dh,auth';
  if (userId) qs += `&user_id=eq.${encodeURIComponent(userId)}`;

  const sbR = await fetch(`${SUPABASE_URL}/rest/v1/push_subscriptions?${qs}`, {
    headers: sbHeaders(),
  });

  if (!sbR.ok) {
    const txt = await sbR.text();
    console.error('[send-push] Supabase error:', sbR.status, txt);
    return res.status(500).json({ error: txt });
  }

  const subs = await sbR.json();
  if (!subs.length) {
    return res.status(200).json({ ok: true, sent: 0, note: 'no subscriptions' });
  }

  const results = await Promise.allSettled(
    subs.map(sub => sendPush(sub, { title, body, url }))
  );

  const sent   = results.filter(r => r.status === 'fulfilled').length;
  const failed = results.filter(r => r.status === 'rejected').length;
  console.log(`[send-push] sent=${sent} failed=${failed}`);
  return res.status(200).json({ ok: true, sent, failed });
};
