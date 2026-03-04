require('dotenv').config();

const crypto = require('crypto');
const http = require('http');
const path = require('path');
const { URL } = require('url');

const express = require('express');
const geoip = require('geoip-lite');
const { Pool } = require('pg');
const { createClient } = require('redis');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL;
const REDIS_URL = process.env.REDIS_URL;
const REDIS_PREFIX = process.env.REDIS_PREFIX || 'yungjewboii_global_chat';
const DATABASE_SSL = process.env.DATABASE_SSL || 'false';
const ADMIN_DASHBOARD_PASSWORD = process.env.ADMIN_DASHBOARD_PASSWORD || 'badwolf';
const HTTP_RATE_LIMIT_WINDOW_SEC = Number(process.env.HTTP_RATE_LIMIT_WINDOW_SEC || 60);
const HTTP_RATE_LIMIT_MAX = Number(process.env.HTTP_RATE_LIMIT_MAX || 240);
const CLAIM_RATE_LIMIT_WINDOW_SEC = Number(process.env.CLAIM_RATE_LIMIT_WINDOW_SEC || 300);
const CLAIM_RATE_LIMIT_MAX = Number(process.env.CLAIM_RATE_LIMIT_MAX || 12);
const WS_MESSAGE_RATE_LIMIT_WINDOW_SEC = Number(process.env.WS_MESSAGE_RATE_LIMIT_WINDOW_SEC || 10);
const WS_MESSAGE_RATE_LIMIT_MAX = Number(process.env.WS_MESSAGE_RATE_LIMIT_MAX || 25);
const HONEYPOT_BLOCK_SECONDS = Number(process.env.HONEYPOT_BLOCK_SECONDS || 86400);
const PRESENCE_TTL_SECONDS = Number(process.env.PRESENCE_TTL_SECONDS || 120);
const PRESENCE_HEARTBEAT_SECONDS = Number(process.env.PRESENCE_HEARTBEAT_SECONDS || 25);
const COUNTRY_BLOCKLIST = String(process.env.COUNTRY_BLOCKLIST || 'CN')
  .split(',')
  .map((entry) => entry.trim().toUpperCase())
  .filter(Boolean);

if (!DATABASE_URL) {
  throw new Error('Missing DATABASE_URL environment variable.');
}

if (!REDIS_URL) {
  throw new Error('Missing REDIS_URL environment variable.');
}

const USERNAME_REGEX = /^[A-Za-z0-9_]{3,24}$/;
const MAX_MESSAGE_LENGTH = 2000;
const MAX_DESCRIPTION_LENGTH = 120;

const INSTANCE_ID = crypto.randomUUID();
const EVENTS_CHANNEL = `${REDIS_PREFIX}:events`;

const app = express();
app.set('trust proxy', true);
app.use(express.json({ limit: '64kb' }));

function parseBoolean(value, defaultValue = false) {
  if (value === undefined || value === null || value === '') {
    return defaultValue;
  }
  return ['1', 'true', 'yes', 'on'].includes(String(value).toLowerCase());
}

const useDatabaseSsl = parseBoolean(DATABASE_SSL, false);

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: useDatabaseSsl ? { rejectUnauthorized: false } : false
});

const redisPublisher = createClient({ url: REDIS_URL });
const redisSubscriber = createClient({ url: REDIS_URL });

const wsClients = new Map();
const subscribers = new Map();

function slugifyChannelName(input) {
  const cleaned = String(input || '')
    .trim()
    .replace(/^#+/, '')
    .toLowerCase()
    .replace(/[^a-z0-9\s_-]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^[-_]+|[-_]+$/g, '');

  if (!cleaned || cleaned.length < 2 || cleaned.length > 32) {
    return null;
  }

  return cleaned;
}

function sanitizeDescription(value) {
  if (typeof value !== 'string') {
    return '';
  }
  return value.trim().slice(0, MAX_DESCRIPTION_LENGTH);
}

function nowIso() {
  return new Date().toISOString();
}

function getTokenFromRequest(req) {
  const authHeader = req.headers.authorization;
  if (authHeader && authHeader.toLowerCase().startsWith('bearer ')) {
    return authHeader.slice(7).trim();
  }

  const xToken = req.headers['x-session-token'];
  if (typeof xToken === 'string' && xToken.trim()) {
    return xToken.trim();
  }

  return null;
}

function parseWsPayload(raw) {
  try {
    const parsed = JSON.parse(String(raw));
    if (parsed && typeof parsed === 'object') {
      return parsed;
    }
  } catch (error) {
    return null;
  }

  return null;
}

function sendJson(ws, payload) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function broadcastAll(payload) {
  for (const ws of wsClients.keys()) {
    sendJson(ws, payload);
  }
}

function broadcastChannel(slug, payload, exceptWs = null) {
  const channelSubscribers = subscribers.get(slug);
  if (!channelSubscribers) {
    return;
  }

  for (const ws of channelSubscribers) {
    if (exceptWs && ws === exceptWs) {
      continue;
    }
    sendJson(ws, payload);
  }
}

function presenceKey(slug) {
  return `${REDIS_PREFIX}:presence:${slug}`;
}

function presenceCutoffMs() {
  return Date.now() - PRESENCE_TTL_SECONDS * 1000;
}

async function ensurePresenceKeyIsZset(slug) {
  const key = presenceKey(slug);
  const currentType = await redisPublisher.type(key);
  if (currentType !== 'none' && currentType !== 'zset') {
    await redisPublisher.del(key);
  }
}

async function normalizePresenceKeys(slugs) {
  if (!slugs.length) {
    return;
  }

  const typePipeline = redisPublisher.multi();
  for (const slug of slugs) {
    typePipeline.type(presenceKey(slug));
  }
  const types = await typePipeline.exec();

  const toDelete = [];
  for (let i = 0; i < slugs.length; i += 1) {
    const keyType = String(types?.[i] || 'none');
    if (keyType !== 'none' && keyType !== 'zset') {
      toDelete.push(presenceKey(slugs[i]));
    }
  }

  if (toDelete.length) {
    await redisPublisher.del(toDelete);
  }
}

async function refreshPresenceHeartbeat(channel, presenceMember) {
  if (!channel || !presenceMember) {
    return;
  }

  await ensurePresenceKeyIsZset(channel);
  await redisPublisher.zAdd(presenceKey(channel), [{ score: Date.now(), value: presenceMember }]);
}

async function getPresenceCount(slug) {
  await ensurePresenceKeyIsZset(slug);
  const cutoff = presenceCutoffMs();
  const results = await redisPublisher
    .multi()
    .zRemRangeByScore(presenceKey(slug), '-inf', String(cutoff))
    .zCount(presenceKey(slug), String(cutoff), '+inf')
    .exec();

  return Number(results?.[1] || 0);
}

function blockedIpKey(ipAddress) {
  return `${REDIS_PREFIX}:blocked_ip:${ipAddress}`;
}

function botStrikeKey(ipAddress) {
  return `${REDIS_PREFIX}:bot_strikes:${ipAddress}`;
}

function rateLimitKey(bucket, identifier) {
  return `${REDIS_PREFIX}:rate:${bucket}:${identifier}`;
}

function fingerprintToken(token) {
  if (!token) {
    return null;
  }
  return crypto.createHash('sha256').update(token).digest('hex').slice(0, 24);
}

function trimValue(value, max = 512) {
  if (value === null || value === undefined) {
    return null;
  }
  const text = String(value);
  if (text.length <= max) {
    return text;
  }
  return `${text.slice(0, max)}...`;
}

function parseIpFromForwarded(forwarded) {
  if (!forwarded || typeof forwarded !== 'string') {
    return null;
  }

  const first = forwarded
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean)[0];

  return first || null;
}

function getClientIp(req) {
  const forwardedForRaw = req.headers['x-forwarded-for'];
  const forwardedFor = Array.isArray(forwardedForRaw)
    ? forwardedForRaw.join(',')
    : forwardedForRaw || null;

  return parseIpFromForwarded(forwardedFor) || req.ip || req.socket?.remoteAddress || 'unknown';
}

function normalizeIp(ipAddress) {
  if (!ipAddress) {
    return null;
  }
  const text = String(ipAddress).trim();
  if (text.startsWith('::ffff:')) {
    return text.slice(7);
  }
  return text;
}

function getCountryCodeForIp(ipAddress) {
  const normalized = normalizeIp(ipAddress);
  if (!normalized || normalized === 'unknown' || normalized === '::1' || normalized === '127.0.0.1') {
    return null;
  }
  const found = geoip.lookup(normalized);
  return found?.country || null;
}

function hasHoneypotPayload(req) {
  if (!req.body || typeof req.body !== 'object') {
    return false;
  }

  const trapValue = req.body.website || req.body.hp || req.body.honey || req.body.contact_email;
  return typeof trapValue === 'string' && trapValue.trim().length > 0;
}

function isSuspiciousPath(pathValue) {
  const pathText = String(pathValue || '').toLowerCase();
  const patterns = [
    '/wp-admin',
    '/wp-login',
    '/xmlrpc.php',
    '/phpmyadmin',
    '/.env',
    '/vendor/phpunit',
    '/autodiscover',
    '/boaform',
    '/actuator',
    '/hudson',
    '/jenkins'
  ];
  return patterns.some((pattern) => pathText.includes(pattern));
}

function isSuspiciousUserAgent(userAgent) {
  const ua = String(userAgent || '').toLowerCase();
  if (!ua) {
    return false;
  }

  const patterns = [
    'sqlmap',
    'nikto',
    'masscan',
    'nmap',
    'acunetix',
    'nessus',
    'zgrab',
    'python-requests',
    'scrapy',
    'httpclient',
    'go-http-client',
    'curl/',
    'wget/'
  ];

  return patterns.some((pattern) => ua.includes(pattern));
}

async function consumeRateLimit(bucket, identifier, windowSec, maxRequests) {
  const key = rateLimitKey(bucket, identifier);
  const results = await redisPublisher
    .multi()
    .incr(key)
    .expire(key, windowSec, 'NX')
    .ttl(key)
    .exec();

  const count = Number(results?.[0] || 0);
  const ttl = Number(results?.[2] || windowSec);

  return {
    allowed: count <= maxRequests,
    count,
    ttl
  };
}

async function blockIp(ipAddress, seconds, reason) {
  if (!ipAddress || ipAddress === 'unknown') {
    return;
  }
  await redisPublisher.set(blockedIpKey(ipAddress), reason || 'blocked', { EX: seconds });
}

async function isIpBlocked(ipAddress) {
  if (!ipAddress || ipAddress === 'unknown') {
    return false;
  }
  const value = await redisPublisher.get(blockedIpKey(ipAddress));
  return Boolean(value);
}

function sanitizeHeaders(headers) {
  const blocked = new Set(['authorization', 'cookie', 'x-admin-password']);
  const safe = {};

  for (const [key, value] of Object.entries(headers || {})) {
    const lower = key.toLowerCase();
    if (blocked.has(lower)) {
      continue;
    }

    if (Array.isArray(value)) {
      safe[lower] = value.map((entry) => trimValue(entry, 512));
    } else {
      safe[lower] = trimValue(value, 512);
    }
  }

  return safe;
}

function sanitizeBody(body) {
  if (!body || typeof body !== 'object') {
    return null;
  }

  const json = JSON.stringify(body);
  if (!json) {
    return null;
  }

  if (json.length <= 4000) {
    return JSON.parse(json);
  }

  return {
    truncated: true,
    preview: `${json.slice(0, 4000)}...`
  };
}

async function insertSiteEvent(event) {
  const eventId = crypto.randomUUID();
  await pool.query(
    `
      INSERT INTO site_events (
        id,
        event_type,
        event_time,
        instance_id,
        ip_address,
        forwarded_for,
        remote_address,
        method,
        path,
        status_code,
        duration_ms,
        username,
        username_key,
        user_agent,
        referer,
        origin,
        accept_language,
        sec_ch_ua,
        sec_ch_ua_mobile,
        sec_ch_ua_platform,
        token_fingerprint,
        headers_json,
        body_json,
        meta_json
      )
      VALUES (
        $1,
        $2,
        COALESCE($3::timestamptz, NOW()),
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        $14,
        $15,
        $16,
        $17,
        $18,
        $19,
        $20,
        $21,
        $22::jsonb,
        $23::jsonb,
        $24::jsonb
      )
    `,
    [
      eventId,
      event.eventType,
      event.eventTime || null,
      INSTANCE_ID,
      event.ipAddress || null,
      event.forwardedFor || null,
      event.remoteAddress || null,
      event.method || null,
      event.path || null,
      event.statusCode ?? null,
      event.durationMs ?? null,
      event.username || null,
      event.usernameKey || null,
      event.userAgent || null,
      event.referer || null,
      event.origin || null,
      event.acceptLanguage || null,
      event.secChUa || null,
      event.secChUaMobile || null,
      event.secChUaPlatform || null,
      event.tokenFingerprint || null,
      JSON.stringify(event.headers || null),
      JSON.stringify(event.body || null),
      JSON.stringify(event.meta || null)
    ]
  );
}

function queueSiteEvent(event) {
  insertSiteEvent(event).catch((error) => {
    console.error('Failed to insert site event:', error);
  });
}

function buildHttpEvent(req, extras = {}) {
  const forwardedForRaw = req.headers['x-forwarded-for'];
  const forwardedFor = Array.isArray(forwardedForRaw)
    ? forwardedForRaw.join(',')
    : forwardedForRaw || null;

  return {
    eventType: extras.eventType || 'http_request',
    eventTime: extras.eventTime || nowIso(),
    ipAddress: parseIpFromForwarded(forwardedFor) || req.ip || null,
    forwardedFor: trimValue(forwardedFor, 512),
    remoteAddress: req.socket?.remoteAddress || null,
    method: req.method,
    path: req.originalUrl,
    statusCode: extras.statusCode ?? null,
    durationMs: extras.durationMs ?? null,
    username: extras.username || req.auth?.username || null,
    usernameKey: extras.usernameKey || req.auth?.usernameKey || null,
    userAgent: trimValue(req.headers['user-agent'], 1024),
    referer: trimValue(req.headers.referer || req.headers.referrer, 1024),
    origin: trimValue(req.headers.origin, 1024),
    acceptLanguage: trimValue(req.headers['accept-language'], 1024),
    secChUa: trimValue(req.headers['sec-ch-ua'], 1024),
    secChUaMobile: trimValue(req.headers['sec-ch-ua-mobile'], 256),
    secChUaPlatform: trimValue(req.headers['sec-ch-ua-platform'], 256),
    tokenFingerprint: fingerprintToken(getTokenFromRequest(req)),
    headers: sanitizeHeaders(req.headers),
    body: extras.bodyOverride !== undefined ? extras.bodyOverride : sanitizeBody(req.body),
    meta: extras.meta || null
  };
}

function buildWsEvent(req, extras = {}) {
  const forwardedForRaw = req.headers['x-forwarded-for'];
  const forwardedFor = Array.isArray(forwardedForRaw)
    ? forwardedForRaw.join(',')
    : forwardedForRaw || null;

  return {
    eventType: extras.eventType,
    eventTime: nowIso(),
    ipAddress: parseIpFromForwarded(forwardedFor) || req.socket?.remoteAddress || null,
    forwardedFor: trimValue(forwardedFor, 512),
    remoteAddress: req.socket?.remoteAddress || null,
    method: extras.method || 'WS',
    path: extras.path || req.url,
    statusCode: extras.statusCode ?? null,
    durationMs: extras.durationMs ?? null,
    username: extras.username || null,
    usernameKey: extras.usernameKey || null,
    userAgent: trimValue(req.headers['user-agent'], 1024),
    referer: trimValue(req.headers.referer || req.headers.referrer, 1024),
    origin: trimValue(req.headers.origin, 1024),
    acceptLanguage: trimValue(req.headers['accept-language'], 1024),
    secChUa: trimValue(req.headers['sec-ch-ua'], 1024),
    secChUaMobile: trimValue(req.headers['sec-ch-ua-mobile'], 256),
    secChUaPlatform: trimValue(req.headers['sec-ch-ua-platform'], 256),
    tokenFingerprint: fingerprintToken(extras.token || null),
    headers: sanitizeHeaders(req.headers),
    body: extras.body ?? null,
    meta: extras.meta || null
  };
}

app.use((req, res, next) => {
  const startedAt = Date.now();

  res.on('finish', () => {
    queueSiteEvent(
      buildHttpEvent(req, {
        eventType: 'http_request',
        statusCode: res.statusCode,
        durationMs: Date.now() - startedAt,
        meta: {
          protocol: req.protocol,
          host: req.headers.host || null
        }
      })
    );
  });

  next();
});

app.use(async (req, res, next) => {
  try {
    const clientIp = getClientIp(req);
    const countryCode = getCountryCodeForIp(clientIp);
    const isAdminRoute = req.path.startsWith('/api/admin/');
    const hasValidAdminPassword = String(req.headers['x-admin-password'] || '') === ADMIN_DASHBOARD_PASSWORD;

    if (await isIpBlocked(clientIp) && !(isAdminRoute && hasValidAdminPassword)) {
      res.status(403).json({ error: 'forbidden', message: 'Access denied.' });
      return;
    }

    if (countryCode && COUNTRY_BLOCKLIST.includes(countryCode) && !(isAdminRoute && hasValidAdminPassword)) {
      queueSiteEvent(
        buildHttpEvent(req, {
          eventType: 'country_blocked',
          statusCode: 403,
          meta: {
            countryCode,
            countryBlocklist: COUNTRY_BLOCKLIST
          }
        })
      );
      res.status(403).json({ error: 'forbidden', message: 'Access denied.' });
      return;
    }

    const suspiciousUa = isSuspiciousUserAgent(req.headers['user-agent']);
    const suspiciousPath = isSuspiciousPath(req.originalUrl);

    if (suspiciousUa || suspiciousPath) {
      const strikes = Number(await redisPublisher.incr(botStrikeKey(clientIp)));
      await redisPublisher.expire(botStrikeKey(clientIp), 86400, 'NX');

      if (suspiciousPath || strikes >= 2) {
        await blockIp(clientIp, HONEYPOT_BLOCK_SECONDS, 'bot_filtered');

        queueSiteEvent(
          buildHttpEvent(req, {
            eventType: 'bot_filtered',
            statusCode: 403,
            meta: {
              suspiciousUa,
              suspiciousPath,
              strikes
            }
          })
        );

        res.status(403).json({ error: 'forbidden', message: 'Bot traffic blocked.' });
        return;
      }
    }

    if (req.path.startsWith('/api/')) {
      const rate = await consumeRateLimit('http_api', clientIp, HTTP_RATE_LIMIT_WINDOW_SEC, HTTP_RATE_LIMIT_MAX);
      if (!rate.allowed) {
        res.setHeader('Retry-After', String(Math.max(rate.ttl, 1)));
        res.status(429).json({ error: 'rate_limited', message: 'Too many requests. Slow down.' });
        return;
      }
    }

    next();
  } catch (error) {
    next(error);
  }
});

app.use(express.static(path.join(__dirname, 'public')));

async function publishEvent(type, payload) {
  await redisPublisher.publish(
    EVENTS_CHANNEL,
    JSON.stringify({
      type,
      origin: INSTANCE_ID,
      payload
    })
  );
}

async function getSessionByToken(token) {
  const result = await pool.query(
    `
      SELECT s.username_key, u.username_original AS username
      FROM sessions s
      INNER JOIN user_claims u ON u.username_key = s.username_key
      WHERE s.token = $1
      LIMIT 1
    `,
    [token]
  );

  return result.rows[0] || null;
}

async function requireAuth(req, res, next) {
  try {
    const token = getTokenFromRequest(req);
    if (!token) {
      res.status(401).json({ error: 'unauthorized' });
      return;
    }

    const session = await getSessionByToken(token);
    if (!session) {
      res.status(401).json({ error: 'unauthorized' });
      return;
    }

    req.auth = {
      token,
      username: session.username,
      usernameKey: session.username_key
    };

    next();
  } catch (error) {
    next(error);
  }
}

function requireAdminPassword(req, res) {
  const providedPassword = String(req.headers['x-admin-password'] || '');
  if (providedPassword !== ADMIN_DASHBOARD_PASSWORD) {
    res.status(401).json({ error: 'unauthorized', message: 'Invalid admin password.' });
    return false;
  }
  return true;
}

async function getChannelRows() {
  const result = await pool.query(
    `
      SELECT slug, name, description, created_at
      FROM channels
      ORDER BY created_at ASC
    `
  );

  return result.rows;
}

async function getChannelCountsBySlug(slugs) {
  if (!slugs.length) {
    return {};
  }

  await normalizePresenceKeys(slugs);
  const pipeline = redisPublisher.multi();
  const cutoff = presenceCutoffMs();
  for (const slug of slugs) {
    pipeline.zRemRangeByScore(presenceKey(slug), '-inf', String(cutoff));
    pipeline.zCount(presenceKey(slug), String(cutoff), '+inf');
  }

  const rawCounts = await pipeline.exec();
  const counts = {};

  for (let i = 0; i < slugs.length; i += 1) {
    const value = rawCounts?.[i * 2 + 1];
    counts[slugs[i]] = Number(value || 0);
  }

  return counts;
}

async function broadcastCounts() {
  const channels = await getChannelRows();
  const slugs = channels.map((channel) => channel.slug);
  const counts = await getChannelCountsBySlug(slugs);
  const payload = { type: 'channel_counts', counts };

  broadcastAll(payload);
  await publishEvent('channel_counts', payload);
}

async function emitPresence(slug, action, username) {
  const onlineCount = await getPresenceCount(slug);

  const payload = {
    type: 'presence',
    channel: slug,
    action,
    username,
    onlineCount,
    timestamp: nowIso()
  };

  broadcastChannel(slug, payload);
  await publishEvent('presence', payload);
}

async function detachFromChannel(ws, reason = 'leave') {
  const meta = wsClients.get(ws);
  if (!meta || !meta.channel) {
    return;
  }

  const currentChannel = meta.channel;
  const set = subscribers.get(currentChannel);
  if (set) {
    set.delete(ws);
    if (set.size === 0) {
      subscribers.delete(currentChannel);
    }
  }

  if (meta.presenceMember) {
    await redisPublisher.zRem(presenceKey(currentChannel), meta.presenceMember);
  }

  meta.channel = null;
  meta.presenceMember = null;

  await emitPresence(currentChannel, reason, meta.username);
  await broadcastCounts();
}

async function attachToChannel(ws, slug) {
  const meta = wsClients.get(ws);
  if (!meta) {
    return;
  }

  if (meta.channel === slug) {
    return;
  }

  if (meta.channel) {
    await detachFromChannel(ws, 'switch');
  }

  if (!subscribers.has(slug)) {
    subscribers.set(slug, new Set());
  }

  subscribers.get(slug).add(ws);
  meta.channel = slug;
  meta.presenceMember = `${INSTANCE_ID}:${meta.connectionId}`;

  await refreshPresenceHeartbeat(slug, meta.presenceMember);

  await emitPresence(slug, 'join', meta.username);
  await broadcastCounts();
}

async function handleRedisEvent(message) {
  let event;

  try {
    event = JSON.parse(message);
  } catch (error) {
    return;
  }

  if (!event || event.origin === INSTANCE_ID || !event.type || !event.payload) {
    return;
  }

  if (event.type === 'channel_counts') {
    broadcastAll(event.payload);
    return;
  }

  if (event.type === 'channel_created') {
    broadcastAll(event.payload);
    return;
  }

  if (event.type === 'channel_deleted') {
    broadcastAll(event.payload);
    return;
  }

  if (event.type === 'presence') {
    broadcastChannel(event.payload.channel, event.payload);
    return;
  }

  if (event.type === 'typing') {
    broadcastChannel(event.payload.channel, event.payload);
    return;
  }

  if (event.type === 'message') {
    broadcastChannel(event.payload.channel, event.payload);
  }
}

app.post('/api/claim', async (req, res, next) => {
  const clientIp = getClientIp(req);

  if (hasHoneypotPayload(req)) {
    await blockIp(clientIp, HONEYPOT_BLOCK_SECONDS, 'honeypot_payload').catch(() => {});

    queueSiteEvent(
      buildHttpEvent(req, {
        eventType: 'honeypot_payload',
        statusCode: 400,
        meta: {
          blockedSeconds: HONEYPOT_BLOCK_SECONDS
        }
      })
    );

    res.status(400).json({ error: 'invalid_request', message: 'Invalid request.' });
    return;
  }

  const claimRate = await consumeRateLimit('claim', clientIp, CLAIM_RATE_LIMIT_WINDOW_SEC, CLAIM_RATE_LIMIT_MAX);
  if (!claimRate.allowed) {
    res.setHeader('Retry-After', String(Math.max(claimRate.ttl, 1)));
    res.status(429).json({ error: 'rate_limited', message: 'Too many claim attempts. Try again later.' });
    return;
  }

  const requested = typeof req.body?.username === 'string' ? req.body.username.trim() : '';

  if (!USERNAME_REGEX.test(requested)) {
    res.status(400).json({ error: 'invalid_username', message: '3–24 characters, letters/numbers/underscores only.' });
    return;
  }

  const usernameKey = requested.toLowerCase();
  const token = crypto.randomBytes(32).toString('hex');
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const claimResult = await client.query(
      `
        INSERT INTO user_claims (username_key, username_original)
        VALUES ($1, $2)
        ON CONFLICT (username_key) DO NOTHING
        RETURNING username_key
      `,
      [usernameKey, requested]
    );

    if (claimResult.rowCount === 0) {
      await client.query('ROLLBACK');
      res.status(409).json({ error: 'username_taken', message: 'That username is taken.' });
      return;
    }

    await client.query(
      `
        INSERT INTO sessions (token, username_key)
        VALUES ($1, $2)
      `,
      [token, usernameKey]
    );

    await client.query('COMMIT');

    queueSiteEvent(
      buildHttpEvent(req, {
        eventType: 'username_claimed',
        username: requested,
        usernameKey,
        meta: {
          claimSuccess: true
        }
      })
    );

    res.status(201).json({ token, username: requested });
  } catch (error) {
    await client.query('ROLLBACK');
    next(error);
  } finally {
    client.release();
  }
});

app.get('/api/session', requireAuth, (req, res) => {
  queueSiteEvent(
    buildHttpEvent(req, {
      eventType: 'session_validated',
      username: req.auth.username,
      usernameKey: req.auth.usernameKey,
      meta: {
        autoLogin: true
      }
    })
  );

  res.json({ username: req.auth.username });
});

app.post('/api/release', requireAuth, async (req, res, next) => {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    await client.query('DELETE FROM sessions WHERE username_key = $1', [req.auth.usernameKey]);
    await client.query('DELETE FROM user_claims WHERE username_key = $1', [req.auth.usernameKey]);

    await client.query('COMMIT');

    queueSiteEvent(
      buildHttpEvent(req, {
        eventType: 'username_released',
        username: req.auth.username,
        usernameKey: req.auth.usernameKey,
        meta: {
          releasedByUser: true
        }
      })
    );

    for (const [ws, meta] of wsClients.entries()) {
      if (meta.token === req.auth.token) {
        sendJson(ws, { type: 'session_revoked' });
        ws.close(4401, 'session revoked');
      }
    }

    res.json({ ok: true });
  } catch (error) {
    await client.query('ROLLBACK');
    next(error);
  } finally {
    client.release();
  }
});

app.get('/api/channels', requireAuth, async (req, res, next) => {
  try {
    const rows = await getChannelRows();
    const slugs = rows.map((row) => row.slug);
    const counts = await getChannelCountsBySlug(slugs);

    const channels = rows.map((row) => ({
      slug: row.slug,
      name: row.name,
      description: row.description,
      createdAt: new Date(row.created_at).toISOString(),
      onlineCount: counts[row.slug] || 0
    }));

    res.json({ channels });
  } catch (error) {
    next(error);
  }
});

app.post('/api/channels', requireAuth, async (req, res, next) => {
  const slug = slugifyChannelName(req.body?.name || '');
  if (!slug) {
    res.status(400).json({ error: 'invalid_channel_name', message: 'Channel names must slugify to 2-32 chars.' });
    return;
  }

  const channel = {
    slug,
    name: `#${slug}`,
    description: sanitizeDescription(req.body?.description),
    createdAt: nowIso()
  };

  try {
    await pool.query(
      `
        INSERT INTO channels (slug, name, description, created_at)
        VALUES ($1, $2, $3, $4)
      `,
      [channel.slug, channel.name, channel.description, channel.createdAt]
    );
  } catch (error) {
    if (error.code === '23505') {
      res.status(409).json({ error: 'channel_exists', message: 'A channel with that name already exists.' });
      return;
    }
    next(error);
    return;
  }

  const payload = {
    type: 'channel_created',
    channel: {
      ...channel,
      onlineCount: 0
    }
  };

  try {
    broadcastAll(payload);
    await publishEvent('channel_created', payload);
    await broadcastCounts();
    res.status(201).json({ channel });
  } catch (error) {
    next(error);
  }
});

app.get('/api/channels/:slug/messages', requireAuth, async (req, res, next) => {
  const slug = String(req.params.slug || '').toLowerCase();

  try {
    const channelResult = await pool.query('SELECT slug FROM channels WHERE slug = $1 LIMIT 1', [slug]);
    if (channelResult.rowCount === 0) {
      res.status(404).json({ error: 'not_found' });
      return;
    }

    const messagesResult = await pool.query(
      `
        SELECT id, channel_slug, username, text, created_at
        FROM messages
        WHERE channel_slug = $1
        ORDER BY created_at DESC
        LIMIT 100
      `,
      [slug]
    );

    const messages = messagesResult.rows
      .map((row) => ({
        id: row.id,
        channel: row.channel_slug,
        username: row.username,
        text: row.text,
        timestamp: new Date(row.created_at).toISOString()
      }))
      .reverse();

    res.json({ messages });
  } catch (error) {
    next(error);
  }
});

app.get('/api/admin/events', async (req, res, next) => {
  if (!requireAdminPassword(req, res)) {
    return;
  }

  const limitRaw = Number(req.query.limit || 250);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(1000, Math.floor(limitRaw))) : 250;

  try {
    const result = await pool.query(
      `
        SELECT
          id,
          event_type,
          event_time,
          ip_address,
          forwarded_for,
          remote_address,
          method,
          path,
          status_code,
          duration_ms,
          username,
          username_key,
          user_agent,
          referer,
          origin,
          accept_language,
          sec_ch_ua,
          sec_ch_ua_mobile,
          sec_ch_ua_platform,
          token_fingerprint,
          headers_json,
          body_json,
          meta_json,
          instance_id
        FROM site_events
        ORDER BY event_time DESC
        LIMIT $1
      `,
      [limit]
    );

    res.json({ events: result.rows });
  } catch (error) {
    next(error);
  }
});

app.get('/api/admin/events/non-us', async (req, res, next) => {
  if (!requireAdminPassword(req, res)) {
    return;
  }

  const limitRaw = Number(req.query.limit || 250);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(1000, Math.floor(limitRaw))) : 250;
  const queryLimit = Math.min(limit * 4, 5000);

  try {
    const result = await pool.query(
      `
        SELECT
          id,
          event_type,
          event_time,
          ip_address,
          forwarded_for,
          remote_address,
          method,
          path,
          status_code,
          duration_ms,
          username,
          username_key,
          user_agent,
          referer,
          origin,
          accept_language,
          sec_ch_ua,
          sec_ch_ua_mobile,
          sec_ch_ua_platform,
          token_fingerprint,
          headers_json,
          body_json,
          meta_json,
          instance_id
        FROM site_events
        ORDER BY event_time DESC
        LIMIT $1
      `,
      [queryLimit]
    );

    const events = [];
    for (const row of result.rows) {
      const candidateIp =
        normalizeIp(parseIpFromForwarded(row.forwarded_for)) ||
        normalizeIp(row.ip_address) ||
        normalizeIp(row.remote_address);
      const countryCode = getCountryCodeForIp(candidateIp);

      if (!countryCode || countryCode === 'US') {
        continue;
      }

      events.push({
        ...row,
        country_code: countryCode
      });

      if (events.length >= limit) {
        break;
      }
    }

    res.json({ events });
  } catch (error) {
    next(error);
  }
});

app.get('/api/admin/usernames', async (req, res, next) => {
  if (!requireAdminPassword(req, res)) {
    return;
  }

  const limitRaw = Number(req.query.limit || 5000);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(10000, Math.floor(limitRaw))) : 5000;

  try {
    const result = await pool.query(
      `
        SELECT
          u.username_key,
          u.username_original,
          u.claimed_at,
          s.created_at AS session_created_at,
          (s.token IS NOT NULL) AS has_session
        FROM user_claims u
        LEFT JOIN sessions s ON s.username_key = u.username_key
        ORDER BY u.claimed_at ASC
        LIMIT $1
      `,
      [limit]
    );

    res.json({ usernames: result.rows });
  } catch (error) {
    next(error);
  }
});

app.get('/api/admin/channels', async (req, res, next) => {
  if (!requireAdminPassword(req, res)) {
    return;
  }

  const limitRaw = Number(req.query.limit || 2000);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(10000, Math.floor(limitRaw))) : 2000;

  try {
    const result = await pool.query(
      `
        SELECT
          c.slug,
          c.name,
          c.description,
          c.created_at,
          COALESCE(m.message_count, 0)::int AS message_count
        FROM channels c
        LEFT JOIN (
          SELECT channel_slug, COUNT(*)::int AS message_count
          FROM messages
          GROUP BY channel_slug
        ) m ON m.channel_slug = c.slug
        ORDER BY c.created_at ASC
        LIMIT $1
      `,
      [limit]
    );

    const slugs = result.rows.map((row) => row.slug);
    const onlineCounts = await getChannelCountsBySlug(slugs);

    const channels = result.rows.map((row) => ({
      ...row,
      online_count: onlineCounts[row.slug] || 0
    }));

    res.json({ channels });
  } catch (error) {
    next(error);
  }
});

app.post('/api/admin/usernames/release', async (req, res, next) => {
  if (!requireAdminPassword(req, res)) {
    return;
  }

  const usernameKeyInput = typeof req.body?.usernameKey === 'string' ? req.body.usernameKey.trim().toLowerCase() : '';
  if (!USERNAME_REGEX.test(usernameKeyInput)) {
    res.status(400).json({ error: 'invalid_username', message: 'Invalid username key.' });
    return;
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const claimResult = await client.query(
      `
        SELECT username_key, username_original
        FROM user_claims
        WHERE username_key = $1
        LIMIT 1
        FOR UPDATE
      `,
      [usernameKeyInput]
    );

    if (claimResult.rowCount === 0) {
      await client.query('ROLLBACK');
      res.status(404).json({ error: 'not_found', message: 'Username not found.' });
      return;
    }

    const sessionResult = await client.query('SELECT token FROM sessions WHERE username_key = $1', [usernameKeyInput]);

    await client.query('DELETE FROM sessions WHERE username_key = $1', [usernameKeyInput]);
    await client.query('DELETE FROM user_claims WHERE username_key = $1', [usernameKeyInput]);

    await client.query('COMMIT');

    const releasedUsername = claimResult.rows[0].username_original;

    queueSiteEvent(
      buildHttpEvent(req, {
        eventType: 'admin_username_released',
        meta: {
          releasedByAdmin: true,
          targetUsername: releasedUsername,
          targetUsernameKey: usernameKeyInput,
          revokedSessions: sessionResult.rowCount
        }
      })
    );

    for (const [ws, meta] of wsClients.entries()) {
      if (meta.usernameKey === usernameKeyInput) {
        sendJson(ws, { type: 'session_revoked' });
        ws.close(4401, 'session revoked');
      }
    }

    res.json({ ok: true, username: releasedUsername, usernameKey: usernameKeyInput });
  } catch (error) {
    await client.query('ROLLBACK');
    next(error);
  } finally {
    client.release();
  }
});

app.post('/api/admin/channels/delete', async (req, res, next) => {
  if (!requireAdminPassword(req, res)) {
    return;
  }

  const slugInput = String(req.body?.slug || '')
    .trim()
    .toLowerCase();
  if (!slugInput || slugifyChannelName(slugInput) !== slugInput) {
    res.status(400).json({ error: 'invalid_channel', message: 'Invalid channel slug.' });
    return;
  }

  const client = await pool.connect();
  let transactionOpen = false;

  try {
    await client.query('BEGIN');
    transactionOpen = true;

    const deleteResult = await client.query(
      `
        DELETE FROM channels
        WHERE slug = $1
        RETURNING slug, name, description, created_at
      `,
      [slugInput]
    );

    if (deleteResult.rowCount === 0) {
      await client.query('ROLLBACK');
      transactionOpen = false;
      res.status(404).json({ error: 'not_found', message: 'Channel not found.' });
      return;
    }

    await client.query('COMMIT');
    transactionOpen = false;

    await redisPublisher.del(presenceKey(slugInput));

    const affectedSockets = [];
    for (const [ws, meta] of wsClients.entries()) {
      if (meta.channel === slugInput) {
        affectedSockets.push(ws);
      }
    }

    for (const ws of affectedSockets) {
      await detachFromChannel(ws, 'channel_deleted');
      sendJson(ws, { type: 'channel_deleted', channel: { slug: slugInput } });
    }

    const deletedChannel = deleteResult.rows[0];
    const payload = {
      type: 'channel_deleted',
      channel: {
        slug: deletedChannel.slug,
        name: deletedChannel.name,
        description: deletedChannel.description,
        createdAt: new Date(deletedChannel.created_at).toISOString()
      }
    };

    broadcastAll(payload);
    await publishEvent('channel_deleted', payload);
    await broadcastCounts();

    queueSiteEvent(
      buildHttpEvent(req, {
        eventType: 'admin_channel_deleted',
        meta: {
          slug: deletedChannel.slug,
          name: deletedChannel.name,
          affectedConnections: affectedSockets.length
        }
      })
    );

    res.json({
      ok: true,
      channel: payload.channel,
      affectedConnections: affectedSockets.length
    });
  } catch (error) {
    if (transactionOpen) {
      await client.query('ROLLBACK');
    }
    next(error);
  } finally {
    client.release();
  }
});

app.post('/api/admin/unblock-me', async (req, res, next) => {
  if (!requireAdminPassword(req, res)) {
    return;
  }

  try {
    const clientIp = getClientIp(req);
    await redisPublisher.del(blockedIpKey(clientIp));
    await redisPublisher.del(botStrikeKey(clientIp));

    queueSiteEvent(
      buildHttpEvent(req, {
        eventType: 'admin_unblocked_ip',
        statusCode: 200,
        meta: {
          unblockedIp: clientIp
        }
      })
    );

    res.json({ ok: true, ip: clientIp });
  } catch (error) {
    next(error);
  }
});

app.all('/__trap__', async (req, res, next) => {
  try {
    const clientIp = getClientIp(req);
    await blockIp(clientIp, HONEYPOT_BLOCK_SECONDS, 'honeypot_triggered');

    queueSiteEvent(
      buildHttpEvent(req, {
        eventType: 'honeypot_triggered',
        statusCode: 403,
        meta: {
          blockedSeconds: HONEYPOT_BLOCK_SECONDS
        }
      })
    );

    res.status(403).send('Forbidden');
  } catch (error) {
    next(error);
  }
});

app.use((req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.use((error, req, res, next) => {
  console.error(error);
  res.status(500).json({ error: 'internal_error', message: 'Internal server error.' });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', async (ws, req) => {
  const wsIp = getClientIp(req);

  if (await isIpBlocked(wsIp)) {
    ws.close(4403, 'forbidden');
    return;
  }

  const wsConnectRate = await consumeRateLimit('ws_connect', wsIp, 60, 80).catch(() => ({ allowed: true }));
  if (!wsConnectRate.allowed) {
    ws.close(4408, 'rate limited');
    return;
  }

  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const token = url.searchParams.get('token');

  if (!token) {
    sendJson(ws, { type: 'auth_error', message: 'Invalid session token.' });
    ws.close(4401, 'unauthorized');
    return;
  }

  const session = await getSessionByToken(token).catch(() => null);
  if (!session) {
    sendJson(ws, { type: 'auth_error', message: 'Invalid session token.' });
    ws.close(4401, 'unauthorized');
    return;
  }

  const meta = {
    token,
    username: session.username,
    usernameKey: session.username_key,
    ipAddress: wsIp,
    channel: null,
    connectionId: crypto.randomUUID(),
    presenceMember: null,
    heartbeatTimer: null
  };

  wsClients.set(ws, meta);

  meta.heartbeatTimer = setInterval(() => {
    const current = wsClients.get(ws);
    if (!current || !current.channel || !current.presenceMember) {
      return;
    }

    refreshPresenceHeartbeat(current.channel, current.presenceMember).catch((error) => {
      console.error('Presence heartbeat failed:', error);
    });
  }, Math.max(PRESENCE_HEARTBEAT_SECONDS, 5) * 1000);

  queueSiteEvent(
    buildWsEvent(req, {
      eventType: 'ws_connected',
      token,
      username: session.username,
      usernameKey: session.username_key,
      meta: {
        connectionId: meta.connectionId
      }
    })
  );

  let counts = {};
  try {
    const channels = await getChannelRows();
    counts = await getChannelCountsBySlug(channels.map((row) => row.slug));
  } catch (error) {
    counts = {};
  }

  sendJson(ws, {
    type: 'connected',
    username: session.username,
    channelCounts: counts
  });

  ws.on('message', async (raw) => {
    try {
      const payload = parseWsPayload(raw);
      if (!payload || typeof payload.type !== 'string') {
        sendJson(ws, { type: 'error', message: 'Invalid payload.' });
        return;
      }

      const localMeta = wsClients.get(ws);
      if (!localMeta) {
        return;
      }

      if (payload.type === 'subscribe') {
        const slug = String(payload.channel || '').toLowerCase();
        const channelResult = await pool.query('SELECT slug FROM channels WHERE slug = $1 LIMIT 1', [slug]);

        if (channelResult.rowCount === 0) {
          sendJson(ws, { type: 'error', message: 'Channel not found.' });
          return;
        }

        await attachToChannel(ws, slug);
        const onlineCount = await getPresenceCount(slug);

        sendJson(ws, {
          type: 'subscribed',
          channel: slug,
          onlineCount
        });
        return;
      }

      if (payload.type === 'typing') {
        const channel = String(payload.channel || '').toLowerCase();
        if (!localMeta.channel || localMeta.channel !== channel) {
          return;
        }

        const typingPayload = {
          type: 'typing',
          channel,
          username: localMeta.username,
          timestamp: nowIso()
        };

        broadcastChannel(channel, typingPayload, ws);
        await publishEvent('typing', typingPayload);
        return;
      }

      if (payload.type === 'message') {
        const channel = String(payload.channel || '').toLowerCase();
        const text = typeof payload.text === 'string' ? payload.text.trim() : '';

        if (!localMeta.channel || localMeta.channel !== channel) {
          sendJson(ws, { type: 'error', message: 'Join the channel before sending messages.' });
          return;
        }

        if (!text) {
          sendJson(ws, { type: 'error', message: 'Message cannot be empty.' });
          return;
        }

        if (text.length > MAX_MESSAGE_LENGTH) {
          sendJson(ws, { type: 'error', message: `Message exceeds ${MAX_MESSAGE_LENGTH} characters.` });
          return;
        }

        const wsRateIdentifier = `${localMeta.usernameKey || localMeta.ipAddress || 'unknown'}:${channel}`;
        const wsMessageRate = await consumeRateLimit(
          'ws_message',
          wsRateIdentifier,
          WS_MESSAGE_RATE_LIMIT_WINDOW_SEC,
          WS_MESSAGE_RATE_LIMIT_MAX
        );
        if (!wsMessageRate.allowed) {
          sendJson(ws, { type: 'error', message: 'You are sending too fast. Slow down.' });
          return;
        }

        const message = {
          id: crypto.randomUUID(),
          channel,
          username: localMeta.username,
          text,
          timestamp: nowIso()
        };

        await pool.query(
          `
            INSERT INTO messages (id, channel_slug, username, text, created_at)
            VALUES ($1, $2, $3, $4, $5)
          `,
          [message.id, message.channel, message.username, message.text, message.timestamp]
        );

        const messagePayload = { type: 'message', ...message };

        queueSiteEvent(
          buildWsEvent(req, {
            eventType: 'ws_message_sent',
            token,
            username: localMeta.username,
            usernameKey: localMeta.usernameKey,
            path: `/ws/channels/${channel}`,
            body: {
              text
            },
            meta: {
              messageId: message.id,
              textLength: text.length
            }
          })
        );

        broadcastChannel(channel, messagePayload);
        await publishEvent('message', messagePayload);
        return;
      }

      sendJson(ws, { type: 'error', message: `Unsupported type: ${payload.type}` });
    } catch (error) {
      console.error('WebSocket message handler error:', error);
      sendJson(ws, { type: 'error', message: 'Message handling failed.' });
    }
  });

  ws.on('close', () => {
    if (meta.heartbeatTimer) {
      clearInterval(meta.heartbeatTimer);
      meta.heartbeatTimer = null;
    }

    queueSiteEvent(
      buildWsEvent(req, {
        eventType: 'ws_disconnected',
        token,
        username: meta.username,
        usernameKey: meta.usernameKey,
        meta: {
          connectionId: meta.connectionId
        }
      })
    );

    detachFromChannel(ws, 'disconnect')
      .catch((error) => console.error('WebSocket close handler error:', error))
      .finally(() => {
        wsClients.delete(ws);
      });
  });

  ws.on('error', () => {
    if (meta.heartbeatTimer) {
      clearInterval(meta.heartbeatTimer);
      meta.heartbeatTimer = null;
    }

    queueSiteEvent(
      buildWsEvent(req, {
        eventType: 'ws_error',
        token,
        username: meta.username,
        usernameKey: meta.usernameKey,
        meta: {
          connectionId: meta.connectionId
        }
      })
    );

    detachFromChannel(ws, 'disconnect')
      .catch((error) => console.error('WebSocket error handler error:', error))
      .finally(() => {
        wsClients.delete(ws);
      });
  });
});

async function initDatabase() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS user_claims (
      username_key TEXT PRIMARY KEY,
      username_original TEXT NOT NULL,
      claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sessions (
      token TEXT PRIMARY KEY,
      username_key TEXT NOT NULL UNIQUE REFERENCES user_claims(username_key) ON DELETE CASCADE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS channels (
      slug TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT NOT NULL DEFAULT '',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id UUID PRIMARY KEY,
      channel_slug TEXT NOT NULL REFERENCES channels(slug) ON DELETE CASCADE,
      username TEXT NOT NULL,
      text TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS site_events (
      id UUID PRIMARY KEY,
      event_type TEXT NOT NULL,
      event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      instance_id TEXT NOT NULL,
      ip_address TEXT,
      forwarded_for TEXT,
      remote_address TEXT,
      method TEXT,
      path TEXT,
      status_code INTEGER,
      duration_ms INTEGER,
      username TEXT,
      username_key TEXT,
      user_agent TEXT,
      referer TEXT,
      origin TEXT,
      accept_language TEXT,
      sec_ch_ua TEXT,
      sec_ch_ua_mobile TEXT,
      sec_ch_ua_platform TEXT,
      token_fingerprint TEXT,
      headers_json JSONB,
      body_json JSONB,
      meta_json JSONB
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_messages_channel_created_at
    ON messages(channel_slug, created_at DESC);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_site_events_time
    ON site_events(event_time DESC);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_site_events_event_type
    ON site_events(event_type);
  `);

  await pool.query(
    `
      INSERT INTO channels (slug, name, description)
      VALUES ('general', '#general', 'Default public channel')
      ON CONFLICT (slug) DO NOTHING
    `
  );
}

async function start() {
  await redisPublisher.connect();
  await redisSubscriber.connect();
  await redisSubscriber.subscribe(EVENTS_CHANNEL, handleRedisEvent);

  await initDatabase();

  server.listen(PORT, () => {
    console.log(`Realtime chat server running on http://localhost:${PORT}`);
  });
}

start().catch((error) => {
  console.error('Failed to start server:', error);
  process.exit(1);
});
