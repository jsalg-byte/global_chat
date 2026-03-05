require('dotenv').config();

const crypto = require('crypto');
const fs = require('fs');
const http = require('http');
const path = require('path');
const { URL } = require('url');

const express = require('express');
const { Reader: MaxMindReader } = require('@maxmind/geoip2-node');
const geoip = require('geoip-lite');
const { IP2Proxy } = require('ip2proxy-nodejs');
const ipaddr = require('ipaddr.js');
const { Pool } = require('pg');
const { createClient } = require('redis');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL;
const REDIS_URL = process.env.REDIS_URL;
const REDIS_PREFIX = process.env.REDIS_PREFIX || 'yungjewboii_global_chat';
const DATABASE_SSL = process.env.DATABASE_SSL || 'false';
const TRUST_PROXY = process.env.TRUST_PROXY;
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
const MAXMIND_CITY_DB_PATH = process.env.MAXMIND_CITY_DB_PATH || path.join(__dirname, 'data', 'GeoLite2-City.mmdb');
const MAXMIND_ASN_DB_PATH = process.env.MAXMIND_ASN_DB_PATH || path.join(__dirname, 'data', 'GeoLite2-ASN.mmdb');
const IP2PROXY_DB_PATH = process.env.IP2PROXY_DB_PATH || path.join(__dirname, 'data', 'IP2PROXY.BIN');
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
const USER_COLOR_PALETTE = [
  '#ffd1dc', '#ffe4b5', '#fff3b0', '#d9f2c2', '#c7f0db', '#bfe8ff', '#cddafd', '#e2d5ff', '#f8d9ff', '#f6c1d0',
  '#f9e2d2', '#fdecc8', '#fef7d7', '#e7f6d5', '#dff7ea', '#d9f1ff', '#dfe7ff', '#ece2ff', '#f4e5ff', '#fbe4ef',
  '#f4f4f4', '#e3e3e3', '#d7d7d7', '#dfe8d9', '#e9f1dc', '#e6f0f7', '#e5e8f5', '#eee7f6'
];

const INSTANCE_ID = crypto.randomUUID();
const EVENTS_CHANNEL = `${REDIS_PREFIX}:events`;

const app = express();
app.use(express.json({ limit: '64kb' }));

function parseBoolean(value, defaultValue = false) {
  if (value === undefined || value === null || value === '') {
    return defaultValue;
  }
  return ['1', 'true', 'yes', 'on'].includes(String(value).toLowerCase());
}

function resolveTrustProxySetting(value) {
  if (value === undefined || value === null || value === '') {
    return false;
  }

  const raw = String(value).trim();
  const lower = raw.toLowerCase();

  if (['false', '0', 'off', 'no'].includes(lower)) {
    return false;
  }

  if (['true', '1', 'on', 'yes'].includes(lower)) {
    return true;
  }

  if (/^\d+$/.test(raw)) {
    return Number(raw);
  }

  const entries = raw
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean);
  if (!entries.length) {
    return false;
  }
  return entries;
}

app.set('trust proxy', resolveTrustProxySetting(TRUST_PROXY));

const useDatabaseSsl = parseBoolean(DATABASE_SSL, false);

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: useDatabaseSsl ? { rejectUnauthorized: false } : false
});

const redisPublisher = createClient({ url: REDIS_URL });
const redisSubscriber = createClient({ url: REDIS_URL });

const wsClients = new Map();
const subscribers = new Map();
const maxMindReaders = {
  city: null,
  asn: null
};
const ip2proxyClient = new IP2Proxy();
let ip2proxyReady = false;

process.on('exit', () => {
  if (ip2proxyReady) {
    try {
      ip2proxyClient.close();
    } catch (error) {
      // Ignore close errors during shutdown.
    }
  }
});

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

function normalizeHexColor(value) {
  if (typeof value !== 'string') {
    return null;
  }
  const text = value.trim();
  const match = text.match(/^#([0-9a-fA-F]{6})$/);
  if (!match) {
    return null;
  }
  return `#${match[1].toLowerCase()}`;
}

function isPaletteColor(color) {
  const normalized = normalizeHexColor(color);
  return Boolean(normalized && USER_COLOR_PALETTE.includes(normalized));
}

function getRandomPaletteColor() {
  return USER_COLOR_PALETTE[crypto.randomInt(0, USER_COLOR_PALETTE.length)];
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
  const fromReqIp = normalizeIp(req.ip);
  const fromSocket = normalizeIp(req.socket?.remoteAddress);
  return fromReqIp || fromSocket || 'unknown';
}

function normalizeIp(ipAddress) {
  if (!ipAddress) {
    return null;
  }

  const text = String(ipAddress).trim().replace(/^\[|\]$/g, '');
  if (!text || text.toLowerCase() === 'unknown') {
    return null;
  }

  const zoneIndex = text.indexOf('%');
  const withoutZone = zoneIndex >= 0 ? text.slice(0, zoneIndex) : text;

  if (!ipaddr.isValid(withoutZone)) {
    return null;
  }

  try {
    return ipaddr.process(withoutZone).toString();
  } catch (error) {
    try {
      return ipaddr.parse(withoutZone).toString();
    } catch (innerError) {
      return null;
    }
  }
}

function classifyIpAddress(ipAddress) {
  const normalized = normalizeIp(ipAddress);
  if (!normalized) {
    return null;
  }

  try {
    const parsed = ipaddr.parse(normalized);
    const range = parsed.range();
    return {
      ip: normalized,
      version: parsed.kind(),
      range,
      isPublic: range === 'unicast'
    };
  } catch (error) {
    return null;
  }
}

function isIpLocalOrLoopback(ipAddress) {
  const profile = classifyIpAddress(ipAddress);
  if (!profile) {
    return true;
  }
  return ['loopback', 'private', 'linkLocal', 'uniqueLocal', 'unspecified'].includes(profile.range);
}

function toCleanExternalValue(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const text = String(value).trim();
  if (!text || text === '-' || text === '?' || text.toUpperCase() === 'N/A') {
    return null;
  }
  return text;
}

function lookupMaxMindCity(ipAddress) {
  if (!maxMindReaders.city) {
    return null;
  }
  try {
    return maxMindReaders.city.city(ipAddress);
  } catch (error) {
    return null;
  }
}

function lookupMaxMindAsn(ipAddress) {
  if (!maxMindReaders.asn) {
    return null;
  }
  try {
    return maxMindReaders.asn.asn(ipAddress);
  } catch (error) {
    return null;
  }
}

function getGeoDetailsForIp(ipAddress) {
  const normalized = normalizeIp(ipAddress);
  if (!normalized || isIpLocalOrLoopback(normalized)) {
    return null;
  }

  const city = lookupMaxMindCity(normalized);
  const asn = lookupMaxMindAsn(normalized);
  const fallback = geoip.lookup(normalized);
  const fallbackLat = Array.isArray(fallback?.ll) ? fallback.ll[0] : null;
  const fallbackLon = Array.isArray(fallback?.ll) ? fallback.ll[1] : null;

  const countryCode = city?.country?.isoCode || fallback?.country || null;
  const countryName = city?.country?.names?.en || null;
  const cityName = city?.city?.names?.en || fallback?.city || null;
  const regionCode = city?.subdivisions?.[0]?.isoCode || fallback?.region || null;
  const regionName = city?.subdivisions?.[0]?.names?.en || null;
  const postalCode = city?.postal?.code || null;
  const timezone = city?.location?.timeZone || fallback?.timezone || null;
  const latitude = city?.location?.latitude ?? fallbackLat;
  const longitude = city?.location?.longitude ?? fallbackLon;
  const accuracyRadiusKm = city?.location?.accuracyRadius ?? null;
  const asnNumber = asn?.autonomousSystemNumber ?? city?.traits?.autonomousSystemNumber ?? null;
  const asnOrg = asn?.autonomousSystemOrganization || city?.traits?.autonomousSystemOrganization || null;
  const network = toCleanExternalValue(asn?.network || city?.traits?.network);

  return {
    ip: normalized,
    country_code: countryCode,
    country_name: countryName,
    region_code: regionCode,
    region_name: regionName,
    city: cityName,
    postal_code: postalCode,
    timezone,
    latitude,
    longitude,
    accuracy_radius_km: accuracyRadiusKm,
    autonomous_system_number: asnNumber,
    autonomous_system_organization: asnOrg,
    network
  };
}

function getProxyDetailsForIp(ipAddress) {
  const normalized = normalizeIp(ipAddress);
  if (!normalized || !ip2proxyReady) {
    return null;
  }

  try {
    const data = ip2proxyClient.getAll(normalized);
    const isProxyRaw = Number(data?.isProxy ?? -1);

    return {
      ip: normalized,
      database_available: true,
      is_proxy: isProxyRaw === 1 || isProxyRaw === 2,
      is_datacenter: isProxyRaw === 2,
      proxy_type: toCleanExternalValue(data?.proxyType),
      provider: toCleanExternalValue(data?.provider),
      usage_type: toCleanExternalValue(data?.usageType),
      isp: toCleanExternalValue(data?.isp),
      domain: toCleanExternalValue(data?.domain),
      threat: toCleanExternalValue(data?.threat),
      fraud_score: toCleanExternalValue(data?.fraudScore),
      last_seen_days: toCleanExternalValue(data?.lastSeen),
      country_code: toCleanExternalValue(data?.countryShort),
      country_name: toCleanExternalValue(data?.countryLong),
      region: toCleanExternalValue(data?.region),
      city: toCleanExternalValue(data?.city),
      asn: toCleanExternalValue(data?.asn),
      as_name: toCleanExternalValue(data?.as)
    };
  } catch (error) {
    return {
      ip: normalized,
      database_available: true,
      error: 'lookup_failed'
    };
  }
}

function getIpIntelligenceForIp(ipAddress) {
  const normalized = normalizeIp(ipAddress);
  if (!normalized) {
    return null;
  }

  return {
    ip: normalized,
    network: classifyIpAddress(normalized),
    geo: getGeoDetailsForIp(normalized),
    proxy: getProxyDetailsForIp(normalized)
  };
}

function getCountryCodeForIp(ipAddress) {
  const details = getGeoDetailsForIp(ipAddress);
  return details?.country_code || null;
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

function getRequestPathname(pathValue) {
  const rawPath = String(pathValue || '/');
  try {
    return new URL(rawPath, 'http://localhost').pathname.toLowerCase();
  } catch (error) {
    return rawPath.split('?')[0].toLowerCase();
  }
}

function getImmediatePathBanReason(req) {
  const pathname = getRequestPathname(req.originalUrl || req.url || req.path);
  if (pathname.includes('/.env')) {
    return 'env_probe';
  }
  if (pathname === '/wp' || pathname.startsWith('/wp/') || pathname === '/wordpress' || pathname.startsWith('/wordpress/')) {
    return 'wp_probe';
  }
  if (pathname.includes('.sh') || pathname.includes('.sql') || pathname.includes('.bak')) {
    return 'sensitive_file_probe';
  }
  return null;
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

async function loadMaxMindReader(dbPath, label) {
  if (!dbPath || !fs.existsSync(dbPath)) {
    return null;
  }

  try {
    const reader = await MaxMindReader.open(dbPath);
    console.log(`Loaded ${label} database from ${dbPath}`);
    return reader;
  } catch (error) {
    console.warn(`Failed to load ${label} database from ${dbPath}:`, error.message);
    return null;
  }
}

async function initIpIntelligenceProviders() {
  maxMindReaders.city = await loadMaxMindReader(MAXMIND_CITY_DB_PATH, 'MaxMind City');
  maxMindReaders.asn = await loadMaxMindReader(MAXMIND_ASN_DB_PATH, 'MaxMind ASN');

  if (!fs.existsSync(IP2PROXY_DB_PATH)) {
    console.warn(`IP2Proxy BIN not found at ${IP2PROXY_DB_PATH}; VPN/proxy detection is disabled.`);
    return;
  }

  try {
    const status = ip2proxyClient.open(IP2PROXY_DB_PATH);
    if (status === 0) {
      ip2proxyReady = true;
      console.log(`Loaded IP2Proxy BIN from ${IP2PROXY_DB_PATH}`);
      return;
    }
    console.warn(`Failed to open IP2Proxy BIN from ${IP2PROXY_DB_PATH}; status=${status}`);
  } catch (error) {
    console.warn(`Failed to load IP2Proxy BIN from ${IP2PROXY_DB_PATH}:`, error.message);
  }
}

async function ensureUserColors() {
  const allowedPalette = USER_COLOR_PALETTE.map((entry) => entry.toLowerCase());
  const result = await pool.query(
    `
      SELECT username_key
      FROM user_claims
      WHERE color_hex IS NULL
         OR color_hex !~ '^#[0-9a-fA-F]{6}$'
         OR lower(color_hex) <> ALL($1::text[])
    `,
    [allowedPalette]
  );

  for (const row of result.rows) {
    const color = getRandomPaletteColor();
    await pool.query('UPDATE user_claims SET color_hex = $2 WHERE username_key = $1', [row.username_key, color]);
  }
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

async function listBlockedIps(limit = 500) {
  const max = Number.isFinite(limit) ? Math.max(1, Math.min(5000, Math.floor(limit))) : 500;
  const prefix = `${REDIS_PREFIX}:blocked_ip:`;
  const keys = [];

  for await (const key of redisPublisher.scanIterator({ MATCH: `${prefix}*`, COUNT: 200 })) {
    keys.push(String(key));
    if (keys.length >= max) {
      break;
    }
  }

  if (!keys.length) {
    return [];
  }

  const pipeline = redisPublisher.multi();
  for (const key of keys) {
    pipeline.get(key);
    pipeline.ttl(key);
  }
  const results = await pipeline.exec();

  const entries = [];
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    const reason = results?.[index * 2] ?? null;
    const ttlRaw = Number(results?.[index * 2 + 1]);

    if (!key.startsWith(prefix) || ttlRaw === -2) {
      continue;
    }

    entries.push({
      ip_address: key.slice(prefix.length),
      reason: reason ? String(reason) : 'blocked',
      ttl_seconds: Number.isFinite(ttlRaw) && ttlRaw >= 0 ? ttlRaw : null
    });
  }

  entries.sort((a, b) => {
    const aTtl = a.ttl_seconds === null ? Number.MAX_SAFE_INTEGER : a.ttl_seconds;
    const bTtl = b.ttl_seconds === null ? Number.MAX_SAFE_INTEGER : b.ttl_seconds;
    return aTtl - bTtl;
  });

  return entries;
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
  const clientIp = getClientIp(req);

  return {
    eventType: extras.eventType || 'http_request',
    eventTime: extras.eventTime || nowIso(),
    ipAddress: clientIp === 'unknown' ? null : clientIp,
    forwardedFor: trimValue(forwardedFor, 512),
    remoteAddress: normalizeIp(req.socket?.remoteAddress) || null,
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
  const clientIp = getClientIp(req);

  return {
    eventType: extras.eventType,
    eventTime: nowIso(),
    ipAddress: clientIp === 'unknown' ? null : clientIp,
    forwardedFor: trimValue(forwardedFor, 512),
    remoteAddress: normalizeIp(req.socket?.remoteAddress) || null,
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

    const immediateBanReason = getImmediatePathBanReason(req);
    if (immediateBanReason) {
      await blockIp(clientIp, HONEYPOT_BLOCK_SECONDS, immediateBanReason);

      queueSiteEvent(
        buildHttpEvent(req, {
          eventType: 'path_probe_blocked',
          statusCode: 403,
          meta: {
            reason: immediateBanReason,
            path: getRequestPathname(req.originalUrl || req.url || req.path)
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
      SELECT s.username_key, u.username_original AS username, u.color_hex
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
      usernameKey: session.username_key,
      userColor: normalizeHexColor(session.color_hex) || '#f8f8f8'
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
  const requestedColor = normalizeHexColor(req.body?.color);
  const selectedColor = isPaletteColor(requestedColor) ? requestedColor : getRandomPaletteColor();
  const token = crypto.randomBytes(32).toString('hex');
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const claimResult = await client.query(
      `
        INSERT INTO user_claims (username_key, username_original, color_hex)
        VALUES ($1, $2, $3)
        ON CONFLICT (username_key) DO NOTHING
        RETURNING username_key
      `,
      [usernameKey, requested, selectedColor]
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

    res.status(201).json({ token, username: requested, color: selectedColor });
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

  res.json({ username: req.auth.username, color: req.auth.userColor, palette: USER_COLOR_PALETTE });
});

app.post('/api/profile/color', requireAuth, async (req, res, next) => {
  const color = normalizeHexColor(req.body?.color);
  if (!isPaletteColor(color)) {
    res.status(400).json({ error: 'invalid_color', message: 'Color must be selected from the palette.' });
    return;
  }

  try {
    await pool.query(
      `
        UPDATE user_claims
        SET color_hex = $2
        WHERE username_key = $1
      `,
      [req.auth.usernameKey, color]
    );

    for (const meta of wsClients.values()) {
      if (meta.usernameKey === req.auth.usernameKey) {
        meta.userColor = color;
      }
    }

    queueSiteEvent(
      buildHttpEvent(req, {
        eventType: 'profile_color_updated',
        username: req.auth.username,
        usernameKey: req.auth.usernameKey,
        meta: {
          color
        }
      })
    );

    res.json({ ok: true, color, palette: USER_COLOR_PALETTE });
  } catch (error) {
    next(error);
  }
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
        SELECT id, channel_slug, username, text, color_hex, created_at
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
        color: normalizeHexColor(row.color_hex) || null,
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
  const offsetRaw = Number(req.query.offset || 0);
  const offset = Number.isFinite(offsetRaw) ? Math.max(0, Math.floor(offsetRaw)) : 0;

  try {
    const [result, countResult] = await Promise.all([
      pool.query(
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
          OFFSET $2
        `,
        [limit, offset]
      ),
      pool.query('SELECT COUNT(*)::bigint AS total FROM site_events')
    ]);

    const total = Number(countResult.rows[0]?.total || 0);
    const nextOffset = offset + result.rows.length;
    const hasMore = nextOffset < total;

    res.json({
      events: result.rows,
      pagination: {
        total,
        limit,
        offset,
        has_more: hasMore,
        next_offset: hasMore ? nextOffset : null,
        prev_offset: offset > 0 ? Math.max(0, offset - limit) : null
      }
    });
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
    const onlineUsernameKeys = new Set();
    for (const meta of wsClients.values()) {
      if (meta?.usernameKey) {
        onlineUsernameKeys.add(meta.usernameKey);
      }
    }

    const result = await pool.query(
      `
        SELECT
          u.username_key,
          u.username_original,
          u.claimed_at,
          s.created_at AS session_created_at,
          (s.token IS NOT NULL) AS has_session,
          latest_event.candidate_ip AS last_ip,
          latest_event.event_time AS last_seen_at
        FROM user_claims u
        LEFT JOIN sessions s ON s.username_key = u.username_key
        LEFT JOIN LATERAL (
          SELECT
            COALESCE(
              NULLIF(BTRIM(e.ip_address), ''),
              NULLIF(BTRIM(SPLIT_PART(e.forwarded_for, ',', 1)), ''),
              NULLIF(BTRIM(e.remote_address), '')
            ) AS candidate_ip,
            e.event_time
          FROM site_events e
          WHERE e.username_key = u.username_key
          ORDER BY e.event_time DESC
          LIMIT 1
        ) latest_event ON TRUE
        ORDER BY u.claimed_at ASC
        LIMIT $1
      `,
      [limit]
    );

    const usernames = result.rows.map((row) => {
      const lastIp = normalizeIp(row.last_ip);
      return {
        ...row,
        last_ip: lastIp,
        last_country_code: getCountryCodeForIp(lastIp),
        is_online: onlineUsernameKeys.has(row.username_key)
      };
    });

    res.json({ usernames });
  } catch (error) {
    next(error);
  }
});

app.get('/api/admin/usernames/:usernameKey/ip-details', async (req, res, next) => {
  if (!requireAdminPassword(req, res)) {
    return;
  }

  const usernameKeyInput = typeof req.params.usernameKey === 'string' ? req.params.usernameKey.trim().toLowerCase() : '';
  if (!USERNAME_REGEX.test(usernameKeyInput)) {
    res.status(400).json({ error: 'invalid_username', message: 'Invalid username key.' });
    return;
  }

  const recentRaw = Number(req.query.recent || 30);
  const historyRaw = Number(req.query.history || 30);
  const recentLimit = Number.isFinite(recentRaw) ? Math.max(1, Math.min(100, Math.floor(recentRaw))) : 30;
  const historyLimit = Number.isFinite(historyRaw) ? Math.max(1, Math.min(200, Math.floor(historyRaw))) : 30;

  try {
    const userResult = await pool.query(
      `
        SELECT username_key, username_original, claimed_at
        FROM user_claims
        WHERE username_key = $1
        LIMIT 1
      `,
      [usernameKeyInput]
    );

    if (userResult.rowCount === 0) {
      res.status(404).json({ error: 'not_found', message: 'User not found.' });
      return;
    }

    const recentVisitsResult = await pool.query(
      `
        SELECT
          event_time,
          event_type,
          method,
          path,
          status_code,
          duration_ms,
          user_agent,
          COALESCE(
            NULLIF(BTRIM(ip_address), ''),
            NULLIF(BTRIM(SPLIT_PART(forwarded_for, ',', 1)), ''),
            NULLIF(BTRIM(remote_address), '')
          ) AS candidate_ip
        FROM site_events
        WHERE username_key = $1
        ORDER BY event_time DESC
        LIMIT $2
      `,
      [usernameKeyInput, recentLimit]
    );

    const ipHistoryResult = await pool.query(
      `
        SELECT
          candidate_ip,
          COUNT(*)::int AS hit_count,
          MIN(event_time) AS first_seen_at,
          MAX(event_time) AS last_seen_at
        FROM (
          SELECT
            event_time,
            COALESCE(
              NULLIF(BTRIM(ip_address), ''),
              NULLIF(BTRIM(SPLIT_PART(forwarded_for, ',', 1)), ''),
              NULLIF(BTRIM(remote_address), '')
            ) AS candidate_ip
          FROM site_events
          WHERE username_key = $1
        ) ip_events
        WHERE candidate_ip IS NOT NULL
        GROUP BY candidate_ip
        ORDER BY last_seen_at DESC
        LIMIT $2
      `,
      [usernameKeyInput, historyLimit]
    );

    const ipIntelCache = new Map();
    const getCachedIntel = (ipAddress) => {
      const normalized = normalizeIp(ipAddress);
      if (!normalized) {
        return null;
      }
      if (!ipIntelCache.has(normalized)) {
        ipIntelCache.set(normalized, getIpIntelligenceForIp(normalized));
      }
      return ipIntelCache.get(normalized);
    };

    const recentVisits = recentVisitsResult.rows.map((row) => {
      const ipAddress = normalizeIp(row.candidate_ip);
      const intelligence = getCachedIntel(ipAddress);
      return {
        event_time: row.event_time,
        event_type: row.event_type,
        method: row.method,
        path: row.path,
        status_code: row.status_code,
        duration_ms: row.duration_ms,
        user_agent: row.user_agent,
        ip_address: ipAddress,
        country_code: intelligence?.geo?.country_code || null,
        ip_range: intelligence?.network?.range || null,
        is_proxy: intelligence?.proxy?.is_proxy ?? null,
        proxy_type: intelligence?.proxy?.proxy_type || null
      };
    });

    const ipHistory = ipHistoryResult.rows.map((row) => {
      const ipAddress = normalizeIp(row.candidate_ip);
      const intelligence = getCachedIntel(ipAddress);
      return {
        ip_address: ipAddress,
        country_code: intelligence?.geo?.country_code || null,
        ip_range: intelligence?.network?.range || null,
        is_proxy: intelligence?.proxy?.is_proxy ?? null,
        proxy_type: intelligence?.proxy?.proxy_type || null,
        hit_count: Number(row.hit_count || 0),
        first_seen_at: row.first_seen_at,
        last_seen_at: row.last_seen_at
      };
    });

    const latestVisitWithIp = recentVisits.find((visit) => visit.ip_address);
    const lastIp = latestVisitWithIp?.ip_address || ipHistory[0]?.ip_address || null;
    const lastSeenAt = latestVisitWithIp?.event_time || ipHistory[0]?.last_seen_at || null;

    const lastIpIntel = getCachedIntel(lastIp);
    const geo = lastIpIntel?.geo || null;
    const location = geo
      ? {
          country_code: geo.country_code || null,
          country_name: geo.country_name || null,
          region: geo.region_code || geo.region_name || null,
          region_name: geo.region_name || null,
          city: geo.city || null,
          postal_code: geo.postal_code || null,
          timezone: geo.timezone || null,
          latitude: geo.latitude ?? null,
          longitude: geo.longitude ?? null,
          accuracy_radius_km: geo.accuracy_radius_km ?? null,
          autonomous_system_number: geo.autonomous_system_number ?? null,
          autonomous_system_organization: geo.autonomous_system_organization || null,
          network: geo.network || null
        }
      : null;

    const networkProfile = lastIpIntel?.network || null;
    const proxyProfile = lastIpIntel?.proxy || {
      ip: lastIp || null,
      database_available: ip2proxyReady,
      is_proxy: null
    };

    res.json({
      user: userResult.rows[0],
      last_ip: lastIp,
      last_seen_at: lastSeenAt,
      location,
      network_profile: networkProfile,
      proxy_profile: proxyProfile,
      ip_intelligence: lastIpIntel,
      intelligence_sources: {
        request_ip: false,
        ipaddr: true,
        maxmind_city: Boolean(maxMindReaders.city),
        maxmind_asn: Boolean(maxMindReaders.asn),
        ip2proxy: Boolean(ip2proxyReady)
      },
      recent_visits: recentVisits,
      ip_history: ipHistory
    });
  } catch (error) {
    next(error);
  }
});

app.get('/api/admin/ip-intel', async (req, res, next) => {
  if (!requireAdminPassword(req, res)) {
    return;
  }

  const ipInput = normalizeIp(req.query.ip);
  if (!ipInput) {
    res.status(400).json({ error: 'invalid_ip', message: 'Invalid IP address.' });
    return;
  }

  try {
    const intelligence = getIpIntelligenceForIp(ipInput);
    const geo = intelligence?.geo || null;
    const proxy = intelligence?.proxy || {
      ip: ipInput,
      database_available: ip2proxyReady,
      is_proxy: null
    };

    const location = geo
      ? {
          country_code: geo.country_code || null,
          country_name: geo.country_name || null,
          region: geo.region_code || geo.region_name || null,
          region_name: geo.region_name || null,
          city: geo.city || null,
          postal_code: geo.postal_code || null,
          timezone: geo.timezone || null,
          latitude: geo.latitude ?? null,
          longitude: geo.longitude ?? null,
          accuracy_radius_km: geo.accuracy_radius_km ?? null,
          autonomous_system_number: geo.autonomous_system_number ?? null,
          autonomous_system_organization: geo.autonomous_system_organization || null,
          network: geo.network || null
        }
      : null;

    res.json({
      ip: ipInput,
      location,
      network_profile: intelligence?.network || null,
      proxy_profile: proxy,
      ip_intelligence: intelligence,
      intelligence_sources: {
        request_ip: false,
        ipaddr: true,
        maxmind_city: Boolean(maxMindReaders.city),
        maxmind_asn: Boolean(maxMindReaders.asn),
        ip2proxy: Boolean(ip2proxyReady)
      }
    });
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

app.get('/api/admin/banned-ips', async (req, res, next) => {
  if (!requireAdminPassword(req, res)) {
    return;
  }

  const limitRaw = Number(req.query.limit || 1000);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(5000, Math.floor(limitRaw))) : 1000;

  try {
    const bannedIps = await listBlockedIps(limit);
    res.json({ banned_ips: bannedIps });
  } catch (error) {
    next(error);
  }
});

app.post('/api/admin/banned-ips/unban', async (req, res, next) => {
  if (!requireAdminPassword(req, res)) {
    return;
  }

  const ipAddress = normalizeIp(req.body?.ipAddress);
  if (!ipAddress) {
    res.status(400).json({ error: 'invalid_ip', message: 'Invalid IP address.' });
    return;
  }

  try {
    await redisPublisher.del(blockedIpKey(ipAddress));
    await redisPublisher.del(botStrikeKey(ipAddress));

    queueSiteEvent(
      buildHttpEvent(req, {
        eventType: 'admin_unblocked_ip',
        statusCode: 200,
        meta: {
          unblockedIp: ipAddress,
          source: 'banned_ips_tab'
        }
      })
    );

    res.json({ ok: true, ip: ipAddress });
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
    userColor: normalizeHexColor(session.color_hex) || '#f8f8f8',
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
          usernameKey: localMeta.usernameKey,
          color: normalizeHexColor(localMeta.userColor) || '#f8f8f8',
          text,
          timestamp: nowIso()
        };

        await pool.query(
          `
            INSERT INTO messages (id, channel_slug, username_key, username, color_hex, text, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
          `,
          [message.id, message.channel, message.usernameKey, message.username, message.color, message.text, message.timestamp]
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
      color_hex TEXT NOT NULL DEFAULT '#f8f8f8',
      claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    ALTER TABLE user_claims
    ADD COLUMN IF NOT EXISTS color_hex TEXT;
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
      username_key TEXT REFERENCES user_claims(username_key) ON DELETE SET NULL,
      username TEXT NOT NULL,
      color_hex TEXT,
      text TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS username_key TEXT;
  `);

  await pool.query(`
    ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS color_hex TEXT;
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

  await ensureUserColors();

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

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_site_events_username_key_time
    ON site_events(username_key, event_time DESC);
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

  await initIpIntelligenceProviders();
  await initDatabase();

  server.listen(PORT, () => {
    console.log(`Realtime chat server running on http://localhost:${PORT}`);
  });
}

start().catch((error) => {
  console.error('Failed to start server:', error);
  process.exit(1);
});
