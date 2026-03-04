require('dotenv').config();

const crypto = require('crypto');
const http = require('http');
const path = require('path');
const { URL } = require('url');

const express = require('express');
const { Pool } = require('pg');
const { createClient } = require('redis');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL;
const REDIS_URL = process.env.REDIS_URL;
const REDIS_PREFIX = process.env.REDIS_PREFIX || 'yungjewboii_global_chat';

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
app.use(express.json({ limit: '16kb' }));
app.use(express.static(path.join(__dirname, 'public')));

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
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

  const pipeline = redisPublisher.multi();
  for (const slug of slugs) {
    pipeline.sCard(presenceKey(slug));
  }

  const rawCounts = await pipeline.exec();
  const counts = {};

  for (let i = 0; i < slugs.length; i += 1) {
    const value = rawCounts?.[i];
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
  const onlineCount = Number(await redisPublisher.sCard(presenceKey(slug)));

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
    await redisPublisher.sRem(presenceKey(currentChannel), meta.presenceMember);
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

  await redisPublisher.sAdd(presenceKey(slug), meta.presenceMember);

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

    res.status(201).json({ token, username: requested });
  } catch (error) {
    await client.query('ROLLBACK');
    next(error);
  } finally {
    client.release();
  }
});

app.get('/api/session', requireAuth, (req, res) => {
  res.json({ username: req.auth.username });
});

app.post('/api/release', requireAuth, async (req, res, next) => {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    await client.query('DELETE FROM sessions WHERE username_key = $1', [req.auth.usernameKey]);
    await client.query('DELETE FROM user_claims WHERE username_key = $1', [req.auth.usernameKey]);

    await client.query('COMMIT');

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

    const channels = rows
      .map((row) => ({
        slug: row.slug,
        name: row.name,
        description: row.description,
        createdAt: new Date(row.created_at).toISOString(),
        onlineCount: counts[row.slug] || 0
      }))
      .sort((a, b) => b.onlineCount - a.onlineCount || a.slug.localeCompare(b.slug));

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
    channel: null,
    connectionId: crypto.randomUUID(),
    presenceMember: null
  };

  wsClients.set(ws, meta);

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
        const onlineCount = Number(await redisPublisher.sCard(presenceKey(slug)));

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
    detachFromChannel(ws, 'disconnect')
      .catch((error) => console.error('WebSocket close handler error:', error))
      .finally(() => {
        wsClients.delete(ws);
      });
  });

  ws.on('error', () => {
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
    CREATE INDEX IF NOT EXISTS idx_messages_channel_created_at
    ON messages(channel_slug, created_at DESC);
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
