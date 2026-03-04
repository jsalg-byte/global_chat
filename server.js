const crypto = require('crypto');
const fs = require('fs/promises');
const http = require('http');
const path = require('path');
const { URL } = require('url');

const express = require('express');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const DATA_DIR = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'store.json');
const USERNAME_REGEX = /^[A-Za-z0-9_]{3,24}$/;
const MAX_MESSAGE_LENGTH = 2000;
const MAX_DESCRIPTION_LENGTH = 120;

const app = express();
app.use(express.json({ limit: '16kb' }));
app.use(express.static(path.join(__dirname, 'public')));

let db = {
  usernameClaims: {},
  tokens: {},
  channels: {},
  messages: {}
};

let persistQueue = Promise.resolve();
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

async function ensureDataStore() {
  await fs.mkdir(DATA_DIR, { recursive: true });

  try {
    const raw = await fs.readFile(DATA_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    db = {
      usernameClaims: parsed.usernameClaims || {},
      tokens: parsed.tokens || {},
      channels: parsed.channels || {},
      messages: parsed.messages || {}
    };
  } catch (error) {
    if (error.code !== 'ENOENT') {
      throw error;
    }
  }

  if (!db.channels.general) {
    db.channels.general = {
      slug: 'general',
      name: '#general',
      description: 'Default public channel',
      createdAt: nowIso()
    };
  }

  if (!db.messages.general) {
    db.messages.general = [];
  }

  await persist();
}

function persist() {
  persistQueue = persistQueue.then(() => fs.writeFile(DATA_FILE, JSON.stringify(db, null, 2), 'utf8'));
  return persistQueue;
}

function sendJson(ws, payload) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(payload));
  }
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

function getUsernameFromToken(token) {
  if (!token) {
    return null;
  }

  const key = db.tokens[token];
  if (!key) {
    return null;
  }

  const claim = db.usernameClaims[key];
  if (!claim || claim.token !== token) {
    return null;
  }

  return claim.username;
}

function requireAuth(req, res, next) {
  const token = getTokenFromRequest(req);
  const username = getUsernameFromToken(token);

  if (!username) {
    res.status(401).json({ error: 'unauthorized' });
    return;
  }

  req.auth = { token, username, usernameKey: username.toLowerCase() };
  next();
}

function getOnlineCount(slug) {
  return subscribers.get(slug)?.size || 0;
}

function getChannelCounts() {
  const counts = {};
  for (const slug of Object.keys(db.channels)) {
    counts[slug] = getOnlineCount(slug);
  }
  return counts;
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

function broadcastCounts() {
  broadcastAll({ type: 'channel_counts', counts: getChannelCounts() });
}

function emitPresence(slug, action, username) {
  broadcastChannel(slug, {
    type: 'presence',
    channel: slug,
    action,
    username,
    onlineCount: getOnlineCount(slug),
    timestamp: nowIso()
  });
}

function detachFromChannel(ws, reason = 'leave') {
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

  meta.channel = null;
  emitPresence(currentChannel, reason, meta.username);
  broadcastCounts();
}

function attachToChannel(ws, slug) {
  const meta = wsClients.get(ws);
  if (!meta) {
    return;
  }

  if (meta.channel === slug) {
    return;
  }

  if (meta.channel) {
    detachFromChannel(ws, 'switch');
  }

  if (!subscribers.has(slug)) {
    subscribers.set(slug, new Set());
  }

  subscribers.get(slug).add(ws);
  meta.channel = slug;
  emitPresence(slug, 'join', meta.username);
  broadcastCounts();
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

app.post('/api/claim', async (req, res) => {
  const requested = typeof req.body?.username === 'string' ? req.body.username.trim() : '';

  if (!USERNAME_REGEX.test(requested)) {
    res.status(400).json({ error: 'invalid_username', message: '3–24 characters, letters/numbers/underscores only.' });
    return;
  }

  const key = requested.toLowerCase();
  if (db.usernameClaims[key]) {
    res.status(409).json({ error: 'username_taken', message: 'That username is taken.' });
    return;
  }

  let token;
  do {
    token = crypto.randomBytes(32).toString('hex');
  } while (db.tokens[token]);

  db.usernameClaims[key] = {
    username: requested,
    token,
    claimedAt: nowIso()
  };
  db.tokens[token] = key;

  await persist();

  res.status(201).json({ token, username: requested });
});

app.get('/api/session', requireAuth, (req, res) => {
  res.json({ username: req.auth.username });
});

app.post('/api/release', requireAuth, async (req, res) => {
  const { token, usernameKey } = req.auth;

  delete db.tokens[token];
  delete db.usernameClaims[usernameKey];

  for (const [ws, meta] of wsClients.entries()) {
    if (meta.token === token) {
      sendJson(ws, { type: 'session_revoked' });
      ws.close(4401, 'session revoked');
    }
  }

  await persist();

  res.json({ ok: true });
});

app.get('/api/channels', requireAuth, (req, res) => {
  const channels = Object.values(db.channels)
    .map((channel) => ({
      ...channel,
      onlineCount: getOnlineCount(channel.slug)
    }))
    .sort((a, b) => b.onlineCount - a.onlineCount || a.slug.localeCompare(b.slug));

  res.json({ channels });
});

app.post('/api/channels', requireAuth, async (req, res) => {
  const slug = slugifyChannelName(req.body?.name || '');
  if (!slug) {
    res.status(400).json({ error: 'invalid_channel_name', message: 'Channel names must slugify to 2-32 chars.' });
    return;
  }

  if (db.channels[slug]) {
    res.status(409).json({ error: 'channel_exists', message: 'A channel with that name already exists.' });
    return;
  }

  const channel = {
    slug,
    name: `#${slug}`,
    description: sanitizeDescription(req.body?.description),
    createdAt: nowIso()
  };

  db.channels[slug] = channel;
  db.messages[slug] = [];

  await persist();

  broadcastAll({
    type: 'channel_created',
    channel: { ...channel, onlineCount: 0 }
  });
  broadcastCounts();

  res.status(201).json({ channel });
});

app.get('/api/channels/:slug/messages', requireAuth, (req, res) => {
  const slug = String(req.params.slug || '').toLowerCase();

  if (!db.channels[slug]) {
    res.status(404).json({ error: 'not_found' });
    return;
  }

  const messages = db.messages[slug] || [];
  res.json({ messages: messages.slice(-100) });
});

app.use((req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const token = url.searchParams.get('token');
  const username = getUsernameFromToken(token);

  if (!username) {
    sendJson(ws, { type: 'auth_error', message: 'Invalid session token.' });
    ws.close(4401, 'unauthorized');
    return;
  }

  wsClients.set(ws, { token, username, channel: null });

  sendJson(ws, {
    type: 'connected',
    username,
    channelCounts: getChannelCounts()
  });

  ws.on('message', async (raw) => {
    const payload = parseWsPayload(raw);
    if (!payload || typeof payload.type !== 'string') {
      sendJson(ws, { type: 'error', message: 'Invalid payload.' });
      return;
    }

    const meta = wsClients.get(ws);
    if (!meta) {
      return;
    }

    if (payload.type === 'subscribe') {
      const slug = String(payload.channel || '').toLowerCase();
      if (!db.channels[slug]) {
        sendJson(ws, { type: 'error', message: 'Channel not found.' });
        return;
      }

      attachToChannel(ws, slug);
      sendJson(ws, {
        type: 'subscribed',
        channel: slug,
        onlineCount: getOnlineCount(slug)
      });
      return;
    }

    if (payload.type === 'message') {
      const channel = String(payload.channel || '').toLowerCase();
      const text = typeof payload.text === 'string' ? payload.text.trim() : '';

      if (!meta.channel || meta.channel !== channel || !db.channels[channel]) {
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
        username: meta.username,
        text,
        timestamp: nowIso()
      };

      db.messages[channel].push(message);
      await persist();

      broadcastChannel(channel, { type: 'message', ...message });
      return;
    }

    if (payload.type === 'typing') {
      const channel = String(payload.channel || '').toLowerCase();
      if (!meta.channel || meta.channel !== channel || !db.channels[channel]) {
        return;
      }

      broadcastChannel(
        channel,
        {
          type: 'typing',
          channel,
          username: meta.username,
          timestamp: nowIso()
        },
        ws
      );
      return;
    }

    sendJson(ws, { type: 'error', message: `Unsupported type: ${payload.type}` });
  });

  ws.on('close', () => {
    detachFromChannel(ws, 'disconnect');
    wsClients.delete(ws);
  });

  ws.on('error', () => {
    detachFromChannel(ws, 'disconnect');
    wsClients.delete(ws);
  });
});

async function start() {
  await ensureDataStore();
  server.listen(PORT, () => {
    console.log(`Realtime chat server running on http://localhost:${PORT}`);
  });
}

start().catch((error) => {
  console.error('Failed to start server:', error);
  process.exit(1);
});
