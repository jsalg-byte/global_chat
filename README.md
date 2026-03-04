# yungjewboii global chat (Postgres + Redis)

A real-time public community chat app based on your PRD.

## Environment

Copy `.env.example` values into your local `.env` or Coolify dashboard:

- `PORT`
- `DATABASE_URL`
- `REDIS_URL`
- `REDIS_PREFIX`
- `NODE_ENV`

## Run

```bash
npm install
npm start
```

Open: http://localhost:3000

## Architecture

- Postgres stores usernames, session tokens, channels, and durable messages.
- Redis stores live presence sets and propagates real-time events across instances via pub/sub.
- WebSocket fanout remains channel-based.

## Implemented v1 Scope

- Username claim auth (no password), globally unique + case-insensitive
- 256-bit opaque token sessions persisted in `localStorage`
- Explicit "Release Username" action that frees the name
- Public channel list with online counts
- Channel creation with slug uniqueness and optional description (120 max)
- Single active channel at a time
- Real-time messaging over WebSockets
- Typing indicators (`max 1/sec` client debounce, `3s` timeout)
- Presence updates and live online-count badges
- Reconnect with exponential backoff (`1s -> 2s -> 4s -> ... -> 30s`)
- Outbound message queue while disconnected
- Last 100 messages loaded on channel join
