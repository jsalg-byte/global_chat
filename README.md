# PulseRooms (PRD v1 Build)

A real-time public community chat app based on your PRD.

## Run

```bash
npm install
npm start
```

Open: http://localhost:3000

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
- Message durability in `data/store.json`
- Last 100 messages loaded on channel join

## Notes

- This implementation is single-node (no Redis pub/sub yet), which is fine for local/dev and small deployments.
- Data is persisted locally in `data/store.json`.
