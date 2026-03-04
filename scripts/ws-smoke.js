const { WebSocket } = require('ws');

const BASE_HTTP = 'http://localhost:3012';
const BASE_WS = 'ws://localhost:3012/ws';

async function claim(username) {
  const response = await fetch(`${BASE_HTTP}/api/claim`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username })
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Claim failed (${response.status}): ${text}`);
  }

  return response.json();
}

function connect(token) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`${BASE_WS}?token=${encodeURIComponent(token)}`);

    const onError = (error) => {
      ws.removeAllListeners();
      reject(error);
    };

    ws.once('error', onError);
    ws.once('open', () => {
      ws.off('error', onError);
      resolve(ws);
    });
  });
}

function send(ws, payload) {
  ws.send(JSON.stringify(payload));
}

function waitFor(checkFn, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const timer = setInterval(() => {
      if (checkFn()) {
        clearInterval(timer);
        resolve();
        return;
      }

      if (Date.now() - started > timeoutMs) {
        clearInterval(timer);
        reject(new Error('Timed out waiting for expected event.'));
      }
    }, 50);
  });
}

async function main() {
  const alice = await claim('WsAlice_1');
  const bob = await claim('WsBob_1');

  const aliceWs = await connect(alice.token);
  const bobWs = await connect(bob.token);

  const aliceEvents = [];
  const bobEvents = [];

  aliceWs.on('message', (data) => {
    aliceEvents.push(JSON.parse(data.toString()));
  });

  bobWs.on('message', (data) => {
    bobEvents.push(JSON.parse(data.toString()));
  });

  send(aliceWs, { type: 'subscribe', channel: 'general' });
  send(bobWs, { type: 'subscribe', channel: 'general' });

  await waitFor(
    () =>
      bobEvents.some((event) => event.type === 'subscribed' && event.channel === 'general') &&
      aliceEvents.some((event) => event.type === 'subscribed' && event.channel === 'general')
  );

  send(aliceWs, { type: 'typing', channel: 'general' });
  send(aliceWs, { type: 'message', channel: 'general', text: 'hello from websocket smoke test' });

  await waitFor(
    () =>
      bobEvents.some((event) => event.type === 'typing' && event.channel === 'general' && event.username === 'WsAlice_1') &&
      bobEvents.some(
        (event) =>
          event.type === 'message' &&
          event.channel === 'general' &&
          event.username === 'WsAlice_1' &&
          event.text === 'hello from websocket smoke test'
      )
  );

  const typingEvent = bobEvents.find((event) => event.type === 'typing' && event.username === 'WsAlice_1');
  const messageEvent = bobEvents.find((event) => event.type === 'message' && event.username === 'WsAlice_1');

  aliceWs.close();
  bobWs.close();

  console.log(JSON.stringify({ typingEvent, messageEvent }, null, 2));
}

main().catch((error) => {
  console.error(error.message || error);
  process.exit(1);
});
