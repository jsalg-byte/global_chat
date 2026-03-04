const MAX_MESSAGE_LENGTH = 2000;
const USERNAME_REGEX = /^[A-Za-z0-9_]{3,24}$/;

const state = {
  token: localStorage.getItem('chat_token'),
  username: null,
  channels: [],
  channelMap: new Map(),
  messagesByChannel: new Map(),
  activeChannel: null,
  onlineCounts: {},
  ws: null,
  connected: false,
  reconnectAttempt: 0,
  reconnectTimer: null,
  outboundQueue: [],
  unreadInActive: 0,
  typingUsers: new Map(),
  lastTypingSentAt: 0
};

const elements = {
  claimScreen: document.getElementById('claim-screen'),
  appScreen: document.getElementById('app-screen'),
  claimForm: document.getElementById('claim-form'),
  claimUsername: document.getElementById('claim-username'),
  claimSubmit: document.getElementById('claim-submit'),
  claimError: document.getElementById('claim-error'),
  currentUsername: document.getElementById('current-username'),
  releaseUsername: document.getElementById('release-username'),
  channelList: document.getElementById('channel-list'),
  activeChannelName: document.getElementById('active-channel-name'),
  activeChannelDescription: document.getElementById('active-channel-description'),
  activeOnline: document.getElementById('active-online'),
  messageList: document.getElementById('message-list'),
  typingIndicator: document.getElementById('typing-indicator'),
  messageForm: document.getElementById('message-form'),
  messageInput: document.getElementById('message-input'),
  messageCount: document.getElementById('message-count'),
  sendMessage: document.getElementById('send-message'),
  connectionStatus: document.getElementById('connection-status'),
  liveAnnouncer: document.getElementById('live-announcer'),
  toggleCreateChannel: document.getElementById('toggle-create-channel'),
  createChannelForm: document.getElementById('create-channel-form'),
  cancelCreateChannel: document.getElementById('cancel-create-channel'),
  createChannelError: document.getElementById('create-channel-error'),
  channelNameInput: document.getElementById('channel-name'),
  channelDescriptionInput: document.getElementById('channel-description'),
  newMessageNudge: document.getElementById('new-message-nudge')
};

function isMobileLikeInput() {
  return window.matchMedia('(max-width: 980px), (pointer: coarse)').matches;
}

function setConnectionBanner(text) {
  if (!text) {
    elements.connectionStatus.hidden = true;
    elements.connectionStatus.textContent = '';
    return;
  }

  elements.connectionStatus.hidden = false;
  elements.connectionStatus.textContent = text;
}

function clearSessionAndShowClaim() {
  state.token = null;
  state.username = null;
  state.channels = [];
  state.channelMap = new Map();
  state.messagesByChannel = new Map();
  state.activeChannel = null;
  state.onlineCounts = {};
  state.outboundQueue = [];
  state.typingUsers = new Map();
  state.unreadInActive = 0;
  localStorage.removeItem('chat_token');
  state.connected = false;
  if (state.ws) {
    state.ws.onclose = null;
    state.ws.close();
    state.ws = null;
  }

  if (state.reconnectTimer) {
    clearTimeout(state.reconnectTimer);
    state.reconnectTimer = null;
  }

  elements.appScreen.hidden = true;
  elements.claimScreen.hidden = false;
  elements.claimUsername.focus();
  setConnectionBanner('');
}

async function api(path, options = {}) {
  const headers = {
    'Content-Type': 'application/json',
    ...(options.headers || {})
  };

  if (state.token) {
    headers.Authorization = `Bearer ${state.token}`;
  }

  const response = await fetch(path, {
    ...options,
    headers
  });

  const payload = await response.json().catch(() => ({}));

  if (!response.ok) {
    if (response.status === 401) {
      clearSessionAndShowClaim();
    }

    const error = new Error(payload.message || payload.error || 'Request failed');
    error.status = response.status;
    error.payload = payload;
    throw error;
  }

  return payload;
}

function showClaimError(message) {
  elements.claimError.textContent = message || '';
}

function showCreateChannelError(message) {
  elements.createChannelError.textContent = message || '';
}

function showApp() {
  elements.claimScreen.hidden = true;
  elements.appScreen.hidden = false;
  elements.currentUsername.textContent = state.username;
  elements.messageInput.focus();
}

function renderChannels() {
  const sorted = [...state.channels].sort((a, b) => {
    const countA = state.onlineCounts[a.slug] || 0;
    const countB = state.onlineCounts[b.slug] || 0;
    if (countA !== countB) {
      return countB - countA;
    }
    return a.slug.localeCompare(b.slug);
  });

  elements.channelList.innerHTML = '';

  for (const channel of sorted) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'channel-item';
    if (channel.slug === state.activeChannel) {
      button.classList.add('active');
    }

    const count = state.onlineCounts[channel.slug] || 0;

    button.innerHTML = `
      <span class="channel-name">${channel.name}</span>
      <span class="channel-count">${count}</span>
    `;

    button.addEventListener('click', () => {
      switchChannel(channel.slug);
    });

    elements.channelList.appendChild(button);
  }
}

function renderActiveChannelHeader() {
  const channel = state.channelMap.get(state.activeChannel);
  if (!channel) {
    elements.activeChannelName.textContent = '#unknown';
    elements.activeChannelDescription.textContent = '';
    elements.activeOnline.textContent = '0 online';
    return;
  }

  elements.activeChannelName.textContent = channel.name;
  elements.activeChannelDescription.textContent = channel.description || 'Public room';

  const online = state.onlineCounts[state.activeChannel] || 0;
  elements.activeOnline.textContent = `${online} online`;
}

function shouldAutoScroll() {
  const { messageList } = elements;
  const distanceFromBottom = messageList.scrollHeight - messageList.scrollTop - messageList.clientHeight;
  return distanceFromBottom < 40;
}

function scrollMessagesToBottom() {
  elements.messageList.scrollTop = elements.messageList.scrollHeight;
  state.unreadInActive = 0;
  elements.newMessageNudge.hidden = true;
}

function renderMessages() {
  const messages = state.messagesByChannel.get(state.activeChannel) || [];
  const wasAtBottom = shouldAutoScroll();

  elements.messageList.innerHTML = '';

  for (const msg of messages) {
    const row = document.createElement('article');
    row.className = 'message-row';

    const isSelf = msg.username === state.username;
    if (isSelf) {
      row.classList.add('self');
    }

    const time = new Date(msg.timestamp).toLocaleTimeString([], {
      hour: '2-digit',
      minute: '2-digit'
    });

    row.innerHTML = `
      <header>
        <span class="message-author">${msg.username}</span>
        <span class="message-time">${time}</span>
      </header>
      <p>${escapeHtml(msg.text)}</p>
    `;

    elements.messageList.appendChild(row);
  }

  if (wasAtBottom) {
    scrollMessagesToBottom();
  }
}

function escapeHtml(text) {
  return String(text)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function renderTypingIndicator() {
  const names = Array.from(state.typingUsers.keys());
  if (names.length === 0) {
    elements.typingIndicator.textContent = '';
    return;
  }

  if (names.length === 1) {
    elements.typingIndicator.textContent = `${names[0]} is typing…`;
    return;
  }

  elements.typingIndicator.textContent = `${names.join(', ')} are typing…`;
}

function queueOrSend(payload) {
  if (state.connected && state.ws && state.ws.readyState === WebSocket.OPEN) {
    state.ws.send(JSON.stringify(payload));
    return;
  }

  state.outboundQueue.push(payload);
}

function flushOutboundQueue() {
  if (!state.connected || !state.ws || state.ws.readyState !== WebSocket.OPEN) {
    return;
  }

  while (state.outboundQueue.length) {
    const payload = state.outboundQueue.shift();
    state.ws.send(JSON.stringify(payload));
  }
}

function scheduleReconnect() {
  if (state.reconnectTimer) {
    return;
  }

  const seconds = Math.min(2 ** state.reconnectAttempt, 30);
  setConnectionBanner(`Disconnected. Reconnecting in ${seconds}s...`);

  state.reconnectTimer = setTimeout(() => {
    state.reconnectTimer = null;
    state.reconnectAttempt += 1;
    connectWebSocket();
  }, seconds * 1000);
}

function handleTypingEvent(username) {
  if (username === state.username) {
    return;
  }

  if (state.typingUsers.has(username)) {
    clearTimeout(state.typingUsers.get(username));
  }

  const timer = setTimeout(() => {
    state.typingUsers.delete(username);
    renderTypingIndicator();
  }, 3000);

  state.typingUsers.set(username, timer);
  renderTypingIndicator();
}

function applyMessage(msg) {
  if (!state.messagesByChannel.has(msg.channel)) {
    state.messagesByChannel.set(msg.channel, []);
  }

  const list = state.messagesByChannel.get(msg.channel);
  list.push(msg);

  if (msg.channel === state.activeChannel) {
    const atBottom = shouldAutoScroll();
    renderMessages();

    if (!atBottom) {
      state.unreadInActive += 1;
      elements.newMessageNudge.hidden = false;
      elements.newMessageNudge.textContent = `↓ ${state.unreadInActive} new message${state.unreadInActive > 1 ? 's' : ''}`;
    }

    elements.liveAnnouncer.textContent = `${msg.username} says ${msg.text}`;
  }
}

function connectWebSocket() {
  if (!state.token) {
    return;
  }

  if (state.ws && (state.ws.readyState === WebSocket.OPEN || state.ws.readyState === WebSocket.CONNECTING)) {
    return;
  }

  const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
  const url = `${protocol}://${window.location.host}/ws?token=${encodeURIComponent(state.token)}`;

  state.ws = new WebSocket(url);
  setConnectionBanner(state.reconnectAttempt > 0 ? 'Reconnecting...' : 'Connecting...');

  state.ws.onopen = () => {
    state.connected = true;
    state.reconnectAttempt = 0;
    setConnectionBanner('');

    if (state.activeChannel) {
      queueOrSend({ type: 'subscribe', channel: state.activeChannel });
    }

    flushOutboundQueue();
  };

  state.ws.onmessage = (event) => {
    let payload;

    try {
      payload = JSON.parse(event.data);
    } catch (error) {
      return;
    }

    if (payload.type === 'connected') {
      if (payload.channelCounts && typeof payload.channelCounts === 'object') {
        state.onlineCounts = { ...state.onlineCounts, ...payload.channelCounts };
        renderChannels();
        renderActiveChannelHeader();
      }
      return;
    }

    if (payload.type === 'channel_counts') {
      state.onlineCounts = { ...state.onlineCounts, ...payload.counts };
      renderChannels();
      renderActiveChannelHeader();
      return;
    }

    if (payload.type === 'channel_created' && payload.channel) {
      if (!state.channelMap.has(payload.channel.slug)) {
        state.channels.push(payload.channel);
        state.channelMap.set(payload.channel.slug, payload.channel);
        state.onlineCounts[payload.channel.slug] = payload.channel.onlineCount || 0;
        renderChannels();
      }
      return;
    }

    if (payload.type === 'subscribed') {
      state.onlineCounts[payload.channel] = payload.onlineCount;
      renderChannels();
      renderActiveChannelHeader();
      return;
    }

    if (payload.type === 'presence') {
      state.onlineCounts[payload.channel] = payload.onlineCount;
      renderChannels();
      renderActiveChannelHeader();
      return;
    }

    if (payload.type === 'typing') {
      if (payload.channel === state.activeChannel) {
        handleTypingEvent(payload.username);
      }
      return;
    }

    if (payload.type === 'message') {
      applyMessage(payload);
      return;
    }

    if (payload.type === 'session_revoked' || payload.type === 'auth_error') {
      clearSessionAndShowClaim();
    }
  };

  state.ws.onclose = (event) => {
    state.connected = false;
    state.ws = null;

    if (event.code === 4401) {
      clearSessionAndShowClaim();
      return;
    }

    scheduleReconnect();
  };

  state.ws.onerror = () => {
    if (state.ws) {
      state.ws.close();
    }
  };
}

async function loadChannels() {
  const result = await api('/api/channels');
  state.channels = result.channels;
  state.channelMap = new Map(state.channels.map((channel) => [channel.slug, channel]));

  for (const channel of state.channels) {
    state.onlineCounts[channel.slug] = channel.onlineCount || 0;
  }

  renderChannels();

  if (!state.activeChannel && state.channels.length > 0) {
    await switchChannel(state.channels[0].slug);
  }
}

async function loadMessages(channel) {
  const response = await api(`/api/channels/${encodeURIComponent(channel)}/messages`);
  state.messagesByChannel.set(channel, response.messages || []);

  if (channel === state.activeChannel) {
    renderMessages();
  }
}

async function switchChannel(channel) {
  if (!state.channelMap.has(channel)) {
    return;
  }

  state.activeChannel = channel;
  state.typingUsers.clear();
  renderTypingIndicator();
  renderChannels();
  renderActiveChannelHeader();

  if (!state.messagesByChannel.has(channel)) {
    await loadMessages(channel);
  } else {
    renderMessages();
  }

  queueOrSend({ type: 'subscribe', channel });
  elements.messageInput.focus();
}

function updateMessageComposerState() {
  const length = elements.messageInput.value.length;
  elements.messageCount.textContent = `${length} / ${MAX_MESSAGE_LENGTH}`;

  const tooLong = length > MAX_MESSAGE_LENGTH;
  elements.messageCount.classList.toggle('danger', tooLong);

  const empty = elements.messageInput.value.trim().length === 0;
  elements.sendMessage.disabled = tooLong || empty;
}

async function initSession() {
  if (!state.token) {
    clearSessionAndShowClaim();
    return;
  }

  try {
    const session = await api('/api/session');
    state.username = session.username;
    showApp();
    await loadChannels();
    connectWebSocket();
  } catch (error) {
    clearSessionAndShowClaim();
  }
}

elements.claimForm.addEventListener('submit', async (event) => {
  event.preventDefault();

  const requested = elements.claimUsername.value.trim();

  if (!USERNAME_REGEX.test(requested)) {
    showClaimError('3–24 characters, letters/numbers/underscores only.');
    return;
  }

  elements.claimSubmit.disabled = true;
  showClaimError('');

  try {
    const result = await api('/api/claim', {
      method: 'POST',
      body: JSON.stringify({ username: requested })
    });

    state.token = result.token;
    state.username = result.username;
    localStorage.setItem('chat_token', result.token);

    showApp();
    await loadChannels();
    connectWebSocket();
  } catch (error) {
    if (error.status === 409) {
      showClaimError('That username is taken.');
    } else if (error.status === 400) {
      showClaimError('3–24 characters, letters/numbers/underscores only.');
    } else {
      showClaimError('Unable to claim username right now.');
    }
  } finally {
    elements.claimSubmit.disabled = false;
  }
});

elements.releaseUsername.addEventListener('click', async () => {
  if (!confirm('Release this username? This cannot be undone in v1.')) {
    return;
  }

  try {
    await api('/api/release', { method: 'POST' });
  } catch (error) {
    // Continue with local cleanup even if release call fails.
  }

  clearSessionAndShowClaim();
});

elements.toggleCreateChannel.addEventListener('click', () => {
  elements.createChannelForm.hidden = false;
  elements.channelNameInput.focus();
});

elements.cancelCreateChannel.addEventListener('click', () => {
  elements.createChannelForm.hidden = true;
  showCreateChannelError('');
  elements.createChannelForm.reset();
});

elements.createChannelForm.addEventListener('submit', async (event) => {
  event.preventDefault();

  const name = elements.channelNameInput.value.trim();
  const description = elements.channelDescriptionInput.value.trim();

  showCreateChannelError('');

  try {
    const result = await api('/api/channels', {
      method: 'POST',
      body: JSON.stringify({ name, description })
    });

    const channel = result.channel;
    state.channels.push(channel);
    state.channelMap.set(channel.slug, channel);
    state.onlineCounts[channel.slug] = 0;

    renderChannels();
    elements.createChannelForm.hidden = true;
    elements.createChannelForm.reset();

    await switchChannel(channel.slug);
  } catch (error) {
    if (error.status === 409) {
      showCreateChannelError('A channel with that name already exists.');
    } else if (error.status === 400) {
      showCreateChannelError(error.message || 'Invalid channel name.');
    } else {
      showCreateChannelError('Could not create channel right now.');
    }
  }
});

elements.messageInput.addEventListener('input', () => {
  updateMessageComposerState();

  const now = Date.now();
  if (state.activeChannel && now - state.lastTypingSentAt > 1000) {
    queueOrSend({ type: 'typing', channel: state.activeChannel });
    state.lastTypingSentAt = now;
  }
});

elements.messageForm.addEventListener('submit', (event) => {
  event.preventDefault();

  const text = elements.messageInput.value.trim();

  if (!text || text.length > MAX_MESSAGE_LENGTH || !state.activeChannel) {
    return;
  }

  queueOrSend({
    type: 'message',
    channel: state.activeChannel,
    text
  });

  elements.messageInput.value = '';
  updateMessageComposerState();
  elements.messageInput.focus();
});

elements.messageInput.addEventListener('keydown', (event) => {
  if (isMobileLikeInput()) {
    return;
  }

  if (event.key === 'Enter' && !event.shiftKey) {
    event.preventDefault();
    elements.messageForm.requestSubmit();
  }
});

elements.newMessageNudge.addEventListener('click', () => {
  scrollMessagesToBottom();
});

elements.messageList.addEventListener('scroll', () => {
  if (shouldAutoScroll()) {
    state.unreadInActive = 0;
    elements.newMessageNudge.hidden = true;
  }
});

updateMessageComposerState();
initSession();
