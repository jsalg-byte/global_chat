const MAX_MESSAGE_LENGTH = 2000;
const USERNAME_REGEX = /^[A-Za-z0-9_]{3,24}$/;
const KONAMI_SEQUENCE = ['ArrowUp', 'ArrowUp', 'ArrowDown', 'ArrowDown', 'ArrowLeft', 'ArrowRight', 'ArrowLeft', 'ArrowRight', 'b', 'a'];

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
  lastTypingSentAt: 0,
  konamiIndex: 0,
  adminPassword: '',
  adminActiveTab: 'events'
};

const elements = {
  claimScreen: document.getElementById('claim-screen'),
  appScreen: document.getElementById('app-screen'),
  claimForm: document.getElementById('claim-form'),
  claimHoneypot: document.getElementById('claim-website'),
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
  newMessageNudge: document.getElementById('new-message-nudge'),
  adminDashboard: document.getElementById('admin-dashboard'),
  adminClose: document.getElementById('admin-close'),
  adminPasswordForm: document.getElementById('admin-password-form'),
  adminPasswordInput: document.getElementById('admin-password'),
  adminUnlock: document.getElementById('admin-unlock'),
  adminRefresh: document.getElementById('admin-refresh'),
  adminUnblock: document.getElementById('admin-unblock'),
  adminError: document.getElementById('admin-error'),
  adminTabs: document.getElementById('admin-tabs'),
  adminTabButtons: document.querySelectorAll('[data-admin-tab]'),
  adminPanels: document.getElementById('admin-panels'),
  adminPanelEvents: document.getElementById('admin-panel-events'),
  adminPanelUsernames: document.getElementById('admin-panel-usernames'),
  adminPanelNonUs: document.getElementById('admin-panel-non-us'),
  adminPanelChannels: document.getElementById('admin-panel-channels'),
  adminEventsBody: document.getElementById('admin-events-body'),
  adminUsernamesBody: document.getElementById('admin-usernames-body'),
  adminNonUsBody: document.getElementById('admin-non-us-body'),
  adminChannelsBody: document.getElementById('admin-channels-body')
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
  state.adminPassword = '';
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
  elements.adminPasswordInput.value = '';
  elements.claimUsername.focus();
  closeAdminDashboard();
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

function showAdminError(message) {
  elements.adminError.textContent = message || '';
}

function normalizeKonamiKey(key) {
  if (!key) {
    return '';
  }
  return key.length === 1 ? key.toLowerCase() : key;
}

function openAdminDashboard() {
  elements.adminDashboard.hidden = false;
  elements.adminDashboard.setAttribute('aria-hidden', 'false');
  showAdminError('');

  if (state.adminPassword) {
    loadAdminDashboard(state.adminPassword);
    return;
  }

  elements.adminTabs.hidden = true;
  elements.adminPanels.hidden = true;
  elements.adminRefresh.hidden = true;
  elements.adminUnblock.hidden = true;
  elements.adminPasswordInput.focus();
}

function closeAdminDashboard() {
  elements.adminDashboard.hidden = true;
  elements.adminDashboard.setAttribute('aria-hidden', 'true');
  showAdminError('');
}

function formatTime(value) {
  try {
    return new Date(value).toLocaleString();
  } catch (error) {
    return String(value || '');
  }
}

function truncateText(value, maxLength = 60) {
  const text = String(value || '');
  if (text.length <= maxLength) {
    return text;
  }
  return `${text.slice(0, maxLength)}...`;
}

function buildDetailsCell(event) {
  const details = {
    tokenFingerprint: event.token_fingerprint || null,
    forwardedFor: event.forwarded_for || null,
    remoteAddress: event.remote_address || null,
    headers: event.headers_json || null,
    body: event.body_json || null,
    meta: event.meta_json || null
  };

  return `<details class=\"admin-details\"><summary>Inspect</summary><pre>${escapeHtml(JSON.stringify(details, null, 2))}</pre></details>`;
}

function setAdminTab(tabId) {
  const nextTab = ['events', 'usernames', 'non-us', 'channels'].includes(tabId) ? tabId : 'events';
  state.adminActiveTab = nextTab;

  for (const button of elements.adminTabButtons) {
    const isActive = button.dataset.adminTab === nextTab;
    button.classList.toggle('active', isActive);
    button.setAttribute('aria-selected', isActive ? 'true' : 'false');
  }

  elements.adminPanelEvents.hidden = nextTab !== 'events';
  elements.adminPanelUsernames.hidden = nextTab !== 'usernames';
  elements.adminPanelNonUs.hidden = nextTab !== 'non-us';
  elements.adminPanelChannels.hidden = nextTab !== 'channels';
}

function renderAdminEvents(events) {
  elements.adminEventsBody.innerHTML = '';

  if (!events.length) {
    const row = document.createElement('tr');
    row.innerHTML = '<td class=\"admin-empty\" colspan=\"8\">No events yet.</td>';
    elements.adminEventsBody.appendChild(row);
    return;
  }

  for (const event of events) {
    const row = document.createElement('tr');
    const route = [event.method, event.path].filter(Boolean).join(' ');
    const ipText = event.ip_address || event.forwarded_for || event.remote_address || '-';
    const userText = event.username || '-';
    const status = event.status_code ?? '-';
    const device = truncateText(event.user_agent || '-', 90);

    row.innerHTML = `
      <td>${escapeHtml(formatTime(event.event_time))}</td>
      <td>${escapeHtml(event.event_type || '-')}</td>
      <td>${escapeHtml(userText)}</td>
      <td title=\"${escapeHtml(ipText)}\">${escapeHtml(truncateText(ipText, 35))}</td>
      <td title=\"${escapeHtml(route)}\">${escapeHtml(truncateText(route, 56))}</td>
      <td>${escapeHtml(String(status))}</td>
      <td title=\"${escapeHtml(event.user_agent || '')}\">${escapeHtml(device)}</td>
      <td>${buildDetailsCell(event)}</td>
    `;

    elements.adminEventsBody.appendChild(row);
  }
}

function renderAdminUsernames(usernames) {
  elements.adminUsernamesBody.innerHTML = '';

  if (!usernames.length) {
    const row = document.createElement('tr');
    row.innerHTML = '<td class=\"admin-empty\" colspan=\"6\">No active usernames.</td>';
    elements.adminUsernamesBody.appendChild(row);
    return;
  }

  for (const username of usernames) {
    const row = document.createElement('tr');
    const actionDisabled = !username.username_key ? 'disabled' : '';

    row.innerHTML = `
      <td>${escapeHtml(username.username_original || '-')}</td>
      <td>${escapeHtml(username.username_key || '-')}</td>
      <td>${escapeHtml(formatTime(username.claimed_at))}</td>
      <td>${username.has_session ? 'Yes' : 'No'}</td>
      <td>${username.session_created_at ? escapeHtml(formatTime(username.session_created_at)) : '-'}</td>
      <td>
        <button
          type=\"button\"
          class=\"admin-release-button\"
          data-release-username-key=\"${escapeHtml(username.username_key || '')}\"
          ${actionDisabled}
        >
          Release
        </button>
      </td>
    `;

    elements.adminUsernamesBody.appendChild(row);
  }
}

function renderAdminNonUsEvents(events) {
  elements.adminNonUsBody.innerHTML = '';

  if (!events.length) {
    const row = document.createElement('tr');
    row.innerHTML = '<td class=\"admin-empty\" colspan=\"8\">No non-USA IP events.</td>';
    elements.adminNonUsBody.appendChild(row);
    return;
  }

  for (const event of events) {
    const row = document.createElement('tr');
    const route = [event.method, event.path].filter(Boolean).join(' ');
    const ipText = event.ip_address || event.forwarded_for || event.remote_address || '-';
    const userText = event.username || '-';
    const status = event.status_code ?? '-';

    row.innerHTML = `
      <td>${escapeHtml(formatTime(event.event_time))}</td>
      <td>${escapeHtml(event.country_code || '-')}</td>
      <td>${escapeHtml(event.event_type || '-')}</td>
      <td>${escapeHtml(userText)}</td>
      <td title=\"${escapeHtml(ipText)}\">${escapeHtml(truncateText(ipText, 35))}</td>
      <td title=\"${escapeHtml(route)}\">${escapeHtml(truncateText(route, 56))}</td>
      <td>${escapeHtml(String(status))}</td>
      <td>${buildDetailsCell(event)}</td>
    `;

    elements.adminNonUsBody.appendChild(row);
  }
}

function renderAdminChannels(channels) {
  elements.adminChannelsBody.innerHTML = '';

  if (!channels.length) {
    const row = document.createElement('tr');
    row.innerHTML = '<td class=\"admin-empty\" colspan=\"7\">No chatrooms found.</td>';
    elements.adminChannelsBody.appendChild(row);
    return;
  }

  for (const channel of channels) {
    const row = document.createElement('tr');
    const description = channel.description || '-';
    const messages = Number(channel.message_count || 0);
    const online = Number(channel.online_count || 0);

    row.innerHTML = `
      <td>${escapeHtml(channel.name || '#unknown')}</td>
      <td>${escapeHtml(channel.slug || '-')}</td>
      <td title=\"${escapeHtml(description)}\">${escapeHtml(truncateText(description, 80))}</td>
      <td>${escapeHtml(formatTime(channel.created_at))}</td>
      <td>${escapeHtml(String(online))}</td>
      <td>${escapeHtml(String(messages))}</td>
      <td>
        <button
          type=\"button\"
          class=\"admin-delete-channel-button\"
          data-delete-channel-slug=\"${escapeHtml(channel.slug || '')}\"
          ${channel.slug ? '' : 'disabled'}
        >
          Delete
        </button>
      </td>
    `;

    elements.adminChannelsBody.appendChild(row);
  }
}

async function fetchAdminData(path, password) {
  const response = await fetch(path, {
    headers: {
      'x-admin-password': password
    }
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.message || 'Unable to load dashboard.');
  }

  return payload;
}

async function loadAdminDashboard(password) {
  if (!password) {
    showAdminError('Password required.');
    return;
  }

  showAdminError('');
  elements.adminUnlock.disabled = true;
  elements.adminRefresh.disabled = true;
  elements.adminUnblock.disabled = true;

  try {
    const [eventsPayload, usernamesPayload, nonUsPayload, channelsPayload] = await Promise.all([
      fetchAdminData('/api/admin/events?limit=500', password),
      fetchAdminData('/api/admin/usernames?limit=10000', password),
      fetchAdminData('/api/admin/events/non-us?limit=500', password),
      fetchAdminData('/api/admin/channels?limit=10000', password)
    ]);

    state.adminPassword = password;
    elements.adminTabs.hidden = false;
    elements.adminPanels.hidden = false;
    elements.adminRefresh.hidden = false;
    elements.adminUnblock.hidden = false;
    renderAdminEvents(eventsPayload.events || []);
    renderAdminUsernames(usernamesPayload.usernames || []);
    renderAdminNonUsEvents(nonUsPayload.events || []);
    renderAdminChannels(channelsPayload.channels || []);
    setAdminTab(state.adminActiveTab);
  } catch (error) {
    state.adminPassword = '';
    elements.adminTabs.hidden = true;
    elements.adminPanels.hidden = true;
    elements.adminRefresh.hidden = true;
    elements.adminUnblock.hidden = true;
    showAdminError(error.message || 'Unable to load dashboard.');
  } finally {
    elements.adminUnlock.disabled = false;
    elements.adminRefresh.disabled = false;
    elements.adminUnblock.disabled = false;
  }
}

async function releaseAdminUsername(usernameKey) {
  const password = state.adminPassword || elements.adminPasswordInput.value;
  if (!password) {
    showAdminError('Password required.');
    return;
  }

  if (!usernameKey) {
    showAdminError('Invalid username key.');
    return;
  }

  showAdminError('');

  try {
    const response = await fetch('/api/admin/usernames/release', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-admin-password': password
      },
      body: JSON.stringify({ usernameKey })
    });

    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.message || 'Failed to release username.');
    }

    await loadAdminDashboard(password);
  } catch (error) {
    showAdminError(error.message || 'Failed to release username.');
  }
}

async function deleteAdminChannel(channelSlug) {
  const password = state.adminPassword || elements.adminPasswordInput.value;
  if (!password) {
    showAdminError('Password required.');
    return;
  }

  if (!channelSlug) {
    showAdminError('Invalid channel slug.');
    return;
  }

  if (!confirm(`Delete #${channelSlug}? This removes the room and its messages permanently.`)) {
    return;
  }

  showAdminError('');

  try {
    const response = await fetch('/api/admin/channels/delete', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-admin-password': password
      },
      body: JSON.stringify({ slug: channelSlug })
    });

    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.message || 'Failed to delete chatroom.');
    }

    await loadAdminDashboard(password);
  } catch (error) {
    showAdminError(error.message || 'Failed to delete chatroom.');
  }
}

function showApp() {
  elements.claimScreen.hidden = true;
  elements.appScreen.hidden = false;
  elements.currentUsername.textContent = state.username;
  elements.messageInput.focus();
}

function renderChannels() {
  elements.channelList.innerHTML = '';

  for (const channel of state.channels) {
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

function removeChannelLocally(channelSlug) {
  if (!channelSlug) {
    return;
  }

  const existing = state.channelMap.get(channelSlug);
  if (!existing) {
    return;
  }

  state.channels = state.channels.filter((channel) => channel.slug !== channelSlug);
  state.channelMap.delete(channelSlug);
  state.messagesByChannel.delete(channelSlug);
  delete state.onlineCounts[channelSlug];

  if (state.activeChannel === channelSlug) {
    state.activeChannel = null;
    state.typingUsers.clear();
    renderTypingIndicator();

    if (state.channels.length > 0) {
      switchChannel(state.channels[0].slug).catch(() => {});
    } else {
      elements.messageList.innerHTML = '';
      renderChannels();
      renderActiveChannelHeader();
      updateMessageComposerState();
    }
    return;
  }

  renderChannels();
  renderActiveChannelHeader();
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

    if (payload.type === 'channel_deleted' && payload.channel?.slug) {
      removeChannelLocally(payload.channel.slug);
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
  updateMessageComposerState();
}

function updateMessageComposerState() {
  const length = elements.messageInput.value.length;
  elements.messageCount.textContent = `${length} / ${MAX_MESSAGE_LENGTH}`;

  const tooLong = length > MAX_MESSAGE_LENGTH;
  elements.messageCount.classList.toggle('danger', tooLong);

  const empty = elements.messageInput.value.trim().length === 0;
  const noActiveChannel = !state.activeChannel;
  elements.sendMessage.disabled = tooLong || empty || noActiveChannel;
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
    if (error.status === 401) {
      clearSessionAndShowClaim();
      return;
    }

    setConnectionBanner(error.message || 'Unable to initialize session.');
  }
}

elements.claimForm.addEventListener('submit', async (event) => {
  event.preventDefault();

  const requested = elements.claimUsername.value.trim();
  const website = elements.claimHoneypot.value.trim();

  if (!USERNAME_REGEX.test(requested)) {
    showClaimError('3–24 characters, letters/numbers/underscores only.');
    return;
  }

  elements.claimSubmit.disabled = true;
  showClaimError('');

  try {
    const result = await api('/api/claim', {
      method: 'POST',
      body: JSON.stringify({ username: requested, website })
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

elements.adminClose.addEventListener('click', () => {
  closeAdminDashboard();
});

elements.adminDashboard.addEventListener('click', (event) => {
  if (event.target === elements.adminDashboard) {
    closeAdminDashboard();
  }
});

elements.adminPasswordForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const password = elements.adminPasswordInput.value;
  await loadAdminDashboard(password);
});

elements.adminRefresh.addEventListener('click', async () => {
  const password = state.adminPassword || elements.adminPasswordInput.value;
  await loadAdminDashboard(password);
});

elements.adminUnblock.addEventListener('click', async () => {
  const password = state.adminPassword || elements.adminPasswordInput.value;
  if (!password) {
    showAdminError('Password required.');
    return;
  }

  showAdminError('');

  try {
    const response = await fetch('/api/admin/unblock-me', {
      method: 'POST',
      headers: {
        'x-admin-password': password
      }
    });

    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.message || 'Failed to unblock IP.');
    }

    await loadAdminDashboard(password);
  } catch (error) {
    showAdminError(error.message || 'Failed to unblock IP.');
  }
});

elements.adminTabs.addEventListener('click', (event) => {
  const button = event.target.closest('[data-admin-tab]');
  if (!button) {
    return;
  }
  setAdminTab(button.dataset.adminTab);
});

elements.adminUsernamesBody.addEventListener('click', async (event) => {
  const button = event.target.closest('[data-release-username-key]');
  if (!button) {
    return;
  }
  const usernameKey = button.getAttribute('data-release-username-key') || '';
  button.disabled = true;
  try {
    await releaseAdminUsername(usernameKey);
  } finally {
    if (button.isConnected) {
      button.disabled = false;
    }
  }
});

elements.adminChannelsBody.addEventListener('click', async (event) => {
  const button = event.target.closest('[data-delete-channel-slug]');
  if (!button) {
    return;
  }
  const channelSlug = button.getAttribute('data-delete-channel-slug') || '';
  button.disabled = true;
  try {
    await deleteAdminChannel(channelSlug);
  } finally {
    if (button.isConnected) {
      button.disabled = false;
    }
  }
});

window.addEventListener('keydown', (event) => {
  if (!elements.adminDashboard.hidden && event.key === 'Escape') {
    closeAdminDashboard();
    return;
  }

  const key = normalizeKonamiKey(event.key);
  const expectedKey = KONAMI_SEQUENCE[state.konamiIndex];

  if (key === expectedKey) {
    state.konamiIndex += 1;
    if (state.konamiIndex === KONAMI_SEQUENCE.length) {
      state.konamiIndex = 0;
      openAdminDashboard();
    }
    return;
  }

  state.konamiIndex = key === KONAMI_SEQUENCE[0] ? 1 : 0;
});

updateMessageComposerState();
initSession();
