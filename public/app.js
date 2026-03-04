const MAX_MESSAGE_LENGTH = 2000;
const USERNAME_REGEX = /^[A-Za-z0-9_]{3,24}$/;
const KONAMI_SEQUENCE = ['ArrowUp', 'ArrowUp', 'ArrowDown', 'ArrowDown', 'ArrowLeft', 'ArrowRight', 'ArrowLeft', 'ArrowRight', 'b', 'a'];
const USER_COLOR_PALETTE = [
  '#ffd1dc', '#ffe4b5', '#fff3b0', '#d9f2c2', '#c7f0db', '#bfe8ff', '#cddafd', '#e2d5ff', '#f8d9ff', '#f6c1d0',
  '#f9e2d2', '#fdecc8', '#fef7d7', '#e7f6d5', '#dff7ea', '#d9f1ff', '#dfe7ff', '#ece2ff', '#f4e5ff', '#fbe4ef',
  '#f4f4f4', '#e3e3e3', '#d7d7d7', '#dfe8d9', '#e9f1dc', '#e6f0f7', '#e5e8f5', '#eee7f6'
];

const state = {
  token: localStorage.getItem('chat_token'),
  username: null,
  userColor: null,
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
  adminActiveTab: 'events',
  adminEvents: [],
  adminNonUsEvents: [],
  modalStack: []
};

const elements = {
  claimScreen: document.getElementById('claim-screen'),
  appScreen: document.getElementById('app-screen'),
  claimForm: document.getElementById('claim-form'),
  claimHoneypot: document.getElementById('claim-website'),
  claimHelp: document.getElementById('claim-help'),
  claimUsername: document.getElementById('claim-username'),
  claimSubmit: document.getElementById('claim-submit'),
  claimError: document.getElementById('claim-error'),
  currentUsername: document.getElementById('current-username'),
  chooseColor: document.getElementById('choose-color'),
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
  adminChannelsBody: document.getElementById('admin-channels-body'),
  appModal: document.getElementById('app-modal'),
  appModalTitle: document.getElementById('app-modal-title'),
  appModalContent: document.getElementById('app-modal-content'),
  appModalClose: document.getElementById('app-modal-close')
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
  state.userColor = null;
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

function openAppModal(title, contentHtml) {
  elements.appModalTitle.textContent = title || 'Details';
  elements.appModalContent.innerHTML = contentHtml || '';
  elements.appModal.hidden = false;
  elements.appModal.setAttribute('aria-hidden', 'false');
}

function closeAppModal() {
  elements.appModal.hidden = true;
  elements.appModal.setAttribute('aria-hidden', 'true');
  elements.appModalContent.innerHTML = '';
  state.modalStack = [];
}

function pushModalState() {
  if (elements.appModal.hidden) {
    return;
  }
  state.modalStack.push({
    title: elements.appModalTitle.textContent,
    content: elements.appModalContent.innerHTML
  });
}

function goBackModalState() {
  const previous = state.modalStack.pop();
  if (!previous) {
    return;
  }
  openAppModal(previous.title, previous.content);
}

function formatMaybe(value, fallback = '-') {
  if (value === null || value === undefined || value === '') {
    return fallback;
  }
  return String(value);
}

function formatTimeOrDash(value) {
  if (!value) {
    return '-';
  }
  return formatTime(value);
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

function colorFromUsername(username) {
  const text = String(username || '');
  if (!text) {
    return '#f8f8f8';
  }
  let hash = 0;
  for (let i = 0; i < text.length; i += 1) {
    hash = (hash * 31 + text.charCodeAt(i)) >>> 0;
  }
  return USER_COLOR_PALETTE[hash % USER_COLOR_PALETTE.length];
}

function getTextColorForBackground(color) {
  const normalized = normalizeHexColor(color);
  if (!normalized) {
    return '#111';
  }

  const r = Number.parseInt(normalized.slice(1, 3), 16);
  const g = Number.parseInt(normalized.slice(3, 5), 16);
  const b = Number.parseInt(normalized.slice(5, 7), 16);
  const luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255;

  return luminance > 0.62 ? '#111' : '#fff';
}

function normalizeKonamiKey(key) {
  if (!key) {
    return '';
  }
  return key.length === 1 ? key.toLowerCase() : key;
}

function setAdminLockedState(isLocked) {
  elements.adminDashboard.classList.toggle('admin-locked', isLocked);
  elements.adminTabs.hidden = isLocked;
  elements.adminPanels.hidden = isLocked;
  elements.adminRefresh.hidden = isLocked;
  elements.adminUnblock.hidden = isLocked;
}

function openAdminDashboard() {
  elements.adminDashboard.hidden = false;
  elements.adminDashboard.setAttribute('aria-hidden', 'false');
  showAdminError('');

  if (state.adminPassword) {
    loadAdminDashboard(state.adminPassword);
    return;
  }

  setAdminLockedState(true);
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

function buildDetailsPayload(event) {
  return {
    eventType: event.event_type || null,
    eventTime: event.event_time || null,
    method: event.method || null,
    path: event.path || null,
    statusCode: event.status_code ?? null,
    durationMs: event.duration_ms ?? null,
    username: event.username || null,
    usernameKey: event.username_key || null,
    ipAddress: event.ip_address || null,
    forwardedFor: event.forwarded_for || null,
    remoteAddress: event.remote_address || null,
    userAgent: event.user_agent || null,
    referer: event.referer || null,
    origin: event.origin || null,
    acceptLanguage: event.accept_language || null,
    secChUa: event.sec_ch_ua || null,
    secChUaMobile: event.sec_ch_ua_mobile || null,
    secChUaPlatform: event.sec_ch_ua_platform || null,
    tokenFingerprint: event.token_fingerprint || null,
    headers: event.headers_json || null,
    body: event.body_json || null,
    meta: event.meta_json || null
  };
}

function buildInspectButton(source, index) {
  return `<button type=\"button\" class=\"admin-inspect-button\" data-inspect-source=\"${escapeHtml(source)}\" data-inspect-index=\"${index}\">Inspect</button>`;
}

function openInspectModal(event) {
  state.modalStack = [];
  const details = buildDetailsPayload(event);
  const route = [event.method, event.path].filter(Boolean).join(' ');
  const summary = `
    <div class="modal-section">
      <p class="modal-text"><strong>Event:</strong> ${escapeHtml(formatMaybe(event.event_type))}</p>
      <p class="modal-text"><strong>Time:</strong> ${escapeHtml(formatTime(event.event_time))}</p>
      <p class="modal-text"><strong>User:</strong> ${escapeHtml(formatMaybe(event.username))}</p>
      <p class="modal-text"><strong>IP:</strong> ${escapeHtml(formatMaybe(event.ip_address || event.forwarded_for || event.remote_address))}</p>
      <p class="modal-text"><strong>Route:</strong> ${escapeHtml(formatMaybe(route))}</p>
    </div>
  `;
  const body = `<pre class=\"modal-json\">${escapeHtml(JSON.stringify(details, null, 2))}</pre>`;
  openAppModal('Event Inspect', `${summary}${body}`);
}

function openAboutModal() {
  state.modalStack = [];
  openAppModal(
    'About This Site',
    `
      <div class="modal-section">
        <p class="modal-text">Realtime public chat with simple username claims and live channels.</p>
      </div>
      <div class="modal-section">
        <p class="modal-text">Built for fast conversation with lightweight moderation telemetry.</p>
      </div>
    `
  );
}

async function updateUserColor(color) {
  const normalized = normalizeHexColor(color);
  if (!normalized) {
    throw new Error('Invalid color.');
  }

  const result = await api('/api/profile/color', {
    method: 'POST',
    body: JSON.stringify({ color: normalized })
  });

  const resolved = normalizeHexColor(result.color) || normalized;
  state.userColor = resolved;
  renderMessages();
  return resolved;
}

function openChooseColorModal() {
  const active = isPaletteColor(state.userColor) ? normalizeHexColor(state.userColor) : colorFromUsername(state.username);
  const swatches = USER_COLOR_PALETTE.map((color) => {
    const selected = color === active ? ' selected' : '';
    return `<button type=\"button\" class=\"color-swatch-button${selected}\" data-color-swatch=\"${color}\" title=\"${color}\" style=\"background:${color}\"></button>`;
  }).join('');

  openAppModal(
    'Choose Color',
    `
      <div class="modal-section">
        <p class="modal-text">Pick a message background color.</p>
      </div>
      <div class="modal-section">
        <div class="color-picker-grid">${swatches}</div>
      </div>
    `
  );
}

async function fetchAdminIpDetails(usernameKey) {
  const password = state.adminPassword || elements.adminPasswordInput.value;
  if (!password) {
    throw new Error('Password required.');
  }

  return fetchAdminData(`/api/admin/usernames/${encodeURIComponent(usernameKey)}/ip-details?recent=30&history=30`, password);
}

async function fetchAdminIpIntel(ipAddress) {
  const password = state.adminPassword || elements.adminPasswordInput.value;
  if (!password) {
    throw new Error('Password required.');
  }

  return fetchAdminData(`/api/admin/ip-intel?ip=${encodeURIComponent(ipAddress)}`, password);
}

function buildSingleIpIntelModal(payload) {
  const location = payload.location || null;
  const networkProfile = payload.network_profile || null;
  const proxyProfile = payload.proxy_profile || null;
  const locationText = location
    ? [location.city, location.region, location.country_code].filter(Boolean).join(', ') || '-'
    : '-';
  const coords =
    location && location.latitude !== null && location.longitude !== null
      ? `${location.latitude}, ${location.longitude}`
      : '-';

  const proxyStatus = (() => {
    if (!proxyProfile) {
      return '-';
    }
    if (proxyProfile.database_available === false) {
      return 'IP2Proxy DB not loaded';
    }
    if (proxyProfile.is_proxy === true) {
      return 'Yes';
    }
    if (proxyProfile.is_proxy === false) {
      return 'No';
    }
    return 'Unknown';
  })();

  return `
    <div class="modal-section">
      <button type="button" data-modal-back>Back</button>
    </div>

    <div class="modal-section">
      <h4>Location (Last IP)</h4>
      <p class="modal-text"><strong>Place:</strong> ${escapeHtml(locationText)}</p>
      <p class="modal-text"><strong>Timezone:</strong> ${escapeHtml(formatMaybe(location?.timezone))}</p>
      <p class="modal-text"><strong>Coordinates:</strong> ${escapeHtml(coords)}</p>
      <p class="modal-text"><strong>Postal Code:</strong> ${escapeHtml(formatMaybe(location?.postal_code))}</p>
      <p class="modal-text"><strong>ASN:</strong> ${escapeHtml(formatMaybe(location?.autonomous_system_number))}</p>
      <p class="modal-text"><strong>ASN Org:</strong> ${escapeHtml(formatMaybe(location?.autonomous_system_organization))}</p>
    </div>

    <div class="modal-section">
      <h4>Network Profile</h4>
      <p class="modal-text"><strong>Version:</strong> ${escapeHtml(formatMaybe(networkProfile?.version))}</p>
      <p class="modal-text"><strong>Range:</strong> ${escapeHtml(formatMaybe(networkProfile?.range))}</p>
      <p class="modal-text"><strong>Public Routable:</strong> ${escapeHtml(formatMaybe(networkProfile?.isPublic === null || networkProfile?.isPublic === undefined ? '-' : networkProfile.isPublic ? 'Yes' : 'No'))}</p>
    </div>

    <div class="modal-section">
      <h4>VPN / Proxy Detection</h4>
      <p class="modal-text"><strong>Detected Proxy/VPN:</strong> ${escapeHtml(proxyStatus)}</p>
      <p class="modal-text"><strong>Proxy Type:</strong> ${escapeHtml(formatMaybe(proxyProfile?.proxy_type))}</p>
      <p class="modal-text"><strong>Provider:</strong> ${escapeHtml(formatMaybe(proxyProfile?.provider))}</p>
      <p class="modal-text"><strong>Usage Type:</strong> ${escapeHtml(formatMaybe(proxyProfile?.usage_type))}</p>
      <p class="modal-text"><strong>Threat:</strong> ${escapeHtml(formatMaybe(proxyProfile?.threat))}</p>
      <p class="modal-text"><strong>Fraud Score:</strong> ${escapeHtml(formatMaybe(proxyProfile?.fraud_score))}</p>
      <p class="modal-text"><strong>ISP:</strong> ${escapeHtml(formatMaybe(proxyProfile?.isp))}</p>
      <p class="modal-text"><strong>Domain:</strong> ${escapeHtml(formatMaybe(proxyProfile?.domain))}</p>
      <p class="modal-text"><strong>Last Seen (days):</strong> ${escapeHtml(formatMaybe(proxyProfile?.last_seen_days))}</p>
    </div>
  `;
}

async function openSingleIpIntelModal(ipAddress) {
  if (!ipAddress) {
    return;
  }

  try {
    pushModalState();
    openAppModal(`Loading IP: ${ipAddress}`, '<p class="modal-text">Loading...</p>');
    const payload = await fetchAdminIpIntel(ipAddress);
    openAppModal(`IP Intelligence: ${payload.ip || ipAddress}`, buildSingleIpIntelModal(payload));
  } catch (error) {
    goBackModalState();
    showAdminError(error.message || 'Unable to load IP intelligence.');
  }
}

function buildIpDetailsModal(payload) {
  const user = payload.user || {};
  const location = payload.location || null;
  const networkProfile = payload.network_profile || null;
  const proxyProfile = payload.proxy_profile || null;
  const sources = payload.intelligence_sources || {};
  const recentVisits = Array.isArray(payload.recent_visits) ? payload.recent_visits : [];
  const ipHistory = Array.isArray(payload.ip_history) ? payload.ip_history : [];

  const locationText = location
    ? [
        location.city,
        location.region,
        location.country_code
      ]
        .filter(Boolean)
        .join(', ') || '-'
    : '-';

  const coords =
    location && location.latitude !== null && location.longitude !== null
      ? `${location.latitude}, ${location.longitude}`
      : '-';

  const recentRows = recentVisits.length
    ? recentVisits
        .map((visit) => {
          const route = [visit.method, visit.path].filter(Boolean).join(' ');
          return `
            <tr>
              <td>${escapeHtml(formatTime(visit.event_time))}</td>
              <td>${escapeHtml(formatMaybe(visit.event_type))}</td>
              <td>${escapeHtml(formatMaybe(route))}</td>
              <td>${escapeHtml(formatMaybe(visit.status_code))}</td>
              <td>${escapeHtml(formatMaybe(visit.ip_address))}</td>
              <td>${escapeHtml(formatMaybe(visit.country_code))}</td>
              <td>${escapeHtml(formatMaybe(visit.ip_range))}</td>
              <td>${escapeHtml(formatMaybe(visit.is_proxy === null ? '-' : visit.is_proxy ? 'Yes' : 'No'))}</td>
              <td>${escapeHtml(formatMaybe(visit.proxy_type))}</td>
            </tr>
          `;
        })
        .join('')
    : '<tr><td class="admin-empty" colspan="9">No recent visits.</td></tr>';

  const historyRows = ipHistory.length
    ? ipHistory
        .map((entry) => {
          return `
            <tr>
              <td>${entry.ip_address ? `<button type=\"button\" class=\"admin-ip-link\" data-ip-intel-open=\"${escapeHtml(entry.ip_address)}\">${escapeHtml(formatMaybe(entry.ip_address))}</button>` : '-'}</td>
              <td>${escapeHtml(formatMaybe(entry.country_code))}</td>
              <td>${escapeHtml(formatMaybe(entry.ip_range))}</td>
              <td>${escapeHtml(formatMaybe(entry.is_proxy === null ? '-' : entry.is_proxy ? 'Yes' : 'No'))}</td>
              <td>${escapeHtml(formatMaybe(entry.proxy_type))}</td>
              <td>${escapeHtml(formatMaybe(entry.hit_count))}</td>
              <td>${escapeHtml(formatTimeOrDash(entry.first_seen_at))}</td>
              <td>${escapeHtml(formatTimeOrDash(entry.last_seen_at))}</td>
            </tr>
          `;
        })
        .join('')
    : '<tr><td class="admin-empty" colspan="8">No IP history.</td></tr>';

  const proxyStatus = (() => {
    if (!proxyProfile) {
      return '-';
    }
    if (proxyProfile.database_available === false) {
      return 'IP2Proxy DB not loaded';
    }
    if (proxyProfile.is_proxy === true) {
      return 'Yes';
    }
    if (proxyProfile.is_proxy === false) {
      return 'No';
    }
    return 'Unknown';
  })();

  const sourcesText = [
    `request-ip: ${sources.request_ip ? 'on' : 'off'}`,
    `ipaddr.js: ${sources.ipaddr ? 'on' : 'off'}`,
    `MaxMind City DB: ${sources.maxmind_city ? 'on' : 'off'}`,
    `MaxMind ASN DB: ${sources.maxmind_asn ? 'on' : 'off'}`,
    `IP2Proxy DB: ${sources.ip2proxy ? 'on' : 'off'}`
  ].join(' | ');

  return `
    <div class="modal-section">
      <p class="modal-text"><strong>Username:</strong> ${escapeHtml(formatMaybe(user.username_original))}</p>
      <p class="modal-text"><strong>Username Key:</strong> ${escapeHtml(formatMaybe(user.username_key))}</p>
      <p class="modal-text"><strong>Last IP:</strong> ${escapeHtml(formatMaybe(payload.last_ip))}</p>
      <p class="modal-text"><strong>Last Seen:</strong> ${escapeHtml(formatTimeOrDash(payload.last_seen_at))}</p>
    </div>

    <div class="modal-section">
      <h4>Location (Last IP)</h4>
      <p class="modal-text"><strong>Place:</strong> ${escapeHtml(locationText)}</p>
      <p class="modal-text"><strong>Timezone:</strong> ${escapeHtml(formatMaybe(location?.timezone))}</p>
      <p class="modal-text"><strong>Coordinates:</strong> ${escapeHtml(coords)}</p>
      <p class="modal-text"><strong>Postal Code:</strong> ${escapeHtml(formatMaybe(location?.postal_code))}</p>
      <p class="modal-text"><strong>ASN:</strong> ${escapeHtml(formatMaybe(location?.autonomous_system_number))}</p>
      <p class="modal-text"><strong>ASN Org:</strong> ${escapeHtml(formatMaybe(location?.autonomous_system_organization))}</p>
    </div>

    <div class="modal-section">
      <h4>Network Profile</h4>
      <p class="modal-text"><strong>Version:</strong> ${escapeHtml(formatMaybe(networkProfile?.version))}</p>
      <p class="modal-text"><strong>Range:</strong> ${escapeHtml(formatMaybe(networkProfile?.range))}</p>
      <p class="modal-text"><strong>Public Routable:</strong> ${escapeHtml(formatMaybe(networkProfile?.isPublic === null || networkProfile?.isPublic === undefined ? '-' : networkProfile.isPublic ? 'Yes' : 'No'))}</p>
    </div>

    <div class="modal-section">
      <h4>VPN / Proxy Detection</h4>
      <p class="modal-text"><strong>Detected Proxy/VPN:</strong> ${escapeHtml(proxyStatus)}</p>
      <p class="modal-text"><strong>Proxy Type:</strong> ${escapeHtml(formatMaybe(proxyProfile?.proxy_type))}</p>
      <p class="modal-text"><strong>Provider:</strong> ${escapeHtml(formatMaybe(proxyProfile?.provider))}</p>
      <p class="modal-text"><strong>Usage Type:</strong> ${escapeHtml(formatMaybe(proxyProfile?.usage_type))}</p>
      <p class="modal-text"><strong>Threat:</strong> ${escapeHtml(formatMaybe(proxyProfile?.threat))}</p>
      <p class="modal-text"><strong>Fraud Score:</strong> ${escapeHtml(formatMaybe(proxyProfile?.fraud_score))}</p>
      <p class="modal-text"><strong>ISP:</strong> ${escapeHtml(formatMaybe(proxyProfile?.isp))}</p>
      <p class="modal-text"><strong>Domain:</strong> ${escapeHtml(formatMaybe(proxyProfile?.domain))}</p>
      <p class="modal-text"><strong>Last Seen (days):</strong> ${escapeHtml(formatMaybe(proxyProfile?.last_seen_days))}</p>
    </div>

    <div class="modal-section">
      <h4>Recent Visits</h4>
      <div class="modal-table-wrap">
        <table class="modal-table">
          <thead>
            <tr>
              <th>Time</th>
              <th>Event</th>
              <th>Route</th>
              <th>Status</th>
              <th>IP</th>
              <th>Country</th>
              <th>Range</th>
              <th>Proxy</th>
              <th>Type</th>
            </tr>
          </thead>
          <tbody>${recentRows}</tbody>
        </table>
      </div>
    </div>

    <div class="modal-section">
      <h4>IP History</h4>
      <div class="modal-table-wrap">
        <table class="modal-table">
          <thead>
            <tr>
              <th>IP</th>
              <th>Country</th>
              <th>Range</th>
              <th>Proxy</th>
              <th>Type</th>
              <th>Visits</th>
              <th>First Seen</th>
              <th>Last Seen</th>
            </tr>
          </thead>
          <tbody>${historyRows}</tbody>
        </table>
      </div>
    </div>

    <div class="modal-section">
      <p class="modal-text"><strong>Intelligence Sources:</strong> ${escapeHtml(sourcesText)}</p>
    </div>
  `;
}

async function openUserIpDetailsModal(usernameKey) {
  if (!usernameKey) {
    showAdminError('Invalid username key.');
    return;
  }

  try {
    showAdminError('');
    state.modalStack = [];
    openAppModal('Loading IP Details...', '<p class="modal-text">Loading...</p>');
    const payload = await fetchAdminIpDetails(usernameKey);
    openAppModal(`IP Details: ${payload?.user?.username_original || usernameKey}`, buildIpDetailsModal(payload));
  } catch (error) {
    closeAppModal();
    showAdminError(error.message || 'Unable to load IP details.');
  }
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
  state.adminEvents = Array.isArray(events) ? events : [];
  elements.adminEventsBody.innerHTML = '';

  if (!state.adminEvents.length) {
    const row = document.createElement('tr');
    row.innerHTML = '<td class=\"admin-empty\" colspan=\"8\">No events yet.</td>';
    elements.adminEventsBody.appendChild(row);
    return;
  }

  for (let index = 0; index < state.adminEvents.length; index += 1) {
    const event = state.adminEvents[index];
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
      <td>${buildInspectButton('events', index)}</td>
    `;

    elements.adminEventsBody.appendChild(row);
  }
}

function renderAdminUsernames(usernames) {
  elements.adminUsernamesBody.innerHTML = '';

  if (!usernames.length) {
    const row = document.createElement('tr');
    row.innerHTML = '<td class=\"admin-empty\" colspan=\"7\">No active usernames.</td>';
    elements.adminUsernamesBody.appendChild(row);
    return;
  }

  for (const username of usernames) {
    const row = document.createElement('tr');
    const actionDisabled = !username.username_key ? 'disabled' : '';
    const hasIp = Boolean(username.last_ip);
    const isOnline = Boolean(username.is_online);
    const statusClass = isOnline ? 'is-online' : 'is-offline';
    const statusLabel = isOnline ? 'Online' : 'Offline';
    const ipCell = hasIp
      ? `<button type=\"button\" class=\"admin-ip-link\" data-ip-username-key=\"${escapeHtml(username.username_key || '')}\" title=\"Open IP details\">${escapeHtml(truncateText(username.last_ip, 42))}</button>`
      : '-';

    row.innerHTML = `
      <td><span class=\"user-status-wrap\"><span class=\"status-dot ${statusClass}\" aria-hidden=\"true\"></span><span class=\"sr-only\">${statusLabel}</span>${escapeHtml(username.username_original || '-')}</span></td>
      <td>${escapeHtml(username.username_key || '-')}</td>
      <td>${escapeHtml(formatTime(username.claimed_at))}</td>
      <td>${username.has_session ? 'Yes' : 'No'}</td>
      <td>${username.session_created_at ? escapeHtml(formatTime(username.session_created_at)) : '-'}</td>
      <td title=\"${escapeHtml(username.last_ip || '')}\">${ipCell}</td>
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
  state.adminNonUsEvents = Array.isArray(events) ? events : [];
  elements.adminNonUsBody.innerHTML = '';

  if (!state.adminNonUsEvents.length) {
    const row = document.createElement('tr');
    row.innerHTML = '<td class=\"admin-empty\" colspan=\"8\">No non-USA IP events.</td>';
    elements.adminNonUsBody.appendChild(row);
    return;
  }

  for (let index = 0; index < state.adminNonUsEvents.length; index += 1) {
    const event = state.adminNonUsEvents[index];
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
      <td>${buildInspectButton('non-us', index)}</td>
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
    setAdminLockedState(false);
    renderAdminEvents(eventsPayload.events || []);
    renderAdminUsernames(usernamesPayload.usernames || []);
    renderAdminNonUsEvents(nonUsPayload.events || []);
    renderAdminChannels(channelsPayload.channels || []);
    setAdminTab(state.adminActiveTab);
  } catch (error) {
    state.adminPassword = '';
    setAdminLockedState(true);
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

    const messageColor = isPaletteColor(msg.color) ? normalizeHexColor(msg.color) : colorFromUsername(msg.username);
    row.style.backgroundColor = messageColor;
    row.style.color = getTextColorForBackground(messageColor);

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

function upsertChannelInState(channelInput) {
  if (!channelInput || !channelInput.slug) {
    return null;
  }

  const existing = state.channelMap.get(channelInput.slug) || {};
  const normalized = {
    ...existing,
    ...channelInput,
    slug: channelInput.slug
  };

  const index = state.channels.findIndex((channel) => channel.slug === normalized.slug);
  if (index >= 0) {
    state.channels[index] = normalized;
  } else {
    state.channels.push(normalized);
  }

  state.channels = state.channels.filter((channel, channelIndex, all) => {
    if (!channel?.slug) {
      return false;
    }
    return all.findIndex((entry) => entry.slug === channel.slug) === channelIndex;
  });

  state.channelMap.set(normalized.slug, normalized);

  if (normalized.onlineCount !== undefined && normalized.onlineCount !== null) {
    state.onlineCounts[normalized.slug] = Number(normalized.onlineCount) || 0;
  } else if (!(normalized.slug in state.onlineCounts)) {
    state.onlineCounts[normalized.slug] = 0;
  }

  return normalized;
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
      upsertChannelInState(payload.channel);
      renderChannels();
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
  state.channels = [];
  state.channelMap = new Map();

  for (const channel of result.channels || []) {
    upsertChannelInState(channel);
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
    state.userColor = normalizeHexColor(session.color) || colorFromUsername(session.username);
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
    state.userColor = normalizeHexColor(result.color) || colorFromUsername(result.username);
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

elements.chooseColor.addEventListener('click', () => {
  openChooseColorModal();
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
    upsertChannelInState(channel);

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

elements.claimHelp.addEventListener('click', () => {
  openAboutModal();
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

elements.adminEventsBody.addEventListener('click', (event) => {
  const button = event.target.closest('[data-inspect-source][data-inspect-index]');
  if (!button) {
    return;
  }

  const source = button.getAttribute('data-inspect-source') || '';
  const index = Number(button.getAttribute('data-inspect-index'));
  const collection = source === 'non-us' ? state.adminNonUsEvents : state.adminEvents;
  const eventRow = Number.isFinite(index) ? collection[index] : null;

  if (!eventRow) {
    showAdminError('Unable to inspect event.');
    return;
  }

  openInspectModal(eventRow);
});

elements.adminNonUsBody.addEventListener('click', (event) => {
  const button = event.target.closest('[data-inspect-source][data-inspect-index]');
  if (!button) {
    return;
  }

  const source = button.getAttribute('data-inspect-source') || '';
  const index = Number(button.getAttribute('data-inspect-index'));
  const collection = source === 'non-us' ? state.adminNonUsEvents : state.adminEvents;
  const eventRow = Number.isFinite(index) ? collection[index] : null;

  if (!eventRow) {
    showAdminError('Unable to inspect event.');
    return;
  }

  openInspectModal(eventRow);
});

elements.adminUsernamesBody.addEventListener('click', async (event) => {
  const ipButton = event.target.closest('[data-ip-username-key]');
  if (ipButton) {
    const usernameKey = ipButton.getAttribute('data-ip-username-key') || '';
    ipButton.disabled = true;
    try {
      await openUserIpDetailsModal(usernameKey);
    } finally {
      if (ipButton.isConnected) {
        ipButton.disabled = false;
      }
    }
    return;
  }

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

elements.appModalClose.addEventListener('click', () => {
  closeAppModal();
});

elements.appModal.addEventListener('click', (event) => {
  if (event.target === elements.appModal) {
    closeAppModal();
  }
});

elements.appModalContent.addEventListener('click', async (event) => {
  const backButton = event.target.closest('[data-modal-back]');
  if (backButton) {
    goBackModalState();
    return;
  }

  const swatch = event.target.closest('[data-color-swatch]');
  if (swatch) {
    const color = swatch.getAttribute('data-color-swatch') || '';
    swatch.disabled = true;
    try {
      await updateUserColor(color);
      openChooseColorModal();
    } catch (error) {
      showAdminError(error.message || 'Unable to update color.');
    } finally {
      if (swatch.isConnected) {
        swatch.disabled = false;
      }
    }
    return;
  }

  const button = event.target.closest('[data-ip-intel-open]');
  if (!button) {
    return;
  }

  const ipAddress = button.getAttribute('data-ip-intel-open') || '';
  if (!ipAddress) {
    return;
  }

  button.disabled = true;
  try {
    await openSingleIpIntelModal(ipAddress);
  } finally {
    if (button.isConnected) {
      button.disabled = false;
    }
  }
});

window.addEventListener('keydown', (event) => {
  if (!elements.appModal.hidden && event.key === 'Escape') {
    closeAppModal();
    return;
  }

  if (!elements.adminDashboard.hidden && event.key === 'Escape') {
    closeAdminDashboard();
    return;
  }

  const key = normalizeKonamiKey(event.key);
  const expectedKey = KONAMI_SEQUENCE[state.konamiIndex];

  if (key === expectedKey) {
    event.preventDefault();
    state.konamiIndex += 1;
    if (state.konamiIndex === KONAMI_SEQUENCE.length) {
      state.konamiIndex = 0;
      elements.messageInput.value = '';
      updateMessageComposerState();
      elements.adminPasswordInput.value = '';
      openAdminDashboard();
    }
    return;
  }

  state.konamiIndex = key === KONAMI_SEQUENCE[0] ? 1 : 0;
});

updateMessageComposerState();
initSession();
