const CSRF_META_NAME = "csrf-token";
const REFRESH_CSRF_META_NAME = "lt-refresh-csrf-token";
const REFRESH_AFTER_META_NAME = "lt-session-refresh-after";
const ACCESS_EXPIRES_AT_META_NAME = "lt-session-access-expires-at";
const SERVER_NOW_META_NAME = "lt-session-server-now";
const CSRF_HEADER_NAME = "X-CSRF-Token";
const REFRESH_URL = "/api/v1/auth/refresh";
const ACCESS_PROBE_URL = "/api/v1/auth/me";
const SESSION_CLOCK_URL = "/health";
const REFRESH_LOCK_NAME = "litoral-trace-session-refresh";
const REFRESH_CHANNEL_NAME = "litoral-trace-session-refresh-events";
const SAFE_METHODS = new Set(["GET", "HEAD", "OPTIONS", "TRACE"]);

function readMeta(name) {
  return (
    document.querySelector(`meta[name="${name}"]`)
      ?.getAttribute("content")
      ?.trim()
    || ""
  );
}

function updateMeta(name, value) {
  const meta = document.querySelector(`meta[name="${name}"]`);
  if (meta && value) {
    meta.setAttribute("content", value);
  }
}

function readEpochMilliseconds(name) {
  const raw = Number.parseInt(readMeta(name), 10);
  if (!Number.isFinite(raw) || raw <= 0) {
    return null;
  }
  return raw * 1000;
}

function monotonicEpochMilliseconds() {
  return performance.timeOrigin + performance.now();
}

function updateRenderedCsrfToken(nextToken) {
  if (!nextToken) {
    return;
  }

  updateMeta(CSRF_META_NAME, nextToken);
  document.querySelectorAll('input[name="csrf_token"]').forEach((input) => {
    input.value = nextToken;
  });
}

function extractSecurityMeta(htmlText) {
  const parsed = new DOMParser().parseFromString(htmlText, "text/html");
  return {
    csrfToken: parsed.querySelector(`meta[name="${CSRF_META_NAME}"]`)
      ?.getAttribute("content")
      ?.trim() || "",
    refreshCsrfToken: parsed.querySelector(`meta[name="${REFRESH_CSRF_META_NAME}"]`)
      ?.getAttribute("content")
      ?.trim() || "",
    accessExpiresAt: parsed.querySelector(`meta[name="${ACCESS_EXPIRES_AT_META_NAME}"]`)
      ?.getAttribute("content")
      ?.trim() || "",
    serverNow: parsed.querySelector(`meta[name="${SERVER_NOW_META_NAME}"]`)
      ?.getAttribute("content")
      ?.trim() || "",
  };
}

function showSessionWarning(message = "La sesión necesita reautenticación.") {
  if (document.querySelector("[data-session-renewal-warning]")) {
    return;
  }

  const warning = document.createElement("div");
  warning.dataset.sessionRenewalWarning = "true";
  warning.setAttribute("role", "alert");
  warning.style.cssText = [
    "position:fixed",
    "left:1rem",
    "right:1rem",
    "top:1rem",
    "z-index:100",
    "max-width:42rem",
    "margin:0 auto",
    "border:1px solid #fcd34d",
    "border-radius:0.75rem",
    "background:#fffbeb",
    "padding:1rem",
    "font-size:0.875rem",
    "line-height:1.5",
    "color:#451a03",
    "box-shadow:0 10px 15px -3px rgb(0 0 0 / 0.1)",
  ].join(";");
  warning.innerHTML = `<strong>${message}</strong> Para proteger los datos, Litoral Trace no continuó la renovación automática. Abrí nuevamente la aplicación antes de seguir.`;
  document.body.appendChild(warning);
}

function installLiveCsrfFetchBridge() {
  if (
    typeof window.fetch !== "function"
    || window.fetch.__litoralTraceLiveCsrf
  ) {
    return;
  }

  const downstreamFetch = window.fetch.bind(window);

  const liveCsrfFetch = (input, init = {}) => {
    const method = String(
      init.method || (input instanceof Request ? input.method : "GET"),
    ).toUpperCase();

    if (SAFE_METHODS.has(method)) {
      return downstreamFetch(input, init);
    }

    const rawUrl = input instanceof Request ? input.url : String(input);
    const requestUrl = new URL(rawUrl, window.location.href);

    if (requestUrl.origin !== window.location.origin) {
      return downstreamFetch(input, init);
    }

    // /refresh uses its dedicated browser-bound token. Every other unsafe API
    // request uses the current session-bound token from the meta tag, even when
    // legacy page code supplied a token that was cached before a rotation.
    if (requestUrl.pathname === REFRESH_URL) {
      return downstreamFetch(input, init);
    }

    const token = readMeta(CSRF_META_NAME);
    if (!token) {
      return downstreamFetch(input, init);
    }

    const headers = new Headers(input instanceof Request ? input.headers : undefined);
    const initHeaders = new Headers(init.headers || undefined);
    initHeaders.forEach((value, key) => headers.set(key, value));
    headers.set(CSRF_HEADER_NAME, token);

    return downstreamFetch(input, {...init, headers});
  };

  Object.defineProperty(liveCsrfFetch, "__litoralTraceLiveCsrf", {
    value: true,
    enumerable: false,
  });
  window.fetch = liveCsrfFetch;
}

async function synchronizeSecurityMeta() {
  try {
    const pageResponse = await window.fetch(
      window.location.href,
      {
        method: "GET",
        credentials: "same-origin",
        cache: "no-store",
        headers: {"Accept": "text/html"},
      },
    );

    if (!pageResponse.ok) {
      return false;
    }

    const securityMeta = extractSecurityMeta(await pageResponse.text());
    if (
      !securityMeta.csrfToken
      || !securityMeta.refreshCsrfToken
      || !securityMeta.accessExpiresAt
      || !securityMeta.serverNow
    ) {
      return false;
    }

    updateRenderedCsrfToken(securityMeta.csrfToken);
    updateMeta(REFRESH_CSRF_META_NAME, securityMeta.refreshCsrfToken);
    updateMeta(ACCESS_EXPIRES_AT_META_NAME, securityMeta.accessExpiresAt);
    updateMeta(SERVER_NOW_META_NAME, securityMeta.serverNow);
    return true;
  } catch (_error) {
    return false;
  }
}

async function refreshServerClock() {
  try {
    const response = await window.fetch(
      SESSION_CLOCK_URL,
      {
        method: "GET",
        credentials: "same-origin",
        cache: "no-store",
        headers: {"Accept": "application/json"},
      },
    );

    if (!response.ok) {
      return false;
    }

    const serverDate = response.headers.get("Date");
    const serverNowMs = Date.parse(serverDate || "");
    if (!Number.isFinite(serverNowMs) || serverNowMs <= 0) {
      return false;
    }

    updateMeta(SERVER_NOW_META_NAME, String(Math.floor(serverNowMs / 1000)));
    return true;
  } catch (_error) {
    return false;
  }
}

async function accessSessionIsUsable() {
  try {
    const response = await window.fetch(
      ACCESS_PROBE_URL,
      {
        method: "GET",
        credentials: "same-origin",
        cache: "no-store",
        headers: {"Accept": "application/json"},
      },
    );
    return response.ok;
  } catch (_error) {
    return false;
  }
}

async function rotateSession() {
  const token = readMeta(REFRESH_CSRF_META_NAME);
  if (!token) {
    return {rotated: false, synchronized: false};
  }

  let response;
  try {
    response = await window.fetch(
      REFRESH_URL,
      {
        method: "POST",
        credentials: "same-origin",
        cache: "no-store",
        keepalive: true,
        headers: {
          "Accept": "application/json",
          "Content-Type": "application/json",
          [CSRF_HEADER_NAME]: token,
        },
        body: "{}",
      },
    );
  } catch (_error) {
    return {rotated: false, synchronized: false};
  }

  if (!response.ok) {
    if (response.status === 401 || response.status === 403) {
      showSessionWarning();
    }
    return {rotated: false, synchronized: false};
  }

  // keepalive lets the browser finish the rotation and apply Set-Cookie even if
  // a navigation starts after the POST reached the server. When this document
  // survives, rehydrate its session-bound CSRF and timing metadata in place.
  const synchronized = await synchronizeSecurityMeta();
  if (!synchronized) {
    showSessionWarning("La sesión se renovó, pero la página no pudo sincronizar su protección CSRF.");
  }
  return {rotated: true, synchronized};
}

function installSessionRenewal() {
  const refreshCsrfToken = readMeta(REFRESH_CSRF_META_NAME);
  const rawInterval = Number.parseInt(readMeta(REFRESH_AFTER_META_NAME), 10);
  const initialExpiryMs = readEpochMilliseconds(ACCESS_EXPIRES_AT_META_NAME);
  const initialServerNowMs = readEpochMilliseconds(SERVER_NOW_META_NAME);

  if (
    !refreshCsrfToken
    || !Number.isFinite(rawInterval)
    || rawInterval < 15
    || initialExpiryMs === null
    || initialServerNowMs === null
  ) {
    return;
  }

  // Correct rotation requires both an origin-scoped exclusive lock and an
  // ephemeral cross-tab notification channel. Without either primitive we fail
  // closed: the server-side session simply expires normally and the user can
  // authenticate again. No credential or coordination state is persisted in
  // localStorage/sessionStorage.
  if (!navigator.locks?.request || typeof window.BroadcastChannel !== "function") {
    return;
  }

  installLiveCsrfFetchBridge();

  const intervalMs = rawInterval * 1000;
  const retryDelayMs = Math.max(5000, Math.min(30000, Math.floor(intervalMs / 4)));
  const duplicateSuppressionMs = Math.max(5000, Math.min(30000, Math.floor(intervalMs / 3)));
  const channel = new window.BroadcastChannel(REFRESH_CHANNEL_NAME);
  let renewalTimer = null;
  let sharedRefreshAt = 0;
  let inFlight = null;
  let peerSyncInFlight = null;
  let foregroundProbeInFlight = null;
  let foregroundDueProbe = false;

  function serverRelativeRefreshDelayMs() {
    const expiryMs = readEpochMilliseconds(ACCESS_EXPIRES_AT_META_NAME);
    const serverNowMs = readEpochMilliseconds(SERVER_NOW_META_NAME);
    if (expiryMs === null || serverNowMs === null) {
      return null;
    }

    // Both values come from server-controlled responses. The workstation wall
    // clock never participates in expiry math.
    return Math.max(0, expiryMs - serverNowMs - intervalMs);
  }

  function rememberSharedRefresh(value) {
    const timestamp = Number(value);
    if (!Number.isFinite(timestamp) || timestamp <= 0) {
      return false;
    }
    sharedRefreshAt = Math.max(sharedRefreshAt, timestamp);
    return true;
  }

  function announceRefresh(timestamp) {
    rememberSharedRefresh(timestamp);
    channel.postMessage({type: "refreshed", at: timestamp});
  }

  function armRenewalTimer(delay) {
    if (renewalTimer !== null) {
      window.clearTimeout(renewalTimer);
    }

    renewalTimer = window.setTimeout(async () => {
      renewalTimer = null;
      const synchronized = await renew();
      scheduleRenewal({retry: !synchronized});
    }, delay);
  }

  function scheduleRenewal({retry = false} = {}) {
    const delay = retry ? retryDelayMs : serverRelativeRefreshDelayMs();
    if (delay === null) {
      showSessionWarning("No fue posible verificar cuándo vence la sesión actual.");
      return;
    }
    armRenewalTimer(delay);
  }

  async function synchronizeFromPeer(timestamp) {
    if (!rememberSharedRefresh(timestamp)) {
      return false;
    }

    if (peerSyncInFlight) {
      return peerSyncInFlight;
    }

    peerSyncInFlight = synchronizeSecurityMeta();
    try {
      const synchronized = await peerSyncInFlight;
      if (!synchronized) {
        showSessionWarning("Otra pestaña renovó la sesión, pero esta página no pudo sincronizar su protección CSRF.");
      } else {
        scheduleRenewal();
      }
      return synchronized;
    } finally {
      peerSyncInFlight = null;
    }
  }

  channel.addEventListener("message", (event) => {
    const payload = event.data;
    if (!payload || typeof payload !== "object") {
      return;
    }

    if (payload.type === "refreshed") {
      void synchronizeFromPeer(payload.at);
      return;
    }

    if (payload.type === "state-request" && sharedRefreshAt > 0) {
      channel.postMessage({type: "refreshed", at: sharedRefreshAt});
    }
  });

  // A newly opened tab can ask existing tabs for their latest in-memory
  // coordination timestamp. Only a monotonic timestamp crosses the channel—
  // never access, refresh or CSRF tokens, tenant IDs, usernames or business data.
  channel.postMessage({type: "state-request"});

  async function renewInsideCrossTabLock() {
    const now = monotonicEpochMilliseconds();

    // A peer may have rotated the shared HttpOnly cookies while this tab waited
    // for the Web Lock. If so, never perform a second immediate rotation: just
    // rehydrate this tab's session-bound CSRF and timing metadata.
    if (sharedRefreshAt && now - sharedRefreshAt < duplicateSuppressionMs) {
      foregroundDueProbe = false;
      return synchronizeSecurityMeta();
    }

    // After waking from suspension, multiple tabs can decide that the old access
    // token is due at the same time. Once the lock is acquired, probe the shared
    // cookie jar before rotating. If another tab already refreshed, /me succeeds
    // and the new server metadata moves the deadline forward; if the old access
    // token is still due/expired, continue with exactly one refresh rotation.
    if (foregroundDueProbe && await accessSessionIsUsable()) {
      const synchronized = await synchronizeSecurityMeta();
      if (synchronized) {
        const refreshedDelay = serverRelativeRefreshDelayMs();
        if (refreshedDelay !== null && refreshedDelay > 0) {
          foregroundDueProbe = false;
          return true;
        }
      }
    }

    foregroundDueProbe = false;
    const result = await rotateSession();
    if (result.rotated) {
      const completedAt = monotonicEpochMilliseconds();
      // Broadcast immediately after the server rotated the HttpOnly cookies so
      // every peer can refresh its session-bound CSRF token before its next POST.
      announceRefresh(completedAt);
    }
    return result.synchronized;
  }

  async function renew() {
    if (inFlight) {
      return inFlight;
    }

    inFlight = navigator.locks.request(
      REFRESH_LOCK_NAME,
      {mode: "exclusive"},
      renewInsideCrossTabLock,
    );

    try {
      return await inFlight;
    } finally {
      inFlight = null;
    }
  }

  async function revalidateAfterForeground() {
    if (foregroundProbeInFlight) {
      return foregroundProbeInFlight;
    }

    foregroundProbeInFlight = (async () => {
      const clockRefreshed = await refreshServerClock();
      if (!clockRefreshed) {
        // If the public server clock cannot be obtained, renewing directly is
        // safer than trusting a monotonic clock that may have paused in sleep.
        foregroundDueProbe = true;
        sharedRefreshAt = 0;
        const synchronized = await renew();
        scheduleRenewal({retry: !synchronized});
        return synchronized;
      }

      const delay = serverRelativeRefreshDelayMs();
      if (delay === null) {
        showSessionWarning("No fue posible verificar cuándo vence la sesión actual.");
        return false;
      }

      if (delay > 0) {
        armRenewalTimer(delay);
        return true;
      }

      if (renewalTimer !== null) {
        window.clearTimeout(renewalTimer);
        renewalTimer = null;
      }

      // The fresh server clock proves the old access token reached its renewal
      // window. Reset any pre-suspend duplicate marker; the Web Lock plus /me
      // probe above still prevents two resumed tabs from rotating sequentially.
      foregroundDueProbe = true;
      sharedRefreshAt = 0;
      const synchronized = await renew();
      scheduleRenewal({retry: !synchronized});
      return synchronized;
    })();

    try {
      return await foregroundProbeInFlight;
    } finally {
      foregroundProbeInFlight = null;
    }
  }

  // Initial delay uses verified JWT expiry minus server render time. Normal page
  // navigation cannot postpone it and workstation clock skew cannot move it.
  scheduleRenewal();

  // performance.now() may pause while a laptop sleeps. On every return to the
  // foreground, obtain a fresh server Date header before deciding whether the
  // current access token is still inside its safe renewal window.
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      void revalidateAfterForeground();
    }
  });
  window.addEventListener("focus", () => {
    void revalidateAfterForeground();
  });
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", installSessionRenewal, {once: true});
} else {
  installSessionRenewal();
}
