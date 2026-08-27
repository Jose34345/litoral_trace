const CSRF_META_NAME = "csrf-token";
const REFRESH_CSRF_META_NAME = "lt-refresh-csrf-token";
const REFRESH_AFTER_META_NAME = "lt-session-refresh-after";
const ACCESS_EXPIRES_AT_META_NAME = "lt-session-access-expires-at";
const CSRF_HEADER_NAME = "X-CSRF-Token";
const REFRESH_URL = "/api/v1/auth/refresh";
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
    ) {
      return false;
    }

    updateRenderedCsrfToken(securityMeta.csrfToken);
    updateMeta(REFRESH_CSRF_META_NAME, securityMeta.refreshCsrfToken);
    updateMeta(ACCESS_EXPIRES_AT_META_NAME, securityMeta.accessExpiresAt);
    return true;
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

  // Set-Cookie has been applied when fetch resolves. Rehydrate this tab's
  // session-bound CSRF material and the new absolute access expiry without
  // reloading the page or losing inputs.
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

  if (
    !refreshCsrfToken
    || !Number.isFinite(rawInterval)
    || rawInterval < 15
    || initialExpiryMs === null
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

  function accessRefreshDueAtMs() {
    const expiryMs = readEpochMilliseconds(ACCESS_EXPIRES_AT_META_NAME);
    if (expiryMs === null) {
      return null;
    }
    return expiryMs - intervalMs;
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

  function scheduleRenewal({retry = false} = {}) {
    if (renewalTimer !== null) {
      window.clearTimeout(renewalTimer);
      renewalTimer = null;
    }

    const dueAt = accessRefreshDueAtMs();
    if (dueAt === null) {
      showSessionWarning("No fue posible verificar cuándo vence la sesión actual.");
      return;
    }

    const minimumDelay = retry ? retryDelayMs : 0;
    const delay = Math.max(minimumDelay, dueAt - Date.now(), 0);

    renewalTimer = window.setTimeout(async () => {
      renewalTimer = null;
      const synchronized = await renew();
      scheduleRenewal({retry: !synchronized});
    }, delay);
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
  // coordination timestamp. Only a timestamp crosses the channel—never access,
  // refresh or CSRF tokens, tenant IDs, usernames or business data.
  channel.postMessage({type: "state-request"});

  async function renewInsideCrossTabLock() {
    const now = Date.now();

    // A peer may have rotated the shared HttpOnly cookies while this tab waited
    // for the Web Lock. If so, never perform a second immediate rotation: just
    // rehydrate this tab's session-bound CSRF and expiry metadata.
    if (sharedRefreshAt && now - sharedRefreshAt < duplicateSuppressionMs) {
      return synchronizeSecurityMeta();
    }

    const result = await rotateSession();
    if (result.rotated) {
      const completedAt = Date.now();
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

  function renewIfDue() {
    const dueAt = accessRefreshDueAtMs();
    if (dueAt === null) {
      showSessionWarning("No fue posible verificar cuándo vence la sesión actual.");
      return;
    }

    if (Date.now() >= dueAt) {
      void renew().then((synchronized) => {
        scheduleRenewal({retry: !synchronized});
      });
      return;
    }

    scheduleRenewal();
  }

  // The deadline is absolute and comes from the verified JWT expiry. A normal
  // full-page navigation therefore cannot postpone renewal by starting a fresh
  // interval from page-load time.
  scheduleRenewal();

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      renewIfDue();
    }
  });
  window.addEventListener("focus", renewIfDue);
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", installSessionRenewal, {once: true});
} else {
  installSessionRenewal();
}
