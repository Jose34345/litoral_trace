const CSRF_META_NAME = "csrf-token";
const REFRESH_CSRF_META_NAME = "lt-refresh-csrf-token";
const REFRESH_AFTER_META_NAME = "lt-session-refresh-after";
const CSRF_HEADER_NAME = "X-CSRF-Token";
const REFRESH_URL = "/api/v1/auth/refresh";
const REFRESH_LOCK_NAME = "litoral-trace-session-refresh";
const LAST_REFRESH_KEY = "litoral-trace:last-session-refresh-at";
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
  };
}

function readSharedRefreshAt() {
  try {
    const value = Number.parseInt(window.localStorage.getItem(LAST_REFRESH_KEY) || "0", 10);
    return Number.isFinite(value) ? value : 0;
  } catch (_error) {
    return 0;
  }
}

function writeSharedRefreshAt(value) {
  try {
    // This is coordination metadata only. No access token, refresh token, CSRF
    // token, tenant id or other credential is ever persisted in Web Storage.
    window.localStorage.setItem(LAST_REFRESH_KEY, String(value));
  } catch (_error) {
    // Storage may be disabled. Web Locks still serialize rotations safely.
  }
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

    // /refresh uses the separate browser-bound token. Every other unsafe API
    // request must use the current subject/session-bound token from the meta tag,
    // even if legacy page code cached and supplied an older explicit header.
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
    if (!securityMeta.csrfToken || !securityMeta.refreshCsrfToken) {
      return false;
    }

    updateRenderedCsrfToken(securityMeta.csrfToken);
    updateMeta(REFRESH_CSRF_META_NAME, securityMeta.refreshCsrfToken);
    return true;
  } catch (_error) {
    return false;
  }
}

async function rotateSession() {
  const token = readMeta(REFRESH_CSRF_META_NAME);
  if (!token) {
    return false;
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
    return false;
  }

  if (!response.ok) {
    if (response.status === 401 || response.status === 403) {
      showSessionWarning();
    }
    return false;
  }

  // Refresh-token rotation changes the session id. Pull only the security meta
  // from a fresh GET and update the existing DOM in place so form inputs and
  // scroll position are preserved.
  const synchronized = await synchronizeSecurityMeta();
  if (!synchronized) {
    showSessionWarning("La sesión se renovó, pero la página no pudo sincronizar su protección CSRF.");
  }
  return synchronized;
}

function installSessionRenewal() {
  const refreshCsrfToken = readMeta(REFRESH_CSRF_META_NAME);
  const rawInterval = Number.parseInt(readMeta(REFRESH_AFTER_META_NAME), 10);

  if (!refreshCsrfToken || !Number.isFinite(rawInterval) || rawInterval < 15) {
    return;
  }

  installLiveCsrfFetchBridge();

  const intervalMs = rawInterval * 1000;
  const duplicateSuppressionMs = Math.max(5000, Math.min(30000, Math.floor(intervalMs / 3)));
  let lastRenewalAt = Date.now();
  let inFlight = null;

  const renewInsideCrossTabLock = async () => {
    const now = Date.now();
    const sharedRefreshAt = readSharedRefreshAt();

    // Another tab may have just rotated the shared cookies while this tab was
    // waiting for the Web Lock. Do not rotate again; only rehydrate this tab's
    // session-bound CSRF token from the newly authenticated GET response.
    if (sharedRefreshAt && now - sharedRefreshAt < duplicateSuppressionMs) {
      const synchronized = await synchronizeSecurityMeta();
      if (synchronized) {
        lastRenewalAt = Date.now();
      }
      return synchronized;
    }

    const rotated = await rotateSession();
    if (rotated) {
      const completedAt = Date.now();
      lastRenewalAt = completedAt;
      writeSharedRefreshAt(completedAt);
    }
    return rotated;
  };

  const renew = async () => {
    if (inFlight) {
      return inFlight;
    }

    inFlight = (async () => {
      // Rotation-based reuse detection intentionally revokes a session family
      // when the same refresh token is used twice. Therefore automatic refresh
      // must be serialized across browser tabs. Web Locks provides an origin-
      // scoped exclusive lock without storing credentials. On browsers without
      // Web Locks we fail closed and leave the existing server expiry behavior.
      if (!navigator.locks?.request) {
        return false;
      }

      return navigator.locks.request(
        REFRESH_LOCK_NAME,
        {mode: "exclusive"},
        renewInsideCrossTabLock,
      );
    })();

    try {
      return await inFlight;
    } finally {
      inFlight = null;
    }
  };

  window.setInterval(() => {
    void renew();
  }, intervalMs);

  const renewIfOverdue = () => {
    if (Date.now() - lastRenewalAt >= intervalMs) {
      void renew();
    }
  };

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      renewIfOverdue();
    }
  });
  window.addEventListener("focus", renewIfOverdue);
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", installSessionRenewal, {once: true});
} else {
  installSessionRenewal();
}
