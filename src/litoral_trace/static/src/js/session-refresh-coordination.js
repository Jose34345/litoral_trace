const REFRESH_URL = "/api/v1/auth/refresh";
const ACCESS_PROBE_URL = "/api/v1/auth/me";
const COORDINATION_COOKIE_NAME = "lt_refresh_inflight";
const COORDINATION_MAX_AGE_SECONDS = 15;
const COORDINATION_POLL_MS = 150;
const ACCESS_EXPIRES_AT_META_NAME = "lt-session-access-expires-at";
const SERVER_NOW_META_NAME = "lt-session-server-now";
const REFRESH_AFTER_META_NAME = "lt-session-refresh-after";

function cookieIsPresent(name) {
  return document.cookie
    .split(";")
    .map((item) => item.trim())
    .some((item) => item.startsWith(`${name}=`));
}

function secureCookieSuffix() {
  return window.location.protocol === "https:" ? "; Secure" : "";
}

function markRefreshInFlight() {
  document.cookie = [
    `${COORDINATION_COOKIE_NAME}=1`,
    "Path=/",
    `Max-Age=${COORDINATION_MAX_AGE_SECONDS}`,
    "SameSite=Strict",
  ].join("; ") + secureCookieSuffix();
}

function clearRefreshInFlight() {
  document.cookie = [
    `${COORDINATION_COOKIE_NAME}=`,
    "Path=/",
    "Max-Age=0",
    "SameSite=Strict",
  ].join("; ") + secureCookieSuffix();
}

function sleep(milliseconds) {
  return new Promise((resolve) => window.setTimeout(resolve, milliseconds));
}

async function waitForOutstandingRefresh() {
  const maxPolls = Math.ceil(
    (COORDINATION_MAX_AGE_SECONDS * 1000) / COORDINATION_POLL_MS,
  ) + 2;

  for (let poll = 0; poll < maxPolls; poll += 1) {
    if (!cookieIsPresent(COORDINATION_COOKIE_NAME)) {
      return true;
    }
    await sleep(COORDINATION_POLL_MS);
  }

  return !cookieIsPresent(COORDINATION_COOKIE_NAME);
}

function parseEpochSeconds(documentNode, metaName) {
  const rawValue = documentNode
    .querySelector(`meta[name="${metaName}"]`)
    ?.getAttribute("content")
    ?.trim();
  const value = Number.parseInt(rawValue || "", 10);
  return Number.isFinite(value) && value > 0 ? value : null;
}

async function refreshedCookieJarHasFutureRenewalWindow(rawFetch) {
  try {
    const accessProbe = await rawFetch(
      ACCESS_PROBE_URL,
      {
        method: "GET",
        credentials: "same-origin",
        cache: "no-store",
        headers: {"Accept": "application/json"},
      },
    );
    if (!accessProbe.ok) {
      return false;
    }

    const pageResponse = await rawFetch(
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

    const parsed = new DOMParser().parseFromString(
      await pageResponse.text(),
      "text/html",
    );
    const expiresAt = parseEpochSeconds(parsed, ACCESS_EXPIRES_AT_META_NAME);
    const serverNow = parseEpochSeconds(parsed, SERVER_NOW_META_NAME);
    const refreshAfter = parseEpochSeconds(parsed, REFRESH_AFTER_META_NAME);

    if (expiresAt === null || serverNow === null || refreshAfter === null) {
      return false;
    }

    return expiresAt - serverNow - refreshAfter > 0;
  } catch (_error) {
    return false;
  }
}

function isRefreshRequest(input, init = {}) {
  const method = String(
    init.method || (input instanceof Request ? input.method : "GET"),
  ).toUpperCase();
  if (method !== "POST") {
    return false;
  }

  const rawUrl = input instanceof Request ? input.url : String(input);
  const requestUrl = new URL(rawUrl, window.location.href);
  return (
    requestUrl.origin === window.location.origin
    && requestUrl.pathname === REFRESH_URL
  );
}

function installCrossDocumentRefreshCoordination() {
  if (
    typeof window.fetch !== "function"
    || window.fetch.__litoralTraceRefreshCoordination
  ) {
    return;
  }

  const rawFetch = window.fetch.bind(window);

  const coordinatedFetch = async (input, init = {}) => {
    if (!isRefreshRequest(input, init)) {
      return rawFetch(input, init);
    }

    if (cookieIsPresent(COORDINATION_COOKIE_NAME)) {
      const leaseReleased = await waitForOutstandingRefresh();
      if (!leaseReleased) {
        // Never race another refresh whose document may have been destroyed.
        // The normal session-renewal retry path will attempt again after the
        // short coordination lease expires.
        return new Response(null, {
          status: 425,
          statusText: "Refresh coordination pending",
        });
      }

      if (await refreshedCookieJarHasFutureRenewalWindow(rawFetch)) {
        // The previous keepalive already rotated the shared HttpOnly cookies.
        // Return success so the caller rehydrates its CSRF/timing metadata
        // without rotating the new refresh token again.
        return new Response("{}", {
          status: 200,
          headers: {"Content-Type": "application/json"},
        });
      }
    }

    markRefreshInFlight();
    try {
      return await rawFetch(input, init);
    } finally {
      // If this document survives, release immediately. If navigation destroys
      // it, the browser keeps the non-sensitive marker only until Max-Age.
      clearRefreshInFlight();
    }
  };

  Object.defineProperty(
    coordinatedFetch,
    "__litoralTraceRefreshCoordination",
    {value: true, enumerable: false},
  );
  window.fetch = coordinatedFetch;
}

installCrossDocumentRefreshCoordination();
