const REFRESH_URL = "/api/v1/auth/refresh";
const ACCESS_PROBE_URL = "/api/v1/auth/me";
const COORDINATION_COOKIE_NAME = "lt_refresh_inflight";
const COORDINATION_WAIT_SECONDS = 30;
const COORDINATION_POLL_MS = 250;
const COORDINATION_PROBE_EVERY_POLLS = 4;
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
  // Session cookie on purpose: there is no wall-clock lease that can expire
  // while an HTTP keepalive request is still active. The marker is not a
  // credential and contains no user, tenant, token or business information.
  document.cookie = [
    `${COORDINATION_COOKIE_NAME}=1`,
    "Path=/",
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

async function waitForOutstandingRefresh(rawFetch) {
  const maxPolls = Math.ceil(
    (COORDINATION_WAIT_SECONDS * 1000) / COORDINATION_POLL_MS,
  );

  for (let poll = 0; poll < maxPolls; poll += 1) {
    if (!cookieIsPresent(COORDINATION_COOKIE_NAME)) {
      return "released";
    }

    if (
      poll % COORDINATION_PROBE_EVERY_POLLS === 0
      && await refreshedCookieJarHasFutureRenewalWindow(rawFetch)
    ) {
      // The prior keepalive finished after destroying its document. The shared
      // HttpOnly cookie jar now proves that rotation succeeded, so this
      // successor document owns cleanup of the non-sensitive marker.
      clearRefreshInFlight();
      return "refreshed";
    }

    await sleep(COORDINATION_POLL_MS);
  }

  // Important: timeout only bounds how long this document waits. It does NOT
  // expire or clear the marker and therefore can never authorize a second
  // rotation while the first request has an unknown outcome.
  return "ambiguous";
}

function ambiguousRefreshResponse() {
  // Use a forbidden response so session-renewal surfaces its existing
  // reauthentication warning. The marker remains present, so later automatic
  // retries are intercepted again and never send the old refresh token.
  return new Response(null, {
    status: 403,
    statusText: "Refresh outcome ambiguous",
  });
}

function syntheticRefreshSuccess() {
  return new Response("{}", {
    status: 200,
    headers: {"Content-Type": "application/json"},
  });
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
      const outstandingState = await waitForOutstandingRefresh(rawFetch);

      if (outstandingState === "refreshed") {
        return syntheticRefreshSuccess();
      }

      if (outstandingState === "ambiguous") {
        return ambiguousRefreshResponse();
      }

      if (await refreshedCookieJarHasFutureRenewalWindow(rawFetch)) {
        // A surviving document released the marker after receiving the refresh
        // response. Revalidate the cookie jar before deciding whether another
        // rotation is necessary.
        return syntheticRefreshSuccess();
      }
    }

    markRefreshInFlight();

    let response;
    try {
      response = await rawFetch(input, init);
    } catch (_error) {
      // Once the POST was sent, a transport failure cannot prove whether the
      // server committed rotation and Set-Cookie was lost. Keep the marker and
      // fail closed; never feed this outcome into the ordinary refresh retry.
      return ambiguousRefreshResponse();
    }

    if (response.status >= 500) {
      // A gateway/server failure can also be ambiguous after the request left
      // the browser. Keep the marker rather than risk replaying the parent token.
      return ambiguousRefreshResponse();
    }

    // A concrete success or client-side rejection is a known outcome observed
    // by this live document, so it can safely release the coordination marker.
    clearRefreshInFlight();
    return response;
  };

  Object.defineProperty(
    coordinatedFetch,
    "__litoralTraceRefreshCoordination",
    {value: true, enumerable: false},
  );
  window.fetch = coordinatedFetch;
}

installCrossDocumentRefreshCoordination();
