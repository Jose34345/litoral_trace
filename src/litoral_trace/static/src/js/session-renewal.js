const CSRF_META_NAME = "csrf-token";
const REFRESH_CSRF_META_NAME = "lt-refresh-csrf-token";
const REFRESH_AFTER_META_NAME = "lt-session-refresh-after";
const CSRF_HEADER_NAME = "X-CSRF-Token";
const REFRESH_URL = "/api/v1/auth/refresh";

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

function showSessionWarning() {
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
  warning.innerHTML = "<strong>La sesión necesita reautenticación.</strong> Para proteger los datos, Litoral Trace no renovó la sesión automáticamente. Abrí nuevamente la aplicación antes de continuar.";
  document.body.appendChild(warning);
}

function installSessionRenewal() {
  const refreshCsrfToken = readMeta(REFRESH_CSRF_META_NAME);
  const rawInterval = Number.parseInt(readMeta(REFRESH_AFTER_META_NAME), 10);

  if (!refreshCsrfToken || !Number.isFinite(rawInterval) || rawInterval < 15) {
    return;
  }

  const intervalMs = rawInterval * 1000;
  let lastRenewalAt = Date.now();
  let inFlight = null;

  const renew = async () => {
    if (inFlight) {
      return inFlight;
    }

    inFlight = (async () => {
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

      // Refresh-token rotation changes the authenticated session id. Fetch a
      // fresh server-rendered copy of the current GET page only to obtain the
      // new signed CSRF tokens, then update the current DOM in place. User
      // inputs and scroll position are therefore preserved.
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
          showSessionWarning();
          return false;
        }

        const securityMeta = extractSecurityMeta(await pageResponse.text());
        if (!securityMeta.csrfToken || !securityMeta.refreshCsrfToken) {
          showSessionWarning();
          return false;
        }

        updateRenderedCsrfToken(securityMeta.csrfToken);
        updateMeta(REFRESH_CSRF_META_NAME, securityMeta.refreshCsrfToken);
        lastRenewalAt = Date.now();
        return true;
      } catch (_error) {
        showSessionWarning();
        return false;
      }
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
