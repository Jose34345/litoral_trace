const CSRF_META_NAME = "csrf-token";
const CSRF_HEADER_NAME = "X-CSRF-Token";
const SAFE_METHODS = new Set([
  "GET",
  "HEAD",
  "OPTIONS",
  "TRACE",
]);

function readCsrfToken() {
  const meta = document.querySelector(
    `meta[name="${CSRF_META_NAME}"]`,
  );

  return (
    meta?.getAttribute("content")?.trim()
    || ""
  );
}

function installHtmxSameOriginGuard() {
  document.body.addEventListener(
    "htmx:validateUrl",
    (event) => {
      if (!event.detail.sameHost) {
        event.preventDefault();
      }
    },
  );
}

function installHtmxCsrfBridge() {
  document.body.addEventListener(
    "htmx:configRequest",
    (event) => {
      const requestMethod = String(
        event.detail.verb || "GET",
      ).toUpperCase();

      if (SAFE_METHODS.has(requestMethod)) {
        return;
      }

      const token = readCsrfToken();

      if (!token) {
        return;
      }

      event.detail.headers[
        CSRF_HEADER_NAME
      ] = token;
    },
  );
}

function installFetchCsrfBridge() {
  if (
    typeof window.fetch !== "function"
    || window.fetch.__litoralTraceCsrf
  ) {
    return;
  }

  const nativeFetch = window.fetch.bind(
    window,
  );

  const csrfFetch = (
    input,
    init = {},
  ) => {
    const requestMethod = (
      init.method
      || (
        input instanceof Request
          ? input.method
          : "GET"
      )
    ).toUpperCase();

    if (SAFE_METHODS.has(requestMethod)) {
      return nativeFetch(input, init);
    }

    const rawUrl = (
      input instanceof Request
        ? input.url
        : String(input)
    );

    const requestUrl = new URL(
      rawUrl,
      window.location.href,
    );

    if (
      requestUrl.origin
      !== window.location.origin
    ) {
      return nativeFetch(input, init);
    }

    const token = readCsrfToken();

    if (!token) {
      return nativeFetch(input, init);
    }

    const headers = new Headers(
      input instanceof Request
        ? input.headers
        : undefined,
    );

    const initHeaders = new Headers(
      init.headers || undefined,
    );

    initHeaders.forEach(
      (value, key) => {
        headers.set(key, value);
      },
    );

    if (!headers.has(CSRF_HEADER_NAME)) {
      headers.set(
        CSRF_HEADER_NAME,
        token,
      );
    }

    return nativeFetch(
      input,
      {
        ...init,
        headers,
      },
    );
  };

  Object.defineProperty(
    csrfFetch,
    "__litoralTraceCsrf",
    {
      value: true,
      enumerable: false,
    },
  );

  window.fetch = csrfFetch;
}

function setDrawerState(
  drawer,
  overlay,
  isOpen,
) {
  drawer.dataset.open = (
    isOpen
      ? "true"
      : "false"
  );

  drawer.classList.toggle(
    "-translate-x-full",
    !isOpen,
  );

  overlay.classList.toggle(
    "hidden",
    !isOpen,
  );

  document.documentElement.classList.toggle(
    "overflow-hidden",
    isOpen,
  );
}

function installMobileDrawer() {
  const drawer = document.querySelector(
    "[data-app-drawer]",
  );

  const overlay = document.querySelector(
    "[data-app-drawer-overlay]",
  );

  const openButton = document.querySelector(
    "[data-app-drawer-open]",
  );

  const closeButton = document.querySelector(
    "[data-app-drawer-close]",
  );

  if (
    !drawer
    || !overlay
    || !openButton
    || !closeButton
  ) {
    return;
  }

  const close = () => setDrawerState(
    drawer,
    overlay,
    false,
  );

  const open = () => setDrawerState(
    drawer,
    overlay,
    true,
  );

  openButton.addEventListener(
    "click",
    open,
  );

  closeButton.addEventListener(
    "click",
    close,
  );

  overlay.addEventListener(
    "click",
    close,
  );

  document.addEventListener(
    "keydown",
    (event) => {
      if (event.key === "Escape") {
        close();
      }
    },
  );
}

function installDisclosureMenus() {
  document.addEventListener(
    "click",
    (event) => {
      const trigger = event.target.closest(
        "[data-disclosure-trigger]",
      );

      if (!trigger) {
        return;
      }

      const targetId = trigger.getAttribute(
        "aria-controls",
      );

      if (!targetId) {
        return;
      }

      const target = document.getElementById(
        targetId,
      );

      if (!target) {
        return;
      }

      const expanded = (
        trigger.getAttribute(
          "aria-expanded",
        ) === "true"
      );

      trigger.setAttribute(
        "aria-expanded",
        String(!expanded),
      );

      target.hidden = expanded;
    },
  );
}

function boot() {
  installHtmxSameOriginGuard();
  installHtmxCsrfBridge();
  installFetchCsrfBridge();
  installMobileDrawer();
  installDisclosureMenus();
}

if (document.readyState === "loading") {
  document.addEventListener(
    "DOMContentLoaded",
    boot,
    {
      once: true,
    },
  );
} else {
  boot();
}

export {
  CSRF_HEADER_NAME,
  CSRF_META_NAME,
  readCsrfToken,
};