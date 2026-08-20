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

function getDrawerFocusableElements(drawer) {
  const selector = [
    "a[href]",
    "button:not([disabled])",
    "input:not([disabled])",
    "select:not([disabled])",
    "textarea:not([disabled])",
    '[tabindex]:not([tabindex="-1"])',
  ].join(",");

  return Array.from(
    drawer.querySelectorAll(selector),
  ).filter(
    (element) => (
      !element.hasAttribute("hidden")
      && element.getAttribute("aria-hidden")
        !== "true"
    ),
  );
}

function setDrawerState(
  drawer,
  overlay,
  openButton,
  isOpen,
) {
  const isDesktop = window.matchMedia(
    "(min-width: 1024px)",
  ).matches;

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

  overlay.setAttribute(
    "aria-hidden",
    String(!isOpen),
  );

  openButton.setAttribute(
    "aria-expanded",
    String(isOpen),
  );

  drawer.setAttribute(
    "aria-hidden",
    (
      isDesktop || isOpen
        ? "false"
        : "true"
    ),
  );

  document.documentElement.classList.toggle(
    "overflow-hidden",
    isOpen && !isDesktop,
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

  let previouslyFocused = null;

  const isOpen = () => (
    drawer.dataset.open === "true"
  );

  const close = (
    restoreFocus = true,
  ) => {
    const wasOpen = isOpen();

    setDrawerState(
      drawer,
      overlay,
      openButton,
      false,
    );

    if (
      wasOpen
      && restoreFocus
      && previouslyFocused
      instanceof HTMLElement
    ) {
      previouslyFocused.focus({
        preventScroll: true,
      });
    }

    previouslyFocused = null;
  };

  const open = () => {
    previouslyFocused = (
      document.activeElement
    );

    setDrawerState(
      drawer,
      overlay,
      openButton,
      true,
    );

    closeButton.focus({
      preventScroll: true,
    });
  };

  openButton.addEventListener(
    "click",
    open,
  );

  closeButton.addEventListener(
    "click",
    () => close(),
  );

  overlay.addEventListener(
    "click",
    () => close(),
  );

  drawer.querySelectorAll(
    "a[href]",
  ).forEach(
    (link) => {
      link.addEventListener(
        "click",
        () => {
          if (
            window.matchMedia(
              "(max-width: 1023px)",
            ).matches
          ) {
            close(false);
          }
        },
      );
    },
  );

  document.addEventListener(
    "keydown",
    (event) => {
      if (!isOpen()) {
        return;
      }

      if (event.key === "Escape") {
        event.preventDefault();
        close();
        return;
      }

      if (event.key !== "Tab") {
        return;
      }

      const focusable = (
        getDrawerFocusableElements(
          drawer,
        )
      );

      if (focusable.length === 0) {
        event.preventDefault();
        closeButton.focus();
        return;
      }

      const first = focusable[0];
      const last = (
        focusable[
          focusable.length - 1
        ]
      );

      if (
        event.shiftKey
        && (
          document.activeElement
            === first
          || !drawer.contains(
            document.activeElement,
          )
        )
      ) {
        event.preventDefault();
        last.focus();
        return;
      }

      if (
        !event.shiftKey
        && document.activeElement
          === last
      ) {
        event.preventDefault();
        first.focus();
      }
    },
  );

  const desktopMedia = (
    window.matchMedia(
      "(min-width: 1024px)",
    )
  );

  const syncDesktopState = (
    event,
  ) => {
    if (
      event.matches
      && isOpen()
    ) {
      close(false);
    }

    drawer.setAttribute(
      "aria-hidden",
      (
        event.matches
          ? "false"
          : String(!isOpen())
      ),
    );
  };

  if (
    typeof desktopMedia.addEventListener
    === "function"
  ) {
    desktopMedia.addEventListener(
      "change",
      syncDesktopState,
    );
  }

  setDrawerState(
    drawer,
    overlay,
    openButton,
    false,
  );
}

function installPublicNavigation() {
  const trigger = document.querySelector(
    "[data-public-nav-trigger]",
  );

  const panel = document.querySelector(
    "[data-public-nav-panel]",
  );

  if (!trigger || !panel) {
    return;
  }

  const isOpen = () => (
    trigger.getAttribute(
      "aria-expanded",
    ) === "true"
  );

  const setOpen = (open) => {
    trigger.setAttribute(
      "aria-expanded",
      String(open),
    );

    trigger.setAttribute(
      "aria-label",
      (
        open
          ? "Cerrar navegación"
          : "Abrir navegación"
      ),
    );

    panel.hidden = !open;
  };

  trigger.addEventListener(
    "click",
    () => {
      setOpen(!isOpen());
    },
  );

  panel.querySelectorAll(
    "a[href]",
  ).forEach(
    (link) => {
      link.addEventListener(
        "click",
        () => setOpen(false),
      );
    },
  );

  document.addEventListener(
    "keydown",
    (event) => {
      if (
        event.key === "Escape"
        && isOpen()
      ) {
        event.preventDefault();
        setOpen(false);
        trigger.focus({
          preventScroll: true,
        });
      }
    },
  );

  const desktopMedia = window.matchMedia(
    "(min-width: 640px)",
  );

  const syncViewport = (event) => {
    if (event.matches) {
      setOpen(false);
    }
  };

  if (
    typeof desktopMedia.addEventListener
    === "function"
  ) {
    desktopMedia.addEventListener(
      "change",
      syncViewport,
    );
  }

  setOpen(false);
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
  installPublicNavigation();
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
