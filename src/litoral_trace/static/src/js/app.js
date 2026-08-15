const CSRF_META_NAME = "csrf-token";
const CSRF_HEADER_NAME = "X-CSRF-Token";

function readCsrfToken() {
  const meta = document.querySelector(`meta[name="${CSRF_META_NAME}"]`);
  return meta?.getAttribute("content")?.trim() || "";
}

function installHtmxCsrfBridge() {
  document.body.addEventListener("htmx:configRequest", (event) => {
    const token = readCsrfToken();
    if (!token) {
      return;
    }

    event.detail.headers[CSRF_HEADER_NAME] = token;
  });
}

function setDrawerState(drawer, overlay, isOpen) {
  drawer.dataset.open = isOpen ? "true" : "false";
  drawer.classList.toggle("-translate-x-full", !isOpen);
  overlay.classList.toggle("hidden", !isOpen);
  document.documentElement.classList.toggle("overflow-hidden", isOpen);
}

function installMobileDrawer() {
  const drawer = document.querySelector("[data-app-drawer]");
  const overlay = document.querySelector("[data-app-drawer-overlay]");
  const openButton = document.querySelector("[data-app-drawer-open]");
  const closeButton = document.querySelector("[data-app-drawer-close]");

  if (!drawer || !overlay || !openButton || !closeButton) {
    return;
  }

  const close = () => setDrawerState(drawer, overlay, false);
  const open = () => setDrawerState(drawer, overlay, true);

  openButton.addEventListener("click", open);
  closeButton.addEventListener("click", close);
  overlay.addEventListener("click", close);

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      close();
    }
  });
}

function installDisclosureMenus() {
  document.addEventListener("click", (event) => {
    const trigger = event.target.closest("[data-disclosure-trigger]");
    if (!trigger) {
      return;
    }

    const targetId = trigger.getAttribute("aria-controls");
    if (!targetId) {
      return;
    }

    const target = document.getElementById(targetId);
    if (!target) {
      return;
    }

    const expanded = trigger.getAttribute("aria-expanded") === "true";
    trigger.setAttribute("aria-expanded", String(!expanded));
    target.hidden = expanded;
  });
}

function boot() {
  installHtmxCsrfBridge();
  installMobileDrawer();
  installDisclosureMenus();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", boot, { once: true });
} else {
  boot();
}

export { CSRF_HEADER_NAME, CSRF_META_NAME, readCsrfToken };
