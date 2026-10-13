/* Native-PDF source inspector: lightweight, delegated and HTMX-safe. */
(() => {
  const root = document.querySelector("[data-source-inspector]");
  if (!root || window.__ltSourceInspectorBound) return;
  window.__ltSourceInspectorBound = true;
  const panel = root.querySelector("[role=dialog]");
  const frame = root.querySelector("[data-inspector-frame]");
  const title = root.querySelector("[data-inspector-title]");
  const locator = root.querySelector("[data-inspector-locator]");
  const external = root.querySelector("[data-inspector-external]");
  const closeButton = root.querySelector(".lt-source-inspector__close");
  let returnFocus = null;

  function close() {
    if (!root.classList.contains("is-open")) return;
    root.classList.remove("is-open");
    root.setAttribute("aria-hidden","true");
    document.documentElement.classList.remove("lt-source-inspector-lock");
    frame.src = "about:blank";
    returnFocus?.focus?.({preventScroll:true});
    returnFocus = null;
  }

  function open(button) {
    const raw = button.getAttribute("data-source-url");
    if (!raw) return;
    const url = new URL(raw, window.location.href);
    // No external URLs, storage keys, data: or javascript: paths.
    if (url.origin !== window.location.origin
        || !/^\/operations\/[0-9a-f-]{36}\/source\/[0-9]+$/.test(url.pathname)) return;
    const page = Number.parseInt(button.getAttribute("data-source-page") || "1", 10);
    url.hash = "page=" + (Number.isInteger(page) && page > 0 && page <= 10000 ? page : 1);
    returnFocus = button;
    title.textContent = button.getAttribute("data-source-label") || "Original source";
    locator.textContent = button.getAttribute("data-source-locator") || "Review the source page before confirming the value.";
    external.href = url.href;
    frame.src = url.href;
    root.classList.add("is-open");
    root.setAttribute("aria-hidden","false");
    document.documentElement.classList.add("lt-source-inspector-lock");
    closeButton.focus({preventScroll:true});
  }
  document.addEventListener("click", (event) => {
    const launch = event.target.closest("[data-source-inspect]");
    if (launch) { event.preventDefault(); open(launch); return; }
    if (event.target.closest("[data-source-inspector-close]")) close();
  });
  document.addEventListener("keydown", (event) => {
    if (!root.classList.contains("is-open")) return;
    if (event.key === "Escape") { event.preventDefault(); close(); return; }
    if (event.key === "Tab") {
      const focusable = [closeButton, external].filter(el => !el.hasAttribute("disabled"));
      if (event.shiftKey && document.activeElement === focusable[0]) {
        event.preventDefault(); focusable[focusable.length - 1].focus();
      } else if (!event.shiftKey && document.activeElement === focusable[focusable.length - 1]) {
        event.preventDefault(); focusable[0].focus();
      }
    }
  });
})();
