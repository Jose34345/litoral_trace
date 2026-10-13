/* Native <dialog> ensures inert background/focus containment (W3C WAI-ARIA). */
(() => {
  const root = document.querySelector("[data-source-inspector]");
  if (!root || window.__ltSourceInspectorBound) return;
  window.__ltSourceInspectorBound = true;
  const frame = root.querySelector("[data-inspector-frame]");
  const title = root.querySelector("[data-inspector-title]");
  const locator = root.querySelector("[data-inspector-locator]");
  const external = root.querySelector("[data-inspector-external]");
  const closeButton = root.querySelector("[data-source-inspector-close]");
  let returnFocus = null;
  let closeTimer = null;
  const reduced = () => window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  function close() {
    if (!root.open) return;
    root.classList.remove("is-open");
    window.clearTimeout(closeTimer);
    closeTimer = window.setTimeout(() => {
      root.close();
      frame.src = "about:blank";
      returnFocus?.focus?.({preventScroll:true});
      returnFocus = null;
    }, reduced() ? 0 : 180);
  }
  function open(button) {
    const raw = button.getAttribute("data-source-url");
    if (!raw) return;
    const url = new URL(raw, window.location.href);
    if (url.origin !== window.location.origin
        || !/^\/operations\/[0-9a-f-]{36}\/source\/[0-9]+$/.test(url.pathname)) return;
    const page = Number.parseInt(button.getAttribute("data-source-page") || "1", 10);
    url.hash = "page=" + (Number.isInteger(page) && page > 0 && page <= 10000 ? page : 1);
    window.clearTimeout(closeTimer);
    returnFocus = button;
    title.textContent = button.getAttribute("data-source-label") || "Original PDF";
    locator.textContent = button.getAttribute("data-source-locator") || "Check the source page.";
    external.href = url.href;
    frame.src = url.href;
    if (!root.open) root.showModal();
    window.requestAnimationFrame(() => root.classList.add("is-open"));
    closeButton.focus({preventScroll:true});
  }
  document.addEventListener("click", (event) => {
    const launch = event.target.closest("[data-source-inspect]");
    if (launch) { event.preventDefault(); open(launch); }
    else if (event.target.closest("[data-source-inspector-close]") || event.target === root) close();
  });
  root.addEventListener("cancel", (event) => { event.preventDefault(); close(); });
})();
