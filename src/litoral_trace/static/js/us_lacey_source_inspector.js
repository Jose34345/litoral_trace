/* PDF bytes stay only in a live, same-origin inspection session. */
class EvidenceSession {
  constructor() {
    this.active = null;
    this.abortController = null;
    this.token = 0;
  }

  async acquire({key, url}) {
    if (this.active?.key === key) return this.active;
    this.invalidate();
    const token = ++this.token;
    const controller = new AbortController();
    this.abortController = controller;
    let response;
    try {
      response = await fetch(url, {
        credentials:"same-origin", cache:"no-store", signal:controller.signal,
        headers:{Accept:"application/pdf"},
      });
    } catch (error) {
      if (error.name === "AbortError") throw error;
      throw new Error("NETWORK");
    }
    if (token !== this.token) throw new DOMException("Stale evidence request", "AbortError");
    if (!response.ok) throw new Error(String(response.status));
    const contentType = (response.headers.get("content-type") || "").toLowerCase();
    if (!contentType.startsWith("application/pdf")) throw new Error("INVALID_DOCUMENT");
    const objectUrl = URL.createObjectURL(await response.blob());
    if (token !== this.token) {
      URL.revokeObjectURL(objectUrl);
      throw new DOMException("Stale evidence request", "AbortError");
    }
    this.active = {key, url, objectUrl};
    return this.active;
  }

  release() { this.invalidate(); }

  invalidate() {
    this.abortController?.abort();
    this.abortController = null;
    if (this.active?.objectUrl) URL.revokeObjectURL(this.active.objectUrl);
    this.active = null;
    this.token += 1;
  }
}

class PdfEvidenceRenderer {
  constructor(canvas, controls) {
    this.canvas = canvas;
    this.controls = controls;
    this.pdf = null;
    this.page = 1;
    this.zoom = 1;
    this.renderTask = null;
  }

  async open(objectUrl, page = 1) {
    this.destroy();
    const pdfjs = await import("/static/vendor/pdfjs/pdf.min.mjs");
    pdfjs.GlobalWorkerOptions.workerSrc = "/static/vendor/pdfjs/pdf.worker.min.mjs";
    this.pdf = await pdfjs.getDocument({url:objectUrl}).promise;
    this.page = Math.min(Math.max(1, page), this.pdf.numPages);
    await this.renderPage();
  }

  async renderPage() {
    if (!this.pdf) return;
    this.renderTask?.cancel();
    const pdfPage = await this.pdf.getPage(this.page);
    const viewport = pdfPage.getViewport({scale:this.zoom});
    const context = this.canvas.getContext("2d", {alpha:false});
    this.canvas.width = Math.ceil(viewport.width);
    this.canvas.height = Math.ceil(viewport.height);
    this.renderTask = pdfPage.render({canvasContext:context, viewport});
    await this.renderTask.promise;
    this.controls.page.textContent = `Page ${this.page} of ${this.pdf.numPages}`;
    this.controls.zoom.textContent = `${Math.round(this.zoom * 100)}%`;
    this.controls.previous.disabled = this.page <= 1;
    this.controls.next.disabled = this.page >= this.pdf.numPages;
  }

  async nextPage() { if (this.pdf && this.page < this.pdf.numPages) { this.page += 1; await this.renderPage(); } }
  async previousPage() { if (this.pdf && this.page > 1) { this.page -= 1; await this.renderPage(); } }
  async setZoom(nextZoom) { if (this.pdf) { this.zoom = Math.min(2.5, Math.max(.65, nextZoom)); await this.renderPage(); } }
  destroy() {
    this.renderTask?.cancel();
    this.renderTask = null;
    this.pdf?.destroy();
    this.pdf = null;
    this.canvas.width = 0;
    this.canvas.height = 0;
  }
}

/* Native <dialog> ensures inert background/focus containment (W3C WAI-ARIA). */
(() => {
  const root = document.querySelector("[data-source-inspector]");
  if (!root || window.__ltSourceInspectorBound) return;
  window.__ltSourceInspectorBound = true;
  const smartEntry = document.querySelector("[data-smart-entry-selector]");

  function bindSmartEntrySelector() {
    if (!smartEntry) return;
    const nativeSelect = smartEntry.querySelector("[data-entry-type]");
    const search = smartEntry.querySelector("[data-entry-search]");
    const current = smartEntry.querySelector("[data-entry-current]");
    const options = Array.from(smartEntry.querySelectorAll("[data-entry-option]"));
    let activeIndex = -1;
    const visibleOptions = () => options.filter((option) => !option.hidden);
    const select = (code, focusOption = false) => {
      const changed = nativeSelect.value !== code;
      nativeSelect.value = code;
      if (changed) nativeSelect.dispatchEvent(new Event("change", {bubbles:true}));
      options.forEach((option) => option.setAttribute("aria-selected", String(option.dataset.entryCode === code)));
      const selected = options.find((option) => option.dataset.entryCode === code);
      current.textContent = selected ? selected.innerText.replace(/\s+/g, " ").trim() : "Entry type not confirmed.";
      if (focusOption && selected) selected.focus({preventScroll:true});
    };
    options.forEach((option) => option.addEventListener("click", () => select(option.dataset.entryCode || "", false)));
    search.addEventListener("input", () => {
      const query = search.value.trim().toLowerCase();
      options.forEach((option) => { option.hidden = !!query && !option.dataset.entrySearchText.toLowerCase().includes(query); });
      activeIndex = -1;
    });
    search.addEventListener("keydown", (event) => {
      const visible = visibleOptions();
      if (event.key === "ArrowDown" || event.key === "ArrowUp") {
        event.preventDefault();
        activeIndex = (activeIndex + (event.key === "ArrowDown" ? 1 : -1) + visible.length) % visible.length;
        visible[activeIndex]?.focus({preventScroll:true});
      } else if (event.key === "Enter" && visible.length === 1) {
        event.preventDefault(); select(visible[0].dataset.entryCode || "", false);
      } else if (event.key === "Escape") {
        search.value = ""; options.forEach((option) => { option.hidden = false; }); search.blur();
      }
    });
    nativeSelect.addEventListener("change", () => select(nativeSelect.value || "", false));
    select(nativeSelect.value || "", false);
  }
  bindSmartEntrySelector();
  const canvas = root.querySelector("[data-inspector-canvas]");
  const title = root.querySelector("[data-inspector-title]");
  const locator = root.querySelector("[data-inspector-locator]");
  const external = root.querySelector("[data-inspector-external]");
  const closeButton = root.querySelector("[data-source-inspector-close]");
  const status = root.querySelector("[data-inspector-status]");
  const evidenceSession = new EvidenceSession();
  const renderer = new PdfEvidenceRenderer(canvas, {
    previous:root.querySelector("[data-pdf-previous-page]"),
    next:root.querySelector("[data-pdf-next-page]"),
    page:root.querySelector("[data-pdf-page-number]"),
    zoom:root.querySelector("[data-pdf-zoom]"),
  });
  window.LitoralEvidenceSession = evidenceSession;
  let returnFocus = null;
  let closeTimer = null;
  const reduced = () => window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  const sourceUrl = (raw) => {
    if (!raw) return null;
    const url = new URL(raw, window.location.href);
    return url.origin === window.location.origin
      && /^\/operations\/[0-9a-f-]{36}\/source\/[0-9]+$/.test(url.pathname) ? url : null;
  };
  const errorMessage = (error) => ({
    "401":"Your session has ended. Sign in again to inspect this original.",
    "403":"You do not have access to this original document.",
    "404":"Original document unavailable. Add a new source to continue.",
    "409":"Evidence changed. Review the current version before confirming.",
    "503":"Original document is temporarily unavailable. Try again shortly.",
    "INVALID_DOCUMENT":"The authorized response was not a PDF document.",
    "NETWORK":"The document could not be reached. Check your connection and try again.",
  })[error?.message] || "The document could not be opened safely.";
  const setStatus = (message, isError = false) => {
    if (!status) return;
    status.textContent = message;
    status.hidden = !message;
    status.classList.toggle("is-error", isError);
  };
  root.querySelector("[data-pdf-previous-page]").addEventListener("click", () => renderer.previousPage());
  root.querySelector("[data-pdf-next-page]").addEventListener("click", () => renderer.nextPage());
  root.querySelector("[data-pdf-zoom-in]").addEventListener("click", () => renderer.setZoom(renderer.zoom + .2));
  root.querySelector("[data-pdf-zoom-out]").addEventListener("click", () => renderer.setZoom(renderer.zoom - .2));

  function close() {
    if (!root.open) return;
    root.classList.remove("is-open");
    window.clearTimeout(closeTimer);
    closeTimer = window.setTimeout(() => {
      root.close();
      renderer.destroy();
      evidenceSession.release();
      returnFocus?.focus?.({preventScroll:true});
      returnFocus = null;
    }, reduced() ? 0 : 180);
  }
  async function open(button) {
    const url = sourceUrl(button.getAttribute("data-source-url"));
    if (!url) return;
    const page = Number.parseInt(button.getAttribute("data-source-page") || "1", 10);
    url.hash = "page=" + (Number.isInteger(page) && page > 0 && page <= 10000 ? page : 1);
    window.clearTimeout(closeTimer);
    returnFocus = button;
    title.textContent = button.getAttribute("data-source-label") || "Original PDF";
    locator.textContent = button.getAttribute("data-source-locator") || "Check the source page.";
    external.href = url.href;
    renderer.destroy();
    setStatus("Loading verified original…");
    if (!root.open) root.showModal();
    window.requestAnimationFrame(() => root.classList.add("is-open"));
    closeButton.focus({preventScroll:true});
    try {
      const resource = await evidenceSession.acquire({key:url.pathname, url:url.href});
      const current = sourceUrl(button.getAttribute("data-source-url"));
      if (!current || resource.key !== current.pathname || !root.open) return;
      await renderer.open(resource.objectUrl, Number.isInteger(page) && page > 0 && page <= 10000 ? page : 1);
      setStatus("");
    } catch (error) {
      if (error.name === "AbortError") return;
      renderer.destroy();
      setStatus(errorMessage(error), true);
    }
  }
  document.addEventListener("change", (event) => {
    const select = event.target.closest("[data-applicability-source-select]");
    if (!select) return;
    const form = select.closest("form");
    const block = select.closest("[data-applicability-source-block]");
    const button = block?.querySelector("[data-applicability-inspect-selected]");
    if (!button) return;
    const selected = select.selectedOptions[0];
    const available = !!select.value && !!selected && !selected.disabled;
    button.disabled = !available;
    if (!available) { button.removeAttribute("data-source-url"); return; }
    const pageInput = block.querySelector('input[type="number"]');
    button.setAttribute("data-source-url",
      (window.location.pathname.endsWith("/") ? window.location.pathname.slice(0, -1) : window.location.pathname) + "/source/" + select.value);
    button.setAttribute("data-source-page", pageInput?.value || "1");
    button.setAttribute("data-source-label", selected.dataset.sourceFile || "Source PDF");
    button.setAttribute("data-source-locator", "Original document · verify the selected page.");
  });
  document.addEventListener("input", (event) => {
    if (!event.target.matches('[data-applicability-review-form] input[type="number"]')) return;
    const button = event.target.closest("[data-applicability-source-block]")?.querySelector("[data-applicability-inspect-selected]");
    if (button) button.setAttribute("data-source-page", event.target.value || "1");
  });
  document.addEventListener("click", (event) => {
    const launch = event.target.closest("[data-source-inspect]");
    if (launch) { event.preventDefault(); open(launch); }
    else if (event.target.closest("[data-source-inspector-close]") || event.target === root) close();
  });
  root.addEventListener("cancel", (event) => { event.preventDefault(); close(); });
})();
