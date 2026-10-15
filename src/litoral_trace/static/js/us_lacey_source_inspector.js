/* Authorized PDF bytes live only in one short-lived, same-origin inspection session. */
class EvidenceSession {
  constructor() {
    this.active = null;
    this.pending = null;
    this.token = 0;
  }

  acquire({key, url}) {
    if (this.active?.key === key) return Promise.resolve(this.active);
    // A hover→pin transition must share an in-flight authenticated request.
    if (this.pending?.key === key) return this.pending.promise;
    this.invalidate();
    const token = this.token;
    const controller = new AbortController();
    const pending = {key, controller, promise:null};
    pending.promise = (async () => {
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
      const blob = await response.blob();
      if (token !== this.token) throw new DOMException("Stale evidence request", "AbortError");
      const objectUrl = URL.createObjectURL(blob);
      if (token !== this.token) {
        URL.revokeObjectURL(objectUrl);
        throw new DOMException("Stale evidence request", "AbortError");
      }
      this.active = {key, url, objectUrl};
      return this.active;
    })().finally(() => {
      if (this.pending === pending) this.pending = null;
    });
    this.pending = pending;
    return pending.promise;
  }

  release() { this.invalidate(); }

  invalidate() {
    this.token += 1;
    this.pending?.controller.abort();
    this.pending = null;
    if (this.active?.objectUrl) URL.revokeObjectURL(this.active.objectUrl);
    this.active = null;
  }
}

/* Page rendering is serialized; stale async work never touches the active canvas. */
class PdfEvidenceRenderer {
  constructor(canvas, controls) {
    this.canvas = canvas;
    this.controls = controls;
    this.pdf = null;
    this.loadingTask = null;
    this.page = 1;
    this.zoom = 1;
    this.renderTask = null;
    this.renderPromise = null;
    this.renderBarrier = Promise.resolve();
    this.generation = 0;
    this.renderGeneration = 0;
  }

  async open(objectUrl, page = 1) {
    this.destroy();
    const generation = this.generation;
    const pdfjs = await import("/static/vendor/pdfjs/pdf.min.mjs");
    if (generation !== this.generation) return;
    pdfjs.GlobalWorkerOptions.workerSrc = "/static/vendor/pdfjs/pdf.worker.min.mjs";
    const loadingTask = pdfjs.getDocument({url:objectUrl});
    this.loadingTask = loadingTask;
    let pdf;
    try {
      pdf = await loadingTask.promise;
    } catch (error) {
      if (generation !== this.generation) return;
      throw error;
    }
    if (generation !== this.generation) {
      void pdf.destroy().catch(() => {});
      return;
    }
    this.loadingTask = null;
    this.pdf = pdf;
    this.page = Math.min(Math.max(1, page), pdf.numPages);
    this.zoom = 1;
    await this.renderPage();
  }

  renderPage() {
    const pdf = this.pdf;
    if (!pdf) return Promise.resolve();
    const generation = this.generation;
    const renderGeneration = ++this.renderGeneration;
    const previous = this.renderPromise || this.renderBarrier;
    const requestedPage = this.page;
    const requestedZoom = this.zoom;
    this.renderTask?.cancel();
    const isCurrent = () => generation === this.generation
      && renderGeneration === this.renderGeneration && pdf === this.pdf;
    const next = (async () => {
      // A PDF.js canvas cannot be reused until its prior render is settled.
      if (previous) await previous.catch(() => {});
      if (!isCurrent()) return;
      const pdfPage = await pdf.getPage(requestedPage);
      if (!isCurrent()) { pdfPage.cleanup?.(); return; }
      const viewport = pdfPage.getViewport({scale:requestedZoom});
      const context = this.canvas.getContext("2d", {alpha:false});
      this.canvas.width = Math.ceil(viewport.width);
      this.canvas.height = Math.ceil(viewport.height);
      const task = pdfPage.render({canvasContext:context, viewport});
      this.renderTask = task;
      try {
        await task.promise;
      } catch (error) {
        if (error?.name !== "RenderingCancelledException" && isCurrent()) throw error;
        return;
      } finally {
        if (this.renderTask === task) this.renderTask = null;
        pdfPage.cleanup?.();
      }
      if (!isCurrent()) return;
      this.controls.page.textContent = `Page ${requestedPage} of ${pdf.numPages}`;
      this.controls.zoom.textContent = `${Math.round(requestedZoom * 100)}%`;
      this.controls.previous.disabled = requestedPage <= 1;
      this.controls.next.disabled = requestedPage >= pdf.numPages;
    })();
    this.renderPromise = next;
    return next;
  }

  async nextPage() {
    if (this.pdf && this.page < this.pdf.numPages) { this.page += 1; await this.renderPage(); }
  }
  async previousPage() {
    if (this.pdf && this.page > 1) { this.page -= 1; await this.renderPage(); }
  }
  async setZoom(nextZoom) {
    if (this.pdf) { this.zoom = Math.min(2.5, Math.max(.65, nextZoom)); await this.renderPage(); }
  }
  destroy() {
    ++this.generation;
    ++this.renderGeneration;
    // Keep the canvas fenced until an older document's render actually settles.
    if (this.renderPromise) {
      this.renderBarrier = Promise.allSettled([this.renderBarrier, this.renderPromise]);
    }
    this.renderTask?.cancel();
    this.renderTask = null;
    if (this.loadingTask) void this.loadingTask.destroy().catch(() => {});
    this.loadingTask = null;
    if (this.pdf) void this.pdf.destroy().catch(() => {});
    this.pdf = null;
    this.renderPromise = null;
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
  const preview = document.querySelector("[data-evidence-preview]");
  const previewCanvas = preview?.querySelector("[data-preview-canvas]");
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
  const previewRenderer = previewCanvas ? new PdfEvidenceRenderer(previewCanvas, {
    previous:{disabled:true}, next:{disabled:true}, page:{textContent:""}, zoom:{textContent:""},
  }) : null;
  window.LitoralEvidenceSession = evidenceSession;
  let returnFocus = null;
  let closeTimer = null;
  let previewTimer = null;
  let previewCloseTimer = null;
  let previewButton = null;
  let previewGeneration = 0;
  let activeInspectorKey = null;
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
  const mobileViewport = () => window.matchMedia("(max-width: 850px)").matches;
  const sourceKey = (button, url) => {
    const revision = button.getAttribute("data-source-revision")
      || button.closest("[data-evidence-card]")?.dataset.sourceRevision
      || document.querySelector("[data-applicability-preview]")?.dataset.sourceRevision
      || "";
    return url.pathname + "|" + revision;
  };
  function closePreview() {
    ++previewGeneration;
    window.clearTimeout(previewTimer);
    window.clearTimeout(previewCloseTimer);
    if (!preview || preview.hidden) return;
    preview.classList.remove("is-open");
    previewCloseTimer = window.setTimeout(() => {
      preview.hidden = true;
      previewRenderer?.destroy();
      if (!root.open) evidenceSession.release();
    }, reduced() ? 0 : 160);
  }
  function positionPreview(button) {
    const rect = button.getBoundingClientRect();
    const width = Math.min(620, window.innerWidth - 24);
    const height = Math.min(window.innerHeight * .72, 680);
    const left = rect.right + width + 12 <= window.innerWidth ? rect.right + 10 : Math.max(12, rect.left - width - 10);
    const top = Math.min(Math.max(12, rect.top), window.innerHeight - height - 12);
    Object.assign(preview.style, {left:`${left}px`, top:`${top}px`});
  }
  async function openPreview(button) {
    if (mobileViewport() || !preview || !previewRenderer) return;
    const url = sourceUrl(button.getAttribute("data-source-url")); if (!url) return;
    const previewRequest = ++previewGeneration;
    window.clearTimeout(previewCloseTimer);
    previewButton = button;
    positionPreview(button);
    preview.hidden = false;
    preview.querySelector("[data-preview-title]").textContent = button.getAttribute("data-source-label") || "Original PDF";
    preview.querySelector("[data-preview-meta]").textContent = "Loading verified original…";
    window.requestAnimationFrame(() => { if (previewRequest === previewGeneration) preview.classList.add("is-open"); });
    try {
      const page = Number.parseInt(button.getAttribute("data-source-page") || "1", 10) || 1;
      const resource = await evidenceSession.acquire({key:sourceKey(button,url), url:url.href});
      if (previewRequest !== previewGeneration || preview.hidden) return;
      await previewRenderer.open(resource.objectUrl, page);
      if (previewRequest !== previewGeneration || preview.hidden) return;
      preview.querySelector("[data-preview-meta]").textContent = `Page ${previewRenderer.page} · ${button.getAttribute("data-source-locator") || "Original evidence"}`;
    } catch (error) {
      if (previewRequest === previewGeneration && error.name !== "AbortError") {
        preview.querySelector("[data-preview-meta]").textContent = errorMessage(error);
      }
    }
  }
  root.querySelector("[data-pdf-previous-page]").addEventListener("click", () => renderer.previousPage());
  root.querySelector("[data-pdf-next-page]").addEventListener("click", () => renderer.nextPage());
  root.querySelector("[data-pdf-zoom-in]").addEventListener("click", () => renderer.setZoom(renderer.zoom + .2));
  root.querySelector("[data-pdf-zoom-out]").addEventListener("click", () => renderer.setZoom(renderer.zoom - .2));

  function close() {
    if (!root.open) return;
    activeInspectorKey = null;
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
    // Pinned mode replaces the preview; retain its authorized in-memory bytes.
    window.clearTimeout(previewTimer);
    window.clearTimeout(previewCloseTimer);
    ++previewGeneration;
    if (preview) { preview.hidden = true; preview.classList.remove("is-open"); }
    previewRenderer?.destroy();
    const page = Number.parseInt(button.getAttribute("data-source-page") || "1", 10);
    const key = sourceKey(button,url);
    activeInspectorKey = key;
    url.hash = "page=" + (Number.isInteger(page) && page > 0 && page <= 10000 ? page : 1);
    window.clearTimeout(closeTimer);
    returnFocus = button;
    title.textContent = button.getAttribute("data-source-label") || "Original PDF";
    locator.textContent = button.getAttribute("data-source-locator") || "Check the source page.";
    external.href = url.href;
    renderer.destroy();
    setStatus("Loading verified original…");
    if (!root.open) { if (mobileViewport()) root.showModal(); else root.show(); }
    window.requestAnimationFrame(() => root.classList.add("is-open"));
    closeButton.focus({preventScroll:true});
    try {
      const resource = await evidenceSession.acquire({key, url:url.href});
      if (!root.open || key !== activeInspectorKey || resource.key !== key) return;
      await renderer.open(resource.objectUrl, Number.isInteger(page) && page > 0 && page <= 10000 ? page : 1);
      if (key === activeInspectorKey) setStatus("");
    } catch (error) {
      if (error.name === "AbortError" || key !== activeInspectorKey) return;
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
  // Delegation survives HTMX replacement of evidence cards and inspector controls.
  const fromCard = (target) => target?.closest?.("[data-evidence-card]");
  const startPreviewIntent = (card) => {
    const button = card?.querySelector("[data-source-inspect]");
    if (!button) return;
    window.clearTimeout(previewCloseTimer);
    window.clearTimeout(previewTimer);
    previewTimer = window.setTimeout(() => openPreview(button), 180);
  };
  const schedulePreviewClose = () => {
    window.clearTimeout(previewTimer);
    window.clearTimeout(previewCloseTimer);
    previewCloseTimer = window.setTimeout(closePreview, 250);
  };
  document.addEventListener("pointerover", (event) => {
    if (!window.matchMedia("(hover:hover) and (pointer:fine)").matches) return;
    const card = fromCard(event.target);
    if (card && !card.contains(event.relatedTarget)) startPreviewIntent(card);
  });
  document.addEventListener("pointerout", (event) => {
    const card = fromCard(event.target);
    if (card && !card.contains(event.relatedTarget)) schedulePreviewClose();
  });
  document.addEventListener("focusin", (event) => {
    const card = fromCard(event.target);
    if (card && !card.contains(event.relatedTarget)) startPreviewIntent(card);
  });
  document.addEventListener("focusout", (event) => {
    const card = fromCard(event.target);
    if (card && !card.contains(event.relatedTarget)) schedulePreviewClose();
  });
  preview?.addEventListener("pointerenter", () => window.clearTimeout(previewCloseTimer));
  preview?.addEventListener("pointerleave", () => { previewCloseTimer = window.setTimeout(closePreview, 250); });
  // Keyboard users can move focus from an evidence card into its preview.
  preview?.addEventListener("focusin", () => window.clearTimeout(previewCloseTimer));
  preview?.addEventListener("focusout", (event) => {
    if (!preview.contains(event.relatedTarget)) schedulePreviewClose();
  });
  preview?.querySelector("[data-preview-pin]")?.addEventListener("click", () => { if (previewButton) { closePreview(); open(previewButton); } });
  document.addEventListener("keydown", (event) => { if (event.key === "Escape" && preview && !preview.hidden) { event.preventDefault(); closePreview(); } });
  root.addEventListener("cancel", (event) => { event.preventDefault(); close(); });
  const discardSourceState = () => {
    ++previewGeneration;
    previewButton = null;
    window.clearTimeout(previewTimer);
    window.clearTimeout(previewCloseTimer);
    window.clearTimeout(closeTimer);
    if (preview) { preview.hidden = true; preview.classList.remove("is-open"); }
    previewRenderer?.destroy();
    if (root.open) root.close();
    root.classList.remove("is-open");
    renderer.destroy();
    evidenceSession.release();
    activeInspectorKey = null;
    returnFocus = null;
  };
  // Never reuse an inspected source after a refreshed evidence generation.
  document.body.addEventListener("htmx:beforeSwap", discardSourceState);
  window.addEventListener("pagehide", discardSourceState);
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") discardSourceState();
  });
  const currentApplicability = document.querySelector("[data-applicability-preview]");
  if (currentApplicability) {
    new MutationObserver((mutations) => {
      if (mutations.some((m) => m.attributeName === "data-source-revision")) discardSourceState();
    }).observe(currentApplicability, {attributes:true, attributeFilter:["data-source-revision"]});
  }
})();
