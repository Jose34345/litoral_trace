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

/* Warm one local PDF.js module while the reviewer is idle. No source bytes are prefetched.
   Loading the module and fetching authorized bytes run concurrently on first inspection. */
let pdfModulePromise = null;
function loadPdfModule() {
  if (!pdfModulePromise) {
    pdfModulePromise = import("/static/vendor/pdfjs/pdf.min.mjs").then((pdfjs) => {
      pdfjs.GlobalWorkerOptions.workerSrc = "/static/vendor/pdfjs/pdf.worker.min.mjs";
      return pdfjs;
    }).catch((error) => { pdfModulePromise = null; throw error; });
  }
  return pdfModulePromise;
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
    this.viewportContainer = canvas.parentElement;
  }
  async open(objectUrl, page = 1) {
    this.destroy();
    const generation = this.generation;
    const pdfjs = await loadPdfModule();
    if (generation !== this.generation) return;
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
      const natural = pdfPage.getViewport({scale:1});
      // A fitted page is more useful than a gigantic canvas clipped on both sides.
      // When no container dimensions are available (e.g. deterministic JS tests),
      // preserve the natural PDF scale exactly.
      const available = this.viewportContainer?.clientWidth || 0;
      const fitted = available > 0
        ? Math.max(.02, Math.min(1.65, (available - 40) / Math.max(1, natural.width)))
        : 1;
      const viewport = pdfPage.getViewport({scale:fitted * requestedZoom});
      const context = this.canvas.getContext("2d", {alpha:false});
      // Cap backing-store pixels to prevent frozen scroll / GPU exhaustion on
      // high DPI, scanned PDFs or multi-page documents.
      const deviceScale = Math.max(1, Math.min(1.6, (typeof window === "undefined" ? 1 : window.devicePixelRatio) || 1));
      const maxPixelScale = Math.sqrt(12_000_000 / Math.max(1, viewport.width * viewport.height));
      const rasterScale = available > 0 ? Math.min(deviceScale, maxPixelScale) : 1;
      const scale = Math.max(.1, rasterScale);
      this.canvas.width = Math.max(1, Math.ceil(viewport.width * scale));
      this.canvas.height = Math.max(1, Math.ceil(viewport.height * scale));
      if (this.canvas.style) {
        this.canvas.style.width = `${Math.round(viewport.width)}px`;
        this.canvas.style.height = `${Math.round(viewport.height)}px`;
      }
      const task = pdfPage.render({canvasContext:context, viewport,
        transform:[scale, 0, 0, scale, 0, 0]});
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
    if (this.canvas.style) { this.canvas.style.width = ""; this.canvas.style.height = ""; }
  }
}

/* Native <dialog> ensures inert background/focus containment (W3C WAI-ARIA). */
(() => {
  const root = document.querySelector("[data-source-inspector]");
  if (!root || window.__ltSourceInspectorBound) return;
  window.__ltSourceInspectorBound = true;
  // Portaling both layers to <body> isolates them from scroll containers,
  // HTMX-transformed ancestors and sticky navigation stacking contexts.
  const previewPortal = document.querySelector("[data-evidence-preview]");
  if (root.parentElement !== document.body) document.body.appendChild(root);
  if (previewPortal && previewPortal.parentElement !== document.body) document.body.appendChild(previewPortal);
  if (document.querySelector("[data-source-inspect]")) {
    const warm = () => void loadPdfModule().catch(() => {});
    if ("requestIdleCallback" in window) window.requestIdleCallback(warm, {timeout:3500});
    else window.setTimeout(warm, 900);
  }
  const smartEntry = document.querySelector("[data-smart-entry-selector]");

  function bindSmartEntrySelector() {
    if (!smartEntry) return;
    const nativeSelect = smartEntry.querySelector("[data-entry-type]");
    const search = smartEntry.querySelector("[data-entry-search]");
    const current = smartEntry.querySelector("[data-entry-current]");
    const options = Array.from(smartEntry.querySelectorAll("[data-entry-option]"));
    // Native select is the single, accessible entry-type control; the former
    // duplicated searchable list is intentionally no longer rendered.
    if (!nativeSelect || !search || !current || options.length === 0) return;
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
  const preview = previewPortal;
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
    preview.hidden = true;
    previewRenderer?.destroy();
    if (!root.open) evidenceSession.release();
  }
  function positionPreview() {
    // Stay fixed to the viewport, not to a document row that may scroll away.
    // Placement and size are owned by responsive CSS.
    preview.style.removeProperty("top");
    preview.style.removeProperty("left");
  }
  async function openPreview(button) {
    if (mobileViewport() || root.open || !preview || !previewRenderer) return;
    const url = sourceUrl(button.getAttribute("data-source-url")); if (!url) return;
    const previewRequest = ++previewGeneration;
    window.clearTimeout(previewCloseTimer);
    previewButton = button;
    positionPreview();
    preview.hidden = false;
    preview.dataset.viewerState = "loading";
    preview.querySelector("[data-preview-title]").textContent = button.getAttribute("data-source-label") || "Original PDF";
    preview.querySelector("[data-preview-meta]").textContent = "Loading verified original…";
    window.requestAnimationFrame(() => { if (previewRequest === previewGeneration) preview.classList.add("is-open"); });
    try {
      const page = Number.parseInt(button.getAttribute("data-source-page") || "1", 10) || 1;
      const [resource] = await Promise.all([
        evidenceSession.acquire({key:sourceKey(button,url), url:url.href}),
        loadPdfModule(),
      ]);
      if (previewRequest !== previewGeneration || preview.hidden) return;
      await previewRenderer.open(resource.objectUrl, page);
      if (previewRequest !== previewGeneration || preview.hidden) return;
      preview.querySelector("[data-preview-meta]").textContent = `Page ${previewRenderer.page} · ${button.getAttribute("data-source-locator") || "Original evidence"}`;
      preview.dataset.viewerState = "ready";
    } catch (error) {
      if (previewRequest === previewGeneration && error.name !== "AbortError") {
        preview.querySelector("[data-preview-meta]").textContent = errorMessage(error);
        preview.dataset.viewerState = "error";
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
    document.querySelector(".lt-app-shell")?.removeAttribute("data-workbench-inspector-open");
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
    root.dataset.viewerState = "loading";
    setStatus("Loading verified original…");
    if (!root.open) { if (mobileViewport()) root.showModal(); else root.show(); }
    if (!mobileViewport()) document.querySelector(".lt-app-shell")?.setAttribute("data-workbench-inspector-open", "true");
    window.requestAnimationFrame(() => root.classList.add("is-open"));
    closeButton.focus({preventScroll:true});
    try {
      const [resource] = await Promise.all([
        evidenceSession.acquire({key, url:url.href}),
        loadPdfModule(),
      ]);
      if (!root.open || key !== activeInspectorKey || resource.key !== key) return;
      await renderer.open(resource.objectUrl, Number.isInteger(page) && page > 0 && page <= 10000 ? page : 1);
      if (key === activeInspectorKey && root.open) {
        root.dataset.viewerState = "ready";
        setStatus("");
      }
    } catch (error) {
      if (error.name === "AbortError" || key !== activeInspectorKey) return;
      renderer.destroy();
      root.dataset.viewerState = "error";
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
  const hoverSurface = (target) => target?.closest?.("[data-evidence-card]")
    || target?.closest?.("[data-source-inspect]");
  const surfaceButton = (surface) => surface?.matches("[data-source-inspect]")
    ? surface : surface?.querySelector("[data-source-inspect]");
  const startPreviewIntent = (button) => {
    if (!button || root.open) return;
    // Moving between elements inside the same card is not a new hover intent.
    if (button === previewButton && preview && !preview.hidden) return;
    window.clearTimeout(previewCloseTimer);
    window.clearTimeout(previewTimer);
    previewTimer = window.setTimeout(() => openPreview(button), 220);
  };
  const schedulePreviewClose = () => {
    window.clearTimeout(previewTimer);
    window.clearTimeout(previewCloseTimer);
    previewCloseTimer = window.setTimeout(closePreview, 400);
  };
  document.addEventListener("pointerover", (event) => {
    if (!window.matchMedia("(hover:hover) and (pointer:fine)").matches) return;
    const surface = hoverSurface(event.target);
    if (surface && !surface.contains(event.relatedTarget)) startPreviewIntent(surfaceButton(surface));
  });
  document.addEventListener("pointerout", (event) => {
    const surface = hoverSurface(event.target);
    if (surface && !surface.contains(event.relatedTarget)) schedulePreviewClose();
  });
  document.addEventListener("focusin", (event) => {
    const surface = hoverSurface(event.target);
    if (surface && !surface.contains(event.relatedTarget)) startPreviewIntent(surfaceButton(surface));
  });
  document.addEventListener("focusout", (event) => {
    const surface = hoverSurface(event.target);
    if (surface && !surface.contains(event.relatedTarget)) schedulePreviewClose();
  });
  preview?.addEventListener("pointerenter", () => window.clearTimeout(previewCloseTimer));
  preview?.addEventListener("pointerleave", () => { previewCloseTimer = window.setTimeout(closePreview, 250); });
  // Keyboard users can move focus from an evidence card into its preview.
  preview?.addEventListener("focusin", () => window.clearTimeout(previewCloseTimer));
  preview?.addEventListener("focusout", (event) => {
    if (!preview.contains(event.relatedTarget)) schedulePreviewClose();
  });
  preview?.querySelector("[data-preview-pin]")?.addEventListener("click", () => { if (previewButton) { closePreview(); open(previewButton); } });
  preview?.querySelector("[data-preview-close]")?.addEventListener("click", closePreview);
  // Page scrolling does not cancel a pinned inspector. Lightweight hover
  // previews close on navigation, never by destroying the pinned PDF.
  window.addEventListener("scroll", () => { if (!root.open) closePreview(); }, {passive:true});
  document.addEventListener("keydown", (event) => { if (event.key === "Escape" && preview && !preview.hidden) { event.preventDefault(); closePreview(); } });
  root.addEventListener("cancel", (event) => { event.preventDefault(); close(); });
  const discardSourceState = () => {
    document.querySelector(".lt-app-shell")?.removeAttribute("data-workbench-inspector-open");
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
  document.body.addEventListener("htmx:beforeSwap", (event) => {
    const target = event.detail?.target;
    if (!(target instanceof Element)) return;
    const sourceScope = "#operation-workspace, #applicability, #documents, [data-applicability-preview], [data-evidence-card]";
    if (target.matches(sourceScope) || target.querySelector(sourceScope)) discardSourceState();
    // Status polling and unrelated HTMX swaps must not erase a pinned PDF.
  });
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
