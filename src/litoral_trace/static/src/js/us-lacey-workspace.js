// U.S. Lacey review UX helpers. Review mutations are server-rendered HTMX
// transitions: no client-side business state is authoritative.

let pendingReviewTransition = null;
let lastXmlPreviewTrigger = null;

const reviewFormFromEvent = (event) => {
  const element = event.detail?.elt;
  if (element instanceof HTMLFormElement) {
    if (element.matches("[data-review-action], [data-review-bulk]")) return element;
  }
  return element?.closest?.("form[data-review-action], form[data-review-bulk]") || null;
};

const isVisibleInViewport = (element) => {
  const rect = element.getBoundingClientRect();
  const viewportHeight = window.innerHeight || document.documentElement.clientHeight;
  const viewportWidth = window.innerWidth || document.documentElement.clientWidth;
  return rect.bottom > 0 && rect.top < viewportHeight && rect.right > 0 && rect.left < viewportWidth;
};

const pendingReviewCards = () => [
  ...document.querySelectorAll("#action-required-fields [data-review-field]"),
];

const reviewInputForCard = (card) => card?.querySelector?.("[data-review-entry-input]") || null;

const focusReviewCard = (card) => {
  if (!card) return;
  const input = reviewInputForCard(card);
  if (!input) return;

  if (isVisibleInViewport(card)) {
    input.focus({ preventScroll: true });
    return;
  }

  card.scrollIntoView({ behavior: "smooth", block: "center" });
  window.setTimeout(() => input.focus({ preventScroll: true }), 180);
};

const toastRegion = () => document.getElementById("lt-toast-region");

const showToast = (message, tone = "success", duration = 4000) => {
  const region = toastRegion();
  if (!region || !message) return;

  const toast = document.createElement("div");
  toast.className = `lt-toast lt-toast--${tone}`;
  toast.setAttribute("role", tone === "danger" ? "alert" : "status");
  toast.style.setProperty("--lt-toast-duration", `${duration}ms`);

  const icon = document.createElement("i");
  icon.className = `fa-solid lt-toast__icon ${tone === "danger" ? "fa-circle-exclamation" : tone === "info" ? "fa-circle-info" : "fa-circle-check"}`;
  icon.setAttribute("aria-hidden", "true");

  const copy = document.createElement("div");
  copy.className = "lt-toast__message";
  copy.textContent = message;

  const close = document.createElement("button");
  close.type = "button";
  close.className = "lt-toast__close";
  close.setAttribute("aria-label", "Dismiss notification");
  close.innerHTML = '<i class="fa-solid fa-xmark" aria-hidden="true"></i>';

  const progress = document.createElement("span");
  progress.className = "lt-toast__progress";
  progress.setAttribute("aria-hidden", "true");

  toast.append(icon, copy, close, progress);
  region.appendChild(toast);

  let removed = false;
  const remove = () => {
    if (removed) return;
    removed = true;
    toast.classList.add("is-leaving");
    window.setTimeout(() => toast.remove(), 190);
  };
  close.addEventListener("click", remove);
  window.setTimeout(remove, duration);
};

window.LitoralTraceToast = showToast;

const scopedNodes = (scope, selector) => {
  const nodes = [...(scope?.querySelectorAll?.(selector) || [])];
  if (scope?.matches?.(selector)) nodes.unshift(scope);
  return nodes;
};

const hydrateBootstrapToasts = (scope = document) => {
  scopedNodes(scope, "[data-toast-bootstrap], [data-toast-success]").forEach((node) => {
    if (node.dataset.toastHydrated === "true") return;
    node.dataset.toastHydrated = "true";
    const message = node.dataset.toastBootstrap || node.dataset.toastSuccess || "";
    const tone = node.dataset.toastTone || "success";
    if (message) showToast(message, tone);
  });
};

const initializeAliasEditor = (scope = document) => {
  scopedNodes(scope, "[data-operation-alias-shell]").forEach((shell) => {
    if (shell.dataset.aliasBound === "true") return;
    shell.dataset.aliasBound = "true";

    const display = shell.querySelector("[data-operation-alias-display]");
    const edit = shell.querySelector("[data-operation-alias-edit]");
    const form = shell.querySelector("[data-operation-alias-form]");
    const input = shell.querySelector("[data-operation-alias-input]");
    const cancel = shell.querySelector("[data-operation-alias-cancel]");
    if (!display || !edit || !form || !input) return;

    const open = () => {
      display.hidden = true;
      form.hidden = false;
      edit.setAttribute("aria-expanded", "true");
      window.requestAnimationFrame(() => {
        input.focus({ preventScroll: true });
        input.select?.();
      });
    };

    const close = () => {
      form.hidden = true;
      display.hidden = false;
      edit.setAttribute("aria-expanded", "false");
      edit.focus({ preventScroll: true });
    };

    edit.addEventListener("click", open);
    cancel?.addEventListener("click", close);
    form.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        event.preventDefault();
        close();
      }
    });

    if (shell.querySelector("[data-inline-field-error]")) open();
  });
};

const escapeHtml = (value) => String(value)
  .replaceAll("&", "&amp;")
  .replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;")
  .replaceAll("'", "&#039;");

const highlightXml = (xml) => String(xml)
  .split(/(<[^>]+>)/g)
  .filter((part) => part !== "")
  .map((part) => (
    part.startsWith("<")
      ? `<span class="lt-xml-token-tag">${escapeHtml(part)}</span>`
      : `<span class="lt-xml-token-text">${escapeHtml(part)}</span>`
  ))
  .join("");

const modalFocusable = (modal) => [
  ...modal.querySelectorAll(
    'button:not([disabled]), a[href], [tabindex]:not([tabindex="-1"])'
  ),
].filter((element) => !element.hidden && element.getAttribute("aria-hidden") !== "true");

const closeXmlPreview = (modal) => {
  if (!modal || modal.hidden) return;
  modal.classList.remove("is-open");
  document.documentElement.classList.remove("overflow-hidden");
  window.setTimeout(() => {
    modal.hidden = true;
    lastXmlPreviewTrigger?.focus?.({ preventScroll: true });
  }, 180);
};

const openXmlPreview = async (trigger) => {
  const modal = document.querySelector("[data-xml-preview-modal]");
  if (!modal) return;
  lastXmlPreviewTrigger = trigger;

  const loading = modal.querySelector("[data-xml-preview-loading]");
  const code = modal.querySelector("[data-xml-preview-code]");
  const codeNode = code?.querySelector("code");
  const error = modal.querySelector("[data-xml-preview-error]");
  const copy = modal.querySelector("[data-xml-copy]");

  modal.hidden = false;
  document.documentElement.classList.add("overflow-hidden");
  loading.hidden = false;
  code.hidden = true;
  error.hidden = true;
  copy.disabled = true;
  modal.__xmlText = "";

  window.requestAnimationFrame(() => {
    modal.classList.add("is-open");
    modal.querySelector("[data-xml-preview-close]:not(.lt-xml-modal__backdrop)")?.focus?.({
      preventScroll: true,
    });
  });

  try {
    const response = await fetch(trigger.dataset.xmlPreviewUrl, {
      method: "GET",
      credentials: "same-origin",
      headers: { Accept: "application/xml, text/xml;q=0.9" },
    });
    if (!response.ok) throw new Error("The XML preview could not be generated.");
    const xml = await response.text();
    modal.__xmlText = xml;
    codeNode.innerHTML = highlightXml(xml);
    loading.hidden = true;
    code.hidden = false;
    copy.disabled = false;
  } catch (previewError) {
    loading.hidden = true;
    error.textContent = previewError?.message || "The XML preview could not be generated.";
    error.hidden = false;
  }
};

const initializeXmlPreview = (scope = document) => {
  scopedNodes(scope, "[data-xml-preview-trigger]").forEach((trigger) => {
    if (trigger.dataset.xmlPreviewBound === "true") return;
    trigger.dataset.xmlPreviewBound = "true";
    trigger.addEventListener("click", () => openXmlPreview(trigger));
  });

  scopedNodes(scope, "[data-xml-preview-modal]").forEach((modal) => {
    if (modal.dataset.xmlModalBound === "true") return;
    modal.dataset.xmlModalBound = "true";

    modal.querySelectorAll("[data-xml-preview-close]").forEach((close) => {
      close.addEventListener("click", () => closeXmlPreview(modal));
    });

    modal.querySelector("[data-xml-copy]")?.addEventListener("click", async () => {
      if (!modal.__xmlText) return;
      try {
        await navigator.clipboard.writeText(modal.__xmlText);
        showToast("LAWGS XML copied to clipboard.");
      } catch (_) {
        showToast("Copy failed. Select the XML and copy it manually.", "danger");
      }
    });

    modal.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        event.preventDefault();
        closeXmlPreview(modal);
        return;
      }
      if (event.key !== "Tab") return;
      const focusable = modalFocusable(modal);
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    });
  });
};

const initializeFastFollowUx = (scope = document) => {
  hydrateBootstrapToasts(scope);
  initializeAliasEditor(scope);
  initializeXmlPreview(scope);
};

const resetExportDownload = (link) => {
  if (!(link instanceof HTMLAnchorElement)) return;

  link.classList.remove("is-loading");
  link.removeAttribute("aria-busy");
  link.dataset.exportBusy = "false";

  const label = link.querySelector("[data-export-label]");
  if (label && link.dataset.defaultLabel) {
    label.textContent = link.dataset.defaultLabel;
  }
};

const beginExportDownload = (link, event) => {
  if (!(link instanceof HTMLAnchorElement)) return;

  if (link.dataset.exportBusy === "true") {
    event.preventDefault();
    return;
  }

  const label = link.querySelector("[data-export-label]");
  if (label && !link.dataset.defaultLabel) {
    link.dataset.defaultLabel = label.textContent.trim();
  }

  link.dataset.exportBusy = "true";
  link.classList.add("is-loading");
  link.setAttribute("aria-busy", "true");
  if (label) {
    label.textContent = link.dataset.loadingLabel || "Generating...";
  }

  window.setTimeout(() => resetExportDownload(link), 1200);
};

document.addEventListener("click", (event) => {
  const exportLink = event.target.closest?.("a[data-export-download]");
  if (exportLink) {
    beginExportDownload(exportLink, event);
    return;
  }

  const nextRequired = event.target.closest?.("[data-focus-next-required]");
  if (nextRequired) {
    event.preventDefault();
    focusReviewCard(pendingReviewCards()[0]);
  }
});

window.addEventListener("pageshow", () => {
  document.querySelectorAll("a[data-export-download].is-loading").forEach(resetExportDownload);
});

document.addEventListener("keydown", (event) => {
  const input = event.target.closest?.("[data-review-entry-input]");
  if (!input) return;

  const form = input.closest("form[data-review-action]");
  if (event.key === "Enter" && !event.shiftKey && !event.ctrlKey && !event.metaKey && !event.altKey) {
    event.preventDefault();
    form?.requestSubmit?.();
    return;
  }

  if (event.key !== "Tab" || event.ctrlKey || event.metaKey || event.altKey) return;
  const inputs = [
    ...document.querySelectorAll("#action-required-fields [data-review-entry-input]"),
  ].filter((candidate) => !candidate.disabled && !candidate.hidden);
  const index = inputs.indexOf(input);
  if (index < 0) return;
  const nextIndex = event.shiftKey ? index - 1 : index + 1;
  if (nextIndex < 0 || nextIndex >= inputs.length) return;

  event.preventDefault();
  const next = inputs[nextIndex];
  next.closest("[data-review-field]")?.scrollIntoView({
    behavior: "smooth",
    block: "center",
  });
  window.setTimeout(() => next.focus({ preventScroll: true }), 140);
});

document.addEventListener("htmx:beforeRequest", (event) => {
  const form = reviewFormFromEvent(event);
  if (!form) return;

  const card = form.closest("[data-review-field]");
  const cards = pendingReviewCards();
  const fieldId = card?.dataset.reviewFieldId || null;
  pendingReviewTransition = {
    bulk: form.hasAttribute("data-review-bulk"),
    kind: form.dataset.reviewActionKind || (form.hasAttribute("data-review-bulk") ? "bulk" : "review"),
    fieldId,
    previousActionCount: cards.length,
    previousIndex: card ? cards.indexOf(card) : 0,
  };
});

document.addEventListener("htmx:afterSwap", (event) => {
  initializeFastFollowUx(event.target || document);

  const target = event.detail?.target || event.target;
  if (target?.id !== "operation-workspace") return;
  if (!pendingReviewTransition) return;

  const transition = pendingReviewTransition;
  pendingReviewTransition = null;

  const fieldCard = transition.fieldId
    ? target.querySelector(`[data-review-field-id="${transition.fieldId}"]`)
    : null;
  const inlineError = fieldCard?.querySelector("[data-inline-field-error]");
  if (inlineError) {
    fieldCard.classList.remove("lt-validation-shake");
    void fieldCard.offsetWidth;
    fieldCard.classList.add("lt-validation-shake");
    focusReviewCard(fieldCard);
    return;
  }

  if (transition.bulk) {
    showToast("Auto-resolved fields confirmed.");
  } else if (transition.kind === "not-required") {
    showToast("Field marked not required.");
  } else {
    showToast("Review update saved.");
  }

  const cards = pendingReviewCards();
  if (transition.previousActionCount > 0 && cards.length === 0) {
    window.setTimeout(() => {
      document.getElementById("declaration-package")?.scrollIntoView({
        behavior: "smooth",
        block: "start",
      });
    }, 120);
    return;
  }

  if (cards.length) {
    const index = Math.max(0, Math.min(transition.previousIndex, cards.length - 1));
    window.requestAnimationFrame(() => focusReviewCard(cards[index]));
  }
});

document.addEventListener("htmx:responseError", () => {
  pendingReviewTransition = null;
  showToast("That update could not be saved. Please try again.", "danger");
});

let reviewTelemetry = null;

const reviewTelemetryKey = (operationId) => `lt:lacey-review:${operationId}`;

const sessionRead = (key) => {
  try {
    return window.sessionStorage.getItem(key);
  } catch (_) {
    return null;
  }
};

const sessionWrite = (key, value) => {
  try {
    window.sessionStorage.setItem(key, value);
  } catch (_) {
    // Telemetry is non-authoritative and must never block review.
  }
};

const sessionRemove = (key) => {
  try {
    window.sessionStorage.removeItem(key);
  } catch (_) {
    // Telemetry is non-authoritative and must never block review.
  }
};

const loadReviewTelemetry = () => {
  const workspace = document.getElementById("operation-workspace");
  if (!workspace) return null;

  const operationId = workspace.dataset.operationId;
  if (!operationId) return null;

  const key = reviewTelemetryKey(operationId);
  if (workspace.dataset.reviewCompleted === "true") {
    sessionRemove(key);
    return null;
  }

  let state = null;
  try {
    state = JSON.parse(sessionRead(key) || "null");
  } catch (_) {
    state = null;
  }

  if (!state?.startedAt) {
    state = {
      startedAt: new Date().toISOString(),
      modifiedFieldIds: [],
    };
    sessionWrite(key, JSON.stringify(state));
  }

  return {
    operationId,
    key,
    startedAt: state.startedAt,
    modifiedFieldIds: new Set(state.modifiedFieldIds || []),
  };
};

const ensureReviewTelemetry = () => {
  const workspace = document.getElementById("operation-workspace");
  const operationId = workspace?.dataset?.operationId;
  if (!operationId) return null;

  if (!reviewTelemetry || reviewTelemetry.operationId !== operationId) {
    reviewTelemetry = loadReviewTelemetry();
  }
  return reviewTelemetry;
};

const persistReviewTelemetry = () => {
  const state = ensureReviewTelemetry();
  if (!state) return;
  sessionWrite(state.key, JSON.stringify({
    startedAt: state.startedAt,
    modifiedFieldIds: [...state.modifiedFieldIds],
  }));
};

const hydrateReviewCompletionForm = (form) => {
  const state = ensureReviewTelemetry();
  if (!state) return;

  const startedMs = Date.parse(state.startedAt);
  const elapsedSeconds = Number.isFinite(startedMs)
    ? Math.max(0, Math.round((Date.now() - startedMs) / 1000))
    : "";

  const startedInput = form.querySelector("[data-review-started-at]");
  const elapsedInput = form.querySelector("[data-review-elapsed-seconds]");
  const modifiedInput = form.querySelector("[data-review-modified-field-ids]");

  if (startedInput) startedInput.value = state.startedAt;
  if (elapsedInput) elapsedInput.value = String(elapsedSeconds);
  if (modifiedInput) modifiedInput.value = [...state.modifiedFieldIds].join(",");
};

const initializeReviewTelemetry = () => {
  ensureReviewTelemetry();

  document.querySelectorAll("[data-review-completion-form]").forEach((form) => {
    if (form.dataset.reviewTelemetryBound === "true") return;
    form.addEventListener("submit", () => hydrateReviewCompletionForm(form));
    form.dataset.reviewTelemetryBound = "true";
  });
};

document.addEventListener("input", (event) => {
  const input = event.target.closest?.(
    '[data-review-field][data-review-required="true"] input[name="value"]'
  );
  if (!input) return;

  const card = input.closest("[data-review-field]");
  const fieldId = Number(card?.dataset.reviewFieldId);
  if (!Number.isInteger(fieldId) || fieldId <= 0) return;

  const state = ensureReviewTelemetry();
  if (!state) return;

  state.modifiedFieldIds.add(fieldId);
  persistReviewTelemetry();
});

const initializeWorkspaceUx = (event) => {
  const scope = event?.detail?.elt || event?.target || document;
  initializeReviewTelemetry();
  initializeFastFollowUx(scope);
};

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initializeWorkspaceUx);
} else {
  initializeWorkspaceUx();
}

document.addEventListener("htmx:load", initializeWorkspaceUx);
