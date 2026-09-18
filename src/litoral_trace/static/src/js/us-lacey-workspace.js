// U.S. Lacey review UX helpers. Review mutations are server-rendered HTMX
// transitions: no client-side business state is authoritative.

let pendingBulkReviewTransition = false;

const reviewFormFromEvent = (event) => {
  const element = event.detail?.elt;
  if (element instanceof HTMLFormElement) return element;
  return element?.closest?.("form[data-review-action]") || null;
};

const isVisibleInViewport = (element) => {
  const rect = element.getBoundingClientRect();
  const viewportHeight = window.innerHeight || document.documentElement.clientHeight;
  const viewportWidth = window.innerWidth || document.documentElement.clientWidth;
  return rect.bottom > 0 && rect.top < viewportHeight && rect.right > 0 && rect.left < viewportWidth;
};

const pendingReviewCards = () => [
  ...document.querySelectorAll("#review-field-list [data-review-field]"),
];

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

  // The first click remains a normal browser navigation to a same-origin
  // Content-Disposition attachment. While that request is being generated,
  // suppress duplicate activations and provide immediate tactile feedback.
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
  if (!exportLink) return;
  beginExportDownload(exportLink, event);
});

window.addEventListener("pageshow", () => {
  document.querySelectorAll("a[data-export-download].is-loading").forEach(resetExportDownload);
});

document.addEventListener("htmx:beforeRequest", (event) => {
  const form = reviewFormFromEvent(event);
  if (!form?.matches?.("[data-review-action]")) return;

  // Individual Confirm / Save / Use this value actions must never move the viewport.
  // Only the explicit bulk action is allowed to advance to the first remaining task.
  pendingBulkReviewTransition = form.hasAttribute("data-review-bulk");
});

document.addEventListener("htmx:afterSwap", (event) => {
  if (!pendingBulkReviewTransition) return;
  if (event.detail?.target?.id !== "review-field-list") return;

  pendingBulkReviewTransition = false;
  const next = pendingReviewCards()[0];
  if (!next) return;

  const input = next.querySelector("input:not([type='hidden']), textarea, select, button");
  if (isVisibleInViewport(next)) {
    input?.focus?.({ preventScroll: true });
    return;
  }

  window.requestAnimationFrame(() => {
    next.scrollIntoView({ behavior: "smooth", block: "nearest" });
    input?.focus?.({ preventScroll: true });
  });
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

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initializeReviewTelemetry);
} else {
  initializeReviewTelemetry();
}

document.addEventListener("htmx:load", initializeReviewTelemetry);
