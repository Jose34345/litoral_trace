// U.S. Lacey review UX helpers. Processing still uses HTMX polling, while the
// per-field review forms are progressively enhanced here so a confirm/edit action
// never forces a full browser navigation and loses the reader's scroll position.

const REVIEW_FIELD_ACTION = /\/operations\/[0-9a-f-]{36}\/review\/fields\/\d+$/i;

const restoreViewport = (x, y) => {
  // Replacing a large workspace can slightly change layout before the next paint.
  // Restore twice without animation so the viewport stays anchored to the review
  // location rather than jumping to the top of the document.
  window.scrollTo({ left: x, top: y, behavior: "instant" });
  window.requestAnimationFrame(() => {
    window.scrollTo({ left: x, top: y, behavior: "instant" });
  });
};

const inlineReviewError = (card, parsedDocument) => {
  const source = parsedDocument.querySelector(".lt-alert--danger");
  if (!source) return false;
  card.querySelector("[data-review-inline-error]")?.remove();
  const wrapper = document.createElement("div");
  wrapper.dataset.reviewInlineError = "true";
  wrapper.className = "mb-3";
  wrapper.append(source.cloneNode(true));
  card.prepend(wrapper);
  return true;
};

const submitReviewWithoutNavigation = async (form, card) => {
  const viewportX = window.scrollX;
  const viewportY = window.scrollY;
  const submitters = [...form.querySelectorAll("button, input[type='submit']")];
  submitters.forEach((element) => { element.disabled = true; });

  try {
    const response = await fetch(form.action, {
      method: "POST",
      body: new FormData(form),
      credentials: "same-origin",
      headers: {
        "X-Requested-With": "LitoralTrace-Review",
      },
      redirect: "follow",
    });
    const html = await response.text();
    const parsed = new DOMParser().parseFromString(html, "text/html");

    if (!response.ok) {
      inlineReviewError(card, parsed);
      submitters.forEach((element) => { element.disabled = false; });
      restoreViewport(viewportX, viewportY);
      return;
    }

    const freshWorkspace = parsed.querySelector("#operation-workspace");
    const currentWorkspace = document.querySelector("#operation-workspace");
    if (!freshWorkspace || !currentWorkspace) {
      // Progressive-enhancement fallback: use the browser's native form behavior
      // only when the expected server-rendered fragment cannot be recovered.
      HTMLFormElement.prototype.submit.call(form);
      return;
    }

    currentWorkspace.replaceWith(freshWorkspace);
    // The replacement came from DOMParser rather than an HTMX swap; initialize any
    // HTMX attributes that may be present in the fresh server-rendered workspace.
    window.htmx?.process?.(freshWorkspace);
    restoreViewport(viewportX, viewportY);
  } catch (_error) {
    // Network/script failures must not make the review action unusable. Native POST
    // remains the no-JavaScript/failure fallback and preserves the backend contract.
    HTMLFormElement.prototype.submit.call(form);
  }
};

document.addEventListener("submit", (event) => {
  const form = event.target instanceof HTMLFormElement ? event.target : null;
  const card = form?.closest?.("[data-review-field]");
  if (!form || !card || !REVIEW_FIELD_ACTION.test(new URL(form.action, window.location.href).pathname)) {
    return;
  }
  event.preventDefault();
  void submitReviewWithoutNavigation(form, card);
});

// Existing HTMX processing/workspace swaps must also avoid moving focus/scroll.
document.addEventListener("htmx:afterSwap", (event) => {
  const workspace = event.detail?.target?.id === "operation-workspace"
    ? event.detail.target
    : event.detail?.target?.querySelector?.("#operation-workspace");
  const heading = workspace?.querySelector?.("#analysis-complete");
  if (heading && typeof heading.focus === "function") {
    heading.focus({ preventScroll: true });
  }
});
