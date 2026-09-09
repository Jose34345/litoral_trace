// U.S. Lacey review UX helpers. Review mutations are server-rendered HTMX
// transitions: no client-side business state is authoritative.

let pendingReviewTransition = null;

const restoreViewport = (x, y) => {
  window.scrollTo({ left: x, top: y, behavior: "instant" });
  window.requestAnimationFrame(() => {
    window.scrollTo({ left: x, top: y, behavior: "instant" });
  });
};

const reviewFormFromEvent = (event) => {
  const element = event.detail?.elt;
  if (element instanceof HTMLFormElement) return element;
  return element?.closest?.("form[data-review-action]") || null;
};

document.addEventListener("htmx:beforeRequest", (event) => {
  const form = reviewFormFromEvent(event);
  if (!form?.matches?.("[data-review-action]")) return;
  const card = form.closest("[data-review-field]");
  pendingReviewTransition = {
    order: card ? Number(card.dataset.reviewOrder || "1") : 1,
    x: window.scrollX,
    y: window.scrollY,
  };
});

document.addEventListener("htmx:afterSwap", (event) => {
  const workspace = document.querySelector("#operation-workspace");
  if (!workspace) return;

  if (pendingReviewTransition) {
    const openCards = [...workspace.querySelectorAll(
      '[data-review-field][data-review-status="MISSING"], ' +
      '[data-review-field][data-review-status="REVIEW"], ' +
      '[data-review-field][data-review-status="FOUND"]'
    )];
    const next = openCards.find(
      (card) => Number(card.dataset.reviewOrder || "0") >= pendingReviewTransition.order
    );
    const viewport = pendingReviewTransition;
    pendingReviewTransition = null;

    if (next) {
      window.requestAnimationFrame(() => {
        next.scrollIntoView({ behavior: "smooth", block: "center" });
        const input = next.querySelector("input:not([type='hidden']), textarea, select, button");
        input?.focus?.({ preventScroll: true });
      });
      return;
    }

    // No next review item exists. Keep the analyst exactly where the completed
    // action left them instead of snapping to the workspace header.
    restoreViewport(viewport.x, viewport.y);
    return;
  }

  // Processing/workspace refreshes that were not review actions remain stable.
  if (event.detail?.target?.id === "operation-workspace") {
    workspace.querySelector("#analysis-complete")?.focus?.({ preventScroll: true });
  }
});
