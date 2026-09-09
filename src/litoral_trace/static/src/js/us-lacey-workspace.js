// U.S. Lacey review UX helpers. Review mutations are server-rendered HTMX
// transitions: no client-side business state is authoritative.

let pendingReviewTransition = null;

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

document.addEventListener("htmx:beforeRequest", (event) => {
  const form = reviewFormFromEvent(event);
  if (!form?.matches?.("[data-review-action]")) return;

  const card = form.closest("[data-review-field]");
  pendingReviewTransition = {
    targetId: card?.id || "review-field-list",
    order: card ? Number(card.dataset.reviewOrder || "1") : 1,
    bulk: form.hasAttribute("data-review-bulk"),
  };
});

document.addEventListener("htmx:afterSwap", (event) => {
  if (!pendingReviewTransition) return;

  // Out-of-band swaps update the summary/banner/final confirmation separately.
  // Only the primary card/list swap may advance the analyst to another field.
  const swappedTargetId = event.detail?.target?.id || "";
  if (swappedTargetId !== pendingReviewTransition.targetId) return;

  const transition = pendingReviewTransition;
  pendingReviewTransition = null;
  const openCards = pendingReviewCards();
  if (!openCards.length) return;

  const next = transition.bulk
    ? openCards[0]
    : openCards.find(
        (card) => Number(card.dataset.reviewOrder || "0") >= transition.order
      ) || openCards[0];

  // Enterprise review flows should remain visually still whenever the next task is
  // already on screen. Scroll only when navigation is actually necessary.
  if (!next || isVisibleInViewport(next)) return;

  window.requestAnimationFrame(() => {
    next.scrollIntoView({ behavior: "smooth", block: "nearest" });
    const input = next.querySelector("input:not([type='hidden']), textarea, select, button");
    input?.focus?.({ preventScroll: true });
  });
});
