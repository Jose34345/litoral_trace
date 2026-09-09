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
