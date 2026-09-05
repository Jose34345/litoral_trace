// Deliberately scoped to the U.S. Lacey HTMX workspace replacement. It neither
// wraps fetch nor scrolls a person away from where they are reading.
document.addEventListener("htmx:afterSwap", (event) => {
  const workspace = event.detail?.target?.id === "operation-workspace"
    ? event.detail.target
    : event.detail?.target?.querySelector?.("#operation-workspace");
  const heading = workspace?.querySelector?.("#analysis-complete");
  if (heading && typeof heading.focus === "function") {
    heading.focus({ preventScroll: true });
  }
});
