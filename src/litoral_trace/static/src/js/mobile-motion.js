const REVEAL_SELECTOR = "[data-reveal]";
const AUTO_REVEAL_SELECTOR = "[data-auto-reveal]";
const REVEAL_VISIBLE_CLASS = "lt-reveal-visible";
const REVEAL_REGISTERED_ATTR = "data-reveal-registered";
const REDUCED_MOTION_QUERY = "(prefers-reduced-motion: reduce)";

let revealObserver = null;

function prefersReducedMotion() {
  return window.matchMedia(REDUCED_MOTION_QUERY).matches;
}

function shouldSkipAutoReveal(element) {
  if (!(element instanceof HTMLElement)) {
    return true;
  }

  if (
    element.hidden
    || element.matches("script, style, template, [data-reveal-skip]")
  ) {
    return true;
  }

  return false;
}

function autoRevealCandidates(root) {
  const children = Array.from(root.children).filter(
    (child) => !shouldSkipAutoReveal(child),
  );

  /*
   * Several authenticated pages use one semantic wrapper (`section.space-y-*`)
   * around all their major blocks. Reveal those blocks instead of animating the
   * whole page as one slab, while keeping ordinary multi-root pages unchanged.
   */
  if (
    children.length === 1
    && children[0] instanceof HTMLElement
    && children[0].children.length > 1
    && !children[0].hasAttribute("data-reveal")
  ) {
    return Array.from(children[0].children).filter(
      (child) => !shouldSkipAutoReveal(child),
    );
  }

  return children;
}

function prepareAutoReveal(scope = document) {
  const roots = [];

  if (
    scope instanceof Element
    && scope.matches(AUTO_REVEAL_SELECTOR)
  ) {
    roots.push(scope);
  }

  scope.querySelectorAll?.(AUTO_REVEAL_SELECTOR).forEach(
    (root) => roots.push(root),
  );

  roots.forEach((root) => {
    autoRevealCandidates(root).forEach((child) => {
      if (child.hasAttribute("data-reveal")) {
        return;
      }

      /* Leaflet must keep a stable transformed coordinate system. */
      const containsMap = Boolean(
        child.querySelector("#map, .leaflet-container"),
      );

      child.dataset.reveal = containsMap ? "fade" : "up";
    });
  });
}

function applyStagger(scope = document) {
  const groups = [];

  if (
    scope instanceof Element
    && scope.hasAttribute("data-reveal-stagger")
  ) {
    groups.push(scope);
  }

  scope.querySelectorAll?.("[data-reveal-stagger]").forEach(
    (group) => groups.push(group),
  );

  groups.forEach((group) => {
    const rawStep = Number.parseInt(
      group.dataset.revealStagger || "70",
      10,
    );
    const step = Number.isFinite(rawStep)
      ? Math.min(Math.max(rawStep, 0), 120)
      : 70;

    Array.from(group.children).forEach((child, index) => {
      if (!(child instanceof HTMLElement)) {
        return;
      }

      if (!child.hasAttribute("data-reveal")) {
        child.dataset.reveal = "up";
      }

      const delay = Math.min(index * step, 280);
      child.style.setProperty(
        "--lt-reveal-delay",
        `${delay}ms`,
      );
    });
  });
}

function markVisible(element) {
  element.classList.add(REVEAL_VISIBLE_CLASS);
}

function ensureObserver() {
  if (revealObserver || !("IntersectionObserver" in window)) {
    return revealObserver;
  }

  revealObserver = new IntersectionObserver(
    (entries, observer) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) {
          return;
        }

        markVisible(entry.target);
        observer.unobserve(entry.target);
      });
    },
    {
      root: null,
      rootMargin: "0px 0px -7% 0px",
      threshold: 0.08,
    },
  );

  return revealObserver;
}

function registerRevealNodes(scope = document) {
  prepareAutoReveal(scope);
  applyStagger(scope);

  const nodes = [];

  if (
    scope instanceof Element
    && scope.matches(REVEAL_SELECTOR)
  ) {
    nodes.push(scope);
  }

  scope.querySelectorAll?.(REVEAL_SELECTOR).forEach(
    (node) => nodes.push(node),
  );

  if (nodes.length === 0) {
    return;
  }

  const reducedMotion = prefersReducedMotion();
  const observer = reducedMotion ? null : ensureObserver();

  document.documentElement.classList.add("lt-motion-ready");

  nodes.forEach((node) => {
    if (
      !(node instanceof HTMLElement)
      || node.hasAttribute(REVEAL_REGISTERED_ATTR)
    ) {
      return;
    }

    node.setAttribute(REVEAL_REGISTERED_ATTR, "true");

    if (reducedMotion || !observer) {
      markVisible(node);
      return;
    }

    observer.observe(node);
  });
}

function revealAll() {
  document.querySelectorAll(REVEAL_SELECTOR).forEach(markVisible);
  revealObserver?.disconnect();
  revealObserver = null;
}

function installReducedMotionListener() {
  const media = window.matchMedia(REDUCED_MOTION_QUERY);

  const handleChange = (event) => {
    if (event.matches) {
      revealAll();
      return;
    }

    registerRevealNodes(document);
  };

  if (typeof media.addEventListener === "function") {
    media.addEventListener("change", handleChange);
  }
}

function installHtmxRevealBridge() {
  document.body.addEventListener("htmx:afterSwap", (event) => {
    const target = event.detail?.target;
    registerRevealNodes(target instanceof Element ? target : document);
  });

  document.body.addEventListener("htmx:load", (event) => {
    const target = event.detail?.elt;
    registerRevealNodes(target instanceof Element ? target : document);
  });
}

function bootMotion() {
  registerRevealNodes(document);
  installReducedMotionListener();
  installHtmxRevealBridge();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", bootMotion, { once: true });
} else {
  bootMotion();
}

export {
  registerRevealNodes,
};
