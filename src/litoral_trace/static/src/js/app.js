const CSRF_META_NAME = "csrf-token";
const CSRF_HEADER_NAME = "X-CSRF-Token";
const SAFE_METHODS = new Set([
  "GET",
  "HEAD",
  "OPTIONS",
  "TRACE",
]);

function readCsrfToken() {
  const meta = document.querySelector(
    `meta[name="${CSRF_META_NAME}"]`,
  );

  return (
    meta?.getAttribute("content")?.trim()
    || ""
  );
}

function installHtmxSameOriginGuard() {
  document.body.addEventListener(
    "htmx:validateUrl",
    (event) => {
      if (!event.detail.sameHost) {
        event.preventDefault();
      }
    },
  );
}

function installHtmxCsrfBridge() {
  document.body.addEventListener(
    "htmx:configRequest",
    (event) => {
      const requestMethod = String(
        event.detail.verb || "GET",
      ).toUpperCase();

      if (SAFE_METHODS.has(requestMethod)) {
        return;
      }

      const token = readCsrfToken();

      if (!token) {
        return;
      }

      event.detail.headers[
        CSRF_HEADER_NAME
      ] = token;
    },
  );
}

function installFetchCsrfBridge() {
  if (
    typeof window.fetch !== "function"
    || window.fetch.__litoralTraceCsrf
  ) {
    return;
  }

  const nativeFetch = window.fetch.bind(
    window,
  );

  const csrfFetch = (
    input,
    init = {},
  ) => {
    const requestMethod = (
      init.method
      || (
        input instanceof Request
          ? input.method
          : "GET"
      )
    ).toUpperCase();

    if (SAFE_METHODS.has(requestMethod)) {
      return nativeFetch(input, init);
    }

    const rawUrl = (
      input instanceof Request
        ? input.url
        : String(input)
    );

    const requestUrl = new URL(
      rawUrl,
      window.location.href,
    );

    if (
      requestUrl.origin
      !== window.location.origin
    ) {
      return nativeFetch(input, init);
    }

    const token = readCsrfToken();

    if (!token) {
      return nativeFetch(input, init);
    }

    const headers = new Headers(
      input instanceof Request
        ? input.headers
        : undefined,
    );

    const initHeaders = new Headers(
      init.headers || undefined,
    );

    initHeaders.forEach(
      (value, key) => {
        headers.set(key, value);
      },
    );

    if (!headers.has(CSRF_HEADER_NAME)) {
      headers.set(
        CSRF_HEADER_NAME,
        token,
      );
    }

    return nativeFetch(
      input,
      {
        ...init,
        headers,
      },
    );
  };

  Object.defineProperty(
    csrfFetch,
    "__litoralTraceCsrf",
    {
      value: true,
      enumerable: false,
    },
  );

  window.fetch = csrfFetch;
}

function getDrawerFocusableElements(drawer) {
  const selector = [
    "a[href]",
    "button:not([disabled])",
    "input:not([disabled])",
    "select:not([disabled])",
    "textarea:not([disabled])",
    '[tabindex]:not([tabindex="-1"])',
  ].join(",");

  return Array.from(
    drawer.querySelectorAll(selector),
  ).filter(
    (element) => (
      !element.hasAttribute("hidden")
      && element.getAttribute("aria-hidden")
        !== "true"
    ),
  );
}

function setDrawerState(
  drawer,
  overlay,
  openButton,
  isOpen,
) {
  const isDesktop = window.matchMedia(
    "(min-width: 1024px)",
  ).matches;

  drawer.dataset.open = (
    isOpen
      ? "true"
      : "false"
  );

  drawer.classList.toggle(
    "-translate-x-full",
    !isOpen,
  );

  overlay.classList.toggle(
    "hidden",
    !isOpen,
  );

  overlay.setAttribute(
    "aria-hidden",
    String(!isOpen),
  );

  openButton.setAttribute(
    "aria-expanded",
    String(isOpen),
  );

  drawer.setAttribute(
    "aria-hidden",
    (
      isDesktop || isOpen
        ? "false"
        : "true"
    ),
  );

  document.documentElement.classList.toggle(
    "overflow-hidden",
    isOpen && !isDesktop,
  );
}

function installMobileDrawer() {
  const drawer = document.querySelector(
    "[data-app-drawer]",
  );

  const overlay = document.querySelector(
    "[data-app-drawer-overlay]",
  );

  const openButton = document.querySelector(
    "[data-app-drawer-open]",
  );

  const closeButton = document.querySelector(
    "[data-app-drawer-close]",
  );

  if (
    !drawer
    || !overlay
    || !openButton
    || !closeButton
  ) {
    return;
  }

  let previouslyFocused = null;

  const isOpen = () => (
    drawer.dataset.open === "true"
  );

  const close = (
    restoreFocus = true,
  ) => {
    const wasOpen = isOpen();

    setDrawerState(
      drawer,
      overlay,
      openButton,
      false,
    );

    if (
      wasOpen
      && restoreFocus
      && previouslyFocused
      instanceof HTMLElement
    ) {
      previouslyFocused.focus({
        preventScroll: true,
      });
    }

    previouslyFocused = null;
  };

  const open = () => {
    previouslyFocused = (
      document.activeElement
    );

    setDrawerState(
      drawer,
      overlay,
      openButton,
      true,
    );

    closeButton.focus({
      preventScroll: true,
    });
  };

  openButton.addEventListener(
    "click",
    open,
  );

  closeButton.addEventListener(
    "click",
    () => close(),
  );

  overlay.addEventListener(
    "click",
    () => close(),
  );

  drawer.querySelectorAll(
    "a[href]",
  ).forEach(
    (link) => {
      link.addEventListener(
        "click",
        () => {
          if (
            window.matchMedia(
              "(max-width: 1023px)",
            ).matches
          ) {
            close(false);
          }
        },
      );
    },
  );

  document.addEventListener(
    "keydown",
    (event) => {
      if (!isOpen()) {
        return;
      }

      if (event.key === "Escape") {
        event.preventDefault();
        close();
        return;
      }

      if (event.key !== "Tab") {
        return;
      }

      const focusable = (
        getDrawerFocusableElements(
          drawer,
        )
      );

      if (focusable.length === 0) {
        event.preventDefault();
        closeButton.focus();
        return;
      }

      const first = focusable[0];
      const last = (
        focusable[
          focusable.length - 1
        ]
      );

      if (
        event.shiftKey
        && (
          document.activeElement
            === first
          || !drawer.contains(
            document.activeElement,
          )
        )
      ) {
        event.preventDefault();
        last.focus();
        return;
      }

      if (
        !event.shiftKey
        && document.activeElement
          === last
      ) {
        event.preventDefault();
        first.focus();
      }
    },
  );

  const desktopMedia = (
    window.matchMedia(
      "(min-width: 1024px)",
    )
  );

  const syncDesktopState = (
    event,
  ) => {
    if (
      event.matches
      && isOpen()
    ) {
      close(false);
    }

    drawer.setAttribute(
      "aria-hidden",
      (
        event.matches
          ? "false"
          : String(!isOpen())
      ),
    );
  };

  if (
    typeof desktopMedia.addEventListener
    === "function"
  ) {
    desktopMedia.addEventListener(
      "change",
      syncDesktopState,
    );
  }

  setDrawerState(
    drawer,
    overlay,
    openButton,
    false,
  );
}

function installPublicNavigation() {
  const trigger = document.querySelector(
    "[data-public-nav-trigger]",
  );

  const panel = document.querySelector(
    "[data-public-nav-panel]",
  );

  if (!trigger || !panel) {
    return;
  }

  const isOpen = () => (
    trigger.getAttribute(
      "aria-expanded",
    ) === "true"
  );

  const setOpen = (open) => {
    trigger.setAttribute(
      "aria-expanded",
      String(open),
    );

    trigger.setAttribute(
      "aria-label",
      (
        open
          ? "Cerrar navegación"
          : "Abrir navegación"
      ),
    );

    panel.hidden = !open;
  };

  trigger.addEventListener(
    "click",
    () => {
      setOpen(!isOpen());
    },
  );

  panel.querySelectorAll(
    "a[href]",
  ).forEach(
    (link) => {
      link.addEventListener(
        "click",
        () => setOpen(false),
      );
    },
  );

  document.addEventListener(
    "keydown",
    (event) => {
      if (
        event.key === "Escape"
        && isOpen()
      ) {
        event.preventDefault();
        setOpen(false);
        trigger.focus({
          preventScroll: true,
        });
      }
    },
  );

  const desktopMedia = window.matchMedia(
    "(min-width: 640px)",
  );

  const syncViewport = (event) => {
    if (event.matches) {
      setOpen(false);
    }
  };

  if (
    typeof desktopMedia.addEventListener
    === "function"
  ) {
    desktopMedia.addEventListener(
      "change",
      syncViewport,
    );
  }

  setOpen(false);
}

function installDisclosureMenus() {
  document.addEventListener(
    "click",
    (event) => {
      const trigger = event.target.closest(
        "[data-disclosure-trigger]",
      );

      if (!trigger) {
        return;
      }

      const targetId = trigger.getAttribute(
        "aria-controls",
      );

      if (!targetId) {
        return;
      }

      const target = document.getElementById(
        targetId,
      );

      if (!target) {
        return;
      }

      const expanded = (
        trigger.getAttribute(
          "aria-expanded",
        ) === "true"
      );

      trigger.setAttribute(
        "aria-expanded",
        String(!expanded),
      );

      target.hidden = expanded;
    },
  );
}


function batchOptionParts(option) {
  const raw = String(option.textContent || "").trim();
  const parts = raw.split(" · ").map((part) => part.trim()).filter(Boolean);
  return {
    label: parts[0] || raw,
    meta: parts.slice(1).join(" · "),
    search: raw.toLocaleLowerCase("es"),
  };
}

function enhanceBatchPicker(select) {
  if (select.dataset.batchPickerEnhanced === "true") {
    return;
  }
  select.dataset.batchPickerEnhanced = "true";

  const shell = document.createElement("div");
  shell.className = "relative min-w-0";
  select.parentNode.insertBefore(shell, select);
  shell.appendChild(select);

  select.classList.add("sr-only");
  select.tabIndex = -1;
  select.setAttribute("aria-hidden", "true");

  const trigger = document.createElement("button");
  trigger.type = "button";
  trigger.className = "flex w-full min-w-0 items-center justify-between gap-3 rounded-lg border border-slate-300 bg-white px-3 py-2 text-left text-sm shadow-sm transition hover:border-slate-400 focus:border-emerald-600 focus:outline-none focus:ring-2 focus:ring-emerald-100";
  trigger.setAttribute("aria-haspopup", "listbox");
  trigger.setAttribute("aria-expanded", "false");

  const copy = document.createElement("span");
  copy.className = "min-w-0 flex-1";
  const label = document.createElement("span");
  label.className = "block truncate font-semibold text-slate-800";
  const meta = document.createElement("span");
  meta.className = "mt-0.5 block truncate text-[11px] font-normal text-slate-500";
  copy.append(label, meta);

  const chevron = document.createElement("i");
  chevron.className = "fa-solid fa-chevron-down shrink-0 text-xs text-slate-400";
  chevron.setAttribute("aria-hidden", "true");
  trigger.append(copy, chevron);

  const menu = document.createElement("div");
  menu.hidden = true;
  menu.className = "absolute left-0 right-0 z-50 mt-2 max-w-full overflow-hidden rounded-xl border border-slate-200 bg-white shadow-2xl shadow-slate-950/15";

  const searchWrap = document.createElement("div");
  searchWrap.className = "border-b border-slate-100 p-2";
  const search = document.createElement("input");
  search.type = "search";
  search.placeholder = "Buscar lote o producto…";
  search.className = "w-full rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm outline-none focus:border-emerald-500 focus:bg-white focus:ring-2 focus:ring-emerald-100";
  searchWrap.appendChild(search);

  const list = document.createElement("div");
  list.className = "max-h-64 overflow-y-auto overscroll-contain p-1";
  list.setAttribute("role", "listbox");
  const empty = document.createElement("p");
  empty.hidden = true;
  empty.className = "px-3 py-4 text-center text-xs text-slate-500";
  empty.textContent = "No hay lotes que coincidan con la búsqueda.";

  const clearSelection = document.createElement("button");
  clearSelection.type = "button";
  clearSelection.className = "mb-1 flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-xs font-semibold text-slate-500 transition hover:bg-slate-50 hover:text-slate-800 disabled:cursor-not-allowed disabled:opacity-40";
  clearSelection.innerHTML = '<i class="fa-solid fa-rotate-left" aria-hidden="true"></i>Limpiar selección';
  clearSelection.addEventListener("click", () => {
    select.value = "";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    menu.hidden = true;
    trigger.setAttribute("aria-expanded", "false");
    shell.dataset.batchPickerMenuOpen = "false";
    trigger.focus({ preventScroll: true });
  });
  list.appendChild(clearSelection);

  const optionButtons = [];
  Array.from(select.options).forEach((option) => {
    if (!option.value) {
      return;
    }
    const parts = batchOptionParts(option);
    const item = document.createElement("button");
    item.type = "button";
    item.dataset.value = option.value;
    item.dataset.search = parts.search;
    item.disabled = option.disabled;
    item.className = "flex w-full min-w-0 flex-col rounded-lg px-3 py-2.5 text-left transition hover:bg-emerald-50 disabled:cursor-not-allowed disabled:opacity-40";
    item.setAttribute("role", "option");

    const title = document.createElement("span");
    title.className = "block w-full truncate text-sm font-semibold text-slate-900";
    title.textContent = parts.label;
    const detail = document.createElement("span");
    detail.className = "mt-0.5 block w-full truncate text-[11px] text-slate-500";
    detail.textContent = parts.meta || "Lote disponible";
    item.append(title, detail);

    item.addEventListener("click", () => {
      select.value = option.value;
      select.dispatchEvent(new Event("change", { bubbles: true }));
      label.textContent = parts.label;
      meta.textContent = parts.meta || "Lote seleccionado";
      menu.hidden = true;
      trigger.setAttribute("aria-expanded", "false");
      trigger.focus({ preventScroll: true });
    });
    optionButtons.push(item);
    list.appendChild(item);
  });
  list.appendChild(empty);

  const placeholder = select.dataset.batchPickerPlaceholder || "Seleccionar lote";
  const syncSelection = () => {
    const selected = select.options[select.selectedIndex];
    clearSelection.disabled = !(selected && selected.value);
    if (selected && selected.value) {
      const parts = batchOptionParts(selected);
      label.textContent = parts.label;
      meta.textContent = parts.meta || "Lote seleccionado";
    } else {
      label.textContent = placeholder;
      meta.textContent = "Buscar por código o producto";
    }
  };

  const close = () => {
    menu.hidden = true;
    trigger.setAttribute("aria-expanded", "false");
  };
  const open = () => {
    menu.hidden = false;
    trigger.setAttribute("aria-expanded", "true");
    search.value = "";
    optionButtons.forEach((item) => { item.hidden = false; });
    empty.hidden = optionButtons.length !== 0;
    window.requestAnimationFrame(() => search.focus({ preventScroll: true }));
  };

  trigger.addEventListener("click", () => {
    document.querySelectorAll("[data-batch-picker-menu-open='true']").forEach((other) => {
      if (other !== shell) {
        other.dispatchEvent(new CustomEvent("batchpicker:close"));
      }
    });
    if (menu.hidden) {
      shell.dataset.batchPickerMenuOpen = "true";
      open();
    } else {
      shell.dataset.batchPickerMenuOpen = "false";
      close();
    }
  });
  shell.addEventListener("batchpicker:close", () => {
    shell.dataset.batchPickerMenuOpen = "false";
    close();
  });
  search.addEventListener("input", () => {
    const query = search.value.trim().toLocaleLowerCase("es");
    let visible = 0;
    optionButtons.forEach((item) => {
      const matches = !query || item.dataset.search.includes(query);
      item.hidden = !matches;
      if (matches) visible += 1;
    });
    empty.hidden = visible !== 0;
  });
  select.addEventListener("change", syncSelection);
  document.addEventListener("click", (event) => {
    if (!shell.contains(event.target)) {
      shell.dataset.batchPickerMenuOpen = "false";
      close();
    }
  });
  shell.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !menu.hidden) {
      event.preventDefault();
      shell.dataset.batchPickerMenuOpen = "false";
      close();
      trigger.focus({ preventScroll: true });
    }
  });

  menu.append(searchWrap, list);
  shell.append(trigger, menu);
  syncSelection();
}

function installBatchPickers() {
  document.querySelectorAll("select[data-batch-picker]").forEach(enhanceBatchPicker);
}

function resetBatchRow(row) {
  const select = row.querySelector("select[name='shipment_batch']");
  const quantity = row.querySelector("input[name='shipment_quantity']");
  if (select) {
    select.value = "";
    select.dispatchEvent(new Event("change", { bubbles: true }));
  }
  if (quantity) {
    quantity.value = "";
  }
}

function installDynamicBatchRows() {
  document.querySelectorAll("[data-dynamic-batch-rows]").forEach((container) => {
    const key = container.dataset.dynamicBatchRows;
    const rows = Array.from(container.querySelectorAll("[data-batch-row]"));
    if (!key || rows.length < 2) {
      return;
    }

    rows.forEach((row, index) => {
      if (index === 0) {
        row.hidden = false;
        return;
      }
      const select = row.querySelector("select[name='shipment_batch']");
      row.hidden = !(select && select.value);

      const remove = document.createElement("button");
      remove.type = "button";
      remove.className = "mt-2 inline-flex items-center gap-1 text-[11px] font-semibold text-slate-500 transition hover:text-rose-700 sm:col-span-2";
      remove.innerHTML = '<i class="fa-solid fa-xmark" aria-hidden="true"></i>Quitar lote';
      remove.addEventListener("click", () => {
        resetBatchRow(row);
        row.hidden = true;
        const add = document.querySelector(`[data-add-batch-row="${key}"]`);
        if (add) add.disabled = false;
      });
      row.appendChild(remove);
    });

    const add = document.querySelector(`[data-add-batch-row="${key}"]`);
    if (!add) {
      return;
    }
    add.addEventListener("click", () => {
      const next = rows.find((row, index) => index > 0 && row.hidden);
      if (!next) {
        add.disabled = true;
        return;
      }
      next.hidden = false;
      const trigger = next.querySelector("button[aria-haspopup='listbox']");
      if (trigger) trigger.focus({ preventScroll: true });
      if (!rows.some((row, index) => index > 0 && row.hidden)) {
        add.disabled = true;
      }
    });
  });
}

function boot() {
  installHtmxSameOriginGuard();
  installHtmxCsrfBridge();
  installFetchCsrfBridge();
  installMobileDrawer();
  installPublicNavigation();
  installDisclosureMenus();
  installBatchPickers();
  installDynamicBatchRows();
}

if (document.readyState === "loading") {
  document.addEventListener(
    "DOMContentLoaded",
    boot,
    {
      once: true,
    },
  );
} else {
  boot();
}

export {
  CSRF_HEADER_NAME,
  CSRF_META_NAME,
  readCsrfToken,
};
