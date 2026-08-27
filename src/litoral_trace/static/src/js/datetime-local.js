function normalizeDatetimeLocal(rawValue) {
  const raw = String(rawValue || "").trim();
  if (!raw) {
    return "";
  }

  // PostgreSQL timestamptz values may render as ISO-8601 with Z/offset, while
  // HTML datetime-local accepts no timezone designator. Preserve the recorded
  // wall-clock fields instead of silently shifting timezones in the browser.
  const match = raw.match(/^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})/);
  return match ? match[1] : raw;
}

function rehydrateDatetimeLocalInputs() {
  document.querySelectorAll('input[type="datetime-local"]').forEach((input) => {
    if (input.value) {
      return;
    }

    const rawValue = input.getAttribute("value");
    const normalized = normalizeDatetimeLocal(rawValue);
    if (!normalized || normalized === rawValue) {
      return;
    }

    try {
      input.value = normalized;
      input.setAttribute("value", normalized);
    } catch (_error) {
      // Fail closed visually: never invent a replacement if the browser still
      // rejects the persisted value.
    }
  });
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", rehydrateDatetimeLocalInputs, {once: true});
} else {
  rehydrateDatetimeLocalInputs();
}

export {normalizeDatetimeLocal};
