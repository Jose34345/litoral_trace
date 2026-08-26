const EVIDENCE_PATH = "/evidence";
const SHIPMENT_PARAM = "shipment_code";
const SUBJECT_PARAM = "subject";

function currentShipmentCode() {
  const params = new URLSearchParams(window.location.search);
  return (params.get(SHIPMENT_PARAM) || "").trim();
}

function preserveShipmentOnEvidenceLinks() {
  const code = currentShipmentCode();
  if (!code) {
    return;
  }

  document.querySelectorAll('a[href]').forEach((anchor) => {
    let target;
    try {
      target = new URL(anchor.href, window.location.href);
    } catch (_error) {
      return;
    }

    if (
      target.origin !== window.location.origin
      || target.pathname !== EVIDENCE_PATH
      || target.searchParams.has(SUBJECT_PARAM)
      || target.searchParams.has(SHIPMENT_PARAM)
    ) {
      return;
    }

    // Shipment evidence subjects use the shipment public UUID internally, not
    // the commercial shipment code. Preserve the code only as navigation
    // context; /evidence resolves it against the tenant-scoped selector before
    // adding an explicit subject key.
    target.searchParams.set(SHIPMENT_PARAM, code);
    anchor.href = `${target.pathname}${target.search}${target.hash}`;
  });
}

function shipmentCodeFromSafeReferrer() {
  if (!document.referrer) {
    return "";
  }

  try {
    const referrer = new URL(document.referrer);
    if (referrer.origin !== window.location.origin) {
      return "";
    }

    return (referrer.searchParams.get(SHIPMENT_PARAM) || "").trim();
  } catch (_error) {
    return "";
  }
}

function findShipmentSubject(selector, shipmentCode) {
  const normalizedCode = String(shipmentCode || "").trim();
  if (!normalizedCode) {
    return "";
  }

  const expectedPrefix = `Despacho · ${normalizedCode}`.toLocaleLowerCase("es");
  const option = Array.from(selector.options).find((candidate) => {
    const text = String(candidate.textContent || "").trim().toLocaleLowerCase("es");
    return text === expectedPrefix || text.startsWith(`${expectedPrefix} ·`);
  });

  // The value is the tenant-scoped key rendered by the server, e.g.
  // SHIPMENT|<public UUID>. We never manufacture that UUID in the browser.
  return option?.value || "";
}

function disableMutationFormsUntilSubjectConfirmed(selector) {
  const guard = document.createElement("div");
  guard.dataset.evidenceContextGuard = "true";
  guard.setAttribute("role", "status");
  guard.style.cssText = [
    "margin-top:0.75rem",
    "border:1px solid #fcd34d",
    "border-radius:0.5rem",
    "background:#fffbeb",
    "padding:0.75rem",
    "font-size:0.75rem",
    "line-height:1.25rem",
    "color:#78350f",
  ].join(";");
  guard.innerHTML = "<strong>Confirmá el eslabón antes de vincular.</strong> La pantalla no reutiliza silenciosamente el primer origen después de un reingreso. Elegí el origen, movimiento, lote o despacho que el documento respalda.";
  selector.closest("form")?.appendChild(guard);

  document.querySelectorAll(
    'form[action="/evidence/link"], form[action="/evidence/upload-link"]',
  ).forEach((form) => {
    form.querySelectorAll('button[type="submit"], button:not([type])').forEach((button) => {
      button.disabled = true;
      button.setAttribute("aria-disabled", "true");
      button.title = "Elegí y confirmá primero el eslabón de trazabilidad.";
      button.style.opacity = "0.5";
      button.style.cursor = "not-allowed";
    });
  });
}

function resolveOrGuardEvidenceSubject() {
  if (window.location.pathname !== EVIDENCE_PATH) {
    return;
  }

  const params = new URLSearchParams(window.location.search);
  if (params.has(SUBJECT_PARAM)) {
    return;
  }

  const selector = document.querySelector("#subject-select");
  if (!selector) {
    return;
  }

  const shipmentCode = (
    (params.get(SHIPMENT_PARAM) || "").trim()
    || shipmentCodeFromSafeReferrer()
  );
  const resolvedSubject = findShipmentSubject(selector, shipmentCode);

  if (resolvedSubject) {
    params.delete(SHIPMENT_PARAM);
    params.set(SUBJECT_PARAM, resolvedSubject);
    window.location.replace(`${EVIDENCE_PATH}?${params.toString()}`);
    return;
  }

  disableMutationFormsUntilSubjectConfirmed(selector);
}

function bootEvidenceContext() {
  preserveShipmentOnEvidenceLinks();
  resolveOrGuardEvidenceSubject();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", bootEvidenceContext, {once: true});
} else {
  bootEvidenceContext();
}
