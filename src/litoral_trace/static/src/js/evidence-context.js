const EVIDENCE_PATH = "/evidence";
const SHIPMENT_PARAM = "shipment_code";
const SUBJECT_PARAM = "subject";

function shipmentSubject(code) {
  const normalized = String(code || "").trim();
  return normalized ? `SHIPMENT|${normalized}` : "";
}

function currentShipmentCode() {
  const params = new URLSearchParams(window.location.search);
  return (params.get(SHIPMENT_PARAM) || "").trim();
}

function preserveShipmentOnEvidenceLinks() {
  const code = currentShipmentCode();
  if (!code) {
    return;
  }

  const subject = shipmentSubject(code);
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
    ) {
      return;
    }

    target.searchParams.set(SUBJECT_PARAM, subject);
    anchor.href = `${target.pathname}${target.search}${target.hash}`;
  });
}

function subjectFromSafeReferrer() {
  if (!document.referrer) {
    return "";
  }

  try {
    const referrer = new URL(document.referrer);
    if (referrer.origin !== window.location.origin) {
      return "";
    }

    const code = (referrer.searchParams.get(SHIPMENT_PARAM) || "").trim();
    return shipmentSubject(code);
  } catch (_error) {
    return "";
  }
}

function addSubjectConfirmationGuard() {
  if (window.location.pathname !== EVIDENCE_PATH) {
    return;
  }

  const params = new URLSearchParams(window.location.search);
  if (params.has(SUBJECT_PARAM)) {
    return;
  }

  const recoveredSubject = subjectFromSafeReferrer();
  if (recoveredSubject) {
    params.set(SUBJECT_PARAM, recoveredSubject);
    window.location.replace(`${EVIDENCE_PATH}?${params.toString()}`);
    return;
  }

  const selector = document.querySelector("#subject-select");
  if (!selector) {
    return;
  }

  const guard = document.createElement("div");
  guard.dataset.evidenceContextGuard = "true";
  guard.setAttribute("role", "status");
  guard.className = "mt-3 rounded-lg border border-amber-300 bg-amber-50 p-3 text-xs leading-5 text-amber-900";
  guard.innerHTML = "<strong>Confirmá el eslabón antes de vincular.</strong> La pantalla no reutiliza silenciosamente el primer origen después de un reingreso. Elegí el origen, movimiento, lote o despacho que el documento respalda.";
  selector.closest("form")?.appendChild(guard);

  const guardedForms = document.querySelectorAll(
    'form[action="/evidence/link"], form[action="/evidence/upload-link"]',
  );

  guardedForms.forEach((form) => {
    form.querySelectorAll('button[type="submit"], button:not([type])').forEach((button) => {
      button.disabled = true;
      button.setAttribute("aria-disabled", "true");
      button.title = "Elegí y confirmá primero el eslabón de trazabilidad.";
      button.classList.add("opacity-50", "cursor-not-allowed");
    });
  });
}

function bootEvidenceContext() {
  preserveShipmentOnEvidenceLinks();
  addSubjectConfirmationGuard();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", bootEvidenceContext, {once: true});
} else {
  bootEvidenceContext();
}
