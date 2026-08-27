const PRESENTATION_LABELS = new Map([
  ["CONFORMANCE_READY", "Preparado para conformidad"],
  ["NON_NEGLIGIBLE_RISK", "Riesgo no despreciable"],
  ["NO_OR_NEGLIGIBLE_RISK", "Riesgo nulo o despreciable"],
  ["UNASSESSED", "Sin evaluar"],
  ["DISPATCHED", "Despachado"],
  ["POSTED", "Contabilizado"],
  ["ATTENTION", "Requiere atención"],
  ["BLOCKED", "Bloqueado"],
  ["READY", "Listo"],
  ["DRAFT", "Borrador"],
  ["IMPORT", "Importación"],
  ["DOMESTIC", "Mercado interno"],
  ["EXPORT", "Exportación"],
  ["DDS_CANDIDATE", "Candidato DDS configurado"],
  ["LINEAGE_COMPLETE", "Genealogía completa"],
  ["SOURCE_PLOTS", "Parcelas de origen"],
  ["ALL_PLOTS_GEOLOCATED", "Parcelas geolocalizadas"],
  ["SHIPMENT_DESTINATION", "País de destino"],
  ["OPERATOR_NAME", "Nombre del operador"],
  ["OPERATOR_ADDRESS", "Domicilio del operador"],
  ["OPERATOR_COUNTRY", "País del operador"],
  ["OPERATOR_EORI", "EORI del operador"],
  ["HS_CODE", "Código HS/CN"],
  ["TRADE_NAME", "Nombre comercial"],
  ["PRODUCT_DESCRIPTION", "Descripción del producto"],
  ["NET_MASS_KG", "Masa neta"],
  ["PRODUCTION_COUNTRY", "País de producción"],
  ["PRODUCTION_DATES", "Fechas de producción"],
  ["WOOD_COMMON_SPECIES", "Especie común"],
  ["WOOD_SCIENTIFIC_SPECIES", "Especie científica"],
  ["PREVIOUS_DDS_REFERENCE", "Referencia DDS previa"],
  ["PREVIOUS_DDS_VERIFICATION", "Verificación DDS previa"],
  ["RISK_CONCLUSION", "Conclusión de riesgo"],
  ["RISK_ASSESSMENT_REFERENCE", "Referencia de evaluación de riesgo"],
  ["RISK_ASSESSED_AT", "Fecha de evaluación de riesgo"],
  ["SPEC_PROFILE_CURRENT", "Perfil técnico vigente"],
  ["REQUIREMENTS_REFERENCE", "Referencia oficial de requisitos"],
  ["REQUIREMENTS_CHECKED_AT", "Fecha de evaluación de requisitos"],
  ["CERT_POV_REFERENCE", "Referencia CERT-POV"],
  ["PHYTOSANITARY_CERTIFICATE_NUMBER", "Número de certificado fitosanitario"],
  ["PHYTOSANITARY_CERTIFICATE_EVIDENCE", "Certificado fitosanitario en evidencias"],
  ["CULTIVATED_INVOICE_OR_REMITO", "Factura o remito del traslado"],
  ["FRUIT_GUIDE", "Guía de Frutos"],
  ["EXPORT_INVOICE_E", "Factura E de exportación"],
  ["SIM_DESTINATION", "Destinación aduanera SIM"],
  ["SIM_SUBREGIME", "Subrégimen SIM"],
  ["RECEIPT", "Recepción"],
  ["TRANSFORMATION", "Transformación"],
]);

const SKIP_TAGS = new Set(["SCRIPT", "STYLE", "TEXTAREA", "PRE", "CODE"]);
const ORDERED_LABELS = Array.from(PRESENTATION_LABELS.entries())
  .sort((left, right) => right[0].length - left[0].length);
const TERMINAL_SEPARATORS = [" · ", ": "];

function translateIsolatedToken(rawText, technical, business) {
  const leadingLength = rawText.length - rawText.trimStart().length;
  const trailingLength = rawText.length - rawText.trimEnd().length;
  const leading = rawText.slice(0, leadingLength);
  const trailing = trailingLength ? rawText.slice(rawText.length - trailingLength) : "";
  const trimmed = rawText.trim();

  if (trimmed === technical) {
    return `${leading}${business}${trailing}`;
  }

  for (const separator of TERMINAL_SEPARATORS) {
    const suffix = `${separator}${technical}`;
    if (trimmed.endsWith(suffix)) {
      const prefix = trimmed.slice(0, -technical.length);
      return `${leading}${prefix}${business}${trailing}`;
    }
  }

  return rawText;
}

function presentText(rawText) {
  const trimmed = String(rawText || "").trim();
  if (!trimmed) {
    return rawText;
  }

  const direct = PRESENTATION_LABELS.get(trimmed);
  if (direct) {
    const leadingLength = rawText.length - rawText.trimStart().length;
    const trailingLength = rawText.length - rawText.trimEnd().length;
    const leading = rawText.slice(0, leadingLength);
    const trailing = trailingLength ? rawText.slice(rawText.length - trailingLength) : "";
    return `${leading}${direct}${trailing}`;
  }

  let rendered = rawText;
  for (const [technical, business] of ORDERED_LABELS) {
    const translated = translateIsolatedToken(rendered, technical, business);
    if (translated !== rendered) {
      rendered = translated;
      break;
    }
  }
  return rendered;
}

function translateTextNodes(root = document.body) {
  if (!root) {
    return;
  }

  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
  const nodes = [];
  while (walker.nextNode()) {
    nodes.push(walker.currentNode);
  }

  nodes.forEach((node) => {
    const parent = node.parentElement;
    if (!parent || SKIP_TAGS.has(parent.tagName) || parent.closest("[data-technical-code]")) {
      return;
    }

    const current = node.nodeValue || "";
    const translated = presentText(current);
    if (translated !== current) {
      node.nodeValue = translated;
    }
  });
}

function installBusinessLanguage() {
  translateTextNodes();

  // HTMX can replace fragments after the first render. Translate only the
  // incoming fragment; data attributes, form values and backend enums remain
  // unchanged and therefore safe for application logic.
  document.body?.addEventListener("htmx:afterSwap", (event) => {
    translateTextNodes(event.detail?.target || document.body);
  });
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", installBusinessLanguage, {once: true});
} else {
  installBusinessLanguage();
}

export {PRESENTATION_LABELS, presentText};
