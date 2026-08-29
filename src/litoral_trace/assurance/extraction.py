"""Deterministic classification and structured extraction for Assurance v1.

Commodity parsing/normalization is delegated to mature libraries where useful,
while business acceptance remains conservative and auditable. Generative AI is
never required and no fuzzy entity match is allowed to mutate operational data.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re
import unicodedata
from typing import Any

import dateparser
from price_parser import Price
from rapidfuzz import fuzz, process

from litoral_trace.assurance.domain import AssuranceDocumentType
from litoral_trace.assurance.normalization import (
    NormalizationError,
    normalize_argentine_number,
    normalize_cuit,
    normalize_date,
    normalize_identifier,
    normalize_quantity,
)
from litoral_trace.assurance.parsers import ParsedDocument


@dataclass(frozen=True, slots=True)
class DocumentSchema:
    document_type: AssuranceDocumentType
    required_fields: tuple[str, ...]
    optional_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ClassificationResult:
    document_type: AssuranceDocumentType
    confidence: float
    evidence: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ExtractedCandidate:
    field_name: str
    original_value: str
    normalized_value: str
    value_type: str
    confidence: float
    source_page: int | None
    source_locator: str


DOCUMENT_SCHEMAS: dict[AssuranceDocumentType, DocumentSchema] = {
    AssuranceDocumentType.INVOICE: DocumentSchema(
        AssuranceDocumentType.INVOICE,
        required_fields=("document_number", "document_date", "issuer_cuit"),
        optional_fields=(
            "receiver_cuit",
            "supplier",
            "product",
            "quantity",
            "unit",
            "total_amount",
            "currency",
            "destination",
            "order_id",
            "sale_reference",
        ),
    ),
    AssuranceDocumentType.DELIVERY_NOTE: DocumentSchema(
        AssuranceDocumentType.DELIVERY_NOTE,
        required_fields=("document_number", "document_date"),
        optional_fields=(
            "issuer_cuit",
            "supplier",
            "product",
            "quantity",
            "unit",
            "lot_id",
            "shipment_code",
            "order_id",
            "sale_reference",
        ),
    ),
    AssuranceDocumentType.FOREST_GUIDE: DocumentSchema(
        AssuranceDocumentType.FOREST_GUIDE,
        required_fields=("document_number", "document_date"),
        optional_fields=(
            "issuer_cuit",
            "supplier",
            "species",
            "product",
            "quantity",
            "unit",
            "origin",
            "lot_id",
            "shipment_code",
        ),
    ),
    AssuranceDocumentType.PHYTOSANITARY_CERTIFICATE: DocumentSchema(
        AssuranceDocumentType.PHYTOSANITARY_CERTIFICATE,
        required_fields=("document_number",),
        optional_fields=(
            "document_date",
            "valid_until",
            "destination",
            "species",
            "product",
            "quantity",
            "unit",
            "shipment_code",
        ),
    ),
    AssuranceDocumentType.CUSTOMS_DOCUMENT: DocumentSchema(
        AssuranceDocumentType.CUSTOMS_DOCUMENT,
        required_fields=("document_number",),
        optional_fields=(
            "document_date",
            "destination",
            "hs_code",
            "quantity",
            "unit",
            "product",
            "shipment_code",
            "order_id",
            "sale_reference",
        ),
    ),
    AssuranceDocumentType.SPREADSHEET: DocumentSchema(
        AssuranceDocumentType.SPREADSHEET,
        required_fields=(),
        optional_fields=(
            "document_number",
            "document_date",
            "issuer_cuit",
            "receiver_cuit",
            "supplier",
            "product",
            "quantity",
            "unit",
            "lot_id",
            "destination",
            "shipment_code",
            "order_id",
            "sale_reference",
        ),
    ),
    AssuranceDocumentType.UNKNOWN: DocumentSchema(
        AssuranceDocumentType.UNKNOWN,
        required_fields=(),
        optional_fields=(),
    ),
}


_CLASSIFICATION_KEYWORDS: dict[AssuranceDocumentType, tuple[str, ...]] = {
    AssuranceDocumentType.INVOICE: (
        "factura e",
        "factura",
        "invoice",
        "rental invoice",
        "comprobante",
        "cae",
    ),
    AssuranceDocumentType.DELIVERY_NOTE: (
        "remito",
        "delivery note",
        "delivery receipt",
        "nota de entrega",
    ),
    AssuranceDocumentType.FOREST_GUIDE: (
        "guia forestal",
        "guia de frutos",
        "guia de productos forestales",
        "vale forestal",
    ),
    AssuranceDocumentType.PHYTOSANITARY_CERTIFICATE: (
        "certificado fitosanitario",
        "phytosanitary certificate",
        "cert-pov",
        "ephyto",
        "senasa",
    ),
    AssuranceDocumentType.CUSTOMS_DOCUMENT: (
        "destinacion aduanera",
        "permiso de embarque",
        "subregimen",
        "aduana",
        "customs declaration",
        "export declaration",
    ),
}

_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "document_number": (
        "numero",
        "numero de factura",
        "nro",
        "n comprobante",
        "comprobante",
        "factura",
        "factura numero",
        "invoice",
        "invoice number",
        "invoice no",
        "invoice #",
        "inv no",
        "bill number",
        "document number",
        "remito",
        "guia",
        "certificado",
        "destinacion",
        "referencia",
    ),
    "document_date": (
        "fecha",
        "fecha emision",
        "fecha de emision",
        "emision",
        "invoice date",
        "document date",
        "issue date",
        "date",
    ),
    "valid_until": (
        "vencimiento",
        "valido hasta",
        "vigencia hasta",
        "fecha vencimiento",
        "valid until",
        "expiry date",
        "expiration date",
    ),
    "issuer_cuit": (
        "cuit emisor",
        "cuit proveedor",
        "cuit",
        "tax id proveedor",
        "supplier tax id",
        "vendor tax id",
        "issuer tax id",
    ),
    "receiver_cuit": (
        "cuit receptor",
        "cuit cliente",
        "tax id cliente",
        "customer tax id",
        "buyer tax id",
    ),
    "supplier": (
        "proveedor",
        "razon social",
        "productor",
        "supplier",
        "vendor",
        "seller",
        "issuer",
    ),
    "product": (
        "producto",
        "descripcion",
        "mercaderia",
        "material",
        "product",
        "description",
        "item description",
        "goods description",
    ),
    "quantity": (
        "cantidad",
        "peso",
        "volumen",
        "total kg",
        "total tn",
        "total t",
        "quantity",
        "qty",
        "weight",
        "volume",
    ),
    "unit": ("unidad", "uom", "unidad medida", "unit", "unit of measure"),
    "lot_id": ("lote", "lote origen", "partida", "batch", "batch no", "lot", "lot no"),
    "destination": (
        "destino",
        "pais destino",
        "mercado",
        "destination",
        "ship to country",
        "country of destination",
    ),
    "species": ("especie", "especie cientifica", "species", "scientific name"),
    "hs_code": ("ncm", "hs", "hs code", "posicion arancelaria", "tariff code"),
    "total_amount": (
        "total",
        "importe total",
        "monto total",
        "total amount",
        "total amount due",
        "amount due",
        "invoice total",
        "net invoice",
        "grand total",
        "item total",
    ),
    "currency": ("moneda", "currency", "currency code"),
    "origin": ("origen", "procedencia", "establecimiento origen", "origin", "country of origin"),
    "shipment_code": (
        "despacho",
        "codigo despacho",
        "codigo de despacho",
        "shipment",
        "shipment code",
        "dispatch code",
    ),
    "order_id": (
        "pedido",
        "pedido id",
        "orden",
        "orden compra",
        "orden de compra",
        "purchase order",
        "purchase order number",
        "po",
        "po number",
        "po #",
        "p o",
        "p o #",
    ),
    "sale_reference": (
        "referencia venta",
        "referencia de venta",
        "sale reference",
        "pedido cliente",
        "customer reference",
        "customer ref",
    ),
}

_FUZZY_ALIAS_THRESHOLD = 88.0
_LABEL_VALUE_RE = re.compile(
    r"^\s*(?P<label>[A-Za-zÀ-ÿ0-9][A-Za-zÀ-ÿ0-9 ._/#()&-]{0,64}?)\s*[:=]\s*(?P<value>.+?)\s*$"
)
_TEXTUAL_DATE_RE = re.compile(r"[A-Za-zÀ-ÿ]")


def _fold(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").lower())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def _document_search_text(filename: str, parsed: ParsedDocument) -> str:
    parts = [_fold(filename), _fold(parsed.text)]
    for table in parsed.tables:
        parts.extend(_fold(header) for header in table.headers)
    return " ".join(part for part in parts if part)


def classify_document(filename: str, parsed: ParsedDocument) -> ClassificationResult:
    haystack = _document_search_text(filename, parsed)
    scored: list[tuple[float, AssuranceDocumentType, list[str]]] = []
    for document_type, keywords in _CLASSIFICATION_KEYWORDS.items():
        evidence: list[str] = []
        score = 0.0
        for keyword in keywords:
            folded = _fold(keyword)
            if folded and folded in haystack:
                evidence.append(keyword)
                score += 1.0
        if evidence:
            filename_folded = _fold(filename)
            if any(_fold(keyword) in filename_folded for keyword in keywords):
                score += 0.75
            scored.append((score, document_type, evidence))

    if scored:
        scored.sort(key=lambda item: (item[0], item[1].value), reverse=True)
        score, document_type, evidence = scored[0]
        confidence = min(0.99, 0.58 + 0.12 * score)
        return ClassificationResult(document_type, confidence, tuple(evidence))

    if parsed.file_kind in {"XLSX", "XLS", "CSV"}:
        return ClassificationResult(
            AssuranceDocumentType.SPREADSHEET,
            0.99,
            (f"structured_{parsed.file_kind.lower()}",),
        )
    return ClassificationResult(AssuranceDocumentType.UNKNOWN, 0.0, ())


def _alias_choices() -> dict[str, str]:
    choices: dict[str, str] = {}
    for field_name, aliases in _HEADER_ALIASES.items():
        for alias in aliases:
            folded = _fold(alias)
            if folded:
                choices.setdefault(folded, field_name)
    return choices


def _canonical_header_with_score(header: str) -> tuple[str | None, float]:
    folded = _fold(header)
    if not folded:
        return None, 0.0
    choices = _alias_choices()
    exact = choices.get(folded)
    if exact is not None:
        return exact, 100.0

    match = process.extractOne(
        folded,
        tuple(choices),
        scorer=fuzz.WRatio,
        score_cutoff=_FUZZY_ALIAS_THRESHOLD,
    )
    if match is None:
        return None, 0.0
    matched_alias, score, _ = match
    return choices[matched_alias], float(score)


def _canonical_header(header: str) -> str | None:
    field_name, _ = _canonical_header_with_score(header)
    return field_name


def _normalize_textual_date(original: str) -> date:
    try:
        return normalize_date(original)
    except NormalizationError:
        # Numeric ambiguous dates remain fail-closed. dateparser is used only
        # when the source contains an explicit textual month/language signal.
        if _TEXTUAL_DATE_RE.search(original) is None:
            raise
        parsed = dateparser.parse(
            original,
            languages=["es", "en"],
            settings={
                "STRICT_PARSING": True,
                "PREFER_LOCALE_DATE_ORDER": True,
                "RETURN_AS_TIMEZONE_AWARE": False,
            },
        )
        if parsed is None:
            raise NormalizationError("Formato de fecha no reconocido.")
        return parsed.date()


def _normalize_candidate(field_name: str, value: object) -> tuple[str, str]:
    original = str(value).strip()
    if not original:
        raise NormalizationError("Valor vacio.")

    if field_name in {"issuer_cuit", "receiver_cuit"}:
        return original, normalize_cuit(original)
    if field_name in {"document_date", "valid_until"}:
        normalized_date = _normalize_textual_date(original)
        return original, normalized_date.isoformat()
    if field_name == "quantity":
        try:
            quantity = normalize_quantity(original)
            return original, str(quantity.amount)
        except NormalizationError:
            return original, str(normalize_argentine_number(original))
    if field_name == "total_amount":
        price = Price.fromstring(original)
        if price.amount is not None:
            return original, str(price.amount)
        return original, str(normalize_argentine_number(original))
    if field_name in {
        "document_number",
        "lot_id",
        "hs_code",
        "shipment_code",
        "order_id",
        "sale_reference",
    }:
        return original, normalize_identifier(original)
    return original, re.sub(r"\s+", " ", original).strip()


def _value_type(field_name: str) -> str:
    if field_name in {"document_date", "valid_until"}:
        return "date"
    if field_name in {"quantity", "total_amount"}:
        return "number"
    if field_name in {
        "document_number",
        "issuer_cuit",
        "receiver_cuit",
        "lot_id",
        "hs_code",
        "shipment_code",
        "order_id",
        "sale_reference",
    }:
        return "identifier"
    return "text"


def _candidate_from_table(
    field_name: str,
    value: object,
    *,
    locator: str,
    page: int | None,
    confidence: float,
) -> ExtractedCandidate | None:
    if value is None:
        return None
    try:
        original, normalized = _normalize_candidate(field_name, value)
    except NormalizationError:
        return None
    return ExtractedCandidate(
        field_name=field_name,
        original_value=original,
        normalized_value=normalized,
        value_type=_value_type(field_name),
        confidence=confidence,
        source_page=page,
        source_locator=locator,
    )


def _extract_from_tables(parsed: ParsedDocument) -> list[ExtractedCandidate]:
    candidates: list[ExtractedCandidate] = []
    for table_index, table in enumerate(parsed.tables, start=1):
        mapped_headers = {
            header: _canonical_header_with_score(header)
            for header in table.headers
        }
        for row_index, row in enumerate(table.rows, start=1):
            unit_value = None
            for header, (canonical, _) in mapped_headers.items():
                if canonical == "unit" and row.get(header) not in {None, ""}:
                    unit_value = row.get(header)
            for column_index, header in enumerate(table.headers, start=1):
                canonical, similarity = mapped_headers[header]
                if canonical is None:
                    continue
                value = row.get(header)
                if value is None:
                    continue
                locator = (
                    f"{table.source.locator or table.name};data_row:{row_index};"
                    f"column:{column_index};header:{header}"
                )
                confidence = 0.98 if similarity == 100.0 else min(0.89, 0.72 + similarity / 600.0)
                candidate = _candidate_from_table(
                    canonical,
                    value,
                    locator=locator,
                    page=table.source.page,
                    confidence=confidence,
                )
                if candidate is not None:
                    candidates.append(candidate)
                    if canonical == "quantity" and unit_value:
                        unit_candidate = _candidate_from_table(
                            "unit",
                            unit_value,
                            locator=locator + ";paired_unit",
                            page=table.source.page,
                            confidence=confidence,
                        )
                        if unit_candidate is not None:
                            candidates.append(unit_candidate)
    return candidates


_DATE_VALUE = (
    r"(?:[A-Za-zÀ-ÿ]{3,12}[ \t]+\d{1,2},?[ \t]+\d{2,4}|"
    r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{1,2}-\d{1,2})"
)
_TEXT_PATTERNS: dict[str, re.Pattern[str]] = {
    "issuer_cuit": re.compile(
        r"(?:CUIT(?:[ \t]+emisor|[ \t]+proveedor)?[ \t]*[:#-]?[ \t]*)(\d{2}-?\d{8}-?\d)",
        re.I,
    ),
    "document_date": re.compile(
        rf"(?:fecha(?:[ \t]+de[ \t]+emision)?|invoice[ \t]+date|issue[ \t]+date|document[ \t]+date|date)"
        rf"[ \t]*[:#-]?[ \t]*({_DATE_VALUE})",
        re.I,
    ),
    "valid_until": re.compile(
        rf"(?:vencimiento|valido[ \t]+hasta|vigencia[ \t]+hasta|valid[ \t]+until|expiry[ \t]+date|expiration[ \t]+date)"
        rf"[ \t]*[:#-]?[ \t]*({_DATE_VALUE})",
        re.I,
    ),
    "document_number": re.compile(
        r"(?:factura|invoice(?:[ \t]+(?:number|no\.?))?|remito|guia|certificado|destinacion|nro\.?|numero)"
        r"[ \t]*(?:[A-Z][ \t]*)?[:#-]?[ \t]*([A-Z0-9][A-Z0-9./-]{2,})",
        re.I,
    ),
    "hs_code": re.compile(
        r"(?:NCM|HS(?:[ \t]+CODE)?|posicion[ \t]+arancelaria|tariff[ \t]+code)"
        r"[ \t]*[:#-]?[ \t]*([0-9.]{4,14})",
        re.I,
    ),
    "destination": re.compile(
        r"(?:destino|pais[ \t]+destino|destination|country[ \t]+of[ \t]+destination)"
        r"[ \t]*[:#-]?[ \t]*([^\n\r]{2,80})",
        re.I,
    ),
    "shipment_code": re.compile(
        r"(?:codigo[ \t]+de[ \t]+despacho|codigo[ \t]+despacho|shipment[ \t]+code|dispatch[ \t]+code)"
        r"[ \t]*[:#-]?[ \t]*([A-Z0-9][A-Z0-9./-]{2,})",
        re.I,
    ),
    "order_id": re.compile(
        r"(?:orden[ \t]+de[ \t]+compra|purchase[ \t]+order(?:[ \t]+number)?|pedido|P\.?[ \t]*O\.?)"
        r"[ \t]*[:#-]?[ \t]*([A-Z0-9][A-Z0-9./-]{2,})",
        re.I,
    ),
    "quantity": re.compile(
        r"(?:cantidad|quantity|qty)[ \t]*[:#-]?[ \t]*([0-9][0-9., ]*(?:[ \t]*(?:kg|kgs|t|tn|tons?|m3|m³|units?|un))?)",
        re.I,
    ),
    "total_amount": re.compile(
        r"(?:importe[ \t]+total|monto[ \t]+total|total[ \t]+amount[ \t]+due|amount[ \t]+due|"
        r"invoice[ \t]+total|net[ \t]+invoice|grand[ \t]+total|item[ \t]+total|total)"
        r"[ \t]*[:#-]?[ \t]*((?:USD|ARS|EUR|GBP)?[ \t]*[$€£]?[ \t]*[0-9][0-9., ]*)",
        re.I,
    ),
}


def _extract_explicit_currency(parsed: ParsedDocument) -> list[ExtractedCandidate]:
    if not parsed.text:
        return []
    signals = (
        (re.compile(r"\bUSD\b|U\.?S\.?[ \t]+Dollars?", re.I), "USD"),
        (re.compile(r"\bARS\b|Pesos?[ \t]+Argentinos?", re.I), "ARS"),
        (re.compile(r"\bEUR\b|Euros?", re.I), "EUR"),
        (re.compile(r"\bGBP\b|Pounds?[ \t]+Sterling", re.I), "GBP"),
    )
    for pattern, currency in signals:
        match = pattern.search(parsed.text)
        if match is None:
            continue
        candidate = _candidate_from_table(
            "currency",
            currency,
            locator=f"pdf:text;span:{match.start()}-{match.end()};explicit_currency",
            page=None,
            confidence=0.94,
        )
        return [candidate] if candidate is not None else []
    return []


def _extract_label_value_lines(parsed: ParsedDocument) -> list[ExtractedCandidate]:
    """Extract label:value pairs using RapidFuzz only for the label itself.

    Fuzzy labels are intentionally kept below auto-accept confidence so OCR
    spelling noise can be recovered without silently writing uncertain facts.
    """
    if not parsed.text:
        return []
    candidates: list[ExtractedCandidate] = []
    offset = 0
    for line_number, raw_line in enumerate(parsed.text.splitlines(), start=1):
        line = raw_line.strip()
        current_offset = offset
        offset += len(raw_line) + 1
        if not line:
            continue
        match = _LABEL_VALUE_RE.match(line)
        if match is None:
            continue
        label = match.group("label").strip()
        value = match.group("value").strip()
        field_name, similarity = _canonical_header_with_score(label)
        if field_name is None or not value:
            continue
        confidence = 0.96 if similarity == 100.0 else min(0.89, 0.70 + similarity / 500.0)
        value_start = current_offset + raw_line.find(match.group("value"))
        candidate = _candidate_from_table(
            field_name,
            value,
            locator=(
                f"pdf:text;line:{line_number};label:{label};"
                f"span:{value_start}-{value_start + len(value)};alias_score:{similarity:.1f}"
            ),
            page=None,
            confidence=confidence,
        )
        if candidate is not None:
            candidates.append(candidate)
            if field_name == "total_amount":
                price = Price.fromstring(value)
                if price.currency:
                    currency_candidate = _candidate_from_table(
                        "currency",
                        price.currency,
                        locator=(
                            f"pdf:text;line:{line_number};label:{label};"
                            f"derived_from_price_parser"
                        ),
                        page=None,
                        confidence=min(confidence, 0.88),
                    )
                    if currency_candidate is not None:
                        candidates.append(currency_candidate)
    return candidates


def _extract_from_text(parsed: ParsedDocument) -> list[ExtractedCandidate]:
    if not parsed.text:
        return []
    candidates: list[ExtractedCandidate] = []
    for field_name, pattern in _TEXT_PATTERNS.items():
        match = pattern.search(parsed.text)
        if match is None:
            continue
        value = match.group(1).strip()
        candidate = _candidate_from_table(
            field_name,
            value,
            locator=f"pdf:text;span:{match.start(1)}-{match.end(1)};pattern",
            page=None,
            confidence=0.90,
        )
        if candidate is not None:
            candidates.append(candidate)
            if field_name == "total_amount":
                price = Price.fromstring(value)
                if price.currency:
                    currency_candidate = _candidate_from_table(
                        "currency",
                        price.currency,
                        locator=f"pdf:text;span:{match.start(1)}-{match.end(1)};price_parser",
                        page=None,
                        confidence=0.86,
                    )
                    if currency_candidate is not None:
                        candidates.append(currency_candidate)
    candidates.extend(_extract_label_value_lines(parsed))
    candidates.extend(_extract_explicit_currency(parsed))
    return candidates


def extract_structured_fields(parsed: ParsedDocument) -> tuple[ExtractedCandidate, ...]:
    """Extract deterministic candidates, keeping all source provenance."""
    candidates = _extract_from_tables(parsed) + _extract_from_text(parsed)
    deduplicated: dict[tuple[str, str, str], ExtractedCandidate] = {}
    for candidate in candidates:
        key = (
            candidate.field_name,
            candidate.normalized_value,
            candidate.source_locator,
        )
        current = deduplicated.get(key)
        if current is None or candidate.confidence > current.confidence:
            deduplicated[key] = candidate
    return tuple(deduplicated.values())


def missing_required_fields(
    document_type: AssuranceDocumentType,
    candidates: tuple[ExtractedCandidate, ...],
) -> tuple[str, ...]:
    schema = DOCUMENT_SCHEMAS[document_type]
    present = {candidate.field_name for candidate in candidates}
    return tuple(field for field in schema.required_fields if field not in present)
