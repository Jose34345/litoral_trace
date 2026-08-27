"""Deterministic classification and structured extraction for Assurance v1.

This module intentionally treats generative AI as optional enrichment, never as
the source of legal truth. Classification is evidence-based and every extracted
field keeps its original value, normalized value, confidence and source locator.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re
import unicodedata
from typing import Any

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
        "comprobante",
        "cae",
    ),
    AssuranceDocumentType.DELIVERY_NOTE: (
        "remito",
        "delivery note",
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
    ),
}

_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "document_number": (
        "numero",
        "nro",
        "n comprobante",
        "comprobante",
        "factura",
        "remito",
        "guia",
        "certificado",
        "destinacion",
        "referencia",
    ),
    "document_date": ("fecha", "fecha emision", "fecha de emision", "emision"),
    "valid_until": ("vencimiento", "valido hasta", "vigencia hasta", "fecha vencimiento"),
    "issuer_cuit": ("cuit emisor", "cuit proveedor", "cuit", "tax id proveedor"),
    "receiver_cuit": ("cuit receptor", "cuit cliente", "tax id cliente"),
    "supplier": ("proveedor", "razon social", "productor"),
    "product": ("producto", "descripcion", "mercaderia", "material"),
    "quantity": ("cantidad", "peso", "volumen", "total kg", "total tn", "total t"),
    "unit": ("unidad", "uom", "unidad medida"),
    "lot_id": ("lote", "lote origen", "partida", "batch"),
    "destination": ("destino", "pais destino", "mercado", "destination"),
    "species": ("especie", "especie cientifica", "species"),
    "hs_code": ("ncm", "hs", "hs code", "posicion arancelaria"),
    "total_amount": ("total", "importe total", "monto total"),
    "currency": ("moneda", "currency"),
    "origin": ("origen", "procedencia", "establecimiento origen"),
    "shipment_code": (
        "despacho",
        "codigo despacho",
        "codigo de despacho",
        "shipment",
        "shipment code",
    ),
    "order_id": (
        "pedido",
        "pedido id",
        "orden",
        "orden compra",
        "orden de compra",
        "purchase order",
        "po",
    ),
    "sale_reference": (
        "referencia venta",
        "referencia de venta",
        "sale reference",
        "pedido cliente",
    ),
}


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


def _canonical_header(header: str) -> str | None:
    folded = _fold(header)
    for field_name, aliases in _HEADER_ALIASES.items():
        alias_folds = {_fold(alias) for alias in aliases}
        if folded in alias_folds:
            return field_name
    return None


def _normalize_candidate(field_name: str, value: object) -> tuple[str, str]:
    original = str(value).strip()
    if not original:
        raise NormalizationError("Valor vacio.")

    if field_name in {"issuer_cuit", "receiver_cuit"}:
        return original, normalize_cuit(original)
    if field_name in {"document_date", "valid_until"}:
        normalized_date: date = normalize_date(original)
        return original, normalized_date.isoformat()
    if field_name == "quantity":
        try:
            quantity = normalize_quantity(original)
            return original, str(quantity.amount)
        except NormalizationError:
            return original, str(normalize_argentine_number(original))
    if field_name == "total_amount":
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
    value_type = (
        "date"
        if field_name in {"document_date", "valid_until"}
        else "number"
        if field_name in {"quantity", "total_amount"}
        else "identifier"
        if field_name in {
            "document_number",
            "issuer_cuit",
            "receiver_cuit",
            "lot_id",
            "hs_code",
            "shipment_code",
            "order_id",
            "sale_reference",
        }
        else "text"
    )
    return ExtractedCandidate(
        field_name=field_name,
        original_value=original,
        normalized_value=normalized,
        value_type=value_type,
        confidence=confidence,
        source_page=page,
        source_locator=locator,
    )


def _extract_from_tables(parsed: ParsedDocument) -> list[ExtractedCandidate]:
    candidates: list[ExtractedCandidate] = []
    for table_index, table in enumerate(parsed.tables, start=1):
        mapped_headers = {
            header: _canonical_header(header)
            for header in table.headers
        }
        for row_index, row in enumerate(table.rows, start=1):
            unit_value = None
            for header, canonical in mapped_headers.items():
                if canonical == "unit" and row.get(header) not in {None, ""}:
                    unit_value = row.get(header)
            for column_index, header in enumerate(table.headers, start=1):
                canonical = mapped_headers[header]
                if canonical is None:
                    continue
                value = row.get(header)
                if value is None:
                    continue
                locator = (
                    f"{table.source.locator or table.name};data_row:{row_index};"
                    f"column:{column_index};header:{header}"
                )
                candidate = _candidate_from_table(
                    canonical,
                    value,
                    locator=locator,
                    page=table.source.page,
                    confidence=0.98,
                )
                if candidate is not None:
                    candidates.append(candidate)
                    if canonical == "quantity" and unit_value:
                        unit_candidate = _candidate_from_table(
                            "unit",
                            unit_value,
                            locator=locator + ";paired_unit",
                            page=table.source.page,
                            confidence=0.98,
                        )
                        if unit_candidate is not None:
                            candidates.append(unit_candidate)
    return candidates


_TEXT_PATTERNS: dict[str, re.Pattern[str]] = {
    "issuer_cuit": re.compile(r"(?:CUIT(?:\s+emisor|\s+proveedor)?\s*[:#-]?\s*)(\d{2}-?\d{8}-?\d)", re.I),
    "document_date": re.compile(r"(?:fecha(?:\s+de\s+emision)?\s*[:#-]?\s*)(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", re.I),
    "valid_until": re.compile(r"(?:vencimiento|valido\s+hasta|vigencia\s+hasta)\s*[:#-]?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", re.I),
    "document_number": re.compile(r"(?:factura|remito|guia|certificado|destinacion|nro\.?|numero)\s*(?:[A-Z]\s*)?[:#-]?\s*([A-Z0-9][A-Z0-9./-]{3,})", re.I),
    "hs_code": re.compile(r"(?:NCM|HS(?:\s+CODE)?|posicion\s+arancelaria)\s*[:#-]?\s*([0-9.]{4,14})", re.I),
    "destination": re.compile(r"(?:destino|pais\s+destino|destination)\s*[:#-]?\s*([^\n\r]{2,80})", re.I),
    "shipment_code": re.compile(r"(?:codigo\s+de\s+despacho|codigo\s+despacho|shipment\s+code)\s*[:#-]?\s*([A-Z0-9][A-Z0-9./-]{2,})", re.I),
    "order_id": re.compile(r"(?:orden\s+de\s+compra|purchase\s+order|pedido)\s*[:#-]?\s*([A-Z0-9][A-Z0-9./-]{2,})", re.I),
}


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
            locator=f"pdf:digital-text;span:{match.start(1)}-{match.end(1)}",
            page=None,
            confidence=0.90,
        )
        if candidate is not None:
            candidates.append(candidate)
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
