"""Deterministic document routing for the Lacey multi-extractor pipeline.

The router intentionally prefers explainable lexical/layout signals.  A bounded external
classifier may be injected for genuinely ambiguous pages, but it is never called when
rule-based confidence is already sufficient.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable, Mapping, Protocol
from uuid import UUID

from .contracts import DocumentType, RoutedDocument, SpecialistRole


DEFAULT_LLM_ESCALATION_THRESHOLD = 0.72
_HEADER_WINDOW_CHARS = 1200


@dataclass(frozen=True, slots=True)
class RoutingClassification:
    document_type: DocumentType
    confidence: float
    signals: tuple[str, ...]


class AmbiguousDocumentClassifier(Protocol):
    """Bounded fallback used only after deterministic routing is inconclusive."""

    def classify(
        self,
        *,
        filename: str,
        page_number: int,
        text: str,
        deterministic: RoutingClassification,
    ) -> RoutingClassification: ...


@dataclass(frozen=True, slots=True)
class RoutingAssignment:
    document: RoutedDocument
    specialists: tuple[SpecialistRole, ...]


@dataclass(frozen=True, slots=True)
class RoutingPlan:
    assignments: tuple[RoutingAssignment, ...]

    @property
    def documents(self) -> tuple[RoutedDocument, ...]:
        return tuple(assignment.document for assignment in self.assignments)

    def specialists_for(self, document: RoutedDocument) -> tuple[SpecialistRole, ...]:
        for assignment in self.assignments:
            if assignment.document == document:
                return assignment.specialists
        return ()


@dataclass(frozen=True, slots=True)
class _Signal:
    pattern: re.Pattern[str]
    weight: float
    label: str
    header_only: bool = False


def _signal(pattern: str, weight: float, label: str, *, header_only: bool = False) -> _Signal:
    return _Signal(re.compile(pattern, re.IGNORECASE | re.MULTILINE), weight, label, header_only)


_SIGNALS: dict[DocumentType, tuple[_Signal, ...]] = {
    DocumentType.COMMERCIAL_INVOICE: (
        _signal(r"\bCOMMERCIAL\s+INVOICE\b", 14, "header:commercial_invoice", header_only=True),
        _signal(r"\bInvoice\s+(?:No\.?|Number)\b", 4, "field:invoice_number"),
        _signal(r"\bCommercial\s+Line\s+Items?\b", 4, "section:commercial_line_items"),
        _signal(r"\bEntered\s+Value\b", 2, "field:entered_value"),
    ),
    DocumentType.ENTRY_WORKSHEET: (
        _signal(r"\b(?:U\.S\.\s+)?ENTRY\s+WORKSHEET\b", 14, "header:entry_worksheet", header_only=True),
        _signal(r"\bEntry\s*/\s*Filing\s+Reference\b", 5, "field:filing_reference"),
        _signal(r"\bEntry\s+Summary\s+Lines?\b", 4, "section:entry_summary_lines"),
        _signal(r"\bImporter\s+Number\b", 2, "field:importer_number"),
    ),
    DocumentType.BILL_OF_LADING: (
        _signal(r"\b(?:OCEAN\s+)?BILL\s+OF\s+LADING\b", 14, "header:bill_of_lading", header_only=True),
        _signal(r"\bPort\s+of\s+Loading\b", 3, "field:port_of_loading"),
        _signal(r"\bPort\s+of\s+Discharge\b", 3, "field:port_of_discharge"),
        _signal(r"\bShipper\b.*\bConsignee\b", 2, "section:shipper_consignee"),
    ),
    DocumentType.ARRIVAL_NOTICE: (
        _signal(r"\bARRIVAL\s+NOTICE\b", 14, "header:arrival_notice", header_only=True),
        _signal(r"\bCarrier\s+Reference\b", 4, "field:carrier_reference"),
        _signal(r"\bAvailability\b.*\bcustoms\s+release\b", 3, "field:availability"),
        _signal(r"\bETA\b", 2, "field:eta"),
    ),
    DocumentType.BOTANICAL_DECLARATION: (
        _signal(r"\bBOTANICAL\b.*\b(?:LACEY\b.*)?\bDECLARATION\b", 14, "header:botanical_declaration", header_only=True),
        _signal(r"\bGenus\b.*\bSpecies\b", 4, "columns:genus_species"),
        _signal(r"\bCountry\s+of\s+Harvest\b", 4, "field:harvest_country"),
        _signal(r"\bPlant\s+Quantity\b", 2, "field:plant_quantity"),
    ),
    DocumentType.SUPPLIER_ORIGIN: (
        _signal(r"\bSUPPLIER\s+(?:MATERIAL\s+)?ORIGIN\s+STATEMENT\b", 14, "header:supplier_origin", header_only=True),
        _signal(r"\bStatement\s+of\s+Material\s+Origin\b", 5, "section:material_origin"),
        _signal(r"\bScientific\s+Name\b", 3, "field:scientific_name"),
        _signal(r"\bHarvest\s+Country\b", 3, "field:harvest_country"),
    ),
    DocumentType.PACKING_LIST: (
        _signal(r"\bPACKING\s+LIST\b", 14, "header:packing_list", header_only=True),
        _signal(r"\bPackage\s+Detail\b", 4, "section:package_detail"),
        _signal(r"\bCartons\b.*\bPieces\b", 3, "columns:cartons_pieces"),
        _signal(r"\bNet\s+Wt\.?\b.*\bGross\s+Wt\.?\b", 3, "columns:weights"),
    ),
}


_SPECIALISTS: dict[DocumentType, tuple[SpecialistRole, ...]] = {
    DocumentType.COMMERCIAL_INVOICE: (
        SpecialistRole.COMMERCIAL_LINES,
        SpecialistRole.CUSTOMS_IDENTITY,
    ),
    DocumentType.ENTRY_WORKSHEET: (
        SpecialistRole.CUSTOMS_IDENTITY,
        SpecialistRole.COMMERCIAL_LINES,
    ),
    DocumentType.BILL_OF_LADING: (
        SpecialistRole.LOGISTICS,
        SpecialistRole.CUSTOMS_IDENTITY,
    ),
    DocumentType.BOTANICAL_DECLARATION: (SpecialistRole.BOTANICAL,),
    DocumentType.SUPPLIER_ORIGIN: (SpecialistRole.BOTANICAL,),
    DocumentType.PACKING_LIST: (SpecialistRole.COMMERCIAL_LINES,),
    # The routing table in the Phase 1 brief omits Arrival Notice, while the later
    # authority table makes it a primary ETA source and LOGISTICS owns ETA.  Route it
    # only to LOGISTICS so that omission does not discard authoritative ETA evidence.
    DocumentType.ARRIVAL_NOTICE: (SpecialistRole.LOGISTICS,),
    DocumentType.UNKNOWN: (),
}


def _normalized_text(text: str) -> str:
    return " ".join(str(text or "").replace("\x00", " ").split())


def classify_page(text: str, *, filename: str = "") -> RoutingClassification:
    """Classify one page using deterministic, explainable signals only."""
    normalized = _normalized_text(text)
    header = normalized[:_HEADER_WINDOW_CHARS]
    scores: dict[DocumentType, float] = {kind: 0.0 for kind in DocumentType}
    matched: dict[DocumentType, list[str]] = {kind: [] for kind in DocumentType}
    header_hit: dict[DocumentType, bool] = {kind: False for kind in DocumentType}

    for kind, signals in _SIGNALS.items():
        for signal in signals:
            haystack = header if signal.header_only else normalized
            if signal.pattern.search(haystack):
                scores[kind] += signal.weight
                matched[kind].append(signal.label)
                header_hit[kind] = header_hit[kind] or signal.header_only

    filename_folded = filename.replace("_", " ").replace("-", " ")
    for kind, hints in {
        DocumentType.COMMERCIAL_INVOICE: ("commercial invoice",),
        DocumentType.ENTRY_WORKSHEET: ("entry worksheet",),
        DocumentType.BILL_OF_LADING: ("bill of lading",),
        DocumentType.ARRIVAL_NOTICE: ("arrival notice",),
        DocumentType.BOTANICAL_DECLARATION: ("botanical declaration",),
        DocumentType.SUPPLIER_ORIGIN: ("supplier origin",),
        DocumentType.PACKING_LIST: ("packing list",),
    }.items():
        if any(hint.casefold() in filename_folded.casefold() for hint in hints):
            scores[kind] += 1.0
            matched[kind].append("filename_hint")

    ordered = sorted(
        ((kind, score) for kind, score in scores.items() if kind is not DocumentType.UNKNOWN),
        key=lambda item: (item[1], item[0].value),
        reverse=True,
    )
    winner, top_score = ordered[0]
    second_score = ordered[1][1]
    if top_score <= 0:
        return RoutingClassification(DocumentType.UNKNOWN, 0.0, ())

    margin = max(0.0, top_score - second_score)
    if header_hit[winner]:
        confidence = min(0.99, 0.88 + min(0.08, margin / 100.0) + min(0.03, top_score / 1000.0))
    else:
        confidence = min(0.86, 0.50 + min(0.20, top_score / 40.0) + min(0.16, margin / 50.0))

    return RoutingClassification(winner, confidence, tuple(matched[winner]))


def route_document(
    *,
    document_id: UUID,
    page_texts: Mapping[int, str],
    filename: str = "",
    ambiguous_classifier: AmbiguousDocumentClassifier | None = None,
    escalation_threshold: float = DEFAULT_LLM_ESCALATION_THRESHOLD,
) -> tuple[RoutedDocument, ...]:
    """Route a document, splitting page ranges when a PDF contains mixed document types."""
    grouped_pages: dict[DocumentType, list[int]] = {}
    grouped_confidence: dict[DocumentType, list[float]] = {}
    grouped_signals: dict[DocumentType, list[str]] = {}

    for page_number in sorted(page_texts):
        text = page_texts[page_number]
        classification = classify_page(text, filename=filename)
        if classification.confidence < escalation_threshold and ambiguous_classifier is not None:
            fallback = ambiguous_classifier.classify(
                filename=filename,
                page_number=page_number,
                text=text,
                deterministic=classification,
            )
            if fallback.document_type in DocumentType and fallback.confidence >= classification.confidence:
                classification = RoutingClassification(
                    fallback.document_type,
                    min(1.0, max(0.0, fallback.confidence)),
                    classification.signals + tuple(f"llm:{signal}" for signal in fallback.signals),
                )

        grouped_pages.setdefault(classification.document_type, []).append(page_number)
        grouped_confidence.setdefault(classification.document_type, []).append(classification.confidence)
        grouped_signals.setdefault(classification.document_type, []).extend(classification.signals)

    routed: list[RoutedDocument] = []
    for kind in sorted(grouped_pages, key=lambda item: min(grouped_pages[item])):
        confidences = grouped_confidence[kind]
        signals = tuple(dict.fromkeys(grouped_signals[kind]))
        routed.append(
            RoutedDocument(
                document_id=document_id,
                document_type=kind,
                pages=tuple(grouped_pages[kind]),
                confidence=min(confidences) if confidences else 0.0,
                signals=signals,
            )
        )
    return tuple(routed)


def build_routing_plan(documents: Iterable[RoutedDocument]) -> RoutingPlan:
    """Attach each routed document to its deterministic specialist fan-out."""
    return RoutingPlan(
        assignments=tuple(
            RoutingAssignment(document=document, specialists=_SPECIALISTS[document.document_type])
            for document in documents
        )
    )


def router_document_accuracy(
    expected: Mapping[tuple[UUID, int], DocumentType],
    routed_documents: Iterable[RoutedDocument],
) -> float:
    """Return page-level routing accuracy for a labeled packet."""
    if not expected:
        return 1.0
    actual: dict[tuple[UUID, int], DocumentType] = {}
    for document in routed_documents:
        for page in document.pages:
            actual[(document.document_id, page)] = document.document_type
    correct = sum(1 for key, kind in expected.items() if actual.get(key) is kind)
    return correct / len(expected)
