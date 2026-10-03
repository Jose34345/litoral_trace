"""Deterministic document routing for the Lacey multi-extractor pipeline."""
from __future__ import annotations

from dataclasses import dataclass
import re
import unicodedata
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
        _signal(r"\b(?:COMMERCIAL\s+INVOICE|FACTURA\s+COMERCIAL|FATURA\s+COMERCIAL)\b", 14, "header:commercial_invoice", header_only=True),
        _signal(r"\b(?:Invoice\s+(?:No\.?|Number)|Numero\s+de\s+factura|Numero\s+da\s+fatura)\b", 4, "field:invoice_number"),
        _signal(r"\b(?:Commercial\s+Line\s+Items?|Partidas\s+comerciales|Itens\s+comerciais)\b", 4, "section:commercial_line_items"),
        _signal(r"\b(?:Entered\s+Value|Valor\s+declarado|Valor\s+aduaneiro)\b", 2, "field:entered_value"),
    ),
    DocumentType.ENTRY_WORKSHEET: (
        _signal(r"\b(?:(?:U\.S\.\s+)?ENTRY\s+WORKSHEET|HOJA\s+DE\s+TRABAJO\s+DE\s+ENTRADA|PLANILHA\s+DE\s+ENTRADA)\b", 14, "header:entry_worksheet", header_only=True),
        _signal(r"\b(?:Entry\s*/\s*Filing\s+Reference|Referencia\s+de\s+entrada\s*/\s*presentacion|Referencia\s+de\s+entrada\s*/\s*registro)\b", 5, "field:filing_reference"),
        _signal(r"\b(?:Entry\s+Summary\s+Lines?|Lineas\s+de\s+resumen\s+de\s+entrada|Linhas\s+do\s+resumo\s+de\s+entrada)\b", 4, "section:entry_summary_lines"),
        _signal(r"\b(?:Importer\s+Number|Numero\s+de\s+importador|Numero\s+do\s+importador)\b", 2, "field:importer_number"),
    ),
    DocumentType.BILL_OF_LADING: (
        _signal(r"\b(?:(?:OCEAN\s+)?BILL\s+OF\s+LADING|CONOCIMIENTO\s+DE\s+EMBARQUE|CONHECIMENTO\s+DE\s+EMBARQUE)\b", 14, "header:bill_of_lading", header_only=True),
        _signal(r"\b(?:Port\s+of\s+Loading|Puerto\s+de\s+carga|Porto\s+de\s+embarque)\b", 3, "field:port_of_loading"),
        _signal(r"\b(?:Port\s+of\s+Discharge|Puerto\s+de\s+descarga|Porto\s+de\s+descarga)\b", 3, "field:port_of_discharge"),
        _signal(r"\b(?:Shipper\b.*\bConsignee|Cargador\b.*\bConsignatario|Embarcador\b.*\bConsignatario)\b", 2, "section:shipper_consignee"),
    ),
    DocumentType.ARRIVAL_NOTICE: (
        _signal(r"\b(?:ARRIVAL\s+NOTICE|AVISO\s+DE\s+LLEGADA|AVISO\s+DE\s+CHEGADA)\b", 14, "header:arrival_notice", header_only=True),
        _signal(r"\b(?:Carrier\s+Reference|Referencia\s+del\s+transportista|Referencia\s+do\s+transportador)\b", 4, "field:carrier_reference"),
        _signal(r"\b(?:Availability\b.*\bcustoms\s+release|Disponibilidad\b.*\bliberacion\s+aduanera|Disponibilidade\b.*\bliberacao\s+aduaneira)\b", 3, "field:availability"),
        _signal(r"\bETA\b", 2, "field:eta"),
    ),
    DocumentType.BOTANICAL_DECLARATION: (
        _signal(r"\b(?:BOTANICAL\b.*\b(?:LACEY\b.*)?\bDECLARATION|DECLARACION\s+BOTANICA|DECLARACAO\s+BOTANICA)\b", 14, "header:botanical_declaration", header_only=True),
        _signal(r"\b(?:Genus\b.*\bSpecies|Genero\b.*\bEspecie)\b", 4, "columns:genus_species"),
        _signal(r"\b(?:Country\s+of\s+Harvest|Pais\s+de\s+cosecha|Pais\s+de\s+colheita)\b", 4, "field:harvest_country"),
        _signal(r"\b(?:Plant\s+Quantity|Cantidad\s+de\s+material\s+vegetal|Quantidade\s+de\s+material\s+vegetal)\b", 2, "field:plant_quantity"),
    ),
    DocumentType.SUPPLIER_ORIGIN: (
        _signal(r"\b(?:SUPPLIER\s+(?:MATERIAL\s+)?ORIGIN\s+STATEMENT|DECLARACION\s+DE\s+ORIGEN\s+DEL\s+PROVEEDOR|DECLARACAO\s+DE\s+ORIGEM\s+DO\s+FORNECEDOR)\b", 14, "header:supplier_origin", header_only=True),
        _signal(r"\b(?:Statement\s+of\s+Material\s+Origin|Declaracion\s+de\s+origen\s+del\s+material|Declaracao\s+de\s+origem\s+do\s+material)\b", 5, "section:material_origin"),
        _signal(r"\b(?:Scientific\s+Name|Nombre\s+cientifico|Nome\s+cientifico)\b", 3, "field:scientific_name"),
        _signal(r"\b(?:Harvest\s+Country|Pais\s+de\s+cosecha|Pais\s+de\s+colheita)\b", 3, "field:harvest_country"),
    ),
    DocumentType.PACKING_LIST: (
        _signal(r"\b(?:PACKING\s+LIST|LISTA\s+DE\s+EMPAQUE|LISTA\s+DE\s+EMBALAGEM)\b", 14, "header:packing_list", header_only=True),
        _signal(r"\b(?:Package\s+Detail|Detalle\s+de\s+bultos|Detalhe\s+dos\s+volumes)\b", 4, "section:package_detail"),
        _signal(r"\b(?:Cartons\b.*\bPieces|Cajas\b.*\bPiezas|Caixas\b.*\bPecas)\b", 3, "columns:cartons_pieces"),
        _signal(r"\b(?:Net\s+Wt\.?\b.*\bGross\s+Wt\.?|Peso\s+neto\b.*\bPeso\s+bruto|Peso\s+liquido\b.*\bPeso\s+bruto)\b", 3, "columns:weights"),
    ),
}

_SPECIALISTS: dict[DocumentType, tuple[SpecialistRole, ...]] = {
    DocumentType.COMMERCIAL_INVOICE: (SpecialistRole.COMMERCIAL_LINES, SpecialistRole.CUSTOMS_IDENTITY),
    DocumentType.ENTRY_WORKSHEET: (SpecialistRole.CUSTOMS_IDENTITY, SpecialistRole.COMMERCIAL_LINES),
    DocumentType.BILL_OF_LADING: (SpecialistRole.LOGISTICS, SpecialistRole.CUSTOMS_IDENTITY),
    DocumentType.BOTANICAL_DECLARATION: (SpecialistRole.BOTANICAL,),
    DocumentType.SUPPLIER_ORIGIN: (SpecialistRole.BOTANICAL,),
    DocumentType.PACKING_LIST: (SpecialistRole.COMMERCIAL_LINES,),
    DocumentType.ARRIVAL_NOTICE: (SpecialistRole.LOGISTICS,),
    DocumentType.UNKNOWN: (),
}


def _normalized_text(text: str) -> str:
    collapsed = " ".join(str(text or "").replace("\x00", " ").split())
    return "".join(
        char for char in unicodedata.normalize("NFKD", collapsed)
        if not unicodedata.combining(char)
    )


def classify_page(text: str, *, filename: str = "") -> RoutingClassification:
    normalized = _normalized_text(text)
    header = normalized[:_HEADER_WINDOW_CHARS]
    scores = {kind: 0.0 for kind in DocumentType}
    content_scores = {kind: 0.0 for kind in DocumentType}
    matched = {kind: [] for kind in DocumentType}
    header_hit = {kind: False for kind in DocumentType}

    for kind, signals in _SIGNALS.items():
        for signal in signals:
            haystack = header if signal.header_only else normalized
            if signal.pattern.search(haystack):
                scores[kind] += signal.weight
                content_scores[kind] += signal.weight
                matched[kind].append(signal.label)
                header_hit[kind] = header_hit[kind] or signal.header_only

    filename_folded = _normalized_text(filename.replace("_", " ").replace("-", " "))
    hints = {
        DocumentType.COMMERCIAL_INVOICE: ("commercial invoice", "factura comercial", "fatura comercial"),
        DocumentType.ENTRY_WORKSHEET: ("entry worksheet", "hoja de trabajo de entrada", "planilha de entrada"),
        DocumentType.BILL_OF_LADING: ("bill of lading", "conocimiento de embarque", "conhecimento de embarque"),
        DocumentType.ARRIVAL_NOTICE: ("arrival notice", "aviso de llegada", "aviso de chegada"),
        DocumentType.BOTANICAL_DECLARATION: ("botanical declaration", "declaracion botanica", "declaracao botanica"),
        DocumentType.SUPPLIER_ORIGIN: ("supplier origin", "origen del proveedor", "origem do fornecedor"),
        DocumentType.PACKING_LIST: ("packing list", "lista de empaque", "lista de embalagem"),
    }
    for kind, kind_hints in hints.items():
        if any(hint.casefold() in filename_folded.casefold() for hint in kind_hints):
            scores[kind] += 1.0
            matched[kind].append("filename_hint")

    ordered = sorted(
        ((kind, score) for kind, score in scores.items() if kind is not DocumentType.UNKNOWN),
        key=lambda item: (item[1], item[0].value),
        reverse=True,
    )
    winner, top_score = ordered[0]
    second_score = ordered[1][1]
    if top_score <= 0 or content_scores[winner] <= 0:
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
    if not expected:
        return 1.0
    actual: dict[tuple[UUID, int], DocumentType] = {}
    for document in routed_documents:
        for page in document.pages:
            actual[(document.document_id, page)] = document.document_type
    correct = sum(1 for key, kind in expected.items() if actual.get(key) is kind)
    return correct / len(expected)
