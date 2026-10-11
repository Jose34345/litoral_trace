"""Assurance V2: immutable, source-verified observations, never canonical facts.

This module is an opt-in, pure domain adapter. Caller owns source bytes, parsed
layout, source-set fencing, storage and human-review decisions. No provider's
verification flag, coordinates or line identity is accepted as source authority.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
from hashlib import sha256
import json
import math
import re
from typing import Iterable, Mapping, Sequence

from .domain import AdmittedCandidate, BoundingBox, EvidenceClass, LayoutBlock, ParsedLayout
from .multi_agent.contracts import CandidateEnvelope
from .semantic_graph import semantic_normalize

CLAIM_CONTRACT_VERSION = "assurance.claim.v1"
NORMALIZER_VERSION = "lacey.semantic_normalize.v1"


class SupportStatus(str, Enum):
    SUPPORTED = "SUPPORTED"  # Source-literal support ONLY; not human approval.
    UNSUPPORTED = "UNSUPPORTED"
    AMBIGUOUS = "AMBIGUOUS"
    OUT_OF_SCOPE = "OUT_OF_SCOPE"


class AssociationStatus(str, Enum):
    EXPLICIT_SKU = "EXPLICIT_SKU"
    SOURCE_LOCAL_LINE = "SOURCE_LOCAL_LINE"
    SOURCE_ROW = "SOURCE_ROW"
    UNBOUND = "UNBOUND"


class ClaimRelation(str, Enum):
    CORROBORATED = "CORROBORATED"
    CONFLICT = "CONFLICT"
    UNRESOLVED = "UNRESOLVED"


@dataclass(frozen=True, slots=True)
class SheetCell:
    sheet: str
    row: int
    column: int
    text: str

    def __post_init__(self) -> None:
        if not self.sheet.strip() or self.row < 1 or self.column < 1:
            raise ValueError("Sheet cell requires a named sheet and 1-indexed row/column")


@dataclass(frozen=True, slots=True)
class SourceEvidenceDocument:
    """The exact source bytes and parser-produced evidence, not a model transcript.

    Parser output is an external trusted input: consumers must derive layout/cells
    from the same original_bytes. No source contents are written to claim JSON.
    """
    document_id: str
    original_bytes: bytes = field(repr=False)
    layout: ParsedLayout | None = None
    cells: tuple[SheetCell, ...] = ()
    historical: bool = False
    document_version: str | None = None

    def __post_init__(self) -> None:
        if (not isinstance(self.document_id, str) or not self.document_id.strip()
                or not isinstance(self.original_bytes, bytes) or not self.original_bytes):
            raise ValueError("Document needs identity and immutable original bytes")
        if (self.layout is None) == (not self.cells):
            raise ValueError("Supply exactly one parsed layout or nonempty sheet cells")
        if self.layout is not None and self.layout.page_count < 1:
            raise ValueError("Invalid source page count")
        if len({(c.sheet, c.row, c.column) for c in self.cells}) != len(self.cells):
            raise ValueError("Duplicate spreadsheet cell locator")

    @property
    def sha256(self) -> str:
        return sha256(self.original_bytes).hexdigest()


@dataclass(frozen=True, slots=True)
class ClaimContext:
    organization_id: str
    operation_id: str
    source_set_revision_id: str
    extraction_run_id: str

    def __post_init__(self) -> None:
        if any(not isinstance(x, str) or not x.strip() for x in (
            self.organization_id, self.operation_id,
            self.source_set_revision_id, self.extraction_run_id,
        )):
            raise ValueError("Claim context must identify tenant, operation, revision and run")


@dataclass(frozen=True, slots=True)
class SourceLocator:
    kind: str  # TEXT_SPAN | SHEET_CELL
    page: int | None = None
    block_id: str | None = None
    start: int | None = None
    end: int | None = None
    bbox: BoundingBox | None = None
    sheet: str | None = None
    row: int | None = None
    column: int | None = None

    def __post_init__(self) -> None:
        if self.kind == "TEXT_SPAN":
            if not (self.page is not None and self.page >= 1
                    and self.block_id and self.start is not None
                    and self.end is not None and 0 <= self.start < self.end):
                raise ValueError("Invalid exact text-span anchor")
            if any(v is not None for v in (self.sheet, self.row, self.column)):
                raise ValueError("Text span cannot be a sheet cell")
        elif self.kind == "SHEET_CELL":
            if not (self.sheet and self.row is not None and self.row >= 1
                    and self.column is not None and self.column >= 1):
                raise ValueError("Invalid sheet-cell anchor")
            if any(v is not None for v in (self.page, self.block_id, self.start, self.end, self.bbox)):
                raise ValueError("Sheet cell cannot be a text span")
        else:
            raise ValueError("Unknown locator kind")


@dataclass(frozen=True, slots=True)
class SourceLinkedClaim:
    organization_id: str
    operation_id: str
    source_set_revision_id: str
    document_id: str
    original_document_sha256: str
    document_version: str | None
    extraction_run_id: str
    field_name: str
    original_value: str
    normalized_value: str | None
    interpretation_value: str | None
    interpretation_status: str
    normalizer_version: str | None
    entity_type: str
    subject_id: str | None
    line_reference: str | None
    sku: str | None
    association_status: AssociationStatus
    locator: SourceLocator | None
    source_excerpt: str | None
    confidence: float
    extractor_name: str
    extractor_version: str
    evidence_class: str
    extractor_score: float | None
    support_status: SupportStatus
    issues: tuple[str, ...]
    observation_id: str
    claim_id: str
    contract_version: str = CLAIM_CONTRACT_VERSION
    candidate_state: str = "CANDIDATE"

    def __post_init__(self) -> None:
        if (self.contract_version != CLAIM_CONTRACT_VERSION
                or self.candidate_state != "CANDIDATE"
                or self.interpretation_status != "PROPOSED"):
            raise ValueError("Extraction cannot grant review or canonical authority")
        if any(not isinstance(x, str) or not x.strip() for x in (
            self.organization_id, self.operation_id, self.source_set_revision_id,
            self.document_id, self.extraction_run_id, self.field_name,
            self.entity_type, self.extractor_name, self.extractor_version,
        )):
            raise ValueError("Claim identifiers, field, entity and extractor must be present")
        if not math.isfinite(self.confidence) or not 0 <= self.confidence <= 1:
            raise ValueError("Confidence must be finite and in [0, 1]")
        if not re.fullmatch("[0-9a-f]{64}", self.original_document_sha256):
            raise ValueError("Missing SHA256 of original bytes")
        if self.support_status is SupportStatus.SUPPORTED and (self.locator is None or not self.source_excerpt):
            raise ValueError("SUPPORTED requires a verifiable source locator")
        if self.association_status is AssociationStatus.EXPLICIT_SKU and not self.sku:
            raise ValueError("Explicit SKU binding requires a SKU")

    def to_dict(self) -> dict[str, object]:
        """Primitive, JSON-safe, versioned wire shape for the next agent."""
        return json.loads(json.dumps(asdict(self), ensure_ascii=False))

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, ensure_ascii=False)


@dataclass(frozen=True, slots=True)
class ClaimGroup:
    field_name: str
    subject_key: str
    relation: ClaimRelation
    claims: tuple[SourceLinkedClaim, ...]
    distinct_normalized_values: tuple[str, ...]


def _identity(*parts: object) -> str:
    return sha256(json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()


def _unique_exact_span(value: str, source: str) -> tuple[int, int] | None:
    """One literal token-aligned occurrence: '500' is not present in '5000'."""
    if not value:
        return None
    offsets: list[int] = []
    pos = source.find(value)
    while pos >= 0:
        end = pos + len(value)
        prev = source[pos - 1] if pos > 0 else ""
        next_char = source[end] if end < len(source) else ""
        numeric_prefix = (value[0].isdigit() and (
            prev in {"-", "/", "_"} or
            (prev in {".", ","} and pos > 1 and source[pos - 2].isdigit())
        ))
        numeric_suffix = (value[-1].isdigit() and (
            next_char in {"-", "/", "_"} or
            (next_char in {".", ","} and end + 1 < len(source)
             and source[end + 1].isdigit())
        ))
        left_ok = not (value[0].isalnum() and pos > 0 and
                       (prev.isalnum() or prev == "_" or numeric_prefix))
        right_ok = not (value[-1].isalnum() and end < len(source) and
                        (next_char.isalnum() or next_char == "_" or numeric_suffix))
        if left_ok and right_ok:
            offsets.append(pos)
        pos = source.find(value, pos + 1)
    return (offsets[0], offsets[0] + len(value)) if len(offsets) == 1 else None


def _locate_layout(
    document: SourceEvidenceDocument, *, page: int, original_value: str,
    source_text: str, block_id: str | None = None,
) -> tuple[SourceLocator | None, str | None, str | None]:
    assert document.layout is not None
    if page < 1 or page > document.layout.page_count:
        return None, None, "PAGE_OUT_OF_RANGE"
    blocks = [b for b in document.layout.blocks if b.page == page
              and (block_id is None or b.block_id == block_id)]
    # Source quote and candidate value must both be exact in the same physical
    # block. A document/page-wide token presence is not line-level evidence.
    exact = [(b, _unique_exact_span(original_value, b.text))
             for b in blocks if source_text and source_text in b.text
             and _unique_exact_span(original_value, b.text) is not None]
    if not exact:
        return None, None, "SOURCE_TEXT_OR_VALUE_NOT_FOUND"
    if len(exact) != 1:
        return None, None, "AMBIGUOUS_SOURCE_ANCHOR"
    block, span = exact[0]
    assert span is not None
    return SourceLocator("TEXT_SPAN", page=page, block_id=block.block_id,
                         start=span[0], end=span[1], bbox=block.bbox), block.text, None


def _locate_cell(
    document: SourceEvidenceDocument, *, sheet: str, row: int, column: int,
    original_value: str,
) -> tuple[SourceLocator | None, str | None, str | None]:
    match = next((c for c in document.cells if (c.sheet, c.row, c.column)
                  == (sheet, row, column)), None)
    if match is None or _unique_exact_span(original_value, match.text) is None:
        return None, None, "CELL_OR_VALUE_NOT_FOUND"
    return SourceLocator("SHEET_CELL", sheet=sheet, row=row, column=column), match.text, None


def _binding(
    *, source_text: str | None, block: LayoutBlock | None,
    supplied_key: str | None, sheet_cell: SheetCell | None = None,
) -> tuple[AssociationStatus, str | None, str | None, str | None]:
    """Identity is SOURCE-LOCAL unless the exact SKU occurs in the SAME anchor.

    Never accept a fused/derived SKU merely because the candidate carries it.
    """
    text = source_text or ""
    if supplied_key and supplied_key.startswith("SKU:"):
        sku = supplied_key[4:]
        if sku and re.search(r"(?<![\w./-])" + re.escape(sku) + r"(?![\w./-])", text, re.I):
            return AssociationStatus.EXPLICIT_SKU, None, sku.upper(), "SKU:" + sku.upper()
    if sheet_cell is not None:
        key = f"{sheet_cell.sheet}:R{sheet_cell.row}"
        return AssociationStatus.SOURCE_ROW, key, None, None
    if block is not None and block.table_id and block.row_index is not None:
        key = f"{block.table_id}:R{block.row_index}"
        return AssociationStatus.SOURCE_ROW, key, None, None
    if supplied_key and supplied_key.startswith("LINE:"):
        number = supplied_key[5:]
        if re.search(r"\b(?:line|item)\s*(?:no\.?|number|#|:)?\s*" + re.escape(number) + r"\b", text, re.I):
            return AssociationStatus.SOURCE_LOCAL_LINE, "LINE:" + number, None, None
    return AssociationStatus.UNBOUND, None, None, None


def _create(
    *, context: ClaimContext, document: SourceEvidenceDocument,
    field_name: str, original_value: str, interpretation_value: str | None,
    entity_type: str, confidence: float, extractor_name: str, extractor_version: str,
    evidence_class: str, locator: SourceLocator | None, source_excerpt: str | None,
    anchor_error: str | None, supplied_line_key: str | None = None,
    normalized: bool = True, extractor_score: float | None = None,
) -> SourceLinkedClaim:
    block = None
    cell = None
    if locator and locator.kind == "TEXT_SPAN" and document.layout:
        block = next(b for b in document.layout.blocks if b.page == locator.page
                     and b.block_id == locator.block_id)
    elif locator and locator.kind == "SHEET_CELL":
        cell = next(c for c in document.cells if (c.sheet, c.row, c.column)
                    == (locator.sheet, locator.row, locator.column))
    association, line_ref, sku, subject_id = _binding(
        source_text=source_excerpt if locator else None,
        block=block, sheet_cell=cell, supplied_key=supplied_line_key if locator else None,
    )
    if subject_id is None and line_ref and locator is not None:
        subject_id = f"{document.document_id}:{line_ref}"
    issues = []
    if anchor_error:
        issues.append(anchor_error)
    if document.historical:
        issues.append("HISTORICAL_DOCUMENT")
    if evidence_class != EvidenceClass.EXPLICIT.value:
        issues.append("NON_LITERAL_INFERENCE")
    if supplied_line_key and association is AssociationStatus.UNBOUND:
        issues.append("UNPROVEN_LINE_ASSOCIATION")
    if extractor_score is not None:
        issues.append("ENGINE2_SCORE_IS_RANKING_NOT_CONFIDENCE")
    if locator is None:
        status = SupportStatus.AMBIGUOUS if anchor_error == "AMBIGUOUS_SOURCE_ANCHOR" else SupportStatus.UNSUPPORTED
    elif document.historical:
        status = SupportStatus.OUT_OF_SCOPE
    elif evidence_class != EvidenceClass.EXPLICIT.value:
        status = SupportStatus.UNSUPPORTED
    else:
        status = SupportStatus.SUPPORTED
    normalized_value = semantic_normalize(field_name, original_value) if normalized and original_value else None
    observation = _identity(context.organization_id, context.operation_id,
                            context.source_set_revision_id, document.document_id,
                            document.sha256, field_name, original_value,
                            locator.kind if locator else None, asdict(locator) if locator else None,
                            entity_type, subject_id)
    return SourceLinkedClaim(
        organization_id=context.organization_id, operation_id=context.operation_id,
        source_set_revision_id=context.source_set_revision_id,
        document_id=document.document_id, original_document_sha256=document.sha256,
        document_version=document.document_version, extraction_run_id=context.extraction_run_id,
        field_name=field_name, original_value=original_value,
        normalized_value=normalized_value, interpretation_value=interpretation_value,
        interpretation_status="PROPOSED",
        normalizer_version=NORMALIZER_VERSION if normalized_value is not None else None,
        entity_type=entity_type, subject_id=subject_id, line_reference=line_ref, sku=sku,
        association_status=association, locator=locator, source_excerpt=source_excerpt,
        confidence=confidence, extractor_name=extractor_name,
        extractor_version=extractor_version, evidence_class=evidence_class,
        extractor_score=extractor_score, support_status=status,
        issues=tuple(issues), observation_id=observation,
        claim_id=_identity(observation, context.extraction_run_id, extractor_name,
                           extractor_version, interpretation_value),
    )


def from_specialist_envelope(
    envelope: CandidateEnvelope, *, context: ClaimContext,
    document: SourceEvidenceDocument,
    entity_type: str | None = None,
) -> SourceLinkedClaim:
    if str(envelope.document_id) != document.document_id:
        raise ValueError("Envelope document identity mismatch")
    if document.layout is None:
        raise ValueError("PDF specialist requires trusted parsed layout")
    c = envelope.candidate
    locator, source, issue = _locate_layout(
        document, page=c.page, original_value=c.value, source_text=c.source_text)
    return _create(
        context=context, document=document, field_name=c.field_key, original_value=c.value,
        interpretation_value=c.normalized_value,
        entity_type=entity_type or field_entity_type(c.field_key),
        confidence=c.confidence, extractor_name=c.provider, extractor_version=c.model,
        evidence_class=c.evidence_class.value, locator=locator,
        source_excerpt=source, anchor_error=issue, supplied_line_key=envelope.line_item_key,
    )


def from_engine2_candidate(
    candidate: AdmittedCandidate, *, context: ClaimContext,
    document: SourceEvidenceDocument, entity_type: str | None = None,
) -> SourceLinkedClaim:
    if document.layout is None:
        raise ValueError("Engine 2 candidate requires trusted parsed layout")
    block = candidate.raw.source_block
    locator, source, issue = _locate_layout(
        document, page=block.page, original_value=candidate.raw.raw_text,
        source_text=candidate.provenance.source_text, block_id=block.block_id)
    return _create(
        context=context, document=document, field_name=candidate.raw.field_key,
        original_value=candidate.raw.raw_text,
        interpretation_value=candidate.raw.normalized_value,
        entity_type=entity_type or field_entity_type(candidate.raw.field_key),
        confidence=0.0, extractor_name=candidate.raw.extractor_name,
        extractor_version=candidate.raw.extractor_version,
        evidence_class=candidate.raw.evidence_class.value, locator=locator,
        source_excerpt=source, anchor_error=issue, extractor_score=candidate.score,
    )


def from_spreadsheet_cell(
    *, context: ClaimContext, document: SourceEvidenceDocument,
    sheet: str, row: int, column: int, field_name: str,
    original_value: str, entity_type: str = "MERCHANDISE_LINE",
    supplied_sku: str | None = None,
    confidence: float = 1.0, extractor_version: str = "deterministic-sheet-v1",
) -> SourceLinkedClaim:
    if not document.cells:
        raise ValueError("Spreadsheet claim requires parsed source cells")
    locator, source, issue = _locate_cell(
        document, sheet=sheet, row=row, column=column, original_value=original_value)
    return _create(
        context=context, document=document, field_name=field_name,
        original_value=original_value, interpretation_value=None,
        entity_type=entity_type, confidence=confidence,
        extractor_name="sheet_parser", extractor_version=extractor_version,
        evidence_class=EvidenceClass.EXPLICIT.value,
        locator=locator, source_excerpt=source, anchor_error=issue,
        supplied_line_key="SKU:" + supplied_sku if supplied_sku else None,
    )


def group_claims(claims: Iterable[SourceLinkedClaim]) -> tuple[ClaimGroup, ...]:
    """Preserve all claims; do not pick a winner or join unproven lines."""
    groups: dict[tuple[str, ...], list[SourceLinkedClaim]] = {}
    for claim in claims:
        # Exact SKU is packet-global; row/line IDs are source-local. Unbound
        # observations remain individually scoped even if values match.
        subject_key = claim.subject_id or claim.observation_id
        groups.setdefault((
            claim.organization_id, claim.operation_id, claim.source_set_revision_id,
            claim.entity_type, claim.field_name, subject_key,
        ), []).append(claim)
    output = []
    for key, values in sorted(groups.items()):
        supported = [c for c in values if c.support_status is SupportStatus.SUPPORTED]
        distinct = tuple(sorted({c.normalized_value or c.original_value for c in supported}))
        unique_observations = {c.observation_id for c in supported}
        relation = (ClaimRelation.CONFLICT if len(distinct) > 1 else
                    ClaimRelation.CORROBORATED if len(unique_observations) > 1 else
                    ClaimRelation.UNRESOLVED)
        output.append(ClaimGroup(
            field_name=key[4], subject_key=key[5], relation=relation,
            claims=tuple(values), distinct_normalized_values=distinct,
        ))
    return tuple(output)


def verify_claim_anchor(claim: SourceLinkedClaim, document: SourceEvidenceDocument) -> bool:
    """Independent consumer-side check before interpreting SUPPORTED as evidence."""
    if (claim.document_id != document.document_id
            or claim.original_document_sha256 != document.sha256 or claim.locator is None):
        return False
    loc = claim.locator
    if loc.kind == "TEXT_SPAN" and document.layout is not None:
        return any(b.page == loc.page and b.block_id == loc.block_id
                   and b.text[loc.start:loc.end] == claim.original_value
                   and b.text == claim.source_excerpt for b in document.layout.blocks)
    if loc.kind == "SHEET_CELL" and document.cells:
        return any((c.sheet, c.row, c.column) == (loc.sheet, loc.row, loc.column)
                   and claim.original_value in c.text and c.text == claim.source_excerpt
                   for c in document.cells)
    return False


def serialize_claims(claims: Sequence[SourceLinkedClaim]) -> dict[str, object]:
    return {"contract_version": CLAIM_CONTRACT_VERSION,
            "claims": [claim.to_dict() for claim in claims]}


_PLANT_FIELDS = frozenset({
    "genus", "species", "country_of_harvest", "plant_quantity",
    "metric_unit", "article_component",
})
_MERCHANDISE_FIELDS = frozenset({"description", "hts_code", "entered_value"})


def field_entity_type(field_name: str) -> str:
    """A typed default only. This never discovers a product or creates a link."""
    if field_name in _PLANT_FIELDS:
        return "PLANT_COMPONENT"
    if field_name in _MERCHANDISE_FIELDS:
        return "MERCHANDISE_LINE"
    return "SHIPMENT"


def claims_from_engine2_resolution(
    resolution: "DocumentResolution", *, context: ClaimContext,
    document: SourceEvidenceDocument,
) -> tuple[SourceLinkedClaim, ...]:
    """Lossless projection of ALL Engine 2 candidates, including losing/conflicting."""
    if document.layout is None or resolution.layout != document.layout:
        raise ValueError("Resolution layout must match the original parsed document")
    return tuple(
        from_engine2_candidate(
            candidate, context=context, document=document,
            entity_type=field_entity_type(field_name),
        )
        for field_name, resolved in resolution.fields.items()
        for candidate in resolved.candidates
    )


def claims_from_specialist_result(
    result: "MultiAgentExtractionResult", *, context: ClaimContext,
    documents: Mapping[str, SourceEvidenceDocument],
) -> tuple[SourceLinkedClaim, ...]:
    """Project raw specialist envelopes, not the fusion winners.

    Deliberately includes admission-rejected and conflicting candidates as
    unsupported/ambiguous where appropriate. No canonical publishing or storage.
    The caller must provide the matching original-document bytes/layout.
    """
    output: list[SourceLinkedClaim] = []
    for specialist in result.specialist_results:
        for envelope in specialist.candidates:
            doc = documents.get(str(envelope.document_id))
            if doc is None:
                raise ValueError("Missing original source document for candidate")
            output.append(from_specialist_envelope(
                envelope, context=context, document=doc,
                entity_type=field_entity_type(envelope.candidate.field_key),
            ))
    return tuple(output)
