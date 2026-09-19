"""Stable, infrastructure-free JSON contracts for Lacey Engine resolutions."""
from __future__ import annotations

from typing import Any

from .domain import (AdmittedCandidate, BoundingBox, DocumentResolution, DocumentSection,
                     DocumentType, EvidenceClass, FieldStatus, LayoutBlock,
                     LayoutStructureType, ParsedLayout, Provenance, RawCandidate, ResolvedField)
from .shipment import (CanonicalFieldCandidate, EvidenceScope, ReconciliationResult,
                       ReconciliationState, ShipmentDocumentResolution, ShipmentEvidence,
                       ShipmentIssue, ShipmentReadiness, ShipmentResolution, QuantitySemanticType)

DOCUMENT_RESOLUTION_SCHEMA_VERSION = "lacey_document_resolution_v1"
SHIPMENT_RESOLUTION_SCHEMA_VERSION = "lacey_shipment_resolution_v1"


def _bbox(value: BoundingBox | None) -> dict[str, float] | None:
    return None if value is None else {"x0": value.x0, "top": value.top, "x1": value.x1, "bottom": value.bottom}


def _load_bbox(value: dict[str, Any] | None) -> BoundingBox | None:
    return None if value is None else BoundingBox(**value)


def _block(value: LayoutBlock) -> dict[str, Any]:
    return {"block_id": value.block_id, "page": value.page, "bbox": _bbox(value.bbox), "text": value.text, "block_type": value.block_type, "structure_type": value.structure_type.value, "table_id": value.table_id, "row_index": value.row_index, "column_index": value.column_index, "table_header": value.table_header, "key_text": value.key_text, "value_text": value.value_text}


def _load_block(value: dict[str, Any]) -> LayoutBlock:
    payload = dict(value); payload["bbox"] = _load_bbox(payload["bbox"]); payload["structure_type"] = LayoutStructureType(payload["structure_type"]); return LayoutBlock(**payload)


def _candidate(value: AdmittedCandidate) -> dict[str, Any]:
    raw = value.raw
    return {"raw": {"field_key": raw.field_key, "raw_text": raw.raw_text, "normalized_value": raw.normalized_value, "source_block": _block(raw.source_block), "evidence_class": raw.evidence_class.value, "extractor_name": raw.extractor_name, "extractor_version": raw.extractor_version, "derived_from_field_key": raw.derived_from_field_key, "label": raw.label}, "provenance": {"filename": value.provenance.filename, "page": value.provenance.page, "bbox": _bbox(value.provenance.bbox), "block_id": value.provenance.block_id, "source_text": value.provenance.source_text, "extractor_name": value.provenance.extractor_name, "extractor_version": value.provenance.extractor_version, "evidence_class": value.provenance.evidence_class.value}, "score": value.score, "document_type": value.document_type.value}


def _load_candidate(value: dict[str, Any]) -> AdmittedCandidate:
    raw = value["raw"]; provenance = value["provenance"]
    return AdmittedCandidate(RawCandidate(raw["field_key"], raw["raw_text"], raw["normalized_value"], _load_block(raw["source_block"]), EvidenceClass(raw["evidence_class"]), raw["extractor_name"], raw["extractor_version"], raw.get("derived_from_field_key"), raw.get("label")), Provenance(provenance["filename"], provenance["page"], _load_bbox(provenance["bbox"]), provenance["block_id"], provenance["source_text"], provenance["extractor_name"], provenance["extractor_version"], EvidenceClass(provenance["evidence_class"])), float(value["score"]), DocumentType(value["document_type"]))


def serialize_document_resolution(resolution: DocumentResolution) -> dict[str, Any]:
    return {"schema_version": DOCUMENT_RESOLUTION_SCHEMA_VERSION, "filename": resolution.filename, "engine_version": resolution.engine_version, "document_type": resolution.document_type.value, "type_confidence": resolution.type_confidence, "layout": {"page_count": resolution.layout.page_count, "blocks": [_block(block) for block in resolution.layout.blocks]}, "sections": [{"section_id": item.section_id, "page_start": item.page_start, "page_end": item.page_end, "document_type": item.document_type.value, "confidence": item.confidence, "block_ids": list(item.block_ids)} for item in resolution.sections], "fields": {key: {"field_key": field.field_key, "status": field.status.value, "effective_value": field.effective_value, "winning_candidate_index": (field.candidates.index(field.winning_candidate) if field.winning_candidate else None), "candidates": [_candidate(candidate) for candidate in field.candidates]} for key, field in resolution.fields.items()}}


def deserialize_document_resolution(payload: dict[str, Any]) -> DocumentResolution:
    if payload.get("schema_version") != DOCUMENT_RESOLUTION_SCHEMA_VERSION: raise ValueError("Unsupported document resolution schema.")
    fields = {}
    for key, item in payload["fields"].items():
        candidates = tuple(_load_candidate(value) for value in item["candidates"]); index = item["winning_candidate_index"]
        fields[key] = ResolvedField(item["field_key"], FieldStatus(item["status"]), item["effective_value"], candidates[index] if index is not None else None, candidates)
    return DocumentResolution(payload["filename"], payload["engine_version"], DocumentType(payload["document_type"]), float(payload["type_confidence"]), ParsedLayout(tuple(_load_block(value) for value in payload["layout"]["blocks"]), payload["layout"]["page_count"]), tuple(DocumentSection(item["section_id"], item["page_start"], item["page_end"], DocumentType(item["document_type"]), float(item["confidence"]), tuple(item["block_ids"])) for item in payload["sections"]), fields)


def serialize_shipment_resolution(resolution: ShipmentResolution) -> dict[str, Any]:
    def evidence(item: ShipmentEvidence) -> dict[str, Any]:
        return {"candidate_id": item.candidate_id, "document_id": item.document_id, "field_key": item.field_key, "normalized_value": item.normalized_value, "candidate": _candidate(item.candidate), "candidate_score": item.candidate_score, "source_authority": item.source_authority, "scope": item.scope.value, "line_key": item.line_key, "component_key": item.component_key, "quantity_semantic_type": item.quantity_semantic_type.value}
    return {"schema_version": SHIPMENT_RESOLUTION_SCHEMA_VERSION, "engine_version": resolution.engine_version, "ruleset_version": resolution.ruleset_version, "documents": [{"document_id": item.document_id, "filename": item.filename, "resolution": serialize_document_resolution(item.resolution)} for item in resolution.documents], "canonical_fields": {key: {"field_key": item.field_key, "state": item.state.value, "values": [{"value": value.value, "evidence_ids": list(value.evidence_ids)} for value in item.values], "supporting_evidence": [evidence(value) for value in item.supporting_evidence]} for key, item in resolution.canonical_fields.items()}, "issues": [{"issue_id": item.issue_id, "field_key": item.field_key, "scope": item.scope, "severity": item.severity, "issue_type": item.issue_type, "message": item.message, "candidate_ids": list(item.candidate_ids), "document_ids": list(item.document_ids), "requires_human_review": item.requires_human_review, "line_key": item.line_key, "component_key": item.component_key} for item in resolution.issues], "readiness": resolution.readiness.value, "metrics": resolution.metrics}


def deserialize_shipment_resolution(payload: dict[str, Any]) -> ShipmentResolution:
    if payload.get("schema_version") != SHIPMENT_RESOLUTION_SCHEMA_VERSION: raise ValueError("Unsupported shipment resolution schema.")
    def evidence(item: dict[str, Any]) -> ShipmentEvidence:
        return ShipmentEvidence(item["candidate_id"], item["document_id"], item["field_key"], item["normalized_value"], _load_candidate(item["candidate"]), float(item["candidate_score"]), float(item["source_authority"]), EvidenceScope(item["scope"]), item.get("line_key"), item.get("component_key"), QuantitySemanticType(item.get("quantity_semantic_type", "OTHER")))
    fields = {key: ReconciliationResult(item["field_key"], ReconciliationState(item["state"]), tuple(CanonicalFieldCandidate(value["value"], tuple(value["evidence_ids"])) for value in item["values"]), tuple(evidence(value) for value in item["supporting_evidence"])) for key, item in payload["canonical_fields"].items()}
    documents = tuple(ShipmentDocumentResolution(item["document_id"], item["filename"], deserialize_document_resolution(item["resolution"])) for item in payload["documents"])
    issues = tuple(ShipmentIssue(item["issue_id"], item["field_key"], item["scope"], item["severity"], item["issue_type"], item["message"], tuple(item["candidate_ids"]), tuple(item["document_ids"]), bool(item["requires_human_review"]), item.get("line_key"), item.get("component_key")) for item in payload["issues"])
    return ShipmentResolution(payload["engine_version"], documents, fields, issues, ShipmentReadiness(payload["readiness"]), dict(payload["metrics"]), payload["ruleset_version"])
