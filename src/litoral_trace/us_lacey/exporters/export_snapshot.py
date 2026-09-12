"""Shared Phase E export projection built from Phase D semantic evidence.

Both XML and XLSX serializers consume the same immutable ``LaceyExportSnapshot``.
Whenever Phase D has an English interpretation for the source evidence selected by
an operation field, ``display_text`` is the exported value. Reviewed/normalized
operation values remain the fallback when semantic evidence is unavailable.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from litoral_trace.us_lacey.operations import OperationDetail, OperationFieldView
from litoral_trace.us_lacey.ppq505 import PPQ505_SHIPMENT_REFERENCE
from litoral_trace.us_lacey.semantic_evidence_read import EvidenceTextView


EvidenceSnapshot = Mapping[str, Sequence[EvidenceTextView]]


@dataclass(frozen=True, slots=True)
class LaceyExportHeader:
    entry_number: str
    importer: str
    estimated_date_of_arrival: str


@dataclass(frozen=True, slots=True)
class LaceyExportPlantLine:
    line_reference: str
    hts_number: str
    entered_value: str
    article_component: str
    genus: str
    species: str
    country_of_harvest: str
    quantity: str
    unit: str


@dataclass(frozen=True, slots=True)
class LaceyExportSnapshot:
    operation_id: str
    client_reference: str
    header: LaceyExportHeader
    plant_lines: tuple[LaceyExportPlantLine, ...]


def _fallback_value(field: OperationFieldView | None) -> str:
    if field is None:
        return ""
    return str(field.effective_value or field.proposed_value or "").strip()


def _semantic_display_text(
    evidence_snapshot: EvidenceSnapshot,
    field: OperationFieldView | None,
) -> str:
    """Resolve the Phase D ``display_text`` for the exact selected field evidence.

    Source locator is the strongest join key. Document/page matching is retained as
    a conservative fallback for older rows whose locator was not persisted.
    """
    if field is None:
        return ""
    candidates = tuple(evidence_snapshot.get(field.field_name, ()))
    if not candidates:
        return _fallback_value(field)

    if field.source_locator:
        exact = tuple(
            item
            for item in candidates
            if item.source_locator == field.source_locator
            and (
                field.source_assurance_document_id is None
                or item.source_assurance_document_id == field.source_assurance_document_id
            )
        )
        if exact:
            return str(exact[0].display_text or "").strip()

    by_document_page = tuple(
        item
        for item in candidates
        if (
            field.source_assurance_document_id is not None
            and item.source_assurance_document_id == field.source_assurance_document_id
        )
        and (field.source_page is None or item.source_page == field.source_page)
    )
    if len(by_document_page) == 1:
        return str(by_document_page[0].display_text or "").strip()

    if len(candidates) == 1:
        return str(candidates[0].display_text or "").strip()

    return _fallback_value(field)


def consolidate_export_snapshot(
    *,
    detail: OperationDetail,
    evidence_snapshot: EvidenceSnapshot,
) -> LaceyExportSnapshot:
    """Build one deterministic export projection for both Phase E serializers."""
    by_key = {
        (field.line_reference, field.field_name): field
        for field in detail.fields
    }

    def shipment(name: str) -> str:
        field = by_key.get((PPQ505_SHIPMENT_REFERENCE, name))
        value = _semantic_display_text(evidence_snapshot, field)
        if value:
            return value
        if name == "importer_name":
            return str(detail.importer_name or "").strip()
        return ""

    line_refs = list(
        dict.fromkeys(
            field.line_reference
            for field in detail.fields
            if field.scope == "PLANT_LINE" and field.line_reference
        )
    )
    for declaration in detail.plant_declarations:
        if declaration.line_reference and declaration.line_reference not in line_refs:
            line_refs.append(declaration.line_reference)

    plant_lines: list[LaceyExportPlantLine] = []
    for line_ref in line_refs:
        def plant(name: str) -> str:
            return _semantic_display_text(
                evidence_snapshot,
                by_key.get((line_ref, name)),
            )

        plant_lines.append(
            LaceyExportPlantLine(
                line_reference=line_ref,
                hts_number=plant("hts_code"),
                entered_value=plant("entered_value"),
                article_component=plant("article_component"),
                genus=plant("genus"),
                species=plant("species"),
                country_of_harvest=plant("country_of_harvest"),
                quantity=plant("plant_quantity"),
                unit=plant("metric_unit"),
            )
        )

    return LaceyExportSnapshot(
        operation_id=str(detail.public_id),
        client_reference=str(detail.client_reference or ""),
        header=LaceyExportHeader(
            entry_number=shipment("filing_entry_reference"),
            importer=shipment("importer_name"),
            estimated_date_of_arrival=shipment("estimated_arrival_date"),
        ),
        plant_lines=tuple(plant_lines),
    )
