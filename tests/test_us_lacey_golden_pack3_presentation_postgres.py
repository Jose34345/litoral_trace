from __future__ import annotations

import hashlib

from litoral_trace.db.models import UsLaceyEngineShipmentRun, UsLaceyOperation
from litoral_trace.lacey_engine.domain import (
    AdmittedCandidate,
    DocumentResolution,
    DocumentType,
    EvidenceClass,
    FieldStatus,
    LayoutBlock,
    ParsedLayout,
    Provenance,
    RawCandidate,
    ResolvedField,
)
from litoral_trace.lacey_engine.serialization import serialize_shipment_resolution
from litoral_trace.lacey_engine.shipment import (
    LaceyRuleset,
    ShipmentDocumentInput,
    ShipmentReadiness,
    process_shipment,
)
from litoral_trace.us_lacey.canonical_shipment_truth import (
    publish_canonical_shipment_truth,
)
from litoral_trace.us_lacey.operations import UsLaceyOperationService
from litoral_trace.web.us_lacey_operational_views import _review_field_groups
from tests.us_lacey_engine2_postgres import (
    create_test_graph,
    engine2_postgres_session_factory,
    tenant_session,
)


def _document(
    identifier: str,
    document_type: DocumentType,
    values: tuple[tuple[str, str, str, str], ...],
) -> DocumentResolution:
    """Build one deterministic Engine 2 document with explicit semantic evidence."""
    fields: dict[str, ResolvedField] = {}
    for index, (field_key, value, label, source_text) in enumerate(values, start=1):
        block = LayoutBlock(
            block_id=f"{identifier}-{index}",
            page=1,
            bbox=None,
            text=source_text,
            block_type="TEXT_LINE",
        )
        raw = RawCandidate(
            field_key=field_key,
            raw_text=value,
            normalized_value=value,
            source_block=block,
            evidence_class=EvidenceClass.EXPLICIT,
            extractor_name="golden-pack3",
            extractor_version="1",
            label=label,
        )
        candidate = AdmittedCandidate(
            raw=raw,
            provenance=Provenance(
                filename=f"{identifier}.pdf",
                page=1,
                bbox=None,
                block_id=block.block_id,
                source_text=source_text,
                extractor_name="golden-pack3",
                extractor_version="1",
                evidence_class=EvidenceClass.EXPLICIT,
            ),
            score=98.0,
            document_type=document_type,
        )
        fields[field_key] = ResolvedField(
            field_key=field_key,
            status=FieldStatus.MATCHED,
            effective_value=value,
            winning_candidate=candidate,
            candidates=(candidate,),
        )
    return DocumentResolution(
        filename=f"{identifier}.pdf",
        engine_version="golden-pack3",
        document_type=document_type,
        type_confidence=0.99,
        layout=ParsedLayout((), 1),
        sections=(),
        fields=fields,
    )


def _golden_pack3(link_id: int) -> tuple[ShipmentDocumentInput, ...]:
    """Synthetic cross-document pack that produces both FOUND and REVIEW publication."""
    return (
        ShipmentDocumentInput(
            document_id=f"{link_id}:logical-001",
            filename="01_Bill_of_Lading.pdf",
            role_hint=DocumentType.BILL_OF_LADING.value,
            resolution=_document(
                "01_Bill_of_Lading",
                DocumentType.BILL_OF_LADING,
                (
                    (
                        "bill_of_lading",
                        "RPT-HOU-260913-42",
                        "Bill of Lading",
                        "Bill of Lading RPT-HOU-260913-42",
                    ),
                ),
            ),
        ),
        ShipmentDocumentInput(
            document_id=f"{link_id}:logical-002",
            filename="02_Entry_Worksheet.pdf",
            role_hint=DocumentType.CUSTOMS_ENTRY_SUMMARY.value,
            resolution=_document(
                "02_Entry_Worksheet",
                DocumentType.CUSTOMS_ENTRY_SUMMARY,
                (
                    (
                        "description",
                        "Pinus taeda KD sawn boards",
                        "Line 1",
                        "Line 1 Pinus taeda KD sawn boards",
                    ),
                    (
                        "hts_code",
                        "4407110190",
                        "Line 1",
                        "Line 1 HTS 4407110190 Pinus taeda KD sawn boards",
                    ),
                    (
                        "entered_value",
                        "18300",
                        "Line 1",
                        "Line 1 entered value USD 18,300.00 Pinus taeda",
                    ),
                ),
            ),
        ),
        ShipmentDocumentInput(
            document_id=f"{link_id}:logical-003",
            filename="03_Supplier_Support.pdf",
            role_hint=DocumentType.COMMERCIAL_INVOICE.value,
            resolution=_document(
                "03_Supplier_Support",
                DocumentType.COMMERCIAL_INVOICE,
                (
                    (
                        "genus",
                        "Pinus",
                        "Genus",
                        "Pinus taeda genus Pinus",
                    ),
                    (
                        "species",
                        "Pinus taeda",
                        "Species",
                        "Pinus taeda species",
                    ),
                    (
                        "country_of_harvest",
                        "Brazil",
                        "Country of Harvest",
                        "Pinus taeda country of harvest Brazil",
                    ),
                    (
                        "plant_quantity",
                        "30",
                        "Plant Quantity",
                        "Pinus taeda plant quantity 30 m3",
                    ),
                    (
                        "metric_unit",
                        "m3",
                        "Metric Unit",
                        "Pinus taeda plant quantity 30 m3",
                    ),
                ),
            ),
        ),
    )


def test_golden_pack3_canonical_review_states_are_visible_in_workspace(
    engine2_postgres_session_factory,
):
    """Engine 2 -> canonical publication -> presentation must never drop FOUND/REVIEW."""
    factory = engine2_postgres_session_factory
    organization_id, operation_id, link_id, _assurance_id, _vault_id, _sha = (
        create_test_graph(factory, content=b"golden-pack3")
    )

    shipment = process_shipment(
        documents=list(_golden_pack3(link_id)),
        ruleset=LaceyRuleset(),
    )
    assert shipment.readiness is ShipmentReadiness.REVIEW_REQUIRED

    session = tenant_session(factory, organization_id)
    operation = session.get(UsLaceyOperation, operation_id)
    operation.document_count = 3
    session.add(
        UsLaceyEngineShipmentRun(
            organization_id=organization_id,
            operation_id=operation_id,
            engine_version=shipment.engine_version,
            ruleset_version=shipment.ruleset_version,
            schema_version="lacey_shipment_resolution_v1",
            source_set_fingerprint=hashlib.sha256(
                f"golden-pack3:{organization_id}:{operation_id}".encode()
            ).hexdigest(),
            document_count=3,
            readiness=shipment.readiness.value,
            resolution_json=serialize_shipment_resolution(shipment),
        )
    )
    session.commit()

    published = publish_canonical_shipment_truth(
        session,
        organization_id=organization_id,
        operation_id=operation_id,
    )
    session.commit()
    assert published.field_count > 0
    session.close()

    service = UsLaceyOperationService(session_factory=factory)
    detail = service.get_detail(
        organization_id=organization_id,
        operation_public_id=operation.public_id,
    )
    canonical_found_ids = {
        field.id for field in detail.fields if field.status == "FOUND"
    }
    canonical_review_ids = {
        field.id for field in detail.fields if field.status == "REVIEW"
    }
    assert canonical_found_ids, "Golden Pack 3 must publish at least one FOUND field."
    assert canonical_review_ids, "Golden Pack 3 must publish at least one REVIEW field."

    action_required, auto_resolved, _settled = _review_field_groups(detail)
    action_ids = {field.id for field in action_required}
    auto_ids = {field.id for field in auto_resolved}

    # Regression contract for PR #298: canonical vocabulary must be absorbed by UI.
    assert canonical_review_ids <= action_ids
    assert canonical_found_ids <= auto_ids

    # Transversal invariant: a REVIEW_REQUIRED canonical dossier cannot render an
    # apparently empty customer workload.
    if shipment.readiness is ShipmentReadiness.REVIEW_REQUIRED:
        assert len(action_required) + len(auto_resolved) > 0
