from __future__ import annotations

from datetime import datetime, timezone
import hashlib

from sqlalchemy import func, select

from litoral_trace.db.models import (
    ReconciliationIssue,
    UsLaceyEngineShipmentRun,
    UsLaceyFieldCandidate,
    UsLaceyOperation,
    UsLaceyOperationField,
    UsLaceyPpqPlantLine,
)
from litoral_trace.us_lacey.canonical_shipment_truth import publish_canonical_shipment_truth
from litoral_trace.us_lacey.ppq505 import PPQ505_PLANT_FIELDS, PPQ505_SHIPMENT_REFERENCE
from tests.lacey_engine.test_canonical_shipment_truth import _golden_payload
from tests.us_lacey_engine2_postgres import (
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


def _seed_two_lines(factory, *, logical_document_ids: bool = False):
    org, operation_id, link_id, assurance_id, _, _ = create_test_graph(
        factory, content=b"canonical-truth"
    )
    session = tenant_session(factory, org)
    operation = session.get(UsLaceyOperation, operation_id)
    operation.document_count = 7
    for ordinal in (1, 2):
        line = UsLaceyPpqPlantLine(
            organization_id=org,
            operation_id=operation_id,
            line_reference=str(ordinal),
            ordinal=ordinal,
        )
        session.add(line)
        session.flush()
        for contract in PPQ505_PLANT_FIELDS:
            session.add(
                UsLaceyOperationField(
                    organization_id=org,
                    operation_id=operation_id,
                    merchandise_line_reference=str(ordinal),
                    field_name=contract.key,
                    field_scope="PLANT_LINE",
                    plant_line_id=line.id,
                    field_status="MISSING",
                    validation_status="MISSING",
                    confidence=0.0,
                )
            )
    operation.merchandise_line_count = 2

    payload = _golden_payload()
    source_document_id = (
        f"{link_id}:logical-001" if logical_document_ids else str(link_id)
    )
    for field in payload["canonical_fields"].values():
        for evidence in field["supporting_evidence"]:
            evidence["document_id"] = source_document_id
            evidence["candidate_id"] = f"{source_document_id}:{evidence['candidate_id']}"
    session.add(
        UsLaceyEngineShipmentRun(
            organization_id=org,
            operation_id=operation_id,
            engine_version="canonical-test",
            ruleset_version="canonical-test",
            schema_version="lacey_shipment_resolution_v1",
            source_set_fingerprint=hashlib.sha256(
                f"canonical:{org}:{operation_id}".encode()
            ).hexdigest(),
            document_count=7,
            readiness="REVIEW_REQUIRED",
            resolution_json=payload,
        )
    )
    session.commit()
    return org, operation_id, assurance_id


def _field(session, org, operation_id, line_reference, field_name):
    return session.scalar(
        select(UsLaceyOperationField).where(
            UsLaceyOperationField.organization_id == org,
            UsLaceyOperationField.operation_id == operation_id,
            UsLaceyOperationField.merchandise_line_reference == line_reference,
            UsLaceyOperationField.field_name == field_name,
        )
    )


def test_canonical_publication_replaces_machine_state_without_cross_line_leakage(
    engine2_postgres_session_factory,
):
    factory = engine2_postgres_session_factory
    org, operation_id, assurance_id = _seed_two_lines(factory)
    session = tenant_session(factory, org)

    first_hts = _field(session, org, operation_id, "1", "hts_code")
    first_hts.original_value = "4407990190"
    first_hts.normalized_value = "4407990190"
    first_hts.field_status = "REVIEW"
    first_hts.validation_status = "REVIEW_REQUIRED"
    first_hts.source_assurance_document_id = assurance_id
    first_hts.extractor = "legacy-projector"
    first_hts.extractor_version = "legacy"
    legacy_candidate = UsLaceyFieldCandidate(
        organization_id=org,
        operation_id=operation_id,
        operation_field_id=first_hts.id,
        source_assurance_document_id=assurance_id,
        original_value="4407990190",
        normalized_value="4407990190",
        validation_status="VALID",
        confidence=0.9,
        source_page=1,
        source_locator="legacy:wrong-line",
        extractor="legacy-projector",
        extractor_version="legacy",
        fingerprint=hashlib.sha256(f"legacy:{org}:{operation_id}".encode()).hexdigest(),
        decision="PENDING",
    )
    session.add(legacy_candidate)

    operation = session.get(UsLaceyOperation, operation_id)
    conflict = ReconciliationIssue(
        organization_id=org,
        operation_reference=f"us_lacey:{operation.public_id}",
        fingerprint=hashlib.sha256(f"conflict:{org}:{operation_id}".encode()).hexdigest(),
        rule_code="US_LACEY_FIELD_CONFLICT",
        severity="BLOCKING",
        status="OPEN",
        field_name="hts_code",
        us_lacey_operation_field_id=first_hts.id,
        left_document_id=assurance_id,
        left_source="legacy line 1",
        left_value="4407110190",
        right_document_id=assurance_id,
        right_source="legacy line 2",
        right_value="4407990190",
        explanation="Legacy projector treated parallel merchandise lines as one conflict.",
    )
    session.add(conflict)
    session.commit()

    result = publish_canonical_shipment_truth(
        session,
        organization_id=org,
        operation_id=operation_id,
    )
    session.commit()

    assert result.line_count == 2
    expected = {
        "1": {
            "hts_code": "4407110190",
            "entered_value": "18300",
            "genus": "Pinus",
            "species": "Pinus taeda",
            "plant_quantity": "30",
            "metric_unit": "m3",
        },
        "2": {
            "hts_code": "4407990190",
            "entered_value": "12640",
            "genus": "Eucalyptus",
            "species": "Eucalyptus grandis",
            "plant_quantity": "16",
            "metric_unit": "m3",
        },
    }
    for line_reference, fields in expected.items():
        for field_name, value in fields.items():
            row = _field(session, org, operation_id, line_reference, field_name)
            assert row is not None
            assert row.normalized_value == value
            assert row.extractor == "canonical-shipment-truth"
            assert row.field_status == "FOUND"

    importer_address = _field(
        session,
        org,
        operation_id,
        PPQ505_SHIPMENT_REFERENCE,
        "importer_address",
    )
    assert importer_address is not None
    assert importer_address.original_value is None
    assert importer_address.normalized_value is None
    assert importer_address.field_status == "MISSING"
    assert importer_address.validation_status == "MISSING"
    assert importer_address.validation_error is None

    description = _field(
        session,
        org,
        operation_id,
        PPQ505_SHIPMENT_REFERENCE,
        "merchandise_description",
    )
    assert description is not None
    assert description.field_scope == "SHIPMENT"
    assert description.plant_line_id is None
    assert description.normalized_value == (
        "Pinus taeda KD sawn boards; Eucalyptus grandis KD sawn boards"
    )
    assert description.field_status == "FOUND"
    assert description.extractor == "canonical-shipment-truth"

    for line_reference in ("1", "2"):
        country = _field(session, org, operation_id, line_reference, "country_of_harvest")
        assert country.normalized_value == "Brazil"
        assert country.field_status == "REVIEW"
        assert country.validation_status == "REVIEW_REQUIRED"
        candidates = session.scalars(
            select(UsLaceyFieldCandidate).where(
                UsLaceyFieldCandidate.organization_id == org,
                UsLaceyFieldCandidate.operation_field_id == country.id,
                UsLaceyFieldCandidate.decision == "PENDING",
            )
        ).all()
        assert {candidate.normalized_value for candidate in candidates} == {"Brazil"}

    session.refresh(legacy_candidate)
    session.refresh(conflict)
    assert legacy_candidate.decision == "REJECTED"
    assert conflict.status == "RESOLVED"
    assert conflict.resolution_justification == (
        "Superseded by canonical shipment-line reconciliation."
    )
    assert conflict.resolved_at is not None

    before_candidates = session.scalar(
        select(func.count(UsLaceyFieldCandidate.id)).where(
            UsLaceyFieldCandidate.organization_id == org,
            UsLaceyFieldCandidate.operation_id == operation_id,
        )
    )
    publish_canonical_shipment_truth(
        session, organization_id=org, operation_id=operation_id
    )
    session.commit()
    after_candidates = session.scalar(
        select(func.count(UsLaceyFieldCandidate.id)).where(
            UsLaceyFieldCandidate.organization_id == org,
            UsLaceyFieldCandidate.operation_id == operation_id,
        )
    )
    assert before_candidates == after_candidates
    session.close()


def test_canonical_publication_accepts_bundle_logical_document_ids(
    engine2_postgres_session_factory,
):
    factory = engine2_postgres_session_factory
    org, operation_id, assurance_id = _seed_two_lines(
        factory,
        logical_document_ids=True,
    )
    session = tenant_session(factory, org)

    result = publish_canonical_shipment_truth(
        session,
        organization_id=org,
        operation_id=operation_id,
    )
    session.commit()

    assert result.line_count == 2
    description = _field(
        session,
        org,
        operation_id,
        PPQ505_SHIPMENT_REFERENCE,
        "merchandise_description",
    )
    assert description is not None
    assert description.source_assurance_document_id == assurance_id
    assert description.field_status == "FOUND"
    session.close()


def test_canonical_publication_never_overwrites_human_review(
    engine2_postgres_session_factory,
):
    factory = engine2_postgres_session_factory
    org, operation_id, assurance_id = _seed_two_lines(factory)
    session = tenant_session(factory, org)
    genus = _field(session, org, operation_id, "1", "genus")
    reviewed_at = datetime(2026, 9, 16, tzinfo=timezone.utc)
    genus.original_value = "legacy genus"
    genus.normalized_value = "PINUS"
    genus.human_value = "Pinus reviewed"
    genus.field_status = "MATCHED"
    genus.validation_status = "VALID"
    genus.source_assurance_document_id = assurance_id
    genus.reviewed_at = reviewed_at
    session.commit()

    publish_canonical_shipment_truth(
        session, organization_id=org, operation_id=operation_id
    )
    session.commit()
    session.refresh(genus)

    assert genus.human_value == "Pinus reviewed"
    assert genus.reviewed_at == reviewed_at
    assert genus.field_status == "MATCHED"
    assert genus.normalized_value == "PINUS"
    session.close()
