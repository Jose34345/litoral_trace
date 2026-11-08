"""Assurance V2 authoritative two-shipment and fail-closed memory acceptance."""
from __future__ import annotations

from uuid import uuid4
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from litoral_trace.db.base import Base
from litoral_trace.db.models import (
    AssuranceDocument, AssuranceV2Decision, AssuranceV2DecisionSource,
    AssuranceV2MemoryLink, Organization, UsLaceyEvidenceClaim,
    UsLaceyAssuranceV2ClaimSnapshot,
    UsLaceySupplierEvidence, UsLaceyOperation,
    UsLaceyOperationDocument, UsLaceyOperationEvent, UsLaceyOperationField,
    UsLaceyOperationProductLink, UsLaceyPpqPlantLine, UsLaceySourceSetMember,
    UsLaceySourceSetRevision, UsLaceySupplier, UsLaceySupplierProduct,
    UsLaceyEngineShipmentRun, User, VaultDocument,
)
from litoral_trace.us_lacey.assurance_v2_memory import (
    EvidenceRelation, build_case_snapshot, classify_evidence, evaluate_reuse,
    promote_decision_to_memory, record_human_decision, record_identity_event,
    revoke_verified_memory, current_identity_bindings,
    authorize_reuse_application, verified_reuse_application,
    canonical_claim_snapshot_digest,
)


@pytest.fixture
def session():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as db:
        yield db
    engine.dispose()


def org_and_actor(db: Session, label: str):
    suffix = uuid4().hex[:8]
    org = Organization(name=label, slug=f"{label.lower()}-{suffix}", tier="pro", is_active=True)
    db.add(org)
    db.flush()
    user = User(
        organization_id=org.id, email=f"{label}-{suffix}@example.com",
        username=f"{label}-{suffix}", password_hash="fake-test-only",
        role="cliente", is_active=True,
    )
    db.add(user)
    db.flush()
    return org, user


def supplier_product(db: Session, org_id: int, *, supplier_key: str, sku: str = "WOOD-01"):
    supplier = UsLaceySupplier(
        organization_id=org_id, supplier_key=supplier_key,
        display_name=supplier_key, normalized_name=supplier_key,
        status="VERIFIED",
    )
    db.add(supplier)
    db.flush()
    product = UsLaceySupplierProduct(
        organization_id=org_id, supplier_id=supplier.id,
        product_key=f"SKU:{sku}", sku=sku, status="VERIFIED",
    )
    db.add(product)
    db.flush()
    return supplier, product


def shipment(
    db: Session, org_id: int, product_id: int,
    *, reference: str, source_value: str | None = None,
):
    op = UsLaceyOperation(
        organization_id=org_id, client_reference=reference,
        status="NEW", document_count=1, merchandise_line_count=1,
    )
    db.add(op)
    db.flush()
    hash_char = ("abcdef0123456789"[(len(reference) + op.id) % 16])
    vault = VaultDocument(
        organization_id=org_id, original_filename=f"{reference}.pdf",
        content_type="application/pdf", size_bytes=35, sha256=hash_char * 64,
        object_key=f"tests/{uuid4().hex}.pdf", storage_backend="s3",
        storage_bucket="test", document_type="OTHER_EVIDENCE", status="available",
    )
    db.add(vault)
    db.flush()
    doc = AssuranceDocument(
        organization_id=org_id, vault_document_id=vault.id,
        semantic_document_type="UNKNOWN", type_confidence=0.0,
        processing_status="UPLOADED",
    )
    db.add(doc)
    db.flush()
    opdoc = UsLaceyOperationDocument(
        organization_id=org_id, operation_id=op.id,
        assurance_document_id=doc.id, document_role="SUPPLIER_DECLARATION",
        version_number=1, is_current=True,
    )
    db.add(opdoc)
    db.flush()
    revision = UsLaceySourceSetRevision(
        organization_id=org_id, operation_id=op.id,
        generation=1, source_set_fingerprint=uuid4().hex * 2,
        document_count=1, status="FINALIZED", is_current=True,
    )
    db.add(revision)
    db.flush()
    db.add(UsLaceySourceSetMember(
        organization_id=org_id, source_set_revision_id=revision.id,
        operation_document_id=opdoc.id, assurance_document_id=doc.id,
    ))
    db.add(UsLaceyOperationProductLink(
        organization_id=org_id, operation_id=op.id,
        source_set_revision_id=revision.id, line_reference="1",
        supplier_product_id=product_id, link_method="EXACT_SKU",
    ))
    line = UsLaceyPpqPlantLine(
        organization_id=org_id, operation_id=op.id,
        line_reference="1", ordinal=1,
    )
    db.add(line)
    db.flush()
    field = UsLaceyOperationField(
        organization_id=org_id, operation_id=op.id,
        merchandise_line_reference="1", field_name="species",
        field_scope="PLANT_LINE", plant_line_id=line.id,
        original_value=source_value, normalized_value=source_value,
        field_status="FOUND" if source_value else "MISSING",
        confidence=0.98 if source_value else 0,
        source_assurance_document_id=doc.id if source_value else None,
        source_page=1 if source_value else None,
        source_locator="page:1:species" if source_value else None,
        extractor="fixture", extractor_version="1",
        validation_status="VALID" if source_value else "MISSING",
    )
    db.add(field)
    db.flush()
    return op, doc, opdoc, revision, field


def approve_and_promote(db: Session, org: Organization, reviewer: User, op, doc, rev, *, key="accept-1"):
    decision = record_human_decision(
        db, organization_id=org.id, operation_id=op.id,
        source_set_revision_id=rev.id, authenticated_user_id=reviewer.id,
        action="ACCEPT", field_name="species", line_reference="1",
        selected_value="Quercus alba", reason="Verified in supplier declaration",
        idempotency_key=key, assurance_document_ids=(doc.id,),
    )
    memory = promote_decision_to_memory(db, organization_id=org.id, decision_id=decision.id)
    assert memory is not None
    return decision, memory


def current_claim_snapshot(db: Session, org, op, doc, rev):
    """A persisted complete (zero botanical claims) source packet for reuse tests."""
    opdoc = db.scalar(select(UsLaceyOperationDocument).where(
        UsLaceyOperationDocument.organization_id == org.id,
        UsLaceyOperationDocument.operation_id == op.id,
        UsLaceyOperationDocument.assurance_document_id == doc.id,
    ))
    vault = db.get(VaultDocument, doc.vault_document_id)
    snapshot = {
        "schema_version": "assurance.claim.snapshot.v1",
        "status": "CANDIDATE_ONLY",
        "source_set_revision_id": rev.id,
        "source_set_fingerprint": rev.source_set_fingerprint,
        "claim_count": 0,
        "documents": [{
            "schema_version": "assurance.claim.snapshot.v1",
            "source_claim_contract_version": "assurance.claim.v1",
            "organization_id": org.id, "operation_id": op.id,
            "source_set_revision_id": rev.id,
            "source_set_fingerprint": rev.source_set_fingerprint,
            "operation_document_id": opdoc.id,
            "assurance_document_id": doc.id,
            "document_version_number": opdoc.version_number,
            "original_sha256": vault.sha256,
            "status": "COMPLETE", "claim_count": 0, "claims": [],
        }],
    }
    db.add(UsLaceyEngineShipmentRun(
        organization_id=org.id, operation_id=op.id,
        engine_version="assurance-test", ruleset_version="test",
        schema_version="test", source_set_fingerprint=rev.source_set_fingerprint,
        document_count=1, readiness="REVIEW_REQUIRED",
        resolution_json={"assurance_v2_claim_snapshot": snapshot},
    ))
    db.flush()
    return snapshot


def test_reuse_application_requires_human_authority_and_preserves_origin(session):
    org, reviewer = org_and_actor(session, "Apply")
    _, product = supplier_product(session, org.id, supplier_key="MID:APPLY")
    op1, doc1, _, rev1, _ = shipment(session, org.id, product.id, reference="AP-1", source_value="Quercus alba")
    decision, memory = approve_and_promote(session, org, reviewer, op1, doc1, rev1)
    op2, doc2, _, rev2, field2 = shipment(session, org.id, product.id, reference="AP-2")
    snapshot = current_claim_snapshot(session, org, op2, doc2, rev2)
    digest = canonical_claim_snapshot_digest(snapshot)
    assert verified_reuse_application(
        session, organization_id=org.id, operation_id=op2.id,
        source_set_revision_id=rev2.id, line_reference="1", field_name="species",
        current_claim_values=(), current_claim_snapshot_digest=digest,
    ) is None
    applied = authorize_reuse_application(
        session, organization_id=org.id, operation_id=op2.id,
        source_set_revision_id=rev2.id, authenticated_user_id=reviewer.id,
        line_reference="1", field_name="species",
        memory_public_id=memory.public_id, current_claim_values=(),
        current_claim_snapshot_digest=digest, reason="Reviewed historical source",
        idempotency_key="ap-2:species",
    )
    assert applied.status == "VERIFIED_REUSE"
    assert applied.value == "Quercus alba"
    assert applied.origin_document_id == doc1.id
    assert applied.origin_decision_public_id == decision.public_id
    assert applied.actor_user_id == reviewer.id
    assert field2.original_value is None and field2.source_assurance_document_id is None
    reread = verified_reuse_application(
        session, organization_id=org.id, operation_id=op2.id,
        source_set_revision_id=rev2.id, line_reference="1", field_name="species",
        current_claim_values=(), current_claim_snapshot_digest=digest,
    )
    assert reread == applied
    audit = session.get(UsLaceyOperationEvent, applied.audit_event_id)
    assert audit.event_type == "HUMAN_REVIEW"
    assert audit.actor_identity == f"user:{reviewer.id}"
    assert audit.details["source_set_fingerprint"] == rev2.source_set_fingerprint
    assert audit.details["generation"] == rev2.generation


def test_reuse_application_is_idempotent_and_detects_payload_collision(session):
    org, reviewer = org_and_actor(session, "Retry")
    _, product = supplier_product(session, org.id, supplier_key="MID:RETRY")
    op1, doc1, _, rev1, _ = shipment(session, org.id, product.id, reference="RE-1", source_value="Quercus alba")
    _, memory = approve_and_promote(session, org, reviewer, op1, doc1, rev1)
    op2, doc2, _, rev2, _ = shipment(session, org.id, product.id, reference="RE-2")
    digest = canonical_claim_snapshot_digest(current_claim_snapshot(session, org, op2, doc2, rev2))
    args = dict(organization_id=org.id, operation_id=op2.id,
                source_set_revision_id=rev2.id, authenticated_user_id=reviewer.id,
                line_reference="1", field_name="species", memory_public_id=memory.public_id,
                current_claim_values=(), current_claim_snapshot_digest=digest,
                reason="Reviewed original", idempotency_key="retry-species")
    first = authorize_reuse_application(session, **args)
    assert authorize_reuse_application(session, **args).audit_event_id == first.audit_event_id
    assert len(session.scalars(select(UsLaceyOperationEvent).where(
        UsLaceyOperationEvent.operation_id == op2.id)).all()) == 1
    with pytest.raises(ValueError, match="idempotency collision"):
        authorize_reuse_application(session, **{**args, "reason": "Different review"})


def test_reuse_application_invalidates_on_revocation_claim_drift_and_stale_revision(session):
    org, reviewer = org_and_actor(session, "Invalidate")
    _, product = supplier_product(session, org.id, supplier_key="MID:INVALIDATE")
    op1, doc1, _, rev1, _ = shipment(session, org.id, product.id, reference="IN-1", source_value="Quercus alba")
    _, memory = approve_and_promote(session, org, reviewer, op1, doc1, rev1)
    op2, doc2, _, rev2, field2 = shipment(session, org.id, product.id, reference="IN-2")
    snapshot = current_claim_snapshot(session, org, op2, doc2, rev2)
    digest = canonical_claim_snapshot_digest(snapshot)
    args = dict(organization_id=org.id, operation_id=op2.id,
                source_set_revision_id=rev2.id, line_reference="1", field_name="species",
                current_claim_values=(), current_claim_snapshot_digest=digest)
    authorize_reuse_application(
        session, **args, authenticated_user_id=reviewer.id,
        memory_public_id=memory.public_id, reason="Reviewed original",
        idempotency_key="invalidate-species")
    assert verified_reuse_application(session, **args) is not None
    assert verified_reuse_application(session, **{**args, "current_claim_values": ("Quercus rubra",)}) is None
    assert verified_reuse_application(session, **{**args, "current_claim_snapshot_digest": "f" * 64}) is None
    field2.original_value = "Quercus rubra"
    assert verified_reuse_application(session, **args) is None
    field2.original_value = None
    assert revoke_verified_memory(session, organization_id=org.id,
                                  authenticated_user_id=reviewer.id,
                                  memory_public_id=memory.public_id,
                                  reason="Withdrawn", idempotency_key="invalidate-revoke")
    assert verified_reuse_application(session, **args) is None
    rev2.is_current = False
    assert verified_reuse_application(session, **args) is None


def test_reuse_application_rejects_wrong_supplier_tenant_and_actor(session):
    org, reviewer = org_and_actor(session, "RejectApply")
    other, other_reviewer = org_and_actor(session, "OtherApply")
    _, product1 = supplier_product(session, org.id, supplier_key="MID:ONE")
    _, product2 = supplier_product(session, org.id, supplier_key="MID:TWO")
    op1, doc1, _, rev1, _ = shipment(session, org.id, product1.id, reference="RA-1", source_value="Quercus alba")
    _, memory = approve_and_promote(session, org, reviewer, op1, doc1, rev1)
    op2, doc2, _, rev2, _ = shipment(session, org.id, product2.id, reference="RA-2")
    digest = canonical_claim_snapshot_digest(current_claim_snapshot(session, org, op2, doc2, rev2))
    args = dict(organization_id=org.id, operation_id=op2.id,
                source_set_revision_id=rev2.id, line_reference="1", field_name="species",
                memory_public_id=memory.public_id, current_claim_values=(),
                current_claim_snapshot_digest=digest, reason="Reviewed original",
                idempotency_key="wrong-supplier")
    with pytest.raises(ValueError, match="not eligible"):
        authorize_reuse_application(session, **args, authenticated_user_id=reviewer.id)
    with pytest.raises(ValueError, match="reviewer does not belong"):
        authorize_reuse_application(session, **args, authenticated_user_id=other_reviewer.id)
    with pytest.raises(ValueError, match="not eligible"):
        authorize_reuse_application(session, **{**args, "organization_id": other.id},
                                    authenticated_user_id=other_reviewer.id)
    assert session.scalar(select(UsLaceyOperationEvent).where(
        UsLaceyOperationEvent.operation_id == op2.id)) is None


def test_reuse_application_rejects_unreported_current_claim_and_incomplete_packet(session):
    org, reviewer = org_and_actor(session, "Packet")
    _, product = supplier_product(session, org.id, supplier_key="MID:PACKET")
    op1, doc1, _, rev1, _ = shipment(session, org.id, product.id, reference="PK-1", source_value="Quercus alba")
    _, memory = approve_and_promote(session, org, reviewer, op1, doc1, rev1)
    op2, doc2, _, rev2, _ = shipment(session, org.id, product.id, reference="PK-2")
    snapshot = current_claim_snapshot(session, org, op2, doc2, rev2)
    run = session.scalar(select(UsLaceyEngineShipmentRun).where(
        UsLaceyEngineShipmentRun.organization_id == org.id,
        UsLaceyEngineShipmentRun.operation_id == op2.id,
    ))
    packet = {**snapshot, "documents": [{**snapshot["documents"][0],
              "claims": [{"field_name": "species", "line_reference": "1",
                          "original_value": "Quercus rubra", "normalized_value": "Quercus rubra"}],
              "claim_count": 1}], "claim_count": 1}
    run.resolution_json = {"assurance_v2_claim_snapshot": packet}
    session.flush()
    args = dict(organization_id=org.id, operation_id=op2.id,
                source_set_revision_id=rev2.id, authenticated_user_id=reviewer.id,
                line_reference="1", field_name="species", memory_public_id=memory.public_id,
                current_claim_values=(), current_claim_snapshot_digest=canonical_claim_snapshot_digest(packet),
                reason="Reviewed original", idempotency_key="packet-species")
    with pytest.raises(ValueError, match="not eligible"):
        authorize_reuse_application(session, **args)
    packet["documents"][0]["original_sha256"] = "0" * 64
    run.resolution_json = {"assurance_v2_claim_snapshot": packet}
    session.flush()
    with pytest.raises(ValueError, match="not eligible"):
        authorize_reuse_application(
            session, **{**args, "current_claim_snapshot_digest": canonical_claim_snapshot_digest(packet),
                        "current_claim_values": ("Quercus rubra",)})
    assert session.scalar(select(UsLaceyOperationEvent).where(
        UsLaceyOperationEvent.operation_id == op2.id)) is None


def test_failed_sidecar_blocks_legacy_packet_fallback_for_reuse(session):
    org, reviewer = org_and_actor(session, "Sidecar")
    _, product = supplier_product(session, org.id, supplier_key="MID:SIDECAR")
    op1, doc1, _, rev1, _ = shipment(session, org.id, product.id, reference="SC-1", source_value="Quercus alba")
    _, memory = approve_and_promote(session, org, reviewer, op1, doc1, rev1)
    op2, doc2, _, rev2, _ = shipment(session, org.id, product.id, reference="SC-2")
    snapshot = current_claim_snapshot(session, org, op2, doc2, rev2)
    digest = canonical_claim_snapshot_digest(snapshot)
    session.add(UsLaceyAssuranceV2ClaimSnapshot(
        organization_id=org.id, operation_id=op2.id,
        source_set_revision_id=rev2.id,
        source_set_fingerprint=rev2.source_set_fingerprint,
        status="FAILED", document_count=1, claim_count=0,
        snapshot_json={**snapshot, "status": "FAILED"},
    ))
    session.flush()
    args = dict(organization_id=org.id, operation_id=op2.id,
                source_set_revision_id=rev2.id, line_reference="1", field_name="species",
                current_claim_values=(), current_claim_snapshot_digest=digest)
    assert verified_reuse_application(session, **args) is None
    with pytest.raises(ValueError, match="not eligible"):
        authorize_reuse_application(
            session, **args, authenticated_user_id=reviewer.id,
            memory_public_id=memory.public_id, reason="Reviewed original",
            idempotency_key="failed-sidecar")


def test_two_shipments_same_supplier_sku_reuse_with_original_provenance(session):
    org, reviewer = org_and_actor(session, "Alpha")
    _, product = supplier_product(session, org.id, supplier_key="MID:ALPHA")
    op1, doc1, _, rev1, field1 = shipment(
        session, org.id, product.id, reference="SHIP-1", source_value="Quercus alba"
    )
    decision, memory = approve_and_promote(session, org, reviewer, op1, doc1, rev1)
    assert field1.original_value == "Quercus alba"
    op2, _, _, rev2, _ = shipment(session, org.id, product.id, reference="SHIP-2")
    reusable = evaluate_reuse(
        session, organization_id=org.id, operation_id=op2.id,
        source_set_revision_id=rev2.id, line_reference="1", field_name="species",
    )
    assert reusable.eligible and reusable.status == "ELIGIBLE"
    assert reusable.value == "Quercus alba"
    assert reusable.provenance == "REUSED_HISTORICAL"
    assert reusable.origin_document_id == doc1.id
    assert reusable.origin_decision_public_id == decision.public_id
    assert reusable.memory_public_id == memory.public_id
    assert reusable.evidence_claim_id is not None


def test_identical_sku_other_supplier_never_joins_memory(session):
    org, reviewer = org_and_actor(session, "Beta")
    _, product1 = supplier_product(session, org.id, supplier_key="MID:SUPPLIER1")
    _, product2 = supplier_product(session, org.id, supplier_key="MID:SUPPLIER2")
    op1, doc1, _, rev1, _ = shipment(session, org.id, product1.id, reference="B-1", source_value="Quercus alba")
    approve_and_promote(session, org, reviewer, op1, doc1, rev1)
    op2, _, _, rev2, _ = shipment(session, org.id, product2.id, reference="B-2")
    result = evaluate_reuse(session, organization_id=org.id, operation_id=op2.id,
                            source_set_revision_id=rev2.id, line_reference="1", field_name="species")
    assert result.eligible is False
    assert result.reason_codes == ("NO_ACTIVE_VERIFIED_MEMORY",)


def test_current_shipment_contradiction_and_human_value_block(session):
    org, reviewer = org_and_actor(session, "Gamma")
    _, product = supplier_product(session, org.id, supplier_key="MID:GAMMA")
    op1, doc1, _, rev1, _ = shipment(session, org.id, product.id, reference="G-1", source_value="Quercus alba")
    approve_and_promote(session, org, reviewer, op1, doc1, rev1)
    op2, _, _, rev2, field2 = shipment(
        session, org.id, product.id, reference="G-2", source_value="Quercus rubra"
    )
    result = evaluate_reuse(session, organization_id=org.id, operation_id=op2.id,
                            source_set_revision_id=rev2.id, line_reference="1", field_name="species")
    assert not result.eligible and result.reason_codes == ("CURRENT_SHIPMENT_EVIDENCE_WINS",)
    field2.original_value = None
    field2.normalized_value = None
    field2.field_status = "CONFLICT"
    result = evaluate_reuse(session, organization_id=org.id, operation_id=op2.id,
                            source_set_revision_id=rev2.id, line_reference="1", field_name="species")
    assert result.reason_codes == ("CURRENT_SOURCE_CONFLICT",)


def test_document_supersession_and_stale_revision_block(session):
    org, reviewer = org_and_actor(session, "Delta")
    _, product = supplier_product(session, org.id, supplier_key="MID:DELTA")
    op1, doc1, opdoc1, rev1, _ = shipment(session, org.id, product.id, reference="D-1", source_value="Quercus alba")
    approve_and_promote(session, org, reviewer, op1, doc1, rev1)
    op2, _, _, rev2, _ = shipment(session, org.id, product.id, reference="D-2")
    opdoc1.is_current = False
    assert evaluate_reuse(session, organization_id=org.id, operation_id=op2.id,
                          source_set_revision_id=rev2.id, line_reference="1",
                          field_name="species").reason_codes == ("ORIGIN_SUPERSEDED_OR_CONTEXT_MISMATCH",)
    opdoc1.is_current = True
    rev1.is_current = False
    assert evaluate_reuse(session, organization_id=org.id, operation_id=op2.id,
                          source_set_revision_id=rev2.id, line_reference="1",
                          field_name="species").eligible is False
    rev2.is_current = False
    assert evaluate_reuse(session, organization_id=org.id, operation_id=op2.id,
                          source_set_revision_id=rev2.id, line_reference="1",
                          field_name="species").reason_codes == ("STALE_SOURCE_SET",)


def test_rejection_no_promotion_and_authentication_audit(session):
    org, reviewer = org_and_actor(session, "Epsilon")
    _, product = supplier_product(session, org.id, supplier_key="MID:EPS")
    op, doc, _, rev, field = shipment(session, org.id, product.id, reference="E-1", source_value="Quercus alba")
    with pytest.raises(ValueError, match="reviewer does not belong"):
        record_human_decision(
            session, organization_id=org.id, operation_id=op.id,
            source_set_revision_id=rev.id, authenticated_user_id=99999,
            action="REJECT", field_name="species", line_reference="1",
            selected_value=None, reason="Not credible", idempotency_key="invalid",
        )
    decision = record_human_decision(
        session, organization_id=org.id, operation_id=op.id,
        source_set_revision_id=rev.id, authenticated_user_id=reviewer.id,
        action="REJECT", field_name="species", line_reference="1",
        selected_value=None, reason="Not credible", idempotency_key="reject",
        assurance_document_ids=(doc.id,),
    )
    assert promote_decision_to_memory(session, organization_id=org.id, decision_id=decision.id) is None
    assert session.scalar(select(AssuranceV2MemoryLink.id)) is None
    assert field.original_value == "Quercus alba"
    event = session.get(UsLaceyOperationEvent, decision.audit_event_id)
    assert event.actor_identity == f"user:{reviewer.id}"
    assert event.event_type == "HUMAN_REVIEW"
    assert session.scalar(select(AssuranceV2DecisionSource).where(
        AssuranceV2DecisionSource.decision_id == decision.id)).assurance_document_id == doc.id


def test_idempotency_and_no_conflicting_repeated_decision(session):
    org, reviewer = org_and_actor(session, "Zeta")
    _, product = supplier_product(session, org.id, supplier_key="MID:ZETA")
    op, doc, _, rev, _ = shipment(session, org.id, product.id, reference="Z-1", source_value="Quercus alba")
    decision, memory = approve_and_promote(session, org, reviewer, op, doc, rev)
    duplicate = record_human_decision(
        session, organization_id=org.id, operation_id=op.id,
        source_set_revision_id=rev.id, authenticated_user_id=reviewer.id,
        action="ACCEPT", field_name="species", line_reference="1",
        selected_value="Quercus alba", reason="Verified in supplier declaration",
        idempotency_key="accept-1", assurance_document_ids=(doc.id,),
    )
    second_memory = promote_decision_to_memory(
        session, organization_id=org.id, decision_id=duplicate.id
    )
    assert duplicate.id == decision.id and second_memory.id == memory.id
    assert len(session.scalars(select(AssuranceV2Decision)).all()) == 1
    assert len(session.scalars(select(AssuranceV2MemoryLink)).all()) == 1
    assert len(session.scalars(select(UsLaceyOperationEvent)).all()) == 1
    with pytest.raises(ValueError, match="idempotency collision"):
        record_human_decision(
            session, organization_id=org.id, operation_id=op.id,
            source_set_revision_id=rev.id, authenticated_user_id=reviewer.id,
            action="ACCEPT", field_name="species", line_reference="1",
            selected_value="Quercus rubra", reason="Different result",
            idempotency_key="accept-1", assurance_document_ids=(doc.id,),
        )


def test_two_tenants_never_access_memory(session):
    org1, actor1 = org_and_actor(session, "TenantA")
    org2, actor2 = org_and_actor(session, "TenantB")
    _, product1 = supplier_product(session, org1.id, supplier_key="MID:COMMON")
    _, product2 = supplier_product(session, org2.id, supplier_key="MID:COMMON")
    op1, doc1, _, rev1, _ = shipment(session, org1.id, product1.id, reference="TA-1", source_value="Quercus alba")
    d, _ = approve_and_promote(session, org1, actor1, op1, doc1, rev1)
    op2, _, _, rev2, _ = shipment(session, org2.id, product2.id, reference="TB-1")
    assert evaluate_reuse(session, organization_id=org2.id, operation_id=op2.id,
                          source_set_revision_id=rev2.id, line_reference="1", field_name="species").eligible is False
    assert promote_decision_to_memory(session, organization_id=org2.id, decision_id=d.id) is None
    with pytest.raises(ValueError, match="reviewer does not belong"):
        record_human_decision(
            session, organization_id=org2.id, operation_id=op2.id,
            source_set_revision_id=rev2.id, authenticated_user_id=actor1.id,
            action="REJECT", field_name="species", line_reference="1",
            selected_value=None, reason="Wrong tenant", idempotency_key="bad-tenant",
        )
    assert actor2.id != actor1.id


def test_context_and_nonstable_field_fails_closed(session):
    org, reviewer = org_and_actor(session, "Theta")
    _, product = supplier_product(session, org.id, supplier_key="MID:THETA")
    op1, doc1, _, rev1, _ = shipment(session, org.id, product.id, reference="T-1", source_value="Quercus alba")
    d = record_human_decision(
        session, organization_id=org.id, operation_id=op1.id,
        source_set_revision_id=rev1.id, authenticated_user_id=reviewer.id,
        action="ACCEPT", field_name="species", line_reference="1",
        selected_value="Quercus alba", reason="Regime A",
        context={"ruleset": "v1"}, idempotency_key="ruleset-v1",
        assurance_document_ids=(doc1.id,),
    )
    assert promote_decision_to_memory(session, organization_id=org.id, decision_id=d.id)
    op2, _, _, rev2, _ = shipment(session, org.id, product.id, reference="T-2")
    assert evaluate_reuse(session, organization_id=org.id, operation_id=op2.id,
                          source_set_revision_id=rev2.id, line_reference="1",
                          field_name="species", regulatory_context={"ruleset": "v2"}).eligible is False
    assert evaluate_reuse(session, organization_id=org.id, operation_id=op2.id,
                          source_set_revision_id=rev2.id, line_reference="1",
                          field_name="plant_quantity").reason_codes == ("FIELD_NOT_REUSABLE",)


def test_identity_event_reversible_and_snapshot_staleness(session):
    org, actor = org_and_actor(session, "Iota")
    s1, product1 = supplier_product(session, org.id, supplier_key="MID:1")
    s2, _ = supplier_product(session, org.id, supplier_key="MID:2")
    op, doc, _, rev, _ = shipment(session, org.id, product1.id, reference="I-1", source_value="Quercus alba")
    merge = record_identity_event(
        session, organization_id=org.id, operation_id=op.id,
        authenticated_user_id=actor.id, entity_type="SUPPLIER",
        source_id=s1.id, target_id=s2.id, action="MERGE",
        reason="Human reviewed identity", idempotency_key="identity-1",
    )
    reversed_event = record_identity_event(
        session, organization_id=org.id, operation_id=op.id,
        authenticated_user_id=actor.id, entity_type="SUPPLIER",
        source_id=s1.id, target_id=s2.id, action="UNMERGE",
        reason="Manual correction", idempotency_key="identity-2",
        reverses_event_id=merge.id,
    )
    assert reversed_event.reverses_event_id == merge.id
    assert session.get(UsLaceySupplier, s1.id).id != session.get(UsLaceySupplier, s2.id).id
    snap = build_case_snapshot(session, organization_id=org.id,
                               operation_id=op.id, source_set_revision_id=rev.id)
    assert snap.state == "CURRENT" and snap.contract_version == "assurance.v2/1.0.0"
    rev.is_current = False
    assert build_case_snapshot(session, organization_id=org.id,
                               operation_id=op.id, source_set_revision_id=rev.id).state == "STALE"


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        (dict(same_entity=True, current_value="Quercus alba", historical_value="quercus alba"), EvidenceRelation.CORROBORATION),
        (dict(same_entity=True, current_value="Quercus rubra", historical_value="Quercus alba"), EvidenceRelation.CONTRADICTION),
        (dict(same_entity=False, current_value="x", historical_value="y"), EvidenceRelation.OTHER_ENTITY),
        (dict(same_entity=None, current_value="x", historical_value="y"), EvidenceRelation.INSUFFICIENT_IDENTITY),
        (dict(same_entity=True, current_value=None, historical_value=None), EvidenceRelation.ABSENT),
        (dict(same_entity=True, current_value=None, historical_value="Quercus alba"), EvidenceRelation.HISTORICAL),
        (dict(same_entity=True, current_value="x", historical_value="y", revoked=True), EvidenceRelation.REVOKED),
        (dict(same_entity=True, current_value="x", historical_value="y", obsolete=True), EvidenceRelation.OBSOLETE),
    ],
)
def test_evidence_relation_distinguishes_semantics(kwargs, expected):
    assert classify_evidence(**kwargs) == expected


def test_human_supersession_preserves_history_and_closes_memory(session):
    org, reviewer = org_and_actor(session, "Kappa")
    _, product = supplier_product(session, org.id, supplier_key="MID:KAPPA")
    op1, doc1, opdoc1, rev1, field1 = shipment(
        session, org.id, product.id, reference="K-1", source_value="Quercus alba"
    )
    decision, _ = approve_and_promote(session, org, reviewer, op1, doc1, rev1)
    source = session.scalar(
        select(AssuranceV2DecisionSource).where(
            AssuranceV2DecisionSource.decision_id == decision.id
        )
    )
    assert source.document_version_number == 1
    assert source.operation_document_id == opdoc1.id
    op2, _, _, rev2, _ = shipment(session, org.id, product.id, reference="K-2")
    assert evaluate_reuse(
        session, organization_id=org.id, operation_id=op2.id,
        source_set_revision_id=rev2.id, line_reference="1", field_name="species",
    ).eligible
    successor = record_human_decision(
        session, organization_id=org.id, operation_id=op1.id,
        source_set_revision_id=rev1.id, authenticated_user_id=reviewer.id,
        action="SUPERSEDE", field_name="species", line_reference="1",
        selected_value=None, reason="Reviewer withdraws earlier authority",
        idempotency_key="kappa:withdraw", supersedes_decision_id=decision.id,
    )
    assert successor.supersedes_decision_id == decision.id
    assert field1.original_value == "Quercus alba"
    assert session.get(AssuranceV2Decision, decision.id).action == "ACCEPT"
    assert not evaluate_reuse(
        session, organization_id=org.id, operation_id=op2.id,
        source_set_revision_id=rev2.id, line_reference="1", field_name="species",
    ).eligible


def test_explicit_memory_revocation_is_audited_not_deleted(session):
    org, reviewer = org_and_actor(session, "Lambda")
    _, product = supplier_product(session, org.id, supplier_key="MID:LAMBDA")
    op1, doc1, _, rev1, _ = shipment(
        session, org.id, product.id, reference="L-1", source_value="Quercus alba"
    )
    decision, memory = approve_and_promote(session, org, reviewer, op1, doc1, rev1)
    op2, _, _, rev2, _ = shipment(session, org.id, product.id, reference="L-2")
    assert revoke_verified_memory(
        session, organization_id=org.id, authenticated_user_id=reviewer.id,
        memory_public_id=memory.public_id, reason="Supplier withdrew statement",
        idempotency_key="lambda:revoke",
    ) is True
    assert revoke_verified_memory(
        session, organization_id=org.id, authenticated_user_id=reviewer.id,
        memory_public_id=memory.public_id, reason="Supplier withdrew statement",
        idempotency_key="lambda:revoke",
    ) is False
    result = evaluate_reuse(
        session, organization_id=org.id, operation_id=op2.id,
        source_set_revision_id=rev2.id, line_reference="1", field_name="species",
    )
    assert result.reason_codes == ("NO_ACTIVE_VERIFIED_MEMORY",)
    assert session.get(AssuranceV2MemoryLink, memory.id) is not None
    assert session.get(AssuranceV2Decision, decision.id) is not None
    assert len(session.scalars(select(UsLaceyOperationEvent)).all()) == 2


def test_old_verified_but_conflicting_evidence_blocks_v2_reuse(session):
    org, reviewer = org_and_actor(session, "Mu")
    _, product = supplier_product(session, org.id, supplier_key="MID:MU")
    op1, doc1, _, rev1, _ = shipment(
        session, org.id, product.id, reference="M-1", source_value="Quercus alba"
    )
    approve_and_promote(session, org, reviewer, op1, doc1, rev1)
    now = datetime.now(timezone.utc)
    other = UsLaceySupplierEvidence(
        organization_id=org.id, supplier_product_id=product.id,
        evidence_type="OLD_VERIFIED", document_hash="f" * 64,
        source_reference="legacy", valid_from=now - timedelta(days=2),
        valid_until=now + timedelta(days=2),
        verified_at=now - timedelta(days=1),
        verified_by_user_id=reviewer.id, status="VERIFIED",
    )
    session.add(other)
    session.flush()
    session.add(UsLaceyEvidenceClaim(
        organization_id=org.id, evidence_id=other.id, field_name="species",
        field_value="Quercus rubra", normalized_value="Quercus rubra",
    ))
    session.flush()
    op2, _, _, rev2, _ = shipment(session, org.id, product.id, reference="M-2")
    result = evaluate_reuse(
        session, organization_id=org.id, operation_id=op2.id,
        source_set_revision_id=rev2.id, line_reference="1", field_name="species",
    )
    assert result.eligible is False
    assert result.reason_codes == ("HISTORICAL_EVIDENCE_CONTRADICTION",)


def test_reversible_identity_projection_never_rewrites_real_supplier(session):
    org, reviewer = org_and_actor(session, "Nu")
    sup1, product = supplier_product(session, org.id, supplier_key="MID:N1")
    sup2, _ = supplier_product(session, org.id, supplier_key="MID:N2")
    op, _, _, _, _ = shipment(session, org.id, product.id, reference="N-1")
    added = record_identity_event(
        session, organization_id=org.id, operation_id=op.id,
        authenticated_user_id=reviewer.id, entity_type="SUPPLIER",
        source_id=sup1.id, target_id=sup2.id, action="MERGE",
        reason="Reviewed alias", idempotency_key="n:merge",
    )
    bindings = current_identity_bindings(
        session, organization_id=org.id, entity_type="SUPPLIER",
    )
    assert len(bindings) == 1 and bindings[0].target_id == sup2.id
    record_identity_event(
        session, organization_id=org.id, operation_id=op.id,
        authenticated_user_id=reviewer.id, entity_type="SUPPLIER",
        source_id=sup1.id, target_id=sup2.id, action="UNMERGE",
        reason="Verified separate legal supplier", idempotency_key="n:unmerge",
        reverses_event_id=added.id,
    )
    assert current_identity_bindings(
        session, organization_id=org.id, entity_type="SUPPLIER",
    ) == ()
    assert session.get(UsLaceySupplier, sup1.id).supplier_key == "MID:N1"
