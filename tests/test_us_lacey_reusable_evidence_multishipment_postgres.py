"""Commercial golden path: human review in shipment 1 fuels shipment 2."""
from __future__ import annotations

from datetime import datetime, timezone
import os
from uuid import uuid4

import pytest
from sqlalchemy import inspect, select

from litoral_trace.db.models import (
    AssuranceSupplier,
    DocumentEntityLink,
    UsLaceyEvidenceClaim,
    UsLaceyOperation,
    UsLaceyOperationField,
    UsLaceyPpqPlantLine,
    UsLaceyProductIntelligenceSnapshot,
    UsLaceySupplierEvidence,
    User,
)
from litoral_trace.us_lacey.operations import UsLaceyOperationService
from litoral_trace.us_lacey.reusable_evidence import REUSABLE_EVIDENCE_EXTRACTOR
from litoral_trace.us_lacey.reusable_evidence_promotion import (
    apply_reusable_evidence_for_operation,
)
from litoral_trace.us_lacey.review import review_us_lacey_field
from litoral_trace.us_lacey.source_sets import seal_current_source_set
from tests.us_lacey_engine2_postgres import (
    add_test_document,
    create_test_graph,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1",
    reason="requires isolated U.S. Lacey PostgreSQL",
)

SKU = "CHAIR-001"
LINE = "CHAIR-001"


def _seed_product_snapshot(
    factory,
    *,
    organization_id: int,
    operation_id: int,
    line_reference: str = LINE,
    sku: str = SKU,
) -> None:
    revision = seal_current_source_set(
        organization_id=organization_id,
        operation_id=operation_id,
        session_factory=factory,
    )
    session = tenant_session(factory, organization_id)
    try:
        session.add(
            UsLaceyProductIntelligenceSnapshot(
                organization_id=organization_id,
                operation_id=operation_id,
                source_set_revision_id=revision.id,
                generation=revision.generation,
                source_set_fingerprint=revision.source_set_fingerprint,
                status="READY",
                document_count=1,
                eligible_document_count=1,
                recognized_bom_table_count=1,
                unique_sku_count=1,
                component_count=1,
                material_count=1,
                issue_count=0,
                payload_json={
                    "schema_version": "product-intelligence-snapshot-v2",
                    "shipment_product_bridge": {
                        "schema_version": "shipment-product-bridge-v1",
                        "shipment_line_count": 1,
                        "composition_count": 1,
                        "linked_count": 1,
                        "review_count": 0,
                        "links": [
                            {
                                "status": "LINKED",
                                "line_item_key": f"SKU:{sku}",
                                "shipment_line_reference": line_reference,
                                "candidate_line_references": [line_reference],
                                "source": {
                                    "document_id": "golden-bom",
                                    "filename": "bom.xlsx",
                                    "assurance_document_id": None,
                                    "table_name": "BOM",
                                    "table_source": {"row": 1},
                                },
                                "product": {
                                    "sku": sku,
                                    "product_name": "White Oak Chair",
                                    "components": [],
                                },
                            }
                        ],
                    },
                    "sources": [],
                    "issues": [],
                },
                finalized_at=datetime.now(timezone.utc),
            )
        )
        session.commit()
    finally:
        session.close()


def _add_supplier_link(
    factory,
    *,
    organization_id: int,
    assurance_document_id: int,
    supplier: AssuranceSupplier,
) -> None:
    session = tenant_session(factory, organization_id)
    try:
        session.add(
            DocumentEntityLink(
                organization_id=organization_id,
                assurance_document_id=assurance_document_id,
                entity_type="SUPPLIER",
                entity_reference=f"supplier:{supplier.public_id}",
                link_confidence=1.0,
                link_method="EXACT_IDENTIFIER",
                human_confirmed=False,
            )
        )
        session.commit()
    finally:
        session.close()


def _add_species_field(
    factory,
    *,
    organization_id: int,
    operation_id: int,
    source_assurance_document_id: int | None,
) -> int:
    session = tenant_session(factory, organization_id)
    try:
        line = UsLaceyPpqPlantLine(
            organization_id=organization_id,
            operation_id=operation_id,
            line_reference=LINE,
            ordinal=1,
        )
        session.add(line)
        session.flush()
        field = UsLaceyOperationField(
            organization_id=organization_id,
            operation_id=operation_id,
            merchandise_line_reference=LINE,
            field_name="species",
            field_scope="PLANT_LINE",
            plant_line_id=line.id,
            original_value=None,
            normalized_value=None,
            field_status="MISSING",
            confidence=0.0,
            source_assurance_document_id=source_assurance_document_id,
            source_page=1 if source_assurance_document_id is not None else None,
            source_locator=(
                "supplier-declaration:species"
                if source_assurance_document_id is not None
                else None
            ),
            extractor="golden-fixture",
            extractor_version="1",
            validation_status="MISSING",
        )
        session.add(field)
        operation = session.get(UsLaceyOperation, operation_id)
        operation.merchandise_line_count = 1
        session.commit()
        return int(field.id)
    finally:
        session.close()


def test_shipment_one_review_promotes_and_shipment_two_reuses(
    monkeypatch,
    engine2_postgres_engine,
    engine2_postgres_session_factory,
):
    if "us_lacey_supplier_evidence" not in inspect(engine2_postgres_engine).get_table_names():
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_071")

    factory = engine2_postgres_session_factory

    # Shipment 1 arrives with a known source document and one missing botanical fact.
    org_id, operation_1_id, _, assurance_1_id, _, _ = create_test_graph(
        factory,
        content=b"shipment-1-supplier-declaration",
    )

    session = tenant_session(factory, org_id)
    try:
        user = User(
            organization_id=org_id,
            email=f"reviewer-{uuid4().hex[:10]}@example.com",
            username=f"reviewer_{uuid4().hex[:10]}",
            password_hash="golden-test-only",
            role="cliente",
            full_name="Golden Reviewer",
            is_active=True,
        )
        session.add(user)
        supplier = AssuranceSupplier(
            organization_id=org_id,
            cuit="30712345678",
            display_name="Northwoods Supplier LLC",
            normalized_name="northwoods supplier llc",
            status="AUTO_CREATED",
            source_assurance_document_id=assurance_1_id,
        )
        session.add(supplier)
        session.commit()
        user_id = int(user.id)
        supplier_id = int(supplier.id)
    finally:
        session.close()

    session = tenant_session(factory, org_id)
    try:
        supplier = session.get(AssuranceSupplier, supplier_id)
        supplier_public_id = supplier.public_id
    finally:
        session.close()

    _add_supplier_link(
        factory,
        organization_id=org_id,
        assurance_document_id=assurance_1_id,
        supplier=supplier,
    )
    field_1_id = _add_species_field(
        factory,
        organization_id=org_id,
        operation_id=operation_1_id,
        source_assurance_document_id=assurance_1_id,
    )
    _seed_product_snapshot(
        factory,
        organization_id=org_id,
        operation_id=operation_1_id,
    )

    session = tenant_session(factory, org_id)
    try:
        operation_1_public_id = session.get(UsLaceyOperation, operation_1_id).public_id
    finally:
        session.close()

    # Route the real manual-review service through this isolated PostgreSQL database.
    import litoral_trace.us_lacey.review as review_module
    monkeypatch.setattr(review_module, "get_us_lacey_db_session", factory)

    review_us_lacey_field(
        organization_id=org_id,
        operation_public_id=operation_1_public_id,
        field_id=field_1_id,
        user_id=user_id,
        user_email="golden-reviewer@example.com",
        action="edit",
        value="Quercus alba",
    )

    # The review transaction automatically created one bounded reusable claim.
    session = tenant_session(factory, org_id)
    try:
        evidence = session.scalar(
            select(UsLaceySupplierEvidence).where(
                UsLaceySupplierEvidence.organization_id == org_id
            )
        )
        assert evidence is not None
        assert evidence.status == "VERIFIED"
        assert evidence.document_hash
        assert evidence.valid_until > evidence.verified_at
        assert (evidence.valid_until - evidence.verified_at).days >= 364

        claim = session.scalar(
            select(UsLaceyEvidenceClaim).where(
                UsLaceyEvidenceClaim.organization_id == org_id,
                UsLaceyEvidenceClaim.evidence_id == evidence.id,
                UsLaceyEvidenceClaim.field_name == "species",
            )
        )
        assert claim is not None
        assert claim.normalized_value == "Quercus alba"
    finally:
        session.close()

    # Shipment 2: same exact Assurance supplier + same explicit SKU, species absent.
    session = tenant_session(factory, org_id)
    try:
        operation_2 = UsLaceyOperation(
            organization_id=org_id,
            client_reference=f"shipment-2-{uuid4().hex[:10]}",
            status="REVIEW_REQUIRED",
            document_count=1,
            merchandise_line_count=1,
        )
        session.add(operation_2)
        session.commit()
        operation_2_id = int(operation_2.id)
        operation_2_public_id = operation_2.public_id
    finally:
        session.close()

    _, assurance_2_id, _, _ = add_test_document(
        factory,
        organization_id=org_id,
        operation_id=operation_2_id,
        role="SUPPLIER_DECLARATION",
        filename="supplier-declaration-2.pdf",
        content=b"shipment-2-supplier-declaration",
    )

    session = tenant_session(factory, org_id)
    try:
        supplier = session.scalar(
            select(AssuranceSupplier).where(
                AssuranceSupplier.organization_id == org_id,
                AssuranceSupplier.public_id == supplier_public_id,
            )
        )
    finally:
        session.close()
    _add_supplier_link(
        factory,
        organization_id=org_id,
        assurance_document_id=assurance_2_id,
        supplier=supplier,
    )

    field_2_id = _add_species_field(
        factory,
        organization_id=org_id,
        operation_id=operation_2_id,
        source_assurance_document_id=None,
    )
    _seed_product_snapshot(
        factory,
        organization_id=org_id,
        operation_id=operation_2_id,
    )

    reused_count = apply_reusable_evidence_for_operation(
        organization_id=org_id,
        operation_id=operation_2_id,
        session_factory=factory,
    )
    assert reused_count == 1

    # The persisted field and the normal workspace response both expose provenance.
    session = tenant_session(factory, org_id)
    try:
        field_2 = session.get(UsLaceyOperationField, field_2_id)
        assert field_2.normalized_value == "Quercus alba"
        assert field_2.field_status == "FOUND"
        assert field_2.extractor == REUSABLE_EVIDENCE_EXTRACTOR
        assert str(field_2.source_locator).startswith("reused_evidence:")
    finally:
        session.close()

    detail = UsLaceyOperationService(session_factory=factory).get_detail(
        organization_id=org_id,
        operation_public_id=operation_2_public_id,
    )
    species = next(field for field in detail.fields if field.id == field_2_id)
    assert species.effective_value == "Quercus alba"
    assert species.provenance == "reused_evidence"
