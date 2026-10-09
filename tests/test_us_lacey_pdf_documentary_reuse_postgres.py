"""PostgreSQL integration for exact colonless-PDF identity -> evidence -> second shipment."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from uuid import uuid4
from pathlib import Path

import pytest
from sqlalchemy import inspect, select

from litoral_trace.db.models import (
    DocumentExtractionRun, ExtractedDocumentField,
    UsLaceyEvidenceClaim, UsLaceyOperationField,
    UsLaceyOperationProductLink, UsLaceySupplier,
    UsLaceySupplierEvidence, UsLaceySupplierProduct, UsLaceyOperation, User,
)
from litoral_trace.us_lacey.identity_memory import resolve_operation_identity_memory
from litoral_trace.us_lacey.reusable_evidence_promotion import (
    promote_reviewed_field, apply_reusable_evidence_for_operation,
)
from litoral_trace.us_lacey.source_sets import seal_current_source_set
from tests.us_lacey_engine2_postgres import (
    engine2_postgres_engine, engine2_postgres_session_factory, tenant_session,
    create_test_graph,
)
from tests.test_us_lacey_true_reuse_e2e import _add_csv_document, _create_operation, _add_review_field


def _documents():
    return json.loads(
        (Path(__file__).parent / "fixtures" / "us_lacey_shipment1_inline_20261008.json").read_text(encoding="utf-8")
    )


def _add_pdf_extractions(factory, *, organization_id, operation_id, omit_botanical=False):
    ids = {}
    for index, doc in enumerate(_documents(), 1):
        lines = list(doc["lines"])
        if omit_botanical:
            lines = [
                line for line in lines
                if not any(
                    value in line.casefold()
                    for value in (
                        "phyllostachys", "species edulis", "harvest country",
                        "country of harvest", "384.0 kg", "384.00 kg",
                    )
                )
            ]
        source = "\n".join(lines)
        doc_id, _, _ = _add_csv_document(
            factory, organization_id=organization_id, operation_id=operation_id,
            role="OTHER_EVIDENCE", filename=doc["filename"], content=source.encode("utf-8"),
        )
        session = tenant_session(factory, organization_id)
        try:
            run = DocumentExtractionRun(
                organization_id=organization_id,
                assurance_document_id=doc_id,
                engine="pdf-text-regression",
                engine_version="2.6.3", status="SUCCEEDED",
            )
            session.add(run)
            session.flush()
            session.add(ExtractedDocumentField(
                organization_id=organization_id,
                assurance_document_id=doc_id,
                extraction_run_id=run.id,
                field_name="raw.document_text",
                original_value=source, normalized_value=None, value_type="text",
                confidence=1.0, confidence_level="HIGH",
                source_page=1, source_locator="pdf:digital-text",
                auto_accepted=False, needs_review=True,
            ))
            session.commit()
        finally:
            session.close()
        ids[index] = doc_id
    return ids


@pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1",
    reason="requires isolated tenant-scoped U.S. Lacey PostgreSQL",
)
def test_pdf_only_shipment_one_promotes_human_confirmed_taxon_and_shipment_two_reuses(
    engine2_postgres_engine, engine2_postgres_session_factory,
):
    tables = set(inspect(engine2_postgres_engine).get_table_names())
    if not {"us_lacey_supplier", "us_lacey_supplier_product", "us_lacey_operation_product_link"}.issubset(tables):
        pytest.skip("POSTGRES_SCHEMA_NOT_MIGRATED_TO_IDENTITY_MEMORY")

    factory = engine2_postgres_session_factory
    org_id, _, _, _, _, _ = create_test_graph(factory)
    session = tenant_session(factory, org_id)
    try:
        user = User(
            organization_id=org_id,
            email=f"pdf-reuse-{uuid4().hex[:10]}@example.test",
            username=f"pdf_reuse_{uuid4().hex[:10]}",
            password_hash="test-only", role="cliente",
            full_name="PDF Identity Reviewer", is_active=True,
        )
        session.add(user)
        session.commit()
        user_id = int(user.id)
    finally:
        session.close()
    op1, _, line1 = _create_operation(factory, organization_id=org_id, label="pdf-identity-one")
    docs1 = _add_pdf_extractions(factory, organization_id=org_id, operation_id=op1)
    rev1 = seal_current_source_set(
        organization_id=org_id, operation_id=op1, session_factory=factory,
    )
    session = tenant_session(factory, org_id)
    try:
        proof = resolve_operation_identity_memory(
            session, organization_id=org_id, operation_id=op1,
            source_set_revision_id=rev1.id, product_payload={"sources":[]},
        )
        assert (proof.supplier_count, proof.product_count, proof.link_count) == (1,1,1)
        assert proof.explicit_links[0].line_reference == "1"
        assert proof.explicit_links[0].sku == "BAM-COAST-04"
        assert session.scalar(select(UsLaceySupplier).where(UsLaceySupplier.organization_id==org_id))
        session.commit()
    finally:
        session.close()

    field_id = _add_review_field(
        factory, organization_id=org_id, operation_id=op1,
        plant_line_id=line1, source_assurance_document_id=docs1[6],
        field_name="species", extracted_value="edulis",
    )
    session = tenant_session(factory, org_id)
    try:
        field = session.get(UsLaceyOperationField, field_id)
        field.field_status = "MATCHED"
        field.human_value = "edulis"
        field.reviewed_at = datetime.now(timezone.utc)
        field.reviewed_by_user_id = user_id
        operation = session.get(UsLaceyOperation, op1)
        result = promote_reviewed_field(
            session, organization_id=org_id, operation=operation,
            field=field, user_id=user_id,
        )
        assert result.promoted, result.reason
        session.commit()
        assert session.scalar(select(UsLaceySupplierEvidence).where(
            UsLaceySupplierEvidence.organization_id == org_id,
        ))
        assert session.scalar(select(UsLaceyEvidenceClaim).where(
            UsLaceyEvidenceClaim.organization_id == org_id,
            UsLaceyEvidenceClaim.field_name == "species",
        ))
    finally:
        session.close()

    op2, op2public, line2 = _create_operation(factory, organization_id=org_id, label="pdf-identity-two")
    _add_pdf_extractions(factory, organization_id=org_id, operation_id=op2, omit_botanical=True)
    rev2 = seal_current_source_set(
        organization_id=org_id, operation_id=op2, session_factory=factory,
    )
    species2 = _add_review_field(
        factory, organization_id=org_id, operation_id=op2, plant_line_id=line2,
        source_assurance_document_id=None, field_name="species", extracted_value=None,
    )
    session = tenant_session(factory, org_id)
    try:
        proof2 = resolve_operation_identity_memory(
            session, organization_id=org_id, operation_id=op2,
            source_set_revision_id=rev2.id, product_payload={"sources":[]},
        )
        assert proof2.link_count == 1
        session.commit()
    finally:
        session.close()

    assert apply_reusable_evidence_for_operation(
        organization_id=org_id, operation_id=op2, session_factory=factory,
    ) >= 1
    session = tenant_session(factory, org_id)
    try:
        resolved = session.get(UsLaceyOperationField, species2)
        assert resolved.human_value or resolved.normalized_value
        assert resolved.field_status not in ("MISSING", "REVIEW")
    finally:
        session.close()
