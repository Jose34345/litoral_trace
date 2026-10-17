"""Engine2 replay preserves a historical run while attaching current source claims."""
from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import os
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.db.models import (
    AssuranceDocument, Organization, UsLaceyEngineShipmentRun, UsLaceyOperation,
    UsLaceyOperationDocument, VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.lacey_engine_service import UsLaceyEngine2Service
from litoral_trace.us_lacey.source_sets import seal_current_source_set
from tests.us_lacey_engine2_postgres import (
    FakeVault, engine2_postgres_session_factory, tenant_session,
)


_PDF = sorted((Path(__file__).parent / "fixtures" / "assurance_v2_pdf_e2e" / "pack2").glob("*.pdf"))[0]


@pytest.fixture
def runtime_factory():
    url = os.environ.get("TEST_POSTGRES_DATABASE_URL")
    if not url:
        pytest.skip("TEST_POSTGRES_DATABASE_URL runtime role required")
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            assert conn.scalar(text("SELECT current_user")) == "litoral_trace_app"
        yield sessionmaker(bind=engine, expire_on_commit=False)
    finally:
        engine.dispose()


def _source_graph(factory, content: bytes):
    """Create valid uploaded-document rows under a tenant-scoped owner session."""
    suffix = uuid4().hex
    session = factory()
    try:
        organization_id = int(session.scalar(text(
            "SELECT nextval(pg_get_serial_sequence('public.organizations','id'))"
        )))
        set_tenant_db_context(session, organization_id)
        organization = Organization(
            id=organization_id, name=f"V2 Replay {suffix}",
            slug=f"v2-replay-{suffix}", tax_id=f"v2-replay-{suffix}",
            tier="pro", is_active=True,
        )
        session.add(organization)
        session.flush()
        vault = VaultDocument(
            organization_id=organization_id, original_filename=_PDF.name,
            content_type="application/pdf", size_bytes=len(content),
            sha256=sha256(content).hexdigest(), object_key=f"tests/{suffix}",
            storage_backend="s3", storage_bucket="tests",
            document_type="OTHER_EVIDENCE", status="available",
        )
        session.add(vault)
        session.flush()
        assurance = AssuranceDocument(
            organization_id=organization_id, vault_document_id=vault.id,
            semantic_document_type="UNKNOWN", processing_status="EXTRACTED",
        )
        session.add(assurance)
        session.flush()
        operation = UsLaceyOperation(
            organization_id=organization_id, client_reference=f"replay-{suffix}",
            status="NEW", document_count=1, merchandise_line_count=0,
        )
        session.add(operation)
        session.flush()
        link = UsLaceyOperationDocument(
            organization_id=organization_id, operation_id=operation.id,
            assurance_document_id=assurance.id, document_role="COMMERCIAL_INVOICE",
            version_number=1, is_current=True,
        )
        session.add(link)
        session.flush()
        result = organization_id, operation.id, link.id, assurance.id
        session.commit()
        return result
    finally:
        session.close()


def test_shadow_replay_creates_idempotent_claim_sidecar_without_rewriting_engine2_run(
    engine2_postgres_session_factory, runtime_factory, monkeypatch,
):
    from litoral_trace.db.models import UsLaceyAssuranceV2ClaimSnapshot

    original = _PDF.read_bytes()
    org, operation, link_id, doc_id = _source_graph(
        engine2_postgres_session_factory, original,
    )
    service = UsLaceyEngine2Service(
        session_factory=runtime_factory,
        vault_service=FakeVault(original),
    )
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_MODE", "OFF")
    first = service.resolve_operation_with_engine2(
        organization_id=org, operation_id=operation,
    )
    assert first.status == "SUCCEEDED"
    revision = seal_current_source_set(
        organization_id=org, operation_id=operation,
        session_factory=runtime_factory,
    )
    session = tenant_session(runtime_factory, org)
    original_run = session.get(UsLaceyEngineShipmentRun, first.shipment_run_id)
    original_resolution = deepcopy(original_run.resolution_json)
    assert session.scalars(select(UsLaceyAssuranceV2ClaimSnapshot).where(
        UsLaceyAssuranceV2ClaimSnapshot.organization_id == org,
        UsLaceyAssuranceV2ClaimSnapshot.operation_id == operation,
    )).all() == []
    session.close()

    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_MODE", "SHADOW")
    second = service.resolve_operation_with_engine2(
        organization_id=org, operation_id=operation,
    )
    third = service.resolve_operation_with_engine2(
        organization_id=org, operation_id=operation,
    )
    assert (second.status, third.status) == ("SUCCEEDED", "SUCCEEDED")
    assert second.shipment_run_id == third.shipment_run_id == first.shipment_run_id

    session = tenant_session(runtime_factory, org)
    run = session.get(UsLaceyEngineShipmentRun, first.shipment_run_id)
    sidecars = session.scalars(select(UsLaceyAssuranceV2ClaimSnapshot).where(
        UsLaceyAssuranceV2ClaimSnapshot.organization_id == org,
        UsLaceyAssuranceV2ClaimSnapshot.operation_id == operation,
    )).all()
    assert run.resolution_json == original_resolution
    assert len(sidecars) == 1
    sidecar = sidecars[0]
    assert sidecar.status == "CANDIDATE_ONLY"
    assert sidecar.source_set_revision_id == revision.id
    assert sidecar.source_set_fingerprint == revision.source_set_fingerprint
    assert sidecar.document_count == 1
    assert sidecar.claim_count > 0
    assert sidecar.snapshot_json["status"] == "CANDIDATE_ONLY"
    document = sidecar.snapshot_json["documents"][0]
    assert document["assurance_document_id"] == doc_id
    assert document["operation_document_id"] == link_id
    assert document["document_version_number"] == 1
    assert document["original_sha256"] == sha256(original).hexdigest()
    assert document["source_set_revision_id"] == revision.id
    assert all(claim["candidate_state"] == "CANDIDATE" for claim in document["claims"])
    session.close()


def test_unverified_replay_bytes_create_failed_sidecar_without_source_claims(
    engine2_postgres_session_factory, runtime_factory, monkeypatch,
):
    from litoral_trace.db.models import UsLaceyAssuranceV2ClaimSnapshot

    original = _PDF.read_bytes()
    org, operation, _, _ = _source_graph(
        engine2_postgres_session_factory, original,
    )
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_MODE", "OFF")
    first = UsLaceyEngine2Service(
        session_factory=runtime_factory,
        vault_service=FakeVault(original),
    ).resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    assert first.status == "SUCCEEDED"
    seal_current_source_set(
        organization_id=org, operation_id=operation,
        session_factory=runtime_factory,
    )
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_MODE", "SHADOW")
    result = UsLaceyEngine2Service(
        session_factory=runtime_factory,
        vault_service=FakeVault(original + b"tampered"),
    ).resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    assert result.status == "SUCCEEDED"  # legacy Engine2 remains independently usable

    session = tenant_session(runtime_factory, org)
    sidecar = session.scalar(select(UsLaceyAssuranceV2ClaimSnapshot).where(
        UsLaceyAssuranceV2ClaimSnapshot.organization_id == org,
        UsLaceyAssuranceV2ClaimSnapshot.operation_id == operation,
    ))
    assert sidecar is not None
    assert sidecar.status == "FAILED"
    assert sidecar.snapshot_json["status"] == "PARTIAL"
    assert sidecar.claim_count == 0
    assert sidecar.snapshot_json["documents"][0]["reason_code"] == "UNVERIFIABLE_SOURCE_CLAIMS"
    session.close()
