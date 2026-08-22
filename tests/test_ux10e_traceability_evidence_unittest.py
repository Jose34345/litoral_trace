"""UX10-E acceptance for contextual documentary evidence."""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from litoral_trace.auth.rbac import Permission, has_permission
from litoral_trace.db.base import Base
from litoral_trace.db.models import (
    Lote,
    Organization,
    Shipment,
    TraceabilityBatch,
    TraceabilityEvent,
    TraceabilityEvidenceLink,
    VaultDocument,
)
from litoral_trace.services.audit import AuditActor
from litoral_trace.services.traceability_evidence import (
    TraceabilityEvidenceConflictError,
    TraceabilityEvidenceNotFoundError,
    TraceabilityEvidenceService,
)
from litoral_trace.web.traceability_evidence import router as evidence_router


@pytest.fixture()
def evidence_env():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        org = Organization(name="Forestal UX10-E", slug="forestal-ux10e")
        other = Organization(name="Tenant ajeno UX10-E", slug="tenant-ajeno-ux10e")
        session.add_all([org, other])
        session.flush()

        source = Lote(
            organization_id=org.id,
            identificador="RODAL-PINO-EVID-001",
            productor_id="CUIT-PROVEEDOR-EVID",
            producto_forestal="Pino resinoso",
            hectareas=55.0,
            latitud=-28.05,
            longitud=-56.03,
            estatus="Verde",
        )
        foreign_source = Lote(
            organization_id=other.id,
            identificador="RODAL-AJENO-EVID",
            productor_id="CUIT-AJENO-EVID",
            producto_forestal="Pino resinoso",
            hectareas=20.0,
            latitud=-27.0,
            longitud=-55.0,
            estatus="Verde",
        )
        session.add_all([source, foreign_source])
        session.flush()

        event = TraceabilityEvent(
            organization_id=org.id,
            event_code="ASERRADO-EVID-001",
            event_type="TRANSFORMATION",
            status="POSTED",
            occurred_at=datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc),
        )
        batch = TraceabilityBatch(
            organization_id=org.id,
            code="TABLA-EVID-001",
            product_name="Madera aserrada",
            stage="FINISHED_GOOD",
            unit="M3",
            status="ACTIVE",
        )
        shipment = Shipment(
            organization_id=org.id,
            shipment_code="EXP-EVID-001",
            sale_reference="FACT-EVID-001",
            buyer_reference="BUYER-EVID",
            destination_country="DE",
            status="DISPATCHED",
        )
        session.add_all([event, batch, shipment])
        session.flush()

        document = VaultDocument(
            organization_id=org.id,
            original_filename="guia-forestal-001.pdf",
            content_type="application/pdf",
            size_bytes=256,
            sha256="a" * 64,
            object_key="ux10e/org/document-1",
            storage_backend="s3",
            storage_bucket="ux10e-test",
            document_type="PDF_CERTIFICADO",
            status="available",
        )
        unavailable = VaultDocument(
            organization_id=org.id,
            original_filename="pendiente.pdf",
            content_type="application/pdf",
            size_bytes=128,
            sha256="b" * 64,
            object_key="ux10e/org/document-2",
            storage_backend="s3",
            storage_bucket="ux10e-test",
            document_type="PDF_CERTIFICADO",
            status="pending_upload",
        )
        foreign_document = VaultDocument(
            organization_id=other.id,
            original_filename="ajeno.pdf",
            content_type="application/pdf",
            size_bytes=64,
            sha256="c" * 64,
            object_key="ux10e/other/document-1",
            storage_backend="s3",
            storage_bucket="ux10e-test",
            document_type="PDF_CERTIFICADO",
            status="available",
        )
        session.add_all([document, unavailable, foreign_document])
        session.commit()

        payload = {
            "org_id": int(org.id),
            "other_id": int(other.id),
            "source_identifier": source.identificador,
            "event_public_id": event.public_id,
            "batch_public_id": batch.public_id,
            "shipment_public_id": shipment.public_id,
            "document_public_id": document.public_id,
            "unavailable_public_id": unavailable.public_id,
            "foreign_document_public_id": foreign_document.public_id,
        }

    yield {
        **payload,
        "engine": engine,
        "SessionLocal": SessionLocal,
        "service": TraceabilityEvidenceService(session_factory=SessionLocal),
    }
    Base.metadata.drop_all(engine)
    engine.dispose()


def _actor(org_id: int) -> AuditActor:
    return AuditActor(
        organization_id=org_id,
        user_id=None,
        username="evidencia@forestal.test",
        role="manager",
    )


def test_ux10e_same_vault_object_can_support_multiple_real_subjects(evidence_env):
    env = evidence_env
    service = env["service"]

    source_link = service.link_evidence(
        organization_id=env["org_id"],
        actor=_actor(env["org_id"]),
        subject_type="SOURCE_LOTE",
        subject_reference=env["source_identifier"],
        vault_document_id=env["document_public_id"],
        evidence_type="FOREST_GUIDE",
        reference_number="GF-001",
        issuer="Autoridad forestal",
        document_date=date(2026, 8, 20),
    )
    event_link = service.link_evidence(
        organization_id=env["org_id"],
        actor=_actor(env["org_id"]),
        subject_type="TRACEABILITY_EVENT",
        subject_reference=env["event_public_id"],
        vault_document_id=env["document_public_id"],
        evidence_type="FOREST_GUIDE",
    )

    assert source_link.replayed is False
    assert event_link.replayed is False
    evidence = service.list_evidence(organization_id=env["org_id"])
    assert len(evidence) == 2
    assert {item.subject_type for item in evidence} == {
        "SOURCE_LOTE",
        "TRACEABILITY_EVENT",
    }
    assert all(item.document_sha256 == "a" * 64 for item in evidence)


def test_ux10e_duplicate_pair_replays_but_type_change_conflicts(evidence_env):
    env = evidence_env
    service = env["service"]
    kwargs = dict(
        organization_id=env["org_id"],
        actor=_actor(env["org_id"]),
        subject_type="SHIPMENT",
        subject_reference=env["shipment_public_id"],
        vault_document_id=env["document_public_id"],
    )
    first = service.link_evidence(**kwargs, evidence_type="INVOICE")
    second = service.link_evidence(**kwargs, evidence_type="INVOICE")
    assert first.replayed is False
    assert second.replayed is True
    assert first.evidence.link_public_id == second.evidence.link_public_id

    with pytest.raises(TraceabilityEvidenceConflictError):
        service.link_evidence(**kwargs, evidence_type="TRANSPORT")


def test_ux10e_tenant_scope_and_available_document_fail_closed(evidence_env):
    env = evidence_env
    service = env["service"]
    base = dict(
        organization_id=env["org_id"],
        actor=_actor(env["org_id"]),
        subject_type="TRACEABILITY_BATCH",
        subject_reference=env["batch_public_id"],
        evidence_type="CERTIFICATE",
    )
    with pytest.raises(TraceabilityEvidenceNotFoundError):
        service.link_evidence(
            **base,
            vault_document_id=env["foreign_document_public_id"],
        )
    with pytest.raises(TraceabilityEvidenceNotFoundError):
        service.link_evidence(
            **base,
            vault_document_id=env["unavailable_public_id"],
        )


def test_ux10e_unlink_preserves_history_and_removes_active_coverage(evidence_env):
    env = evidence_env
    service = env["service"]
    result = service.link_evidence(
        organization_id=env["org_id"],
        actor=_actor(env["org_id"]),
        subject_type="SOURCE_LOTE",
        subject_reference=env["source_identifier"],
        vault_document_id=env["document_public_id"],
        evidence_type="ORIGIN_AUTHORIZATION",
    )
    before = service.coverage(organization_id=env["org_id"])
    assert before.subjects_with_evidence == 1
    assert before.total_subjects == 4

    service.unlink_evidence(
        organization_id=env["org_id"],
        actor=_actor(env["org_id"]),
        link_public_id=result.evidence.link_public_id,
    )
    assert service.list_evidence(organization_id=env["org_id"]) == ()
    after = service.coverage(organization_id=env["org_id"])
    assert after.subjects_with_evidence == 0

    with env["SessionLocal"]() as session:
        persisted = session.scalar(
            select(TraceabilityEvidenceLink).where(
                TraceabilityEvidenceLink.public_id == result.evidence.link_public_id
            )
        )
        assert persisted is not None
        assert persisted.unlinked_at is not None


def test_ux10e_database_requires_exactly_one_subject(evidence_env):
    env = evidence_env
    with env["SessionLocal"]() as session:
        document_id = session.scalar(
            select(VaultDocument.id).where(
                VaultDocument.organization_id == env["org_id"],
                VaultDocument.public_id == env["document_public_id"],
            )
        )
        session.add(
            TraceabilityEvidenceLink(
                organization_id=env["org_id"],
                vault_document_id=document_id,
                evidence_type="OTHER",
            )
        )
        with pytest.raises(IntegrityError):
            session.flush()


def test_ux10e_rbac_keeps_auditors_read_only():
    def user(role: str):
        return SimpleNamespace(role=role)

    for role in ("superadmin", "admin", "manager"):
        assert has_permission(user(role), Permission.TRACEABILITY_EVIDENCE)
        assert has_permission(user(role), Permission.VAULT_READ)
    for role in ("auditor", "cliente"):
        assert not has_permission(user(role), Permission.TRACEABILITY_EVIDENCE)
        assert has_permission(user(role), Permission.VAULT_READ)


def test_ux10e_web_contract_is_contextual_and_does_not_duplicate_vault():
    routes = {
        (route.path, tuple(sorted(route.methods or set())))
        for route in evidence_router.routes
    }
    assert ("/evidence", ("GET",)) in routes
    assert ("/evidence/link", ("POST",)) in routes
    assert ("/evidence/upload-link", ("POST",)) in routes
    assert ("/evidence/{link_public_id}/unlink", ("POST",)) in routes

    root = Path(__file__).resolve().parents[1]
    template = (root / "src/litoral_trace/templates/traceability_evidence.html").read_text(encoding="utf-8")
    service_source = (root / "src/litoral_trace/services/traceability_evidence.py").read_text(encoding="utf-8")
    model_source = (root / "src/litoral_trace/db/models/traceability_evidence_link.py").read_text(encoding="utf-8")

    assert "Evidencia de la cadena de custodia" in template
    assert "Subir un documento sin salir del flujo" in template
    assert "No constituye por sí sola una certificación" in template
    assert "SHA-256" in template
    assert "El archivo no se elimina de la bóveda" in template
    assert "current_stock" not in service_source
    assert "object_key" not in model_source
    assert "vault_document_id" in model_source
