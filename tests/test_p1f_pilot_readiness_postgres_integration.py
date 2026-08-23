from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import AuditActor
from litoral_trace.services.pilot_readiness import (
    PILOT_NOT_STARTED,
    PILOT_READY,
    PilotReadinessService,
)
from litoral_trace.services.traceability_operations import (
    ProcessInputDraft,
    ProcessOutputDraft,
    ShipmentItemDraft,
    TraceabilityOperationService,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.integration"
EXPECTED_REVISION = "026_add_eudr_acceptance_attempts"


def _read_env() -> dict[str, str]:
    values: dict[str, str] = {}
    if not ENV_PATH.exists():
        return values
    for raw_line in ENV_PATH.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#") and "=" in line:
            name, value = line.split("=", 1)
            values[name.strip()] = value.strip()
    return values


ENV = _read_env()
ENABLED = (ENV.get("ENABLE_POSTGRES_TESTS") or "").lower() in {"1", "true", "yes", "on"}
RUNTIME_URL = ENV.get("TEST_POSTGRES_DATABASE_URL")
OWNER_URL = ENV.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not (ENABLED and RUNTIME_URL and OWNER_URL),
    reason="P1-F PostgreSQL tests require the isolated integration contract.",
)


def _engine(url: str):
    return create_engine(
        normalize_database_url(url),
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _actor(org_id: int) -> AuditActor:
    return AuditActor(
        organization_id=org_id,
        user_id=None,
        username="p1f.pilot@test.invalid",
        role="admin",
    )


class _ReadyService:
    def readiness(self, shipment_code: str):
        return SimpleNamespace(ready=True)


class _ReadyCandidateService:
    def conformance(self, shipment_code: str):
        return SimpleNamespace(ready=True)


def test_p1f_readiness_tracks_real_tenant_progress_and_isolation(monkeypatch) -> None:
    owner_engine = _engine(OWNER_URL)
    runtime_engine = _engine(RUNTIME_URL)
    RuntimeSession = sessionmaker(bind=runtime_engine, autoflush=False)
    suffix = uuid4().hex[:10]
    org_ids: list[int] = []

    with owner_engine.begin() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
        assert revision == EXPECTED_REVISION

        for label in ("A", "B"):
            org_id = int(
                connection.execute(
                    text(
                        """
                        INSERT INTO public.organizations
                            (name, slug, tax_id, tier, description, is_active)
                        VALUES
                            (:name, :slug, :tax_id, 'pro', 'P1-F pilot gate', true)
                        RETURNING id
                        """
                    ),
                    {
                        "name": f"P1F Pilot Org {label} {suffix}",
                        "slug": f"p1f-pilot-{label.lower()}-{suffix}",
                        "tax_id": f"P1F-{label}-{suffix}",
                    },
                ).scalar_one()
            )
            org_ids.append(org_id)
            connection.execute(
                text(
                    """
                    INSERT INTO public.users
                        (organization_id, email, username, password_hash, role, is_active)
                    VALUES
                        (:org, :email, :username, 'test-hash', 'admin', true)
                    """
                ),
                {
                    "org": org_id,
                    "email": f"p1f-{label.lower()}-{suffix}@test.invalid",
                    "username": f"p1f-{label.lower()}-{suffix}",
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO public.licenses
                        (organization_id, plan_type, max_lotes, max_volume_tons, max_batch_rows, is_active)
                    VALUES
                        (:org, 'pro', 50, 3000, 500, true)
                    """
                ),
                {"org": org_id},
            )

    org_a, org_b = org_ids

    session_b = RuntimeSession()
    try:
        set_tenant_db_context(session_b, org_b)
        initial_b = PilotReadinessService(
            session=session_b,
            organization_id=org_b,
            organization_name=f"P1F Pilot Org B {suffix}",
        ).evaluate()
        assert initial_b.state == PILOT_NOT_STARTED
        assert initial_b.completed_steps == 1
        assert initial_b.counts["lotes"] == 0
        assert initial_b.shipment_code is None
    finally:
        session_b.close()

    with owner_engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO public.lotes (
                    organization_id, identificador, productor_id,
                    producto_forestal, hectareas, latitud, longitud,
                    polygon_wkt, geom, estatus,
                    volumen_ingresado_ton, volumen_exportar_ton
                ) VALUES (
                    :org, :identifier, :producer,
                    'Pino taeda', 12.0, -28.05, -56.03,
                    :polygon_wkt,
                    ST_SetSRID(ST_GeomFromText(:polygon_wkt), 4326),
                    'Pendiente', 0.0, 0.0
                )
                """
            ),
            {
                "org": org_a,
                "identifier": f"RODAL-P1F-{suffix}",
                "producer": f"PROV-P1F-{suffix}",
                "polygon_wkt": (
                    "POLYGON((-56.04 -28.06,-56.02 -28.06,-56.02 -28.04,"
                    "-56.04 -28.04,-56.04 -28.06))"
                ),
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO public.vault_documents (
                    organization_id, original_filename, content_type, size_bytes,
                    sha256, object_key, storage_backend, storage_bucket,
                    document_type, status
                ) VALUES (
                    :org, 'p1f-evidence.pdf', 'application/pdf', 128,
                    :sha, :object_key, 's3', 'p1f-ci-bucket',
                    'OTHER_EVIDENCE', 'available'
                )
                """
            ),
            {
                "org": org_a,
                "sha": "a" * 64,
                "object_key": f"p1f/{suffix}/evidence.pdf",
            },
        )

    operations = TraceabilityOperationService(session_factory=RuntimeSession)
    actor = _actor(org_a)
    t0 = datetime.now(timezone.utc) - timedelta(hours=4)
    receipt = operations.create_receipt_draft(
        organization_id=org_a,
        actor=actor,
        source_identifier=f"RODAL-P1F-{suffix}",
        event_code=f"REC-P1F-{suffix}",
        batch_code=f"MP-P1F-{suffix}",
        product_name="Madera rolliza Pino taeda",
        quantity="100",
        unit="M3",
        occurred_at=t0,
        facility_reference="Planta piloto Corrientes",
    )
    operations.post_event(
        organization_id=org_a,
        event_public_id=receipt.event_public_id,
        actor=actor,
    )
    process = operations.create_process_draft(
        organization_id=org_a,
        actor=actor,
        event_code=f"PROC-P1F-{suffix}",
        event_type="TRANSFORMATION",
        occurred_at=t0 + timedelta(hours=1),
        inputs=(
            ProcessInputDraft(
                batch_public_id=receipt.output_batch_public_ids[0],
                quantity=Decimal("70"),
            ),
        ),
        outputs=(
            ProcessOutputDraft(
                code=f"ASERRADO-P1F-{suffix}",
                product_name="Madera aserrada Pino taeda",
                stage="FINISHED_GOOD",
                unit="M3",
                quantity=Decimal("65"),
            ),
        ),
        facility_reference="Planta piloto Corrientes",
    )
    operations.post_event(
        organization_id=org_a,
        event_public_id=process.event_public_id,
        actor=actor,
    )
    shipment = operations.create_shipment_draft(
        organization_id=org_a,
        actor=actor,
        shipment_code=f"EXP-P1F-{suffix}",
        sale_reference=f"FAC-P1F-{suffix}",
        buyer_reference="Importador UE piloto",
        destination_country="DE",
        items=(
            ShipmentItemDraft(
                batch_public_id=process.output_batch_public_ids[0],
                quantity=Decimal("60"),
            ),
        ),
    )
    operations.dispatch_shipment(
        organization_id=org_a,
        shipment_public_id=shipment.shipment_public_id,
        actor=actor,
    )

    monkeypatch.setattr(
        "litoral_trace.services.pilot_readiness.ShipmentExportCaseService",
        lambda **kwargs: _ReadyService(),
    )
    monkeypatch.setattr(
        "litoral_trace.services.pilot_readiness.ShipmentPhytosanitaryCaseService",
        lambda **kwargs: _ReadyService(),
    )
    monkeypatch.setattr(
        "litoral_trace.services.pilot_readiness.EudrDdsCandidateService",
        lambda **kwargs: _ReadyCandidateService(),
    )

    session_a = RuntimeSession()
    try:
        set_tenant_db_context(session_a, org_a)
        ready_a = PilotReadinessService(
            session=session_a,
            organization_id=org_a,
            organization_name=f"P1F Pilot Org A {suffix}",
        ).evaluate()
        assert ready_a.state == PILOT_READY
        assert ready_a.ready is True
        assert ready_a.completed_steps == ready_a.total_steps == 7
        assert ready_a.shipment_code == shipment.shipment_code
        assert ready_a.counts["lotes"] == 1
        assert ready_a.counts["vault_documents"] == 1
        assert ready_a.counts["posted_receipts"] == 1
        assert ready_a.counts["posted_transformations"] == 1
        assert ready_a.counts["dispatched_shipments"] == 1
        assert all(step.completed for step in ready_a.steps)
    finally:
        session_a.close()

    # A newer shipment from a different receipt has no TRANSFORMATION in its
    # own lineage. Tenant-wide activity must not be combined with that newer
    # shipment, and the already-qualified older shipment must remain the pilot
    # evidence instead of regressing readiness.
    receipt_only = operations.create_receipt_draft(
        organization_id=org_a,
        actor=actor,
        source_identifier=f"RODAL-P1F-{suffix}",
        event_code=f"REC-RAW-P1F-{suffix}",
        batch_code=f"RAW-P1F-{suffix}",
        product_name="Madera rolliza sin transformar",
        quantity="10",
        unit="M3",
        occurred_at=t0 + timedelta(hours=2),
        facility_reference="Planta piloto Corrientes",
    )
    operations.post_event(
        organization_id=org_a,
        event_public_id=receipt_only.event_public_id,
        actor=actor,
    )
    newer_shipment = operations.create_shipment_draft(
        organization_id=org_a,
        actor=actor,
        shipment_code=f"EXP-RAW-P1F-{suffix}",
        sale_reference=f"FAC-RAW-P1F-{suffix}",
        buyer_reference="Importador UE piloto",
        destination_country="DE",
        items=(
            ShipmentItemDraft(
                batch_public_id=receipt_only.output_batch_public_ids[0],
                quantity=Decimal("5"),
            ),
        ),
    )
    operations.dispatch_shipment(
        organization_id=org_a,
        shipment_public_id=newer_shipment.shipment_public_id,
        actor=actor,
    )

    session_a = RuntimeSession()
    try:
        set_tenant_db_context(session_a, org_a)
        preserved_a = PilotReadinessService(
            session=session_a,
            organization_id=org_a,
            organization_name=f"P1F Pilot Org A {suffix}",
        ).evaluate()
        assert preserved_a.state == PILOT_READY
        assert preserved_a.ready is True
        assert preserved_a.shipment_code == shipment.shipment_code
        assert preserved_a.shipment_code != newer_shipment.shipment_code
        assert preserved_a.counts["posted_transformations"] == 1
        assert preserved_a.counts["dispatched_shipments"] == 2
    finally:
        session_a.close()

    session_b = RuntimeSession()
    try:
        set_tenant_db_context(session_b, org_b)
        unchanged_b = PilotReadinessService(
            session=session_b,
            organization_id=org_b,
            organization_name=f"P1F Pilot Org B {suffix}",
        ).evaluate()
        assert unchanged_b.state == PILOT_NOT_STARTED
        assert unchanged_b.completed_steps == 1
        assert unchanged_b.counts["lotes"] == 0
        assert unchanged_b.counts["vault_documents"] == 0
        assert unchanged_b.counts["dispatched_shipments"] == 0
    finally:
        session_b.close()

    with owner_engine.begin() as connection:
        params = {"org_a": org_a, "org_b": org_b}
        for table_name in (
            "audit_logs",
            "traceability_evidence_links",
            "shipment_items",
            "shipments",
            "traceability_event_inputs",
            "traceability_event_outputs",
            "traceability_events",
            "traceability_batches",
            "vault_documents",
            "lotes",
            "licenses",
            "users",
        ):
            connection.execute(
                text(
                    f"DELETE FROM public.{table_name} "
                    "WHERE organization_id IN (:org_a, :org_b)"
                ),
                params,
            )
        connection.execute(
            text("DELETE FROM public.organizations WHERE id IN (:org_a, :org_b)"),
            params,
        )

    runtime_engine.dispose()
    owner_engine.dispose()
