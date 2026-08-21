"""UX10-G PostgreSQL regression for the browser operations runtime.

The runtime session intentionally keeps SQLAlchemy's default
``expire_on_commit=True``. This catches regressions where a tenant-scoped ORM
object is read after COMMIT, after the transaction-local RLS context has been
cleared.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.services.audit import AuditActor
from litoral_trace.services.traceability_operations import TraceabilityOperationService


ENABLED = os.getenv("ENABLE_POSTGRES_TESTS", "").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
RUNTIME_URL = os.getenv("TEST_POSTGRES_DATABASE_URL")
OWNER_URL = os.getenv("TEST_POSTGRES_MIGRATION_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not (ENABLED and RUNTIME_URL and OWNER_URL),
    reason="UX10-G PostgreSQL regression requires the isolated integration contract.",
)


def _engine(url: str):
    return create_engine(
        normalize_database_url(url),
        pool_pre_ping=True,
        hide_parameters=True,
    )


@pytest.fixture()
def ux10g_pg():
    owner_engine = _engine(OWNER_URL)
    runtime_engine = _engine(RUNTIME_URL)
    RuntimeSession = sessionmaker(
        bind=runtime_engine,
        autoflush=False,
        expire_on_commit=True,
    )
    suffix = uuid4().hex[:10]

    with owner_engine.begin() as connection:
        revision = connection.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one()
        if revision != "020_add_traceability_evidence_links":
            raise RuntimeError(
                "UX10-G requires canonical head "
                f"020_add_traceability_evidence_links; found {revision!r}."
            )

        org_id = int(
            connection.execute(
                text(
                    """
                    INSERT INTO public.organizations
                        (name, slug, tax_id, tier, description, is_active)
                    VALUES
                        (:name, :slug, :tax_id, 'pro', 'UX10-G runtime regression', true)
                    RETURNING id
                    """
                ),
                {
                    "name": f"UX10-G Corrientes {suffix}",
                    "slug": f"ux10g-corrientes-{suffix}",
                    "tax_id": f"UX10G-{suffix}",
                },
            ).scalar_one()
        )
        connection.execute(
            text(
                """
                INSERT INTO public.lotes (
                    organization_id, identificador, productor_id,
                    producto_forestal, hectareas, latitud, longitud,
                    estatus, volumen_ingresado_ton, volumen_exportar_ton
                ) VALUES (
                    :organization_id, :identificador, :productor_id,
                    'Pino resinoso', 100.0, -28.05, -56.03,
                    'Verde', 0.0, 0.0
                )
                """
            ),
            {
                "organization_id": org_id,
                "identificador": f"RODAL-UX10G-{suffix}",
                "productor_id": f"PROV-UX10G-{suffix}",
            },
        )

    try:
        yield RuntimeSession, owner_engine, org_id, suffix
    finally:
        with owner_engine.begin() as connection:
            params = {"org_id": org_id}
            for table_name in (
                "audit_logs",
                "traceability_evidence_links",
                "shipment_items",
                "shipments",
                "traceability_event_inputs",
                "traceability_event_outputs",
                "traceability_events",
                "traceability_batches",
            ):
                connection.execute(
                    text(
                        f"DELETE FROM public.{table_name} "
                        "WHERE organization_id = :org_id"
                    ),
                    params,
                )
            connection.execute(
                text("DELETE FROM public.lotes WHERE organization_id = :org_id"),
                params,
            )
            connection.execute(
                text("DELETE FROM public.organizations WHERE id = :org_id"),
                params,
            )
        runtime_engine.dispose()
        owner_engine.dispose()


def _actor(org_id: int) -> AuditActor:
    return AuditActor(
        organization_id=org_id,
        user_id=None,
        username="operaciones.ux10g@corrientes.test",
        role="admin",
    )


def test_receipt_draft_survives_commit_with_runtime_expiration_and_posts(ux10g_pg):
    RuntimeSession, _, org_id, suffix = ux10g_pg
    service = TraceabilityOperationService(session_factory=RuntimeSession)

    draft = service.create_receipt_draft(
        organization_id=org_id,
        actor=_actor(org_id),
        source_identifier=f"RODAL-UX10G-{suffix}",
        event_code=f"REC-UX10G-{suffix}",
        batch_code=f"MP-UX10G-{suffix}",
        product_name="Madera rolliza demo",
        quantity="100",
        unit="M3",
        occurred_at=datetime.now(timezone.utc),
        facility_reference="Planta Demo Corrientes",
    )

    assert draft.status == "DRAFT"
    assert draft.event_code == f"REC-UX10G-{suffix}"
    assert len(draft.output_batch_public_ids) == 1

    posting = service.post_event(
        organization_id=org_id,
        event_public_id=draft.event_public_id,
        actor=_actor(org_id),
    )
    assert posting.status == "POSTED"

    snapshot = service.snapshot(organization_id=org_id)
    balances = {batch.code: batch.available for batch in snapshot.active_batches}
    assert balances[f"MP-UX10G-{suffix}"] == Decimal("100.000000")
