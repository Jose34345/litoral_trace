"""UX10-G PostgreSQL regression for the browser operations runtime.

The runtime session intentionally keeps SQLAlchemy's default
``expire_on_commit=True``. This catches regressions where a tenant-scoped ORM
object is read after COMMIT, after the transaction-local RLS context has been
cleared.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.services.audit import AuditActor
from litoral_trace.services.traceability_operations import (
    ProcessInputDraft,
    ProcessOutputDraft,
    ShipmentItemDraft,
    TraceabilityOperationService,
)


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
        if revision != "023_add_shipment_export_cases":
            raise RuntimeError(
                "UX10-G requires canonical head "
                f"023_add_shipment_export_cases; found {revision!r}."
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


def test_all_operation_draft_results_survive_commit_and_keep_p1b_authority(ux10g_pg):
    RuntimeSession, _, org_id, suffix = ux10g_pg
    service = TraceabilityOperationService(session_factory=RuntimeSession)
    actor = _actor(org_id)
    t0 = datetime.now(timezone.utc) - timedelta(hours=3)

    receipt = service.create_receipt_draft(
        organization_id=org_id,
        actor=actor,
        source_identifier=f"RODAL-UX10G-{suffix}",
        event_code=f"REC-UX10G-{suffix}",
        batch_code=f"MP-UX10G-{suffix}",
        product_name="Madera rolliza demo",
        quantity="100",
        unit="M3",
        occurred_at=t0,
        facility_reference="Planta Demo Corrientes",
    )
    assert receipt.status == "DRAFT"
    assert receipt.event_code == f"REC-UX10G-{suffix}"
    assert len(receipt.output_batch_public_ids) == 1

    receipt_posting = service.post_event(
        organization_id=org_id,
        event_public_id=receipt.event_public_id,
        actor=actor,
    )
    assert receipt_posting.status == "POSTED"

    process = service.create_process_draft(
        organization_id=org_id,
        actor=actor,
        event_code=f"PROC-UX10G-{suffix}",
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
                code=f"ASERRADO-UX10G-{suffix}",
                product_name="Madera aserrada demo",
                stage="FINISHED_GOOD",
                unit="M3",
                quantity=Decimal("65"),
            ),
        ),
        facility_reference="Planta Demo Corrientes",
    )
    assert process.status == "DRAFT"
    assert process.event_code == f"PROC-UX10G-{suffix}"
    assert len(process.output_batch_public_ids) == 1

    process_posting = service.post_event(
        organization_id=org_id,
        event_public_id=process.event_public_id,
        actor=actor,
    )
    assert process_posting.status == "POSTED"
    assert process_posting.unit_balances[0].input_quantity == Decimal("70.000000")
    assert process_posting.unit_balances[0].output_quantity == Decimal("65.000000")
    assert process_posting.unit_balances[0].loss_quantity == Decimal("5.000000")

    shipment = service.create_shipment_draft(
        organization_id=org_id,
        actor=actor,
        shipment_code=f"EXP-UX10G-{suffix}",
        sale_reference=f"FAC-UX10G-{suffix}",
        buyer_reference="Comprador UE Demo",
        destination_country="DE",
        items=(
            ShipmentItemDraft(
                batch_public_id=process.output_batch_public_ids[0],
                quantity=Decimal("60"),
            ),
        ),
    )
    assert shipment.status == "DRAFT"
    assert shipment.shipment_code == f"EXP-UX10G-{suffix}"

    dispatch = service.dispatch_shipment(
        organization_id=org_id,
        shipment_public_id=shipment.shipment_public_id,
        actor=actor,
    )
    assert dispatch.status == "DISPATCHED"

    snapshot = service.snapshot(organization_id=org_id)
    balances = {batch.code: batch.available for batch in snapshot.active_batches}
    assert balances[f"MP-UX10G-{suffix}"] == Decimal("30.000000")
    assert balances[f"ASERRADO-UX10G-{suffix}"] == Decimal("5.000000")
