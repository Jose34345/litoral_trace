from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.models import (
    Shipment,
    ShipmentItem,
    TraceabilityBatch,
    TraceabilityEvent,
    TraceabilityEventInput,
    TraceabilityEventOutput,
)
from litoral_trace.db.tenant import set_tenant_db_context

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT_DIR / ".env.integration"
EXPECTED_REVISION = "019_add_traceability_genealogy"


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
    reason="P1A PostgreSQL tests require the isolated integration contract.",
)


def _engine(url: str):
    return create_engine(normalize_database_url(url), pool_pre_ping=True, hide_parameters=True)


@pytest.fixture()
def pg_p1a():
    owner_engine = _engine(OWNER_URL)
    runtime_engine = _engine(RUNTIME_URL)
    RuntimeSession = sessionmaker(bind=runtime_engine, autoflush=False, expire_on_commit=False)
    suffix = uuid4().hex[:10]
    org_ids: list[int] = []
    lote_ids: list[int] = []

    with owner_engine.begin() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
        if revision != EXPECTED_REVISION:
            raise RuntimeError(f"P1A requires {EXPECTED_REVISION}; found {revision!r}.")

        for label in ("A", "B"):
            org_id = int(connection.execute(
                text("""
                    INSERT INTO public.organizations
                        (name, slug, tax_id, tier, description, is_active)
                    VALUES
                        (:name, :slug, :tax_id, 'pro', 'P1A integration', true)
                    RETURNING id
                """),
                {
                    "name": f"P1A Org {label} {suffix}",
                    "slug": f"p1a-{label.lower()}-{suffix}",
                    "tax_id": f"P1A-{label}-{suffix}",
                },
            ).scalar_one())
            org_ids.append(org_id)

            lote_id = int(connection.execute(
                text("""
                    INSERT INTO public.lotes (
                        organization_id, identificador, productor_id,
                        producto_forestal, hectareas, latitud, longitud,
                        estatus, volumen_ingresado_ton, volumen_exportar_ton
                    ) VALUES (
                        :organization_id, :identificador, :productor_id,
                        'Madera Aserrada (Pino)', 10.0, -27.45, -59.05,
                        'Pendiente', 100.0, 0.0
                    ) RETURNING id
                """),
                {
                    "organization_id": org_id,
                    "identificador": f"P1A-{label}-{suffix}",
                    "productor_id": f"PROV-{label}-{suffix}",
                },
            ).scalar_one())
            lote_ids.append(lote_id)

    try:
        yield RuntimeSession, org_ids, lote_ids, suffix
    finally:
        with owner_engine.begin() as connection:
            params = {"org_a": org_ids[0], "org_b": org_ids[1]}
            for table_name in (
                "shipment_items", "shipments",
                "traceability_event_inputs", "traceability_event_outputs",
                "traceability_events", "traceability_batches",
            ):
                connection.execute(
                    text(f"DELETE FROM public.{table_name} WHERE organization_id IN (:org_a, :org_b)"),
                    params,
                )
            connection.execute(
                text("DELETE FROM public.lotes WHERE organization_id IN (:org_a, :org_b)"), params
            )
            connection.execute(
                text("DELETE FROM public.organizations WHERE id IN (:org_a, :org_b)"), params
            )
        runtime_engine.dispose()
        owner_engine.dispose()


def _source_batch(session, *, organization_id: int, lote_id: int, code: str):
    batch = TraceabilityBatch(
        organization_id=organization_id,
        code=code,
        product_name="Rollizo de pino",
        stage="RAW_MATERIAL",
        unit="M3",
        status="ACTIVE",
        source_lote_id=lote_id,
    )
    session.add(batch)
    session.flush()
    return batch


def test_p1a_rls_and_composite_source_fk_fail_closed(pg_p1a) -> None:
    RuntimeSession, org_ids, lote_ids, suffix = pg_p1a
    org_a, org_b = org_ids
    lote_a, lote_b = lote_ids

    session = RuntimeSession()
    try:
        set_tenant_db_context(session, org_a)
        batch = _source_batch(
            session,
            organization_id=org_a,
            lote_id=lote_a,
            code=f"SOURCE-A-{suffix}",
        )
        batch_id = batch.id
        session.commit()
    finally:
        session.close()

    other_tenant = RuntimeSession()
    try:
        set_tenant_db_context(other_tenant, org_b)
        assert other_tenant.execute(
            select(TraceabilityBatch).where(TraceabilityBatch.id == batch_id)
        ).scalar_one_or_none() is None
    finally:
        other_tenant.rollback()
        other_tenant.close()

    cross_tenant = RuntimeSession()
    try:
        set_tenant_db_context(cross_tenant, org_a)
        cross_tenant.add(TraceabilityBatch(
            organization_id=org_a,
            code=f"ILLEGAL-{suffix}",
            product_name="Rollizo de pino",
            stage="RAW_MATERIAL",
            unit="M3",
            status="ACTIVE",
            source_lote_id=lote_b,
        ))
        with pytest.raises(IntegrityError):
            cross_tenant.commit()
    finally:
        cross_tenant.rollback()
        cross_tenant.close()


def test_p1a_many_to_many_genealogy_and_shipment_persist(pg_p1a) -> None:
    RuntimeSession, org_ids, lote_ids, suffix = pg_p1a
    org_a, lote_a = org_ids[0], lote_ids[0]
    session = RuntimeSession()

    try:
        set_tenant_db_context(session, org_a)
        source_1 = _source_batch(
            session, organization_id=org_a, lote_id=lote_a, code=f"INPUT-1-{suffix}"
        )
        source_2 = _source_batch(
            session, organization_id=org_a, lote_id=lote_a, code=f"INPUT-2-{suffix}"
        )
        output = TraceabilityBatch(
            organization_id=org_a,
            code=f"OUTPUT-{suffix}",
            product_name="Madera aserrada",
            stage="FINISHED_GOOD",
            unit="M3",
            status="ACTIVE",
        )
        session.add(output)
        session.flush()

        event = TraceabilityEvent(
            organization_id=org_a,
            event_code=f"MIX-{suffix}",
            event_type="MIX",
            status="DRAFT",
            occurred_at=datetime.now(timezone.utc),
            facility_reference="PLANTA-01",
        )
        session.add(event)
        session.flush()
        session.add_all([
            TraceabilityEventInput(
                organization_id=org_a, event_id=event.id, batch_id=source_1.id,
                quantity=Decimal("40.000000"), unit="M3"
            ),
            TraceabilityEventInput(
                organization_id=org_a, event_id=event.id, batch_id=source_2.id,
                quantity=Decimal("60.000000"), unit="M3"
            ),
            TraceabilityEventOutput(
                organization_id=org_a, event_id=event.id, batch_id=output.id,
                quantity=Decimal("80.000000"), unit="M3"
            ),
        ])

        shipment = Shipment(
            organization_id=org_a,
            shipment_code=f"EXP-{suffix}",
            sale_reference=f"SALE-{suffix}",
            buyer_reference="EU-BUYER",
            destination_country="DE",
            status="DRAFT",
        )
        session.add(shipment)
        session.flush()
        session.add(ShipmentItem(
            organization_id=org_a,
            shipment_id=shipment.id,
            batch_id=output.id,
            quantity=Decimal("50.000000"),
            unit="M3",
        ))
        session.commit()
        set_tenant_db_context(session, org_a)

        assert len(session.execute(
            select(TraceabilityEventInput).where(TraceabilityEventInput.event_id == event.id)
        ).scalars().all()) == 2
        assert session.execute(
            select(TraceabilityEventOutput).where(TraceabilityEventOutput.event_id == event.id)
        ).scalar_one().batch_id == output.id
        assert session.execute(
            select(ShipmentItem).where(ShipmentItem.shipment_id == shipment.id)
        ).scalar_one().batch_id == output.id
    finally:
        session.rollback()
        session.close()
