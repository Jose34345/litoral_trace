from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.models import (
    TraceabilityBatch,
    TraceabilityEvent,
    TraceabilityEventInput,
    TraceabilityEventOutput,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import AuditActor
from litoral_trace.services.traceability_ledger import (
    TraceabilityLedgerService,
    TraceabilityValidationError,
)


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
    reason="P1B PostgreSQL tests require the isolated integration contract.",
)


def _engine(url: str):
    return create_engine(
        normalize_database_url(url),
        pool_pre_ping=True,
        hide_parameters=True,
    )


@pytest.fixture()
def pg_p1b():
    owner_engine = _engine(OWNER_URL)
    runtime_engine = _engine(RUNTIME_URL)
    RuntimeSession = sessionmaker(
        bind=runtime_engine,
        autoflush=False,
        expire_on_commit=False,
    )
    suffix = uuid4().hex[:10]

    with owner_engine.begin() as connection:
        revision = connection.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one()
        if revision != EXPECTED_REVISION:
            raise RuntimeError(
                f"P1B requires {EXPECTED_REVISION}; found {revision!r}."
            )

        org_id = int(
            connection.execute(
                text(
                    """
                    INSERT INTO public.organizations
                        (name, slug, tax_id, tier, description, is_active)
                    VALUES
                        (:name, :slug, :tax_id, 'pro', 'P1B ledger integration', true)
                    RETURNING id
                    """
                ),
                {
                    "name": f"P1B Aserradero Virasoro {suffix}",
                    "slug": f"p1b-virasoro-{suffix}",
                    "tax_id": f"P1B-{suffix}",
                },
            ).scalar_one()
        )
        lote_id = int(
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
                    ) RETURNING id
                    """
                ),
                {
                    "organization_id": org_id,
                    "identificador": f"RODAL-P1B-{suffix}",
                    "productor_id": f"PROV-P1B-{suffix}",
                },
            ).scalar_one()
        )

    try:
        yield RuntimeSession, owner_engine, org_id, lote_id, suffix
    finally:
        with owner_engine.begin() as connection:
            params = {"org_id": org_id}
            for table_name in (
                "audit_logs",
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
        username="jefe.planta@virasoro.integration",
        role="admin",
    )


def _seed_concurrent_events(RuntimeSession, *, org_id: int, lote_id: int, suffix: str):
    t0 = datetime.now(timezone.utc) - timedelta(hours=2)
    session = RuntimeSession()
    try:
        set_tenant_db_context(session, org_id)
        source = TraceabilityBatch(
            organization_id=org_id,
            code=f"ROLLIZO-{suffix}",
            product_name="Rollizo de pino",
            stage="RAW_MATERIAL",
            unit="M3",
            status="ACTIVE",
            source_lote_id=lote_id,
        )
        output_a = TraceabilityBatch(
            organization_id=org_id,
            code=f"ASERRADO-A-{suffix}",
            product_name="Madera aserrada",
            stage="FINISHED_GOOD",
            unit="M3",
            status="ACTIVE",
        )
        output_b = TraceabilityBatch(
            organization_id=org_id,
            code=f"ASERRADO-B-{suffix}",
            product_name="Madera aserrada",
            stage="FINISHED_GOOD",
            unit="M3",
            status="ACTIVE",
        )
        session.add_all([source, output_a, output_b])
        session.flush()

        receipt = TraceabilityEvent(
            organization_id=org_id,
            event_code=f"RECEIPT-{suffix}",
            event_type="RECEIPT",
            status="DRAFT",
            occurred_at=t0,
            facility_reference="Virasoro",
        )
        session.add(receipt)
        session.flush()
        session.add(
            TraceabilityEventOutput(
                organization_id=org_id,
                event_id=receipt.id,
                batch_id=source.id,
                quantity=Decimal("100.000000"),
                unit="M3",
            )
        )

        events: list[TraceabilityEvent] = []
        for label, output in (("A", output_a), ("B", output_b)):
            event = TraceabilityEvent(
                organization_id=org_id,
                event_code=f"SAW-{label}-{suffix}",
                event_type="TRANSFORMATION",
                status="DRAFT",
                occurred_at=t0 + timedelta(hours=1),
                facility_reference="Virasoro",
            )
            session.add(event)
            session.flush()
            session.add_all(
                [
                    TraceabilityEventInput(
                        organization_id=org_id,
                        event_id=event.id,
                        batch_id=source.id,
                        quantity=Decimal("80.000000"),
                        unit="M3",
                    ),
                    TraceabilityEventOutput(
                        organization_id=org_id,
                        event_id=event.id,
                        batch_id=output.id,
                        quantity=Decimal("50.000000"),
                        unit="M3",
                    ),
                ]
            )
            events.append(event)

        session.commit()
        return int(source.id), int(receipt.id), tuple(int(event.id) for event in events)
    finally:
        session.close()


def test_p1b_postgres_serializes_competing_consumers(pg_p1b) -> None:
    RuntimeSession, _, org_id, lote_id, suffix = pg_p1b
    source_id, receipt_id, event_ids = _seed_concurrent_events(
        RuntimeSession,
        org_id=org_id,
        lote_id=lote_id,
        suffix=suffix,
    )
    service = TraceabilityLedgerService(session_factory=RuntimeSession)
    actor = _actor(org_id)

    receipt_result = service.post_event(
        organization_id=org_id,
        event_id=receipt_id,
        actor=actor,
    )
    assert receipt_result.status == "POSTED"

    def _post(event_id: int):
        local_service = TraceabilityLedgerService(session_factory=RuntimeSession)
        try:
            result = local_service.post_event(
                organization_id=org_id,
                event_id=event_id,
                actor=actor,
            )
            return ("posted", result.event_id)
        except TraceabilityValidationError as exc:
            return ("rejected", exc.code)

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(_post, event_ids))

    assert sorted(outcome[0] for outcome in outcomes) == ["posted", "rejected"]
    rejected_codes = [value for state, value in outcomes if state == "rejected"]
    assert rejected_codes == ["INSUFFICIENT_BATCH_STOCK"]

    balance = service.get_batch_balance(
        organization_id=org_id,
        batch_id=source_id,
    )
    assert balance.produced == Decimal("100.000000")
    assert balance.consumed == Decimal("80.000000")
    assert balance.available == Decimal("20.000000")

    session = RuntimeSession()
    try:
        set_tenant_db_context(session, org_id)
        posted_transformations = int(
            session.execute(
                select(func.count(TraceabilityEvent.id)).where(
                    TraceabilityEvent.organization_id == org_id,
                    TraceabilityEvent.event_type == "TRANSFORMATION",
                    TraceabilityEvent.status == "POSTED",
                )
            ).scalar_one()
        )
        assert posted_transformations == 1
    finally:
        session.rollback()
        session.close()
