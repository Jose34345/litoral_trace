"""P1B unit acceptance for Corrientes-style forestry inventory ledger."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from litoral_trace.db.base import Base
from litoral_trace.db.models import (
    AuditLog,
    Lote,
    Organization,
    Shipment,
    ShipmentItem,
    TraceabilityBatch,
    TraceabilityEvent,
    TraceabilityEventInput,
    TraceabilityEventOutput,
)
from litoral_trace.services.audit import AuditActor
from litoral_trace.services.traceability_ledger import (
    TraceabilityAuthorizationError,
    TraceabilityLedgerService,
    TraceabilityStateError,
    TraceabilityValidationError,
)


@pytest.fixture()
def ledger_env():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        org = Organization(name="Aserradero Virasoro", slug="aserradero-virasoro-p1b")
        other_org = Organization(name="Otro Tenant", slug="otro-tenant-p1b")
        session.add_all([org, other_org])
        session.flush()

        plot_a = Lote(
            organization_id=org.id,
            identificador="RODAL-PINO-A",
            productor_id="CUIT-PROVEEDOR-A",
            producto_forestal="Pino resinoso",
            hectareas=80.0,
            latitud=-28.05,
            longitud=-56.03,
            estatus="Verde",
        )
        plot_b = Lote(
            organization_id=org.id,
            identificador="RODAL-PINO-B",
            productor_id="CUIT-PROVEEDOR-B",
            producto_forestal="Pino resinoso",
            hectareas=65.0,
            latitud=-28.06,
            longitud=-56.04,
            estatus="Verde",
        )
        session.add_all([plot_a, plot_b])
        session.flush()

        session.add_all(
            [
                TraceabilityBatch(
                    organization_id=org.id,
                    code="REC-A-001",
                    product_name="Rollizo de pino",
                    stage="RAW_MATERIAL",
                    unit="M3",
                    source_lote_id=plot_a.id,
                ),
                TraceabilityBatch(
                    organization_id=org.id,
                    code="REC-B-001",
                    product_name="Rollizo de pino",
                    stage="RAW_MATERIAL",
                    unit="M3",
                    source_lote_id=plot_b.id,
                ),
                TraceabilityBatch(
                    organization_id=org.id,
                    code="ASERRADO-001",
                    product_name="Madera aserrada de pino",
                    stage="FINISHED_GOOD",
                    unit="M3",
                ),
                TraceabilityBatch(
                    organization_id=org.id,
                    code="PELLET-TEST-001",
                    product_name="Pellet",
                    stage="FINISHED_GOOD",
                    unit="TON",
                ),
                TraceabilityBatch(
                    organization_id=org.id,
                    code="SIN-ORIGEN-001",
                    product_name="Rollizo sin parcela",
                    stage="RAW_MATERIAL",
                    unit="M3",
                ),
            ]
        )
        session.commit()
        org_id = int(org.id)
        other_org_id = int(other_org.id)

    yield {
        "engine": engine,
        "SessionLocal": SessionLocal,
        "service": TraceabilityLedgerService(session_factory=SessionLocal),
        "org_id": org_id,
        "other_org_id": other_org_id,
    }
    Base.metadata.drop_all(engine)
    engine.dispose()


def _actor(org_id: int) -> AuditActor:
    return AuditActor(
        organization_id=org_id,
        user_id=None,
        username="jefe.planta@virasoro.test",
        role="admin",
    )


def _batch_id(SessionLocal, code: str) -> int:
    with SessionLocal() as session:
        return int(
            session.execute(
                select(TraceabilityBatch.id).where(TraceabilityBatch.code == code)
            ).scalar_one()
        )


def _create_event(
    SessionLocal,
    *,
    org_id: int,
    code: str,
    event_type: str,
    inputs: tuple[tuple[str, str, Decimal], ...] = (),
    outputs: tuple[tuple[str, str, Decimal], ...] = (),
    occurred_at: datetime | None = None,
) -> int:
    occurred_at = occurred_at or datetime.now(timezone.utc)
    with SessionLocal() as session:
        event = TraceabilityEvent(
            organization_id=org_id,
            event_code=code,
            event_type=event_type,
            status="DRAFT",
            occurred_at=occurred_at,
            facility_reference="Planta Virasoro",
        )
        session.add(event)
        session.flush()
        for batch_code, unit, quantity in inputs:
            batch_id = int(
                session.execute(
                    select(TraceabilityBatch.id).where(
                        TraceabilityBatch.organization_id == org_id,
                        TraceabilityBatch.code == batch_code,
                    )
                ).scalar_one()
            )
            session.add(
                TraceabilityEventInput(
                    organization_id=org_id,
                    event_id=event.id,
                    batch_id=batch_id,
                    quantity=quantity,
                    unit=unit,
                )
            )
        for batch_code, unit, quantity in outputs:
            batch_id = int(
                session.execute(
                    select(TraceabilityBatch.id).where(
                        TraceabilityBatch.organization_id == org_id,
                        TraceabilityBatch.code == batch_code,
                    )
                ).scalar_one()
            )
            session.add(
                TraceabilityEventOutput(
                    organization_id=org_id,
                    event_id=event.id,
                    batch_id=batch_id,
                    quantity=quantity,
                    unit=unit,
                )
            )
        session.commit()
        return int(event.id)


def _post_receipts(env) -> tuple[int, int]:
    t0 = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)
    receipt_a = _create_event(
        env["SessionLocal"],
        org_id=env["org_id"],
        code="INGRESO-A-001",
        event_type="RECEIPT",
        outputs=(("REC-A-001", "M3", Decimal("100.000000")),),
        occurred_at=t0,
    )
    receipt_b = _create_event(
        env["SessionLocal"],
        org_id=env["org_id"],
        code="INGRESO-B-001",
        event_type="RECEIPT",
        outputs=(("REC-B-001", "M3", Decimal("80.000000")),),
        occurred_at=t0 + timedelta(minutes=10),
    )
    env["service"].post_event(
        organization_id=env["org_id"], event_id=receipt_a, actor=_actor(env["org_id"])
    )
    env["service"].post_event(
        organization_id=env["org_id"], event_id=receipt_b, actor=_actor(env["org_id"])
    )
    return receipt_a, receipt_b


def test_corrientes_flow_reconciles_two_origins_transformation_and_dispatch(ledger_env):
    env = ledger_env
    _post_receipts(env)

    process_id = _create_event(
        env["SessionLocal"],
        org_id=env["org_id"],
        code="ASERRADO-TURNO-001",
        event_type="TRANSFORMATION",
        inputs=(
            ("REC-A-001", "M3", Decimal("70.000000")),
            ("REC-B-001", "M3", Decimal("30.000000")),
        ),
        outputs=(("ASERRADO-001", "M3", Decimal("65.000000")),),
        occurred_at=datetime(2026, 8, 20, 14, 0, tzinfo=timezone.utc),
    )
    result = env["service"].post_event(
        organization_id=env["org_id"],
        event_id=process_id,
        actor=_actor(env["org_id"]),
    )

    assert result.status == "POSTED"
    assert result.unit_balances[0].unit == "M3"
    assert result.unit_balances[0].input_quantity == Decimal("100.000000")
    assert result.unit_balances[0].output_quantity == Decimal("65.000000")
    assert result.unit_balances[0].loss_quantity == Decimal("35.000000")
    assert result.unit_balances[0].yield_ratio == Decimal("0.650000")

    raw_a = env["service"].get_batch_balance(
        organization_id=env["org_id"], batch_id=_batch_id(env["SessionLocal"], "REC-A-001")
    )
    raw_b = env["service"].get_batch_balance(
        organization_id=env["org_id"], batch_id=_batch_id(env["SessionLocal"], "REC-B-001")
    )
    finished = env["service"].get_batch_balance(
        organization_id=env["org_id"], batch_id=_batch_id(env["SessionLocal"], "ASERRADO-001")
    )
    assert raw_a.available == Decimal("30.000000")
    assert raw_b.available == Decimal("50.000000")
    assert finished.available == Decimal("65.000000")

    with env["SessionLocal"]() as session:
        shipment = Shipment(
            organization_id=env["org_id"],
            shipment_code="EXP-UE-2026-001",
            sale_reference="FACTURA-E-001",
            buyer_reference="BUYER-EU-001",
            destination_country="DE",
            status="CONFIRMED",
            shipped_at=datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc),
        )
        session.add(shipment)
        session.flush()
        session.add(
            ShipmentItem(
                organization_id=env["org_id"],
                shipment_id=shipment.id,
                batch_id=_batch_id(env["SessionLocal"], "ASERRADO-001"),
                quantity=Decimal("60.000000"),
                unit="M3",
            )
        )
        session.commit()
        shipment_id = int(shipment.id)

    dispatch = env["service"].dispatch_shipment(
        organization_id=env["org_id"],
        shipment_id=shipment_id,
        actor=_actor(env["org_id"]),
    )
    assert dispatch.status == "DISPATCHED"

    finished_after = env["service"].get_batch_balance(
        organization_id=env["org_id"], batch_id=_batch_id(env["SessionLocal"], "ASERRADO-001")
    )
    assert finished_after.produced == Decimal("65.000000")
    assert finished_after.dispatched == Decimal("60.000000")
    assert finished_after.available == Decimal("5.000000")

    with env["SessionLocal"]() as session:
        actions = set(session.execute(select(AuditLog.action)).scalars().all())
    assert "traceability.event.post" in actions
    assert "traceability.shipment.dispatch" in actions


def test_p1b_blocks_overconsumption(ledger_env):
    env = ledger_env
    _post_receipts(env)
    event_id = _create_event(
        env["SessionLocal"],
        org_id=env["org_id"],
        code="SOBRECONSUMO-001",
        event_type="TRANSFORMATION",
        inputs=(("REC-A-001", "M3", Decimal("101.000000")),),
        outputs=(("ASERRADO-001", "M3", Decimal("60.000000")),),
        occurred_at=datetime(2026, 8, 20, 15, 0, tzinfo=timezone.utc),
    )
    with pytest.raises(TraceabilityValidationError) as exc_info:
        env["service"].post_event(
            organization_id=env["org_id"],
            event_id=event_id,
            actor=_actor(env["org_id"]),
        )
    assert exc_info.value.code == "INSUFFICIENT_BATCH_STOCK"


def test_p1b_fails_closed_on_hidden_m3_to_ton_conversion(ledger_env):
    env = ledger_env
    _post_receipts(env)
    event_id = _create_event(
        env["SessionLocal"],
        org_id=env["org_id"],
        code="CONVERSION-NO-DOCUMENTADA-001",
        event_type="TRANSFORMATION",
        inputs=(("REC-A-001", "M3", Decimal("50.000000")),),
        outputs=(("PELLET-TEST-001", "TON", Decimal("10.000000")),),
        occurred_at=datetime(2026, 8, 20, 15, 0, tzinfo=timezone.utc),
    )
    with pytest.raises(TraceabilityValidationError) as exc_info:
        env["service"].post_event(
            organization_id=env["org_id"],
            event_id=event_id,
            actor=_actor(env["org_id"]),
        )
    assert exc_info.value.code == "UNIT_CONVERSION_REQUIRED"


def test_p1b_receipt_requires_source_plot(ledger_env):
    env = ledger_env
    event_id = _create_event(
        env["SessionLocal"],
        org_id=env["org_id"],
        code="INGRESO-SIN-ORIGEN-001",
        event_type="RECEIPT",
        outputs=(("SIN-ORIGEN-001", "M3", Decimal("20.000000")),),
    )
    with pytest.raises(TraceabilityValidationError) as exc_info:
        env["service"].post_event(
            organization_id=env["org_id"],
            event_id=event_id,
            actor=_actor(env["org_id"]),
        )
    assert exc_info.value.code == "RECEIPT_WITHOUT_SOURCE_PLOT"


def test_p1b_event_can_only_be_posted_once(ledger_env):
    env = ledger_env
    receipt_a, _ = _post_receipts(env)
    with pytest.raises(TraceabilityStateError):
        env["service"].post_event(
            organization_id=env["org_id"],
            event_id=receipt_a,
            actor=_actor(env["org_id"]),
        )


def test_p1b_actor_cannot_post_for_another_tenant(ledger_env):
    env = ledger_env
    with pytest.raises(TraceabilityAuthorizationError):
        env["service"].get_batch_balance(
            organization_id=env["org_id"],
            batch_id=_batch_id(env["SessionLocal"], "REC-A-001"),
        )
        # get_batch_balance has no actor by design; posting is the protected write.

    # Explicitly exercise the write scope check.
    event_id = _create_event(
        env["SessionLocal"],
        org_id=env["org_id"],
        code="TENANT-SCOPE-001",
        event_type="RECEIPT",
        outputs=(("REC-A-001", "M3", Decimal("1.000000")),),
    )
    with pytest.raises(TraceabilityAuthorizationError):
        env["service"].post_event(
            organization_id=env["org_id"],
            event_id=event_id,
            actor=_actor(env["other_org_id"]),
        )
