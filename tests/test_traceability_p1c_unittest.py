"""P1C unit acceptance for reverse industrial genealogy queries."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from litoral_trace.db.base import Base
from litoral_trace.db.models import (
    Lote,
    Organization,
    Shipment,
    ShipmentItem,
    TraceabilityBatch,
    TraceabilityEvent,
    TraceabilityEventInput,
    TraceabilityEventOutput,
)
from litoral_trace.services.traceability_lineage import TraceabilityLineageService


NOW = datetime(2026, 8, 20, 18, 0, tzinfo=timezone.utc)


def _session_factory():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return engine, sessionmaker(bind=engine, expire_on_commit=False)


def _source_lote(session, *, organization_id: int, identificador: str, productor_id: str):
    lote = Lote(
        organization_id=organization_id,
        identificador=identificador,
        productor_id=productor_id,
        producto_forestal="Pino resinoso",
        hectareas=50.0,
        latitud=-28.05,
        longitud=-56.03,
        polygon_wkt="POLYGON((-56.04 -28.06,-56.02 -28.06,-56.02 -28.04,-56.04 -28.04,-56.04 -28.06))",
        estatus="Verde",
    )
    session.add(lote)
    session.flush()
    return lote


def _batch(
    session,
    *,
    organization_id: int,
    code: str,
    product_name: str,
    stage: str,
    source_lote_id: int | None = None,
):
    batch = TraceabilityBatch(
        organization_id=organization_id,
        code=code,
        product_name=product_name,
        stage=stage,
        unit="M3",
        status="ACTIVE",
        source_lote_id=source_lote_id,
    )
    session.add(batch)
    session.flush()
    return batch


def _posted_event(
    session,
    *,
    organization_id: int,
    code: str,
    event_type: str,
    inputs: tuple[tuple[TraceabilityBatch, Decimal], ...] = (),
    outputs: tuple[tuple[TraceabilityBatch, Decimal], ...] = (),
):
    event = TraceabilityEvent(
        organization_id=organization_id,
        event_code=code,
        event_type=event_type,
        status="POSTED",
        occurred_at=NOW,
        facility_reference="Planta Virasoro",
    )
    session.add(event)
    session.flush()
    for batch, quantity in inputs:
        session.add(
            TraceabilityEventInput(
                organization_id=organization_id,
                event_id=event.id,
                batch_id=batch.id,
                quantity=quantity,
                unit="M3",
            )
        )
    for batch, quantity in outputs:
        session.add(
            TraceabilityEventOutput(
                organization_id=organization_id,
                event_id=event.id,
                batch_id=batch.id,
                quantity=quantity,
                unit="M3",
            )
        )
    session.flush()
    return event


def _shipment(
    session,
    *,
    organization_id: int,
    code: str,
    batch: TraceabilityBatch,
    quantity: Decimal,
    status: str = "DISPATCHED",
):
    shipment = Shipment(
        organization_id=organization_id,
        shipment_code=code,
        sale_reference="FACTURA-E-001",
        buyer_reference="BUYER-EU-001",
        destination_country="DE",
        status=status,
        shipped_at=NOW,
    )
    session.add(shipment)
    session.flush()
    session.add(
        ShipmentItem(
            organization_id=organization_id,
            shipment_id=shipment.id,
            batch_id=batch.id,
            quantity=quantity,
            unit="M3",
        )
    )
    session.flush()
    return shipment


def test_p1c_corrientes_reverse_lineage_attributes_70_30_mix_to_shipment() -> None:
    engine, SessionLocal = _session_factory()
    try:
        with SessionLocal() as session:
            org = Organization(name="Aserradero Virasoro", slug="p1c-virasoro")
            session.add(org)
            session.flush()

            lote_a = _source_lote(
                session,
                organization_id=org.id,
                identificador="RODAL-A",
                productor_id="CUIT-PROVEEDOR-A",
            )
            lote_b = _source_lote(
                session,
                organization_id=org.id,
                identificador="RODAL-B",
                productor_id="CUIT-PROVEEDOR-B",
            )
            raw_a = _batch(
                session,
                organization_id=org.id,
                code="REC-A-001",
                product_name="Rollizo de pino",
                stage="RAW_MATERIAL",
                source_lote_id=lote_a.id,
            )
            raw_b = _batch(
                session,
                organization_id=org.id,
                code="REC-B-001",
                product_name="Rollizo de pino",
                stage="RAW_MATERIAL",
                source_lote_id=lote_b.id,
            )
            finished = _batch(
                session,
                organization_id=org.id,
                code="ASERRADO-001",
                product_name="Madera aserrada de pino",
                stage="FINISHED_GOOD",
            )

            _posted_event(
                session,
                organization_id=org.id,
                code="INGRESO-A-001",
                event_type="RECEIPT",
                outputs=((raw_a, Decimal("100.000000")),),
            )
            _posted_event(
                session,
                organization_id=org.id,
                code="INGRESO-B-001",
                event_type="RECEIPT",
                outputs=((raw_b, Decimal("80.000000")),),
            )
            _posted_event(
                session,
                organization_id=org.id,
                code="ASERRADO-TURNO-001",
                event_type="TRANSFORMATION",
                inputs=(
                    (raw_a, Decimal("70.000000")),
                    (raw_b, Decimal("30.000000")),
                ),
                outputs=((finished, Decimal("65.000000")),),
            )
            _shipment(
                session,
                organization_id=org.id,
                code="EXP-UE-2026-001",
                batch=finished,
                quantity=Decimal("60.000000"),
            )
            session.commit()

            payload = TraceabilityLineageService(
                session=session,
                organization_id=org.id,
            ).trace_shipment("exp-ue-2026-001")

            assert payload["complete"] is True
            assert payload["shipment"]["lineage_state"] == "FINAL"
            assert payload["allocation_method"] == "PROPORTIONAL_INPUT_ALLOCATION"
            assert payload["unit_totals"] == [
                {
                    "unit": "M3",
                    "shipped_quantity": "60.000000",
                    "attributed_quantity": "60.000000",
                    "unresolved_quantity": "0.000000",
                }
            ]

            sources = {
                item["lote"]["identificador"]: item
                for item in payload["source_lotes"]
            }
            assert sources["RODAL-A"]["attributed_shipment_quantity"] == "42.000000"
            assert sources["RODAL-A"]["share_of_shipped_unit"] == "0.700000"
            assert sources["RODAL-B"]["attributed_shipment_quantity"] == "18.000000"
            assert sources["RODAL-B"]["share_of_shipped_unit"] == "0.300000"
            assert sources["RODAL-A"]["lote"]["productor_id"] == "CUIT-PROVEEDOR-A"

            events = {event["event_code"]: event for event in payload["events"]}
            assert set(events) == {
                "INGRESO-A-001",
                "INGRESO-B-001",
                "ASERRADO-TURNO-001",
            }
            assert events["ASERRADO-TURNO-001"]["reconciliation"] == {
                "unit": "M3",
                "input_quantity": "100.000000",
                "output_quantity": "65.000000",
                "loss_quantity": "35.000000",
                "yield_ratio": "0.650000",
            }
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


def test_p1c_missing_provenance_is_explicit_and_unresolved() -> None:
    engine, SessionLocal = _session_factory()
    try:
        with SessionLocal() as session:
            org = Organization(name="Aserradero Incompleto", slug="p1c-incomplete")
            session.add(org)
            session.flush()
            orphan = _batch(
                session,
                organization_id=org.id,
                code="SIN-ORIGEN-001",
                product_name="Madera sin genealogía",
                stage="FINISHED_GOOD",
            )
            _shipment(
                session,
                organization_id=org.id,
                code="EXP-INCOMPLETO-001",
                batch=orphan,
                quantity=Decimal("25.000000"),
                status="CONFIRMED",
            )
            session.commit()

            payload = TraceabilityLineageService(
                session=session,
                organization_id=org.id,
            ).trace_shipment("EXP-INCOMPLETO-001")

            assert payload["complete"] is False
            assert payload["shipment"]["lineage_state"] == "PREVIEW"
            assert payload["unit_totals"][0]["unresolved_quantity"] == "25.000000"
            assert "MISSING_PROVENANCE" in {
                issue["code"] for issue in payload["issues"]
            }
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


def test_p1c_detects_corrupt_genealogy_cycle() -> None:
    engine, SessionLocal = _session_factory()
    try:
        with SessionLocal() as session:
            org = Organization(name="Aserradero Ciclo", slug="p1c-cycle")
            session.add(org)
            session.flush()
            batch_a = _batch(
                session,
                organization_id=org.id,
                code="CYCLE-A",
                product_name="Intermedio A",
                stage="INTERMEDIATE",
            )
            batch_b = _batch(
                session,
                organization_id=org.id,
                code="CYCLE-B",
                product_name="Intermedio B",
                stage="INTERMEDIATE",
            )
            _posted_event(
                session,
                organization_id=org.id,
                code="CYCLE-EVENT-A",
                event_type="TRANSFORMATION",
                inputs=((batch_b, Decimal("10.000000")),),
                outputs=((batch_a, Decimal("10.000000")),),
            )
            _posted_event(
                session,
                organization_id=org.id,
                code="CYCLE-EVENT-B",
                event_type="TRANSFORMATION",
                inputs=((batch_a, Decimal("10.000000")),),
                outputs=((batch_b, Decimal("10.000000")),),
            )
            _shipment(
                session,
                organization_id=org.id,
                code="EXP-CYCLE-001",
                batch=batch_a,
                quantity=Decimal("5.000000"),
            )
            session.commit()

            payload = TraceabilityLineageService(
                session=session,
                organization_id=org.id,
            ).trace_shipment("EXP-CYCLE-001")

            assert payload["complete"] is False
            assert "LINEAGE_CYCLE_DETECTED" in {
                issue["code"] for issue in payload["issues"]
            }
            assert payload["unit_totals"][0]["unresolved_quantity"] == "5.000000"
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()
