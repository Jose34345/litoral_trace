"""UX10-D acceptance for operational chain-of-custody workflows."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, select
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
)
from litoral_trace.services.audit import AuditActor
from litoral_trace.services.traceability_operations import (
    ProcessInputDraft,
    ProcessOutputDraft,
    ShipmentItemDraft,
    TraceabilityOperationAuthorizationError,
    TraceabilityOperationNotFoundError,
    TraceabilityOperationService,
    TraceabilityOperationValidationError,
)
from litoral_trace.web.traceability_operations import router as operations_router


@pytest.fixture()
def operations_env():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    with SessionLocal() as session:
        org = Organization(name="Aserradero UX10-D", slug="aserradero-ux10d")
        other = Organization(name="Tenant Ajeno UX10-D", slug="tenant-ajeno-ux10d")
        session.add_all([org, other])
        session.flush()
        session.add_all(
            [
                Lote(
                    organization_id=org.id,
                    identificador="RODAL-PINO-A",
                    productor_id="CUIT-PROVEEDOR-A",
                    producto_forestal="Pino resinoso",
                    hectareas=80.0,
                    latitud=-28.05,
                    longitud=-56.03,
                    estatus="Verde",
                ),
                Lote(
                    organization_id=org.id,
                    identificador="RODAL-PINO-B",
                    productor_id="CUIT-PROVEEDOR-B",
                    producto_forestal="Pino resinoso",
                    hectareas=65.0,
                    latitud=-28.06,
                    longitud=-56.04,
                    estatus="Verde",
                ),
                Lote(
                    organization_id=other.id,
                    identificador="RODAL-AJENO",
                    productor_id="CUIT-AJENO",
                    producto_forestal="Pino resinoso",
                    hectareas=20.0,
                    latitud=-27.0,
                    longitud=-55.0,
                    estatus="Verde",
                ),
            ]
        )
        session.commit()
        org_id = int(org.id)
        other_id = int(other.id)

    yield {
        "engine": engine,
        "SessionLocal": SessionLocal,
        "service": TraceabilityOperationService(session_factory=SessionLocal),
        "org_id": org_id,
        "other_id": other_id,
    }

    Base.metadata.drop_all(engine)
    engine.dispose()


def _actor(org_id: int, *, role: str = "manager") -> AuditActor:
    return AuditActor(
        organization_id=org_id,
        user_id=None,
        username="operaciones@virasoro.test",
        role=role,
    )


def _batch_public_id(SessionLocal, org_id: int, code: str):
    with SessionLocal() as session:
        return session.execute(
            select(TraceabilityBatch.public_id).where(
                TraceabilityBatch.organization_id == org_id,
                TraceabilityBatch.code == code,
            )
        ).scalar_one()


def _receipt(
    env,
    *,
    source: str,
    event_code: str,
    batch_code: str,
    quantity: str,
    occurred_at: datetime,
):
    result = env["service"].create_receipt_draft(
        organization_id=env["org_id"],
        actor=_actor(env["org_id"]),
        source_identifier=source,
        event_code=event_code,
        batch_code=batch_code,
        product_name="Rollizo de pino",
        quantity=quantity,
        unit="M3",
        occurred_at=occurred_at,
        facility_reference="Playa Virasoro",
    )
    env["service"].post_event(
        organization_id=env["org_id"],
        event_public_id=result.event_public_id,
        actor=_actor(env["org_id"]),
    )
    return result


def _post_corrientes_inputs(env):
    t0 = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)
    _receipt(
        env,
        source="RODAL-PINO-A",
        event_code="INGRESO-A-UX10D",
        batch_code="REC-A-UX10D",
        quantity="100",
        occurred_at=t0,
    )
    _receipt(
        env,
        source="RODAL-PINO-B",
        event_code="INGRESO-B-UX10D",
        batch_code="REC-B-UX10D",
        quantity="80",
        occurred_at=t0 + timedelta(minutes=10),
    )


def test_ux10d_corrientes_flow_uses_p1b_ledger_end_to_end(operations_env):
    env = operations_env
    _post_corrientes_inputs(env)

    process = env["service"].create_process_draft(
        organization_id=env["org_id"],
        actor=_actor(env["org_id"]),
        event_code="ASERRADO-UX10D-001",
        event_type="TRANSFORMATION",
        occurred_at=datetime(2026, 8, 20, 15, 0, tzinfo=timezone.utc),
        facility_reference="Planta Virasoro",
        inputs=(
            ProcessInputDraft(
                batch_public_id=_batch_public_id(
                    env["SessionLocal"], env["org_id"], "REC-A-UX10D"
                ),
                quantity=Decimal("70"),
            ),
            ProcessInputDraft(
                batch_public_id=_batch_public_id(
                    env["SessionLocal"], env["org_id"], "REC-B-UX10D"
                ),
                quantity=Decimal("30"),
            ),
        ),
        outputs=(
            ProcessOutputDraft(
                code="ASERRADO-UX10D-001",
                product_name="Madera aserrada de pino",
                stage="FINISHED_GOOD",
                unit="M3",
                quantity=Decimal("65"),
            ),
        ),
    )
    posting = env["service"].post_event(
        organization_id=env["org_id"],
        event_public_id=process.event_public_id,
        actor=_actor(env["org_id"]),
    )
    assert posting.status == "POSTED"
    assert posting.unit_balances[0].input_quantity == Decimal("100.000000")
    assert posting.unit_balances[0].output_quantity == Decimal("65.000000")
    assert posting.unit_balances[0].loss_quantity == Decimal("35.000000")
    assert posting.unit_balances[0].yield_ratio == Decimal("0.650000")

    snapshot = env["service"].snapshot(organization_id=env["org_id"])
    balances = {batch.code: batch.available for batch in snapshot.active_batches}
    assert balances["REC-A-UX10D"] == Decimal("30.000000")
    assert balances["REC-B-UX10D"] == Decimal("50.000000")
    assert balances["ASERRADO-UX10D-001"] == Decimal("65.000000")

    shipment = env["service"].create_shipment_draft(
        organization_id=env["org_id"],
        actor=_actor(env["org_id"]),
        shipment_code="EXP-UE-UX10D-001",
        sale_reference="FACTURA-E-UX10D-001",
        buyer_reference="BUYER-EU-UX10D",
        destination_country="DE",
        items=(
            ShipmentItemDraft(
                batch_public_id=_batch_public_id(
                    env["SessionLocal"], env["org_id"], "ASERRADO-UX10D-001"
                ),
                quantity=Decimal("60"),
            ),
        ),
    )
    assert shipment.status == "DRAFT"

    dispatch = env["service"].dispatch_shipment(
        organization_id=env["org_id"],
        shipment_public_id=shipment.shipment_public_id,
        actor=_actor(env["org_id"]),
    )
    assert dispatch.status == "DISPATCHED"

    snapshot = env["service"].snapshot(organization_id=env["org_id"])
    balances = {batch.code: batch.available for batch in snapshot.active_batches}
    assert balances["ASERRADO-UX10D-001"] == Decimal("5.000000")


def test_ux10d_hidden_cross_unit_conversion_fails_closed_and_keeps_draft(operations_env):
    env = operations_env
    _post_corrientes_inputs(env)
    process = env["service"].create_process_draft(
        organization_id=env["org_id"],
        actor=_actor(env["org_id"]),
        event_code="PELLET-CONVERSION-UX10D",
        event_type="TRANSFORMATION",
        occurred_at=datetime(2026, 8, 20, 16, 0, tzinfo=timezone.utc),
        inputs=(
            ProcessInputDraft(
                batch_public_id=_batch_public_id(
                    env["SessionLocal"], env["org_id"], "REC-A-UX10D"
                ),
                quantity=Decimal("50"),
            ),
        ),
        outputs=(
            ProcessOutputDraft(
                code="PELLET-UX10D-001",
                product_name="Pellet",
                stage="FINISHED_GOOD",
                unit="TON",
                quantity=Decimal("10"),
            ),
        ),
    )

    with pytest.raises(TraceabilityOperationValidationError) as exc_info:
        env["service"].post_event(
            organization_id=env["org_id"],
            event_public_id=process.event_public_id,
            actor=_actor(env["org_id"]),
        )
    assert exc_info.value.code == "UNIT_CONVERSION_REQUIRED"

    with env["SessionLocal"]() as session:
        status_value = session.execute(
            select(TraceabilityEvent.status).where(
                TraceabilityEvent.public_id == process.event_public_id
            )
        ).scalar_one()
    assert status_value == "DRAFT"


def test_ux10d_overdispatch_is_rejected_and_shipment_remains_recoverable(operations_env):
    env = operations_env
    _post_corrientes_inputs(env)
    batch_id = _batch_public_id(env["SessionLocal"], env["org_id"], "REC-A-UX10D")
    shipment = env["service"].create_shipment_draft(
        organization_id=env["org_id"],
        actor=_actor(env["org_id"]),
        shipment_code="OVERDISPATCH-UX10D",
        items=(ShipmentItemDraft(batch_public_id=batch_id, quantity=Decimal("101")),),
    )

    with pytest.raises(TraceabilityOperationValidationError) as exc_info:
        env["service"].dispatch_shipment(
            organization_id=env["org_id"],
            shipment_public_id=shipment.shipment_public_id,
            actor=_actor(env["org_id"]),
        )
    assert exc_info.value.code == "INSUFFICIENT_BATCH_STOCK"

    with env["SessionLocal"]() as session:
        status_value = session.execute(
            select(Shipment.status).where(
                Shipment.public_id == shipment.shipment_public_id
            )
        ).scalar_one()
    assert status_value == "DRAFT"


def test_ux10d_tenant_scope_blocks_foreign_actor_and_foreign_source(operations_env):
    env = operations_env
    with pytest.raises(TraceabilityOperationAuthorizationError):
        env["service"].create_receipt_draft(
            organization_id=env["org_id"],
            actor=_actor(env["other_id"]),
            source_identifier="RODAL-PINO-A",
            event_code="TENANT-DENIED-UX10D",
            batch_code="TENANT-DENIED-BATCH",
            product_name=None,
            quantity="10",
            unit="M3",
            occurred_at=datetime.now(timezone.utc),
        )

    with pytest.raises(TraceabilityOperationNotFoundError):
        env["service"].create_receipt_draft(
            organization_id=env["org_id"],
            actor=_actor(env["org_id"]),
            source_identifier="RODAL-AJENO",
            event_code="FOREIGN-SOURCE-UX10D",
            batch_code="FOREIGN-SOURCE-BATCH",
            product_name=None,
            quantity="10",
            unit="M3",
            occurred_at=datetime.now(timezone.utc),
        )


def test_ux10d_process_shape_rules_are_fail_closed(operations_env):
    env = operations_env
    _post_corrientes_inputs(env)
    a = _batch_public_id(env["SessionLocal"], env["org_id"], "REC-A-UX10D")

    with pytest.raises(TraceabilityOperationValidationError) as exc_info:
        env["service"].create_process_draft(
            organization_id=env["org_id"],
            actor=_actor(env["org_id"]),
            event_code="BAD-MIX-UX10D",
            event_type="MIX",
            occurred_at=datetime.now(timezone.utc),
            inputs=(ProcessInputDraft(batch_public_id=a, quantity=Decimal("10")),),
            outputs=(
                ProcessOutputDraft(
                    code="BAD-MIX-OUT",
                    product_name="Producto",
                    stage="INTERMEDIATE",
                    unit="M3",
                    quantity=Decimal("10"),
                ),
            ),
        )
    assert exc_info.value.code == "MIX_REQUIRES_MULTIPLE_INPUTS"

    with pytest.raises(TraceabilityOperationValidationError) as exc_info:
        env["service"].create_process_draft(
            organization_id=env["org_id"],
            actor=_actor(env["org_id"]),
            event_code="BAD-SPLIT-UX10D",
            event_type="SPLIT",
            occurred_at=datetime.now(timezone.utc),
            inputs=(ProcessInputDraft(batch_public_id=a, quantity=Decimal("10")),),
            outputs=(
                ProcessOutputDraft(
                    code="BAD-SPLIT-OUT",
                    product_name="Producto",
                    stage="INTERMEDIATE",
                    unit="M3",
                    quantity=Decimal("10"),
                ),
            ),
        )
    assert exc_info.value.code == "SPLIT_REQUIRES_MULTIPLE_OUTPUTS"


def test_ux10d_rbac_separates_operation_from_read_only_roles():
    def user(role: str):
        return SimpleNamespace(role=role)

    for role in ("superadmin", "admin", "manager"):
        assert has_permission(user(role), Permission.TRACEABILITY_OPERATE)
        assert has_permission(user(role), Permission.TRACEABILITY_DISPATCH)
    for role in ("auditor", "cliente"):
        assert not has_permission(user(role), Permission.TRACEABILITY_OPERATE)
        assert not has_permission(user(role), Permission.TRACEABILITY_DISPATCH)
        assert has_permission(user(role), Permission.LOTE_READ)


def test_ux10d_routes_and_template_keep_browser_as_presentation_layer():
    import json
    import os
    import subprocess
    import sys

    from litoral_trace.web.traceability_operations import router as runtime_operations_router

    expected_routes = {
        ("/operations", ("GET",)),
        ("/operations/receipts", ("POST",)),
        ("/operations/processes", ("POST",)),
        ("/operations/events/{event_public_id}/post", ("POST",)),
        ("/operations/shipments", ("POST",)),
        ("/operations/shipments/{shipment_public_id}/dispatch", ("POST",)),
    }
    declared_routes = {
        (route.path, tuple(sorted(route.methods or set())))
        for route in runtime_operations_router.routes
        if getattr(route, "path", "").startswith("/operations")
    }
    assert declared_routes == expected_routes

    root = Path(__file__).resolve().parents[1]
    env = dict(os.environ)
    env["PYTHONPATH"] = str(root / "src")
    probe_script = """
import json
import main

resolved = {
    "render_traceability_operations": str(
        main.app.url_path_for("render_traceability_operations")
    ),
    "create_receipt_operation": str(
        main.app.url_path_for("create_receipt_operation")
    ),
    "create_process_operation": str(
        main.app.url_path_for("create_process_operation")
    ),
    "post_existing_event_operation": str(
        main.app.url_path_for(
            "post_existing_event_operation",
            event_public_id="00000000-0000-0000-0000-000000000001",
        )
    ),
    "create_shipment_operation": str(
        main.app.url_path_for("create_shipment_operation")
    ),
    "dispatch_existing_shipment_operation": str(
        main.app.url_path_for(
            "dispatch_existing_shipment_operation",
            shipment_public_id="00000000-0000-0000-0000-000000000002",
        )
    ),
}
print("__UX10_RESOLVED__" + json.dumps(resolved, sort_keys=True))
"""
    probe = subprocess.run(
        [sys.executable, "-c", probe_script],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    payload_line = next(
        line
        for line in probe.stdout.splitlines()
        if line.startswith("__UX10_RESOLVED__")
    )
    resolved = json.loads(payload_line.removeprefix("__UX10_RESOLVED__"))
    assert resolved == {
        "render_traceability_operations": "/operations",
        "create_receipt_operation": "/operations/receipts",
        "create_process_operation": "/operations/processes",
        "post_existing_event_operation": (
            "/operations/events/00000000-0000-0000-0000-000000000001/post"
        ),
        "create_shipment_operation": "/operations/shipments",
        "dispatch_existing_shipment_operation": (
            "/operations/shipments/00000000-0000-0000-0000-000000000002/dispatch"
        ),
    }

    template = (
        root / "src/litoral_trace/templates/traceability_operations.html"
    ).read_text(encoding="utf-8")
    service_source = (
        root / "src/litoral_trace/services/traceability_operations.py"
    ).read_text(encoding="utf-8")

    assert "Borrador → ledger → trazabilidad" in template
    assert "no aplica densidades ni coeficientes ocultos" in template
    assert "{{ csrf_token }}" in template
    assert "fetch(" not in template
    assert "current_stock" not in service_source
    assert ".post_event(" in service_source
    assert ".dispatch_shipment(" in service_source