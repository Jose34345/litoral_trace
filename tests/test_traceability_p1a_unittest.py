"""P1A unit contracts for the industrial genealogy schema."""
from __future__ import annotations

from pathlib import Path

from sqlalchemy import Numeric

from litoral_trace.db.base import Base
from litoral_trace.db.models import (
    Shipment,
    ShipmentItem,
    TraceabilityBatch,
    TraceabilityEvent,
    TraceabilityEventInput,
    TraceabilityEventOutput,
)
from litoral_trace.db.models.traceability import (
    TRACEABILITY_BATCH_STAGES,
    TRACEABILITY_EVENT_TYPES,
    TRACEABILITY_UNITS,
)


def _composite_fk_targets(table, constraint_name: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    constraint = next(
        fk for fk in table.foreign_key_constraints if fk.name == constraint_name
    )
    local = tuple(element.parent.name for element in constraint.elements)
    remote = tuple(element.target_fullname for element in constraint.elements)
    return local, remote


def test_p1a_registers_genealogy_tables() -> None:
    expected = {
        "traceability_batches",
        "traceability_events",
        "traceability_event_inputs",
        "traceability_event_outputs",
        "shipments",
        "shipment_items",
    }
    assert expected.issubset(Base.metadata.tables)


def test_traceability_batch_links_source_lote_with_tenant_guard() -> None:
    local, remote = _composite_fk_targets(
        TraceabilityBatch.__table__,
        "fk_traceability_batches_source_lote_tenant",
    )
    assert local == ("source_lote_id", "organization_id")
    assert remote == ("lotes.id", "lotes.organization_id")


def test_event_edges_are_many_to_many_and_quantity_safe() -> None:
    for model, fk_name in (
        (TraceabilityEventInput, "fk_traceability_event_inputs_batch_tenant"),
        (TraceabilityEventOutput, "fk_traceability_event_outputs_batch_tenant"),
    ):
        quantity_type = model.__table__.c.quantity.type
        assert isinstance(quantity_type, Numeric)
        assert quantity_type.precision == 18
        assert quantity_type.scale == 6

        local, remote = _composite_fk_targets(model.__table__, fk_name)
        assert local == ("batch_id", "organization_id")
        assert remote == (
            "traceability_batches.id",
            "traceability_batches.organization_id",
        )


def test_shipment_items_are_tenant_bound_to_shipment_and_batch() -> None:
    shipment_local, shipment_remote = _composite_fk_targets(
        ShipmentItem.__table__,
        "fk_shipment_items_shipment_tenant",
    )
    batch_local, batch_remote = _composite_fk_targets(
        ShipmentItem.__table__,
        "fk_shipment_items_batch_tenant",
    )

    assert shipment_local == ("shipment_id", "organization_id")
    assert shipment_remote == ("shipments.id", "shipments.organization_id")
    assert batch_local == ("batch_id", "organization_id")
    assert batch_remote == (
        "traceability_batches.id",
        "traceability_batches.organization_id",
    )


def test_p1a_domain_supports_required_industrial_operations() -> None:
    assert {
        "RECEIPT",
        "RAW_MATERIAL",
        "INTERMEDIATE",
        "FINISHED_GOOD",
    } <= TRACEABILITY_BATCH_STAGES
    assert {
        "RECEIPT",
        "TRANSFORMATION",
        "MIX",
        "SPLIT",
        "REPACK",
        "ADJUSTMENT",
    } <= TRACEABILITY_EVENT_TYPES
    assert {"TON", "KG", "M3"} == TRACEABILITY_UNITS


def test_p1a_migration_enables_and_forces_rls_for_all_new_tables() -> None:
    migration = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "019_add_traceability_genealogy.py"
    ).read_text(encoding="utf-8")

    assert 'revision: str = "019_add_traceability_genealogy"' in migration
    assert (
        'down_revision: Union[str, Sequence[str], None] = "018_add_batch_evidence_links"'
        in migration
    )
    for table_name in (
        "traceability_batches",
        "traceability_events",
        "traceability_event_inputs",
        "traceability_event_outputs",
        "shipments",
        "shipment_items",
    ):
        assert table_name in migration
    assert "ENABLE ROW LEVEL SECURITY" in migration
    assert "FORCE ROW LEVEL SECURITY" in migration
    assert "GRANT SELECT, INSERT, UPDATE" in migration


def test_model_classes_point_to_expected_tables() -> None:
    assert TraceabilityEvent.__tablename__ == "traceability_events"
    assert Shipment.__tablename__ == "shipments"
