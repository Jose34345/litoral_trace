from __future__ import annotations

from sqlalchemy import func, select

from litoral_trace.db.models import (
    UsLaceyOperation,
    UsLaceyOperationField,
    UsLaceyPpqPlantLine,
)
from litoral_trace.us_lacey.canonical_shipment_truth import publish_canonical_shipment_truth
from litoral_trace.us_lacey.ppq505 import PPQ505_PLANT_FIELDS
from tests.test_us_lacey_canonical_shipment_truth_postgres import _seed_two_lines
from tests.us_lacey_engine2_postgres import tenant_session


def _add_stale_machine_line(session, *, org: int, operation_id: int, ordinal: int) -> UsLaceyPpqPlantLine:
    reference = f"CANONICAL-{ordinal}"
    line = UsLaceyPpqPlantLine(
        organization_id=org,
        operation_id=operation_id,
        line_reference=reference,
        ordinal=ordinal,
    )
    session.add(line)
    session.flush()
    for contract in PPQ505_PLANT_FIELDS:
        session.add(
            UsLaceyOperationField(
                organization_id=org,
                operation_id=operation_id,
                merchandise_line_reference=reference,
                field_name=contract.key,
                field_scope="PLANT_LINE",
                plant_line_id=line.id,
                field_status="MISSING",
                validation_status="MISSING",
                confidence=0.0,
                extractor="canonical-shipment-truth",
                extractor_version="lacey_canonical_shipment_truth_v1",
            )
        )
    return line


def test_publication_compacts_unreviewed_stale_machine_canonical_lines(
    engine2_postgres_session_factory,
):
    factory = engine2_postgres_session_factory
    org, operation_id, _ = _seed_two_lines(factory)
    session = tenant_session(factory, org)
    operation = session.get(UsLaceyOperation, operation_id)
    _add_stale_machine_line(session, org=org, operation_id=operation_id, ordinal=3)
    _add_stale_machine_line(session, org=org, operation_id=operation_id, ordinal=4)
    operation.merchandise_line_count = 4
    session.commit()

    result = publish_canonical_shipment_truth(
        session,
        organization_id=org,
        operation_id=operation_id,
    )
    session.commit()

    remaining = session.scalars(
        select(UsLaceyPpqPlantLine)
        .where(
            UsLaceyPpqPlantLine.organization_id == org,
            UsLaceyPpqPlantLine.operation_id == operation_id,
        )
        .order_by(UsLaceyPpqPlantLine.ordinal.asc())
    ).all()
    session.refresh(operation)

    assert result.line_count == 2
    assert [line.line_reference for line in remaining] == ["1", "2"]
    assert operation.merchandise_line_count == 2
    assert session.scalar(
        select(func.count(UsLaceyOperationField.id)).where(
            UsLaceyOperationField.organization_id == org,
            UsLaceyOperationField.operation_id == operation_id,
            UsLaceyOperationField.plant_line_id.is_not(None),
        )
    ) == 2 * len(PPQ505_PLANT_FIELDS)
    session.close()
