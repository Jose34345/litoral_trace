from __future__ import annotations

from types import SimpleNamespace

from litoral_trace.db.models import (
    UsLaceyOperationField,
    UsLaceyPlantDeclaration,
    UsLaceyPpqPlantLine,
)
from litoral_trace.us_lacey.ppq505 import PPQ505_PLANT_FIELDS
from litoral_trace.us_lacey.specialized_projection import (
    LineMaterializationPlan,
    PlannedPlantLine,
    materialize_planned_plant_lines,
)


class _ScalarRows:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _FakeSession:
    def __init__(self, existing_lines):
        self.existing_lines = list(existing_lines)
        self.added = []
        self._next_id = 100

    def scalars(self, _statement):
        dynamic = [
            row for row in self.added if isinstance(row, UsLaceyPpqPlantLine)
        ]
        return _ScalarRows(self.existing_lines + dynamic)

    def add(self, row):
        if isinstance(row, UsLaceyPpqPlantLine) and row.id is None:
            row.id = self._next_id
            self._next_id += 1
        self.added.append(row)

    def flush(self):
        return None


def _plan() -> LineMaterializationPlan:
    generated = (
        PlannedPlantLine("SKU:PINE-001", "LT-PINE0000000000000001"),
        PlannedPlantLine("SKU:EUC-002", "LT-EUCA0000000000000002"),
    )
    return LineMaterializationPlan(
        line_references=("HUMAN-Z",) + tuple(item.line_reference for item in generated),
        generated_lines=generated,
        review_only_line_keys=(),
    )


def test_enforce_materializes_planned_lines_without_touching_existing_human_line() -> None:
    existing = SimpleNamespace(id=1, line_reference="HUMAN-Z", ordinal=1)
    session = _FakeSession([existing])
    operation = SimpleNamespace(id=77, merchandise_line_count=1)

    created = materialize_planned_plant_lines(
        session,
        organization_id=5,
        operation=operation,
        plan=_plan(),
    )

    assert created == ("LT-PINE0000000000000001", "LT-EUCA0000000000000002")
    assert operation.merchandise_line_count == 3
    lines = [row for row in session.added if isinstance(row, UsLaceyPpqPlantLine)]
    declarations = [
        row for row in session.added if isinstance(row, UsLaceyPlantDeclaration)
    ]
    fields = [row for row in session.added if isinstance(row, UsLaceyOperationField)]
    assert [(row.line_reference, row.ordinal) for row in lines] == [
        ("LT-PINE0000000000000001", 2),
        ("LT-EUCA0000000000000002", 3),
    ]
    assert len(declarations) == 2
    assert len(fields) == 2 * len(PPQ505_PLANT_FIELDS)
    assert {field.field_name for field in fields} == {
        contract.key for contract in PPQ505_PLANT_FIELDS
    }
    assert {field.field_status for field in fields} == {"MISSING"}
    assert {field.validation_status for field in fields} == {"MISSING"}
    assert existing.line_reference == "HUMAN-Z"
    assert existing.ordinal == 1


def test_materialization_is_idempotent_when_generated_lines_already_exist() -> None:
    existing = SimpleNamespace(id=1, line_reference="HUMAN-Z", ordinal=1)
    session = _FakeSession([existing])
    operation = SimpleNamespace(id=77, merchandise_line_count=1)
    plan = _plan()

    first = materialize_planned_plant_lines(
        session,
        organization_id=5,
        operation=operation,
        plan=plan,
    )
    added_after_first = len(session.added)
    second = materialize_planned_plant_lines(
        session,
        organization_id=5,
        operation=operation,
        plan=plan,
    )

    assert len(first) == 2
    assert second == ()
    assert len(session.added) == added_after_first
    assert operation.merchandise_line_count == 3
