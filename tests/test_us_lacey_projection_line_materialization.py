from types import SimpleNamespace

from litoral_trace.db.models import (
    UsLaceyOperationField,
    UsLaceyPlantDeclaration,
    UsLaceyPpqPlantLine,
)
from litoral_trace.us_lacey.ppq505 import PPQ505_PLANT_FIELDS
from litoral_trace.us_lacey.projection import (
    _explicit_plant_data_rows,
    _line_reference,
    _materialize_explicit_plant_lines,
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
        return _ScalarRows(self.existing_lines)

    def add(self, row):
        if isinstance(row, UsLaceyPpqPlantLine) and row.id is None:
            row.id = self._next_id
            self._next_id += 1
        self.added.append(row)

    def flush(self):
        return None


def _source(*, header: str, value: str, row: int | None):
    locator = "table:1"
    if row is not None:
        locator += f";data_row:{row};column:1"
    return SimpleNamespace(
        field_name=f"raw.table.1.{header}",
        original_value=value,
        normalized_value=None,
        source_locator=locator,
    )


def test_explicit_plant_rows_ignore_entered_value_without_plant_identity():
    sources = [
        _source(header="Entered Value", value="18600", row=2),
        _source(header="Article Component", value="Solid rubberwood coasters", row=1),
    ]

    assert _explicit_plant_data_rows(sources, table_headers=frozenset()) == (1,)


def test_projection_materializes_second_explicit_plant_component_before_mapping():
    existing = SimpleNamespace(id=1, line_reference="1", ordinal=1)
    session = _FakeSession([existing])
    operation = SimpleNamespace(id=77, merchandise_line_count=1)
    sources = [
        _source(header="Article Component", value="Solid rubberwood coasters", row=1),
        _source(header="Genus", value="Hevea", row=1),
        _source(header="Species", value="brasiliensis", row=1),
        _source(header="Country of Harvest", value="Thailand", row=1),
        _source(header="Plant Quantity", value="1440", row=1),
        _source(header="Metric Unit", value="KG", row=1),
        _source(header="Article Component", value="MDF holder", row=2),
        _source(header="Genus", value="SPECIAL", row=2),
        _source(header="Species", value="COMPOSITE", row=2),
        _source(header="Country of Harvest", value="Malaysia", row=2),
        _source(header="Plant Quantity", value="360", row=2),
        _source(header="Metric Unit", value="KG", row=2),
    ]

    line_references = _materialize_explicit_plant_lines(
        session,
        organization_id=5,
        operation=operation,
        extracted=sources,
        table_headers=frozenset(),
    )

    assert line_references == ("1", "2")
    assert operation.merchandise_line_count == 2

    new_lines = [row for row in session.added if isinstance(row, UsLaceyPpqPlantLine)]
    declarations = [row for row in session.added if isinstance(row, UsLaceyPlantDeclaration)]
    fields = [row for row in session.added if isinstance(row, UsLaceyOperationField)]
    assert len(new_lines) == 1
    assert new_lines[0].line_reference == "2"
    assert new_lines[0].ordinal == 2
    assert len(declarations) == 1
    assert len(fields) == len(PPQ505_PLANT_FIELDS)
    assert {field.merchandise_line_reference for field in fields} == {"2"}
    assert {field.field_name for field in fields} == {
        contract.key for contract in PPQ505_PLANT_FIELDS
    }

    assert _line_reference(
        target="country_of_harvest",
        source_locator="table:1;data_row:1;column:4",
        line_references=line_references,
    ) == "1"
    assert _line_reference(
        target="country_of_harvest",
        source_locator="table:1;data_row:2;column:4",
        line_references=line_references,
    ) == "2"
    assert _line_reference(
        target="entered_value",
        source_locator="page:2;label:Total Entered Value",
        line_references=line_references,
    ) == ""


def test_materialization_refuses_nonconsecutive_row_jump():
    existing = SimpleNamespace(id=1, line_reference="1", ordinal=1)
    session = _FakeSession([existing])
    operation = SimpleNamespace(id=77, merchandise_line_count=1)
    sources = [
        _source(header="Article Component", value="Unexpected distant row", row=20),
    ]

    line_references = _materialize_explicit_plant_lines(
        session,
        organization_id=5,
        operation=operation,
        extracted=sources,
        table_headers=frozenset(),
    )

    assert line_references == ("1",)
    assert operation.merchandise_line_count == 1
    assert not session.added