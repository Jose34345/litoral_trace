from types import SimpleNamespace

from litoral_trace.db.models import (
    UsLaceyOperationField,
    UsLaceyPlantDeclaration,
    UsLaceyPpqPlantLine,
)
from litoral_trace.us_lacey.ppq505 import PPQ505_PLANT_FIELDS
from litoral_trace.us_lacey.projection import (
    _explicit_merchandise_rows,
    _line_reference,
    _materialize_applicable_plant_lines,
)
from litoral_trace.us_lacey.regulatory.applicability import PlantMaterialEvidence


class _ScalarRows:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _FakeSession:
    def __init__(self, existing_lines=()):
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


def _source(*, header: str, value: str, row: int, table: int = 1, source_id: int = 1):
    return SimpleNamespace(
        id=source_id,
        field_name=f"raw.table.{table}.{header}",
        original_value=value,
        normalized_value=None,
        source_locator=f"table:{table};data_row:{row};column:1",
    )


def _merchandise_headers() -> dict[int, frozenset[str]]:
    return {
        1: frozenset(
            {
                "line",
                "hts number",
                "description",
                "qty",
                "unit value",
                "entered value",
            }
        )
    }


def test_merchandise_extraction_is_pure_and_does_not_materialize_ppq_lines():
    sources = [
        _source(header="Line", value="1", row=1, source_id=1),
        _source(header="HTS Number", value="7613.00.0000", row=1, source_id=2),
        _source(header="Description", value="Cryogenic cylinder", row=1, source_id=3),
        _source(header="Entered Value", value="10000.00", row=1, source_id=4),
        _source(header="Line", value="2", row=2, source_id=5),
        _source(header="HTS Number", value="8424.89.0000", row=2, source_id=6),
        _source(header="Description", value="Industrial spray equipment", row=2, source_id=7),
        _source(header="Entered Value", value="9000.00", row=2, source_id=8),
        _source(header="Line", value="3", row=3, source_id=9),
        _source(header="HTS Number", value="8716.80.5070", row=3, source_id=10),
        _source(header="Description", value="Industrial cart", row=3, source_id=11),
        _source(header="Entered Value", value="12110.21", row=3, source_id=12),
    ]

    rows = _explicit_merchandise_rows(
        sources,
        table_headers=_merchandise_headers(),
    )

    assert [(row.line_key, row.hts10) for row in rows] == [
        ("1", "7613000000"),
        ("2", "8424890000"),
        ("3", "8716805070"),
    ]
    assert all(row.plant_material is PlantMaterialEvidence.UNKNOWN for row in rows)


def test_materializer_creates_only_applicability_approved_rows():
    session = _FakeSession()
    operation = SimpleNamespace(id=77, merchandise_line_count=0)

    line_references = _materialize_applicable_plant_lines(
        session,
        organization_id=5,
        operation=operation,
        applicable_line_keys=("3",),
    )

    assert line_references == ("3",)
    assert operation.merchandise_line_count == 1

    new_lines = [row for row in session.added if isinstance(row, UsLaceyPpqPlantLine)]
    declarations = [row for row in session.added if isinstance(row, UsLaceyPlantDeclaration)]
    fields = [row for row in session.added if isinstance(row, UsLaceyOperationField)]
    assert len(new_lines) == 1
    assert new_lines[0].line_reference == "3"
    assert len(declarations) == 1
    assert len(fields) == len(PPQ505_PLANT_FIELDS)
    assert {field.field_name for field in fields} == {
        contract.key for contract in PPQ505_PLANT_FIELDS
    }


def test_materializer_with_no_applicable_rows_creates_zero_botanical_state():
    session = _FakeSession()
    operation = SimpleNamespace(id=77, merchandise_line_count=0)

    line_references = _materialize_applicable_plant_lines(
        session,
        organization_id=5,
        operation=operation,
        applicable_line_keys=(),
    )

    assert line_references == ()
    assert operation.merchandise_line_count == 0
    assert session.added == []


def test_sparse_lazy_line_reference_never_leaks_row_one_into_row_three():
    line_references = ("3",)

    assert _line_reference(
        target="genus",
        source_locator="table:1;data_row:1;column:4",
        line_references=line_references,
    ) == ""
    assert _line_reference(
        target="genus",
        source_locator="table:1;data_row:3;column:4",
        line_references=line_references,
    ) == "3"


def test_explicit_plant_material_flag_is_used_without_description_guessing():
    headers = {
        1: frozenset(
            {
                "line",
                "hts number",
                "description",
                "entered value",
                "plant material",
            }
        )
    }
    rows = _explicit_merchandise_rows(
        [
            _source(header="Line", value="1", row=1, source_id=1),
            _source(header="HTS Number", value="4407.99.0190", row=1, source_id=2),
            _source(header="Description", value="Product", row=1, source_id=3),
            _source(header="Entered Value", value="100", row=1, source_id=4),
            _source(header="Plant Material", value="Yes", row=1, source_id=5),
        ],
        table_headers=headers,
    )

    assert len(rows) == 1
    assert rows[0].plant_material is PlantMaterialEvidence.PRESENT
