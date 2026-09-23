from types import SimpleNamespace

from litoral_trace.db.models import (
    UsLaceyOperationField,
    UsLaceyPlantDeclaration,
    UsLaceyPpqPlantLine,
)
from litoral_trace.us_lacey.ppq505 import PPQ505_PLANT_FIELDS
from litoral_trace.us_lacey.projection import (
    _evaluate_merchandise_rows,
    _explicit_merchandise_rows,
    _line_reference,
    _materialize_applicable_plant_lines,
)
from litoral_trace.us_lacey.regulatory.applicability.domain import (
    DeclarationScope,
    PlantMaterialEvidence,
)


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
            self.existing_lines.append(row)
        self.added.append(row)

    def flush(self):
        return None


def _source(*, header: str, value: str, row: int, table: int = 1):
    return SimpleNamespace(
        field_name=f"raw.table.{table}.{header}",
        original_value=value,
        normalized_value=None,
        source_locator=f"table:{table};data_row:{row};column:1",
    )


def _headers(*names: str) -> dict[int, frozenset[str]]:
    return {1: frozenset(name.casefold() for name in names)}


def test_explicit_merchandise_rows_are_domain_facts_not_ppq_lines():
    sources = [
        _source(header="Line", value="1", row=1),
        _source(header="HTS Number", value="4407.99.0190", row=1),
        _source(header="Description", value="Sawn eucalyptus boards", row=1),
        _source(header="Entered Value", value="7200.00", row=1),
    ]
    rows = _explicit_merchandise_rows(
        sources,
        table_headers=_headers("Line", "HTS Number", "Description", "Entered Value"),
    )

    assert len(rows) == 1
    assert rows[0].hts10 == "4407990190"
    assert rows[0].description == "Sawn eucalyptus boards"
    assert rows[0].entered_value == "7200.00"
    assert rows[0].plant_material is PlantMaterialEvidence.UNKNOWN


def test_pack1_non_scheduled_hts_rows_materialize_zero_botanical_lines():
    sources = []
    values = (
        ("7613.00.0000", "Aluminum cylinder"),
        ("8424.89.0000", "Mechanical equipment"),
        ("8716.80.5070", "Industrial cart"),
    )
    for row_number, (hts, description) in enumerate(values, start=1):
        sources.extend(
            (
                _source(header="Line", value=str(row_number), row=row_number),
                _source(header="HS Code", value=hts, row=row_number),
                _source(header="Description", value=description, row=row_number),
                _source(header="Entered Value", value="100.00", row=row_number),
            )
        )

    merchandise = _explicit_merchandise_rows(
        sources,
        table_headers=_headers("Line", "HS Code", "Description", "Entered Value"),
    )
    evaluated = _evaluate_merchandise_rows(merchandise)

    assert [row.hts10 for row, _ in evaluated] == [
        "7613000000",
        "8424890000",
        "8716805070",
    ]
    assert all(
        decision.scope in {DeclarationScope.NOT_REQUIRED, DeclarationScope.REVIEW_REQUIRED}
        and decision.requires_botanical_fields is False
        for _, decision in evaluated
    )

    session = _FakeSession()
    operation = SimpleNamespace(id=77, merchandise_line_count=0)
    row_map = _materialize_applicable_plant_lines(
        session,
        organization_id=5,
        operation=operation,
        evaluated_rows=evaluated,
    )

    assert row_map == {}
    assert operation.merchandise_line_count == 0
    assert not [row for row in session.added if isinstance(row, UsLaceyPpqPlantLine)]
    assert not [row for row in session.added if isinstance(row, UsLaceyOperationField)]


def test_scheduled_hts_unknown_material_requires_review_but_not_botanical_fields():
    sources = [
        _source(header="HTS Number", value="4407.99.0190", row=1),
        _source(header="Description", value="Unspecified merchandise", row=1),
        _source(header="Entered Value", value="100.00", row=1),
    ]
    merchandise = _explicit_merchandise_rows(
        sources,
        table_headers=_headers("HTS Number", "Description", "Entered Value"),
    )
    evaluated = _evaluate_merchandise_rows(merchandise)

    assert evaluated[0][1].scope is DeclarationScope.REVIEW_REQUIRED
    assert evaluated[0][1].reason_codes == ("PLANT_MATERIAL_NOT_ESTABLISHED",)
    assert evaluated[0][1].requires_botanical_fields is False

    session = _FakeSession()
    operation = SimpleNamespace(id=77, merchandise_line_count=0)
    assert _materialize_applicable_plant_lines(
        session,
        organization_id=5,
        operation=operation,
        evaluated_rows=evaluated,
    ) == {}
    assert operation.merchandise_line_count == 0


def test_scheduled_hts_with_explicit_botanical_evidence_materializes_one_line():
    sources = [
        _source(header="HTS Number", value="4407.99.0190", row=1),
        _source(header="Description", value="Sawn wood", row=1),
        _source(header="Entered Value", value="7200", row=1),
        _source(header="Genus", value="Eucalyptus", row=1),
        _source(header="Species", value="grandis", row=1),
    ]
    merchandise = _explicit_merchandise_rows(
        sources,
        table_headers=_headers(
            "HTS Number", "Description", "Entered Value", "Genus", "Species"
        ),
    )
    evaluated = _evaluate_merchandise_rows(merchandise)
    assert evaluated[0][0].plant_material is PlantMaterialEvidence.PRESENT
    assert evaluated[0][1].scope is DeclarationScope.POTENTIALLY_REQUIRED
    assert evaluated[0][1].requires_botanical_fields is True

    session = _FakeSession()
    operation = SimpleNamespace(id=77, merchandise_line_count=0)
    row_map = _materialize_applicable_plant_lines(
        session,
        organization_id=5,
        operation=operation,
        evaluated_rows=evaluated,
    )

    assert row_map == {(1, 1): "1"}
    assert operation.merchandise_line_count == 1
    new_lines = [row for row in session.added if isinstance(row, UsLaceyPpqPlantLine)]
    declarations = [row for row in session.added if isinstance(row, UsLaceyPlantDeclaration)]
    fields = [row for row in session.added if isinstance(row, UsLaceyOperationField)]
    assert len(new_lines) == 1
    assert len(declarations) == 1
    assert len(fields) == len(PPQ505_PLANT_FIELDS)
    assert {field.field_name for field in fields} == {
        contract.key for contract in PPQ505_PLANT_FIELDS
    }


def test_applicability_row_map_prevents_non_applicable_row_evidence_leakage():
    assert _line_reference(
        target="genus",
        source_locator="table:1;data_row:2;column:4",
        source_field_name="raw.table.1.Genus",
        line_references=("1",),
        row_line_references={(1, 1): "1"},
    ) == ""
    assert _line_reference(
        target="genus",
        source_locator="table:1;data_row:1;column:4",
        source_field_name="raw.table.1.Genus",
        line_references=("1",),
        row_line_references={(1, 1): "1"},
    ) == "1"
