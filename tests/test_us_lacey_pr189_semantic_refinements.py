from pathlib import Path
from types import SimpleNamespace

import pytest

from litoral_trace.us_lacey.projection import (
    _target_field,
    refresh_us_lacey_operation_status,
)


class _ScalarRows:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _ConditionalStatusSession:
    def __init__(self, fields):
        self.fields = fields
        self.scalars_calls = 0
        self.scalar_calls = 0

    def flush(self):
        return None

    def scalars(self, _statement):
        self.scalars_calls += 1
        if self.scalars_calls == 1:
            return _ScalarRows(self.fields)
        return _ScalarRows([])

    def scalar(self, _statement):
        self.scalar_calls += 1
        if self.scalar_calls == 1:
            return sum(
                field.field_status in {"MISSING", "REVIEW"}
                for field in self.fields
            )
        return 0


def _field(*, name, line="1", value=None, status="FOUND"):
    return SimpleNamespace(
        merchandise_line_reference=line,
        field_name=name,
        human_value=None,
        normalized_value=value,
        original_value=value,
        field_status=status,
        validation_status="VALID" if value is not None else "MISSING",
        validation_error=None,
        not_required_reason_code=None,
    )


def _extracted_row(value, *, field_name="product", locator="page:1"):
    return SimpleNamespace(
        field_name=field_name,
        normalized_value=None,
        original_value=value,
        source_locator=locator,
    )


def test_non_paper_percent_recycled_is_not_required_and_not_needs_attention():
    article = _field(name="article_component", value="Solid wood frame")
    recycled = _field(
        name="percent_recycled",
        value="0",
        status="REVIEW",
    )
    session = _ConditionalStatusSession([article, recycled])
    operation = SimpleNamespace(
        id=189,
        public_id="pr189-test",
        document_count=1,
        status="REVIEW_REQUIRED",
        review_result="NEEDS_HUMAN_REVIEW",
    )

    status = refresh_us_lacey_operation_status(
        session,
        organization_id=1,
        operation=operation,
    )

    assert recycled.field_status == "NOT_REQUIRED"
    assert recycled.not_required_reason_code == "NOT_PAPER_OR_PAPERBOARD"
    assert recycled.normalized_value == "0"  # provenance is retained
    assert recycled.original_value == "0"
    assert recycled.field_status not in {"MISSING", "REVIEW"}
    assert status == "READY_FOR_REVIEW"


@pytest.mark.parametrize(
    ("value", "locator"),
    [
        ("2,050 KG", "page:1;field:merchandise_description"),
        ("Plant component description: solid wood frame", "page:1"),
        ("Packaging description: corrugated carton", "page:1"),
        ("Supplier statement: species confirmed by supplier", "page:1"),
    ],
)
def test_non_merchandise_semantic_roles_do_not_enter_description_candidate_pool(value, locator):
    target, priority = _target_field(
        _extracted_row(value, locator=locator),
        table_headers=frozenset(),
    )

    assert target is None
    assert priority == 0


def test_legitimate_product_description_still_enters_candidate_pool():
    target, priority = _target_field(
        _extracted_row("Solid wood dining chair"),
        table_headers=frozenset(),
    )

    assert target == "merchandise_description"
    assert priority == 2


def test_file_input_native_chrome_is_hidden_and_controlled_in_english_for_print():
    root = Path(__file__).resolve().parents[1]
    js = (root / "src/litoral_trace/static/src/js/file-input.js").read_text(encoding="utf-8")
    css = (root / "src/litoral_trace/static/src/form-controls.css").read_text(encoding="utf-8")

    assert "Choose file" in js
    assert "No file selected" in js
    assert "Seleccionar archivo" not in js
    assert "Ningún archivo seleccionado" not in js
    assert '.lt-control[type="file"]' in css
    assert ".lt-file-input__native" in css
    assert "@media print" in css
    assert "display: none !important" in css
