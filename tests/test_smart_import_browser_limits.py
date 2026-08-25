from __future__ import annotations

from fastapi import status

from litoral_trace.services.batch import BATCH_MAX_SHEETS
from litoral_trace.services.smart_import import SMART_MAX_SHEETS, SmartImportError
from litoral_trace.web.batch_import import _smart_error_to_html, workspace_limits_view


def test_browser_exposes_smart_import_sheet_limit_not_strict_template_limit() -> None:
    limits = workspace_limits_view()

    assert BATCH_MAX_SHEETS == 4
    assert SMART_MAX_SHEETS == 20
    assert limits.max_sheets == SMART_MAX_SHEETS
    assert limits.max_sheets > BATCH_MAX_SHEETS


def test_smart_too_many_sheets_is_a_structural_422() -> None:
    translated = _smart_error_to_html(
        SmartImportError(
            code="SMART_TOO_MANY_SHEETS",
            detail="El workbook excede el límite de discovery.",
        )
    )

    assert translated.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert translated.code == "SMART_TOO_MANY_SHEETS"


def test_smart_sparse_source_range_is_a_structural_422() -> None:
    translated = _smart_error_to_html(
        SmartImportError(
            code="SMART_SOURCE_RANGE_TOO_LARGE",
            detail="La hoja declara un rango de filas demasiado amplio.",
        )
    )

    assert translated.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert translated.code == "SMART_SOURCE_RANGE_TOO_LARGE"
