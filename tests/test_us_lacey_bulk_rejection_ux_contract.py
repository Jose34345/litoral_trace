from __future__ import annotations

from pathlib import Path

from jinja2 import Environment


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "src" / "litoral_trace" / "templates" / "us_lacey" / "operation_detail.html"


def test_bulk_rejection_is_customer_guidance_not_internal_error_ui():
    source = TEMPLATE.read_text(encoding="utf-8")
    Environment().parse(source)

    assert 'error.startswith("This file contains multiple shipments.")' in source
    assert 'data-bulk-upload-rejection' in source
    assert '>This file contains multiple shipments</p>' in source
    assert (
        "Litoral Trace processes one shipment per operation. Split this file so it contains only one shipment, "
        "then upload that file to this operation."
    ) in source
    assert "Nothing was added to this operation." in source

    lowered = source.lower()
    assert "bulk benchmark" not in lowered
    assert "benchmark importer" not in lowered
    assert "try again" not in lowered
    assert "contact support" not in lowered


def test_empty_bulk_rejection_keeps_documents_current_and_processing_pending():
    source = TEMPLATE.read_text(encoding="utf-8")

    assert '{% set empty_bulk_rejection = bulk_upload_rejection and not detail.documents %}' in source
    assert '("Documents", "complete" if detail.documents else "current")' in source
    assert '{% set processing_step_state = "pending" if empty_bulk_rejection else' in source
    assert '("Processing", processing_step_state)' in source

    # A rejected first upload must not imply analysis started or poll a job that
    # was never queued. Existing analysis remains visible when documents already
    # belong to the operation.
    assert source.count('{% if not empty_bulk_rejection %}') >= 2
    assert 'id="engine2-dossier"' in source
    assert 'id="processing-panel"' in source


def test_generic_errors_remain_distinct_from_expected_bulk_guidance():
    source = TEMPLATE.read_text(encoding="utf-8")

    assert '{% elif error %}' in source
    assert '{{ alert("We could not complete that action", error, "danger") }}' in source
    assert '{{ alert("Update complete", notice, "positive") }}' in source
