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


def test_bulk_rejection_does_not_mix_request_feedback_with_stale_analysis_ui():
    source = TEMPLATE.read_text(encoding="utf-8")

    assert '{% set empty_bulk_rejection = bulk_upload_rejection and not detail.documents %}' in source
    assert '("Documents", "complete" if detail.documents else "current")' in source
    assert '{% set processing_step_state = "pending" if empty_bulk_rejection else ("current" if processing.failed or not processing.terminal else "complete") %}' in source
    assert '("Processing", processing_step_state)' in source

    # On the response to a rejected multi-shipment upload, do not show an old
    # Engine 2 placeholder or a processing failure that belongs to a previously
    # stored document. The operation history remains untouched and is visible on
    # the normal operation GET after the customer leaves this request-local state.
    assert source.count('{% if not bulk_upload_rejection %}') >= 2
    assert 'id="engine2-dossier"' in source
    assert 'id="processing-panel"' in source


def test_failed_processing_is_not_marked_complete_in_progress_steps():
    source = TEMPLATE.read_text(encoding="utf-8")

    assert '"current" if processing.failed or not processing.terminal else "complete"' in source


def test_generic_errors_remain_distinct_from_expected_bulk_guidance():
    source = TEMPLATE.read_text(encoding="utf-8")

    assert '{% elif error %}' in source
    assert '{{ alert("We could not complete that action", error, "danger") }}' in source
    assert '{{ alert("Update complete", notice, "positive") }}' in source
