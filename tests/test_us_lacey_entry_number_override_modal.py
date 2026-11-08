"""Customer-facing review modal contract for missing pre-entry CBP number."""
from __future__ import annotations

from litoral_trace.us_lacey._operations_core import OperationFieldView
from litoral_trace.web import us_lacey_operational_views as views
from tests.test_us_lacey_regulatory_assessment_ui import _detail, _request, _engine2


def _render_entry(monkeypatch, *, error=None):
    monkeypatch.setattr(views, "_semantic_evidence_for_detail", lambda *_: {})
    monkeypatch.setattr(views, "_product_intelligence_for_detail", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(views, "_regulatory_assessment_for_detail", lambda *_args, **_kwargs: None)
    detail = _detail()
    detail.fields = (OperationFieldView(
        id=101,
        line_reference="__shipment__",
        field_name="filing_entry_reference",
        label="Entry Number",
        ppq_number=2,
        scope="SHIPMENT",
        proposed_value=None,
        effective_value=None,
        status="REVIEW",
        confidence=0.0,
        source_assurance_document_id=None,
        source_page=None,
        source_locator=None,
        extractor=None,
        extractor_version=None,
        reviewed_by_user_id=None,
        reviewed_at=None,
        validation_status="REVIEW_REQUIRED",
        validation_error=None,
        not_required_reason_code=None,
        candidates=(),
    ),)
    return views.render_operation_workspace(
        request=_request(),
        identity=type("Identity", (), {"organization_id": 7})(),
        detail=detail,
        engine2_dossier=_engine2(),
        complete_csrf="complete",
        review_csrf={101: "field-csrf-value"},
        field_errors={101: error} if error else {},
        field_input_values={101: "bad value"} if error else {},
    )


def test_entry_number_modal_is_native_top_layer_and_keep_pending_is_not_a_post(monkeypatch):
    html = _render_entry(monkeypatch)
    assert 'data-review-field-name="filing_entry_reference"' in html
    assert 'data-review-keep-pending' in html
    assert 'data-keep-pending-field="101"' in html
    assert 'aria-controls="review-override-101"' in html
    assert 'id="review-override-101"' in html
    assert 'data-override-dialog' in html
    assert 'aria-labelledby="review-override-title-101"' in html
    assert "Keep pending" in html
    assert "Not yet issued by CBP?" in html
    assert 'hx-post="/operations/' in html
    assert 'name="csrf_token" value="field-csrf-value"' in html
    assert 'name="action" value="edit"' in html
    assert "No value was saved. This field remains pending." in html
    # Only the verified-value submit is a POST; Keep pending must be a local,
    # side-effect-free cancel rather than an unauthorized NOT_REQUIRED decision.
    assert '<button type="button" class="lt-btn lt-btn--ghost" data-review-keep-pending' in html
    assert 'data-override-keep-pending' in html
    assert '<details class="relative">' not in html
    assert "absolute right-0 z-20 mt-2 w-80" not in html
    assert 'data-override-has-error="false"' in html


def test_failed_verified_entry_review_reopens_modal_with_error_and_retained_input(monkeypatch):
    html = _render_entry(monkeypatch, error="A verified CBP entry number is required.")
    assert 'data-override-has-error="true"' in html
    assert 'data-inline-field-error' in html
    assert 'aria-invalid="true"' in html
    assert 'aria-describedby="field-error-101"' in html
    assert "A verified CBP entry number is required." in html
    assert 'value="bad value"' in html
    assert 'root.querySelectorAll(\'[data-override-has-error="true"]\').forEach(openOverride)' in html
