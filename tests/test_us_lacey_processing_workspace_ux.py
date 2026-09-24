from types import SimpleNamespace

from litoral_trace.web.templates import templates
from litoral_trace.web.us_lacey_operational_views import processing_view


def _detail(*, status="OPEN", documents=()):
    return SimpleNamespace(status=status, documents=documents)


def _document(*, job_status=None, processing_status="STORED", last_error_code=None):
    return SimpleNamespace(
        job_status=job_status,
        processing_status=processing_status,
        last_error_code=last_error_code,
    )


def test_processing_progress_uses_durable_job_checkpoints_not_elapsed_time():
    assert processing_view(_detail(documents=(_document(job_status="QUEUED"),))).percent == 35
    assert processing_view(_detail(documents=(_document(job_status="RUNNING"),))).percent == 60


def test_processing_failure_is_terminal_and_customer_safe():
    progress = processing_view(_detail(documents=(_document(job_status="FAILED"),)))
    assert (progress.percent, progress.terminal, progress.failed) == (100, True, True)
    assert "traceback" not in progress.message.casefold()


def test_completed_operation_is_terminal_without_waiting_for_optional_shadow_work():
    progress = processing_view(_detail(status="COMPLETED", documents=(_document(),)))
    assert (progress.percent, progress.state, progress.terminal) == (100, "COMPLETED", True)



def test_processing_failure_retry_uses_htmx_and_support_mailto():
    operation_id = "11111111-2222-3333-4444-555555555555"
    html = templates.get_template(
        "us_lacey/fragments/processing_fragment_body.html"
    ).render(
        detail=SimpleNamespace(public_id=operation_id),
        processing=SimpleNamespace(failed=True, state="FAILED"),
        retry_csrf="retry-csrf-token",
    )

    assert (
        f'hx-post="/operations/{operation_id}/actions/retry"'
        in html
    )
    assert 'hx-include="closest form"' in html
    assert 'hx-disabled-elt="this"' in html
    assert 'name="csrf_token" value="retry-csrf-token"' in html
    assert "Retrying..." in html
    assert (
        f'href="mailto:support@litoraltrace.com?subject=Analysis Error - Operation {operation_id}"'
        in html
    )
    assert "comercial@litoraltrace.com" not in html



def test_unsupported_domain_is_terminal_and_does_not_render_review_retry():
    operation_id = "11111111-2222-3333-4444-555555555555"
    progress = processing_view(
        _detail(
            status="FAILED",
            documents=(
                _document(
                    job_status="FAILED",
                    processing_status="FAILED",
                    last_error_code="UNSUPPORTED_DOMAIN",
                ),
            ),
        )
    )

    assert progress.state == "UNSUPPORTED_DOMAIN"
    assert progress.terminal is True
    assert progress.failed is True
    assert "legal or administrative files" in progress.message

    html = templates.get_template(
        "us_lacey/fragments/processing_fragment_body.html"
    ).render(
        detail=SimpleNamespace(public_id=operation_id),
        processing=progress,
        retry_csrf="retry-token",
    )
    assert 'data-unsupported-document-domain' in html
    assert "Unsupported document type" in html
    assert "legal or administrative files" in html
    assert "/actions/retry" not in html



def _action_field_html(*, proposed_value):
    template = templates.get_template(
        "us_lacey/fragments/operation_workspace.html"
    )
    field = SimpleNamespace(
        id=17,
        status="MISSING",
        scope="SHIPMENT",
        line_reference="__SHIPMENT__",
        field_name="container_number",
        label="Container Number(s)",
        proposed_value=proposed_value,
        effective_value=proposed_value,
        candidates=(),
    )
    module = template.make_module(
        {
            "detail": SimpleNamespace(
                public_id="11111111-2222-3333-4444-555555555555"
            ),
            "review_csrf": {17: "csrf"},
        }
    )
    return str(module.action_field(field, 1))


def test_action_required_badge_says_needs_confirmation_when_value_exists():
    html = _action_field_html(proposed_value="TGHU5519023")

    assert 'data-action-status>Needs confirmation</span>' in html
    assert "Best available value:" in html
    assert "TGHU5519023" in html
    assert 'data-action-status>Missing information</span>' not in html


def test_action_required_badge_says_missing_only_when_value_is_absent():
    html = _action_field_html(proposed_value=None)

    assert 'data-action-status>Missing information</span>' in html
    assert "Not found in the uploaded evidence." in html
    assert "Best available value:" not in html
