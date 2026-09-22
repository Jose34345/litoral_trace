from pathlib import Path


def test_exception_first_workspace_uses_one_authoritative_outer_html_swap():
    template = Path(
        "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html"
    ).read_text(encoding="utf-8")

    assert 'id="operation-workspace"' in template
    assert 'hx-target="#operation-workspace"' in template
    assert 'hx-swap="outerHTML"' in template
    assert 'hx-swap-oob="true"' not in template
    assert 'id="review-summary"' in template
    assert 'data-action-required-list' in template
    assert 'data-review-tab-panel="resolved"' in template
    assert 'data-review-tab-panel="regulatory"' in template
    assert '{% include "us_lacey/fragments/export_declaration_package.html" %}' in template


def test_pr192_individual_actions_do_not_autoscroll():
    source = Path("src/litoral_trace/static/src/js/us-lacey-workspace.js").read_text(
        encoding="utf-8"
    )

    assert 'form.hasAttribute("data-review-bulk")' in source
    assert 'if (!pendingBulkReviewTransition) return;' in source
    assert 'event.detail?.target?.id !== "operation-workspace"' in source
    assert 'scrollIntoView({ behavior: "smooth", block: "nearest" })' in source
    assert "pendingReviewTransition" not in source
    assert "window.scrollTo" not in source
