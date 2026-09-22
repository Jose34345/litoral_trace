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
    assert 'id="entered-value-reconciliation-region"' in template
    assert 'id="final-confirmation"' in template


def test_pr192_individual_actions_do_not_autoscroll():
    source = Path("src/litoral_trace/static/src/js/us-lacey-workspace.js").read_text(
        encoding="utf-8"
    )

    assert 'form.hasAttribute("data-review-bulk")' in source
    assert 'if (!pendingBulkReviewTransition) return;' in source
    assert 'event.detail?.target?.id !== "review-field-list"' in source
    assert 'scrollIntoView({ behavior: "smooth", block: "nearest" })' in source
    assert "pendingReviewTransition" not in source
    assert "window.scrollTo" not in source
