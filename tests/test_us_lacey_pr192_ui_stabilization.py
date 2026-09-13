from pathlib import Path


def test_pr192_initial_workspace_regions_are_not_unconditionally_oob():
    template = Path(
        "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html"
    ).read_text(encoding="utf-8")

    conditional = '{% if is_oob_update %} hx-swap-oob="true"{% endif %}'
    assert template.count(conditional) >= 4
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
