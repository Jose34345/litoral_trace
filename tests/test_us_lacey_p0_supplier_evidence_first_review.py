"""Keep customer-facing supplier evidence semantics and first pending navigation."""
from pathlib import Path
from jinja2 import Environment

ROOT = Path(__file__).resolve().parents[1]
BASE = (ROOT / "src/litoral_trace/templates/us_lacey/base.html").read_text("utf-8")
EVIDENCE = (ROOT / "src/litoral_trace/templates/us_lacey/evidence.html").read_text("utf-8")
OPERATION = (ROOT / "src/litoral_trace/templates/us_lacey/operation_detail.html").read_text("utf-8")
SUMMARY = (ROOT / "src/litoral_trace/templates/us_lacey/fragments/shipment_workspace_summary.html").read_text("utf-8")
JS = (ROOT / "src/litoral_trace/static/src/js/us-lacey-workspace.js").read_text("utf-8")


def test_navigation_does_not_claim_general_document_library():
    assert ">Supplier Evidence</span>" in BASE
    assert "label: 'Supplier Evidence'" in BASE
    assert 'href="/evidence"' in BASE
    assert "<h1 class=\"lt-page-header__title\">Supplier Evidence</h1>" in EVIDENCE
    assert "Reusable supplier evidence" in EVIDENCE
    assert "Assurance V2 Lab" not in BASE


def test_review_cta_deep_links_actual_first_pending_row():
    assert 'shipment_workspace_summary.html' in OPERATION
    assert 'href="#review-summary"' in SUMMARY
    assert "data-jump-first-review" in SUMMARY
    assert 'firstReview = event.target.closest?.("[data-jump-first-review]")' in JS
    assert "pendingReviewCards()[0]" in JS
    assert 'window.history.replaceState(null, "", "#" + firstCard.id)' in JS
    assert 'firstCard.scrollIntoView({' in JS
    assert '"11.5rem"' in JS
    assert 'Review the remaining evidence below' not in OPERATION
    Environment().parse(BASE)
    Environment().parse(EVIDENCE)
    Environment().parse(OPERATION)
    Environment().parse(SUMMARY)
