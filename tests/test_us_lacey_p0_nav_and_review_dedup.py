"""Customer navigation and first-review P0 regression contracts."""
from pathlib import Path
from jinja2 import Environment

ROOT = Path(__file__).resolve().parents[1]
BASE = (ROOT / "src/litoral_trace/templates/us_lacey/base.html").read_text(encoding="utf-8")
DETAIL = (ROOT / "src/litoral_trace/templates/us_lacey/operation_detail.html").read_text(encoding="utf-8")
SUMMARY = (ROOT / "src/litoral_trace/templates/us_lacey/fragments/shipment_workspace_summary.html").read_text(encoding="utf-8")
REGULATORY = (ROOT / "src/litoral_trace/templates/us_lacey/fragments/shipment_package_regulatory.html").read_text(encoding="utf-8")
REVIEW = (ROOT / "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html").read_text(encoding="utf-8")


def test_sidebar_uses_trader_names_and_preserves_destination_paths():
    for href, label in (
        ("/operations", "Shipments"),
        ("/evidence", "Supplier Evidence"),
        ("/suppliers", "Suppliers"),
        ("/audit-log", "Activity Log"),
        ("/regulatory", "Lacey Review"),
        ("/trust", "Data &amp; Security"),
        ("/legal/product-use-terms", "Terms"),
    ):
        assert f'href="{href}"' in BASE
        assert f'>{label}</span>' in BASE
    for old in (">Evidence</span>", ">Audit Log</span>",
                ">Regulatory Analysis</span>", ">Trust &amp; Controls</span>"):
        assert old not in BASE
    for internal in ("Test only", ">Source</span>", ">Memory</span>",
                     ">Enabled</span>", ">Rules v4</span>",
                     ">Active</span>", ">Queue</span>"):
        assert internal not in BASE


def test_lab_entry_is_not_advertised_but_authority_guards_remain():
    assert 'href="/assurance-v2"' not in BASE
    assert "Assurance V2 Lab" not in BASE
    assert "data-private-v2-boundary" in DETAIL
    assert "Filing package issuance is disabled" in DETAIL


def test_one_review_summary_and_one_navigation_cta_when_pending():
    assert 'shipment_workspace_summary.html' in DETAIL
    assert 'id="shipment-summary"' in SUMMARY
    assert 'data-first-value-cta' in SUMMARY
    assert "readiness_summary.exception_count" in SUMMARY
    assert "No automatic ACE / LAWGS submission" in SUMMARY
    assert "Review decisions" not in REVIEW
    assert "Go to first issue" not in REVIEW
    assert 'id="review-summary"' in REVIEW
    assert 'data-review-tab-button="action"' in REVIEW
    assert 'data-review-kpi="documents"' not in REVIEW
    assert "data-review-next-controls" in REVIEW
    assert '{% if combined_action_count == 0 %}' in REVIEW


def test_zero_pending_confirmation_and_source_controls_remain_operational():
    assert 'hx-target="#operation-workspace"' in REVIEW
    assert 'data-review-bulk' in REVIEW
    assert 'data-review-completion-form' in REVIEW
    assert 'data-review-tab-panel="regulatory"' in REGULATORY
    assert "data-action-required-list" in REVIEW
    assert "Request original evidence" in REVIEW
    Environment().parse(BASE)
    Environment().parse(DETAIL)
    Environment().parse(REVIEW)
