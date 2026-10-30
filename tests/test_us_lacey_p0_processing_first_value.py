"""Unready extraction never looks like actionable review in the real summary."""
from pathlib import Path
from types import SimpleNamespace
import re

import pytest
from jinja2 import Environment, FileSystemLoader, select_autoescape

ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src/litoral_trace/templates"
TPL = (TEMPLATES / "us_lacey/operation_detail.html").read_text(encoding="utf-8")
SUMMARY = (TEMPLATES / "us_lacey/fragments/shipment_workspace_summary.html").read_text(encoding="utf-8")
ENV = Environment(loader=FileSystemLoader(str(TEMPLATES)), autoescape=select_autoescape(("html",)))


def _render_summary(*, documents=True, terminal=True, failed=False, private=False,
                    pending=9, supported=25, confirmed=3, reused_claim_count=0,
                    overall="NOT READY", package_ready=False):
    return ENV.get_template("us_lacey/fragments/shipment_workspace_summary.html").render(
        detail=SimpleNamespace(documents=(object(),) if documents else (), status="READY_FOR_REVIEW"),
        processing=SimpleNamespace(terminal=terminal, failed=failed),
        private_v2=private,
        readiness_summary=SimpleNamespace(overall=overall, exception_count=pending,
                                          package_ready=package_ready, species="MISSING", country_of_harvest="MISSING"),
        auto_supported_fields=(object(),) * supported,
        settled_fields=(object(),) * confirmed,
        reused_evidence_summary=SimpleNamespace(claim_count=reused_claim_count) if reused_claim_count else None,
    )


def test_processing_overview_is_accurate_and_not_actionable():
    processing = _render_summary(terminal=False, reused_claim_count=3)
    assert "Analyzing shipment documents" in processing
    assert "Processing in progress" in processing
    assert "data-first-value-wait" in processing
    assert "data-first-value-processing" in processing
    assert "data-first-value-cta" not in processing
    assert "data-workspace-count" not in processing
    assert "verified claims reused" not in processing
    assert "No automatic ACE / LAWGS submission" in processing

    failed = _render_summary(failed=True, reused_claim_count=3)
    assert "Document analysis needs attention" in failed
    assert "data-first-value-issue" in failed
    assert 'href="#processing-status"' in failed
    assert "data-first-value-cta" not in failed
    assert "data-workspace-count" not in failed
    assert "verified claims reused" not in failed
    assert "No automatic ACE / LAWGS submission" in failed


@pytest.mark.parametrize("terminal,failed", [(False, False), (True, True), (True, False)])
def test_navigation_never_claims_pending_action_count_during_processing(terminal, failed):
    nav = re.search(r"<nav[^>]*data-shipment-section-nav.*?</nav>", TPL, re.S).group()
    rendered = ENV.from_string(nav).render(
        private_v2=False, processing=SimpleNamespace(terminal=terminal, failed=failed),
        readiness_summary=SimpleNamespace(exception_count=9),
    )
    assert re.findall(r'href="([^"]+)"', rendered) == [
        "#operation-workspace", "#documents", "#declaration-package",
    ]
    assert "9" not in rendered
    assert "items" not in rendered


def test_review_only_after_source_set_is_terminal_and_valid():
    assert "processing.terminal and not processing.failed" in TPL
    assert 'shipment_workspace_summary.html' in TPL
    ready = _render_summary(reused_claim_count=3)
    assert "data-first-value-actions" in ready
    assert "data-first-value-cta" in ready
    assert 'href="#review-summary"' in ready
    assert 'data-workspace-count="pending">9<' in ready
    assert "3 verified claims reused" in ready
    exported = _render_summary(overall="READY FOR EXPORT", package_ready=True, pending=0)
    assert "READY FOR EXPORT" in exported
    assert 'data-workspace-count="pending">0<' in exported
    private = _render_summary(private=True, reused_claim_count=3,
                              overall="READY FOR EXPORT", package_ready=True)
    assert "Filing disabled" in private
    assert "READY FOR EXPORT" not in private
    assert "data-workspace-count" not in private
    assert "verified claims reused" not in private
    assert "No automatic ACE / LAWGS submission" in private
    ENV.parse(TPL)
    ENV.parse(SUMMARY)


def test_empty_shipment_guides_upload_instead_of_analyzing_nonexistent_files():
    summary = _render_summary(documents=False, terminal=False)
    assert "Add shipment documents to begin" in summary
    assert "Analyzing shipment documents" not in summary
    assert "data-first-value-empty" in summary
    assert "data-first-value-upload" in summary
    assert 'href="#documents"' in summary
    assert "data-first-value-cta" not in summary
    assert "data-workspace-count" not in summary
