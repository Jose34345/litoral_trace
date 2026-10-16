"""During multi-PDF processing, unready extraction must not look like actionable review."""
from pathlib import Path
from jinja2 import Environment

ROOT = Path(__file__).resolve().parents[1]
TPL = (ROOT / "src/litoral_trace/templates/us_lacey/operation_detail.html").read_text(encoding="utf-8")


def test_processing_overview_is_accurate_and_not_actionable():
    first = TPL[TPL.index('<section id="shipment-summary"'):TPL.index("</section>", TPL.index('<section id="shipment-summary"'))]
    assert '{% elif not processing.terminal %}' in first
    assert 'Analyzing shipment documents' in first
    assert 'Processing in progress' in first
    assert 'data-first-value-wait' in first
    assert 'Preparing the review queue' in first
    assert '{% elif processing.failed %}' in first
    assert 'data-first-value-issue' in first
    assert 'NEEDS ATTENTION' in first
    assert 'data-first-value-cta' in first
    assert 'processing.terminal and not processing.failed and reused_evidence_summary' in first
    assert 'No automatic ACE / LAWGS submission' in first


def test_navigation_never_claims_pending_action_count_during_processing():
    nav = TPL[TPL.index('<nav class="sticky'):TPL.index('</nav>')+6]
    assert 'not private_v2 and processing.terminal and not processing.failed' in nav
    assert 'readiness_summary.exception_count' in nav


def test_review_only_after_source_set_is_terminal_and_valid():
    assert "processing.terminal and not processing.failed" in TPL
    assert 'data-first-value-actions' in TPL
    Environment().parse(TPL)
