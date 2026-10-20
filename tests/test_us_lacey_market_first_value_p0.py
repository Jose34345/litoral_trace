"""Market-backed first-value P0: present truthful source-linked work, never filing authority."""
from pathlib import Path
from jinja2 import Environment

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = (ROOT / "src/litoral_trace/templates/us_lacey/operation_detail.html").read_text(encoding="utf-8")


def test_first_value_surface_uses_live_pending_count_not_a_demo_literal():
    assert 'data-first-value-heading' in TEMPLATE
    assert 'readiness_summary.exception_count' in TEMPLATE
    assert 'data-first-value-cta' in TEMPLATE
    assert 'href="#operation-workspace"' in TEMPLATE
    assert 'item{{' in TEMPLATE
    assert "4 items need your review" not in TEMPLATE
    assert 'verified claims reused' in TEMPLATE
    assert 'reused_evidence_summary.claim_count' in TEMPLATE


def test_source_claim_authority_and_filing_boundary_remain_visible():
    assert 'data-human-filing-boundary' in TEMPLATE
    assert 'No automatic ACE / LAWGS submission.' in TEMPLATE
    assert 'source documents' in TEMPLATE
    assert 'data-readiness-summary' in TEMPLATE
    assert 'readiness_summary.overall' in TEMPLATE
    assert 'data-private-v2-boundary' in TEMPLATE


def test_public_workflow_does_not_auto_activate_lab():
    assert '{% if private_v2 %}' in TEMPLATE
    assert 'Private V2 lab' in TEMPLATE
    assert 'unified_v2' in TEMPLATE
    # Private V2 cases keep their authority banner. The sidebar is not an
    # entry point to an experimental capability for prospects.
    nav = (ROOT / "src/litoral_trace/templates/us_lacey/base.html").read_text(encoding="utf-8")
    assert 'href="/assurance-v2"' not in nav
    assert "Assurance V2 Lab" not in nav
    assert "Private V2 lab" in TEMPLATE


def test_jinja_syntax_compiles():
    Environment().parse(TEMPLATE)
