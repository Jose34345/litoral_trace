"""Market-backed first-value P0: present truthful source-linked work, never filing authority."""
from pathlib import Path

import pytest
from jinja2 import Environment
from tests.test_us_lacey_p0_processing_first_value import _render_summary

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = (ROOT / "src/litoral_trace/templates/us_lacey/operation_detail.html").read_text(encoding="utf-8")


@pytest.mark.parametrize("pending", [0, 1, 9, 73])
def test_first_value_surface_uses_live_pending_count_not_a_demo_literal(pending):
    rendered = _render_summary(pending=pending, reused_claim_count=3)
    assert 'shipment_workspace_summary.html' in TEMPLATE
    assert 'data-first-value-heading' in rendered
    assert f'data-workspace-count="pending">{pending}<' in rendered
    assert 'data-first-value-cta' in rendered
    assert 'href="#review-summary"' in rendered
    assert 'href="#operation-workspace"' in TEMPLATE
    assert "4 items need your review" not in rendered
    assert '3 verified claims reused' in rendered
    assert 'href="/evidence"' in rendered


def test_source_claim_authority_and_filing_boundary_remain_visible():
    rendered = _render_summary()
    assert 'data-human-filing-boundary' in rendered
    assert 'No automatic ACE / LAWGS submission.' in rendered
    assert 'source documents' in rendered
    assert 'data-readiness-summary' in rendered
    assert 'NOT READY' in rendered
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
