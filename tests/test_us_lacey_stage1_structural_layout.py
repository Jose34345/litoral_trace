"""Stage 1 structural UX regression contracts without changing regulatory behavior."""
from pathlib import Path

BASE = Path(__file__).resolve().parents[1] / "src" / "litoral_trace"
TEMPLATES = BASE / "templates" / "us_lacey"
STYLES = BASE / "static" / "css"


def test_readiness_status_uses_explicit_responsive_grid():
    html = (TEMPLATES / "fragments/shipment_workspace_summary.html").read_text(encoding="utf-8")
    css = (STYLES / "shipment_workspace.css").read_text(encoding="utf-8")
    assert 'class="lt-shipment-status-grid"' in html
    for file in ("shipment_readiness_status.html", "species_status.html", "country_status.html"):
        assert f'fragments/{file}' in html
    assert "grid-template-columns:repeat(3,minmax(0,1fr))" in css
    assert ".lt-shipment-status-grid > :last-child" in css


def test_task_layout_uses_named_slots_not_child_positions():
    html = (TEMPLATES / "fragments/operation_workspace.html").read_text(encoding="utf-8")
    css = (STYLES / "us_lacey_exception_tasks.css").read_text(encoding="utf-8")
    for name in ("identity", "value", "status", "decision", "evidence"):
        assert f"lt-exception-field__{name}" in html
        assert f"grid-area:{name}" in css
    assert "nth-child(" not in css
    assert 'grid-template-areas:"identity value" "status decision" "evidence evidence"' in css
    assert 'grid-template-areas:"identity" "value" "status" "decision" "evidence"' in css


def test_css_loads_in_head_and_nav_has_anchor_offsets():
    page = (TEMPLATES / "operation_detail.html").read_text(encoding="utf-8")
    fragment = (TEMPLATES / "fragments/operation_workspace.html").read_text(encoding="utf-8")
    nav = (STYLES / "shipment_workspace.css").read_text(encoding="utf-8")
    assert "css/us_lacey_exception_tasks.css" in page
    assert "css/us_lacey_exception_tasks.css" not in fragment
    assert "scroll-margin-top:10rem" in nav
    assert ".lt-shipment-nav a:focus-visible" in nav
