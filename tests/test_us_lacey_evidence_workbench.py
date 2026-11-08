"""Evidence Review Workbench: structural, authority and responsive contracts."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "src" / "litoral_trace"
TEMPLATES = ROOT / "templates" / "us_lacey"
STATIC = ROOT / "static"


def read(path):
    return path.read_text(encoding="utf-8")


def test_compact_workbench_keeps_authoritative_controls():
    operation = read(TEMPLATES / "operation_detail.html")
    summary = read(TEMPLATES / "fragments/shipment_workspace_summary.html")
    tasks = read(TEMPLATES / "fragments/operation_workspace.html")
    assert "data-evidence-workbench" in operation
    assert "data-shipment-section-nav" in operation
    assert "data-workflow-stage-details" in operation
    assert "lt-workbench-legacy-workflow" in operation
    assert 'id="operation-workspace"' in tasks
    assert 'id="review-summary"' in tasks
    assert "data-workbench-queue-title" in tasks
    assert 'id="shipment-summary"' in summary
    for name in ("data-readiness-summary", "data-human-filing-boundary",
                 "data-first-value-cta", "data-workspace-count", "lt-shipment-status-grid"):
        assert name in summary
    assert "No automatic ACE / LAWGS submission." in summary
    assert "fragments/shipment_readiness_status.html" in summary
    assert "fragments/species_status.html" in summary
    assert "fragments/country_status.html" in summary


def test_both_upload_entry_points_use_existing_authorized_dialog():
    operation = read(TEMPLATES / "operation_detail.html")
    assert operation.count("data-add-documents-trigger") >= 3
    assert "triggers.forEach" in operation
    assert 'id="add-evidence-dialog"' in operation
    assert 'name="csrf_token" value="{{ upload_csrf }}"' in operation
    assert 'action="/operations/{{ detail.public_id }}/upload"' in operation


def test_sidebar_rail_preserves_mobile_drawer_and_accessible_names():
    base = read(TEMPLATES / "base.html")
    css = read(STATIC / "src/app-shell.css")
    script = read(STATIC / "js/sidebar_collapse.js")
    assert 'data-app-sidebar-toggle aria-controls="app-navigation"' in base
    for token in ("data-app-drawer-open", "data-app-drawer-close", "data-app-drawer-overlay"):
        assert token in base
    assert "@media(min-width:1024px)" in css
    assert '.lt-app-shell[data-sidebar-collapsed="true"] .lt-workspace' in css
    assert "Expand navigation" in script and "Collapse navigation" in script
    assert "localStorage" in script and "aria-expanded" in script
    assert "aria-label" in script
    assert "sidebar_collapse.js') }}?v=20261031-workbench1" in base


def test_pdf_split_is_desktop_only_and_released_with_source():
    css = read(STATIC / "css/us_lacey_source_inspector.css")
    script = read(STATIC / "js/us_lacey_source_inspector.js")
    html = read(TEMPLATES / "fragments/source_inspector.html")
    assert "@media(min-width:1280px)" in css
    assert ".lt-source-inspector:not(:modal)" in css
    assert 'data-workbench-inspector-open="true"' in css
    assert script.count('removeAttribute("data-workbench-inspector-open")') >= 2
    assert 'setAttribute("data-workbench-inspector-open", "true")' in script
    assert "if (mobileViewport()) root.showModal(); else root.show();" in script
    assert "data-source-inspector" in html
    assert "data-inspector-canvas" in html


def test_cache_versions_bumped_for_all_changed_assets():
    operation = read(TEMPLATES / "operation_detail.html")
    base = read(TEMPLATES / "base.html")
    assert "shipment_workspace.css') }}?v=20261031-workbench1" in operation
    assert "us_lacey_source_inspector.css') }}?v=20261031-workbench1" in operation
    assert "us_lacey_source_inspector.js') }}?v=20261031-workbench1" in operation
    assert "app-shell.css') }}?v=20261031-workbench1" in base
