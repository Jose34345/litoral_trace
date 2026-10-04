from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
STATIC = ROOT / "src" / "litoral_trace" / "static"


def test_enterprise_shell_exposes_real_navigation_and_no_fake_routes() -> None:
    source = (TEMPLATES / "us_lacey" / "base.html").read_text(encoding="utf-8")
    assert 'class="lt-sidebar' in source
    assert 'class="lt-topbar' in source
    assert 'href="/operations"' in source
    assert 'href="/billing"' in source
    assert 'href="/legal/product-use-terms"' in source
    assert ">Evidence<" in source
    assert ">Settings<" in source
    assert 'aria-disabled="true"' in source
    assert 'href="/evidence"' not in source
    assert 'href="/settings"' not in source
    assert "⌘K" in source


def test_enterprise_semantic_tokens_and_jinja_primitives_exist() -> None:
    app_css = (STATIC / "src" / "app.css").read_text(encoding="utf-8")
    components = (TEMPLATES / "us_lacey" / "_components.html").read_text(encoding="utf-8")
    for token in (
        "--color-app:",
        "--color-panel:",
        "--color-muted:",
        "--color-enterprise-navy:",
        "--color-enterprise-emerald:",
        "--color-enterprise-amber:",
        "--color-enterprise-red:",
    ):
        assert token in app_css
    for macro in ("lt_badge", "lt_button", "lt_card", "lt_data_table"):
        assert f"macro {macro}" in components
    for state in ("READY", "REVIEW REQUIRED", "MISSING", "CONFLICT", "AUTO-RESOLVED"):
        assert state in components


def test_operations_is_dashboard_first_and_upload_is_compact() -> None:
    source = (TEMPLATES / "us_lacey" / "operations.html").read_text(encoding="utf-8")
    assert "Prepare and review Lacey declaration evidence." in source
    for metric in ("Active operations", "Review required", "Ready / completed", "Plan usage"):
        assert metric in source
    for column in ("Reference", "Supplier", "Documents", "Readiness", "Updated"):
        assert column in source
    assert 'id="new-operation-panel"' in source
    assert "lt-compact-uploader" in source
    assert 'action="/operations/intake"' in source


def test_review_workspace_uses_enterprise_density_and_reuse_badge() -> None:
    source = (
        TEMPLATES / "us_lacey" / "fragments" / "operation_workspace.html"
    ).read_text(encoding="utf-8")
    assert "lt-enterprise-review" in source
    assert "lt-enterprise-stat" in source
    assert 'lt_badge("AUTO-RESOLVED", "Reused from verified supplier evidence"' in source


def test_marketing_has_trust_pipeline_workflow_and_separate_pricing() -> None:
    source = (TEMPLATES / "public" / "lacey.html").read_text(encoding="utf-8")
    assert "U.S. Lacey Act Compliance Infrastructure" in source
    assert "Built for U.S. Lacey Act preparation" in source
    for value in ('("7", "documents")', '("31", "fields")', '("24", "resolved")', '("4", "exceptions")', '("3", "missing evidence")'):
        assert value in source
    for stage in ('("01", "Upload"', '("02", "Reconcile"', '("03", "Resolve"', '("04", "Review"', '("05", "Export"'):
        assert stage in source
    assert 'id="pricing"' in source
    hero = source.split('<section id="trust"', 1)[0]
    assert "USD 149" not in hero
