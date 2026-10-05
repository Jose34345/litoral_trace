from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
STATIC = ROOT / "src" / "litoral_trace" / "static"


def test_enterprise_shell_exposes_only_real_navigation() -> None:
    source = (TEMPLATES / "us_lacey" / "base.html").read_text(encoding="utf-8")
    assert 'class="lt-sidebar' in source
    assert 'class="lt-topbar' in source
    assert 'href="/operations"' in source
    assert 'href="/evidence"' in source
    assert 'href="/suppliers"' in source
    assert ">Suppliers<" in source
    assert 'href="/audit-log"' in source
    assert ">Audit Log<" in source
    assert 'href="/billing"' in source
    assert 'href="/legal/product-use-terms"' in source
    assert ">Evidence<" in source
    assert ">Settings<" not in source
    assert "Planned" not in source
    assert 'href="/settings"' not in source
    assert ">Ctrl K</span>" in source
    assert "isApplePlatform ? '⌘ K' : 'Ctrl K'" in source


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


def test_operations_is_command_center_and_upload_is_compact() -> None:
    source = (TEMPLATES / "us_lacey" / "operations.html").read_text(encoding="utf-8")
    assert "Prepare and review Lacey declaration evidence." in source
    for metric in ("Open", "Needs review", "Ready to export", "Completed this month"):
        assert metric in source
    for column in ("Reference", "Supplier", "Documents", "Exceptions", "Readiness", "Updated"):
        assert column in source
    assert "Supplier unresolved" in source
    assert "relative_time(item.updated_at)" in source
    assert 'id="new-operation-panel"' in source
    assert "lt-compact-uploader" in source
    assert 'action="/operations/intake"' in source
    assert 'href="/operations/new"' in source


def test_review_workspace_uses_matrix_auditability_and_reuse_memory() -> None:
    source = (
        TEMPLATES / "us_lacey" / "fragments" / "operation_workspace.html"
    ).read_text(encoding="utf-8")
    assert "lt-enterprise-review" in source
    assert "Exception work queue" in source
    for column in ("Proposed value", "Evidence", "Status", "Decision"):
        assert column in source
    for action in ("Accept", "Override", "Request evidence", "Mark not applicable"):
        assert action in source
    assert "Resolved by Litoral Trace" in source
    assert "Authorized reviewer" in source
    assert "Preparation Package" in source
    assert "default_review_tab" in source


def test_marketing_has_command_center_security_lawgs_and_separate_pricing() -> None:
    source = (TEMPLATES / "public" / "lacey.html").read_text(encoding="utf-8")
    assert "U.S. Lacey Act Compliance Infrastructure" in source
    assert "Built for U.S. Lacey Act preparation" in source
    assert "Operations Command Center" in source
    assert "SYSTEM OF RECORD" in source
    for stage in ('("01", "Upload"', '("02", "Reconcile"', '("03", "Resolve"', '("04", "Review"', '("05", "Export"'):
        assert stage in source
    assert 'id="security"' in source
    assert "Tenant isolation" in source
    assert "Protected transport & storage" in source
    assert "Ephemeral evaluation sandbox" in source
    assert "No customer-document training pipeline" in source
    assert 'id="lawgs-output"' in source
    assert "&lt;LaceyDeclaration&gt;" in source
    assert 'id="pricing"' in source
    hero = source.split('<section id="trust"', 1)[0]
    assert "USD 149" not in hero
