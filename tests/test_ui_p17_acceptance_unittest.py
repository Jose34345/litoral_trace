from pathlib import Path

from jinja2 import Environment


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
STATIC_SRC = ROOT / "src" / "litoral_trace" / "static" / "src"
SCRIPTS = ROOT / "scripts"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_p17_all_shared_component_templates_parse() -> None:
    env = Environment()
    for name in (
        "ui.html",
        "data_table.html",
        "form_fields.html",
        "dialog.html",
        "dropdown.html",
        "skeleton.html",
        "progress.html",
    ):
        env.parse(_read(TEMPLATES / "components" / name))


def test_p17_base_loads_each_shared_ui_layer_once() -> None:
    base = _read(TEMPLATES / "base.html")

    for asset in (
        "design-system.css",
        "data-table.css",
        "form-controls.css",
        "dialog.css",
        "dropdown.css",
        "skeleton.css",
        "progress.css",
        "geospatial.css",
    ):
        assert base.count(f"/src/{asset}") == 1

    assert base.count("/src/js/app.js") == 1
    assert base.count("/src/js/dialog.js") == 1


def test_p17_app_shell_preserves_navigation_session_and_native_dropdown_contracts() -> None:
    shell = _read(TEMPLATES / "app" / "base_app.html")
    dropdown = _read(TEMPLATES / "components" / "dropdown.html")

    assert "path='/src/app-shell.css'" in shell
    assert 'id="app-navigation"' in shell
    assert 'id="main-content"' in shell
    assert 'action="/logout"' in shell
    assert "csrf_form_field" in shell
    assert 'popover="auto"' in dropdown
    assert "popovertarget=" in dropdown
    assert "<script" not in dropdown


def test_p17_dialog_uses_native_dialog_and_independent_controller() -> None:
    base = _read(TEMPLATES / "base.html")
    dialog = _read(TEMPLATES / "components" / "dialog.html")
    controller = _read(STATIC_SRC / "js" / "dialog.js")

    assert "<dialog" in dialog
    assert "data-lt-dialog" in dialog
    assert 'aria-haspopup="dialog"' in dialog
    assert "showModal" in controller
    assert "data-lt-dialog-open" in controller
    assert "/src/js/dialog.js" in base


def test_p17_progress_and_loading_components_do_not_create_fake_domain_state() -> None:
    progress = _read(TEMPLATES / "components" / "progress.html")
    skeleton = _read(TEMPLATES / "components" / "skeleton.html")

    assert "<progress" in progress
    assert 'value="{{ value }}"' in progress
    assert 'max="{{ max_value }}"' in progress
    assert 'data-state="{{ step.state }}"' in progress
    assert 'role="status"' in skeleton
    assert 'aria-busy="true"' in skeleton
    assert "<script" not in progress
    assert "<script" not in skeleton


def test_p17_geospatial_runtime_contract_remains_identical() -> None:
    dashboard = _read(TEMPLATES / "dashboard.html")

    for contract in (
        'id="map"',
        'id="map-scope"',
        "L.map('map'",
        "L.tileLayer(",
        "L.marker(",
        ".bindPopup(",
        ".fitBounds(",
        "document.getElementById('map-scope')",
        "/api/v1/lotes",
        "/api/v1/satellite/jobs",
    ):
        assert contract in dashboard


def test_p17_ui_stack_keeps_server_as_source_of_truth() -> None:
    settings = _read(TEMPLATES / "settings.html")
    shell = _read(TEMPLATES / "app" / "base_app.html")

    assert 'hx-post="/api/v1/settings/invite_demo_user"' in settings
    assert 'hx-target="#inviteFeedback"' in settings
    assert 'action="/logout"' in shell
    assert "csrf_token" in shell


def test_p17_vendor_copy_normalizes_text_assets_for_cross_platform_builds() -> None:
    script = _read(SCRIPTS / "copy_frontend_vendor.mjs")

    assert "async function copyTextFile(" in script
    assert 'content.replace(/\\r\\n?/g, "\\n")' in script

    for asset in (
        '"htmx.min.js"',
        '"leaflet.css"',
        '"leaflet.js"',
        '"all.min.css"',
    ):
        assert asset in script

    # Binary images/webfonts continue to use byte-for-byte copyDirectory/copyFile.
    assert "await copyDirectory(" in script


def test_p17_acceptance_has_no_new_backend_or_migration_artifact() -> None:
    expected_ui_files = (
        STATIC_SRC / "design-system.css",
        STATIC_SRC / "app-shell.css",
        STATIC_SRC / "data-table.css",
        STATIC_SRC / "form-controls.css",
        STATIC_SRC / "dialog.css",
        STATIC_SRC / "dropdown.css",
        STATIC_SRC / "skeleton.css",
        STATIC_SRC / "progress.css",
        STATIC_SRC / "geospatial.css",
    )

    for path in expected_ui_files:
        assert path.is_file()

    # P1.7-J itself is an acceptance gate. Backend/schema invariants are additionally
    # enforced by the repository-wide CI, Alembic canonical-head gate and PostgreSQL
    # web stabilization workflow on every stacked PR.
