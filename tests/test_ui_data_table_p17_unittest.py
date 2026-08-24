from pathlib import Path

from jinja2 import Environment


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
STATIC_SRC = ROOT / "src" / "litoral_trace" / "static" / "src"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_p17_data_table_templates_parse_and_global_layer_is_loaded() -> None:
    base = _read(TEMPLATES / "base.html")
    component = _read(TEMPLATES / "components" / "data_table.html")
    canary = _read(TEMPLATES / "admin_organizations.html")
    catalog = _read(TEMPLATES / "dev" / "ui_catalog.html")

    env = Environment()
    for template in (base, component, canary, catalog):
        env.parse(template)

    assert "path='/src/data-table.css'" in base
    assert '{% from "components/data_table.html" import empty_row %}' in canary
    assert "components/data_table.html" in catalog


def test_p17_data_table_component_exposes_server_rendered_primitives() -> None:
    component = _read(TEMPLATES / "components" / "data_table.html")

    for macro in (
        "macro empty_row",
        "macro search_control",
        "macro page_link",
        "macro pagination",
    ):
        assert macro in component

    assert 'type="search"' in component
    assert 'aria-label="Paginación de resultados"' in component
    assert 'aria-disabled="true"' in component
    assert 'aria-current="page"' in component

    # LTDataTable remains server-rendered in C: no new JS/event contract is
    # introduced merely to provide search, empty and pagination primitives.
    assert "<script" not in component
    assert "onclick=" not in component


def test_p17_data_table_canary_preserves_existing_admin_contract() -> None:
    canary = _read(TEMPLATES / "admin_organizations.html")

    assert 'hx-post="/api/v1/admin/organizations"' in canary
    assert 'hx-target="#adminCreateResult"' in canary
    assert "{% for organization in organizations %}" in canary
    assert "{{ organization_count }} registradas" in canary
    assert "organization.license_plan_type" in canary
    assert "organization.license_max_lotes" in canary
    assert "organization.admin_username" in canary
    assert "organization.is_active" in canary
    assert 'empty_row(7, "No hay organizaciones registradas para mostrar.")' in canary

    for class_name in (
        "lt-data-table",
        "lt-data-table__summary",
        "lt-data-table__scroll",
        "lt-data-table__table",
        "lt-data-table__head",
        "lt-data-table__body",
        "lt-data-table__row",
        "lt-data-table__cell",
    ):
        assert class_name in canary


def test_p17_data_table_canary_keeps_tailwind_reproducibility_candidates() -> None:
    canary = _read(TEMPLATES / "admin_organizations.html")

    # C layers semantic classes after the historical utilities. Keeping these
    # candidates prevents a UI-only migration from changing dist/app.css.
    for candidate in (
        "overflow-x-auto rounded-xl border border-slate-200",
        "w-full text-left text-xs",
        "border-b border-slate-200 bg-slate-50",
        "divide-y divide-slate-200",
        "hover:bg-slate-50",
        "p-3 font-bold",
        "p-3 text-slate-500",
    ):
        assert candidate in canary


def test_p17_data_table_css_defines_operational_and_accessibility_states() -> None:
    css = _read(STATIC_SRC / "data-table.css")

    for selector in (
        ".lt-data-table__toolbar",
        ".lt-data-table__search",
        ".lt-data-table__scroll",
        ".lt-data-table__table",
        ".lt-data-table__row[data-selected=\"true\"]",
        ".lt-data-table__row[data-state=\"attention\"]",
        ".lt-data-table__cell--numeric",
        ".lt-data-table__sort:focus-visible",
        ".lt-data-table__bulkbar[hidden]",
        ".lt-data-table__empty-cell",
        ".lt-data-table__pagination",
        ".lt-data-table__page[aria-current=\"page\"]",
        ".lt-data-table__page[aria-disabled=\"true\"]",
        ".lt-visually-hidden",
    ):
        assert selector in css

    assert "@media (max-width: 639px)" in css
    assert "@media (prefers-reduced-motion: reduce)" in css
