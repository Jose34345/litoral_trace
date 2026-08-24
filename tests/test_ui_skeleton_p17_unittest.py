from pathlib import Path

from jinja2 import Environment


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
STATIC_SRC = ROOT / "src" / "litoral_trace" / "static" / "src"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_p17_skeleton_component_parses_and_layer_is_loaded() -> None:
    base = _read(TEMPLATES / "base.html")
    component = _read(TEMPLATES / "components" / "skeleton.html")

    Environment().parse(component)
    assert "path='/src/skeleton.css'" in base


def test_p17_skeleton_exposes_accessible_loading_primitives() -> None:
    component = _read(TEMPLATES / "components" / "skeleton.html")

    for macro in (
        "macro skeleton_line",
        "macro skeleton_avatar",
        "macro skeleton_block",
        "macro loading_region",
        "macro skeleton_table",
    ):
        assert macro in component

    assert 'role="status"' in component
    assert 'aria-live="polite"' in component
    assert 'aria-busy="true"' in component
    assert "lt-visually-hidden" in component
    assert 'aria-hidden="true"' in component


def test_p17_skeleton_is_presentational_only() -> None:
    component = _read(TEMPLATES / "components" / "skeleton.html")

    assert "hx-post=" not in component
    assert "hx-get=" not in component
    assert "<script" not in component
    assert "data-state=" not in component


def test_p17_skeleton_css_honors_motion_and_table_density() -> None:
    css = _read(STATIC_SRC / "skeleton.css")

    for selector in (
        ".lt-skeleton-region",
        ".lt-skeleton",
        ".lt-skeleton::after",
        ".lt-skeleton--line",
        ".lt-skeleton--block",
        ".lt-skeleton--avatar",
        ".lt-skeleton-table",
        ".lt-skeleton-table__row",
    ):
        assert selector in css

    assert "@keyframes lt-skeleton-shimmer" in css
    assert "@media (prefers-reduced-motion: reduce)" in css
    assert "animation: none" in css
