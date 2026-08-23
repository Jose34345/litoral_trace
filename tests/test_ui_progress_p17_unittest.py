from pathlib import Path

from jinja2 import Environment


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
STATIC_SRC = ROOT / "src" / "litoral_trace" / "static" / "src"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_p17_progress_component_parses_and_layer_is_loaded() -> None:
    base = _read(TEMPLATES / "base.html")
    component = _read(TEMPLATES / "components" / "progress.html")

    Environment().parse(component)
    assert "path='/src/progress.css'" in base


def test_p17_progress_uses_native_measurable_progress() -> None:
    component = _read(TEMPLATES / "components" / "progress.html")

    assert "macro progress_bar" in component
    assert "<progress" in component
    assert 'value="{{ value }}"' in component
    assert 'max="{{ max_value }}"' in component
    assert 'aria-label="{{ label }}"' in component

    # Jinja itself uses {% ... %}; guard specifically against converting a
    # server-supplied value/max pair into a fabricated user-facing percentage.
    assert 'value ~ "%"' not in component
    assert 'max_value ~ "%"' not in component
    assert "100%" not in component


def test_p17_progress_steps_express_state_without_fake_percentage() -> None:
    component = _read(TEMPLATES / "components" / "progress.html")

    assert "macro progress_steps" in component
    assert 'data-state="{{ step.state }}"' in component
    assert 'aria-current="step"' in component
    assert "step.state == 'complete'" in component
    assert "step.state == 'current'" in component
    assert "step.state == 'blocked'" in component
    assert "<script" not in component


def test_p17_progress_css_defines_server_state_visuals() -> None:
    css = _read(STATIC_SRC / "progress.css")

    for selector in (
        ".lt-progress",
        ".lt-progress__bar",
        ".lt-progress-steps",
        ".lt-progress-step",
        '.lt-progress-step[data-state="complete"]',
        '.lt-progress-step[data-state="current"]',
        '.lt-progress-step[data-state="blocked"]',
        '.lt-progress-step[data-state="pending"]',
    ):
        assert selector in css

    assert "font-variant-numeric: tabular-nums" in css
    assert "@media (prefers-reduced-motion: reduce)" in css
