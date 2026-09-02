"""Architectural contracts preventing a second U.S. Lacey visual system."""
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VIEWS = ROOT / "src" / "litoral_trace" / "web"
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates" / "us_lacey"
PUBLIC_LACEY_TEMPLATES = (
    ROOT / "src" / "litoral_trace" / "templates" / "public" / "lacey.html",
    ROOT / "src" / "litoral_trace" / "templates" / "public" / "lacey_demo.html",
)


def test_us_lacey_views_delegate_html_to_shared_jinja_templates() -> None:
    for name in ("us_lacey_portal_views.py", "us_lacey_operational_views.py"):
        source = (VIEWS / name).read_text(encoding="utf-8")
        assert 'templates.get_template(f"us_lacey/{name}.html")' in source
        assert "<style" not in source
        assert "style=" not in source
        assert "background:#" not in source


def test_us_lacey_templates_use_shared_design_system_with_an_isolated_english_shell() -> None:
    base = (TEMPLATES / "base.html").read_text(encoding="utf-8")
    assert '{% extends "base.html" %}' in base
    assert '{% extends "public/base_public.html" %}' not in base
    assert '{% from "components/ui.html" import' in base
    assert 'lang="en"' in base
    assert "U.S. Lacey Act workspace" in base
    assert "PPQ 505 preparation" in base
    assert "Trazabilidad de origen" not in base
    assert "Debida diligencia" not in base

    for path in TEMPLATES.glob("*.html"):
        if path.name != "base.html":
            assert '{% extends "us_lacey/base.html" %}' in path.read_text(encoding="utf-8")

    # Public marketing/demo pages remain on the canonical public shell; only the
    # private U.S. customer portal is isolated from its regional navigation copy.
    for path in PUBLIC_LACEY_TEMPLATES:
        assert '{% extends "public/base_public.html" %}' in path.read_text(encoding="utf-8")


def test_us_lacey_operation_date_keeps_native_iso_control_with_us_guidance() -> None:
    source = (TEMPLATES / "new_operation.html").read_text(encoding="utf-8")
    assert 'name="operation_date" type="date"' in source
    assert 'lang="en-US"' in source
    assert "U.S. reference format: MM/DD/YYYY" in source
    assert "Your browser may display the date using your device locale." in source


def test_us_lacey_has_no_parallel_stylesheet_or_hardcoded_palette() -> None:
    assert not (ROOT / "src" / "litoral_trace" / "static" / "css" / "us-lacey.css").exists()
    assert not (ROOT / "src" / "litoral_trace" / "static" / "css" / "lacey_beta.css").exists()
    for path in (*TEMPLATES.glob("*.html"), *PUBLIC_LACEY_TEMPLATES):
        source = path.read_text(encoding="utf-8")
        assert "<style" not in source
        assert "style=" not in source
        assert "--lacey-" not in source
        assert "lacey_beta.css" not in source


def test_python_lacey_views_do_not_reintroduce_an_html_shell() -> None:
    for path in VIEWS.glob("*lacey*.py"):
        source = path.read_text(encoding="utf-8")
        assert "<style" not in source
        assert "<!doctype html" not in source.lower()
        assert "--lacey-" not in source


def test_us_lacey_templates_preserve_portal_actions_and_statuses() -> None:
    combined = "\n".join(path.read_text(encoding="utf-8") for path in TEMPLATES.glob("*.html"))
    for value in ("/signup", "/login", "/billing", "/operations", "/logout", "PAYMENT_PENDING", "PILOT"):
        assert value in combined
    for field in ("legal_name", "admin_email", "accept_terms", "csrf_token", "document"):
        assert f'name=\"{field}\"' in combined
