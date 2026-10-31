from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
STATIC = ROOT / "src" / "litoral_trace" / "static"
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"


def test_enterprise_brand_assets_are_versioned() -> None:
    svg = (STATIC / "img" / "logo.svg").read_text(encoding="utf-8")
    assert 'viewBox="0 0 512 512"' in svg
    assert '#059669' in svg
    assert '#020617' in svg

    favicon = (STATIC / "favicon.ico").read_bytes()
    apple = (STATIC / "apple-touch-icon.png").read_bytes()
    assert favicon.startswith(b"\x00\x00\x01\x00")
    assert apple.startswith(b"\x89PNG\r\n\x1a\n")


def test_root_head_serves_modern_and_legacy_brand_icons() -> None:
    base = (TEMPLATES / "base.html").read_text(encoding="utf-8")
    for path in ("/favicon.ico", "/img/logo.svg", "/apple-touch-icon.png"):
        assert path in base
    assert 'name="theme-color" content="#020617"' in base

    impersonation = (
        TEMPLATES / "us_lacey" / "impersonation_base.html"
    ).read_text(encoding="utf-8")
    assert "{% block brand_meta %}{{ super() }}{% endblock %}" in impersonation


def test_primary_shells_use_vector_brand_mark_instead_of_tree_icon() -> None:
    shell_paths = (
        TEMPLATES / "app" / "base_app.html",
        TEMPLATES / "public" / "base_public.html",
        TEMPLATES / "us_lacey" / "base.html",
        TEMPLATES / "us_lacey" / "marketing_base.html",
        TEMPLATES / "us_lacey" / "sandbox_start.html",
    )
    for path in shell_paths:
        source = path.read_text(encoding="utf-8")
        assert "path='/img/logo.svg'" in source
        assert "fa-tree" not in source
