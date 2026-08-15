from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

STATIC = (
    ROOT
    / "src"
    / "litoral_trace"
    / "static"
)

BASE = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
    / "base.html"
)


def test_frontend_runtime_dependencies_are_exactly_pinned():
    package = json.loads(
        (ROOT / "package.json").read_text(
            encoding="utf-8"
        )
    )

    assert package["devDependencies"] == {
        "@fortawesome/fontawesome-free": "6.5.1",
        "@tailwindcss/cli": "4.3.1",
        "htmx.org": "2.0.10",
        "leaflet": "1.9.4",
        "tailwindcss": "4.3.1",
    }


def test_base_template_uses_only_local_runtime_assets():
    base = BASE.read_text(
        encoding="utf-8"
    )

    forbidden = (
        "cdn.tailwindcss.com",
        "unpkg.com",
        "cdnjs.cloudflare.com",
    )

    for dependency in forbidden:
        assert dependency not in base

    required = (
        "/dist/app.css",
        "/vendor/htmx/htmx.min.js",
        "/vendor/leaflet/leaflet.css",
        "/vendor/leaflet/leaflet.js",
        "/vendor/fontawesome/css/all.min.css",
        "/src/js/app.js",
    )

    for asset in required:
        assert asset in base


def test_compiled_and_vendored_assets_exist_and_are_nonempty():
    assets = (
        STATIC / "dist" / "app.css",
        STATIC / "vendor" / "htmx" / "htmx.min.js",
        STATIC / "vendor" / "leaflet" / "leaflet.css",
        STATIC / "vendor" / "leaflet" / "leaflet.js",
        (
            STATIC
            / "vendor"
            / "leaflet"
            / "images"
            / "marker-icon.png"
        ),
        (
            STATIC
            / "vendor"
            / "fontawesome"
            / "css"
            / "all.min.css"
        ),
        (
            STATIC
            / "vendor"
            / "fontawesome"
            / "webfonts"
            / "fa-solid-900.woff2"
        ),
    )

    for asset in assets:
        assert asset.is_file(), asset
        assert asset.stat().st_size > 0, asset


def test_leaflet_keeps_local_image_references():
    leaflet_css = (
        STATIC
        / "vendor"
        / "leaflet"
        / "leaflet.css"
    ).read_text(
        encoding="utf-8"
    )

    assert "images/layers.png" in leaflet_css
    assert "images/marker-icon.png" in leaflet_css


def test_fontawesome_keeps_local_webfont_references():
    fontawesome_css = (
        STATIC
        / "vendor"
        / "fontawesome"
        / "css"
        / "all.min.css"
    ).read_text(
        encoding="utf-8"
    )

    assert "../webfonts/" in fontawesome_css