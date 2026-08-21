from __future__ import annotations

import re

from pathlib import Path


ROOT = (
    Path(__file__).resolve().parents[1]
)

TEMPLATES = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
)

STATIC_JS = (
    ROOT
    / "src"
    / "litoral_trace"
    / "static"
    / "src"
    / "js"
)

REGIONAL_INDEX = (
    TEMPLATES
    / "public"
    / "regional_index.html"
)

REGIONAL_DETAIL = (
    TEMPLATES
    / "public"
    / "regional_detail.html"
)

PUBLIC_BASE = (
    TEMPLATES
    / "public"
    / "base_public.html"
)

REGIONAL_MAP_JS = (
    STATIC_JS
    / "regional_intelligence_map.js"
)


def _read(
    path: Path,
) -> str:
    return path.read_text(
        encoding="utf-8"
    )


def _map_status_tag(
    template: str,
) -> str:
    match = re.search(
        (
            r"<div"
            r"(?=[^>]*"
            r"data-regional-map-status)"
            r"[^>]*>"
        ),
        template,
        flags=re.DOTALL,
    )

    assert match is not None

    return match.group(0)


def test_regional_map_status_is_hidden_before_javascript_boot():
    for path in (
        REGIONAL_INDEX,
        REGIONAL_DETAIL,
    ):
        template = _read(path)

        status_tag = (
            _map_status_tag(
                template
            )
        )

        assert re.search(
            r"\bhidden\b",
            status_tag,
        )


def test_regional_map_status_is_accessible_live_region():
    for path in (
        REGIONAL_INDEX,
        REGIONAL_DETAIL,
    ):
        template = _read(path)

        status_tag = (
            _map_status_tag(
                template
            )
        )

        assert (
            'role="status"'
            in status_tag
        )

        assert (
            'aria-live="polite"'
            in status_tag
        )

        assert (
            'aria-atomic="true"'
            in status_tag
        )


def test_regional_maps_have_explicit_no_javascript_fallback():
    for path in (
        REGIONAL_INDEX,
        REGIONAL_DETAIL,
    ):
        template = _read(path)

        assert "<noscript>" in template
        assert "</noscript>" in template
        assert "La visualización interactiva requiere JavaScript." in template
        assert "continúa" in template
        assert "disponible" in template


def test_no_javascript_fallback_does_not_claim_risk():
    for path in (
        REGIONAL_INDEX,
        REGIONAL_DETAIL,
    ):
        template = _read(path)

        start = template.find(
            "<noscript>"
        )

        end = template.find(
            "</noscript>"
        )

        assert start >= 0
        assert end > start

        fallback = (
            template[
                start:end
            ].lower()
        )

        forbidden_claims = (
            "low risk",
            "medium risk",
            "high risk",
            "compliant",
            "non-compliant",
            "non compliant",
        )

        for claim in forbidden_claims:
            assert (
                claim
                not in fallback
            )


def test_map_containers_have_accessible_region_labels():
    for path in (
        REGIONAL_INDEX,
        REGIONAL_DETAIL,
    ):
        template = _read(path)

        match = re.search(
            (
                r"<div"
                r"(?=[^>]*"
                r"\bdata-regional-map\b)"
                r"[^>]*>"
            ),
            template,
            flags=re.DOTALL,
        )

        assert match is not None

        map_tag = match.group(0)

        assert (
            'role="region"'
            in map_tag
        )

        assert (
            'aria-label="'
            in map_tag
        )


def test_index_map_controls_use_native_buttons():
    template = _read(
        REGIONAL_INDEX
    )

    match = re.search(
        (
            r"<button"
            r"(?=[^>]*"
            r"data-regional-map-control)"
            r"[^>]*>"
        ),
        template,
        flags=re.DOTALL,
    )

    assert match is not None

    button_tag = (
        match.group(0)
    )

    assert (
        'type="button"'
        in button_tag
    )

    assert (
        'aria-pressed="false"'
        in button_tag
    )


def test_index_profile_navigation_works_without_javascript():
    template = _read(
        REGIONAL_INDEX
    )

    assert (
        'href="/regional-intelligence/'
        '{{ profile.slug }}"'
        in template
    )

    assert (
        "data-regional-link"
        in template
    )


def test_map_runtime_has_explicit_status_show_hide_contract():
    javascript = _read(
        REGIONAL_MAP_JS
    )

    assert "const showStatus =" in javascript
    assert "const hideStatus =" in javascript
    assert "status.hidden = false" in javascript
    assert "status.hidden = true" in javascript


def test_map_runtime_reports_loading_and_fail_safe_states():
    javascript = _read(
        REGIONAL_MAP_JS
    )

    assert "Cargando referencia territorial" in javascript
    assert "El mapa territorial no está disponible" in javascript
    assert "No hay un dataset territorial configurado" in javascript
    assert "No fue posible cargar la visualización territorial" in javascript
    assert "continúa" in javascript or "continúan" in javascript


def test_map_runtime_respects_reduced_motion():
    javascript = _read(
        REGIONAL_MAP_JS
    )

    assert (
        "prefers-reduced-motion: reduce"
        in javascript
    )

    assert (
        "animate: !reducedMotion"
        in javascript
    )


def test_map_runtime_remains_local_and_tile_free():
    javascript = _read(
        REGIONAL_MAP_JS
    ).lower()

    assert (
        "window.fetch"
        in javascript
    )

    forbidden = (
        "tilelayer",
        "openstreetmap",
        "mapbox",
        "maps.googleapis",
        "apis.datos.gob.ar",
    )

    for value in forbidden:
        assert (
            value
            not in javascript
        )


def test_public_shell_keeps_skip_navigation_contract():
    template = _read(
        PUBLIC_BASE
    )

    assert (
        'href="#main-content"'
        in template
    )

    assert (
        'id="main-content"'
        in template
    )

    assert (
        'tabindex="-1"'
        in template
    )


def test_public_shell_does_not_expose_authenticated_context():
    template = _read(
        PUBLIC_BASE
    )

    forbidden = (
        "user.organization_name",
        "user.username",
        'action="/logout"',
    )

    for value in forbidden:
        assert (
            value
            not in template
        )
