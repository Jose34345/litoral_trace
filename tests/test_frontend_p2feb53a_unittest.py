from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi.responses import HTMLResponse

from litoral_trace.web import router as router_module
from litoral_trace.web.regional_intelligence import (
    REGIONAL_PROFILES,
    get_regional_profile,
    list_regional_profiles,
)
from litoral_trace.web.router import (
    router as web_router,
)


ROOT = Path(__file__).resolve().parents[1]

TEMPLATES = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
    / "public"
)


def _route_methods(
    path: str,
) -> set[str]:
    methods: set[str] = set()

    for route in web_router.routes:
        if route.path == path:
            methods.update(
                route.methods or set()
            )

    return methods


def test_regional_catalog_has_exact_supported_profiles():
    profiles = list_regional_profiles()

    assert profiles is REGIONAL_PROFILES

    assert tuple(
        profile.slug
        for profile in profiles
    ) == (
        "chaco",
        "corrientes",
        "misiones",
        "nea",
        "argentina",
    )

    assert len(
        {
            profile.slug
            for profile in profiles
        }
    ) == 5


def test_regional_profile_lookup_normalizes_slug():
    profile = get_regional_profile(
        "  ChAcO  "
    )

    assert profile is not None
    assert profile.slug == "chaco"
    assert profile.name == "Chaco"

    assert (
        get_regional_profile(
            "unsupported-region"
        )
        is None
    )


def test_regional_public_routes_exist():
    assert "GET" in _route_methods(
        "/regional-intelligence"
    )

    assert "GET" in _route_methods(
        "/regional-intelligence/{region_slug}"
    )


def test_regional_index_route_uses_catalog(
    monkeypatch,
):
    captured: dict[str, object] = {}

    def fake_render(
        request,
        name,
        *,
        user,
        context=None,
        status_code=200,
    ):
        captured["name"] = name
        captured["user"] = user
        captured["context"] = context

        return HTMLResponse(
            "ok",
            status_code=status_code,
        )

    monkeypatch.setattr(
        router_module,
        "render_web_template",
        fake_render,
    )

    response = asyncio.run(
        router_module
        .render_regional_intelligence_index_view(
            object()
        )
    )

    assert response.status_code == 200
    assert captured["name"] == (
        "public/regional_index.html"
    )
    assert captured["user"] is None

    context = captured["context"]

    assert context is not None
    assert len(
        context["regional_profiles"]
    ) == 5


def test_regional_detail_route_uses_single_profile(
    monkeypatch,
):
    captured: dict[str, object] = {}

    def fake_render(
        request,
        name,
        *,
        user,
        context=None,
        status_code=200,
    ):
        captured["name"] = name
        captured["context"] = context

        return HTMLResponse(
            "ok",
            status_code=status_code,
        )

    monkeypatch.setattr(
        router_module,
        "render_web_template",
        fake_render,
    )

    response = asyncio.run(
        router_module
        .render_regional_intelligence_detail_view(
            object(),
            "corrientes",
        )
    )

    assert response.status_code == 200
    assert captured["name"] == (
        "public/regional_detail.html"
    )

    profile = captured[
        "context"
    ]["profile"]

    assert profile.slug == "corrientes"


def test_unknown_regional_profile_returns_404():
    with pytest.raises(
        HTTPException
    ) as exc_info:
        asyncio.run(
            router_module
            .render_regional_intelligence_detail_view(
                object(),
                "unknown",
            )
        )

    assert (
        exc_info.value.status_code
        == 404
    )


def test_regional_index_is_data_driven():
    template = (
        TEMPLATES
        / "regional_index.html"
    ).read_text(
        encoding="utf-8"
    )

    assert (
        "{% for profile in regional_profiles %}"
        in template
    )

    assert (
        'href="/regional-intelligence/{{ profile.slug }}"'
        in template
    )

    assert (
        "{{ profile.name }}"
        in template
    )


def test_regional_detail_is_reusable_not_region_specific():
    template = (
        TEMPLATES
        / "regional_detail.html"
    ).read_text(
        encoding="utf-8"
    )

    assert "{{ profile.name }}" in template
    assert "{{ profile.headline }}" in template
    assert "{{ profile.summary }}" in template

    assert (
        "{% for focus in profile.focus_areas %}"
        in template
    )

    for forbidden_template in (
        "regional_chaco.html",
        "regional_corrientes.html",
        "regional_misiones.html",
        "regional_nea.html",
        "regional_argentina.html",
    ):
        assert not (
            TEMPLATES
            / forbidden_template
        ).exists()


def test_regional_foundation_avoids_unsourced_metrics():
    combined = "\n".join(
        (
            regional_module_text,
            (
                TEMPLATES
                / "regional_index.html"
            ).read_text(
                encoding="utf-8"
            ),
            (
                TEMPLATES
                / "regional_detail.html"
            ).read_text(
                encoding="utf-8"
            ),
        )
    )

    forbidden_claims = (
        "million hectares",
        "million tons",
        "market share",
        "largest producer",
        "number one producer",
        "99%",
    )

    for claim in forbidden_claims:
        assert claim not in combined.lower()


regional_module_text = (
    ROOT
    / "src"
    / "litoral_trace"
    / "web"
    / "regional_intelligence.py"
).read_text(
    encoding="utf-8"
)
