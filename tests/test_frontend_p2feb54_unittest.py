from __future__ import annotations

import json

import pytest

from fastapi.testclient import TestClient

import main

from litoral_trace.web.csrf import (
    CSRF_BROWSER_COOKIE_KEY,
)


_TEST_JWT_SECRET = (
    "p2feb54-http-acceptance-secret-"
    + ("x" * 64)
)


PUBLIC_HTML_ROUTES = (
    "/",
    "/login",
    "/regional-intelligence",
    "/regional-intelligence/chaco",
    "/regional-intelligence/corrientes",
    "/regional-intelligence/misiones",
    "/regional-intelligence/nea",
    "/regional-intelligence/argentina",
)


PRIVATE_HTML_ROUTES = (
    "/dashboard",
    "/vault",
    "/settings",
    "/admin",
)


CRITICAL_STATIC_ASSETS = (
    (
        "/static/dist/app.css",
        "text/css",
    ),
    (
        "/static/src/js/app.js",
        "javascript",
    ),
    (
        "/static/src/js/"
        "regional_intelligence_map.js",
        "javascript",
    ),
    (
        "/static/vendor/htmx/"
        "htmx.min.js",
        "javascript",
    ),
    (
        "/static/vendor/leaflet/"
        "leaflet.js",
        "javascript",
    ),
    (
        "/static/vendor/leaflet/"
        "leaflet.css",
        "text/css",
    ),
    (
        "/static/vendor/fontawesome/"
        "css/all.min.css",
        "text/css",
    ),
)


REGIONAL_ROUTES = (
    (
        "/regional-intelligence/chaco",
        "ARG-CHACO",
        "Chaco",
    ),
    (
        "/regional-intelligence/corrientes",
        "ARG-CORRIENTES",
        "Corrientes",
    ),
    (
        "/regional-intelligence/misiones",
        "ARG-MISIONES",
        "Misiones",
    ),
    (
        "/regional-intelligence/nea",
        "ARG-NEA",
        "NEA",
    ),
    (
        "/regional-intelligence/argentina",
        "ARG",
        "Argentina",
    ),
)


@pytest.fixture
def web_client(
    monkeypatch: pytest.MonkeyPatch,
):
    """
    Isolated browser-like client for frontend
    acceptance tests.

    Production still fails closed when JWT_SECRET_KEY
    is absent. The test secret exists only for the
    duration of this fixture.
    """

    monkeypatch.setenv(
        "JWT_SECRET_KEY",
        _TEST_JWT_SECRET,
    )

    with TestClient(
        main.app,
        base_url="https://testserver",
    ) as client:
        yield client


@pytest.mark.parametrize(
    "path",
    PUBLIC_HTML_ROUTES,
)
def test_public_html_routes_render_successfully(
    web_client: TestClient,
    path: str,
):
    response = web_client.get(
        path,
        follow_redirects=False,
    )

    assert (
        response.status_code
        == 200
    )

    assert (
        "text/html"
        in response.headers.get(
            "content-type",
            "",
        ).lower()
    )

    assert response.content


@pytest.mark.parametrize(
    "path",
    PUBLIC_HTML_ROUTES,
)
def test_public_html_is_not_browser_cached(
    web_client: TestClient,
    path: str,
):
    response = web_client.get(
        path,
        follow_redirects=False,
    )

    assert (
        response.status_code
        == 200
    )

    assert (
        response.headers.get(
            "cache-control"
        )
        == "no-store, max-age=0"
    )

    assert (
        response.headers.get(
            "pragma"
        )
        == "no-cache"
    )


def test_public_html_establishes_browser_csrf_binding(
    web_client: TestClient,
):
    response = web_client.get(
        "/login",
        follow_redirects=False,
    )

    assert (
        response.status_code
        == 200
    )

    assert (
        CSRF_BROWSER_COOKIE_KEY
        in web_client.cookies
    )

    browser_nonce = (
        web_client.cookies.get(
            CSRF_BROWSER_COOKIE_KEY
        )
    )

    assert browser_nonce

    assert (
        len(browser_nonce)
        >= 32
    )


@pytest.mark.parametrize(
    "path",
    PRIVATE_HTML_ROUTES,
)
def test_anonymous_private_routes_redirect_to_login(
    web_client: TestClient,
    path: str,
):
    response = web_client.get(
        path,
        follow_redirects=False,
    )

    assert (
        response.status_code
        == 303
    )

    assert (
        response.headers.get(
            "location"
        )
        == "/login"
    )


def test_unknown_regional_profile_returns_404(
    web_client: TestClient,
):
    response = web_client.get(
        "/regional-intelligence/"
        "no-existe",
        follow_redirects=False,
    )

    assert (
        response.status_code
        == 404
    )


@pytest.mark.parametrize(
    (
        "path",
        "region_id",
        "display_name",
    ),
    REGIONAL_ROUTES,
)
def test_regional_detail_routes_render_canonical_identity(
    web_client: TestClient,
    path: str,
    region_id: str,
    display_name: str,
):
    response = web_client.get(
        path,
        follow_redirects=False,
    )

    assert (
        response.status_code
        == 200
    )

    body = response.text

    assert region_id in body
    assert display_name in body

    assert (
        "data-regional-map"
        in body
    )

    assert (
        "/static/data/georef/"
        "provincias.geojson"
        in body
    )

    assert (
        "regional_intelligence_map.js"
        in body
    )

    assert (
        "Geography, not risk"
        in body
    )


def test_regional_index_exposes_all_canonical_profiles(
    web_client: TestClient,
):
    response = web_client.get(
        "/regional-intelligence",
        follow_redirects=False,
    )

    assert (
        response.status_code
        == 200
    )

    body = response.text

    for region_id in (
        "ARG-CHACO",
        "ARG-CORRIENTES",
        "ARG-MISIONES",
        "ARG-NEA",
        "ARG",
    ):
        assert region_id in body

    assert (
        "data-regional-map"
        in body
    )

    assert (
        "data-regional-map-control"
        in body
    )

    assert (
        "/static/data/georef/"
        "provincias.geojson"
        in body
    )


@pytest.mark.parametrize(
    (
        "path",
        "content_type_fragment",
    ),
    CRITICAL_STATIC_ASSETS,
)
def test_critical_frontend_assets_are_served(
    web_client: TestClient,
    path: str,
    content_type_fragment: str,
):
    response = web_client.get(
        path,
        follow_redirects=False,
    )

    assert (
        response.status_code
        == 200
    )

    assert response.content

    assert (
        content_type_fragment
        in response.headers.get(
            "content-type",
            "",
        ).lower()
    )


def test_georef_snapshot_is_served_as_valid_geojson(
    web_client: TestClient,
):
    response = web_client.get(
        "/static/data/georef/"
        "provincias.geojson",
        follow_redirects=False,
    )

    assert (
        response.status_code
        == 200
    )

    assert response.content

    payload = json.loads(
        response.content
    )

    assert (
        payload.get("type")
        == "FeatureCollection"
    )

    features = payload.get(
        "features"
    )

    assert isinstance(
        features,
        list,
    )

    assert (
        len(features)
        == 24
    )


def test_public_pages_reference_only_local_frontend_runtime(
    web_client: TestClient,
):
    response = web_client.get(
        "/regional-intelligence",
        follow_redirects=False,
    )

    assert (
        response.status_code
        == 200
    )

    body = (
        response.text.lower()
    )

    expected_local_assets = (
        "/static/dist/app.css",
        "/static/src/js/app.js",
        "/static/vendor/htmx/",
        "/static/vendor/leaflet/",
        "/static/vendor/fontawesome/",
    )

    for asset in expected_local_assets:
        assert asset in body

    forbidden_runtime_hosts = (
        "cdn.tailwindcss.com",
        "unpkg.com",
        "tile.openstreetmap.org",
        "mapbox.com",
        "maps.googleapis.com",
        "apis.datos.gob.ar",
    )

    for host in forbidden_runtime_hosts:
        assert host not in body