from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.testclient import TestClient

import main
import litoral_trace.web.router as web_router_module
from litoral_trace.api.auth import (
    UserTenantContext,
)
from litoral_trace.auth.rbac import (
    Permission,
)


TEMPLATE_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "litoral_trace"
    / "templates"
    / "vault.html"
)


def _request() -> Request:
    return Request(
        scope={
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/vault",
            "raw_path": b"/vault",
            "query_string": b"",
            "headers": [],
            "client": (
                "127.0.0.1",
                50000,
            ),
            "server": (
                "testserver",
                80,
            ),
            "root_path": "",
        }
    )


def _user(
    role: str,
) -> UserTenantContext:
    return UserTenantContext(
        user_id=10,
        username=f"{role}_user",
        organization_id=7,
        organization_name="P23F Tenant",
        organization_slug="p23f-tenant",
        role=role,
        email=f"{role}@example.com",
        session_id=99,
        is_platform_superadmin=(
            role == "superadmin"
        ),
    )


def _template_text() -> str:
    return TEMPLATE_PATH.read_text(
        encoding="utf-8"
    )


def test_vault_template_removes_all_synthetic_demo_documents():
    template = _template_text()

    assert (
        "DOC-DDS-2026-001"
        not in template
    )
    assert (
        "DOC-PDF-2026-002"
        not in template
    )
    assert (
        "DOC-XLS-2026-004"
        not in template
    )
    assert (
        "const docsData"
        not in template
    )
    assert (
        "254.7 KB"
        not in template
    )
    assert (
        "4 Archivos"
        not in template
    )


def test_vault_template_uses_real_enterprise_api_endpoints():
    template = _template_text()

    assert (
        'listUrl: "/api/v1/vault/documents"'
        in template
    )

    assert (
        'method: "POST"'
        in template
    )

    assert (
        'method: "DELETE"'
        in template
    )

    assert (
        "/api/v1/vault/documents/"
        "${encodeURIComponent(String(doc.id))}"
        "/download"
        in template
    )


def test_vault_template_uses_same_origin_cookie_auth_and_no_store():
    template = _template_text()

    assert (
        'credentials: "same-origin"'
        in template
    )

    assert (
        'cache: "no-store"'
        in template
    )

    assert (
        '"X-Requested-With": "XMLHttpRequest"'
        in template
    )


def test_vault_upload_uses_idempotency_key_and_server_authoritative_validation():
    template = _template_text()

    assert (
        '"Idempotency-Key": '
        "createIdempotencyKey()"
        in template
    )

    assert (
        "VAULT_CONFIG.maxUploadBytes"
        in template
    )

    assert (
        "La validación de extensión, MIME, "
        "estructura, tamaño y SHA-256"
        in template
    )


def test_vault_template_avoids_rendering_api_document_fields_with_inner_html():
    template = _template_text()

    assert (
        "innerHTML +="
        not in template
    )

    assert (
        "document.createElement"
        in template
    )

    assert (
        ".textContent ="
        in template
    )

    assert (
        "replaceChildren()"
        in template
    )


def test_vault_template_has_loading_empty_error_and_integrity_states():
    template = _template_text()

    assert (
        'id="vaultLoadingState"'
        in template
    )

    assert (
        'id="vaultEmptyState"'
        in template
    )

    assert (
        'id="vaultAlert"'
        in template
    )

    assert (
        "SHA-256"
        in template
    )

    assert (
        "available"
        in template
    )

    assert (
        "upload_failed"
        in template
    )

    assert (
        "delete_failed"
        in template
    )


def test_vault_template_hides_upload_and_delete_controls_by_server_capability():
    template = _template_text()

    assert (
        "{% if vault_can_upload %}"
        in template
    )

    assert (
        "{% if vault_can_delete %}"
        in template
    )

    assert (
        'canUpload: {{ "true" '
        'if vault_can_upload else "false" }}'
        in template
    )

    assert (
        'canDelete: {{ "true" '
        'if vault_can_delete else "false" }}'
        in template
    )


def _install_route_mocks(
    monkeypatch,
    *,
    user: UserTenantContext,
    max_upload_bytes: int,
    captured: dict,
) -> None:
    monkeypatch.setattr(
        web_router_module,
        "get_html_route_user",
        lambda request, required_permission: (
            user,
            None,
        ),
    )

    class Storage:
        pass

    Storage.max_upload_bytes = (
        max_upload_bytes
    )

    class Settings:
        storage = Storage()

    monkeypatch.setattr(
        web_router_module,
        "get_settings",
        lambda: Settings(),
    )

    def _capture(
        request,
        name,
        *,
        user,
        context=None,
        status_code=200,
    ):
        captured["name"] = name
        captured["user"] = user
        captured["context"] = (
            context or {}
        )

        return HTMLResponse(
            "ok",
            status_code=status_code,
        )

    monkeypatch.setattr(
        web_router_module,
        "render_web_template",
        _capture,
    )


def test_render_vault_view_passes_manager_capabilities(
    monkeypatch,
):
    user = _user(
        "manager"
    )

    captured: dict = {}

    _install_route_mocks(
        monkeypatch,
        user=user,
        max_upload_bytes=(
            25 * 1024 * 1024
        ),
        captured=captured,
    )

    response = asyncio.run(
        web_router_module
        .render_vault_view(
            _request()
        )
    )

    assert (
        response.status_code
        == 200
    )

    assert (
        captured["name"]
        == "vault.html"
    )

    assert (
        captured["user"]
        is user
    )

    assert (
        captured["context"][
            "vault_can_upload"
        ]
        is True
    )

    assert (
        captured["context"][
            "vault_can_delete"
        ]
        is False
    )

    assert (
        captured["context"][
            "vault_max_upload_bytes"
        ]
        == 25 * 1024 * 1024
    )

    assert (
        captured["context"][
            "vault_max_upload_mb"
        ]
        == 25.0
    )


def test_render_vault_view_passes_admin_capabilities(
    monkeypatch,
):
    user = _user(
        "admin"
    )

    captured: dict = {}

    _install_route_mocks(
        monkeypatch,
        user=user,
        max_upload_bytes=(
            8 * 1024 * 1024
        ),
        captured=captured,
    )

    response = asyncio.run(
        web_router_module
        .render_vault_view(
            _request()
        )
    )

    assert (
        response.status_code
        == 200
    )

    assert (
        captured["name"]
        == "vault.html"
    )

    assert (
        captured["context"][
            "vault_can_upload"
        ]
        is True
    )

    assert (
        captured["context"][
            "vault_can_delete"
        ]
        is True
    )

    assert (
        captured["context"][
            "vault_max_upload_mb"
        ]
        == 8.0
    )


def test_render_vault_view_passes_auditor_read_only_capabilities(
    monkeypatch,
):
    user = _user(
        "auditor"
    )

    captured: dict = {}

    _install_route_mocks(
        monkeypatch,
        user=user,
        max_upload_bytes=(
            25 * 1024 * 1024
        ),
        captured=captured,
    )

    response = asyncio.run(
        web_router_module
        .render_vault_view(
            _request()
        )
    )

    assert (
        response.status_code
        == 200
    )

    assert (
        captured["name"]
        == "vault.html"
    )

    assert (
        captured["context"][
            "vault_can_upload"
        ]
        is False
    )

    assert (
        captured["context"][
            "vault_can_delete"
        ]
        is False
    )


def test_vault_web_router_still_requires_vault_read():
    route = next(
        route
        for route
        in web_router_module.router.routes
        if (
            getattr(
                route,
                "path",
                None,
            )
            == "/vault"
            and "GET"
            in getattr(
                route,
                "methods",
                set(),
            )
        )
    )

    assert (
        route.endpoint
        is web_router_module.render_vault_view
    )

    assert (
        Permission.VAULT_READ.value
        == "vault:read"
    )


def test_vault_route_is_registered_in_main_application():
    """Verify registration through the public ASGI behavior.

    FastAPI >= 0.137 keeps included routers as lazy _IncludedRouter
    instances inside app.routes, so direct inspection of route.path
    on main.app.routes is no longer a stable registration contract.
    """

    with TestClient(
        main.app,
    ) as client:
        response = client.get(
            "/vault",
            follow_redirects=False,
        )

    assert (
        response.status_code
        == 303
    )

    assert (
        response.headers["location"]
        == "/login"
    )