from __future__ import annotations

import asyncio
import io
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.testclient import TestClient
from openpyxl import Workbook
from starlette.datastructures import UploadFile

import main
import litoral_trace.web.router as web_router_module
from litoral_trace.auth.rbac import Permission
from litoral_trace.services.batch import (
    BATCH_COLUMNAS,
    BATCH_SHEET_NAME,
    BATCH_XLSX_MEDIA_TYPE,
)
from litoral_trace.services.batch_imports import (
    BatchImportConflictError,
    BatchImportIdempotencyConflictError,
    BatchImportPersistenceError,
    BatchImportResult,
)
from litoral_trace.services.batch_queries import (
    BatchImportSnapshot,
)
from litoral_trace.services.batch_evidence import (
    BatchEvidenceConflictError,
    BatchEvidenceError,
    BatchEvidenceLinkResult,
    BatchEvidenceNotFoundError,
    BatchEvidencePersistenceError,
    BatchEvidenceValidationError,
    BatchEvidenceView,
)
from litoral_trace.services.vault import (
    VaultDocumentView,
)
from litoral_trace.web.navigation import build_navigation


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
    / "batch_import.html"
)
UPLOAD_SERVICE_PATH = (
    ROOT
    / "src"
    / "litoral_trace"
    / "services"
    / "batch_upload.py"
)
WEB_MODULE_PATH = (
    ROOT
    / "src"
    / "litoral_trace"
    / "web"
    / "batch_import.py"
)
APP_SHELL_PATH = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
    / "app"
    / "base_app.html"
)
PUBLIC_HOME_PATH = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
    / "public"
    / "home.html"
)
DASHBOARD_TEMPLATE_PATH = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
    / "dashboard.html"
)

_TEST_JWT_SECRET = (
    "p24g-ui-http-acceptance-secret-"
    + ("x" * 64)
)


def _request(
    path: str = "/imports",
    *,
    method: str = "GET",
    content_length: int | None = None,
    query_string: str = "",
) -> Request:
    headers: list[tuple[bytes, bytes]] = []

    if content_length is not None:
        headers.append(
            (
                b"content-length",
                str(content_length).encode(),
            )
        )

    return Request(
        scope={
            "type": "http",
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": query_string.encode(),
            "headers": headers,
            "client": ("127.0.0.1", 50000),
            "server": ("testserver", 80),
            "root_path": "",
        }
    )


def _user(role: str) -> SimpleNamespace:
    return SimpleNamespace(
        user_id=10,
        username=f"{role}_user",
        organization_id=7,
        organization_name="P24G Tenant",
        organization_slug="p24g-tenant",
        role=role,
        email=f"{role}@example.com",
        session_id=99,
        is_platform_superadmin=(role == "superadmin"),
    )


def _template_text() -> str:
    return TEMPLATE_PATH.read_text(encoding="utf-8")


def _source_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _row(
    *,
    identificador="RODAL-001",
    proveedor="30-12345678-9",
    latitud=-27.45,
):
    return [
        identificador,
        proveedor,
        "Madera Aserrada (Pino)",
        50.0,
        latitud,
        -58.90,
        100.0,
        45.0,
    ]


def _xlsx_bytes(*rows) -> bytes:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = BATCH_SHEET_NAME
    worksheet.append(BATCH_COLUMNAS)

    for row in rows:
        worksheet.append(list(row))

    output = io.BytesIO()
    workbook.save(output)
    workbook.close()
    return output.getvalue()


def _upload(
    payload: bytes,
    *,
    filename: str = "batch.xlsx",
) -> UploadFile:
    return UploadFile(
        filename=filename,
        file=io.BytesIO(payload),
        headers={
            "content-type": BATCH_XLSX_MEDIA_TYPE,
        },
    )


class FakeImportService:
    def __init__(self, result: BatchImportResult):
        self.result = result
        self.error = None
        self.calls = 0
        self.kwargs = None

    def import_validated(
        self,
        validation,
        **kwargs,
    ):
        self.calls += 1
        self.kwargs = {
            "validation": validation,
            **kwargs,
        }

        if self.error is not None:
            raise self.error

        return self.result


class FakeQueryService:
    def __init__(
        self,
        snapshot: BatchImportSnapshot | None,
    ):
        self.snapshot = snapshot
        self.error = None
        self.kwargs = None

    def get_by_public_id(
        self,
        **kwargs,
    ):
        self.kwargs = kwargs

        if self.error is not None:
            raise self.error

        return self.snapshot


class FakeEvidenceService:
    def __init__(
        self,
        evidence: tuple[BatchEvidenceView, ...],
    ):
        self.evidence = evidence
        self.error = None
        self.kwargs = None
        self.link_kwargs = None
        self.unlink_kwargs = None
        self.link_error = None
        self.unlink_error = None
        self.link_result = None
        self.unlink_result = None

    def list_evidence(
        self,
        **kwargs,
    ):
        self.kwargs = kwargs

        if self.error is not None:
            raise self.error

        return self.evidence

    def link_evidence(
        self,
        **kwargs,
    ):
        self.link_kwargs = kwargs

        if self.link_error is not None:
            raise self.link_error

        if self.link_result is None:
            raise AssertionError(
                "link_result must be configured for link_evidence()."
            )

        return self.link_result

    def unlink_evidence(
        self,
        **kwargs,
    ):
        self.unlink_kwargs = kwargs

        if self.unlink_error is not None:
            raise self.unlink_error

        if self.unlink_result is None:
            raise AssertionError(
                "unlink_result must be configured for unlink_evidence()."
            )

        return self.unlink_result


class FakeVaultService:
    def __init__(
        self,
        documents: list[VaultDocumentView],
    ):
        self.documents = documents
        self.error = None
        self.kwargs = None
        self.delete_calls = 0

    def list_documents(
        self,
        **kwargs,
    ):
        self.kwargs = kwargs

        if self.error is not None:
            raise self.error

        return self.documents

    def delete_document(
        self,
        *args,
        **kwargs,
    ):
        self.delete_calls += 1
        raise AssertionError(
            "delete_document must not be called by the import evidence UI."
        )


def _result(
    *,
    replayed: bool = False,
) -> BatchImportResult:
    return BatchImportResult(
        organization_id=7,
        total_rows=1,
        inserted_rows=1,
        lote_ids=(501,),
        identifiers=("RODAL-001",),
        import_public_id=uuid4(),
        replayed=replayed,
    )


def _snapshot() -> BatchImportSnapshot:
    now = datetime.now(
        timezone.utc
    )

    return BatchImportSnapshot(
        organization_id=7,
        public_id=uuid4(),
        status="completed",
        source_filename="batch.xlsx",
        source_sha256="a" * 64,
        total_rows=2,
        inserted_rows=2,
        lote_ids=(101, 102),
        identifiers=("RODAL-001", "RODAL-002"),
        created_at=now,
        completed_at=now,
    )


def _evidence_view(
    *,
    status: str = "available",
    evidence_type: str = "SUPPORTING_EVIDENCE",
) -> BatchEvidenceView:
    document_public_id = uuid4()
    return BatchEvidenceView(
        link_internal_id=11,
        link_public_id=uuid4(),
        organization_id=7,
        batch_import_public_id=uuid4(),
        vault_document_public_id=document_public_id,
        evidence_type=evidence_type,
        linked_at=datetime.now(
            timezone.utc
        ),
        linked_by_user_id=10,
        document_filename="evidence.pdf",
        document_type="PDF_CERTIFICADO",
        document_content_type="application/pdf",
        document_size_bytes=1234,
        document_sha256="b" * 64,
        document_status=status,
    )


def _document_view(
    *,
    status: str = "available",
    document_type: str = "PDF_CERTIFICADO",
    filename: str = "evidence.pdf",
) -> VaultDocumentView:
    now = datetime.now(
        timezone.utc
    )
    return VaultDocumentView(
        internal_id=21,
        public_id=uuid4(),
        filename=filename,
        document_type=document_type,
        content_type="application/pdf",
        size_bytes=2048,
        sha256="c" * 64,
        status=status,
        created_at=now,
        updated_at=now,
        deleted_at=None,
    )


def _capture_render(monkeypatch):
    captured: dict[str, object] = {}

    def _render(
        request,
        name,
        *,
        user,
        context=None,
        status_code=200,
    ):
        captured["name"] = name
        captured["user"] = user
        captured["context"] = context or {}
        captured["status_code"] = status_code
        return HTMLResponse("ok", status_code=status_code)

    monkeypatch.setattr(
        web_router_module,
        "render_web_template",
        _render,
    )
    return captured


async def _csrf_ok(*args, **kwargs):
    return None


async def _csrf_invalid(*args, **kwargs):
    raise HTTPException(status_code=403, detail="csrf")


def test_imports_get_and_post_routes_exist():
    paths = {
        (getattr(route, "path", None), tuple(sorted(getattr(route, "methods", set()))))
        for route in web_router_module.router.routes
    }

    assert ("/imports", ("GET",)) in paths
    assert ("/imports", ("POST",)) in paths
    assert ("/imports/validate", ("POST",)) in paths


def test_import_detail_route_exists():
    paths = {
        (getattr(route, "path", None), tuple(sorted(getattr(route, "methods", set()))))
        for route in web_router_module.router.routes
    }

    assert ("/imports/{public_id}", ("GET",)) in paths
    assert ("/imports/{public_id}/evidence", ("POST",)) in paths
    assert (
        "/imports/{public_id}/evidence/{document_id}/unlink",
        ("POST",),
    ) in paths


def test_anonymous_imports_route_redirects_to_login(monkeypatch):
    monkeypatch.setenv("JWT_SECRET_KEY", _TEST_JWT_SECRET)

    with TestClient(main.app, base_url="https://testserver") as client:
        response = client.get("/imports", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/login"


def test_anonymous_import_detail_route_redirects_to_login(monkeypatch):
    monkeypatch.setenv("JWT_SECRET_KEY", _TEST_JWT_SECRET)

    with TestClient(main.app, base_url="https://testserver") as client:
        response = client.get(f"/imports/{uuid4()}", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/login"


def test_imports_route_requires_lote_create_and_issues_server_key(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")

    def _route_user(request, required_permission):
        captured["required_permission"] = required_permission
        return user, None

    monkeypatch.setattr(web_router_module, "get_html_route_user", _route_user)

    response = asyncio.run(web_router_module.render_batch_import_view(_request()))

    workspace = captured["context"]["batch_import_view"]
    assert response.status_code == 200
    assert captured["required_permission"] == Permission.LOTE_CREATE
    assert captured["name"] == "batch_import.html"
    assert captured["user"] is user
    assert workspace.form.idempotency_key
    assert workspace.form.file_accept == ".xlsx"


def test_import_detail_route_requires_lote_read(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("auditor")
    snapshot = _snapshot()

    def _route_user(request, required_permission):
        captured["required_permission"] = required_permission
        return user, None

    monkeypatch.setattr(web_router_module, "get_html_route_user", _route_user)
    monkeypatch.setattr(
        web_router_module,
        "_new_batch_import_query_service",
        lambda: FakeQueryService(snapshot),
    )
    monkeypatch.setattr(
        web_router_module,
        "_new_batch_evidence_service",
        lambda: FakeEvidenceService(tuple()),
    )

    response = asyncio.run(
        web_router_module.render_batch_import_detail_view(
            _request(f"/imports/{snapshot.public_id}"),
            public_id=snapshot.public_id,
        )
    )

    assert response.status_code == 200
    assert captured["required_permission"] == Permission.LOTE_READ


def test_navigation_contains_imports_only_for_roles_with_lote_create():
    admin_nav = build_navigation(_user("admin"), current_path="/imports")
    manager_nav = build_navigation(_user("manager"), current_path="/imports/example")
    auditor_nav = build_navigation(_user("auditor"), current_path="/imports")

    admin_imports = next(item for item in admin_nav if item.key == "imports")
    manager_imports = next(item for item in manager_nav if item.key == "imports")

    assert admin_imports.href == "/imports"
    assert admin_imports.active is True
    assert manager_imports.href == "/imports"
    assert manager_imports.active is True
    assert all(item.key != "imports" for item in auditor_nav)


def test_navigation_keeps_existing_entries_and_superadmin_gets_imports():
    superadmin_nav = build_navigation(_user("superadmin"), current_path="/dashboard")

    assert [(item.key, item.href) for item in superadmin_nav] == [
        ("dashboard", "/dashboard"),
        ("imports", "/imports"),
        ("traceability", "/traceability"),
        ("vault", "/vault"),
        ("settings", "/settings"),
        ("platform", "/admin"),
    ]


def test_batch_import_template_is_multipart_and_xlsx_only():
    template = _template_text()

    assert '{% extends "app/base_app.html" %}' in template
    assert 'enctype="{{ workspace.form.enctype }}"' in template
    assert 'type="file"' in template
    assert 'accept="{{ workspace.form.file_accept }}"' in template
    assert "Importación masiva de lotes" in template
    assert ".xlsx" in template
    assert ".xls," not in template
    assert ".xls " not in template


def test_batch_import_template_carries_csrf_and_idempotency_without_tenant_fields():
    template = _template_text()

    assert 'name="{{ csrf_form_field }}"' in template
    assert 'name="{{ workspace.form.idempotency_field_name }}"' in template
    assert 'name="organization_id"' not in template
    assert 'name="tenant_id"' not in template
    assert 'name="user_id"' not in template


def test_batch_import_template_has_no_legacy_upload_or_external_runtime_or_js():
    template = _template_text()

    assert "/api/v1/batch/upload" not in template
    assert "https://" not in template
    assert "http://" not in template
    assert "<script" not in template.lower()
    assert "fake progress" not in template.lower()
    assert "/api/v1/batch/" not in template


def test_app_shell_has_explicit_imports_icon_support():
    shell = APP_SHELL_PATH.read_text(encoding="utf-8")

    assert '{% elif item.key == "imports" %}' in shell
    assert "fa-solid fa-file-arrow-up" in shell


def test_public_navigation_and_public_templates_are_not_affected():
    public_home = PUBLIC_HOME_PATH.read_text(encoding="utf-8")

    assert "data-app-drawer" not in public_home
    assert "/imports" not in public_home
    assert "user.organization_name" not in public_home


def test_neutral_upload_service_contains_no_fastapi_starlette_or_jinja_imports():
    source = _source_text(
        UPLOAD_SERVICE_PATH
    )

    assert "from fastapi" not in source
    assert "import fastapi" not in source
    assert "from starlette" not in source
    assert "import starlette" not in source
    assert "jinja2" not in source


def test_dashboard_template_no_longer_exposes_retired_batch_upload_ui():
    dashboard = DASHBOARD_TEMPLATE_PATH.read_text(encoding="utf-8")

    assert "/api/v1/batch/upload" not in dashboard
    assert 'accept=".xlsx,.xls"' not in dashboard
    assert "Stress Test & Carga Masiva Batch" not in dashboard
    assert "Procesamiento Masivo de Guias Forestales y Remitos" not in dashboard
    assert "batchResult" not in dashboard
    assert "btn-tab-batch" not in dashboard
    assert 'id="tab-batch"' not in dashboard
    assert "Plantilla Excel" in dashboard
    assert "Mapa geoespacial de rodales" in dashboard
    assert "Evaluación individual del rodal" in dashboard


def test_web_batch_import_module_no_longer_imports_api_batch():
    source = _source_text(
        WEB_MODULE_PATH
    )

    assert "litoral_trace.api.batch" not in source
    assert "batch_api._parse_upload" not in source
    assert "batch_api." not in source


def test_validate_post_rejects_invalid_csrf(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_invalid)
    monkeypatch.setattr(
        web_router_module,
        "render_csrf_failure",
        lambda: HTMLResponse("csrf", status_code=403),
    )

    response = asyncio.run(
        web_router_module.validate_batch_import_view(
            _request("/imports/validate", method="POST"),
            file=_upload(_xlsx_bytes(_row())),
            idempotency_key="browser-key",
        )
    )

    assert response.status_code == 403
    assert captured == {}


def test_import_post_rejects_invalid_csrf(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_invalid)
    monkeypatch.setattr(
        web_router_module,
        "render_csrf_failure",
        lambda: HTMLResponse("csrf", status_code=403),
    )

    response = asyncio.run(
        web_router_module.submit_batch_import_view(
            _request("/imports", method="POST"),
            file=_upload(_xlsx_bytes(_row())),
            idempotency_key="browser-key",
        )
    )

    assert response.status_code == 403
    assert captured == {}


def test_validation_post_does_not_invoke_persistence_and_renders_safe_row_errors(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")
    service_called = {"value": False}

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
    monkeypatch.setattr(
        web_router_module,
        "_new_batch_import_service",
        lambda: service_called.__setitem__("value", True),
    )

    response = asyncio.run(
        web_router_module.validate_batch_import_view(
            _request("/imports/validate", method="POST"),
            file=_upload(_xlsx_bytes(_row(latitud=999.0))),
            idempotency_key="browser-key",
        )
    )

    workspace = captured["context"]["batch_import_view"]
    assert response.status_code == 200
    assert service_called["value"] is False
    assert workspace.validation is not None
    assert workspace.validation.valid is False
    assert workspace.validation.row_errors
    first_error = workspace.validation.row_errors[0]
    assert first_error.row >= 2
    assert first_error.field
    assert first_error.code
    assert first_error.message
    assert workspace.result is None


def test_validation_post_renders_meaningful_valid_state(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)

    response = asyncio.run(
        web_router_module.validate_batch_import_view(
            _request("/imports/validate", method="POST"),
            file=_upload(_xlsx_bytes(_row())),
            idempotency_key="browser-key",
        )
    )

    workspace = captured["context"]["batch_import_view"]
    assert response.status_code == 200
    assert workspace.validation is not None
    assert workspace.validation.valid is True
    assert workspace.validation.total_rows == 1
    assert workspace.validation.valid_rows == 1
    assert workspace.form.requires_reupload is True


def test_validation_post_wrong_extension_renders_safe_html(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")
    service_called = {"value": False}

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
    monkeypatch.setattr(
        web_router_module,
        "_new_batch_import_service",
        lambda: service_called.__setitem__("value", True),
    )

    response = asyncio.run(
        web_router_module.validate_batch_import_view(
            _request("/imports/validate", method="POST"),
            file=_upload(
                b"legacy-content",
                filename="legacy.xls",
            ),
            idempotency_key="browser-key",
        )
    )

    workspace = captured["context"]["batch_import_view"]
    assert response.status_code == 400
    assert service_called["value"] is False
    assert workspace.alert is not None
    assert workspace.alert.code == "UNSUPPORTED_FILE_TYPE"
    assert "Traceback" not in workspace.alert.message
    assert "HTTPException" not in workspace.alert.message


def test_validation_post_malformed_xlsx_renders_safe_html(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)

    response = asyncio.run(
        web_router_module.validate_batch_import_view(
            _request("/imports/validate", method="POST"),
            file=_upload(b"PKnot-a-real-xlsx"),
            idempotency_key="browser-key",
        )
    )

    workspace = captured["context"]["batch_import_view"]
    assert response.status_code == 400
    assert workspace.alert is not None
    assert workspace.alert.code in {
        "INVALID_XLSX_CONTAINER",
        "INVALID_WORKBOOK",
    }
    assert "Traceback" not in workspace.alert.message
    assert '"code"' not in workspace.alert.message


def test_validation_post_formula_rejection_renders_safe_html(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")
    formula_row = _row()
    formula_row[3] = "=25+25"

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)

    response = asyncio.run(
        web_router_module.validate_batch_import_view(
            _request("/imports/validate", method="POST"),
            file=_upload(_xlsx_bytes(formula_row)),
            idempotency_key="browser-key",
        )
    )

    workspace = captured["context"]["batch_import_view"]
    assert response.status_code == 422
    assert workspace.alert is not None
    assert workspace.alert.code == "FORMULA_NOT_ALLOWED"
    assert workspace.alert.title == "Planilla invalida"


def test_validation_post_empty_file_renders_safe_html(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)

    response = asyncio.run(
        web_router_module.validate_batch_import_view(
            _request("/imports/validate", method="POST"),
            file=_upload(b""),
            idempotency_key="browser-key",
        )
    )

    workspace = captured["context"]["batch_import_view"]
    assert response.status_code == 400
    assert workspace.alert is not None
    assert workspace.alert.code == "EMPTY_FILE"
    assert "Traceback" not in workspace.alert.message


def test_import_post_invokes_persistence_once_for_new_submission(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")
    service = FakeImportService(_result())

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
    monkeypatch.setattr(web_router_module, "_new_batch_import_service", lambda: service)

    response = asyncio.run(
        web_router_module.submit_batch_import_view(
            _request("/imports", method="POST"),
            file=_upload(_xlsx_bytes(_row())),
            idempotency_key="browser-key",
        )
    )

    workspace = captured["context"]["batch_import_view"]
    assert response.status_code == 201
    assert service.calls == 1
    assert service.kwargs["organization_id"] == user.organization_id
    assert service.kwargs["idempotency_key"] == "browser-key"
    assert workspace.result.code == "IMPORT_CREATED"
    assert workspace.result.replayed is False
    assert workspace.result.detail_href == f"/imports/{workspace.result.import_id}"


def test_import_post_replay_renders_distinct_state(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")
    service = FakeImportService(_result(replayed=True))

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
    monkeypatch.setattr(web_router_module, "_new_batch_import_service", lambda: service)

    response = asyncio.run(
        web_router_module.submit_batch_import_view(
            _request("/imports", method="POST"),
            file=_upload(_xlsx_bytes(_row())),
            idempotency_key="browser-key",
        )
    )

    workspace = captured["context"]["batch_import_view"]
    assert response.status_code == 200
    assert workspace.result.code == "IMPORT_REPLAYED"
    assert workspace.result.replayed is True
    assert workspace.result.detail_href == f"/imports/{workspace.result.import_id}"


def test_import_post_semantic_validation_failure_does_not_persist(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")
    service = FakeImportService(_result())

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
    monkeypatch.setattr(web_router_module, "_new_batch_import_service", lambda: service)

    response = asyncio.run(
        web_router_module.submit_batch_import_view(
            _request("/imports", method="POST"),
            file=_upload(_xlsx_bytes(_row(latitud=999.0))),
            idempotency_key="browser-key",
        )
    )

    workspace = captured["context"]["batch_import_view"]
    assert response.status_code == 422
    assert service.calls == 0
    assert workspace.validation is not None
    assert workspace.result.code == "ROW_VALIDATION_FAILED"


def test_import_post_duplicate_lote_conflict_renders_distinct_state(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")
    service = FakeImportService(_result())
    service.error = BatchImportConflictError(("RODAL-001", "RODAL-002"))

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
    monkeypatch.setattr(web_router_module, "_new_batch_import_service", lambda: service)

    response = asyncio.run(
        web_router_module.submit_batch_import_view(
            _request("/imports", method="POST"),
            file=_upload(_xlsx_bytes(_row())),
            idempotency_key="browser-key",
        )
    )

    workspace = captured["context"]["batch_import_view"]
    assert response.status_code == 409
    assert workspace.result.code == "DUPLICATE_LOTE_IDENTIFIERS"
    assert workspace.result.duplicate_identifiers == ("RODAL-001", "RODAL-002")


def test_import_post_idempotency_conflict_renders_distinct_state(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")
    service = FakeImportService(_result())
    service.error = BatchImportIdempotencyConflictError(uuid4())

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
    monkeypatch.setattr(web_router_module, "_new_batch_import_service", lambda: service)

    response = asyncio.run(
        web_router_module.submit_batch_import_view(
            _request("/imports", method="POST"),
            file=_upload(_xlsx_bytes(_row())),
            idempotency_key="browser-key",
        )
    )

    workspace = captured["context"]["batch_import_view"]
    assert response.status_code == 409
    assert workspace.result.code == "IDEMPOTENCY_CONFLICT"
    assert workspace.result.import_id is not None
    assert workspace.form.idempotency_key != "browser-key"


def test_import_post_persistence_failure_produces_safe_operator_output(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")
    service = FakeImportService(_result())
    service.error = BatchImportPersistenceError("db down")

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
    monkeypatch.setattr(web_router_module, "_new_batch_import_service", lambda: service)

    response = asyncio.run(
        web_router_module.submit_batch_import_view(
            _request("/imports", method="POST"),
            file=_upload(_xlsx_bytes(_row())),
            idempotency_key="browser-key",
        )
    )

    workspace = captured["context"]["batch_import_view"]
    assert response.status_code == 503
    assert workspace.result.code == "SERVICE_UNAVAILABLE"
    assert "db down" not in workspace.result.message.lower()


def test_import_detail_visible_snapshot_renders_safe_viewmodel(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("auditor")
    snapshot = _snapshot()
    query_service = FakeQueryService(snapshot)
    evidence_service = FakeEvidenceService((_evidence_view(),))

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "_new_batch_import_query_service", lambda: query_service)
    monkeypatch.setattr(web_router_module, "_new_batch_evidence_service", lambda: evidence_service)

    response = asyncio.run(
        web_router_module.render_batch_import_detail_view(
            _request(f"/imports/{snapshot.public_id}"),
            public_id=snapshot.public_id,
        )
    )

    page = captured["context"]["batch_import_detail_view"]
    assert response.status_code == 200
    assert query_service.kwargs["organization_id"] == user.organization_id
    assert page.detail is not None
    assert page.detail.public_id == str(snapshot.public_id)
    assert page.detail.source_filename == "batch.xlsx"
    assert page.detail.status == "completed"
    assert page.detail.total_rows == 2
    assert page.detail.inserted_rows == 2
    assert page.detail.identifiers == snapshot.identifiers


def test_import_detail_not_found_and_cross_tenant_invisible_share_safe_404(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("auditor")
    public_id = uuid4()

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(
        web_router_module,
        "_new_batch_import_query_service",
        lambda: FakeQueryService(None),
    )

    response = asyncio.run(
        web_router_module.render_batch_import_detail_view(
            _request(f"/imports/{public_id}"),
            public_id=public_id,
        )
    )

    page = captured["context"]["batch_import_detail_view"]
    assert response.status_code == 404
    assert page.not_found is True
    assert page.detail is None


def test_import_detail_with_vault_read_shows_evidence(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("auditor")
    snapshot = _snapshot()
    evidence = _evidence_view()
    evidence_service = FakeEvidenceService((evidence,))

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "_new_batch_import_query_service", lambda: FakeQueryService(snapshot))
    monkeypatch.setattr(web_router_module, "_new_batch_evidence_service", lambda: evidence_service)

    response = asyncio.run(
        web_router_module.render_batch_import_detail_view(
            _request(f"/imports/{snapshot.public_id}"),
            public_id=snapshot.public_id,
        )
    )

    page = captured["context"]["batch_import_detail_view"]
    assert response.status_code == 200
    assert page.can_view_evidence is True
    assert evidence_service.kwargs["batch_import_id"] == snapshot.public_id
    assert page.evidence_items[0].document_filename == "evidence.pdf"
    assert page.evidence_items[0].document_type == "PDF_CERTIFICADO"
    assert page.evidence_items[0].document_available is True


def test_import_detail_without_vault_read_omits_evidence_metadata(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("auditor")
    snapshot = _snapshot()
    evidence_service = FakeEvidenceService((_evidence_view(),))

    def _has_permission(subject, permission):
        if permission == Permission.VAULT_READ:
            return False
        return True

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "has_permission", _has_permission)
    monkeypatch.setattr(web_router_module, "_new_batch_import_query_service", lambda: FakeQueryService(snapshot))
    monkeypatch.setattr(web_router_module, "_new_batch_evidence_service", lambda: evidence_service)

    response = asyncio.run(
        web_router_module.render_batch_import_detail_view(
            _request(f"/imports/{snapshot.public_id}"),
            public_id=snapshot.public_id,
        )
    )

    page = captured["context"]["batch_import_detail_view"]
    assert response.status_code == 200
    assert page.can_view_evidence is False
    assert page.evidence_items == ()
    assert evidence_service.kwargs is None


def test_import_detail_tombstone_evidence_renders_unavailable_state(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("auditor")
    snapshot = _snapshot()
    evidence = _evidence_view(status="deleted")

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "_new_batch_import_query_service", lambda: FakeQueryService(snapshot))
    monkeypatch.setattr(
        web_router_module,
        "_new_batch_evidence_service",
        lambda: FakeEvidenceService((evidence,)),
    )

    response = asyncio.run(
        web_router_module.render_batch_import_detail_view(
            _request(f"/imports/{snapshot.public_id}"),
            public_id=snapshot.public_id,
        )
    )

    page = captured["context"]["batch_import_detail_view"]
    assert response.status_code == 200
    assert page.evidence_items[0].document_available is False
    assert page.evidence_items[0].document_status == "deleted"


def test_import_detail_authorized_empty_evidence_state(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("auditor")
    snapshot = _snapshot()

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "_new_batch_import_query_service", lambda: FakeQueryService(snapshot))
    monkeypatch.setattr(
        web_router_module,
        "_new_batch_evidence_service",
        lambda: FakeEvidenceService(tuple()),
    )

    response = asyncio.run(
        web_router_module.render_batch_import_detail_view(
            _request(f"/imports/{snapshot.public_id}"),
            public_id=snapshot.public_id,
        )
    )

    page = captured["context"]["batch_import_detail_view"]
    assert response.status_code == 200
    assert page.can_view_evidence is True
    assert page.evidence_items == ()


def test_import_detail_evidence_failure_renders_safe_operator_state(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("auditor")
    snapshot = _snapshot()
    evidence_service = FakeEvidenceService(tuple())
    evidence_service.error = BatchEvidenceError("down")

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "_new_batch_import_query_service", lambda: FakeQueryService(snapshot))
    monkeypatch.setattr(web_router_module, "_new_batch_evidence_service", lambda: evidence_service)

    response = asyncio.run(
        web_router_module.render_batch_import_detail_view(
            _request(f"/imports/{snapshot.public_id}"),
            public_id=snapshot.public_id,
        )
    )

    page = captured["context"]["batch_import_detail_view"]
    assert response.status_code == 503
    assert page.detail is not None
    assert page.evidence_error is not None
    assert "Traceback" not in page.evidence_error.message


def test_detail_template_hides_storage_coordinates_and_has_html_forms_only():
    detail_template = (
        ROOT
        / "src"
        / "litoral_trace"
        / "templates"
        / "batch_import_detail.html"
    ).read_text(encoding="utf-8")

    assert "object_key" not in detail_template
    assert "storage_bucket" not in detail_template
    assert "storage_backend" not in detail_template
    assert "/api/v1/batch/" not in detail_template
    assert "Vincular documento" in detail_template
    assert "Desvincular evidencia" in detail_template
    assert 'method="post"' in detail_template
    assert "<script" not in detail_template.lower()


def test_import_detail_with_lote_update_and_vault_read_shows_link_controls(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")
    snapshot = _snapshot()
    evidence = _evidence_view()
    evidence_service = FakeEvidenceService((evidence,))
    vault_service = FakeVaultService([
        _document_view(filename="available-a.pdf"),
        _document_view(status="deleted", filename="deleted.pdf"),
        _document_view(status="upload_failed", filename="failed.pdf"),
    ])

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "_new_batch_import_query_service", lambda: FakeQueryService(snapshot))
    monkeypatch.setattr(web_router_module, "_new_batch_evidence_service", lambda: evidence_service)
    monkeypatch.setattr(web_router_module, "_new_vault_service", lambda: vault_service)

    response = asyncio.run(
        web_router_module.render_batch_import_detail_view(
            _request(f"/imports/{snapshot.public_id}", query_string="evidence_result=linked"),
            public_id=snapshot.public_id,
        )
    )

    page = captured["context"]["batch_import_detail_view"]
    assert response.status_code == 200
    assert page.can_manage_evidence is True
    assert vault_service.kwargs["organization_id"] == user.organization_id
    assert [choice.filename for choice in page.evidence_form.document_choices] == ["available-a.pdf"]
    assert {choice.value for choice in page.evidence_form.evidence_type_choices} == {
        "SOURCE_WORKBOOK",
        "SUPPORTING_EVIDENCE",
        "COMPLIANCE_EVIDENCE",
    }
    assert page.page_message is not None
    assert page.page_message.code == "EVIDENCE_LINKED"
    assert page.evidence_items[0].can_unlink is True
    assert page.evidence_items[0].unlink_action.endswith(
        f"/evidence/{page.evidence_items[0].vault_document_public_id}/unlink"
    )


def test_import_detail_without_lote_update_hides_mutation_controls(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("auditor")
    snapshot = _snapshot()

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "_new_batch_import_query_service", lambda: FakeQueryService(snapshot))
    monkeypatch.setattr(web_router_module, "_new_batch_evidence_service", lambda: FakeEvidenceService((_evidence_view(),)))

    response = asyncio.run(
        web_router_module.render_batch_import_detail_view(
            _request(f"/imports/{snapshot.public_id}"),
            public_id=snapshot.public_id,
        )
    )

    page = captured["context"]["batch_import_detail_view"]
    assert response.status_code == 200
    assert page.can_view_evidence is True
    assert page.can_manage_evidence is False
    assert page.evidence_form is None


def test_import_detail_tombstone_does_not_offer_unlink(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")
    snapshot = _snapshot()
    evidence = _evidence_view(status="deleted")
    vault_service = FakeVaultService([_document_view()])

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "_new_batch_import_query_service", lambda: FakeQueryService(snapshot))
    monkeypatch.setattr(web_router_module, "_new_batch_evidence_service", lambda: FakeEvidenceService((evidence,)))
    monkeypatch.setattr(web_router_module, "_new_vault_service", lambda: vault_service)

    response = asyncio.run(
        web_router_module.render_batch_import_detail_view(
            _request(f"/imports/{snapshot.public_id}"),
            public_id=snapshot.public_id,
        )
    )

    page = captured["context"]["batch_import_detail_view"]
    assert response.status_code == 200
    assert page.evidence_items[0].document_available is False
    assert page.evidence_items[0].can_unlink is False


def test_link_post_requires_csrf(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")
    snapshot = _snapshot()

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_invalid)
    monkeypatch.setattr(web_router_module, "render_csrf_failure", lambda: HTMLResponse("csrf", status_code=403))
    monkeypatch.setattr(web_router_module, "_new_batch_import_query_service", lambda: FakeQueryService(snapshot))

    response = asyncio.run(
        web_router_module.link_batch_import_evidence_view(
            _request(f"/imports/{snapshot.public_id}/evidence", method="POST"),
            public_id=snapshot.public_id,
            document_id=str(uuid4()),
            evidence_type="SUPPORTING_EVIDENCE",
        )
    )

    assert response.status_code == 403
    assert captured == {}


def test_unlink_post_requires_csrf(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")
    snapshot = _snapshot()

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_invalid)
    monkeypatch.setattr(web_router_module, "render_csrf_failure", lambda: HTMLResponse("csrf", status_code=403))
    monkeypatch.setattr(web_router_module, "_new_batch_import_query_service", lambda: FakeQueryService(snapshot))

    response = asyncio.run(
        web_router_module.unlink_batch_import_evidence_view(
            _request(f"/imports/{snapshot.public_id}/evidence/{uuid4()}/unlink", method="POST"),
            public_id=snapshot.public_id,
            document_id=uuid4(),
        )
    )

    assert response.status_code == 403
    assert captured == {}


def test_link_post_requires_lote_update(monkeypatch):
    captured = {}
    user = _user("admin")
    public_id = uuid4()

    def _route_user(request, required_permission):
        captured["required_permission"] = required_permission
        return user, None

    monkeypatch.setattr(web_router_module, "get_html_route_user", _route_user)
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
    service = FakeEvidenceService(tuple())
    service.link_result = BatchEvidenceLinkResult(
        evidence=_evidence_view(),
        replayed=False,
    )
    monkeypatch.setattr(web_router_module, "_new_batch_evidence_service", lambda: service)

    response = asyncio.run(
        web_router_module.link_batch_import_evidence_view(
            _request(f"/imports/{public_id}/evidence", method="POST"),
            public_id=public_id,
            document_id=str(uuid4()),
            evidence_type="SUPPORTING_EVIDENCE",
        )
    )

    assert response.status_code == 303
    assert captured["required_permission"] == Permission.LOTE_UPDATE


def test_unlink_post_requires_lote_update(monkeypatch):
    captured = {}
    user = _user("admin")
    public_id = uuid4()
    document_id = uuid4()

    def _route_user(request, required_permission):
        captured["required_permission"] = required_permission
        return user, None

    monkeypatch.setattr(web_router_module, "get_html_route_user", _route_user)
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
    service = FakeEvidenceService(tuple())
    service.unlink_result = _evidence_view()
    monkeypatch.setattr(web_router_module, "_new_batch_evidence_service", lambda: service)

    response = asyncio.run(
        web_router_module.unlink_batch_import_evidence_view(
            _request(f"/imports/{public_id}/evidence/{document_id}/unlink", method="POST"),
            public_id=public_id,
            document_id=document_id,
        )
    )

    assert response.status_code == 303
    assert captured["required_permission"] == Permission.LOTE_UPDATE


def test_link_and_unlink_post_require_vault_read_server_side(monkeypatch):
    captured = {}
    user = _user("admin")

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "has_permission", lambda subject, permission: permission != Permission.VAULT_READ)
    monkeypatch.setattr(web_router_module, "render_access_denied", lambda: HTMLResponse("denied", status_code=403))

    link_response = asyncio.run(
        web_router_module.link_batch_import_evidence_view(
            _request(f"/imports/{uuid4()}/evidence", method="POST"),
            public_id=uuid4(),
            document_id=str(uuid4()),
            evidence_type="SUPPORTING_EVIDENCE",
        )
    )
    unlink_response = asyncio.run(
        web_router_module.unlink_batch_import_evidence_view(
            _request(f"/imports/{uuid4()}/evidence/{uuid4()}/unlink", method="POST"),
            public_id=uuid4(),
            document_id=uuid4(),
        )
    )

    assert link_response.status_code == 403
    assert unlink_response.status_code == 403
    assert captured == {}


def test_link_post_success_uses_authenticated_org_and_prg(monkeypatch):
    user = _user("admin")
    public_id = uuid4()
    document_id = uuid4()
    service = FakeEvidenceService(tuple())
    service.link_result = BatchEvidenceLinkResult(
        evidence=_evidence_view(),
        replayed=False,
    )

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
    monkeypatch.setattr(web_router_module, "_new_batch_evidence_service", lambda: service)

    response = asyncio.run(
        web_router_module.link_batch_import_evidence_view(
            _request(f"/imports/{public_id}/evidence", method="POST"),
            public_id=public_id,
            document_id=str(document_id),
            evidence_type="SUPPORTING_EVIDENCE",
        )
    )

    assert response.status_code == 303
    assert response.headers["location"] == f"/imports/{public_id}?evidence_result=linked"
    assert service.link_kwargs["organization_id"] == user.organization_id
    assert service.link_kwargs["batch_import_id"] == public_id
    assert service.link_kwargs["vault_document_id"] == str(document_id)
    assert service.link_kwargs["evidence_type"] == "SUPPORTING_EVIDENCE"


def test_link_post_replay_redirects_without_duplicate_semantics(monkeypatch):
    user = _user("admin")
    public_id = uuid4()
    service = FakeEvidenceService(tuple())
    service.link_result = BatchEvidenceLinkResult(
        evidence=_evidence_view(),
        replayed=True,
    )

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
    monkeypatch.setattr(web_router_module, "_new_batch_evidence_service", lambda: service)

    response = asyncio.run(
        web_router_module.link_batch_import_evidence_view(
            _request(f"/imports/{public_id}/evidence", method="POST"),
            public_id=public_id,
            document_id=str(uuid4()),
            evidence_type="SUPPORTING_EVIDENCE",
        )
    )

    assert response.status_code == 303
    assert response.headers["location"] == f"/imports/{public_id}?evidence_result=replayed"


def test_link_post_invalid_evidence_type_renders_safe_feedback(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")
    snapshot = _snapshot()
    service = FakeEvidenceService((_evidence_view(),))
    service.link_error = BatchEvidenceValidationError(
        "INVALID_EVIDENCE_TYPE",
        "unsafe details",
    )
    vault_service = FakeVaultService([_document_view()])

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
    monkeypatch.setattr(web_router_module, "_new_batch_import_query_service", lambda: FakeQueryService(snapshot))
    monkeypatch.setattr(web_router_module, "_new_batch_evidence_service", lambda: service)
    monkeypatch.setattr(web_router_module, "_new_vault_service", lambda: vault_service)

    response = asyncio.run(
        web_router_module.link_batch_import_evidence_view(
            _request(f"/imports/{snapshot.public_id}/evidence", method="POST"),
            public_id=snapshot.public_id,
            document_id=str(uuid4()),
            evidence_type="NOT_ALLOWED",
        )
    )

    page = captured["context"]["batch_import_detail_view"]
    assert response.status_code == 422
    assert page.page_message.code == "INVALID_EVIDENCE_TYPE"
    assert "unsafe details" not in page.page_message.message


def test_link_post_source_workbook_conflicts_render_safe_feedback(monkeypatch):
    user = _user("admin")
    snapshot = _snapshot()
    vault_service = FakeVaultService([_document_view(document_type="REMITO_EXCEL")])

    for code in (
        "SOURCE_WORKBOOK_REQUIRES_REMITO_EXCEL",
        "SOURCE_WORKBOOK_HASH_MISMATCH",
    ):
        captured = _capture_render(monkeypatch)
        service = FakeEvidenceService((_evidence_view(),))
        service.link_error = BatchEvidenceConflictError(
            code,
            "unsafe details",
        )

        monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
        monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
        monkeypatch.setattr(web_router_module, "_new_batch_import_query_service", lambda: FakeQueryService(snapshot))
        monkeypatch.setattr(web_router_module, "_new_batch_evidence_service", lambda: service)
        monkeypatch.setattr(web_router_module, "_new_vault_service", lambda: vault_service)

        response = asyncio.run(
            web_router_module.link_batch_import_evidence_view(
                _request(f"/imports/{snapshot.public_id}/evidence", method="POST"),
                public_id=snapshot.public_id,
                document_id=str(uuid4()),
                evidence_type="SOURCE_WORKBOOK",
            )
        )

        page = captured["context"]["batch_import_detail_view"]
        assert response.status_code == 409
        assert page.page_message.code == code
        assert "unsafe details" not in page.page_message.message


def test_link_post_not_found_does_not_leak_tenant_details(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")
    snapshot = _snapshot()
    service = FakeEvidenceService((_evidence_view(),))
    service.link_error = BatchEvidenceNotFoundError(
        "VAULT_DOCUMENT_NOT_FOUND",
        "cross-tenant leak",
    )
    vault_service = FakeVaultService([_document_view()])

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
    monkeypatch.setattr(web_router_module, "_new_batch_import_query_service", lambda: FakeQueryService(snapshot))
    monkeypatch.setattr(web_router_module, "_new_batch_evidence_service", lambda: service)
    monkeypatch.setattr(web_router_module, "_new_vault_service", lambda: vault_service)

    response = asyncio.run(
        web_router_module.link_batch_import_evidence_view(
            _request(f"/imports/{snapshot.public_id}/evidence", method="POST"),
            public_id=snapshot.public_id,
            document_id=str(uuid4()),
            evidence_type="SUPPORTING_EVIDENCE",
        )
    )

    page = captured["context"]["batch_import_detail_view"]
    assert response.status_code == 404
    assert page.page_message.code == "VAULT_DOCUMENT_NOT_FOUND"
    assert "cross-tenant" not in page.page_message.message.lower()


def test_link_post_persistence_failure_renders_safe_503(monkeypatch):
    captured = _capture_render(monkeypatch)
    user = _user("admin")
    snapshot = _snapshot()
    service = FakeEvidenceService((_evidence_view(),))
    service.link_error = BatchEvidencePersistenceError(
        "db details"
    )
    vault_service = FakeVaultService([_document_view()])

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
    monkeypatch.setattr(web_router_module, "_new_batch_import_query_service", lambda: FakeQueryService(snapshot))
    monkeypatch.setattr(web_router_module, "_new_batch_evidence_service", lambda: service)
    monkeypatch.setattr(web_router_module, "_new_vault_service", lambda: vault_service)

    response = asyncio.run(
        web_router_module.link_batch_import_evidence_view(
            _request(f"/imports/{snapshot.public_id}/evidence", method="POST"),
            public_id=snapshot.public_id,
            document_id=str(uuid4()),
            evidence_type="SUPPORTING_EVIDENCE",
        )
    )

    page = captured["context"]["batch_import_detail_view"]
    assert response.status_code == 503
    assert page.page_message.code == "BATCH_EVIDENCE_UNAVAILABLE"
    assert "db details" not in page.page_message.message.lower()


def test_unlink_post_uses_authenticated_tenant_and_never_deletes_vault(monkeypatch):
    user = _user("admin")
    snapshot = _snapshot()
    document_id = uuid4()
    service = FakeEvidenceService(tuple())
    service.unlink_result = _evidence_view()
    vault_service = FakeVaultService([_document_view()])

    monkeypatch.setattr(web_router_module, "get_html_route_user", lambda request, required_permission: (user, None))
    monkeypatch.setattr(web_router_module, "enforce_csrf", _csrf_ok)
    monkeypatch.setattr(web_router_module, "_new_batch_evidence_service", lambda: service)
    monkeypatch.setattr(web_router_module, "_new_vault_service", lambda: vault_service)

    response = asyncio.run(
        web_router_module.unlink_batch_import_evidence_view(
            _request(f"/imports/{snapshot.public_id}/evidence/{document_id}/unlink", method="POST"),
            public_id=snapshot.public_id,
            document_id=document_id,
        )
    )

    assert response.status_code == 303
    assert response.headers["location"] == f"/imports/{snapshot.public_id}?evidence_result=unlinked"
    assert service.unlink_kwargs["organization_id"] == user.organization_id
    assert service.unlink_kwargs["batch_import_id"] == snapshot.public_id
    assert service.unlink_kwargs["vault_document_id"] == document_id
    assert vault_service.delete_calls == 0


def test_detail_template_uses_public_ids_and_no_authoritative_tenant_fields():
    detail_template = (
        ROOT
        / "src"
        / "litoral_trace"
        / "templates"
        / "batch_import_detail.html"
    ).read_text(encoding="utf-8")

    assert 'name="organization_id"' not in detail_template
    assert 'name="tenant_id"' not in detail_template
    assert 'name="user_id"' not in detail_template
    assert "internal_id" not in detail_template


def test_imports_remains_the_only_canonical_browser_batch_workflow():
    template = _template_text()
    dashboard = DASHBOARD_TEMPLATE_PATH.read_text(encoding="utf-8")
    navigation = build_navigation(_user("admin"), current_path="/imports")

    assert 'action="{{ workspace.form.validate_action }}"' in template
    assert 'action="{{ workspace.form.import_action }}"' in template
    assert 'workspace.form.validate_action' in template
    assert 'workspace.form.import_action' in template
    assert "/api/v1/batch/upload" not in template
    assert "/api/v1/batch/upload" not in dashboard
    assert any(item.href == "/imports" for item in navigation)
