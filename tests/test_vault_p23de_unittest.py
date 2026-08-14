from __future__ import annotations

import asyncio
import io
import json
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException
from starlette.datastructures import UploadFile
from starlette.requests import Request

import litoral_trace.api.vault as vault_api
from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import (
    Permission,
    ensure_permission,
    permissions_for_role,
)
from litoral_trace.services.audit import (
    AuditAction,
    SENSITIVE_METADATA_KEYS,
    sanitize_audit_detail,
    sanitize_audit_metadata,
)
from litoral_trace.services.vault import (
    VaultConflictError,
    VaultDocumentView,
    VaultIntegrityError,
    VaultNotFoundError,
    VaultValidationError,
    VerifiedVaultDownload,
)


def _request(path: str, method: str = "GET") -> Request:
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": method,
            "scheme": "https",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": [
                (b"x-request-id", b"p23de-request"),
                (b"user-agent", b"p23de-test/1.0"),
            ],
            "client": ("203.0.113.20", 50000),
            "server": ("test", 443),
            "root_path": "",
        }
    )


def _user(role: str = "admin", organization_id: int = 1) -> UserTenantContext:
    return UserTenantContext(
        user_id=10,
        username=f"{role}_user",
        organization_id=organization_id,
        organization_name=f"Org {organization_id}",
        organization_slug=f"org-{organization_id}",
        role=role,
        email=f"{role}@example.com",
    )


def _document(*, status: str = "available") -> VaultDocumentView:
    now = datetime.now(timezone.utc)
    return VaultDocumentView(
        internal_id=700,
        public_id=uuid4(),
        filename='Certificado Ñ "2026".pdf',
        document_type="PDF_CERTIFICADO",
        content_type="application/pdf",
        size_bytes=15,
        sha256="a" * 64,
        status=status,
        created_at=now,
        updated_at=now,
        deleted_at=(now if status == "deleted" else None),
    )


class FakeService:
    def __init__(self, document: VaultDocumentView):
        self.document = document
        self.upload_kwargs = None
        self.list_kwargs = None
        self.get_kwargs = None
        self.delete_kwargs = None
        self.download_error = None
        self.upload_error = None
        self.delete_error = None

    def upload_document(self, **kwargs):
        self.upload_kwargs = kwargs
        if self.upload_error:
            raise self.upload_error
        return self.document

    def list_documents(self, **kwargs):
        self.list_kwargs = kwargs
        return [self.document]

    def get_document(self, **kwargs):
        self.get_kwargs = kwargs
        if str(kwargs["document_id"]) != str(self.document.public_id):
            raise VaultNotFoundError("not found")
        return self.document

    def materialize_verified_download(self, **kwargs):
        if self.download_error:
            raise self.download_error
        return VerifiedVaultDownload(
            document=self.document,
            fileobj=io.BytesIO(b"%PDF-1.4\n%%EOF"),
        )

    def delete_document(self, **kwargs):
        self.delete_kwargs = kwargs
        if self.delete_error:
            raise self.delete_error
        return _document(status="deleted")


def _capture_audit(monkeypatch):
    events = []

    def _record(**kwargs):
        events.append(kwargs)
        return True

    monkeypatch.setattr(vault_api, "record_audit_event_now", _record)
    return events


def test_rbac_vault_permissions_are_least_privilege():
    superadmin = permissions_for_role("superadmin")
    admin = permissions_for_role("admin")
    manager = permissions_for_role("manager")
    auditor = permissions_for_role("auditor")
    cliente = permissions_for_role("cliente")

    assert Permission.VAULT_UPLOAD in superadmin
    assert Permission.VAULT_DELETE in superadmin

    assert Permission.VAULT_UPLOAD in admin
    assert Permission.VAULT_DELETE in admin

    assert Permission.VAULT_UPLOAD in manager
    assert Permission.VAULT_DELETE not in manager

    for read_only in (auditor, cliente):
        assert Permission.VAULT_READ in read_only
        assert Permission.VAULT_UPLOAD not in read_only
        assert Permission.VAULT_DELETE not in read_only


def test_rbac_guards_reject_destructive_vault_access_for_read_only_roles():
    for role in ("auditor", "cliente", "manager"):
        with pytest.raises(HTTPException) as exc_info:
            ensure_permission(_user(role), Permission.VAULT_DELETE)
        assert exc_info.value.status_code == 403


def test_upload_endpoint_forwards_authenticated_tenant_and_records_success(monkeypatch):
    document = _document()
    service = FakeService(document)
    monkeypatch.setattr(vault_api, "_new_vault_service", lambda: service)
    events = _capture_audit(monkeypatch)

    upload = UploadFile(
        filename="certificate.pdf",
        file=io.BytesIO(b"%PDF-1.4\n%%EOF"),
        headers={"content-type": "application/pdf"},
    )

    response = asyncio.run(
        vault_api.subir_documento_boveda(
            file=upload,
            document_type="PDF_CERTIFICADO",
            idempotency_key="idem-123",
            request=_request("/api/v1/vault/documents", "POST"),
            user=_user("admin", 77),
        )
    )
    body = json.loads(response.body.decode())

    assert response.status_code == 201
    assert body["public_id"] == str(document.public_id)
    assert "object_key" not in body
    assert "storage_bucket" not in body
    assert service.upload_kwargs["organization_id"] == 77
    assert service.upload_kwargs["created_by_user_id"] == 10
    assert service.upload_kwargs["idempotency_key"] == "idem-123"
    assert events[-1]["action"] == AuditAction.VAULT_UPLOAD
    assert events[-1]["entity_id"] == document.internal_id


def test_upload_endpoint_maps_validation_and_conflict_without_leaking_details(monkeypatch):
    document = _document()
    service = FakeService(document)
    monkeypatch.setattr(vault_api, "_new_vault_service", lambda: service)
    _capture_audit(monkeypatch)

    for error, expected_status in (
        (VaultValidationError("secret validation detail"), 422),
        (VaultConflictError("secret conflict detail"), 409),
    ):
        service.upload_error = error
        upload = UploadFile(
            filename="certificate.pdf",
            file=io.BytesIO(b"%PDF-1.4\n%%EOF"),
            headers={"content-type": "application/pdf"},
        )
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(
                vault_api.subir_documento_boveda(
                    file=upload,
                    document_type="PDF_CERTIFICADO",
                    user=_user(),
                )
            )
        assert exc_info.value.status_code == expected_status
        assert "secret" not in str(exc_info.value.detail).lower()


def test_list_and_detail_return_enterprise_fields_without_storage_identity(monkeypatch):
    document = _document()
    service = FakeService(document)
    monkeypatch.setattr(vault_api, "_new_vault_service", lambda: service)

    list_response = asyncio.run(
        vault_api.consultar_documentos_boveda(
            q="cert",
            type="PDF_CERTIFICADO",
            user=_user(organization_id=8),
        )
    )
    list_body = json.loads(list_response.body.decode())
    item = list_body["documents"][0]

    assert item["document_type"] == "PDF_CERTIFICADO"
    assert item["sha256"] == "a" * 64
    assert item["organization_id"] == 8
    assert "object_key" not in item
    assert "storage_bucket" not in item
    assert service.list_kwargs["organization_id"] == 8

    detail = asyncio.run(
        vault_api.obtener_documento_boveda(
            document_id=str(document.public_id),
            user=_user(organization_id=8),
        )
    )
    assert detail.status_code == 200


def test_detail_cross_tenant_or_unknown_resource_maps_to_404(monkeypatch):
    document = _document()
    service = FakeService(document)
    monkeypatch.setattr(vault_api, "_new_vault_service", lambda: service)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            vault_api.obtener_documento_boveda(
                document_id=str(uuid4()),
                user=_user(organization_id=99),
            )
        )
    assert exc_info.value.status_code == 404


def test_download_uses_safe_private_headers_and_audits_after_verification(monkeypatch):
    document = _document()
    service = FakeService(document)
    monkeypatch.setattr(vault_api, "_new_vault_service", lambda: service)
    events = _capture_audit(monkeypatch)

    response = asyncio.run(
        vault_api.descargar_documento_vault(
            document_id=str(document.public_id),
            request=_request(
                f"/api/v1/vault/documents/{document.public_id}/download"
            ),
            user=_user(),
        )
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "private, no-store"
    assert response.headers["x-content-type-options"] == "nosniff"
    disposition = response.headers["content-disposition"]
    assert "filename*=UTF-8''" in disposition
    assert "\r" not in disposition
    assert "\n" not in disposition
    assert events[-1]["action"] == AuditAction.VAULT_DOWNLOAD
    assert events[-1]["entity_id"] == document.internal_id


def test_integrity_failure_is_audited_and_returns_generic_500(monkeypatch):
    document = _document()
    service = FakeService(document)
    service.download_error = VaultIntegrityError(
        "bucket private-secret object_key secret/path"
    )
    monkeypatch.setattr(vault_api, "_new_vault_service", lambda: service)
    events = _capture_audit(monkeypatch)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            vault_api.descargar_documento_vault(
                document_id=str(document.public_id),
                user=_user(),
            )
        )

    assert exc_info.value.status_code == 500
    assert "private-secret" not in str(exc_info.value.detail)
    assert events[-1]["action"] == AuditAction.VAULT_INTEGRITY_FAILURE
    assert events[-1]["outcome"].value == "failure"


def test_delete_endpoint_is_soft_delete_contract_and_audited(monkeypatch):
    document = _document()
    service = FakeService(document)
    monkeypatch.setattr(vault_api, "_new_vault_service", lambda: service)
    events = _capture_audit(monkeypatch)

    response = asyncio.run(
        vault_api.eliminar_documento_boveda(
            document_id=str(document.public_id),
            request=_request(
                f"/api/v1/vault/documents/{document.public_id}",
                "DELETE",
            ),
            user=_user("admin", 12),
        )
    )

    assert response.status_code == 204
    assert service.delete_kwargs["organization_id"] == 12
    assert events[-1]["action"] == AuditAction.VAULT_DELETE


def test_delete_conflict_returns_409_and_failure_audit(monkeypatch):
    document = _document()
    service = FakeService(document)
    service.delete_error = VaultConflictError("secret lifecycle")
    monkeypatch.setattr(vault_api, "_new_vault_service", lambda: service)
    events = _capture_audit(monkeypatch)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            vault_api.eliminar_documento_boveda(
                document_id=str(document.public_id),
                user=_user(),
            )
        )

    assert exc_info.value.status_code == 409
    assert events[-1]["action"] == AuditAction.VAULT_DELETE
    assert events[-1]["outcome"].value == "failure"


def test_audit_sanitizer_covers_storage_secrets_and_object_locations():
    payload = sanitize_audit_metadata(
        {
            "document_public_id": str(uuid4()),
            "object_key": "vault/tenant/secret",
            "storage_bucket": "private-bucket",
            "secret_access_key": "secret",
            "session_token": "token",
            "presigned_url": "https://signed.example/secret",
            "safe": "keep",
        }
    )

    assert payload is not None
    assert payload["safe"] == "keep"
    assert "object_key" not in payload
    assert "storage_bucket" not in payload
    assert "secret_access_key" not in payload
    assert "session_token" not in payload
    assert "presigned_url" not in payload

    assert {
        "object_key",
        "storage_bucket",
        "secret_access_key",
        "session_token",
        "presigned_url",
    }.issubset(SENSITIVE_METADATA_KEYS)


def test_audit_detail_redacts_storage_secret_patterns():
    detail = sanitize_audit_detail(
        "object_key=vault/tenant/secret secret_access_key=ABC123"
    )
    assert detail is not None
    assert "vault/tenant/secret" not in detail
    assert "ABC123" not in detail


def test_api_module_contains_no_synthetic_document_ids():
    source = open(vault_api.__file__, "r", encoding="utf-8").read()
    assert "DOC-DDS-2026-001" not in source
    assert "Litoral Trace Certificate" not in source