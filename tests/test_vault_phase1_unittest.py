from __future__ import annotations

import asyncio
import io
import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from fastapi import Response
from starlette.datastructures import UploadFile

import litoral_trace.api.vault as vault_api
from litoral_trace.api.auth import UserTenantContext
from litoral_trace.services.vault import (
    VaultDocumentView,
    VaultNotFoundError,
    VerifiedVaultDownload,
)


def _user(*, organization_id: int = 1, role: str = "admin") -> UserTenantContext:
    return UserTenantContext(
        user_id=11,
        username="vault_phase1_user",
        organization_id=organization_id,
        organization_name=f"Org {organization_id}",
        organization_slug=f"org-{organization_id}",
        role=role,
        email="vault-phase1@example.com",
    )


def _document(*, organization_id: int = 1) -> VaultDocumentView:
    del organization_id
    now = datetime.now(timezone.utc)
    return VaultDocumentView(
        internal_id=501,
        public_id=uuid4(),
        filename="evidence.json",
        document_type="DDS_JSON_TRACES",
        content_type="application/json",
        size_bytes=2,
        sha256=("a" * 64),
        status="available",
        created_at=now,
        updated_at=now,
        deleted_at=None,
    )


class FakeVaultService:
    def __init__(self, document: VaultDocumentView):
        self.document = document

    def list_documents(self, **kwargs):
        return [self.document]

    def materialize_verified_download(self, **kwargs):
        if str(kwargs["document_id"]) != str(self.document.public_id):
            raise VaultNotFoundError("not found")
        return VerifiedVaultDownload(
            document=self.document,
            fileobj=io.BytesIO(b"{}"),
        )


def test_consultar_documentos_boveda_uses_persistent_service_contract(monkeypatch):
    document = _document()
    monkeypatch.setattr(
        vault_api,
        "_new_vault_service",
        lambda: FakeVaultService(document),
    )

    response = asyncio.run(
        vault_api.consultar_documentos_boveda(
            q=None,
            type=None,
            user=_user(),
        )
    )
    body = json.loads(response.body.decode("utf-8"))

    assert response.status_code == 200
    assert body["total_documents"] == 1
    assert body["documents"][0]["public_id"] == str(document.public_id)
    assert body["documents"][0]["download_url"].endswith("/download")


def test_descargar_documento_boveda_legacy_alias_streams_real_service(monkeypatch):
    document = _document()
    monkeypatch.setattr(
        vault_api,
        "_new_vault_service",
        lambda: FakeVaultService(document),
    )
    monkeypatch.setattr(
        vault_api,
        "record_audit_event_now",
        lambda **kwargs: True,
    )

    response = asyncio.run(
        vault_api.descargar_documento_boveda(
            doc_id=str(document.public_id),
            user=_user(),
        )
    )

    assert response.status_code == 200
    assert response.media_type == "application/json"
    assert response.headers["cache-control"] == "private, no-store"


def test_descargar_documento_boveda_invalid_or_unknown_id_returns_404(monkeypatch):
    document = _document()
    monkeypatch.setattr(
        vault_api,
        "_new_vault_service",
        lambda: FakeVaultService(document),
    )

    with pytest.raises(Exception) as exc_info:
        asyncio.run(
            vault_api.descargar_documento_boveda(
                doc_id=str(uuid4()),
                user=_user(),
            )
        )

    assert getattr(exc_info.value, "status_code", None) == 404


def test_phase1_no_longer_depends_on_synthetic_document_ids():
    assert "DOC-DDS-2026-001" not in open(
        vault_api.__file__,
        "r",
        encoding="utf-8",
    ).read()