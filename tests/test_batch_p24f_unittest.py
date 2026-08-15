from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from fastapi import HTTPException
from starlette.requests import Request

import litoral_trace.api.batch_evidence as evidence_api
from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import (
    Permission,
    permissions_for_role,
)
from litoral_trace.db.models import BatchEvidenceLink
from litoral_trace.services.audit import AuditAction
from litoral_trace.services.batch_evidence import (
    BatchEvidenceConflictError,
    BatchEvidenceLinkResult,
    BatchEvidenceNotFoundError,
    BatchEvidencePersistenceError,
    BatchEvidenceValidationError,
    BatchEvidenceView,
    normalize_evidence_type,
)


def _user(
    *,
    role: str = "admin",
    organization_id: int = 41,
) -> UserTenantContext:
    return UserTenantContext(
        user_id=7,
        username="p24f-user",
        organization_id=organization_id,
        organization_name="P24F Org",
        organization_slug="p24f-org",
        role=role,
        email="p24f@example.com",
    )


def _request(
    *,
    method: str,
    path: str,
) -> Request:
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
                (
                    b"x-request-id",
                    b"p24f-unit-request",
                ),
                (
                    b"user-agent",
                    b"p24f-unit/1.0",
                ),
            ],
            "client": (
                "203.0.113.50",
                50000,
            ),
            "server": (
                "test",
                443,
            ),
            "root_path": "",
        }
    )


def _view(
    *,
    status: str = "available",
    evidence_type: str = "SUPPORTING_EVIDENCE",
) -> BatchEvidenceView:
    return BatchEvidenceView(
        link_internal_id=11,
        link_public_id=uuid4(),
        organization_id=41,
        batch_import_public_id=uuid4(),
        vault_document_public_id=uuid4(),
        evidence_type=evidence_type,
        linked_at=datetime.now(
            timezone.utc
        ),
        linked_by_user_id=7,
        document_filename="evidence.pdf",
        document_type="PDF_CERTIFICADO",
        document_content_type="application/pdf",
        document_size_bytes=1234,
        document_sha256="a" * 64,
        document_status=status,
    )


def _body(
    response,
):
    return json.loads(
        response.body.decode()
    )


def test_evidence_type_normalization_accepts_supported_values():
    assert (
        normalize_evidence_type(
            " source_workbook "
        )
        == "SOURCE_WORKBOOK"
    )
    assert (
        normalize_evidence_type(
            "supporting_evidence"
        )
        == "SUPPORTING_EVIDENCE"
    )
    assert (
        normalize_evidence_type(
            "COMPLIANCE_EVIDENCE"
        )
        == "COMPLIANCE_EVIDENCE"
    )


def test_evidence_type_normalization_rejects_unknown_value():
    with pytest.raises(
        BatchEvidenceValidationError
    ) as exc_info:
        normalize_evidence_type(
            "OTHER"
        )

    assert (
        exc_info.value.code
        == "INVALID_EVIDENCE_TYPE"
    )


def test_audit_actions_exist_for_link_and_unlink():
    assert (
        AuditAction.LOTE_BATCH_EVIDENCE_LINK.value
        == "lote.batch_evidence.link"
    )
    assert (
        AuditAction.LOTE_BATCH_EVIDENCE_UNLINK.value
        == "lote.batch_evidence.unlink"
    )


def test_rbac_read_and_write_capabilities_remain_least_privilege():
    auditor = permissions_for_role(
        "auditor"
    )
    manager = permissions_for_role(
        "manager"
    )

    assert Permission.LOTE_READ in auditor
    assert Permission.VAULT_READ in auditor
    assert Permission.LOTE_UPDATE not in auditor

    assert Permission.LOTE_UPDATE in manager
    assert Permission.VAULT_READ in manager


def test_model_declares_active_pair_and_source_uniqueness():
    index_names = {
        index.name
        for index in BatchEvidenceLink.__table__.indexes
    }

    assert (
        "uq_batch_evidence_links_active_pair"
        in index_names
    )
    assert (
        "uq_batch_evidence_links_active_source"
        in index_names
    )


def test_list_serializes_available_evidence_without_storage_coordinates(
    monkeypatch,
):
    evidence = _view()

    class FakeService:
        def list_evidence(
            self,
            **kwargs,
        ):
            assert (
                kwargs["organization_id"]
                == 41
            )
            return (
                evidence,
            )

    monkeypatch.setattr(
        evidence_api,
        "_new_batch_evidence_service",
        lambda: FakeService(),
    )

    response = asyncio.run(
        evidence_api.listar_evidencia_batch_endpoint(
            import_id=(
                evidence.batch_import_public_id
            ),
            user=_user(),
        )
    )

    payload = _body(
        response
    )
    assert response.status_code == 200
    assert payload["evidence_count"] == 1
    assert (
        payload["evidence"][0]["document"]["available"]
        is True
    )

    serialized = json.dumps(
        payload
    ).lower()
    assert "object_key" not in serialized
    assert "storage_bucket" not in serialized
    assert "secret" not in serialized


def test_list_surfaces_deleted_document_as_explicit_tombstone(
    monkeypatch,
):
    evidence = _view(
        status="deleted"
    )

    class FakeService:
        def list_evidence(
            self,
            **kwargs,
        ):
            return (
                evidence,
            )

    monkeypatch.setattr(
        evidence_api,
        "_new_batch_evidence_service",
        lambda: FakeService(),
    )

    response = asyncio.run(
        evidence_api.listar_evidencia_batch_endpoint(
            import_id=(
                evidence.batch_import_public_id
            ),
            user=_user(),
        )
    )

    document = _body(
        response
    )["evidence"][0]["document"]

    assert document["status"] == "deleted"
    assert document["available"] is False


def test_link_first_execution_returns_201(
    monkeypatch,
):
    evidence = _view()

    class FakeService:
        def link_evidence(
            self,
            **kwargs,
        ):
            assert (
                kwargs["organization_id"]
                == 41
            )
            assert (
                kwargs["evidence_type"]
                == "SUPPORTING_EVIDENCE"
            )
            return BatchEvidenceLinkResult(
                evidence=evidence,
                replayed=False,
            )

    monkeypatch.setattr(
        evidence_api,
        "_new_batch_evidence_service",
        lambda: FakeService(),
    )

    response = asyncio.run(
        evidence_api.vincular_evidencia_batch_endpoint(
            import_id=(
                evidence.batch_import_public_id
            ),
            payload=(
                evidence_api.BatchEvidenceLinkRequest(
                    document_id=(
                        evidence.vault_document_public_id
                    ),
                    evidence_type=(
                        "SUPPORTING_EVIDENCE"
                    ),
                )
            ),
            request=_request(
                method="POST",
                path="/api/v1/batch/imports/x/evidence",
            ),
            user=_user(),
        )
    )

    assert response.status_code == 201
    assert (
        _body(
            response
        )["replayed"]
        is False
    )


def test_link_replay_returns_200(
    monkeypatch,
):
    evidence = _view()

    class FakeService:
        def link_evidence(
            self,
            **kwargs,
        ):
            return BatchEvidenceLinkResult(
                evidence=evidence,
                replayed=True,
            )

    monkeypatch.setattr(
        evidence_api,
        "_new_batch_evidence_service",
        lambda: FakeService(),
    )

    response = asyncio.run(
        evidence_api.vincular_evidencia_batch_endpoint(
            import_id=(
                evidence.batch_import_public_id
            ),
            payload=(
                evidence_api.BatchEvidenceLinkRequest(
                    document_id=(
                        evidence.vault_document_public_id
                    ),
                    evidence_type=(
                        "SUPPORTING_EVIDENCE"
                    ),
                )
            ),
            request=_request(
                method="POST",
                path="/api/v1/batch/imports/x/evidence",
            ),
            user=_user(),
        )
    )

    assert response.status_code == 200
    assert (
        _body(
            response
        )["replayed"]
        is True
    )


@pytest.mark.parametrize(
    (
        "domain_error",
        "expected_status",
        "expected_code",
    ),
    [
        (
            BatchEvidenceValidationError(
                "INVALID_EVIDENCE_TYPE",
                "bad",
            ),
            422,
            "INVALID_EVIDENCE_TYPE",
        ),
        (
            BatchEvidenceNotFoundError(
                "BATCH_IMPORT_NOT_FOUND",
                "missing",
            ),
            404,
            "BATCH_IMPORT_NOT_FOUND",
        ),
        (
            BatchEvidenceConflictError(
                "SOURCE_WORKBOOK_HASH_MISMATCH",
                "conflict",
            ),
            409,
            "SOURCE_WORKBOOK_HASH_MISMATCH",
        ),
    ],
)
def test_link_maps_domain_errors_to_stable_http_contract(
    monkeypatch,
    domain_error,
    expected_status,
    expected_code,
):
    class FakeService:
        def link_evidence(
            self,
            **kwargs,
        ):
            raise domain_error

    monkeypatch.setattr(
        evidence_api,
        "_new_batch_evidence_service",
        lambda: FakeService(),
    )

    with pytest.raises(
        HTTPException
    ) as exc_info:
        asyncio.run(
            evidence_api.vincular_evidencia_batch_endpoint(
                import_id=uuid4(),
                payload=(
                    evidence_api.BatchEvidenceLinkRequest(
                        document_id=uuid4(),
                        evidence_type=(
                            "SUPPORTING_EVIDENCE"
                        ),
                    )
                ),
                request=_request(
                    method="POST",
                    path="/api/v1/batch/imports/x/evidence",
                ),
                user=_user(),
            )
        )

    assert (
        exc_info.value.status_code
        == expected_status
    )
    assert (
        exc_info.value.detail["code"]
        == expected_code
    )


def test_link_maps_persistence_failure_to_sanitized_503(
    monkeypatch,
):
    class FakeService:
        def link_evidence(
            self,
            **kwargs,
        ):
            raise BatchEvidencePersistenceError(
                "postgresql://secret@host/db"
            )

    monkeypatch.setattr(
        evidence_api,
        "_new_batch_evidence_service",
        lambda: FakeService(),
    )

    with pytest.raises(
        HTTPException
    ) as exc_info:
        asyncio.run(
            evidence_api.vincular_evidencia_batch_endpoint(
                import_id=uuid4(),
                payload=(
                    evidence_api.BatchEvidenceLinkRequest(
                        document_id=uuid4(),
                        evidence_type=(
                            "SUPPORTING_EVIDENCE"
                        ),
                    )
                ),
                request=_request(
                    method="POST",
                    path="/api/v1/batch/imports/x/evidence",
                ),
                user=_user(),
            )
        )

    assert (
        exc_info.value.status_code
        == 503
    )
    serialized = json.dumps(
        exc_info.value.detail
    ).lower()
    assert "postgresql://" not in serialized
    assert "secret" not in serialized


def test_unlink_returns_204_and_does_not_delete_document(
    monkeypatch,
):
    evidence = _view()
    called = {}

    class FakeService:
        def unlink_evidence(
            self,
            **kwargs,
        ):
            called.update(
                kwargs
            )
            return evidence

    monkeypatch.setattr(
        evidence_api,
        "_new_batch_evidence_service",
        lambda: FakeService(),
    )

    response = asyncio.run(
        evidence_api.desvincular_evidencia_batch_endpoint(
            import_id=(
                evidence.batch_import_public_id
            ),
            document_id=(
                evidence.vault_document_public_id
            ),
            request=_request(
                method="DELETE",
                path="/api/v1/batch/imports/x/evidence/y",
            ),
            user=_user(),
        )
    )

    assert response.status_code == 204
    assert (
        called["vault_document_id"]
        == evidence.vault_document_public_id
    )


def test_routes_are_registered_in_application():
    import main

    openapi_paths = main.app.openapi()["paths"]

    collection_path = (
        "/api/v1/batch/imports/{import_id}/evidence"
    )
    item_path = (
        "/api/v1/batch/imports/"
        "{import_id}/evidence/{document_id}"
    )

    assert collection_path in openapi_paths
    assert item_path in openapi_paths

    collection_methods = set(
        openapi_paths[collection_path]
    )
    item_methods = set(
        openapi_paths[item_path]
    )

    assert "get" in collection_methods
    assert "post" in collection_methods
    assert "delete" in item_methods