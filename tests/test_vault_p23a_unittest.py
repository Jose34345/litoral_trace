from __future__ import annotations

from uuid import UUID

from sqlalchemy import CheckConstraint, Index, UniqueConstraint

from litoral_trace.db.models import VaultDocument
from litoral_trace.db.models.vault_document import (
    VAULT_DOCUMENT_STATUSES,
    VAULT_DOCUMENT_TYPES,
    VAULT_STORAGE_BACKENDS,
)


EXPECTED_COLUMNS = {
    "id",
    "public_id",
    "organization_id",
    "created_by_user_id",
    "original_filename",
    "content_type",
    "size_bytes",
    "sha256",
    "object_key",
    "storage_backend",
    "storage_bucket",
    "storage_etag",
    "storage_version_id",
    "document_type",
    "status",
    "idempotency_key",
    "last_error_code",
    "last_error_message",
    "deleted_at",
    "created_at",
    "updated_at",
}


def test_vault_document_model_is_registered_with_expected_columns():
    assert VaultDocument.__tablename__ == "vault_documents"
    assert set(VaultDocument.__table__.columns.keys()) == EXPECTED_COLUMNS


def test_vault_document_contract_uses_opaque_public_uuid_and_internal_integer_pk():
    table = VaultDocument.__table__

    assert table.c.id.primary_key is True
    assert table.c.id.autoincrement is True

    generated = table.c.public_id.default.arg(None)
    assert isinstance(generated, UUID)


def test_vault_document_contract_keeps_storage_location_internal_and_explicit():
    table = VaultDocument.__table__

    assert table.c.object_key.nullable is False
    assert table.c.storage_backend.nullable is False
    assert table.c.storage_bucket.nullable is False
    assert table.c.storage_etag.nullable is True
    assert table.c.storage_version_id.nullable is True


def test_vault_document_contract_contains_required_lifecycle_states():
    assert VAULT_DOCUMENT_STATUSES == {
        "pending_upload",
        "available",
        "upload_failed",
        "delete_pending",
        "delete_failed",
        "deleted",
    }

    assert VAULT_DOCUMENT_TYPES == {
        "PDF_CERTIFICADO",
        "DDS_JSON_TRACES",
        "REMITO_EXCEL",
        "OTHER_EVIDENCE",
    }

    assert VAULT_STORAGE_BACKENDS == {"s3"}


def test_vault_document_constraints_cover_identity_lifecycle_and_idempotency():
    table = VaultDocument.__table__

    constraint_names = {
        constraint.name
        for constraint in table.constraints
        if constraint.name
    }

    expected_constraints = {
        "uq_vault_documents_public_id",
        "uq_vault_documents_object_key",
        "uq_vault_documents_tenant_idempotency_key",
        "ck_vault_documents_size_bytes_positive",
        "ck_vault_documents_sha256_length",
        "ck_vault_documents_filename_not_blank",
        "ck_vault_documents_content_type_not_blank",
        "ck_vault_documents_object_key_not_blank",
        "ck_vault_documents_storage_bucket_not_blank",
        "ck_vault_documents_storage_backend",
        "ck_vault_documents_document_type",
        "ck_vault_documents_status",
        "ck_vault_documents_deleted_at_state",
        "ck_vault_documents_failure_has_error_code",
    }

    assert expected_constraints.issubset(constraint_names)

    unique_constraints = {
        constraint.name
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert "uq_vault_documents_public_id" in unique_constraints
    assert "uq_vault_documents_object_key" in unique_constraints
    assert (
        "uq_vault_documents_tenant_idempotency_key"
        in unique_constraints
    )

    check_constraints = {
        constraint.name
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    }
    assert "ck_vault_documents_status" in check_constraints


def test_vault_document_indexes_cover_tenant_query_patterns():
    index_names = {
        index.name
        for index in VaultDocument.__table__.indexes
        if isinstance(index, Index)
    }

    assert {
        "ix_vault_documents_organization_id",
        "ix_vault_documents_created_by_user_id",
        "ix_vault_documents_tenant_created_at",
        "ix_vault_documents_tenant_type_created_at",
        "ix_vault_documents_tenant_status_created_at",
        "ix_vault_documents_tenant_sha256",
    }.issubset(index_names)


def test_vault_document_repr_does_not_expose_object_key_or_filename():
    document = VaultDocument(
        id=7,
        organization_id=11,
        original_filename="sensitive.pdf",
        content_type="application/pdf",
        size_bytes=100,
        sha256="a" * 64,
        object_key="tenant/secret/object-key",
        storage_bucket="private-bucket",
        document_type="PDF_CERTIFICADO",
    )

    rendered = repr(document)

    assert "tenant/secret/object-key" not in rendered
    assert "sensitive.pdf" not in rendered
    assert "private-bucket" not in rendered