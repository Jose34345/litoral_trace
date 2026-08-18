from __future__ import annotations

import inspect
from pathlib import Path

from litoral_trace.db.models import VaultDocument
from litoral_trace.services.vault import VaultService
from litoral_trace.storage.s3 import Boto3S3ObjectStorage


ROOT = Path(__file__).resolve().parents[1]
DR_RUNBOOK = ROOT / "DISASTER_RECOVERY_RUNBOOK.md"


def _dr_text() -> str:
    return DR_RUNBOOK.read_text(encoding="utf-8")


def test_p27a4_contract_defines_coordinated_metadata_and_object_recovery():
    dr = _dr_text()

    for token in (
        "P2.7A4 Vault object-storage recovery contract",
        "PostgreSQL Vault metadata",
        "private object-storage bytes",
        "coordinated tuple",
        "storage_version_id when present",
        "ETag is not accepted as recovery integrity proof.",
        "SHA-256 stored in PostgreSQL is the canonical content-integrity value.",
    ):
        assert token in dr


def test_p27a4_contract_requires_exact_version_and_full_integrity_verification():
    dr = _dr_text()

    for token in (
        "When storage_version_id is present",
        "that exact object version",
        "stream the complete object",
        "recompute SHA-256 over the complete byte stream",
        "A matching ETag alone is never sufficient.",
        "A size-only match is never sufficient.",
    ):
        assert token in dr


def test_p27a4_contract_is_tenant_safe_non_destructive_and_delete_safe():
    dr = _dr_text()

    for token in (
        "<key_prefix>/tenants/<organization_id>/objects/<opaque_object_id>",
        "isolated recovery bucket/key",
        "Blind in-place overwrite",
        "MUST NOT be automatically resurrected",
        "delete_pending and delete_failed",
        "tenant binding is inconsistent",
    ):
        assert token in dr


def test_p27a4_contract_does_not_confuse_versioning_with_independent_backup():
    dr = _dr_text()

    assert (
        "The active primary object store is not an independent backup "
        "merely because versioning is enabled."
    ) in dr
    assert "P2.7A6" in dr
    assert "provider-loss" in dr
    assert "independent Vault recovery copy/replica" in dr


def test_p27a4_database_model_persists_recovery_identity_primitives():
    columns = VaultDocument.__table__.columns

    for name in (
        "organization_id",
        "public_id",
        "status",
        "storage_backend",
        "storage_bucket",
        "object_key",
        "storage_version_id",
        "size_bytes",
        "content_type",
        "sha256",
    ):
        assert name in columns


def test_p27a4_storage_adapter_supports_exact_version_reads():
    head_signature = inspect.signature(
        Boto3S3ObjectStorage.head_object
    )
    get_signature = inspect.signature(
        Boto3S3ObjectStorage.get_object_stream
    )

    assert "version_id" in head_signature.parameters
    assert "version_id" in get_signature.parameters


def test_p27a4_verified_download_uses_persisted_version_and_sha256_not_etag():
    source = inspect.getsource(
        VaultService.materialize_verified_download
    )

    for token in (
        "version_id = document.storage_version_id",
        "version_id=version_id",
        "expected_size = document.size_bytes",
        "expected_content_type = document.content_type",
        "expected_sha256 = document.sha256",
        "digest = hashlib.sha256()",
        "digest.hexdigest() != expected_sha256",
    ):
        assert token in source

    assert "storage_etag" not in source
