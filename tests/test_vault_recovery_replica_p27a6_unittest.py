from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
import hashlib
import io
import json

import pytest

from litoral_trace.config.settings import (
    StorageSettings,
)
from litoral_trace.storage import (
    ObjectDeleteResult,
    ObjectHead,
    ObjectStorageNotFoundError,
    ObjectStorageStream,
    ObjectWriteResult,
)
from scripts.vault_recovery_replica import (
    COMPLETE_FORMAT_VERSION,
    MANIFEST_FORMAT_VERSION,
    RESULT_FORMAT_VERSION,
    RecoveryDomain,
    VaultDatabaseSnapshot,
    VaultReplicaDocument,
    VaultReplicaError,
    _assert_vault_backup_visibility,
    assert_independent_domains,
    replicate_vault_snapshot,
)


NOW = datetime(
    2026,
    8,
    18,
    16,
    30,
    0,
    tzinfo=UTC,
)
SNAPSHOT_ID = "20260818T163000Z"
RELEASE = (
    "e05005666009c17f8a1bcb92ccfaff28092c5e36"
)
DB_IDENTITY = "d" * 64
ALEMBIC = "018_example_p2_head"


class _StoredObject:
    def __init__(
        self,
        *,
        data: bytes,
        content_type: str,
        metadata: dict[str, str],
        version_id: str | None,
    ):
        self.data = bytes(data)
        self.content_type = content_type
        self.metadata = dict(
            metadata
        )
        self.version_id = version_id


class MemoryStorage:
    def __init__(
        self,
        bucket_name: str,
    ):
        self.bucket_name = bucket_name
        self._objects: dict[
            str,
            _StoredObject,
        ] = {}
        self._versions: dict[
            tuple[str, str],
            _StoredObject,
        ] = {}
        self._counter = 0
        self.events: list[
            tuple[str, str]
        ] = []

    def seed(
        self,
        *,
        key: str,
        data: bytes,
        content_type: str,
        metadata: dict[str, str] | None = None,
        version_id: str | None = None,
    ) -> None:
        stored = _StoredObject(
            data=data,
            content_type=content_type,
            metadata=metadata or {},
            version_id=version_id,
        )
        self._objects[key] = stored

        if version_id is not None:
            self._versions[
                (key, version_id)
            ] = stored

    def _resolve(
        self,
        *,
        key: str,
        version_id: str | None,
    ) -> _StoredObject:
        if version_id is not None:
            stored = self._versions.get(
                (key, version_id)
            )
        else:
            stored = self._objects.get(
                key
            )

        if stored is None:
            raise ObjectStorageNotFoundError(
                "memory"
            )

        return stored

    def put_object(
        self,
        *,
        key: str,
        body,
        content_type: str,
        content_length: int,
        metadata=None,
    ) -> ObjectWriteResult:
        if isinstance(
            body,
            bytes,
        ):
            payload = body
        else:
            payload = body.read()

        if len(payload) != content_length:
            raise AssertionError(
                "test storage length mismatch"
            )

        self._counter += 1
        version_id = (
            f"replica-v{self._counter}"
        )

        self.seed(
            key=key,
            data=payload,
            content_type=content_type,
            metadata=dict(
                metadata or {}
            ),
            version_id=version_id,
        )

        self.events.append(
            ("put", key)
        )

        return ObjectWriteResult(
            etag="etag",
            version_id=version_id,
        )

    def head_object(
        self,
        *,
        key: str,
        version_id: str | None = None,
    ) -> ObjectHead:
        stored = self._resolve(
            key=key,
            version_id=version_id,
        )

        return ObjectHead(
            size_bytes=len(
                stored.data
            ),
            content_type=(
                stored.content_type
            ),
            etag="etag",
            version_id=(
                stored.version_id
            ),
            metadata=(
                stored.metadata
            ),
        )

    def get_object_stream(
        self,
        *,
        key: str,
        version_id: str | None = None,
    ) -> ObjectStorageStream:
        stored = self._resolve(
            key=key,
            version_id=version_id,
        )

        return ObjectStorageStream(
            body=io.BytesIO(
                stored.data
            ),
            head=ObjectHead(
                size_bytes=len(
                    stored.data
                ),
                content_type=(
                    stored.content_type
                ),
                etag="etag",
                version_id=(
                    stored.version_id
                ),
                metadata=(
                    stored.metadata
                ),
            ),
        )

    def delete_object(
        self,
        *,
        key: str,
        version_id: str | None = None,
    ) -> ObjectDeleteResult:
        raise AssertionError(
            "replica tests must never delete"
        )

    def object_exists(
        self,
        *,
        key: str,
        version_id: str | None = None,
    ) -> bool:
        try:
            self._resolve(
                key=key,
                version_id=version_id,
            )
        except ObjectStorageNotFoundError:
            return False

        return True

    def health_check(self) -> bool:
        return True

    def bytes_for(
        self,
        key: str,
    ) -> bytes:
        return self._resolve(
            key=key,
            version_id=None,
        ).data


class MisreportingVersionStorage(
    MemoryStorage
):
    def get_object_stream(
        self,
        *,
        key: str,
        version_id: str | None = None,
    ) -> ObjectStorageStream:
        stream = super().get_object_stream(
            key=key,
            version_id=version_id,
        )

        return ObjectStorageStream(
            body=io.BytesIO(
                stream.read()
            ),
            head=replace(
                stream.head,
                version_id="wrong-version",
            ),
        )


def _settings(
    *,
    bucket: str,
    key_prefix: str,
    endpoint: str,
) -> StorageSettings:
    return StorageSettings(
        backend="s3",
        bucket_name=bucket,
        region="test-1",
        endpoint_url=endpoint,
        force_path_style=True,
        use_tls=True,
        verify_tls=True,
        key_prefix=key_prefix,
    )


def _primary_settings() -> StorageSettings:
    return _settings(
        bucket="primary-vault",
        key_prefix="vault",
        endpoint="https://primary.example",
    )


def _replica_settings() -> StorageSettings:
    return _settings(
        bucket="independent-recovery",
        key_prefix=(
            "litoral-trace/vault-recovery"
        ),
        endpoint="https://replica.example",
    )


def _primary_domain() -> RecoveryDomain:
    return RecoveryDomain(
        provider_id="primary-provider",
        failure_domain="primary-provider-global",
    )


def _replica_domain() -> RecoveryDomain:
    return RecoveryDomain(
        provider_id="secondary-provider",
        failure_domain="secondary-provider-global",
    )


def _document(
    *,
    data: bytes = b"vault-evidence",
    organization_id: int = 7,
    status: str = "available",
    object_key: str = (
        "vault/tenants/7/objects/opaque-1"
    ),
    storage_bucket: str = "primary-vault",
    storage_version_id: str | None = (
        "source-v1"
    ),
    sha256: str | None = None,
) -> VaultReplicaDocument:
    return VaultReplicaDocument(
        organization_id=organization_id,
        public_id=(
            "11111111-1111-4111-8111-111111111111"
        ),
        status=status,
        storage_backend="s3",
        storage_bucket=storage_bucket,
        object_key=object_key,
        storage_version_id=(
            storage_version_id
        ),
        size_bytes=len(data),
        content_type="application/pdf",
        sha256=(
            sha256
            or hashlib.sha256(
                data
            ).hexdigest()
        ),
    )


def _snapshot(
    *documents: VaultReplicaDocument,
) -> VaultDatabaseSnapshot:
    return VaultDatabaseSnapshot(
        database_name="neondb",
        source_identity_sha256=(
            DB_IDENTITY
        ),
        postgres_server_version="17.10",
        postgis_version="3.5.0",
        alembic_revision=ALEMBIC,
        documents=tuple(
            documents
        ),
    )


def _run(
    *,
    document: VaultReplicaDocument,
    primary: MemoryStorage,
    replica: MemoryStorage,
):
    return replicate_vault_snapshot(
        database_snapshot=_snapshot(
            document
        ),
        primary_storage=primary,
        replica_storage=replica,
        primary_settings=(
            _primary_settings()
        ),
        replica_settings=(
            _replica_settings()
        ),
        primary_domain=(
            _primary_domain()
        ),
        replica_domain=(
            _replica_domain()
        ),
        source_label="production",
        release_commit=RELEASE,
        snapshot_id=SNAPSHOT_ID,
        now_utc=NOW,
    )


def test_p27a6a_available_object_is_verified_and_replicated():
    data = b"vault-evidence"
    document = _document(
        data=data
    )

    primary = MemoryStorage(
        "primary-vault"
    )
    primary.seed(
        key=document.object_key,
        data=data,
        content_type=document.content_type,
        metadata={},
        version_id="source-v1",
    )

    replica = MemoryStorage(
        "independent-recovery"
    )

    result = _run(
        document=document,
        primary=primary,
        replica=replica,
    )

    assert result["result"] == "PASS"
    assert (
        result["format_version"]
        == RESULT_FORMAT_VERSION
    )
    assert (
        result[
            "replicated_document_count"
        ]
        == 1
    )
    assert (
        result["replica_object_count"]
        == 1
    )

    manifest = json.loads(
        replica.bytes_for(
            result["manifest_key"]
        )
    )

    assert (
        manifest["format_version"]
        == MANIFEST_FORMAT_VERSION
    )
    assert (
        manifest["source_database"][
            "source_identity_sha256"
        ]
        == DB_IDENTITY
    )

    entry = manifest[
        "documents"
    ][0]

    assert (
        entry["replica_state"]
        == "copied_verified"
    )
    assert (
        entry[
            "source_exact_version_verified"
        ]
        is True
    )
    assert (
        entry["storage_version_id"]
        == "source-v1"
    )
    assert (
        replica.bytes_for(
            entry["replica_key"]
        )
        == data
    )


def test_p27a6a_complete_marker_is_published_last():
    data = b"vault-evidence"
    document = _document(
        data=data
    )

    primary = MemoryStorage(
        "primary-vault"
    )
    primary.seed(
        key=document.object_key,
        data=data,
        content_type=document.content_type,
        version_id="source-v1",
    )

    replica = MemoryStorage(
        "independent-recovery"
    )

    result = _run(
        document=document,
        primary=primary,
        replica=replica,
    )

    assert (
        replica.events[-1]
        == (
            "put",
            result["complete_key"],
        )
    )

    complete = json.loads(
        replica.bytes_for(
            result["complete_key"]
        )
    )

    assert (
        complete["format_version"]
        == COMPLETE_FORMAT_VERSION
    )
    assert (
        complete["manifest_sha256"]
        == result["manifest_sha256"]
    )


@pytest.mark.parametrize(
    "status",
    [
        "pending_upload",
        "upload_failed",
        "delete_pending",
        "delete_failed",
        "deleted",
    ],
)
def test_p27a6a_non_available_states_are_not_auto_recovered(
    status,
):
    document = _document(
        status=status
    )

    primary = MemoryStorage(
        "primary-vault"
    )
    replica = MemoryStorage(
        "independent-recovery"
    )

    result = _run(
        document=document,
        primary=primary,
        replica=replica,
    )

    assert (
        result[
            "replicated_document_count"
        ]
        == 0
    )
    assert (
        result[
            "state_only_document_count"
        ]
        == 1
    )

    manifest = json.loads(
        replica.bytes_for(
            result["manifest_key"]
        )
    )
    entry = manifest[
        "documents"
    ][0]

    assert (
        entry["recovery_eligible"]
        is False
    )
    assert (
        entry["replica_key"]
        is None
    )
    assert (
        entry["replica_state"]
        == "state_only_not_auto_recoverable"
    )


def test_p27a6a_rejects_cross_tenant_primary_key():
    data = b"vault-evidence"
    document = _document(
        data=data,
        object_key=(
            "vault/tenants/8/objects/opaque-1"
        ),
    )

    primary = MemoryStorage(
        "primary-vault"
    )
    primary.seed(
        key=document.object_key,
        data=data,
        content_type=document.content_type,
        version_id="source-v1",
    )

    replica = MemoryStorage(
        "independent-recovery"
    )

    with pytest.raises(
        VaultReplicaError,
        match="tenant binding",
    ):
        _run(
            document=document,
            primary=primary,
            replica=replica,
        )


def test_p27a6a_rejects_primary_sha256_mismatch():
    expected = b"vault-evidence"
    actual = b"vault-evidencf"

    assert len(expected) == len(
        actual
    )

    document = _document(
        data=expected
    )

    primary = MemoryStorage(
        "primary-vault"
    )
    primary.seed(
        key=document.object_key,
        data=actual,
        content_type=document.content_type,
        version_id="source-v1",
    )

    replica = MemoryStorage(
        "independent-recovery"
    )

    with pytest.raises(
        VaultReplicaError,
        match="SHA-256",
    ):
        _run(
            document=document,
            primary=primary,
            replica=replica,
        )


def test_p27a6a_rejects_exact_source_version_mismatch():
    data = b"vault-evidence"
    document = _document(
        data=data
    )

    primary = (
        MisreportingVersionStorage(
            "primary-vault"
        )
    )
    primary.seed(
        key=document.object_key,
        data=data,
        content_type=document.content_type,
        version_id="source-v1",
    )

    replica = MemoryStorage(
        "independent-recovery"
    )

    with pytest.raises(
        VaultReplicaError,
        match="exact-version",
    ):
        _run(
            document=document,
            primary=primary,
            replica=replica,
        )


def test_p27a6a_rejects_corrupt_existing_replica_without_overwrite():
    data = b"vault-evidence"
    document = _document(
        data=data
    )

    primary = MemoryStorage(
        "primary-vault"
    )
    primary.seed(
        key=document.object_key,
        data=data,
        content_type=document.content_type,
        version_id="source-v1",
    )

    replica = MemoryStorage(
        "independent-recovery"
    )

    replica_key = (
        "litoral-trace/vault-recovery/"
        "tenants/7/objects/sha256/"
        f"{document.sha256[:2]}/"
        f"{document.sha256}"
    )

    corrupt = b"fault-evidence"
    assert len(corrupt) == len(data)

    replica.seed(
        key=replica_key,
        data=corrupt,
        content_type=document.content_type,
        metadata={
            "sha256": document.sha256,
            "organization-id": "7",
        },
        version_id="existing-v1",
    )

    with pytest.raises(
        VaultReplicaError,
        match="full SHA-256",
    ):
        _run(
            document=document,
            primary=primary,
            replica=replica,
        )

    assert (
        ("put", replica_key)
        not in replica.events
    )


def test_p27a6a_rejects_wrong_primary_bucket_binding():
    data = b"vault-evidence"
    document = _document(
        data=data,
        storage_bucket="other-bucket",
    )

    primary = MemoryStorage(
        "primary-vault"
    )
    replica = MemoryStorage(
        "independent-recovery"
    )

    with pytest.raises(
        VaultReplicaError,
        match="storage bucket",
    ):
        _run(
            document=document,
            primary=primary,
            replica=replica,
        )


def test_p27a6a_rejects_same_provider():
    with pytest.raises(
        VaultReplicaError,
        match="different storage providers",
    ):
        assert_independent_domains(
            primary=RecoveryDomain(
                provider_id="aws",
                failure_domain="aws-global",
            ),
            replica=RecoveryDomain(
                provider_id="aws",
                failure_domain="other",
            ),
            primary_settings=(
                _primary_settings()
            ),
            replica_settings=(
                _replica_settings()
            ),
        )


def test_p27a6a_rejects_same_failure_domain():
    with pytest.raises(
        VaultReplicaError,
        match="different failure domains",
    ):
        assert_independent_domains(
            primary=RecoveryDomain(
                provider_id="provider-a",
                failure_domain="shared-domain",
            ),
            replica=RecoveryDomain(
                provider_id="provider-b",
                failure_domain="shared-domain",
            ),
            primary_settings=(
                _primary_settings()
            ),
            replica_settings=(
                _replica_settings()
            ),
        )


def test_p27a6a_reuses_verified_content_addressed_payload():
    data = b"vault-evidence"
    document = _document(
        data=data
    )

    primary = MemoryStorage(
        "primary-vault"
    )
    primary.seed(
        key=document.object_key,
        data=data,
        content_type=document.content_type,
        version_id="source-v1",
    )

    replica = MemoryStorage(
        "independent-recovery"
    )

    first = _run(
        document=document,
        primary=primary,
        replica=replica,
    )

    payload_puts_before = [
        event
        for event in replica.events
        if "/objects/sha256/" in event[1]
    ]

    assert len(
        payload_puts_before
    ) == 1

    # Same verified payload under another snapshot.
    second = replicate_vault_snapshot(
        database_snapshot=_snapshot(
            document
        ),
        primary_storage=primary,
        replica_storage=replica,
        primary_settings=(
            _primary_settings()
        ),
        replica_settings=(
            _replica_settings()
        ),
        primary_domain=(
            _primary_domain()
        ),
        replica_domain=(
            _replica_domain()
        ),
        source_label="production",
        release_commit=RELEASE,
        snapshot_id="20260818T164500Z",
        now_utc=NOW,
    )

    payload_puts_after = [
        event
        for event in replica.events
        if "/objects/sha256/" in event[1]
    ]

    assert len(
        payload_puts_after
    ) == 1
    assert (
        first["replica_object_count"]
        == 1
    )
    assert (
        second["replica_object_count"]
        == 1
    )


def test_p27a6a_rejects_missing_primary_content_type():
    data = b"vault-evidence"
    document = _document(
        data=data
    )

    primary = MemoryStorage(
        "primary-vault"
    )
    primary.seed(
        key=document.object_key,
        data=data,
        content_type=None,
        version_id="source-v1",
    )

    replica = MemoryStorage(
        "independent-recovery"
    )

    with pytest.raises(
        VaultReplicaError,
        match="content type is missing",
    ):
        _run(
            document=document,
            primary=primary,
            replica=replica,
        )


def test_p27a6a_rejects_backup_role_without_rls_bypass():
    with pytest.raises(
        VaultReplicaError,
        match="cannot bypass tenant RLS",
    ):
        _assert_vault_backup_visibility(
            {
                "rls_enabled": True,
                "rls_forced": True,
                "role_bypassrls": False,
                "role_superuser": False,
            }
        )


def test_p27a6a_accepts_verified_cross_tenant_backup_visibility():
    _assert_vault_backup_visibility(
        {
            "rls_enabled": True,
            "rls_forced": True,
            "role_bypassrls": True,
            "role_superuser": False,
        }
    )
