from __future__ import annotations

from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path

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
from scripts.vault_provider_loss_drill import (
    DRILL_FORMAT_VERSION,
    VaultProviderLossDrillError,
    assert_primary_provider_not_configured,
    run_provider_loss_drill,
)
from scripts.vault_recovery_replica import (
    COMPLETE_FORMAT_VERSION,
    MANIFEST_FORMAT_VERSION,
    RecoveryDomain,
)


SNAPSHOT_ID = "20260818T170000Z"
RELEASE = (
    "556ac5c7f49247b52862827fe3af912cc557c18b"
)
ALEMBIC = "018_add_batch_evidence_links"
SOURCE_IDENTITY = "a" * 64

REPLICA_PREFIX = (
    "litoral-trace/vault-recovery"
)
REPLICA_BUCKET = (
    "independent-vault-recovery"
)

PUBLIC_ID = (
    "11111111-1111-4111-8111-111111111111"
)

STATE_PUBLIC_ID = (
    "22222222-2222-4222-8222-222222222222"
)

PDF = (
    b"%PDF-1.4\n"
    b"1 0 obj\n"
    b"<<>>\n"
    b"endobj\n"
    b"%%EOF\n"
)


class _StoredObject:
    def __init__(
        self,
        *,
        data: bytes,
        content_type: str | None,
        metadata: dict[str, str],
        version_id: str | None,
    ):
        self.data = bytes(data)
        self.content_type = (
            content_type
        )
        self.metadata = dict(
            metadata
        )
        self.version_id = (
            version_id
        )


class ReadOnlyMemoryStorage:
    def __init__(self):
        self._objects: dict[
            str,
            _StoredObject,
        ] = {}
        self._versions: dict[
            tuple[str, str],
            _StoredObject,
        ] = {}
        self.reads: list[
            tuple[str, str]
        ] = []

    def seed(
        self,
        *,
        key: str,
        data: bytes,
        content_type: str | None,
        metadata: dict[str, str],
        version_id: str | None,
    ) -> None:
        stored = _StoredObject(
            data=data,
            content_type=(
                content_type
            ),
            metadata=metadata,
            version_id=(
                version_id
            ),
        )

        self._objects[
            key
        ] = stored

        if version_id is not None:
            self._versions[
                (
                    key,
                    version_id,
                )
            ] = stored

    def _resolve(
        self,
        *,
        key: str,
        version_id: str | None,
    ) -> _StoredObject:
        if version_id is None:
            stored = (
                self._objects.get(
                    key
                )
            )
        else:
            stored = (
                self._versions.get(
                    (
                        key,
                        version_id,
                    )
                )
            )

        if stored is None:
            raise ObjectStorageNotFoundError(
                "memory"
            )

        return stored

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

        self.reads.append(
            ("head", key)
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

        self.reads.append(
            ("get", key)
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

    def put_object(
        self,
        *,
        key: str,
        body,
        content_type: str,
        content_length: int,
        metadata=None,
    ) -> ObjectWriteResult:
        raise AssertionError(
            "Provider-loss drill must never write to replica storage."
        )

    def delete_object(
        self,
        *,
        key: str,
        version_id: str | None = None,
    ) -> ObjectDeleteResult:
        raise AssertionError(
            "Provider-loss drill must never delete replica objects."
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

    def health_check(
        self,
    ) -> bool:
        return True


def _canonical_json(
    payload,
) -> bytes:
    return (
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
        )
        + "\n"
    ).encode("utf-8")


def _sha(
    data: bytes,
) -> str:
    return hashlib.sha256(
        data
    ).hexdigest()


def _settings() -> StorageSettings:
    return StorageSettings(
        backend="s3",
        bucket_name=(
            REPLICA_BUCKET
        ),
        region="test-1",
        endpoint_url=(
            "https://replica.example"
        ),
        force_path_style=True,
        use_tls=True,
        verify_tls=True,
        key_prefix=(
            REPLICA_PREFIX
        ),
    )


def _domain() -> RecoveryDomain:
    return RecoveryDomain(
        provider_id=(
            "secondary-provider"
        ),
        failure_domain=(
            "secondary-provider-global"
        ),
    )


def _base_manifest(
    *,
    data: bytes = PDF,
    content_type: str = (
        "application/pdf"
    ),
    status: str = "available",
):
    digest = _sha(
        data
    )

    replica_key = (
        f"{REPLICA_PREFIX}/tenants/"
        f"7/objects/sha256/"
        f"{digest[:2]}/{digest}"
    )

    if status == "available":
        document = {
            "organization_id": 7,
            "public_id": PUBLIC_ID,
            "status": "available",
            "storage_backend": "s3",
            "storage_bucket": (
                "primary-vault"
            ),
            "object_key": (
                "vault/tenants/7/"
                "objects/source-object"
            ),
            "storage_version_id": (
                "source-v1"
            ),
            "size_bytes": len(
                data
            ),
            "content_type": (
                content_type
            ),
            "sha256": digest,
            "replica_state": (
                "copied_verified"
            ),
            "recovery_eligible": True,
            "replica_key": (
                replica_key
            ),
            "replica_version_id": (
                "replica-payload-v1"
            ),
            "source_exact_version_verified": (
                True
            ),
        }

        replicated = 1
        state_only = 0
        object_count = 1

    else:
        document = {
            "organization_id": 7,
            "public_id": PUBLIC_ID,
            "status": status,
            "storage_backend": "s3",
            "storage_bucket": (
                "primary-vault"
            ),
            "object_key": (
                "vault/tenants/7/"
                "objects/source-object"
            ),
            "storage_version_id": (
                "source-v1"
            ),
            "size_bytes": len(
                data
            ),
            "content_type": (
                content_type
            ),
            "sha256": digest,
            "replica_state": (
                "state_only_not_auto_recoverable"
            ),
            "recovery_eligible": False,
            "replica_key": None,
            "replica_version_id": None,
            "source_exact_version_verified": (
                False
            ),
        }

        replicated = 0
        state_only = 1
        object_count = 0

    manifest = {
        "format_version": (
            MANIFEST_FORMAT_VERSION
        ),
        "created_at_utc": (
            "2026-08-18T17:00:00Z"
        ),
        "snapshot_id": (
            SNAPSHOT_ID
        ),
        "source_label": (
            "p2-release-candidate"
        ),
        "release_commit": (
            RELEASE
        ),
        "source_database": {
            "database_name": (
                "neondb"
            ),
            "source_identity_sha256": (
                SOURCE_IDENTITY
            ),
            "postgres_server_version": (
                "17.10"
            ),
            "postgis_version": (
                "3.5.0"
            ),
            "alembic_revision": (
                ALEMBIC
            ),
        },
        "primary_storage": {
            "provider_id": (
                "primary-provider"
            ),
            "failure_domain": (
                "primary-provider-global"
            ),
            "bucket_name": (
                "primary-vault"
            ),
            "key_prefix": "vault",
        },
        "replica_storage": {
            "provider_id": (
                "secondary-provider"
            ),
            "failure_domain": (
                "secondary-provider-global"
            ),
            "bucket_name": (
                REPLICA_BUCKET
            ),
            "key_prefix": (
                REPLICA_PREFIX
            ),
        },
        "document_count": 1,
        "replicated_document_count": (
            replicated
        ),
        "state_only_document_count": (
            state_only
        ),
        "replica_object_count": (
            object_count
        ),
        "documents": [
            document
        ],
    }

    return (
        manifest,
        document,
    )


def _seed_snapshot(
    *,
    manifest=None,
    payload: bytes = PDF,
    payload_content_type: (
        str | None
    ) = "application/pdf",
):
    storage = (
        ReadOnlyMemoryStorage()
    )

    if manifest is None:
        manifest, document = (
            _base_manifest(
                data=payload,
            )
        )
    else:
        document = (
            manifest[
                "documents"
            ][0]
        )

    manifest_key = (
        f"{REPLICA_PREFIX}/snapshots/"
        f"{SNAPSHOT_ID}/manifest.json"
    )

    complete_key = (
        f"{REPLICA_PREFIX}/snapshots/"
        f"{SNAPSHOT_ID}/complete.json"
    )

    manifest_bytes = (
        _canonical_json(
            manifest
        )
    )
    manifest_sha = _sha(
        manifest_bytes
    )

    storage.seed(
        key=manifest_key,
        data=manifest_bytes,
        content_type=(
            "application/json"
        ),
        metadata={
            "sha256": (
                manifest_sha
            ),
            "artifact-kind": (
                "vault-recovery-manifest"
            ),
        },
        version_id=(
            "manifest-v1"
        ),
    )

    complete = {
        "format_version": (
            COMPLETE_FORMAT_VERSION
        ),
        "snapshot_id": (
            SNAPSHOT_ID
        ),
        "source_label": (
            "p2-release-candidate"
        ),
        "release_commit": (
            RELEASE
        ),
        "published_at_utc": (
            "2026-08-18T17:00:00Z"
        ),
        "source_identity_sha256": (
            SOURCE_IDENTITY
        ),
        "alembic_revision": (
            ALEMBIC
        ),
        "replica_provider_id": (
            "secondary-provider"
        ),
        "replica_failure_domain": (
            "secondary-provider-global"
        ),
        "manifest_key": (
            manifest_key
        ),
        "manifest_sha256": (
            manifest_sha
        ),
        "manifest_version_id": (
            "manifest-v1"
        ),
        "document_count": (
            manifest[
                "document_count"
            ]
        ),
        "replicated_document_count": (
            manifest[
                "replicated_document_count"
            ]
        ),
        "state_only_document_count": (
            manifest[
                "state_only_document_count"
            ]
        ),
        "replica_object_count": (
            manifest[
                "replica_object_count"
            ]
        ),
    }

    complete_bytes = (
        _canonical_json(
            complete
        )
    )
    complete_sha = _sha(
        complete_bytes
    )

    storage.seed(
        key=complete_key,
        data=complete_bytes,
        content_type=(
            "application/json"
        ),
        metadata={
            "sha256": (
                complete_sha
            ),
            "artifact-kind": (
                "vault-recovery-complete"
            ),
        },
        version_id=(
            "complete-v1"
        ),
    )

    if (
        document[
            "status"
        ]
        == "available"
    ):
        storage.seed(
            key=document[
                "replica_key"
            ],
            data=payload,
            content_type=(
                payload_content_type
            ),
            metadata={
                "sha256": (
                    document[
                        "sha256"
                    ]
                ),
                "organization-id": (
                    str(
                        document[
                            "organization_id"
                        ]
                    )
                ),
            },
            version_id=(
                document[
                    "replica_version_id"
                ]
            ),
        )

    return {
        "storage": storage,
        "complete_key": (
            complete_key
        ),
        "complete_sha": (
            complete_sha
        ),
        "complete": complete,
        "manifest": manifest,
        "document": document,
    }


def _run(
    *,
    seeded,
    output: Path,
):
    return run_provider_loss_drill(
        replica_storage=(
            seeded["storage"]
        ),
        replica_settings=(
            _settings()
        ),
        replica_domain=(
            _domain()
        ),
        complete_key=(
            seeded[
                "complete_key"
            ]
        ),
        complete_version_id=(
            "complete-v1"
        ),
        expected_complete_sha256=(
            seeded[
                "complete_sha"
            ]
        ),
        expected_snapshot_id=(
            SNAPSHOT_ID
        ),
        expected_source_label=(
            "p2-release-candidate"
        ),
        expected_release_commit=(
            RELEASE
        ),
        expected_alembic_revision=(
            ALEMBIC
        ),
        output_directory=output,
        operator="Jose Lezcano",
    )


def test_p27a6b_recovers_verified_bytes_without_primary_access(
    tmp_path,
):
    seeded = _seed_snapshot()

    output = (
        tmp_path
        / "isolated-recovery"
    )

    result = _run(
        seeded=seeded,
        output=output,
    )

    assert (
        result["result"]
        == "PASS"
    )
    assert (
        result[
            "format_version"
        ]
        == DRILL_FORMAT_VERSION
    )
    assert (
        result[
            "primary_access_attempted"
        ]
        is False
    )
    assert (
        result[
            "replica_read_only"
        ]
        is True
    )
    assert (
        result[
            "production_modified"
        ]
        is False
    )
    assert (
        result[
            "recovered_document_count"
        ]
        == 1
    )
    assert (
        result[
            "state_only_document_count"
        ]
        == 0
    )
    assert (
        result[
            "recovered_bytes"
        ]
        == len(PDF)
    )

    payload_path = (
        output
        / "tenants"
        / "7"
        / "documents"
        / PUBLIC_ID
        / "payload.pdf"
    )

    assert (
        payload_path.read_bytes()
        == PDF
    )

    index = json.loads(
        (
            output
            / "recovery_index.json"
        ).read_text(
            encoding="utf-8"
        )
    )

    assert (
        index[
            "provider_loss_mode"
        ]
        == "primary_unavailable"
    )


def test_p27a6b_rejects_wrong_complete_sha256(
    tmp_path,
):
    seeded = _seed_snapshot()

    with pytest.raises(
        VaultProviderLossDrillError,
        match="complete marker metadata SHA-256 binding failed|complete marker full SHA-256",
    ):
        run_provider_loss_drill(
            replica_storage=(
                seeded["storage"]
            ),
            replica_settings=(
                _settings()
            ),
            replica_domain=(
                _domain()
            ),
            complete_key=(
                seeded[
                    "complete_key"
                ]
            ),
            complete_version_id=(
                "complete-v1"
            ),
            expected_complete_sha256=(
                "0" * 64
            ),
            expected_snapshot_id=(
                SNAPSHOT_ID
            ),
            expected_source_label=(
                "p2-release-candidate"
            ),
            expected_release_commit=(
                RELEASE
            ),
            expected_alembic_revision=(
                ALEMBIC
            ),
            output_directory=(
                tmp_path
                / "recovery"
            ),
            operator="Jose Lezcano",
        )


def test_p27a6b_rejects_manifest_tamper(
    tmp_path,
):
    seeded = _seed_snapshot()

    key = (
        seeded[
            "complete"
        ][
            "manifest_key"
        ]
    )

    original = (
        seeded[
            "storage"
        ]._objects[
            key
        ]
    )

    corrupt = (
        original.data
        + b" "
    )

    seeded[
        "storage"
    ].seed(
        key=key,
        data=corrupt,
        content_type=(
            "application/json"
        ),
        metadata={
            "sha256": _sha(
                corrupt
            ),
        },
        version_id=(
            "manifest-v1"
        ),
    )

    with pytest.raises(
        VaultProviderLossDrillError,
        match="recovery manifest metadata SHA-256 binding failed|recovery manifest full SHA-256",
    ):
        _run(
            seeded=seeded,
            output=(
                tmp_path
                / "recovery"
            ),
        )


def test_p27a6b_rejects_cross_tenant_replica_key(
    tmp_path,
):
    manifest, document = (
        _base_manifest()
    )

    document[
        "replica_key"
    ] = (
        document[
            "replica_key"
        ].replace(
            "/tenants/7/",
            "/tenants/8/",
        )
    )

    seeded = _seed_snapshot(
        manifest=manifest,
    )

    with pytest.raises(
        VaultProviderLossDrillError,
        match="tenant/content-address binding",
    ):
        _run(
            seeded=seeded,
            output=(
                tmp_path
                / "recovery"
            ),
        )


def test_p27a6b_rejects_primary_source_cross_tenant_binding(
    tmp_path,
):
    manifest, document = (
        _base_manifest()
    )

    document[
        "object_key"
    ] = (
        "vault/tenants/8/"
        "objects/source-object"
    )

    seeded = _seed_snapshot(
        manifest=manifest,
    )

    with pytest.raises(
        VaultProviderLossDrillError,
        match="source object violates tenant binding",
    ):
        _run(
            seeded=seeded,
            output=(
                tmp_path
                / "recovery"
            ),
        )


def test_p27a6b_rejects_corrupt_replica_payload(
    tmp_path,
):
    seeded = _seed_snapshot()

    document = (
        seeded[
            "document"
        ]
    )

    corrupt = bytearray(
        PDF
    )
    corrupt[6] = (
        ord("9")
        if corrupt[6]
        != ord("9")
        else ord("8")
    )
    corrupt = bytes(
        corrupt
    )

    assert len(
        corrupt
    ) == len(
        PDF
    )

    seeded[
        "storage"
    ].seed(
        key=document[
            "replica_key"
        ],
        data=corrupt,
        content_type=(
            "application/pdf"
        ),
        metadata={
            "sha256": (
                document[
                    "sha256"
                ]
            ),
            "organization-id": "7",
        },
        version_id=(
            "replica-payload-v1"
        ),
    )

    output = (
        tmp_path
        / "recovery"
    )

    with pytest.raises(
        VaultProviderLossDrillError,
        match="full SHA-256",
    ):
        _run(
            seeded=seeded,
            output=output,
        )

    assert not output.exists()


def test_p27a6b_rejects_replica_content_type_mismatch(
    tmp_path,
):
    seeded = _seed_snapshot(
        payload_content_type=(
            "application/json"
        )
    )

    with pytest.raises(
        VaultProviderLossDrillError,
        match="content-type binding",
    ):
        _run(
            seeded=seeded,
            output=(
                tmp_path
                / "recovery"
            ),
        )


def test_p27a6b_rejects_non_application_compatible_bytes(
    tmp_path,
):
    invalid_pdf = (
        b"NOT-A-PDF"
    )

    manifest, document = (
        _base_manifest(
            data=invalid_pdf,
            content_type=(
                "application/pdf"
            ),
        )
    )

    seeded = _seed_snapshot(
        manifest=manifest,
        payload=invalid_pdf,
        payload_content_type=(
            "application/pdf"
        ),
    )

    output = (
        tmp_path
        / "recovery"
    )

    with pytest.raises(
        VaultProviderLossDrillError,
        match="not application-compatible",
    ):
        _run(
            seeded=seeded,
            output=output,
        )

    assert not output.exists()


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
def test_p27a6b_state_only_documents_are_not_resurrected(
    tmp_path,
    status,
):
    manifest, _ = (
        _base_manifest()
    )

    _, state_document = (
        _base_manifest(
            status=status
        )
    )

    state_document = deepcopy(
        state_document
    )

    state_document[
        "public_id"
    ] = STATE_PUBLIC_ID

    state_document[
        "object_key"
    ] = (
        "vault/tenants/7/"
        "objects/state-only-object"
    )

    manifest[
        "documents"
    ].append(
        state_document
    )

    manifest[
        "document_count"
    ] = 2

    manifest[
        "state_only_document_count"
    ] = 1

    # One available object remains the real provider-loss payload.
    assert (
        manifest[
            "replicated_document_count"
        ]
        == 1
    )

    assert (
        manifest[
            "replica_object_count"
        ]
        == 1
    )

    seeded = _seed_snapshot(
        manifest=manifest,
    )

    output = (
        tmp_path
        / "recovery"
    )

    result = _run(
        seeded=seeded,
        output=output,
    )

    assert (
        result[
            "recovered_document_count"
        ]
        == 1
    )

    assert (
        result[
            "state_only_document_count"
        ]
        == 1
    )

    document_directory = (
        output
        / "tenants"
        / "7"
        / "documents"
        / STATE_PUBLIC_ID
    )

    assert (
        document_directory
        / "metadata.json"
    ).exists()

    assert not list(
        document_directory.glob(
            "payload.*"
        )
    )


def test_p27a6b_rejects_snapshot_without_recoverable_payload(
    tmp_path,
):
    manifest, _ = (
        _base_manifest(
            status="deleted"
        )
    )

    seeded = _seed_snapshot(
        manifest=manifest,
    )

    with pytest.raises(
        VaultProviderLossDrillError,
        match="at least one recovery-eligible Vault object",
    ):
        _run(
            seeded=seeded,
            output=(
                tmp_path
                / "recovery"
            ),
        )


def test_p27a6b_rejects_document_primary_bucket_mismatch(
    tmp_path,
):
    manifest, document = (
        _base_manifest()
    )

    document[
        "storage_bucket"
    ] = "unexpected-primary-bucket"

    seeded = _seed_snapshot(
        manifest=manifest,
    )

    with pytest.raises(
        VaultProviderLossDrillError,
        match="storage bucket does not match bound primary storage",
    ):
        _run(
            seeded=seeded,
            output=(
                tmp_path
                / "recovery"
            ),
        )


def test_p27a6b_rejects_existing_isolated_target(
    tmp_path,
):
    seeded = _seed_snapshot()

    output = (
        tmp_path
        / "recovery"
    )
    output.mkdir()

    with pytest.raises(
        VaultProviderLossDrillError,
        match="already exists",
    ):
        _run(
            seeded=seeded,
            output=output,
        )


def test_p27a6b_rejects_primary_configuration_in_provider_loss_mode():
    with pytest.raises(
        VaultProviderLossDrillError,
        match="Primary Vault configuration is present",
    ):
        assert_primary_provider_not_configured(
            {
                "VAULT_PRIMARY_BUCKET_NAME": (
                    "primary-vault"
                ),
                "VAULT_REPLICA_BUCKET_NAME": (
                    "secondary"
                ),
            }
        )


def test_p27a6b_accepts_environment_without_primary_configuration():
    assert_primary_provider_not_configured(
        {
            "VAULT_REPLICA_BUCKET_NAME": (
                "secondary"
            ),
            "VAULT_REPLICA_PROVIDER_ID": (
                "secondary-provider"
            ),
        }
    )


def test_p27a6b_rejects_replica_provider_identity_mismatch(
    tmp_path,
):
    seeded = _seed_snapshot()

    wrong_domain = RecoveryDomain(
        provider_id=(
            "different-provider"
        ),
        failure_domain=(
            "secondary-provider-global"
        ),
    )

    with pytest.raises(
        VaultProviderLossDrillError,
        match="replica-provider binding",
    ):
        run_provider_loss_drill(
            replica_storage=(
                seeded["storage"]
            ),
            replica_settings=(
                _settings()
            ),
            replica_domain=(
                wrong_domain
            ),
            complete_key=(
                seeded[
                    "complete_key"
                ]
            ),
            complete_version_id=(
                "complete-v1"
            ),
            expected_complete_sha256=(
                seeded[
                    "complete_sha"
                ]
            ),
            expected_snapshot_id=(
                SNAPSHOT_ID
            ),
            expected_source_label=(
                "p2-release-candidate"
            ),
            expected_release_commit=(
                RELEASE
            ),
            expected_alembic_revision=(
                ALEMBIC
            ),
            output_directory=(
                tmp_path
                / "recovery"
            ),
            operator="Jose Lezcano",
        )


def test_p27a6b_rejects_duplicate_public_document_binding(
    tmp_path,
):
    manifest, document = (
        _base_manifest()
    )

    second = deepcopy(
        document
    )

    manifest[
        "documents"
    ].append(
        second
    )
    manifest[
        "document_count"
    ] = 2
    manifest[
        "replicated_document_count"
    ] = 2

    # Both documents intentionally share the same CAS object.
    manifest[
        "replica_object_count"
    ] = 1

    seeded = _seed_snapshot(
        manifest=manifest,
    )

    with pytest.raises(
        VaultProviderLossDrillError,
        match="duplicate public document binding",
    ):
        _run(
            seeded=seeded,
            output=(
                tmp_path
                / "recovery"
            ),
        )
