from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import os
import re
import sys
import tempfile
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from litoral_trace.config.settings import StorageSettings
from litoral_trace.storage import (
    Boto3S3ObjectStorage,
    ObjectStorageClient,
    ObjectStorageError,
    ObjectStorageNotFoundError,
)
from scripts.postgres_logical_backup import (
    BackupToolError,
    build_identity_fingerprint,
    normalize_database_url,
    parse_database_url,
    reject_pooled_neon_endpoint,
    sanitize_cli_error_message,
)


MANIFEST_FORMAT_VERSION = "p27a6.vault-replica.manifest.v1"
COMPLETE_FORMAT_VERSION = "p27a6.vault-replica.complete.v1"
RESULT_FORMAT_VERSION = "p27a6.vault-replica.result.v1"

DEFAULT_PRIMARY_KEY_PREFIX = "vault"
DEFAULT_REPLICA_KEY_PREFIX = "litoral-trace/vault-recovery"

_REPLICABLE_STATUS = "available"
_KNOWN_STATUSES = frozenset(
    {
        "pending_upload",
        "available",
        "upload_failed",
        "delete_pending",
        "delete_failed",
        "deleted",
    }
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_COMMIT_RE = re.compile(r"^[0-9a-fA-F]{7,64}$")
_SNAPSHOT_RE = re.compile(r"^\d{8}T\d{6}Z$")


class VaultReplicaError(BackupToolError):
    """Fail-closed independent Vault-recovery replica error."""


@dataclass(frozen=True)
class RecoveryDomain:
    provider_id: str
    failure_domain: str


@dataclass(frozen=True)
class VaultReplicaDocument:
    organization_id: int
    public_id: str
    status: str
    storage_backend: str
    storage_bucket: str
    object_key: str
    storage_version_id: str | None
    size_bytes: int
    content_type: str
    sha256: str


@dataclass(frozen=True)
class VaultDatabaseSnapshot:
    database_name: str
    source_identity_sha256: str
    postgres_server_version: str
    postgis_version: str
    alembic_revision: str
    documents: tuple[VaultReplicaDocument, ...]


def _nonempty(value: object, *, label: str) -> str:
    normalized = str(
        value if value is not None else ""
    ).strip()
    if not normalized:
        raise VaultReplicaError(
            f"{label} must not be empty."
        )
    return normalized


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _sha256(value: object, *, label: str) -> str:
    normalized = _nonempty(
        value,
        label=label,
    ).lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise VaultReplicaError(
            f"{label} must be a canonical SHA-256."
        )
    return normalized


def _release_commit(value: object) -> str:
    normalized = _nonempty(
        value,
        label="release_commit",
    ).lower()
    if not _COMMIT_RE.fullmatch(normalized):
        raise VaultReplicaError(
            "release_commit is not a valid Git commit identifier."
        )
    return normalized


def _snapshot_id(value: object) -> str:
    normalized = _nonempty(
        value,
        label="snapshot_id",
    )
    if not _SNAPSHOT_RE.fullmatch(normalized):
        raise VaultReplicaError(
            "snapshot_id must use YYYYMMDDTHHMMSSZ."
        )
    return normalized


def _utc_iso(value: datetime) -> str:
    return value.astimezone(
        UTC
    ).replace(
        microsecond=0
    ).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def utc_snapshot_id(value: datetime | None = None) -> str:
    active = (
        value.astimezone(UTC)
        if value is not None
        else datetime.now(UTC)
    )
    return active.strftime(
        "%Y%m%dT%H%M%SZ"
    )


def _canonical_json_bytes(
    payload: dict[str, Any],
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


def _canonical_content_type(value: object) -> str:
    normalized = _nonempty(
        value,
        label="content_type",
    )
    return normalized.split(
        ";",
        1,
    )[0].strip().lower()


def _normalize_domain_value(
    value: object,
    *,
    label: str,
) -> str:
    normalized = _nonempty(
        value,
        label=label,
    ).strip().lower()

    if any(ord(char) < 32 for char in normalized):
        raise VaultReplicaError(
            f"{label} contains invalid control characters."
        )

    return normalized


def assert_independent_domains(
    *,
    primary: RecoveryDomain,
    replica: RecoveryDomain,
    primary_settings: StorageSettings,
    replica_settings: StorageSettings,
) -> None:
    primary_provider = _normalize_domain_value(
        primary.provider_id,
        label="primary provider_id",
    )
    replica_provider = _normalize_domain_value(
        replica.provider_id,
        label="replica provider_id",
    )

    if primary_provider == replica_provider:
        raise VaultReplicaError(
            "Primary and replica must use different storage providers."
        )

    primary_failure = _normalize_domain_value(
        primary.failure_domain,
        label="primary failure_domain",
    )
    replica_failure = _normalize_domain_value(
        replica.failure_domain,
        label="replica failure_domain",
    )

    if primary_failure == replica_failure:
        raise VaultReplicaError(
            "Primary and replica must use different failure domains."
        )

    primary_endpoint = (
        str(primary_settings.endpoint_url or "")
        .strip()
        .rstrip("/")
        .lower()
    )
    replica_endpoint = (
        str(replica_settings.endpoint_url or "")
        .strip()
        .rstrip("/")
        .lower()
    )

    primary_bucket = _nonempty(
        primary_settings.bucket_name,
        label="primary bucket",
    )
    replica_bucket = _nonempty(
        replica_settings.bucket_name,
        label="replica bucket",
    )

    if (
        primary_endpoint == replica_endpoint
        and primary_bucket == replica_bucket
    ):
        raise VaultReplicaError(
            "Primary and replica resolve to the same storage location."
        )


def _validate_document(
    document: VaultReplicaDocument,
) -> VaultReplicaDocument:
    try:
        organization_id = int(
            document.organization_id
        )
    except (TypeError, ValueError) as exc:
        raise VaultReplicaError(
            "Vault organization_id is invalid."
        ) from exc

    if organization_id <= 0:
        raise VaultReplicaError(
            "Vault organization_id is invalid."
        )

    try:
        public_id = str(
            UUID(
                _nonempty(
                    document.public_id,
                    label="Vault public_id",
                )
            )
        )
    except (ValueError, AttributeError) as exc:
        raise VaultReplicaError(
            "Vault public_id is invalid."
        ) from exc

    status = _nonempty(
        document.status,
        label="Vault status",
    )

    if status not in _KNOWN_STATUSES:
        raise VaultReplicaError(
            "Vault document has an unsupported status."
        )

    storage_backend = _nonempty(
        document.storage_backend,
        label="Vault storage_backend",
    ).lower()

    if storage_backend != "s3":
        raise VaultReplicaError(
            "Vault recovery replica supports only S3-compatible source objects."
        )

    storage_bucket = _nonempty(
        document.storage_bucket,
        label="Vault storage_bucket",
    )

    object_key = _nonempty(
        document.object_key,
        label="Vault object_key",
    )

    if object_key.startswith("/") or "\\" in object_key:
        raise VaultReplicaError(
            "Vault object_key is not canonical."
        )

    storage_version_id = _optional_string(
        document.storage_version_id
    )

    try:
        size_bytes = int(
            document.size_bytes
        )
    except (TypeError, ValueError) as exc:
        raise VaultReplicaError(
            "Vault size_bytes is invalid."
        ) from exc

    if size_bytes <= 0:
        raise VaultReplicaError(
            "Vault size_bytes must be positive."
        )

    content_type = _canonical_content_type(
        document.content_type
    )
    digest = _sha256(
        document.sha256,
        label="Vault sha256",
    )

    return VaultReplicaDocument(
        organization_id=organization_id,
        public_id=public_id,
        status=status,
        storage_backend=storage_backend,
        storage_bucket=storage_bucket,
        object_key=object_key,
        storage_version_id=storage_version_id,
        size_bytes=size_bytes,
        content_type=content_type,
        sha256=digest,
    )


def _document_from_row(
    row: dict[str, Any],
) -> VaultReplicaDocument:
    return _validate_document(
        VaultReplicaDocument(
            organization_id=row["organization_id"],
            public_id=row["public_id"],
            status=row["status"],
            storage_backend=row["storage_backend"],
            storage_bucket=row["storage_bucket"],
            object_key=row["object_key"],
            storage_version_id=row[
                "storage_version_id"
            ],
            size_bytes=row["size_bytes"],
            content_type=row["content_type"],
            sha256=row["sha256"],
        )
    )


def _assert_vault_backup_visibility(
    security_row: dict[str, Any] | None,
) -> None:
    if not security_row:
        raise VaultReplicaError(
            "Unable to verify Vault RLS backup visibility."
        )

    if (
        not bool(
            security_row.get("rls_enabled")
        )
        or not bool(
            security_row.get("rls_forced")
        )
    ):
        raise VaultReplicaError(
            "vault_documents RLS contract is not enabled and forced."
        )

    if not (
        bool(
            security_row.get("role_bypassrls")
        )
        or bool(
            security_row.get("role_superuser")
        )
    ):
        raise VaultReplicaError(
            "Vault backup database role cannot bypass tenant RLS; "
            "cross-tenant recovery metadata would be incomplete."
        )


def collect_vault_database_snapshot(
    database_url: str,
) -> VaultDatabaseSnapshot:
    normalized_url = normalize_database_url(
        database_url
    )
    parsed = parse_database_url(
        normalized_url
    )
    reject_pooled_neon_endpoint(
        parsed["hostname"]
    )

    try:
        with psycopg.connect(
            normalized_url,
            row_factory=dict_row,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SET TRANSACTION ISOLATION LEVEL "
                    "REPEATABLE READ READ ONLY"
                )

                cursor.execute(
                    "SELECT current_database() AS database_name"
                )
                database_row = cursor.fetchone()
                if not database_row:
                    raise VaultReplicaError(
                        "Unable to identify recovery database."
                    )
                database_name = _nonempty(
                    database_row["database_name"],
                    label="database_name",
                )

                cursor.execute(
                    "SHOW server_version"
                )
                postgres_row = cursor.fetchone()
                postgres_version = _nonempty(
                    postgres_row["server_version"]
                    if postgres_row
                    else None,
                    label="postgres_server_version",
                )

                cursor.execute(
                    """
                    SELECT version_num AS alembic_revision
                    FROM alembic_version
                    """
                )
                alembic_row = cursor.fetchone()
                alembic_revision = _nonempty(
                    alembic_row["alembic_revision"]
                    if alembic_row
                    else None,
                    label="alembic_revision",
                )

                cursor.execute(
                    """
                    SELECT extversion AS postgis_version
                    FROM pg_extension
                    WHERE extname = 'postgis'
                    """
                )
                postgis_row = cursor.fetchone()
                postgis_version = _nonempty(
                    postgis_row["postgis_version"]
                    if postgis_row
                    else None,
                    label="postgis_version",
                )

                cursor.execute(
                    """
                    SELECT
                        to_regclass(
                            'public.vault_documents'
                        ) AS vault_table
                    """
                )
                table_row = cursor.fetchone()
                if (
                    not table_row
                    or table_row["vault_table"] is None
                ):
                    raise VaultReplicaError(
                        "vault_documents is not present in the target schema."
                    )

                cursor.execute(
                    """
                    SELECT
                        c.relrowsecurity AS rls_enabled,
                        c.relforcerowsecurity AS rls_forced,
                        r.rolbypassrls AS role_bypassrls,
                        r.rolsuper AS role_superuser
                    FROM pg_class AS c
                    JOIN pg_namespace AS n
                      ON n.oid = c.relnamespace
                    JOIN pg_roles AS r
                      ON r.rolname = current_user
                    WHERE n.nspname = 'public'
                      AND c.relname = 'vault_documents'
                    """
                )

                _assert_vault_backup_visibility(
                    cursor.fetchone()
                )

                cursor.execute(
                    """
                    SELECT
                        organization_id,
                        public_id::text AS public_id,
                        status,
                        storage_backend,
                        storage_bucket,
                        object_key,
                        storage_version_id,
                        size_bytes,
                        content_type,
                        sha256
                    FROM vault_documents
                    ORDER BY organization_id, id
                    """
                )

                documents = tuple(
                    _document_from_row(row)
                    for row in cursor.fetchall()
                )

    except VaultReplicaError:
        raise
    except psycopg.Error as exc:
        raise VaultReplicaError(
            "Unable to read Vault recovery metadata from PostgreSQL."
        ) from exc

    source_identity_sha256 = (
        build_identity_fingerprint(
            hostname=parsed["hostname"],
            port=parsed["port"],
            database_name=database_name,
        )
    )

    return VaultDatabaseSnapshot(
        database_name=database_name,
        source_identity_sha256=(
            source_identity_sha256
        ),
        postgres_server_version=(
            postgres_version
        ),
        postgis_version=postgis_version,
        alembic_revision=alembic_revision,
        documents=documents,
    )


def _expected_primary_tenant_prefix(
    *,
    organization_id: int,
    primary_key_prefix: str,
) -> str:
    normalized_prefix = (
        str(primary_key_prefix or "")
        .strip()
        .strip("/")
    )

    if not normalized_prefix:
        raise VaultReplicaError(
            "Primary Vault key prefix is invalid."
        )

    return (
        f"{normalized_prefix}/tenants/"
        f"{organization_id}/objects/"
    )


def _replica_payload_key(
    *,
    organization_id: int,
    sha256: str,
    replica_key_prefix: str,
) -> str:
    normalized_prefix = (
        str(replica_key_prefix or "")
        .strip()
        .strip("/")
    )

    if not normalized_prefix:
        raise VaultReplicaError(
            "Replica key prefix is invalid."
        )

    return (
        f"{normalized_prefix}/tenants/"
        f"{organization_id}/objects/sha256/"
        f"{sha256[:2]}/{sha256}"
    )


def _verify_head(
    *,
    size_bytes: int,
    content_type: str | None,
    expected_size: int,
    expected_content_type: str,
    label: str,
) -> None:
    if int(size_bytes) != int(expected_size):
        raise VaultReplicaError(
            f"{label} size does not match PostgreSQL metadata."
        )

    if (
        content_type is None
        or not str(content_type).strip()
    ):
        raise VaultReplicaError(
            f"{label} content type is missing."
        )

    observed_type = _canonical_content_type(
        content_type
    )

    if observed_type != expected_content_type:
        raise VaultReplicaError(
            f"{label} content type does not match PostgreSQL metadata."
        )


def _stream_digest(
    stream,
    *,
    spool=None,
) -> tuple[int, str]:
    digest = hashlib.sha256()
    total = 0

    while True:
        chunk = stream.read(
            1024 * 1024
        )
        if not chunk:
            break

        total += len(chunk)
        digest.update(chunk)

        if spool is not None:
            spool.write(chunk)

    return total, digest.hexdigest()


def _verify_replica_object(
    *,
    replica_storage: ObjectStorageClient,
    key: str,
    version_id: str | None,
    document: VaultReplicaDocument,
) -> None:
    try:
        head = replica_storage.head_object(
            key=key,
            version_id=version_id,
        )
    except ObjectStorageNotFoundError as exc:
        raise VaultReplicaError(
            "Replica object disappeared during verification."
        ) from exc
    except ObjectStorageError as exc:
        raise VaultReplicaError(
            "Replica object verification failed."
        ) from exc

    _verify_head(
        size_bytes=head.size_bytes,
        content_type=head.content_type,
        expected_size=document.size_bytes,
        expected_content_type=(
            document.content_type
        ),
        label="Replica object",
    )

    metadata_sha = str(
        head.metadata.get(
            "sha256",
            "",
        )
    ).strip().lower()

    if metadata_sha != document.sha256:
        raise VaultReplicaError(
            "Replica object SHA-256 metadata is inconsistent."
        )

    metadata_org = str(
        head.metadata.get(
            "organization-id",
            "",
        )
    ).strip()

    if metadata_org != str(
        document.organization_id
    ):
        raise VaultReplicaError(
            "Replica object tenant binding is inconsistent."
        )

    if (
        version_id is not None
        and head.version_id != version_id
    ):
        raise VaultReplicaError(
            "Replica object version binding is inconsistent."
        )

    try:
        with replica_storage.get_object_stream(
            key=key,
            version_id=version_id,
        ) as stream:
            _verify_head(
                size_bytes=stream.head.size_bytes,
                content_type=stream.head.content_type,
                expected_size=document.size_bytes,
                expected_content_type=(
                    document.content_type
                ),
                label="Replica stream",
            )

            total, digest = _stream_digest(
                stream
            )
    except ObjectStorageNotFoundError as exc:
        raise VaultReplicaError(
            "Replica object disappeared during full verification."
        ) from exc
    except ObjectStorageError as exc:
        raise VaultReplicaError(
            "Replica full-object verification failed."
        ) from exc

    if total != document.size_bytes:
        raise VaultReplicaError(
            "Replica object byte count is inconsistent."
        )

    if digest != document.sha256:
        raise VaultReplicaError(
            "Replica object full SHA-256 verification failed."
        )


def _ensure_replica_payload(
    *,
    replica_storage: ObjectStorageClient,
    document: VaultReplicaDocument,
    replica_key: str,
    verified_spool,
) -> tuple[str | None, str]:
    try:
        existing = replica_storage.head_object(
            key=replica_key,
        )
    except ObjectStorageNotFoundError:
        existing = None
    except ObjectStorageError as exc:
        raise VaultReplicaError(
            "Unable to inspect replica object."
        ) from exc

    if existing is not None:
        _verify_replica_object(
            replica_storage=replica_storage,
            key=replica_key,
            version_id=existing.version_id,
            document=document,
        )
        return (
            existing.version_id,
            "reused_verified",
        )

    verified_spool.seek(0)

    try:
        write_result = replica_storage.put_object(
            key=replica_key,
            body=verified_spool,
            content_type=document.content_type,
            content_length=document.size_bytes,
            metadata={
                "sha256": document.sha256,
                "organization-id": str(
                    document.organization_id
                ),
            },
        )
    except ObjectStorageError as exc:
        raise VaultReplicaError(
            "Unable to publish verified Vault recovery payload."
        ) from exc

    _verify_replica_object(
        replica_storage=replica_storage,
        key=replica_key,
        version_id=write_result.version_id,
        document=document,
    )

    return (
        write_result.version_id,
        "copied_verified",
    )


def _replicate_available_document(
    *,
    document: VaultReplicaDocument,
    primary_storage: ObjectStorageClient,
    replica_storage: ObjectStorageClient,
    primary_settings: StorageSettings,
    replica_key_prefix: str,
) -> dict[str, Any]:
    primary_bucket = _nonempty(
        primary_settings.bucket_name,
        label="primary bucket",
    )

    if document.storage_bucket != primary_bucket:
        raise VaultReplicaError(
            "Vault document storage bucket does not match the configured primary."
        )

    expected_prefix = (
        _expected_primary_tenant_prefix(
            organization_id=(
                document.organization_id
            ),
            primary_key_prefix=(
                primary_settings.normalized_key_prefix
            ),
        )
    )

    if not document.object_key.startswith(
        expected_prefix
    ):
        raise VaultReplicaError(
            "Vault document object key violates tenant binding."
        )

    try:
        source_stream = (
            primary_storage.get_object_stream(
                key=document.object_key,
                version_id=(
                    document.storage_version_id
                ),
            )
        )
    except ObjectStorageNotFoundError as exc:
        raise VaultReplicaError(
            "Available Vault source object is missing."
        ) from exc
    except ObjectStorageError as exc:
        raise VaultReplicaError(
            "Unable to read primary Vault source object."
        ) from exc

    with source_stream:
        _verify_head(
            size_bytes=(
                source_stream.head.size_bytes
            ),
            content_type=(
                source_stream.head.content_type
            ),
            expected_size=document.size_bytes,
            expected_content_type=(
                document.content_type
            ),
            label="Primary Vault object",
        )

        if document.storage_version_id:
            if (
                source_stream.head.version_id
                != document.storage_version_id
            ):
                raise VaultReplicaError(
                    "Primary Vault exact-version verification failed."
                )

        with tempfile.TemporaryFile(
            mode="w+b"
        ) as spool:
            total, digest = _stream_digest(
                source_stream,
                spool=spool,
            )

            if total != document.size_bytes:
                raise VaultReplicaError(
                    "Primary Vault object byte count is inconsistent."
                )

            if digest != document.sha256:
                raise VaultReplicaError(
                    "Primary Vault object SHA-256 does not match PostgreSQL."
                )

            replica_key = _replica_payload_key(
                organization_id=(
                    document.organization_id
                ),
                sha256=document.sha256,
                replica_key_prefix=(
                    replica_key_prefix
                ),
            )

            replica_version_id, state = (
                _ensure_replica_payload(
                    replica_storage=(
                        replica_storage
                    ),
                    document=document,
                    replica_key=replica_key,
                    verified_spool=spool,
                )
            )

    return {
        "replica_state": state,
        "recovery_eligible": True,
        "replica_key": replica_key,
        "replica_version_id": (
            replica_version_id
        ),
        "source_exact_version_verified": (
            document.storage_version_id
            is not None
        ),
    }


def _base_manifest_entry(
    document: VaultReplicaDocument,
) -> dict[str, Any]:
    return {
        "organization_id": (
            document.organization_id
        ),
        "public_id": document.public_id,
        "status": document.status,
        "storage_backend": (
            document.storage_backend
        ),
        "storage_bucket": (
            document.storage_bucket
        ),
        "object_key": document.object_key,
        "storage_version_id": (
            document.storage_version_id
        ),
        "size_bytes": document.size_bytes,
        "content_type": (
            document.content_type
        ),
        "sha256": document.sha256,
    }


def _verify_json_artifact(
    *,
    storage: ObjectStorageClient,
    key: str,
    version_id: str | None,
    expected_bytes: bytes,
    expected_sha256: str,
    artifact_kind: str,
) -> None:
    try:
        head = storage.head_object(
            key=key,
            version_id=version_id,
        )
    except ObjectStorageNotFoundError as exc:
        raise VaultReplicaError(
            f"{artifact_kind} disappeared during verification."
        ) from exc
    except ObjectStorageError as exc:
        raise VaultReplicaError(
            f"{artifact_kind} verification failed."
        ) from exc

    if head.size_bytes != len(
        expected_bytes
    ):
        raise VaultReplicaError(
            f"{artifact_kind} size verification failed."
        )

    if _canonical_content_type(
        head.content_type or ""
    ) != "application/json":
        raise VaultReplicaError(
            f"{artifact_kind} content type verification failed."
        )

    metadata_sha = str(
        head.metadata.get(
            "sha256",
            "",
        )
    ).strip().lower()

    if metadata_sha != expected_sha256:
        raise VaultReplicaError(
            f"{artifact_kind} metadata SHA-256 verification failed."
        )

    try:
        with storage.get_object_stream(
            key=key,
            version_id=version_id,
        ) as stream:
            total, digest = _stream_digest(
                stream
            )
    except ObjectStorageError as exc:
        raise VaultReplicaError(
            f"{artifact_kind} full verification failed."
        ) from exc

    if (
        total != len(expected_bytes)
        or digest != expected_sha256
    ):
        raise VaultReplicaError(
            f"{artifact_kind} full SHA-256 verification failed."
        )


def _publish_immutable_json(
    *,
    storage: ObjectStorageClient,
    key: str,
    payload: dict[str, Any],
    artifact_kind: str,
) -> tuple[str, str | None]:
    body = _canonical_json_bytes(
        payload
    )
    digest = hashlib.sha256(
        body
    ).hexdigest()

    try:
        existing = storage.head_object(
            key=key
        )
    except ObjectStorageNotFoundError:
        existing = None
    except ObjectStorageError as exc:
        raise VaultReplicaError(
            f"Unable to inspect {artifact_kind}."
        ) from exc

    if existing is not None:
        _verify_json_artifact(
            storage=storage,
            key=key,
            version_id=existing.version_id,
            expected_bytes=body,
            expected_sha256=digest,
            artifact_kind=artifact_kind,
        )
        return digest, existing.version_id

    try:
        result = storage.put_object(
            key=key,
            body=body,
            content_type="application/json",
            content_length=len(body),
            metadata={
                "sha256": digest,
                "artifact-kind": artifact_kind,
            },
        )
    except ObjectStorageError as exc:
        raise VaultReplicaError(
            f"Unable to publish {artifact_kind}."
        ) from exc

    _verify_json_artifact(
        storage=storage,
        key=key,
        version_id=result.version_id,
        expected_bytes=body,
        expected_sha256=digest,
        artifact_kind=artifact_kind,
    )

    return digest, result.version_id


def replicate_vault_snapshot(
    *,
    database_snapshot: VaultDatabaseSnapshot,
    primary_storage: ObjectStorageClient,
    replica_storage: ObjectStorageClient,
    primary_settings: StorageSettings,
    replica_settings: StorageSettings,
    primary_domain: RecoveryDomain,
    replica_domain: RecoveryDomain,
    source_label: str,
    release_commit: str,
    snapshot_id: str,
    now_utc: datetime,
) -> dict[str, Any]:
    assert_independent_domains(
        primary=primary_domain,
        replica=replica_domain,
        primary_settings=primary_settings,
        replica_settings=replica_settings,
    )

    normalized_source_label = _nonempty(
        source_label,
        label="source_label",
    )
    normalized_release = _release_commit(
        release_commit
    )
    normalized_snapshot_id = _snapshot_id(
        snapshot_id
    )

    active_now = now_utc.astimezone(
        UTC
    )

    replica_prefix = (
        replica_settings.normalized_key_prefix
    )

    entries: list[dict[str, Any]] = []
    replica_keys: set[str] = set()
    replicated_count = 0
    state_only_count = 0

    for raw_document in (
        database_snapshot.documents
    ):
        document = _validate_document(
            raw_document
        )
        entry = _base_manifest_entry(
            document
        )

        if document.status == _REPLICABLE_STATUS:
            recovery = (
                _replicate_available_document(
                    document=document,
                    primary_storage=(
                        primary_storage
                    ),
                    replica_storage=(
                        replica_storage
                    ),
                    primary_settings=(
                        primary_settings
                    ),
                    replica_key_prefix=(
                        replica_prefix
                    ),
                )
            )
            entry.update(
                recovery
            )
            replica_keys.add(
                recovery["replica_key"]
            )
            replicated_count += 1
        else:
            entry.update(
                {
                    "replica_state": (
                        "state_only_not_auto_recoverable"
                    ),
                    "recovery_eligible": False,
                    "replica_key": None,
                    "replica_version_id": None,
                    "source_exact_version_verified": False,
                }
            )
            state_only_count += 1

        entries.append(entry)

    manifest = {
        "format_version": MANIFEST_FORMAT_VERSION,
        "created_at_utc": _utc_iso(
            active_now
        ),
        "snapshot_id": normalized_snapshot_id,
        "source_label": (
            normalized_source_label
        ),
        "release_commit": normalized_release,
        "source_database": {
            "database_name": (
                database_snapshot.database_name
            ),
            "source_identity_sha256": (
                database_snapshot.source_identity_sha256
            ),
            "postgres_server_version": (
                database_snapshot.postgres_server_version
            ),
            "postgis_version": (
                database_snapshot.postgis_version
            ),
            "alembic_revision": (
                database_snapshot.alembic_revision
            ),
        },
        "primary_storage": {
            "provider_id": (
                _normalize_domain_value(
                    primary_domain.provider_id,
                    label="primary provider_id",
                )
            ),
            "failure_domain": (
                _normalize_domain_value(
                    primary_domain.failure_domain,
                    label="primary failure_domain",
                )
            ),
            "bucket_name": (
                _nonempty(
                    primary_settings.bucket_name,
                    label="primary bucket",
                )
            ),
            "key_prefix": (
                primary_settings.normalized_key_prefix
            ),
        },
        "replica_storage": {
            "provider_id": (
                _normalize_domain_value(
                    replica_domain.provider_id,
                    label="replica provider_id",
                )
            ),
            "failure_domain": (
                _normalize_domain_value(
                    replica_domain.failure_domain,
                    label="replica failure_domain",
                )
            ),
            "bucket_name": (
                _nonempty(
                    replica_settings.bucket_name,
                    label="replica bucket",
                )
            ),
            "key_prefix": replica_prefix,
        },
        "document_count": len(entries),
        "replicated_document_count": (
            replicated_count
        ),
        "state_only_document_count": (
            state_only_count
        ),
        "replica_object_count": len(
            replica_keys
        ),
        "documents": entries,
    }

    manifest_key = (
        f"{replica_prefix}/snapshots/"
        f"{normalized_snapshot_id}/manifest.json"
    )

    manifest_sha256, manifest_version_id = (
        _publish_immutable_json(
            storage=replica_storage,
            key=manifest_key,
            payload=manifest,
            artifact_kind=(
                "vault-recovery-manifest"
            ),
        )
    )

    complete = {
        "format_version": COMPLETE_FORMAT_VERSION,
        "snapshot_id": normalized_snapshot_id,
        "source_label": normalized_source_label,
        "release_commit": normalized_release,
        "published_at_utc": _utc_iso(
            active_now
        ),
        "source_identity_sha256": (
            database_snapshot.source_identity_sha256
        ),
        "alembic_revision": (
            database_snapshot.alembic_revision
        ),
        "replica_provider_id": (
            _normalize_domain_value(
                replica_domain.provider_id,
                label="replica provider_id",
            )
        ),
        "replica_failure_domain": (
            _normalize_domain_value(
                replica_domain.failure_domain,
                label="replica failure_domain",
            )
        ),
        "manifest_key": manifest_key,
        "manifest_sha256": (
            manifest_sha256
        ),
        "manifest_version_id": (
            manifest_version_id
        ),
        "document_count": len(entries),
        "replicated_document_count": (
            replicated_count
        ),
        "state_only_document_count": (
            state_only_count
        ),
        "replica_object_count": len(
            replica_keys
        ),
    }

    complete_key = (
        f"{replica_prefix}/snapshots/"
        f"{normalized_snapshot_id}/complete.json"
    )

    complete_sha256, complete_version_id = (
        _publish_immutable_json(
            storage=replica_storage,
            key=complete_key,
            payload=complete,
            artifact_kind=(
                "vault-recovery-complete"
            ),
        )
    )

    return {
        "format_version": RESULT_FORMAT_VERSION,
        "result": "PASS",
        "snapshot_id": normalized_snapshot_id,
        "source_label": normalized_source_label,
        "release_commit": normalized_release,
        "source_identity_sha256": (
            database_snapshot.source_identity_sha256
        ),
        "alembic_revision": (
            database_snapshot.alembic_revision
        ),
        "document_count": len(entries),
        "replicated_document_count": (
            replicated_count
        ),
        "state_only_document_count": (
            state_only_count
        ),
        "replica_object_count": len(
            replica_keys
        ),
        "manifest_key": manifest_key,
        "manifest_sha256": (
            manifest_sha256
        ),
        "complete_key": complete_key,
        "complete_sha256": (
            complete_sha256
        ),
        "complete_version_id": (
            complete_version_id
        ),
    }


def _read_bool_env(
    variable_name: str,
    *,
    default: bool,
) -> bool:
    raw = os.environ.get(
        variable_name
    )

    if raw is None or not raw.strip():
        return default

    normalized = raw.strip().lower()

    if normalized in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return True

    if normalized in {
        "0",
        "false",
        "no",
        "off",
    }:
        return False

    raise VaultReplicaError(
        f"{variable_name} must be boolean."
    )


def _env_optional(
    variable_name: str,
) -> str | None:
    value = os.environ.get(
        variable_name
    )
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _env_required(
    variable_name: str,
) -> str:
    value = _env_optional(
        variable_name
    )
    if value is None:
        raise VaultReplicaError(
            f"{variable_name} is required."
        )
    return value


def _storage_from_environment(
    *,
    prefix: str,
    default_key_prefix: str,
) -> StorageSettings:
    try:
        settings = StorageSettings(
            backend="s3",
            bucket_name=_env_required(
                f"{prefix}_BUCKET_NAME"
            ),
            region=(
                _env_optional(
                    f"{prefix}_REGION"
                )
                or "us-east-1"
            ),
            endpoint_url=_env_optional(
                f"{prefix}_ENDPOINT_URL"
            ),
            access_key_id=_env_optional(
                f"{prefix}_ACCESS_KEY_ID"
            ),
            secret_access_key=_env_optional(
                f"{prefix}_SECRET_ACCESS_KEY"
            ),
            session_token=_env_optional(
                f"{prefix}_SESSION_TOKEN"
            ),
            force_path_style=_read_bool_env(
                f"{prefix}_FORCE_PATH_STYLE",
                default=False,
            ),
            use_tls=_read_bool_env(
                f"{prefix}_USE_TLS",
                default=True,
            ),
            verify_tls=_read_bool_env(
                f"{prefix}_VERIFY_TLS",
                default=True,
            ),
            ca_bundle_path=_env_optional(
                f"{prefix}_CA_BUNDLE_PATH"
            ),
            key_prefix=(
                _env_optional(
                    f"{prefix}_KEY_PREFIX"
                )
                or default_key_prefix
            ),
        )
    except VaultReplicaError:
        raise
    except Exception as exc:
        raise VaultReplicaError(
            f"{prefix} storage configuration is invalid."
        ) from exc

    if (
        not settings.use_tls
        or not settings.verify_tls
    ):
        raise VaultReplicaError(
            f"{prefix} recovery storage requires TLS verification."
        )

    return settings


def _domain_from_environment(
    *,
    prefix: str,
) -> RecoveryDomain:
    return RecoveryDomain(
        provider_id=_env_required(
            f"{prefix}_PROVIDER_ID"
        ),
        failure_domain=_env_required(
            f"{prefix}_FAILURE_DOMAIN"
        ),
    )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create and verify an independent "
            "tenant-scoped Vault recovery replica."
        )
    )

    parser.add_argument(
        "--source-label",
        default="production",
    )
    parser.add_argument(
        "--release-commit",
        required=True,
    )
    parser.add_argument(
        "--snapshot-id",
        default=None,
    )

    return parser


def main(
    argv: list[str] | None = None,
) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(
        argv
    )

    try:
        database_url = _env_required(
            "VAULT_BACKUP_DATABASE_URL"
        )
        database_url = normalize_database_url(
            database_url
        )

        primary_settings = (
            _storage_from_environment(
                prefix="VAULT_PRIMARY",
                default_key_prefix=(
                    DEFAULT_PRIMARY_KEY_PREFIX
                ),
            )
        )

        replica_settings = (
            _storage_from_environment(
                prefix="VAULT_REPLICA",
                default_key_prefix=(
                    DEFAULT_REPLICA_KEY_PREFIX
                ),
            )
        )

        primary_domain = (
            _domain_from_environment(
                prefix="VAULT_PRIMARY"
            )
        )
        replica_domain = (
            _domain_from_environment(
                prefix="VAULT_REPLICA"
            )
        )

        assert_independent_domains(
            primary=primary_domain,
            replica=replica_domain,
            primary_settings=primary_settings,
            replica_settings=replica_settings,
        )

        primary_storage = (
            Boto3S3ObjectStorage(
                primary_settings
            )
        )
        replica_storage = (
            Boto3S3ObjectStorage(
                replica_settings
            )
        )

        if not primary_storage.health_check():
            raise VaultReplicaError(
                "Primary Vault storage readiness failed."
            )

        if not replica_storage.health_check():
            raise VaultReplicaError(
                "Independent Vault replica readiness failed."
            )

        database_snapshot = (
            collect_vault_database_snapshot(
                database_url
            )
        )

        now_utc = datetime.now(
            UTC
        )

        snapshot_id = (
            args.snapshot_id
            or utc_snapshot_id(
                now_utc
            )
        )

        result = replicate_vault_snapshot(
            database_snapshot=(
                database_snapshot
            ),
            primary_storage=(
                primary_storage
            ),
            replica_storage=(
                replica_storage
            ),
            primary_settings=(
                primary_settings
            ),
            replica_settings=(
                replica_settings
            ),
            primary_domain=(
                primary_domain
            ),
            replica_domain=(
                replica_domain
            ),
            source_label=(
                args.source_label
            ),
            release_commit=(
                args.release_commit
            ),
            snapshot_id=(
                snapshot_id
            ),
            now_utc=now_utc,
        )

    except VaultReplicaError as exc:
        print(
            sanitize_cli_error_message(
                str(exc)
            ),
            file=sys.stderr,
        )
        return 1
    except BackupToolError as exc:
        print(
            sanitize_cli_error_message(
                str(exc)
            ),
            file=sys.stderr,
        )
        return 1
    except ObjectStorageError:
        print(
            "Vault recovery storage operation failed.",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Vault recovery replica operational failure.",
            file=sys.stderr,
        )
        return 1

    print(
        json.dumps(
            result,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
