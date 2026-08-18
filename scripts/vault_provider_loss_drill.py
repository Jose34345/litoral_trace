from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import time
from typing import Any, Mapping
from uuid import UUID, uuid4

from litoral_trace.config.settings import StorageSettings
from litoral_trace.services.vault import (
    VaultValidationError,
    validate_vault_upload,
)
from litoral_trace.storage import (
    Boto3S3ObjectStorage,
    ObjectStorageClient,
    ObjectStorageError,
    ObjectStorageNotFoundError,
)
from scripts.postgres_logical_backup import (
    sanitize_cli_error_message,
)
from scripts.vault_recovery_replica import (
    COMPLETE_FORMAT_VERSION,
    DEFAULT_REPLICA_KEY_PREFIX,
    MANIFEST_FORMAT_VERSION,
    RecoveryDomain,
)


DRILL_FORMAT_VERSION = "p27a6.vault-provider-loss-drill.v1"
INDEX_FORMAT_VERSION = "p27a6.vault-provider-loss-index.v1"

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

_RECOVERABLE_REPLICA_STATES = frozenset(
    {
        "copied_verified",
        "reused_verified",
    }
)

_CONTENT_TYPE_EXTENSION = {
    "application/pdf": ".pdf",
    "application/json": ".json",
    (
        "application/"
        "vnd.openxmlformats-officedocument."
        "spreadsheetml.sheet"
    ): ".xlsx",
}

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_COMMIT_RE = re.compile(r"^[0-9a-fA-F]{7,64}$")
_SNAPSHOT_RE = re.compile(r"^\d{8}T\d{6}Z$")

_PRIMARY_ENV_PREFIX = "VAULT_PRIMARY_"


class VaultProviderLossDrillError(RuntimeError):
    """Fail-closed Vault provider-loss recovery-drill failure."""


@dataclass(frozen=True)
class DrillDocument:
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

    recovery_eligible: bool
    replica_state: str
    replica_key: str | None
    replica_version_id: str | None

    source_exact_version_verified: bool


@dataclass(frozen=True)
class ValidatedSnapshot:
    snapshot_id: str
    source_label: str
    release_commit: str
    source_identity_sha256: str
    alembic_revision: str

    document_count: int
    replicated_document_count: int
    state_only_document_count: int
    replica_object_count: int

    manifest_key: str
    manifest_sha256: str
    documents: tuple[DrillDocument, ...]


def _nonempty(
    value: object,
    *,
    label: str,
) -> str:
    normalized = str(
        value if value is not None else ""
    ).strip()

    if not normalized:
        raise VaultProviderLossDrillError(
            f"{label} must not be empty."
        )

    return normalized


def _optional_string(
    value: object,
) -> str | None:
    if value is None:
        return None

    normalized = str(value).strip()
    return normalized or None


def _sha256(
    value: object,
    *,
    label: str,
) -> str:
    normalized = _nonempty(
        value,
        label=label,
    ).lower()

    if not _SHA256_RE.fullmatch(
        normalized
    ):
        raise VaultProviderLossDrillError(
            f"{label} must be a canonical SHA-256."
        )

    return normalized


def _snapshot_id(
    value: object,
) -> str:
    normalized = _nonempty(
        value,
        label="snapshot_id",
    )

    if not _SNAPSHOT_RE.fullmatch(
        normalized
    ):
        raise VaultProviderLossDrillError(
            "snapshot_id must use YYYYMMDDTHHMMSSZ."
        )

    return normalized


def _release_commit(
    value: object,
) -> str:
    normalized = _nonempty(
        value,
        label="release_commit",
    ).lower()

    if not _COMMIT_RE.fullmatch(
        normalized
    ):
        raise VaultProviderLossDrillError(
            "release_commit is invalid."
        )

    return normalized


def _canonical_content_type(
    value: object,
) -> str:
    normalized = _nonempty(
        value,
        label="content_type",
    )

    return normalized.split(
        ";",
        1,
    )[0].strip().lower()


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


def _canonical_key(
    value: object,
    *,
    label: str,
) -> str:
    key = _nonempty(
        value,
        label=label,
    )

    if (
        key.startswith("/")
        or "\\" in key
        or any(
            ord(character) < 32
            for character in key
        )
        or any(
            segment in {
                "",
                ".",
                "..",
            }
            for segment in key.split("/")
        )
    ):
        raise VaultProviderLossDrillError(
            f"{label} is not canonical."
        )

    return key


def _positive_int(
    value: object,
    *,
    label: str,
) -> int:
    try:
        normalized = int(value)
    except (
        TypeError,
        ValueError,
    ) as exc:
        raise VaultProviderLossDrillError(
            f"{label} is invalid."
        ) from exc

    if normalized <= 0:
        raise VaultProviderLossDrillError(
            f"{label} must be positive."
        )

    return normalized


def _nonnegative_int(
    value: object,
    *,
    label: str,
) -> int:
    try:
        normalized = int(value)
    except (
        TypeError,
        ValueError,
    ) as exc:
        raise VaultProviderLossDrillError(
            f"{label} is invalid."
        ) from exc

    if normalized < 0:
        raise VaultProviderLossDrillError(
            f"{label} must not be negative."
        )

    return normalized


def _uuid(
    value: object,
    *,
    label: str,
) -> str:
    try:
        return str(
            UUID(
                _nonempty(
                    value,
                    label=label,
                )
            )
        )
    except (
        ValueError,
        AttributeError,
    ) as exc:
        raise VaultProviderLossDrillError(
            f"{label} is invalid."
        ) from exc


def _domain_value(
    value: object,
    *,
    label: str,
) -> str:
    normalized = _nonempty(
        value,
        label=label,
    ).lower()

    if any(
        ord(character) < 32
        for character in normalized
    ):
        raise VaultProviderLossDrillError(
            f"{label} contains control characters."
        )

    return normalized


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
        raise VaultProviderLossDrillError(
            f"{variable_name} is required."
        )

    return value


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

    raise VaultProviderLossDrillError(
        f"{variable_name} must be boolean."
    )


def _replica_settings_from_environment() -> StorageSettings:
    try:
        settings = StorageSettings(
            backend="s3",
            bucket_name=_env_required(
                "VAULT_REPLICA_BUCKET_NAME"
            ),
            region=(
                _env_optional(
                    "VAULT_REPLICA_REGION"
                )
                or "us-east-1"
            ),
            endpoint_url=_env_optional(
                "VAULT_REPLICA_ENDPOINT_URL"
            ),
            access_key_id=_env_optional(
                "VAULT_REPLICA_ACCESS_KEY_ID"
            ),
            secret_access_key=_env_optional(
                "VAULT_REPLICA_SECRET_ACCESS_KEY"
            ),
            session_token=_env_optional(
                "VAULT_REPLICA_SESSION_TOKEN"
            ),
            force_path_style=_read_bool_env(
                "VAULT_REPLICA_FORCE_PATH_STYLE",
                default=False,
            ),
            use_tls=_read_bool_env(
                "VAULT_REPLICA_USE_TLS",
                default=True,
            ),
            verify_tls=_read_bool_env(
                "VAULT_REPLICA_VERIFY_TLS",
                default=True,
            ),
            ca_bundle_path=_env_optional(
                "VAULT_REPLICA_CA_BUNDLE_PATH"
            ),
            key_prefix=(
                _env_optional(
                    "VAULT_REPLICA_KEY_PREFIX"
                )
                or DEFAULT_REPLICA_KEY_PREFIX
            ),
        )
    except VaultProviderLossDrillError:
        raise
    except Exception as exc:
        raise VaultProviderLossDrillError(
            "VAULT_REPLICA storage configuration is invalid."
        ) from exc

    if (
        not settings.use_tls
        or not settings.verify_tls
    ):
        raise VaultProviderLossDrillError(
            "Replica recovery access requires TLS verification."
        )

    return settings


def _replica_domain_from_environment() -> RecoveryDomain:
    return RecoveryDomain(
        provider_id=_env_required(
            "VAULT_REPLICA_PROVIDER_ID"
        ),
        failure_domain=_env_required(
            "VAULT_REPLICA_FAILURE_DOMAIN"
        ),
    )


def assert_primary_provider_not_configured(
    environment: Mapping[str, str] | None = None,
) -> None:
    source = (
        os.environ
        if environment is None
        else environment
    )

    configured = sorted(
        name
        for name, value in source.items()
        if (
            name.startswith(
                _PRIMARY_ENV_PREFIX
            )
            and str(value).strip()
        )
    )

    if configured:
        raise VaultProviderLossDrillError(
            "Primary Vault configuration is present during "
            "provider-loss mode."
        )


def _verify_json_artifact(
    *,
    storage: ObjectStorageClient,
    key: str,
    version_id: str | None,
    expected_sha256: str,
    artifact_kind: str,
) -> tuple[
    dict[str, Any],
    str,
    str | None,
]:
    canonical_key = _canonical_key(
        key,
        label=f"{artifact_kind} key",
    )
    expected_digest = _sha256(
        expected_sha256,
        label=f"{artifact_kind} expected SHA-256",
    )

    try:
        head = storage.head_object(
            key=canonical_key,
            version_id=version_id,
        )
    except ObjectStorageNotFoundError as exc:
        raise VaultProviderLossDrillError(
            f"{artifact_kind} is missing."
        ) from exc
    except ObjectStorageError as exc:
        raise VaultProviderLossDrillError(
            f"{artifact_kind} HEAD verification failed."
        ) from exc

    if (
        version_id is not None
        and head.version_id != version_id
    ):
        raise VaultProviderLossDrillError(
            f"{artifact_kind} version binding failed."
        )

    if (
        head.content_type is None
        or _canonical_content_type(
            head.content_type
        )
        != "application/json"
    ):
        raise VaultProviderLossDrillError(
            f"{artifact_kind} content type is invalid."
        )

    metadata_digest = str(
        head.metadata.get(
            "sha256",
            "",
        )
    ).strip().lower()

    if metadata_digest != expected_digest:
        raise VaultProviderLossDrillError(
            f"{artifact_kind} metadata SHA-256 binding failed."
        )

    digest = hashlib.sha256()
    body = bytearray()
    total = 0

    try:
        with storage.get_object_stream(
            key=canonical_key,
            version_id=version_id,
        ) as stream:
            if (
                version_id is not None
                and stream.head.version_id
                != version_id
            ):
                raise VaultProviderLossDrillError(
                    f"{artifact_kind} stream version binding failed."
                )

            if (
                stream.head.content_type is None
                or _canonical_content_type(
                    stream.head.content_type
                )
                != "application/json"
            ):
                raise VaultProviderLossDrillError(
                    f"{artifact_kind} stream content type is invalid."
                )

            while True:
                chunk = stream.read(
                    1024 * 1024
                )

                if not chunk:
                    break

                total += len(chunk)
                digest.update(chunk)
                body.extend(chunk)

    except VaultProviderLossDrillError:
        raise
    except ObjectStorageError as exc:
        raise VaultProviderLossDrillError(
            f"{artifact_kind} full read failed."
        ) from exc

    observed_digest = (
        digest.hexdigest()
    )

    if observed_digest != expected_digest:
        raise VaultProviderLossDrillError(
            f"{artifact_kind} full SHA-256 verification failed."
        )

    if head.size_bytes != total:
        raise VaultProviderLossDrillError(
            f"{artifact_kind} size verification failed."
        )

    try:
        payload = json.loads(
            bytes(body).decode(
                "utf-8"
            )
        )
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise VaultProviderLossDrillError(
            f"{artifact_kind} JSON is invalid."
        ) from exc

    if not isinstance(
        payload,
        dict,
    ):
        raise VaultProviderLossDrillError(
            f"{artifact_kind} must contain a JSON object."
        )

    return (
        payload,
        observed_digest,
        head.version_id,
    )


def _validate_complete_marker(
    *,
    payload: dict[str, Any],
    complete_key: str,
    expected_snapshot_id: str,
    expected_source_label: str,
    expected_release_commit: str,
    expected_alembic_revision: str,
    replica_settings: StorageSettings,
    replica_domain: RecoveryDomain,
) -> dict[str, Any]:
    if (
        payload.get(
            "format_version"
        )
        != COMPLETE_FORMAT_VERSION
    ):
        raise VaultProviderLossDrillError(
            "Complete marker format version is invalid."
        )

    snapshot = _snapshot_id(
        payload.get(
            "snapshot_id"
        )
    )

    expected_snapshot = _snapshot_id(
        expected_snapshot_id
    )

    if snapshot != expected_snapshot:
        raise VaultProviderLossDrillError(
            "Complete marker snapshot binding failed."
        )

    source_label = _nonempty(
        payload.get(
            "source_label"
        ),
        label="complete source_label",
    )

    if source_label != _nonempty(
        expected_source_label,
        label="expected source_label",
    ):
        raise VaultProviderLossDrillError(
            "Complete marker source-label binding failed."
        )

    release_commit = _release_commit(
        payload.get(
            "release_commit"
        )
    )

    if release_commit != _release_commit(
        expected_release_commit
    ):
        raise VaultProviderLossDrillError(
            "Complete marker release binding failed."
        )

    alembic_revision = _nonempty(
        payload.get(
            "alembic_revision"
        ),
        label="complete alembic_revision",
    )

    if alembic_revision != _nonempty(
        expected_alembic_revision,
        label="expected alembic_revision",
    ):
        raise VaultProviderLossDrillError(
            "Complete marker Alembic binding failed."
        )

    source_identity = _sha256(
        payload.get(
            "source_identity_sha256"
        ),
        label="complete source_identity_sha256",
    )

    expected_provider = _domain_value(
        replica_domain.provider_id,
        label="configured replica provider_id",
    )
    expected_failure = _domain_value(
        replica_domain.failure_domain,
        label="configured replica failure_domain",
    )

    if (
        _domain_value(
            payload.get(
                "replica_provider_id"
            ),
            label="complete replica_provider_id",
        )
        != expected_provider
    ):
        raise VaultProviderLossDrillError(
            "Complete marker replica-provider binding failed."
        )

    if (
        _domain_value(
            payload.get(
                "replica_failure_domain"
            ),
            label="complete replica_failure_domain",
        )
        != expected_failure
    ):
        raise VaultProviderLossDrillError(
            "Complete marker replica failure-domain binding failed."
        )

    prefix = (
        replica_settings.normalized_key_prefix
    )

    canonical_complete_key = (
        _canonical_key(
            complete_key,
            label="complete key",
        )
    )

    expected_complete_key = (
        f"{prefix}/snapshots/"
        f"{snapshot}/complete.json"
    )

    if (
        canonical_complete_key
        != expected_complete_key
    ):
        raise VaultProviderLossDrillError(
            "Complete marker key is outside the bound snapshot."
        )

    manifest_key = _canonical_key(
        payload.get(
            "manifest_key"
        ),
        label="manifest_key",
    )

    expected_manifest_key = (
        f"{prefix}/snapshots/"
        f"{snapshot}/manifest.json"
    )

    if (
        manifest_key
        != expected_manifest_key
    ):
        raise VaultProviderLossDrillError(
            "Manifest key is outside the bound snapshot."
        )

    manifest_sha256 = _sha256(
        payload.get(
            "manifest_sha256"
        ),
        label="manifest_sha256",
    )

    manifest_version_id = (
        _optional_string(
            payload.get(
                "manifest_version_id"
            )
        )
    )

    document_count = _nonnegative_int(
        payload.get(
            "document_count"
        ),
        label="document_count",
    )

    replicated_count = _nonnegative_int(
        payload.get(
            "replicated_document_count"
        ),
        label="replicated_document_count",
    )

    state_only_count = _nonnegative_int(
        payload.get(
            "state_only_document_count"
        ),
        label="state_only_document_count",
    )

    replica_object_count = (
        _nonnegative_int(
            payload.get(
                "replica_object_count"
            ),
            label="replica_object_count",
        )
    )

    if (
        document_count
        != replicated_count
        + state_only_count
    ):
        raise VaultProviderLossDrillError(
            "Complete marker document counts are inconsistent."
        )

    if replicated_count == 0:
        if replica_object_count != 0:
            raise VaultProviderLossDrillError(
                "Complete marker replica object count is inconsistent."
            )
    elif not (
        1
        <= replica_object_count
        <= replicated_count
    ):
        raise VaultProviderLossDrillError(
            "Complete marker replica object count is inconsistent."
        )

    return {
        "snapshot_id": snapshot,
        "source_label": source_label,
        "release_commit": release_commit,
        "source_identity_sha256": (
            source_identity
        ),
        "alembic_revision": (
            alembic_revision
        ),
        "manifest_key": manifest_key,
        "manifest_sha256": (
            manifest_sha256
        ),
        "manifest_version_id": (
            manifest_version_id
        ),
        "document_count": (
            document_count
        ),
        "replicated_document_count": (
            replicated_count
        ),
        "state_only_document_count": (
            state_only_count
        ),
        "replica_object_count": (
            replica_object_count
        ),
    }


def _validate_document_entry(
    *,
    raw: dict[str, Any],
    primary_bucket: str,
    primary_key_prefix: str,
    replica_key_prefix: str,
) -> DrillDocument:
    organization_id = _positive_int(
        raw.get(
            "organization_id"
        ),
        label="document organization_id",
    )

    public_id = _uuid(
        raw.get(
            "public_id"
        ),
        label="document public_id",
    )

    status = _nonempty(
        raw.get(
            "status"
        ),
        label="document status",
    )

    if status not in _KNOWN_STATUSES:
        raise VaultProviderLossDrillError(
            "Manifest contains an unsupported Vault lifecycle state."
        )

    storage_backend = _nonempty(
        raw.get(
            "storage_backend"
        ),
        label="document storage_backend",
    ).lower()

    if storage_backend != "s3":
        raise VaultProviderLossDrillError(
            "Manifest source storage backend is unsupported."
        )

    storage_bucket = _nonempty(
        raw.get(
            "storage_bucket"
        ),
        label="document storage_bucket",
    )

    if storage_bucket != primary_bucket:
        raise VaultProviderLossDrillError(
            "Manifest document storage bucket does not match "
            "bound primary storage."
        )

    object_key = _canonical_key(
        raw.get(
            "object_key"
        ),
        label="document object_key",
    )

    source_prefix = (
        f"{primary_key_prefix}/tenants/"
        f"{organization_id}/objects/"
    )

    if not object_key.startswith(
        source_prefix
    ):
        raise VaultProviderLossDrillError(
            "Manifest source object violates tenant binding."
        )

    storage_version_id = (
        _optional_string(
            raw.get(
                "storage_version_id"
            )
        )
    )

    size_bytes = _positive_int(
        raw.get(
            "size_bytes"
        ),
        label="document size_bytes",
    )

    content_type = (
        _canonical_content_type(
            raw.get(
                "content_type"
            )
        )
    )

    digest = _sha256(
        raw.get(
            "sha256"
        ),
        label="document sha256",
    )

    recovery_eligible = raw.get(
        "recovery_eligible"
    )

    if not isinstance(
        recovery_eligible,
        bool,
    ):
        raise VaultProviderLossDrillError(
            "Manifest recovery_eligible must be boolean."
        )

    replica_state = _nonempty(
        raw.get(
            "replica_state"
        ),
        label="document replica_state",
    )

    replica_key = _optional_string(
        raw.get(
            "replica_key"
        )
    )

    replica_version_id = (
        _optional_string(
            raw.get(
                "replica_version_id"
            )
        )
    )

    source_exact_verified = raw.get(
        "source_exact_version_verified"
    )

    if not isinstance(
        source_exact_verified,
        bool,
    ):
        raise VaultProviderLossDrillError(
            "Manifest source_exact_version_verified must be boolean."
        )

    if status == "available":
        if not recovery_eligible:
            raise VaultProviderLossDrillError(
                "Available document is not marked recovery eligible."
            )

        if (
            replica_state
            not in _RECOVERABLE_REPLICA_STATES
        ):
            raise VaultProviderLossDrillError(
                "Available document has an invalid replica state."
            )

        if replica_key is None:
            raise VaultProviderLossDrillError(
                "Available document is missing replica key."
            )

        canonical_replica_key = (
            _canonical_key(
                replica_key,
                label="document replica_key",
            )
        )

        expected_replica_key = (
            f"{replica_key_prefix}/tenants/"
            f"{organization_id}/objects/sha256/"
            f"{digest[:2]}/{digest}"
        )

        if (
            canonical_replica_key
            != expected_replica_key
        ):
            raise VaultProviderLossDrillError(
                "Replica object violates tenant/content-address binding."
            )

        if (
            source_exact_verified
            != (
                storage_version_id
                is not None
            )
        ):
            raise VaultProviderLossDrillError(
                "Manifest source exact-version state is inconsistent."
            )

        replica_key = (
            canonical_replica_key
        )

    else:
        if recovery_eligible:
            raise VaultProviderLossDrillError(
                "Non-available document must not be recovery eligible."
            )

        if (
            replica_state
            != "state_only_not_auto_recoverable"
        ):
            raise VaultProviderLossDrillError(
                "Non-available lifecycle state is inconsistent."
            )

        if (
            replica_key is not None
            or replica_version_id
            is not None
        ):
            raise VaultProviderLossDrillError(
                "Non-available document unexpectedly references recovery bytes."
            )

        if source_exact_verified:
            raise VaultProviderLossDrillError(
                "Non-available document has invalid exact-version state."
            )

    return DrillDocument(
        organization_id=(
            organization_id
        ),
        public_id=public_id,
        status=status,
        storage_backend=(
            storage_backend
        ),
        storage_bucket=(
            storage_bucket
        ),
        object_key=object_key,
        storage_version_id=(
            storage_version_id
        ),
        size_bytes=size_bytes,
        content_type=content_type,
        sha256=digest,
        recovery_eligible=(
            recovery_eligible
        ),
        replica_state=replica_state,
        replica_key=replica_key,
        replica_version_id=(
            replica_version_id
        ),
        source_exact_version_verified=(
            source_exact_verified
        ),
    )


def _validate_manifest(
    *,
    payload: dict[str, Any],
    complete: dict[str, Any],
    replica_settings: StorageSettings,
    replica_domain: RecoveryDomain,
) -> ValidatedSnapshot:
    if (
        payload.get(
            "format_version"
        )
        != MANIFEST_FORMAT_VERSION
    ):
        raise VaultProviderLossDrillError(
            "Manifest format version is invalid."
        )

    if (
        _snapshot_id(
            payload.get(
                "snapshot_id"
            )
        )
        != complete["snapshot_id"]
    ):
        raise VaultProviderLossDrillError(
            "Manifest snapshot binding failed."
        )

    if (
        _nonempty(
            payload.get(
                "source_label"
            ),
            label="manifest source_label",
        )
        != complete["source_label"]
    ):
        raise VaultProviderLossDrillError(
            "Manifest source-label binding failed."
        )

    if (
        _release_commit(
            payload.get(
                "release_commit"
            )
        )
        != complete["release_commit"]
    ):
        raise VaultProviderLossDrillError(
            "Manifest release binding failed."
        )

    source_database = payload.get(
        "source_database"
    )

    if not isinstance(
        source_database,
        dict,
    ):
        raise VaultProviderLossDrillError(
            "Manifest source_database is invalid."
        )

    if (
        _sha256(
            source_database.get(
                "source_identity_sha256"
            ),
            label="manifest source identity",
        )
        != complete[
            "source_identity_sha256"
        ]
    ):
        raise VaultProviderLossDrillError(
            "Manifest source-identity binding failed."
        )

    if (
        _nonempty(
            source_database.get(
                "alembic_revision"
            ),
            label="manifest alembic_revision",
        )
        != complete[
            "alembic_revision"
        ]
    ):
        raise VaultProviderLossDrillError(
            "Manifest Alembic binding failed."
        )

    primary_storage = payload.get(
        "primary_storage"
    )
    replica_storage = payload.get(
        "replica_storage"
    )

    if not isinstance(
        primary_storage,
        dict,
    ) or not isinstance(
        replica_storage,
        dict,
    ):
        raise VaultProviderLossDrillError(
            "Manifest storage-domain metadata is invalid."
        )

    primary_provider = _domain_value(
        primary_storage.get(
            "provider_id"
        ),
        label="manifest primary provider_id",
    )
    primary_failure = _domain_value(
        primary_storage.get(
            "failure_domain"
        ),
        label="manifest primary failure_domain",
    )

    replica_provider = _domain_value(
        replica_storage.get(
            "provider_id"
        ),
        label="manifest replica provider_id",
    )
    replica_failure = _domain_value(
        replica_storage.get(
            "failure_domain"
        ),
        label="manifest replica failure_domain",
    )

    if (
        primary_provider
        == replica_provider
    ):
        raise VaultProviderLossDrillError(
            "Manifest does not represent an independent storage provider."
        )

    if (
        primary_failure
        == replica_failure
    ):
        raise VaultProviderLossDrillError(
            "Manifest does not represent an independent failure domain."
        )

    configured_provider = (
        _domain_value(
            replica_domain.provider_id,
            label="configured replica provider_id",
        )
    )
    configured_failure = (
        _domain_value(
            replica_domain.failure_domain,
            label="configured replica failure_domain",
        )
    )

    if (
        replica_provider
        != configured_provider
        or replica_failure
        != configured_failure
    ):
        raise VaultProviderLossDrillError(
            "Manifest replica-domain binding failed."
        )

    primary_bucket = _nonempty(
        primary_storage.get(
            "bucket_name"
        ),
        label="manifest primary bucket",
    )

    configured_bucket = _nonempty(
        replica_settings.bucket_name,
        label="configured replica bucket",
    )

    manifest_bucket = _nonempty(
        replica_storage.get(
            "bucket_name"
        ),
        label="manifest replica bucket",
    )

    if manifest_bucket != configured_bucket:
        raise VaultProviderLossDrillError(
            "Manifest replica-bucket binding failed."
        )

    replica_prefix = (
        replica_settings.normalized_key_prefix
    )

    if (
        _nonempty(
            replica_storage.get(
                "key_prefix"
            ),
            label="manifest replica key_prefix",
        )
        != replica_prefix
    ):
        raise VaultProviderLossDrillError(
            "Manifest replica-prefix binding failed."
        )

    primary_prefix = _nonempty(
        primary_storage.get(
            "key_prefix"
        ),
        label="manifest primary key_prefix",
    ).strip("/")

    document_count = _nonnegative_int(
        payload.get(
            "document_count"
        ),
        label="manifest document_count",
    )

    replicated_count = _nonnegative_int(
        payload.get(
            "replicated_document_count"
        ),
        label="manifest replicated_document_count",
    )

    state_only_count = _nonnegative_int(
        payload.get(
            "state_only_document_count"
        ),
        label="manifest state_only_document_count",
    )

    replica_object_count = (
        _nonnegative_int(
            payload.get(
                "replica_object_count"
            ),
            label="manifest replica_object_count",
        )
    )

    for key, observed in (
        (
            "document_count",
            document_count,
        ),
        (
            "replicated_document_count",
            replicated_count,
        ),
        (
            "state_only_document_count",
            state_only_count,
        ),
        (
            "replica_object_count",
            replica_object_count,
        ),
    ):
        if observed != complete[key]:
            raise VaultProviderLossDrillError(
                f"Manifest {key} does not match complete marker."
            )

    raw_documents = payload.get(
        "documents"
    )

    if not isinstance(
        raw_documents,
        list,
    ):
        raise VaultProviderLossDrillError(
            "Manifest documents collection is invalid."
        )

    if len(
        raw_documents
    ) != document_count:
        raise VaultProviderLossDrillError(
            "Manifest document count does not match collection."
        )

    documents: list[
        DrillDocument
    ] = []
    public_ids: set[str] = set()
    available_count = 0
    state_count = 0
    replica_keys: set[str] = set()

    for raw_document in raw_documents:
        if not isinstance(
            raw_document,
            dict,
        ):
            raise VaultProviderLossDrillError(
                "Manifest document entry is invalid."
            )

        document = (
            _validate_document_entry(
                raw=raw_document,
                primary_bucket=(
                    primary_bucket
                ),
                primary_key_prefix=(
                    primary_prefix
                ),
                replica_key_prefix=(
                    replica_prefix
                ),
            )
        )

        if document.public_id in public_ids:
            raise VaultProviderLossDrillError(
                "Manifest contains duplicate public document binding."
            )

        public_ids.add(
            document.public_id
        )

        if document.status == "available":
            available_count += 1

            assert (
                document.replica_key
                is not None
            )

            replica_keys.add(
                document.replica_key
            )
        else:
            state_count += 1

        documents.append(
            document
        )

    if (
        available_count
        != replicated_count
        or state_count
        != state_only_count
        or len(
            replica_keys
        )
        != replica_object_count
    ):
        raise VaultProviderLossDrillError(
            "Manifest lifecycle/replica counts are inconsistent."
        )

    return ValidatedSnapshot(
        snapshot_id=complete[
            "snapshot_id"
        ],
        source_label=complete[
            "source_label"
        ],
        release_commit=complete[
            "release_commit"
        ],
        source_identity_sha256=complete[
            "source_identity_sha256"
        ],
        alembic_revision=complete[
            "alembic_revision"
        ],
        document_count=(
            document_count
        ),
        replicated_document_count=(
            replicated_count
        ),
        state_only_document_count=(
            state_only_count
        ),
        replica_object_count=(
            replica_object_count
        ),
        manifest_key=complete[
            "manifest_key"
        ],
        manifest_sha256=complete[
            "manifest_sha256"
        ],
        documents=tuple(
            documents
        ),
    )


def _verify_replica_payload_to_file(
    *,
    storage: ObjectStorageClient,
    document: DrillDocument,
    destination: Path,
) -> int:
    if (
        not document.recovery_eligible
        or document.replica_key is None
    ):
        raise VaultProviderLossDrillError(
            "Attempted byte recovery for a non-recoverable document."
        )

    try:
        head = storage.head_object(
            key=document.replica_key,
            version_id=(
                document.replica_version_id
            ),
        )
    except ObjectStorageNotFoundError as exc:
        raise VaultProviderLossDrillError(
            "Replica recovery payload is missing."
        ) from exc
    except ObjectStorageError as exc:
        raise VaultProviderLossDrillError(
            "Replica recovery payload HEAD failed."
        ) from exc

    if (
        document.replica_version_id
        is not None
        and head.version_id
        != document.replica_version_id
    ):
        raise VaultProviderLossDrillError(
            "Replica payload version binding failed."
        )

    if head.size_bytes != document.size_bytes:
        raise VaultProviderLossDrillError(
            "Replica payload size binding failed."
        )

    if (
        head.content_type is None
        or _canonical_content_type(
            head.content_type
        )
        != document.content_type
    ):
        raise VaultProviderLossDrillError(
            "Replica payload content-type binding failed."
        )

    metadata_sha = str(
        head.metadata.get(
            "sha256",
            "",
        )
    ).strip().lower()

    if metadata_sha != document.sha256:
        raise VaultProviderLossDrillError(
            "Replica payload SHA-256 metadata binding failed."
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
        raise VaultProviderLossDrillError(
            "Replica payload tenant metadata binding failed."
        )

    destination.parent.mkdir(
        parents=True,
        exist_ok=False,
    )

    digest = hashlib.sha256()
    total = 0

    try:
        with storage.get_object_stream(
            key=document.replica_key,
            version_id=(
                document.replica_version_id
            ),
        ) as stream:
            if (
                document.replica_version_id
                is not None
                and stream.head.version_id
                != document.replica_version_id
            ):
                raise VaultProviderLossDrillError(
                    "Replica payload stream version binding failed."
                )

            if (
                stream.head.content_type
                is None
                or _canonical_content_type(
                    stream.head.content_type
                )
                != document.content_type
            ):
                raise VaultProviderLossDrillError(
                    "Replica payload stream content-type binding failed."
                )

            with destination.open(
                "xb"
            ) as output:
                while True:
                    chunk = stream.read(
                        1024 * 1024
                    )

                    if not chunk:
                        break

                    total += len(chunk)
                    digest.update(chunk)
                    output.write(chunk)

    except VaultProviderLossDrillError:
        destination.unlink(
            missing_ok=True
        )
        raise
    except ObjectStorageError as exc:
        destination.unlink(
            missing_ok=True
        )
        raise VaultProviderLossDrillError(
            "Replica payload full recovery failed."
        ) from exc
    except OSError as exc:
        destination.unlink(
            missing_ok=True
        )
        raise VaultProviderLossDrillError(
            "Isolated recovery-target write failed."
        ) from exc

    if total != document.size_bytes:
        destination.unlink(
            missing_ok=True
        )
        raise VaultProviderLossDrillError(
            "Recovered byte count does not match manifest."
        )

    if (
        digest.hexdigest()
        != document.sha256
    ):
        destination.unlink(
            missing_ok=True
        )
        raise VaultProviderLossDrillError(
            "Recovered payload full SHA-256 verification failed."
        )

    return total


def _validate_application_compatibility(
    *,
    document: DrillDocument,
    payload_path: Path,
) -> None:
    extension = _CONTENT_TYPE_EXTENSION.get(
        document.content_type
    )

    if extension is None:
        raise VaultProviderLossDrillError(
            "Recovered payload uses a content type unsupported by the Vault runtime."
        )

    try:
        payload = payload_path.read_bytes()
    except OSError as exc:
        raise VaultProviderLossDrillError(
            "Recovered payload cannot be reopened for application validation."
        ) from exc

    try:
        validation_settings = (
            StorageSettings(
                max_upload_bytes=(
                    document.size_bytes
                ),
                allowed_content_types=(
                    document.content_type,
                ),
            )
        )

        validated = validate_vault_upload(
            filename=(
                f"{document.public_id}"
                f"{extension}"
            ),
            document_type=(
                "OTHER_EVIDENCE"
            ),
            content_type=(
                document.content_type
            ),
            content=payload,
            settings=(
                validation_settings
            ),
        )
    except (
        VaultValidationError,
        ValueError,
    ) as exc:
        raise VaultProviderLossDrillError(
            "Recovered bytes are not application-compatible Vault evidence."
        ) from exc

    if (
        validated.size_bytes
        != document.size_bytes
        or validated.sha256
        != document.sha256
        or validated.content_type
        != document.content_type
    ):
        raise VaultProviderLossDrillError(
            "Application validation changed the recovered evidence identity."
        )


def _metadata_for_document(
    *,
    document: DrillDocument,
    payload_relative_path: str | None,
) -> dict[str, Any]:
    return {
        "organization_id": (
            document.organization_id
        ),
        "public_id": (
            document.public_id
        ),
        "status": (
            document.status
        ),
        "storage_backend": (
            document.storage_backend
        ),
        "storage_bucket": (
            document.storage_bucket
        ),
        "object_key": (
            document.object_key
        ),
        "storage_version_id": (
            document.storage_version_id
        ),
        "size_bytes": (
            document.size_bytes
        ),
        "content_type": (
            document.content_type
        ),
        "sha256": (
            document.sha256
        ),
        "recovery_eligible": (
            document.recovery_eligible
        ),
        "replica_state": (
            document.replica_state
        ),
        "replica_key": (
            document.replica_key
        ),
        "replica_version_id": (
            document.replica_version_id
        ),
        "payload_relative_path": (
            payload_relative_path
        ),
    }


def _write_json_file(
    path: Path,
    payload: dict[str, Any],
) -> str:
    body = _canonical_json_bytes(
        payload
    )

    path.write_bytes(
        body
    )

    return hashlib.sha256(
        body
    ).hexdigest()


def run_provider_loss_drill(
    *,
    replica_storage: ObjectStorageClient,
    replica_settings: StorageSettings,
    replica_domain: RecoveryDomain,
    complete_key: str,
    complete_version_id: str | None,
    expected_complete_sha256: str,
    expected_snapshot_id: str,
    expected_source_label: str,
    expected_release_commit: str,
    expected_alembic_revision: str,
    output_directory: Path,
    operator: str,
) -> dict[str, Any]:
    started = time.perf_counter()

    final_output = (
        output_directory.resolve()
    )

    if final_output.exists():
        raise VaultProviderLossDrillError(
            "Isolated recovery target already exists."
        )

    final_output.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    partial_output = (
        final_output.parent
        / (
            f".{final_output.name}."
            f"partial-{uuid4().hex}"
        )
    )

    complete_sha = _sha256(
        expected_complete_sha256,
        label="expected complete SHA-256",
    )

    complete_payload, observed_complete_sha, _ = (
        _verify_json_artifact(
            storage=replica_storage,
            key=complete_key,
            version_id=(
                complete_version_id
            ),
            expected_sha256=(
                complete_sha
            ),
            artifact_kind=(
                "complete marker"
            ),
        )
    )

    complete = (
        _validate_complete_marker(
            payload=complete_payload,
            complete_key=complete_key,
            expected_snapshot_id=(
                expected_snapshot_id
            ),
            expected_source_label=(
                expected_source_label
            ),
            expected_release_commit=(
                expected_release_commit
            ),
            expected_alembic_revision=(
                expected_alembic_revision
            ),
            replica_settings=(
                replica_settings
            ),
            replica_domain=(
                replica_domain
            ),
        )
    )

    manifest_payload, observed_manifest_sha, _ = (
        _verify_json_artifact(
            storage=replica_storage,
            key=complete[
                "manifest_key"
            ],
            version_id=complete[
                "manifest_version_id"
            ],
            expected_sha256=complete[
                "manifest_sha256"
            ],
            artifact_kind=(
                "recovery manifest"
            ),
        )
    )

    snapshot = _validate_manifest(
        payload=manifest_payload,
        complete=complete,
        replica_settings=(
            replica_settings
        ),
        replica_domain=(
            replica_domain
        ),
    )

    if (
        snapshot.replicated_document_count <= 0
        or snapshot.replica_object_count <= 0
    ):
        raise VaultProviderLossDrillError(
            "Provider-loss drill requires at least one "
            "recovery-eligible Vault object."
        )

    recovered_count = 0
    state_only_count = 0
    recovered_bytes = 0
    index_documents: list[
        dict[str, Any]
    ] = []

    try:
        partial_output.mkdir(
            parents=False,
            exist_ok=False,
        )

        for document in (
            snapshot.documents
        ):
            document_directory = (
                partial_output
                / "tenants"
                / str(
                    document.organization_id
                )
                / "documents"
                / document.public_id
            )

            payload_relative_path: (
                str | None
            ) = None

            if document.status == "available":
                extension = (
                    _CONTENT_TYPE_EXTENSION.get(
                        document.content_type
                    )
                )

                if extension is None:
                    raise VaultProviderLossDrillError(
                        "Available recovery evidence has an unsupported Vault content type."
                    )

                payload_path = (
                    document_directory
                    / f"payload{extension}"
                )

                recovered_bytes += (
                    _verify_replica_payload_to_file(
                        storage=(
                            replica_storage
                        ),
                        document=document,
                        destination=(
                            payload_path
                        ),
                    )
                )

                _validate_application_compatibility(
                    document=document,
                    payload_path=(
                        payload_path
                    ),
                )

                payload_relative_path = (
                    payload_path.relative_to(
                        partial_output
                    ).as_posix()
                )

                recovered_count += 1

            else:
                document_directory.mkdir(
                    parents=True,
                    exist_ok=False,
                )
                state_only_count += 1

            metadata = (
                _metadata_for_document(
                    document=document,
                    payload_relative_path=(
                        payload_relative_path
                    ),
                )
            )

            _write_json_file(
                document_directory
                / "metadata.json",
                metadata,
            )

            index_documents.append(
                metadata
            )

        if (
            recovered_count
            != snapshot.replicated_document_count
            or state_only_count
            != snapshot.state_only_document_count
        ):
            raise VaultProviderLossDrillError(
                "Recovered lifecycle counts do not match bound snapshot."
            )

        elapsed_seconds = round(
            time.perf_counter()
            - started,
            6,
        )

        index_payload = {
            "format_version": (
                INDEX_FORMAT_VERSION
            ),
            "result": "PASS",
            "provider_loss_mode": (
                "primary_unavailable"
            ),
            "primary_access_attempted": (
                False
            ),
            "replica_read_only": True,
            "production_modified": False,
            "operator": _nonempty(
                operator,
                label="operator",
            ),
            "snapshot_id": (
                snapshot.snapshot_id
            ),
            "source_label": (
                snapshot.source_label
            ),
            "release_commit": (
                snapshot.release_commit
            ),
            "source_identity_sha256": (
                snapshot.source_identity_sha256
            ),
            "alembic_revision": (
                snapshot.alembic_revision
            ),
            "complete_key": (
                _canonical_key(
                    complete_key,
                    label="complete key",
                )
            ),
            "complete_sha256": (
                observed_complete_sha
            ),
            "manifest_key": (
                snapshot.manifest_key
            ),
            "manifest_sha256": (
                observed_manifest_sha
            ),
            "document_count": (
                snapshot.document_count
            ),
            "recovered_document_count": (
                recovered_count
            ),
            "state_only_document_count": (
                state_only_count
            ),
            "replica_object_count": (
                snapshot.replica_object_count
            ),
            "recovered_bytes": (
                recovered_bytes
            ),
            "elapsed_seconds": (
                elapsed_seconds
            ),
            "documents": (
                index_documents
            ),
        }

        index_sha256 = _write_json_file(
            partial_output
            / "recovery_index.json",
            index_payload,
        )

        partial_output.replace(
            final_output
        )

    except Exception:
        shutil.rmtree(
            partial_output,
            ignore_errors=True,
        )
        raise

    return {
        "format_version": (
            DRILL_FORMAT_VERSION
        ),
        "result": "PASS",
        "verification_status": (
            "PASS"
        ),
        "provider_loss_mode": (
            "primary_unavailable"
        ),
        "primary_access_attempted": (
            False
        ),
        "replica_read_only": True,
        "production_modified": False,
        "snapshot_id": (
            snapshot.snapshot_id
        ),
        "source_label": (
            snapshot.source_label
        ),
        "release_commit": (
            snapshot.release_commit
        ),
        "source_identity_sha256": (
            snapshot.source_identity_sha256
        ),
        "alembic_revision": (
            snapshot.alembic_revision
        ),
        "complete_sha256": (
            observed_complete_sha
        ),
        "manifest_sha256": (
            observed_manifest_sha
        ),
        "document_count": (
            snapshot.document_count
        ),
        "recovered_document_count": (
            recovered_count
        ),
        "state_only_document_count": (
            state_only_count
        ),
        "recovered_bytes": (
            recovered_bytes
        ),
        "elapsed_seconds": (
            elapsed_seconds
        ),
        "recovery_index_sha256": (
            index_sha256
        ),
        "output_directory": str(
            final_output
        ),
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Recover and verify a Vault snapshot "
            "from the independent replica while "
            "the primary provider is treated as unavailable."
        )
    )

    parser.add_argument(
        "--complete-key",
        required=True,
    )
    parser.add_argument(
        "--complete-version-id",
        default=None,
    )
    parser.add_argument(
        "--complete-sha256",
        required=True,
    )
    parser.add_argument(
        "--expected-snapshot-id",
        required=True,
    )
    parser.add_argument(
        "--expected-source-label",
        required=True,
    )
    parser.add_argument(
        "--expected-release-commit",
        required=True,
    )
    parser.add_argument(
        "--expected-alembic-revision",
        required=True,
    )
    parser.add_argument(
        "--output-dir",
        required=True,
    )
    parser.add_argument(
        "--operator",
        required=True,
    )
    parser.add_argument(
        "--primary-unavailable",
        action="store_true",
    )

    return parser


def main(
    argv: list[str] | None = None,
) -> int:
    parser = (
        build_argument_parser()
    )
    args = parser.parse_args(
        argv
    )

    try:
        if not args.primary_unavailable:
            raise VaultProviderLossDrillError(
                "--primary-unavailable is required for P2.7A6B."
            )

        assert_primary_provider_not_configured()

        replica_settings = (
            _replica_settings_from_environment()
        )

        replica_domain = (
            _replica_domain_from_environment()
        )

        replica_storage = (
            Boto3S3ObjectStorage(
                replica_settings
            )
        )

        if not replica_storage.health_check():
            raise VaultProviderLossDrillError(
                "Independent Vault replica readiness failed."
            )

        result = run_provider_loss_drill(
            replica_storage=(
                replica_storage
            ),
            replica_settings=(
                replica_settings
            ),
            replica_domain=(
                replica_domain
            ),
            complete_key=(
                args.complete_key
            ),
            complete_version_id=(
                args.complete_version_id
            ),
            expected_complete_sha256=(
                args.complete_sha256
            ),
            expected_snapshot_id=(
                args.expected_snapshot_id
            ),
            expected_source_label=(
                args.expected_source_label
            ),
            expected_release_commit=(
                args.expected_release_commit
            ),
            expected_alembic_revision=(
                args.expected_alembic_revision
            ),
            output_directory=Path(
                args.output_dir
            ),
            operator=args.operator,
        )

    except (
        VaultProviderLossDrillError,
        ObjectStorageError,
    ) as exc:
        print(
            sanitize_cli_error_message(
                str(exc)
            ),
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Vault provider-loss drill operational failure.",
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
