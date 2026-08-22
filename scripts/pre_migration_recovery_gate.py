from __future__ import annotations

import argparse
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import sys
from typing import Any

from scripts.postgres_backup_publish import REMOTE_FORMAT_VERSION
from scripts.postgres_logical_backup import (
    BackupToolError,
    FORMAT_VERSION,
    collect_database_metadata,
    extract_major_version,
    require_environment_url,
    sanitize_cli_error_message,
)


GATE_FORMAT_VERSION = "p27a5.gate.v1"
DEFAULT_MAX_AGE_MINUTES = 120
MAX_CLOCK_SKEW_MINUTES = 5
_SHA256_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")
_COMMIT_PATTERN = re.compile(r"^[0-9a-fA-F]{7,64}$")


class RecoveryGateError(BackupToolError):
    """Fail-closed pre-migration recovery-gate error."""


DatabaseMetadataCollector = Callable[[str], dict[str, Any]]


def _require_mapping(
    value: object,
    *,
    label: str,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RecoveryGateError(
            f"{label} must be a JSON object."
        )
    return value


def _load_json_file(
    path: Path,
    *,
    label: str,
) -> tuple[dict[str, Any], bytes]:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise RecoveryGateError(
            f"{label} is missing or unreadable."
        ) from exc

    try:
        parsed = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RecoveryGateError(
            f"{label} is not valid JSON."
        ) from exc

    return (
        _require_mapping(
            parsed,
            label=label,
        ),
        payload,
    )


def _require_fields(
    payload: dict[str, Any],
    *,
    required: set[str],
    label: str,
) -> None:
    missing = sorted(
        required.difference(payload)
    )
    if missing:
        raise RecoveryGateError(
            f"{label} is missing required fields."
        )


def _require_nonempty_string(
    value: object,
    *,
    label: str,
) -> str:
    normalized = str(
        value if value is not None else ""
    ).strip()

    if not normalized:
        raise RecoveryGateError(
            f"{label} must not be empty."
        )

    return normalized


def _require_sha256(
    value: object,
    *,
    label: str,
) -> str:
    normalized = _require_nonempty_string(
        value,
        label=label,
    ).lower()

    if not _SHA256_PATTERN.fullmatch(
        normalized
    ):
        raise RecoveryGateError(
            f"{label} is not a canonical SHA-256."
        )

    return normalized


def _require_commit(
    value: object,
    *,
    label: str,
) -> str:
    normalized = _require_nonempty_string(
        value,
        label=label,
    ).lower()

    if not _COMMIT_PATTERN.fullmatch(
        normalized
    ):
        raise RecoveryGateError(
            f"{label} is not a valid commit identifier."
        )

    return normalized


def _require_positive_int(
    value: object,
    *,
    label: str,
) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise RecoveryGateError(
            f"{label} must be a positive integer."
        ) from exc

    if normalized <= 0:
        raise RecoveryGateError(
            f"{label} must be a positive integer."
        )

    return normalized


def _parse_utc_timestamp(
    value: object,
    *,
    label: str,
) -> datetime:
    normalized = _require_nonempty_string(
        value,
        label=label,
    )

    try:
        parsed = datetime.strptime(
            normalized,
            "%Y-%m-%dT%H:%M:%SZ",
        ).replace(
            tzinfo=UTC,
        )
    except ValueError as exc:
        raise RecoveryGateError(
            f"{label} must use canonical UTC ISO format."
        ) from exc

    return parsed


def _basename_from_remote_key(
    value: object,
    *,
    label: str,
) -> str:
    normalized = _require_nonempty_string(
        value,
        label=label,
    )

    if "\\" in normalized:
        raise RecoveryGateError(
            f"{label} is not a canonical object key."
        )

    basename = PurePosixPath(
        normalized
    ).name

    if (
        not basename
        or basename in {".", ".."}
    ):
        raise RecoveryGateError(
            f"{label} is not a canonical object key."
        )

    return basename


def _major_minor_version(
    value: object,
    *,
    label: str,
) -> tuple[int, int]:
    normalized = _require_nonempty_string(
        value,
        label=label,
    )

    match = re.search(
        r"(\d+)\.(\d+)",
        normalized,
    )
    if not match:
        raise RecoveryGateError(
            f"{label} does not expose major.minor version."
        )

    return (
        int(match.group(1)),
        int(match.group(2)),
    )


def _utc_iso(
    value: datetime,
) -> str:
    return value.astimezone(
        UTC
    ).replace(
        microsecond=0
    ).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def verify_recovery_gate(
    *,
    manifest_path: Path,
    complete_marker_path: Path,
    database_url: str,
    expected_source_release_commit: str,
    operator: str,
    target_environment: str,
    expected_source_label: str = "production",
    max_age_minutes: int = DEFAULT_MAX_AGE_MINUTES,
    now_utc: datetime | None = None,
    database_metadata_collector: DatabaseMetadataCollector = (
        collect_database_metadata
    ),
) -> dict[str, Any]:
    if (
        _require_nonempty_string(
            target_environment,
            label="target_environment",
        ).lower()
        != "production"
    ):
        raise RecoveryGateError(
            "Pre-migration recovery gate accepts only production target."
        )

    normalized_operator = _require_nonempty_string(
        operator,
        label="operator",
    )
    if len(normalized_operator) > 160:
        raise RecoveryGateError(
            "operator is too long."
        )

    normalized_source_label = (
        _require_nonempty_string(
            expected_source_label,
            label="expected_source_label",
        )
    )

    expected_commit = _require_commit(
        expected_source_release_commit,
        label="expected_source_release_commit",
    )

    normalized_max_age = _require_positive_int(
        max_age_minutes,
        label="max_age_minutes",
    )

    manifest, manifest_bytes = _load_json_file(
        manifest_path,
        label="Recovery manifest",
    )
    complete, _ = _load_json_file(
        complete_marker_path,
        label="Recovery complete marker",
    )

    _require_fields(
        manifest,
        required={
            "format_version",
            "created_at_utc",
            "source_label",
            "release_commit",
            "database_name",
            "source_identity_sha256",
            "postgres_server_version",
            "alembic_revision",
            "postgis_version",
            "dump_filename",
            "dump_sha256",
            "dump_size_bytes",
        },
        label="Recovery manifest",
    )

    _require_fields(
        complete,
        required={
            "format_version",
            "source_label",
            "release_commit",
            "backup_created_at_utc",
            "published_at_utc",
            "dump_key",
            "dump_sha256",
            "dump_size_bytes",
            "manifest_key",
            "manifest_sha256",
        },
        label="Recovery complete marker",
    )

    if manifest["format_version"] != FORMAT_VERSION:
        raise RecoveryGateError(
            "Recovery manifest format is unsupported."
        )

    if complete["format_version"] != REMOTE_FORMAT_VERSION:
        raise RecoveryGateError(
            "Recovery complete-marker format is unsupported."
        )

    manifest_sha256 = hashlib.sha256(
        manifest_bytes
    ).hexdigest()

    expected_manifest_sha256 = _require_sha256(
        complete["manifest_sha256"],
        label="complete.manifest_sha256",
    )

    if manifest_sha256 != expected_manifest_sha256:
        raise RecoveryGateError(
            "Recovery manifest SHA-256 does not match complete marker."
        )

    remote_manifest_basename = (
        _basename_from_remote_key(
            complete["manifest_key"],
            label="complete.manifest_key",
        )
    )

    if remote_manifest_basename != manifest_path.name:
        raise RecoveryGateError(
            "Recovery manifest filename does not match complete marker."
        )

    manifest_dump_filename = (
        _require_nonempty_string(
            manifest["dump_filename"],
            label="manifest.dump_filename",
        )
    )

    remote_dump_basename = _basename_from_remote_key(
        complete["dump_key"],
        label="complete.dump_key",
    )

    if remote_dump_basename != manifest_dump_filename:
        raise RecoveryGateError(
            "Recovery dump binding does not match manifest."
        )

    manifest_dump_sha256 = _require_sha256(
        manifest["dump_sha256"],
        label="manifest.dump_sha256",
    )
    complete_dump_sha256 = _require_sha256(
        complete["dump_sha256"],
        label="complete.dump_sha256",
    )

    if manifest_dump_sha256 != complete_dump_sha256:
        raise RecoveryGateError(
            "Recovery dump SHA-256 binding is inconsistent."
        )

    manifest_dump_size = _require_positive_int(
        manifest["dump_size_bytes"],
        label="manifest.dump_size_bytes",
    )
    complete_dump_size = _require_positive_int(
        complete["dump_size_bytes"],
        label="complete.dump_size_bytes",
    )

    if manifest_dump_size != complete_dump_size:
        raise RecoveryGateError(
            "Recovery dump size binding is inconsistent."
        )

    manifest_source_label = (
        _require_nonempty_string(
            manifest["source_label"],
            label="manifest.source_label",
        )
    )
    complete_source_label = (
        _require_nonempty_string(
            complete["source_label"],
            label="complete.source_label",
        )
    )

    if (
        manifest_source_label
        != normalized_source_label
        or complete_source_label
        != normalized_source_label
    ):
        raise RecoveryGateError(
            "Recovery source label does not match production."
        )

    manifest_commit = _require_commit(
        manifest["release_commit"],
        label="manifest.release_commit",
    )
    complete_commit = _require_commit(
        complete["release_commit"],
        label="complete.release_commit",
    )

    if (
        manifest_commit != expected_commit
        or complete_commit != expected_commit
    ):
        raise RecoveryGateError(
            "Recovery point does not match the currently deployed release."
        )

    manifest_created_at = _parse_utc_timestamp(
        manifest["created_at_utc"],
        label="manifest.created_at_utc",
    )
    complete_created_at = _parse_utc_timestamp(
        complete["backup_created_at_utc"],
        label="complete.backup_created_at_utc",
    )
    published_at = _parse_utc_timestamp(
        complete["published_at_utc"],
        label="complete.published_at_utc",
    )

    if manifest_created_at != complete_created_at:
        raise RecoveryGateError(
            "Recovery timestamps are inconsistent."
        )

    active_now = (
        now_utc.astimezone(UTC)
        if now_utc is not None
        else datetime.now(UTC)
    )

    if published_at < manifest_created_at:
        raise RecoveryGateError(
            "Recovery publication timestamp precedes backup creation."
        )

    max_future = active_now + timedelta(
        minutes=MAX_CLOCK_SKEW_MINUTES
    )

    if (
        manifest_created_at > max_future
        or published_at > max_future
    ):
        raise RecoveryGateError(
            "Recovery evidence has an invalid future timestamp."
        )

    age = active_now - manifest_created_at

    if age > timedelta(
        minutes=normalized_max_age
    ):
        raise RecoveryGateError(
            "Recovery point is too old for a schema-changing migration."
        )

    live_metadata = database_metadata_collector(
        database_url
    )

    manifest_database_name = (
        _require_nonempty_string(
            manifest["database_name"],
            label="manifest.database_name",
        )
    )
    live_database_name = (
        _require_nonempty_string(
            live_metadata.get("database_name"),
            label="live.database_name",
        )
    )

    if manifest_database_name != live_database_name:
        raise RecoveryGateError(
            "Recovery point targets a different database."
        )

    manifest_identity = _require_sha256(
        manifest["source_identity_sha256"],
        label="manifest.source_identity_sha256",
    )
    live_identity = _require_sha256(
        live_metadata.get("source_identity_sha256"),
        label="live.source_identity_sha256",
    )

    if manifest_identity != live_identity:
        raise RecoveryGateError(
            "Recovery point targets a different database identity."
        )

    manifest_alembic = _require_nonempty_string(
        manifest["alembic_revision"],
        label="manifest.alembic_revision",
    )
    live_alembic = _require_nonempty_string(
        live_metadata.get("alembic_revision"),
        label="live.alembic_revision",
    )

    if manifest_alembic != live_alembic:
        raise RecoveryGateError(
            "Recovery point Alembic revision does not match production."
        )

    manifest_pg_major = extract_major_version(
        _require_nonempty_string(
            manifest["postgres_server_version"],
            label="manifest.postgres_server_version",
        )
    )
    live_pg_major = extract_major_version(
        _require_nonempty_string(
            live_metadata.get(
                "postgres_server_version"
            ),
            label="live.postgres_server_version",
        )
    )

    if manifest_pg_major != live_pg_major:
        raise RecoveryGateError(
            "Recovery point PostgreSQL major version does not match production."
        )

    manifest_postgis = _major_minor_version(
        manifest["postgis_version"],
        label="manifest.postgis_version",
    )
    live_postgis = _major_minor_version(
        live_metadata.get("postgis_version"),
        label="live.postgis_version",
    )

    if manifest_postgis != live_postgis:
        raise RecoveryGateError(
            "Recovery point PostGIS major.minor does not match production."
        )

    age_seconds = max(
        0,
        int(age.total_seconds()),
    )

    return {
        "format_version": GATE_FORMAT_VERSION,
        "result": "PASS",
        "verification_status": "PASS",
        "verified_at_utc": _utc_iso(
            active_now
        ),
        "target_environment": "production",
        "operator": normalized_operator,
        "source_label": normalized_source_label,
        "source_release_commit": expected_commit,
        "backup_created_at_utc": _utc_iso(
            manifest_created_at
        ),
        "backup_age_seconds": age_seconds,
        "alembic_revision": live_alembic,
        "source_identity_sha256": live_identity,
        "manifest_sha256": manifest_sha256,
    }


def _positive_cli_int(
    raw_value: str,
) -> int:
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "must be a positive integer"
        ) from exc

    if value <= 0:
        raise argparse.ArgumentTypeError(
            "must be a positive integer"
        )

    return value


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fail-closed production recovery-point gate "
            "before schema migration."
        )
    )
    parser.add_argument(
        "--manifest",
        required=True,
    )
    parser.add_argument(
        "--complete-marker",
        required=True,
    )
    parser.add_argument(
        "--source-label",
        default="production",
    )
    parser.add_argument(
        "--max-age-minutes",
        type=_positive_cli_int,
        default=_positive_cli_int(
            os.environ.get(
                "PRE_MIGRATION_MAX_AGE_MINUTES",
                str(DEFAULT_MAX_AGE_MINUTES),
            )
        ),
    )
    return parser


def main(
    argv: list[str] | None = None,
) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    try:
        database_url = require_environment_url(
            "MIGRATION_DATABASE_URL"
        )
        source_release_commit = (
            _require_nonempty_string(
                os.environ.get(
                    "PRE_MIGRATION_SOURCE_RELEASE_COMMIT"
                ),
                label=(
                    "PRE_MIGRATION_SOURCE_RELEASE_COMMIT"
                ),
            )
        )
        operator = _require_nonempty_string(
            os.environ.get(
                "PRE_MIGRATION_OPERATOR"
            ),
            label="PRE_MIGRATION_OPERATOR",
        )
        target_environment = (
            os.environ.get(
                "PRE_MIGRATION_TARGET_ENV"
            )
            or "production"
        )

        result = verify_recovery_gate(
            manifest_path=Path(
                args.manifest
            ),
            complete_marker_path=Path(
                args.complete_marker
            ),
            database_url=database_url,
            expected_source_release_commit=(
                source_release_commit
            ),
            operator=operator,
            target_environment=target_environment,
            expected_source_label=(
                args.source_label
            ),
            max_age_minutes=(
                args.max_age_minutes
            ),
        )
    except BackupToolError as exc:
        print(
            sanitize_cli_error_message(
                str(exc)
            ),
            file=sys.stderr,
        )
        return 1
    except Exception as exc:
        print(
            sanitize_cli_error_message(
                str(exc)
            ),
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
    raise SystemExit(main())
