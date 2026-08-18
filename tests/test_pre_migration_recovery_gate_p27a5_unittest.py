from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path

import pytest

from scripts.pre_migration_recovery_gate import (
    GATE_FORMAT_VERSION,
    RecoveryGateError,
    verify_recovery_gate,
)
from scripts.postgres_backup_publish import (
    REMOTE_FORMAT_VERSION,
)
from scripts.postgres_logical_backup import (
    FORMAT_VERSION,
)


NOW = datetime(
    2026,
    8,
    18,
    1,
    30,
    0,
    tzinfo=UTC,
)
RELEASE = (
    "0c94022ad7ffdae781b85b8dac38f18e64de0e05"
)
IDENTITY = "a" * 64
ALEMBIC = "008_add_platform_control_plane_functions"


def _iso(value: datetime) -> str:
    return value.astimezone(
        UTC
    ).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def _live_metadata(
    _database_url: str,
) -> dict[str, object]:
    return {
        "database_name": "neondb",
        "postgres_server_version": "17.10",
        "alembic_revision": ALEMBIC,
        "postgis_version": "3.5.0",
        "source_identity_sha256": IDENTITY,
    }


def _write_evidence(
    tmp_path: Path,
    *,
    created_at: datetime | None = None,
    release_commit: str = RELEASE,
    identity: str = IDENTITY,
    alembic_revision: str = ALEMBIC,
    dump_sha256: str | None = None,
) -> tuple[Path, Path]:
    created = created_at or (
        NOW - timedelta(minutes=30)
    )
    dump_sha = dump_sha256 or (
        hashlib.sha256(
            b"p27a5-dump"
        ).hexdigest()
    )
    dump_filename = (
        "20260818T010000Z_production.dump"
    )
    manifest_filename = (
        "20260818T010000Z_production.manifest.json"
    )

    manifest = {
        "format_version": FORMAT_VERSION,
        "created_at_utc": _iso(created),
        "source_label": "production",
        "release_commit": release_commit,
        "database_name": "neondb",
        "source_identity_sha256": identity,
        "postgres_server_version": "17.10",
        "alembic_revision": alembic_revision,
        "postgis_version": "3.5.0",
        "dump_filename": dump_filename,
        "dump_sha256": dump_sha,
        "dump_size_bytes": 91895,
    }

    manifest_path = (
        tmp_path / manifest_filename
    )
    manifest_path.write_text(
        json.dumps(
            manifest,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    manifest_sha = hashlib.sha256(
        manifest_path.read_bytes()
    ).hexdigest()

    complete = {
        "format_version": REMOTE_FORMAT_VERSION,
        "source_label": "production",
        "release_commit": release_commit,
        "backup_created_at_utc": _iso(created),
        "published_at_utc": _iso(
            created + timedelta(minutes=1)
        ),
        "dump_key": (
            "litoral-trace/postgres/production/"
            "2026/08/18/20260818T010000Z/"
            f"{dump_filename}"
        ),
        "dump_sha256": dump_sha,
        "dump_size_bytes": 91895,
        "manifest_key": (
            "litoral-trace/postgres/production/"
            "2026/08/18/20260818T010000Z/"
            f"{manifest_filename}"
        ),
        "manifest_sha256": manifest_sha,
    }

    complete_path = (
        tmp_path / "complete.json"
    )
    complete_path.write_text(
        json.dumps(
            complete,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return manifest_path, complete_path


def _verify(
    manifest: Path,
    complete: Path,
    **kwargs,
):
    return verify_recovery_gate(
        manifest_path=manifest,
        complete_marker_path=complete,
        database_url=(
            "postgresql://ignored-for-unit-test/neondb"
        ),
        expected_source_release_commit=(
            kwargs.pop(
                "expected_source_release_commit",
                RELEASE,
            )
        ),
        operator="p27a5-unit-test",
        target_environment="production",
        max_age_minutes=kwargs.pop(
            "max_age_minutes",
            120,
        ),
        now_utc=kwargs.pop(
            "now_utc",
            NOW,
        ),
        database_metadata_collector=kwargs.pop(
            "database_metadata_collector",
            _live_metadata,
        ),
        **kwargs,
    )


def test_p27a5_valid_complete_recovery_point_passes(
    tmp_path,
):
    manifest, complete = _write_evidence(
        tmp_path
    )

    result = _verify(
        manifest,
        complete,
    )

    assert result["result"] == "PASS"
    assert (
        result["verification_status"]
        == "PASS"
    )
    assert (
        result["format_version"]
        == GATE_FORMAT_VERSION
    )
    assert (
        result["source_release_commit"]
        == RELEASE
    )
    assert (
        result["alembic_revision"]
        == ALEMBIC
    )
    assert (
        result["source_identity_sha256"]
        == IDENTITY
    )
    assert (
        result["backup_age_seconds"]
        == 30 * 60
    )


def test_p27a5_rejects_tampered_manifest(
    tmp_path,
):
    manifest, complete = _write_evidence(
        tmp_path
    )

    payload = json.loads(
        manifest.read_text(
            encoding="utf-8"
        )
    )
    payload["database_name"] = "tampered"

    manifest.write_text(
        json.dumps(
            payload,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        RecoveryGateError,
        match="SHA-256",
    ):
        _verify(
            manifest,
            complete,
        )


def test_p27a5_rejects_wrong_current_release(
    tmp_path,
):
    manifest, complete = _write_evidence(
        tmp_path
    )

    with pytest.raises(
        RecoveryGateError,
        match="currently deployed release",
    ):
        _verify(
            manifest,
            complete,
            expected_source_release_commit=(
                "1" * 40
            ),
        )


def test_p27a5_rejects_different_database_identity(
    tmp_path,
):
    manifest, complete = _write_evidence(
        tmp_path
    )

    def wrong_database(
        _url: str,
    ) -> dict[str, object]:
        metadata = _live_metadata(
            _url
        )
        metadata[
            "source_identity_sha256"
        ] = "b" * 64
        return metadata

    with pytest.raises(
        RecoveryGateError,
        match="different database identity",
    ):
        _verify(
            manifest,
            complete,
            database_metadata_collector=(
                wrong_database
            ),
        )


def test_p27a5_rejects_alembic_mismatch(
    tmp_path,
):
    manifest, complete = _write_evidence(
        tmp_path
    )

    def wrong_revision(
        _url: str,
    ) -> dict[str, object]:
        metadata = _live_metadata(
            _url
        )
        metadata[
            "alembic_revision"
        ] = "999_wrong_revision"
        return metadata

    with pytest.raises(
        RecoveryGateError,
        match="Alembic revision",
    ):
        _verify(
            manifest,
            complete,
            database_metadata_collector=(
                wrong_revision
            ),
        )


def test_p27a5_rejects_stale_recovery_point(
    tmp_path,
):
    manifest, complete = _write_evidence(
        tmp_path,
        created_at=(
            NOW - timedelta(minutes=121)
        ),
    )

    with pytest.raises(
        RecoveryGateError,
        match="too old",
    ):
        _verify(
            manifest,
            complete,
        )


def test_p27a5_rejects_future_evidence(
    tmp_path,
):
    manifest, complete = _write_evidence(
        tmp_path,
        created_at=(
            NOW + timedelta(minutes=10)
        ),
    )

    with pytest.raises(
        RecoveryGateError,
        match="future timestamp",
    ):
        _verify(
            manifest,
            complete,
        )


def test_p27a5_rejects_dump_binding_mismatch(
    tmp_path,
):
    manifest, complete = _write_evidence(
        tmp_path
    )

    marker = json.loads(
        complete.read_text(
            encoding="utf-8"
        )
    )
    marker[
        "dump_sha256"
    ] = "c" * 64

    complete.write_text(
        json.dumps(
            marker,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(
        RecoveryGateError,
        match="dump SHA-256 binding",
    ):
        _verify(
            manifest,
            complete,
        )


def test_p27a5_rejects_postgres_major_mismatch(
    tmp_path,
):
    manifest, complete = _write_evidence(
        tmp_path
    )

    def wrong_postgres(
        _url: str,
    ) -> dict[str, object]:
        metadata = _live_metadata(
            _url
        )
        metadata[
            "postgres_server_version"
        ] = "18.0"
        return metadata

    with pytest.raises(
        RecoveryGateError,
        match="PostgreSQL major version",
    ):
        _verify(
            manifest,
            complete,
            database_metadata_collector=(
                wrong_postgres
            ),
        )


def test_p27a5_runbooks_define_executable_fail_closed_gate():
    root = Path(
        __file__
    ).resolve().parents[1]

    dr = (
        root
        / "DISASTER_RECOVERY_RUNBOOK.md"
    ).read_text(
        encoding="utf-8"
    )

    deployment = (
        root
        / "DEPLOYMENT_RUNBOOK.md"
    ).read_text(
        encoding="utf-8"
    )

    for token in (
        "13. P2.7A5 executable pre-migration recovery gate",
        "PRE_MIGRATION_SOURCE_RELEASE_COMMIT",
        "120 minutes",
        "source_identity_sha256",
        "manifest_sha256",
        "before Alembic",
        "NO MIGRATION",
    ):
        assert token in dr

    for token in (
        "python -m scripts.pre_migration_recovery_gate",
        "PRE_MIGRATION_RECOVERY_MANIFEST",
        "PRE_MIGRATION_RECOVERY_COMPLETE",
        "PRE_MIGRATION_SOURCE_RELEASE_COMMIT",
        "PRE_MIGRATION_OPERATOR",
        "PRE_MIGRATION_MAX_AGE_MINUTES=120",
        "currently deployed production release",
    ):
        assert token in deployment
