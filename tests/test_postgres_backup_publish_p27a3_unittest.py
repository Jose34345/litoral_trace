from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

from botocore.exceptions import ClientError
import pytest


ROOT = Path(__file__).resolve().parents[1]
BACKUP_PATH = ROOT / "scripts" / "postgres_logical_backup.py"
PUBLISH_PATH = ROOT / "scripts" / "postgres_backup_publish.py"


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(
        name,
        path,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    import sys

    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


backup = _load_module(
    "scripts.postgres_logical_backup",
    BACKUP_PATH,
)
publish = _load_module(
    "scripts.postgres_backup_publish",
    PUBLISH_PATH,
)


class FakeS3Client:
    def __init__(
        self,
        *,
        preexisting: dict[str, dict[str, Any]] | None = None,
        fail_put_keys: set[str] | None = None,
        mutate_after_put: dict[str, dict[str, Any]] | None = None,
    ):
        self.objects = dict(preexisting or {})
        self.fail_put_keys = set(fail_put_keys or set())
        self.mutate_after_put = dict(
            mutate_after_put or {}
        )
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def head_object(self, **kwargs):
        self.calls.append(("head_object", kwargs))
        key = kwargs["Key"]
        if key not in self.objects:
            raise ClientError(
                {
                    "Error": {
                        "Code": "404",
                        "Message": "Not Found",
                    }
                },
                "HeadObject",
            )
        obj = self.objects[key]
        return {
            "ContentLength": obj["ContentLength"],
            "Metadata": dict(obj["Metadata"]),
        }

    def put_object(self, **kwargs):
        self.calls.append(("put_object", kwargs))
        key = kwargs["Key"]
        if key in self.fail_put_keys:
            raise RuntimeError(
                f"upload failed for {key}"
            )
        self.objects[key] = {
            "Body": kwargs["Body"],
            "ContentLength": len(kwargs["Body"]),
            "Metadata": dict(kwargs["Metadata"]),
            "ServerSideEncryption": kwargs[
                "ServerSideEncryption"
            ],
            "ContentType": kwargs["ContentType"],
            "SSEKMSKeyId": kwargs.get("SSEKMSKeyId"),
            "ACL": kwargs.get("ACL"),
        }
        if key in self.mutate_after_put:
            self.objects[key].update(
                self.mutate_after_put[key]
            )
        return {"ETag": '"ok"'}

    def delete_object(self, **kwargs):
        self.calls.append(("delete_object", kwargs))
        raise AssertionError(
            "DeleteObject must never be called."
        )


def _base_metadata() -> dict[str, Any]:
    return {
        "database_name": "neondb",
        "source_identity_sha256": "a" * 64,
        "postgres_server_version": "17.10",
        "alembic_revision": "008_add_platform_control_plane_functions",
        "postgis_version": "3.5.0",
        "table_inventory": [
            "api_keys",
            "audit_logs",
            "licenses",
            "lotes",
            "organizations",
            "satellite_ndvi_observations",
            "user_sessions",
            "users",
        ],
        "critical_row_counts": {
            "organizations": 4,
            "users": 4,
            "lotes": 1,
            "audit_logs": 6,
        },
    }


def _make_artifacts(
    tmp_path: Path,
    *,
    source_label: str = "production",
    created_at_utc: str = "2026-08-17T18:16:32Z",
    dump_name: str = "20260817T181632Z_production.dump",
    release_commit: str = "894f5d3",
) -> tuple[Path, Path, dict[str, Any]]:
    dump_path = tmp_path / dump_name
    dump_path.write_bytes(b"backup-bytes")
    manifest = backup.create_backup_manifest(
        created_at_utc=created_at_utc,
        source_label=source_label,
        release_commit=release_commit,
        metadata=_base_metadata(),
        pg_dump_version="pg_dump (PostgreSQL) 17.11",
        dump_filename=dump_path.name,
        dump_sha256=backup.sha256_file(dump_path),
        dump_size_bytes=dump_path.stat().st_size,
    )
    manifest_path = tmp_path / (
        "20260817T181632Z_production.manifest.json"
    )
    manifest_path.write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )
    return dump_path, manifest_path, manifest


def _set_publish_env(monkeypatch):
    monkeypatch.setenv(
        "BACKUP_S3_BUCKET", "dr-bucket"
    )
    monkeypatch.setenv(
        "BACKUP_S3_PREFIX",
        "litoral-trace/postgres",
    )
    monkeypatch.delenv(
        "BACKUP_S3_REGION", raising=False
    )
    monkeypatch.delenv(
        "BACKUP_S3_ENDPOINT_URL", raising=False
    )
    monkeypatch.delenv(
        "BACKUP_S3_SSE", raising=False
    )
    monkeypatch.delenv(
        "BACKUP_S3_KMS_KEY_ID", raising=False
    )


def _expected_keys() -> dict[str, str]:
    base = (
        "litoral-trace/postgres/production/"
        "2026/08/17/20260817T181632Z"
    )
    return {
        "dump_key": (
            f"{base}/20260817T181632Z_production.dump"
        ),
        "manifest_key": (
            f"{base}/20260817T181632Z_production.manifest.json"
        ),
        "complete_key": f"{base}/complete.json",
    }


def test_valid_manifest_and_dump_can_be_published(
    tmp_path, monkeypatch
):
    dump_path, manifest_path, manifest = _make_artifacts(
        tmp_path
    )
    _set_publish_env(monkeypatch)
    s3 = FakeS3Client()

    with patch.object(
        publish, "build_s3_client", return_value=s3
    ):
        result = publish.run_publish(
            dump_path=dump_path,
            manifest_path=manifest_path,
        )

    assert result["result"] == "PASS"
    assert result["format_version"] == (
        publish.REMOTE_FORMAT_VERSION
    )
    assert result["dump_sha256"] == manifest[
        "dump_sha256"
    ]
    assert result["complete_key"] == _expected_keys()[
        "complete_key"
    ]


def test_wrong_dump_sha_fails_before_s3_write(
    tmp_path, monkeypatch
):
    dump_path, manifest_path, manifest = _make_artifacts(
        tmp_path
    )
    manifest["dump_sha256"] = "0" * 64
    manifest_path.write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    _set_publish_env(monkeypatch)
    s3 = FakeS3Client()

    with patch.object(
        publish, "build_s3_client", return_value=s3
    ):
        with pytest.raises(
            backup.BackupToolError, match="SHA-256"
        ):
            publish.run_publish(
                dump_path=dump_path,
                manifest_path=manifest_path,
            )

    assert not [
        call
        for call in s3.calls
        if call[0] == "put_object"
    ]


def test_wrong_dump_size_and_filename_mismatch_fail_before_s3_write(
    tmp_path, monkeypatch
):
    dump_path, manifest_path, manifest = _make_artifacts(
        tmp_path
    )
    manifest["dump_size_bytes"] = 999
    manifest["dump_filename"] = "other.dump"
    manifest_path.write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    _set_publish_env(monkeypatch)
    s3 = FakeS3Client()

    with patch.object(
        publish, "build_s3_client", return_value=s3
    ):
        with pytest.raises(
            backup.BackupToolError, match="dump_filename"
        ):
            publish.run_publish(
                dump_path=dump_path,
                manifest_path=manifest_path,
            )
    assert not [
        call
        for call in s3.calls
        if call[0] == "put_object"
    ]


def test_path_traversal_is_rejected(
    tmp_path, monkeypatch
):
    _set_publish_env(monkeypatch)

    with pytest.raises(
        backup.BackupToolError, match="Path traversal"
    ):
        publish.ensure_safe_basename("../evil.dump")


def test_bucket_and_prefix_are_required(
    monkeypatch,
):
    monkeypatch.delenv(
        "BACKUP_S3_BUCKET", raising=False
    )
    monkeypatch.delenv(
        "BACKUP_S3_PREFIX", raising=False
    )
    with pytest.raises(
        backup.BackupToolError, match="BACKUP_S3_BUCKET"
    ):
        publish.build_publish_config()

    monkeypatch.setenv(
        "BACKUP_S3_BUCKET", "dr-bucket"
    )
    with pytest.raises(
        backup.BackupToolError, match="BACKUP_S3_PREFIX"
    ):
        publish.build_publish_config()


def test_dump_uploaded_before_manifest_and_complete_is_last(
    tmp_path, monkeypatch
):
    dump_path, manifest_path, _manifest = _make_artifacts(
        tmp_path
    )
    _set_publish_env(monkeypatch)
    s3 = FakeS3Client()

    with patch.object(
        publish, "build_s3_client", return_value=s3
    ):
        publish.run_publish(
            dump_path=dump_path,
            manifest_path=manifest_path,
        )

    put_keys = [
        kwargs["Key"]
        for name, kwargs in s3.calls
        if name == "put_object"
    ]
    keys = _expected_keys()
    assert put_keys == [
        keys["dump_key"],
        keys["manifest_key"],
        keys["complete_key"],
    ]


def test_failed_dump_or_manifest_upload_writes_no_complete_marker(
    tmp_path, monkeypatch
):
    dump_path, manifest_path, _manifest = _make_artifacts(
        tmp_path
    )
    _set_publish_env(monkeypatch)
    keys = _expected_keys()

    for failed_key in (
        keys["dump_key"],
        keys["manifest_key"],
    ):
        s3 = FakeS3Client(fail_put_keys={failed_key})
        with patch.object(
            publish,
            "build_s3_client",
            return_value=s3,
        ):
            with pytest.raises(RuntimeError):
                publish.run_publish(
                    dump_path=dump_path,
                    manifest_path=manifest_path,
                )
        assert keys["complete_key"] not in s3.objects


def test_remote_length_or_sha_mismatch_fails_closed(
    tmp_path, monkeypatch
):
    dump_path, manifest_path, manifest = _make_artifacts(
        tmp_path
    )
    _set_publish_env(monkeypatch)
    keys = _expected_keys()

    bad_length = FakeS3Client(
        mutate_after_put={
            keys["dump_key"]: {
                "ContentLength": 1,
            }
        }
    )
    with patch.object(
        publish, "build_s3_client", return_value=bad_length
    ):
        with pytest.raises(
            backup.BackupToolError, match="length"
        ):
            publish.run_publish(
                dump_path=dump_path,
                manifest_path=manifest_path,
            )

    bad_sha = FakeS3Client(
        preexisting={
            keys["dump_key"]: {
                "ContentLength": int(
                    manifest["dump_size_bytes"]
                ),
                "Metadata": {
                    "sha256": "0" * 64
                },
            }
        }
    )
    with patch.object(
        publish, "build_s3_client", return_value=bad_sha
    ):
        with pytest.raises(
            backup.BackupToolError, match="SHA-256"
        ):
            publish.run_publish(
                dump_path=dump_path,
                manifest_path=manifest_path,
            )


def test_existing_identical_remote_object_is_idempotent_and_conflicting_fails(
    tmp_path, monkeypatch
):
    dump_path, manifest_path, manifest = _make_artifacts(
        tmp_path
    )
    _set_publish_env(monkeypatch)
    keys = _expected_keys()
    manifest_bytes = manifest_path.read_bytes()
    manifest_sha = publish.compute_sha256_bytes(
        manifest_bytes
    )
    complete_body = publish.build_complete_marker(
        manifest=manifest,
        object_keys=keys,
        manifest_sha256=manifest_sha,
    )
    complete_sha = publish.compute_sha256_bytes(
        complete_body
    )

    identical = FakeS3Client(
        preexisting={
            keys["dump_key"]: {
                "ContentLength": dump_path.stat().st_size,
                "Metadata": {
                    "sha256": manifest["dump_sha256"]
                },
            },
            keys["manifest_key"]: {
                "ContentLength": len(manifest_bytes),
                "Metadata": {
                    "sha256": manifest_sha
                },
            },
            keys["complete_key"]: {
                "ContentLength": len(complete_body),
                "Metadata": {
                    "sha256": complete_sha
                },
            },
        }
    )
    with patch.object(
        publish, "build_s3_client", return_value=identical
    ):
        result = publish.run_publish(
            dump_path=dump_path,
            manifest_path=manifest_path,
        )
    assert result["result"] == "PASS"
    assert not [
        call
        for call in identical.calls
        if call[0] == "put_object"
    ]

    conflicting = FakeS3Client(
        preexisting={
            keys["dump_key"]: {
                "ContentLength": dump_path.stat().st_size,
                "Metadata": {"sha256": "9" * 64},
            }
        }
    )
    with patch.object(
        publish, "build_s3_client", return_value=conflicting
    ):
        with pytest.raises(
            backup.BackupToolError, match="SHA-256"
        ):
            publish.run_publish(
                dump_path=dump_path,
                manifest_path=manifest_path,
            )


def test_delete_object_is_never_called_and_no_public_acl_is_used(
    tmp_path, monkeypatch
):
    dump_path, manifest_path, _manifest = _make_artifacts(
        tmp_path
    )
    _set_publish_env(monkeypatch)
    s3 = FakeS3Client()

    with patch.object(
        publish, "build_s3_client", return_value=s3
    ):
        publish.run_publish(
            dump_path=dump_path,
            manifest_path=manifest_path,
        )

    assert not [
        call
        for call in s3.calls
        if call[0] == "delete_object"
    ]
    for name, kwargs in s3.calls:
        if name != "put_object":
            continue
        assert "ACL" not in kwargs


def test_aes256_is_default_and_kms_requires_key_id(
    tmp_path, monkeypatch
):
    dump_path, manifest_path, _manifest = _make_artifacts(
        tmp_path
    )
    _set_publish_env(monkeypatch)
    s3 = FakeS3Client()
    with patch.object(
        publish, "build_s3_client", return_value=s3
    ):
        publish.run_publish(
            dump_path=dump_path,
            manifest_path=manifest_path,
        )

    put_calls = [
        kwargs
        for name, kwargs in s3.calls
        if name == "put_object"
    ]
    assert put_calls
    assert all(
        call["ServerSideEncryption"] == "AES256"
        for call in put_calls
    )

    monkeypatch.setenv(
        "BACKUP_S3_SSE", "aws:kms"
    )
    monkeypatch.delenv(
        "BACKUP_S3_KMS_KEY_ID", raising=False
    )
    with pytest.raises(
        backup.BackupToolError, match="KMS_KEY_ID"
    ):
        publish.build_publish_config()

    monkeypatch.setenv(
        "BACKUP_S3_KMS_KEY_ID", "kms-key-123"
    )
    config = publish.build_publish_config()
    assert config["sse"] == "aws:kms"
    assert config["kms_key_id"] == "kms-key-123"


def test_complete_marker_and_keys_contain_no_secret_material(
    tmp_path, monkeypatch
):
    dump_path, manifest_path, manifest = _make_artifacts(
        tmp_path
    )
    _set_publish_env(monkeypatch)
    keys = publish.build_object_keys(
        prefix="litoral-trace/postgres",
        source_label=manifest["source_label"],
        created_at_utc=manifest["created_at_utc"],
        dump_filename=dump_path.name,
        manifest_filename=manifest_path.name,
    )
    manifest_sha = publish.compute_sha256_bytes(
        manifest_path.read_bytes()
    )
    complete = publish.build_complete_marker(
        manifest=manifest,
        object_keys=keys,
        manifest_sha256=manifest_sha,
    ).decode("utf-8")
    all_keys = " ".join(keys.values())

    for forbidden in (
        "postgresql://",
        "postgresql+psycopg://",
        "target-host",
        "secret",
        "password",
        "endpoint",
    ):
        assert forbidden not in complete.lower()
        assert forbidden not in all_keys.lower()


def test_cli_does_not_accept_credentials_in_argv():
    parser = publish.build_argument_parser()
    option_strings = {
        option
        for action in parser._actions
        for option in action.option_strings
    }

    for forbidden in (
        "--access-key",
        "--secret-key",
        "--session-token",
        "--password",
    ):
        assert forbidden not in option_strings


def test_cli_sanitizes_secret_like_exception_and_exits_nonzero(
    tmp_path, monkeypatch, capsys
):
    dump_path, manifest_path, _manifest = _make_artifacts(
        tmp_path
    )
    _set_publish_env(monkeypatch)

    with patch.object(
        publish,
        "run_publish",
        side_effect=RuntimeError(
            "boom https://secret-endpoint.example "
            "postgresql://user:SUPERSECRET@example/db "
            "aws_secret_access_key=TOPSECRET"
        ),
    ):
        exit_code = publish.main(
            [
                "--dump-file",
                str(dump_path),
                "--manifest",
                str(manifest_path),
            ]
        )

    captured = capsys.readouterr()
    assert exit_code != 0
    assert "SUPERSECRET" not in captured.err
    assert "TOPSECRET" not in captured.err
    assert "postgresql://" not in captured.err
    assert "https://secret-endpoint.example" not in captured.err
