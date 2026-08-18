from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace
from unittest.mock import patch

import pytest


ROOT = Path(__file__).resolve().parents[1]
BACKUP_PATH = ROOT / "scripts" / "postgres_logical_backup.py"
RESTORE_PATH = ROOT / "scripts" / "postgres_logical_restore.py"
GITIGNORE_PATH = ROOT / ".gitignore"
DR_RUNBOOK_PATH = ROOT / "DISASTER_RECOVERY_RUNBOOK.md"


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
restore = _load_module(
    "scripts.postgres_logical_restore",
    RESTORE_PATH,
)


def _base_manifest() -> dict[str, object]:
    return {
        "format_version": backup.FORMAT_VERSION,
        "created_at_utc": "2026-08-17T00:00:00Z",
        "source_label": "production",
        "release_commit": "894f5d3",
        "database_name": "appdb",
        "source_identity_sha256": "a" * 64,
        "postgres_server_version": "17.10",
        "pg_dump_version": "pg_dump (PostgreSQL) 17.3",
        "alembic_revision": "018_add_batch_evidence_links",
        "postgis_version": "3.5",
        "table_inventory": ["organizations", "users"],
        "critical_row_counts": {
            "organizations": 4,
            "users": 4,
        },
        "dump_filename": "dump.dump",
        "dump_sha256": "b" * 64,
        "dump_size_bytes": 123,
    }


def test_backup_database_url_required(monkeypatch):
    monkeypatch.delenv("BACKUP_DATABASE_URL", raising=False)

    with pytest.raises(backup.BackupToolError, match="BACKUP_DATABASE_URL is required"):
        backup.require_environment_url("BACKUP_DATABASE_URL")


def test_restore_database_url_required(monkeypatch):
    monkeypatch.delenv("RESTORE_DATABASE_URL", raising=False)

    with pytest.raises(backup.BackupToolError, match="RESTORE_DATABASE_URL is required"):
        backup.require_environment_url("RESTORE_DATABASE_URL")


def test_database_url_is_not_a_fallback(monkeypatch):
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg://runtime:secret@direct-host/appdb",
    )
    monkeypatch.delenv("BACKUP_DATABASE_URL", raising=False)

    with pytest.raises(backup.BackupToolError):
        backup.require_environment_url("BACKUP_DATABASE_URL")


def test_pooled_neon_source_url_rejected():
    with pytest.raises(backup.BackupToolError, match="direct/unpooled"):
        backup.reject_pooled_neon_endpoint("ep-cool-db-pooler.us-east-1.aws.neon.tech")


def test_pooled_neon_target_url_rejected():
    with pytest.raises(backup.BackupToolError, match="direct/unpooled"):
        backup.reject_pooled_neon_endpoint("ep-cool-db-pooler.us-east-1.aws.neon.tech")


def test_sqlalchemy_psycopg_url_normalized_safely():
    assert (
        backup.normalize_database_url(
            "postgresql+psycopg://user:secret@host:5432/appdb"
        )
        == "postgresql://user:secret@host:5432/appdb"
    )


def test_normalized_url_preserves_database_and_safe_query_parameters():
    normalized = backup.normalize_database_url(
        "postgresql+psycopg://user:secret@host:5432/appdb?sslmode=require&channel_binding=require"
    )
    parsed = backup.parse_database_url(normalized)

    assert normalized == (
        "postgresql://user:secret@host:5432/appdb"
        "?sslmode=require&channel_binding=require"
    )
    assert parsed["database"] == "appdb"
    assert parsed["query"]["sslmode"] == ["require"]
    assert parsed["query"]["channel_binding"] == ["require"]


def test_backup_metadata_connection_uses_normalized_libpq_url():
    connected_urls: list[str] = []

    class FakeCursor:
        def __init__(self):
            self._result = None

        def execute(self, query):
            if "current_database" in query:
                self._result = ("appdb",)
            elif "SHOW server_version" in query:
                self._result = ("17.10",)
            elif "FROM alembic_version" in query:
                self._result = ("018_add_batch_evidence_links",)
            elif "FROM pg_extension" in query:
                self._result = ("3.5",)
            elif "FROM pg_tables" in query:
                self._result = [("organizations",), ("users",)]
            elif 'COUNT(*) FROM "organizations"' in query:
                self._result = (4,)
            elif 'COUNT(*) FROM "users"' in query:
                self._result = (4,)
            else:
                self._result = None

        def fetchone(self):
            return self._result

        def fetchall(self):
            return self._result

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def _fake_connect(url):
        connected_urls.append(url)
        return FakeConnection()

    with patch.object(backup.psycopg, "connect", side_effect=_fake_connect):
        metadata = backup.collect_database_metadata(
            "postgresql+psycopg://user:secret@host:5432/appdb?sslmode=require"
        )

    assert connected_urls == [
        "postgresql://user:secret@host:5432/appdb?sslmode=require"
    ]
    assert metadata["database_name"] == "appdb"


def test_restore_preflight_connection_uses_normalized_libpq_url():
    captured_urls: list[str] = []

    class FakeCursor:
        def __init__(self):
            self._result = None

        def execute(self, query):
            if "current_database" in query:
                self._result = ("restoredb",)
            elif "SHOW server_version" in query:
                self._result = ("17.10",)
            elif "FROM pg_tables" in query:
                self._result = []
            else:
                self._result = None

        def fetchone(self):
            return self._result

        def fetchall(self):
            return self._result

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def _fake_connect(url):
        captured_urls.append(url)
        return FakeConnection()

    with patch.object(restore.psycopg, "connect", side_effect=_fake_connect):
        metadata = restore.query_target_identity(
            "postgresql+psycopg://user:secret@target-host:5432/restoredb?sslmode=require"
        )

    assert captured_urls == [
        "postgresql://user:secret@target-host:5432/restoredb?sslmode=require"
    ]
    assert metadata["database_name"] == "restoredb"
    assert metadata["table_inventory"] == []


def test_secret_not_in_generated_subprocess_argv():
    command = backup.build_pg_dump_command(
        binary_path="pg_dump",
        dump_path=Path("backup.dump.partial"),
    )
    rendered = " ".join(command)

    assert "PGPASSWORD" not in rendered
    assert "postgresql://" not in rendered
    assert "postgresql+psycopg://" not in rendered


def test_pg_dump_command_uses_custom_format(tmp_path):
    command = backup.build_pg_dump_command(
        binary_path="pg_dump",
        dump_path=tmp_path / "archive.dump.partial",
    )

    assert "--format=custom" in command


def test_pg_dump_command_does_not_use_clean_or_create(tmp_path):
    command = backup.build_pg_dump_command(
        binary_path="pg_dump",
        dump_path=tmp_path / "archive.dump.partial",
    )

    assert "--clean" not in command
    assert "--create" not in command


def test_pg_dump_uses_no_password(tmp_path):
    command = backup.build_pg_dump_command(
        binary_path="pg_dump",
        dump_path=tmp_path / "archive.dump.partial",
    )

    assert "--no-password" in command


def test_client_server_major_mismatch_fails_closed(monkeypatch, tmp_path):
    monkeypatch.setenv(
        "BACKUP_DATABASE_URL",
        "postgresql+psycopg://user:secret@direct-host/appdb",
    )

    with patch.object(backup, "require_binary_in_path", return_value="pg_dump"), patch.object(
        backup, "get_binary_version", return_value="pg_dump (PostgreSQL) 16.9"
    ), patch.object(
        backup,
        "collect_database_metadata",
        return_value={
            "database_name": "appdb",
            "postgres_server_version": "17.10",
            "alembic_revision": "018",
            "postgis_version": "3.5",
            "table_inventory": [],
            "critical_row_counts": {},
            "source_identity_sha256": "a" * 64,
        },
    ):
        with pytest.raises(backup.BackupToolError, match="major version must match"):
            backup.run_backup(
                output_dir=tmp_path,
                source_label="production",
                release_commit="894f5d3",
            )


def test_partial_backup_not_promoted_after_pg_dump_failure(monkeypatch, tmp_path):
    monkeypatch.setenv(
        "BACKUP_DATABASE_URL",
        "postgresql+psycopg://user:secret@direct-host/appdb",
    )

    def _failing_run(command, check, env):
        partial_path = Path(command[3])
        partial_path.write_bytes(b"partial")
        raise subprocess.CalledProcessError(1, command)

    with patch.object(backup, "require_binary_in_path", return_value="pg_dump"), patch.object(
        backup, "get_binary_version", return_value="pg_dump (PostgreSQL) 17.3"
    ), patch.object(
        backup,
        "collect_database_metadata",
        return_value={
            "database_name": "appdb",
            "postgres_server_version": "17.10",
            "alembic_revision": "018",
            "postgis_version": "3.5",
            "table_inventory": [],
            "critical_row_counts": {},
            "source_identity_sha256": "a" * 64,
        },
    ), patch.object(backup.subprocess, "run", side_effect=_failing_run):
        with pytest.raises(subprocess.CalledProcessError):
            backup.run_backup(
                output_dir=tmp_path,
                source_label="production",
                release_commit="894f5d3",
            )

    assert list(tmp_path.iterdir()) == []


def test_manifest_contains_required_safe_metadata():
    manifest = backup.create_backup_manifest(
        created_at_utc="2026-08-17T00:00:00Z",
        source_label="production",
        release_commit="894f5d3",
        metadata={
            "database_name": "appdb",
            "source_identity_sha256": "a" * 64,
            "postgres_server_version": "17.10",
            "alembic_revision": "018",
            "postgis_version": "3.5",
            "table_inventory": ["organizations"],
            "critical_row_counts": {"organizations": 4},
        },
        pg_dump_version="pg_dump (PostgreSQL) 17.3",
        dump_filename="archive.dump",
        dump_sha256="b" * 64,
        dump_size_bytes=123,
    )

    for field in (
        "format_version",
        "created_at_utc",
        "source_label",
        "release_commit",
        "database_name",
        "source_identity_sha256",
        "postgres_server_version",
        "pg_dump_version",
        "alembic_revision",
        "postgis_version",
        "table_inventory",
        "critical_row_counts",
        "dump_filename",
        "dump_sha256",
        "dump_size_bytes",
    ):
        assert field in manifest


def test_manifest_contains_no_password_or_connection_url():
    manifest = backup.create_backup_manifest(
        created_at_utc="2026-08-17T00:00:00Z",
        source_label="production",
        release_commit="894f5d3",
        metadata={
            "database_name": "appdb",
            "source_identity_sha256": "a" * 64,
            "postgres_server_version": "17.10",
            "alembic_revision": "018",
            "postgis_version": "3.5",
            "table_inventory": [],
            "critical_row_counts": {},
        },
        pg_dump_version="pg_dump (PostgreSQL) 17.3",
        dump_filename="archive.dump",
        dump_sha256="b" * 64,
        dump_size_bytes=123,
    )
    rendered = json.dumps(manifest, sort_keys=True)

    assert "password" not in rendered.lower()
    assert "postgresql+psycopg://" not in rendered
    assert "BACKUP_DATABASE_URL" not in rendered


def test_dump_sha256_is_recorded():
    manifest = backup.create_backup_manifest(
        created_at_utc="2026-08-17T00:00:00Z",
        source_label="production",
        release_commit="894f5d3",
        metadata={
            "database_name": "appdb",
            "source_identity_sha256": "a" * 64,
            "postgres_server_version": "17.10",
            "alembic_revision": "018",
            "postgis_version": "3.5",
            "table_inventory": [],
            "critical_row_counts": {},
        },
        pg_dump_version="pg_dump (PostgreSQL) 17.3",
        dump_filename="archive.dump",
        dump_sha256="b" * 64,
        dump_size_bytes=123,
    )

    assert manifest["dump_sha256"] == "b" * 64


def test_checksum_mismatch_prevents_pg_restore(tmp_path, monkeypatch):
    dump_path = tmp_path / "archive.dump"
    dump_path.write_bytes(b"real")
    manifest_path = tmp_path / "archive.manifest.json"
    manifest = _base_manifest()
    manifest["dump_sha256"] = "0" * 64
    manifest_path.write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "RESTORE_DATABASE_URL",
        "postgresql+psycopg://user:secret@direct-target/restoredb",
    )

    with pytest.raises(backup.BackupToolError, match="SHA-256"):
        restore.run_restore(
            dump_path=dump_path,
            manifest_path=manifest_path,
            confirm_isolated_restore=True,
        )


def test_explicit_isolated_restore_confirmation_is_mandatory(tmp_path, monkeypatch):
    dump_path = tmp_path / "archive.dump"
    dump_path.write_bytes(b"real")
    manifest_path = tmp_path / "archive.manifest.json"
    manifest_path.write_text(
        json.dumps(_base_manifest()),
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "RESTORE_DATABASE_URL",
        "postgresql+psycopg://user:secret@direct-target/restoredb",
    )

    with pytest.raises(backup.BackupToolError, match="confirm-isolated-restore"):
        restore.run_restore(
            dump_path=dump_path,
            manifest_path=manifest_path,
            confirm_isolated_restore=False,
        )


def test_identical_source_target_identity_fingerprint_prevents_restore(tmp_path, monkeypatch):
    dump_path = tmp_path / "archive.dump"
    dump_path.write_bytes(b"real")
    manifest_path = tmp_path / "archive.manifest.json"
    manifest = _base_manifest()
    manifest["dump_sha256"] = backup.sha256_file(dump_path)
    manifest_path.write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )
    monkeypatch.setenv(
        "RESTORE_DATABASE_URL",
        "postgresql+psycopg://user:secret@direct-target/restoredb",
    )

    with patch.object(restore, "require_binary_in_path", return_value="pg_restore"), patch.object(
        restore, "get_binary_version", return_value="pg_restore (PostgreSQL) 17.3"
    ), patch.object(
        restore,
        "query_target_identity",
        return_value={
            "database_name": "restoredb",
            "postgres_server_version": "17.10",
            "postgis_version": "3.5",
            "alembic_revision": "018",
            "table_inventory": [],
            "critical_row_counts": {},
            "target_identity_sha256": "a" * 64,
        },
    ):
        with pytest.raises(backup.BackupToolError, match="fingerprints must differ"):
            restore.run_restore(
                dump_path=dump_path,
                manifest_path=manifest_path,
                confirm_isolated_restore=True,
            )


def test_non_empty_target_prevents_restore(tmp_path, monkeypatch):
    dump_path = tmp_path / "archive.dump"
    dump_path.write_bytes(b"real")
    manifest_path = tmp_path / "archive.manifest.json"
    manifest = _base_manifest()
    manifest["dump_sha256"] = backup.sha256_file(dump_path)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setenv(
        "RESTORE_DATABASE_URL",
        "postgresql+psycopg://user:secret@direct-target/restoredb",
    )

    with patch.object(restore, "require_binary_in_path", return_value="pg_restore"), patch.object(
        restore, "get_binary_version", return_value="pg_restore (PostgreSQL) 17.3"
    ), patch.object(
        restore,
        "query_target_identity",
        return_value={
            "database_name": "restoredb",
            "postgres_server_version": "17.10",
            "postgis_version": "3.5",
            "alembic_revision": "018",
            "table_inventory": ["organizations"],
            "critical_row_counts": {},
            "target_identity_sha256": "c" * 64,
        },
    ):
        with pytest.raises(backup.BackupToolError, match="isolated empty target"):
            restore.run_restore(
                dump_path=dump_path,
                manifest_path=manifest_path,
                confirm_isolated_restore=True,
            )


def test_empty_target_without_alembic_version_passes_pre_restore_identity_preflight():
    class FakeCursor:
        def __init__(self):
            self._result = None

        def execute(self, query):
            if "current_database" in query:
                self._result = ("restoredb",)
            elif "SHOW server_version" in query:
                self._result = ("17.10",)
            elif "FROM pg_tables" in query:
                self._result = []
            else:
                raise AssertionError(
                    f"Unexpected pre-restore query: {query}"
                )

        def fetchone(self):
            return self._result

        def fetchall(self):
            return self._result

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch.object(restore.psycopg, "connect", return_value=FakeConnection()):
        metadata = restore.query_target_identity(
            "postgresql+psycopg://user:secret@target-host/restoredb"
        )

    assert metadata["database_name"] == "restoredb"
    assert metadata["postgres_server_version"] == "17.10"
    assert metadata["table_inventory"] == []


def test_empty_target_without_postgis_passes_pre_restore_preflight():
    target_metadata = {
        "database_name": "restoredb",
        "postgres_server_version": "17.10",
        "table_inventory": [],
        "target_identity_sha256": "c" * 64,
    }

    restore.ensure_empty_restore_target(
        table_inventory=target_metadata["table_inventory"]
    )

    assert target_metadata["table_inventory"] == []


def test_no_override_nonempty_bypass_exists():
    parser = restore.build_argument_parser()
    option_strings = {
        option
        for action in parser._actions
        for option in action.option_strings
    }

    for forbidden in (
        "--force",
        "--overwrite",
        "--clean",
        "--drop",
        "--allow-nonempty",
    ):
        assert forbidden not in option_strings


def test_pg_restore_uses_exit_on_error(tmp_path):
    command = restore.build_pg_restore_command(
        binary_path="pg_restore",
        database_name="p27a3_restore",
        dump_path=tmp_path / "archive.dump",
    )

    assert "--exit-on-error" in command


def test_pg_restore_command_contains_dbname_only(tmp_path):
    command = restore.build_pg_restore_command(
        binary_path="pg_restore",
        database_name="p27a3_restore",
        dump_path=tmp_path / "archive.dump",
    )

    dbname_index = command.index("--dbname")
    assert command[dbname_index + 1] == "p27a3_restore"


def test_pg_restore_command_contains_no_connection_url_or_credentials(tmp_path):
    command = restore.build_pg_restore_command(
        binary_path="pg_restore",
        database_name="p27a3_restore",
        dump_path=tmp_path / "archive.dump",
    )
    rendered = " ".join(command)

    assert "postgresql://" not in rendered
    assert "postgresql+psycopg://" not in rendered
    assert "target-host" not in rendered
    assert "user" not in rendered
    assert "secret" not in rendered


def test_pg_restore_uses_no_owner_no_privileges_and_single_transaction(tmp_path):
    command = restore.build_pg_restore_command(
        binary_path="pg_restore",
        database_name="p27a3_restore",
        dump_path=tmp_path / "archive.dump",
    )

    assert "--no-owner" in command
    assert "--no-privileges" in command
    assert "--single-transaction" in command


def test_pg_restore_libpq_environment_keeps_connection_fields():
    env = backup.build_libpq_environment(
        backup.parse_database_url(
            "postgresql://restore_user:secret@target-host:5432/p27a3_restore?sslmode=require"
        )
    )

    assert env["PGHOST"] == "target-host"
    assert env["PGPORT"] == "5432"
    assert env["PGUSER"] == "restore_user"
    assert env["PGPASSWORD"] == "secret"
    assert env["PGDATABASE"] == "p27a3_restore"
    assert env["PGSSLMODE"] == "require"


def test_alembic_is_checked_after_pg_restore(tmp_path, monkeypatch):
    dump_path = tmp_path / "archive.dump"
    dump_path.write_bytes(b"real")
    manifest_path = tmp_path / "archive.manifest.json"
    manifest = _base_manifest()
    manifest["dump_sha256"] = backup.sha256_file(dump_path)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setenv(
        "RESTORE_DATABASE_URL",
        "postgresql+psycopg://user:secret@direct-target/restoredb",
    )
    call_order: list[str] = []

    def _fake_query_target_identity(_url):
        call_order.append("preflight")
        return {
            "database_name": "restoredb",
            "postgres_server_version": "17.10",
            "table_inventory": [],
            "target_identity_sha256": "c" * 64,
        }

    def _fake_pg_restore(*args, **kwargs):
        call_order.append("pg_restore")
        return SimpleNamespace(returncode=0)

    def _fake_verify(*args, **kwargs):
        call_order.append("verify")
        return {
            "database_name": "restoredb",
            "postgres_version": "17.10",
            "postgis_version": "3.5",
            "table_inventory_match": True,
            "critical_row_counts_match": True,
            "alembic_revision": "018_add_batch_evidence_links",
        }

    with patch.object(restore, "require_binary_in_path", return_value="pg_restore"), patch.object(
        restore, "get_binary_version", return_value="pg_restore (PostgreSQL) 17.3"
    ), patch.object(
        restore, "query_target_identity", side_effect=_fake_query_target_identity
    ), patch.object(
        restore.subprocess, "run", side_effect=_fake_pg_restore
    ), patch.object(
        restore, "verify_restore_against_manifest", side_effect=_fake_verify
    ):
        restore.run_restore(
            dump_path=dump_path,
            manifest_path=manifest_path,
            confirm_isolated_restore=True,
        )

    assert call_order == ["preflight", "pg_restore", "verify"]


def test_postgis_is_checked_after_pg_restore(tmp_path, monkeypatch):
    dump_path = tmp_path / "archive.dump"
    dump_path.write_bytes(b"real")
    manifest_path = tmp_path / "archive.manifest.json"
    manifest = _base_manifest()
    manifest["dump_sha256"] = backup.sha256_file(dump_path)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setenv(
        "RESTORE_DATABASE_URL",
        "postgresql+psycopg://user:secret@direct-target/restoredb",
    )
    call_order: list[str] = []

    def _fake_query_target_identity(_url):
        call_order.append("preflight")
        return {
            "database_name": "restoredb",
            "postgres_server_version": "17.10",
            "table_inventory": [],
            "target_identity_sha256": "c" * 64,
        }

    def _fake_pg_restore(*args, **kwargs):
        call_order.append("pg_restore")
        return SimpleNamespace(returncode=0)

    def _fake_collect(_url):
        call_order.append("post_restore_metadata")
        return {
            "database_name": "restoredb",
            "postgres_server_version": "17.10",
            "alembic_revision": "018_add_batch_evidence_links",
            "postgis_version": None,
            "table_inventory": ["organizations", "users"],
            "critical_row_counts": {"organizations": 4, "users": 4},
            "source_identity_sha256": "x" * 64,
        }

    with patch.object(restore, "require_binary_in_path", return_value="pg_restore"), patch.object(
        restore, "get_binary_version", return_value="pg_restore (PostgreSQL) 17.3"
    ), patch.object(
        restore, "query_target_identity", side_effect=_fake_query_target_identity
    ), patch.object(
        restore.subprocess, "run", side_effect=_fake_pg_restore
    ), patch.object(
        restore, "collect_database_metadata", side_effect=_fake_collect
    ):
        with pytest.raises(backup.BackupToolError, match="PostGIS"):
            restore.run_restore(
                dump_path=dump_path,
                manifest_path=manifest_path,
                confirm_isolated_restore=True,
            )

    assert call_order == ["preflight", "pg_restore", "post_restore_metadata", "post_restore_metadata"]


def test_pg_restore_does_not_use_clean_or_create(tmp_path):
    command = restore.build_pg_restore_command(
        binary_path="pg_restore",
        database_name="p27a3_restore",
        dump_path=tmp_path / "archive.dump",
    )

    assert "--clean" not in command
    assert "--create" not in command


def test_pg_restore_credential_not_in_argv(tmp_path):
    command = restore.build_pg_restore_command(
        binary_path="pg_restore",
        database_name="p27a3_restore",
        dump_path=tmp_path / "archive.dump",
    )
    rendered = " ".join(command)

    assert "secret" not in rendered
    assert "postgresql+psycopg://" not in rendered


def test_run_restore_passes_only_database_name_in_pg_restore_argv(tmp_path, monkeypatch):
    dump_path = tmp_path / "archive.dump"
    dump_path.write_bytes(b"real")
    manifest_path = tmp_path / "archive.manifest.json"
    manifest = _base_manifest()
    manifest["dump_sha256"] = backup.sha256_file(dump_path)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setenv(
        "RESTORE_DATABASE_URL",
        "postgresql+psycopg://restore_user:secret@target-host:5432/p27a3_restore?sslmode=require",
    )
    captured_command: list[str] = []
    captured_env: dict[str, str] = {}

    def _fake_pg_restore(command, check, env):
        captured_command.extend(command)
        captured_env.update(env)
        return SimpleNamespace(returncode=0)

    with patch.object(restore, "require_binary_in_path", return_value="pg_restore"), patch.object(
        restore, "get_binary_version", return_value="pg_restore (PostgreSQL) 17.3"
    ), patch.object(
        restore,
        "query_target_identity",
        return_value={
            "database_name": "p27a3_restore",
            "postgres_server_version": "17.10",
            "table_inventory": [],
            "target_identity_sha256": "c" * 64,
        },
    ), patch.object(
        restore.subprocess, "run", side_effect=_fake_pg_restore
    ), patch.object(
        restore,
        "verify_restore_against_manifest",
        return_value={
            "database_name": "p27a3_restore",
            "postgres_version": "17.10",
            "postgis_version": "3.5",
            "table_inventory_match": True,
            "critical_row_counts_match": True,
            "alembic_revision": "018_add_batch_evidence_links",
        },
    ):
        restore.run_restore(
            dump_path=dump_path,
            manifest_path=manifest_path,
            confirm_isolated_restore=True,
        )

    dbname_index = captured_command.index("--dbname")
    assert captured_command[dbname_index + 1] == "p27a3_restore"
    rendered = " ".join(captured_command)
    assert "postgresql://" not in rendered
    assert "postgresql+psycopg://" not in rendered
    assert "target-host" not in rendered
    assert "restore_user" not in rendered
    assert "secret" not in rendered
    assert captured_env["PGHOST"] == "target-host"
    assert captured_env["PGPORT"] == "5432"
    assert captured_env["PGUSER"] == "restore_user"
    assert captured_env["PGPASSWORD"] == "secret"
    assert captured_env["PGDATABASE"] == "p27a3_restore"


def test_pg_restore_failure_does_not_run_post_restore_verification(tmp_path, monkeypatch):
    dump_path = tmp_path / "archive.dump"
    dump_path.write_bytes(b"real")
    manifest_path = tmp_path / "archive.manifest.json"
    manifest = _base_manifest()
    manifest["dump_sha256"] = backup.sha256_file(dump_path)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setenv(
        "RESTORE_DATABASE_URL",
        "postgresql+psycopg://restore_user:secret@target-host:5432/p27a3_restore?sslmode=require",
    )
    pg_restore_command: list[str] = []

    def _failing_pg_restore(command, check, env):
        pg_restore_command.extend(command)
        raise subprocess.CalledProcessError(1, command)

    with patch.object(restore, "require_binary_in_path", return_value="pg_restore"), patch.object(
        restore, "get_binary_version", return_value="pg_restore (PostgreSQL) 17.3"
    ), patch.object(
        restore,
        "query_target_identity",
        return_value={
            "database_name": "p27a3_restore",
            "postgres_server_version": "17.10",
            "table_inventory": [],
            "target_identity_sha256": "c" * 64,
        },
    ), patch.object(
        restore.subprocess, "run", side_effect=_failing_pg_restore
    ), patch.object(
        restore, "verify_restore_against_manifest"
    ) as verify_restore_mock:
        with pytest.raises(subprocess.CalledProcessError):
            restore.run_restore(
                dump_path=dump_path,
                manifest_path=manifest_path,
                confirm_isolated_restore=True,
            )

    verify_restore_mock.assert_not_called()
    assert "--single-transaction" in pg_restore_command


def test_backup_cli_sanitizes_exception_with_secret_url(capsys):
    with patch.object(
        backup,
        "run_backup",
        side_effect=RuntimeError(
            "boom postgresql://user:SUPERSECRET@example/db"
        ),
    ):
        exit_code = backup.main(
            ["--source-label", "production", "--release-commit", "894f5d3"]
        )

    captured = capsys.readouterr()
    assert exit_code != 0
    assert "SUPERSECRET" not in captured.err
    assert "postgresql://" not in captured.err


def test_restore_cli_sanitizes_exception_with_secret_url(tmp_path, monkeypatch, capsys):
    dump_path = tmp_path / "archive.dump"
    manifest_path = tmp_path / "archive.manifest.json"
    dump_path.write_bytes(b"real")
    manifest_path.write_text(json.dumps(_base_manifest()), encoding="utf-8")
    monkeypatch.setenv(
        "RESTORE_DATABASE_URL",
        "postgresql+psycopg://user:secret@target-host/restoredb",
    )

    with patch.object(
        restore,
        "run_restore",
        side_effect=RuntimeError(
            "boom postgresql://user:SUPERSECRET@example/db"
        ),
    ):
        exit_code = restore.main(
            [
                "--dump-file",
                str(dump_path),
                "--manifest",
                str(manifest_path),
                "--confirm-isolated-restore",
            ]
        )

    captured = capsys.readouterr()
    assert exit_code != 0
    assert "SUPERSECRET" not in captured.err
    assert "postgresql://" not in captured.err


def test_post_restore_alembic_mismatch_fails_verification():
    with patch.object(
        restore,
        "collect_database_metadata",
        return_value={
            "database_name": "restoredb",
            "postgres_server_version": "17.10",
            "alembic_revision": "wrong",
            "postgis_version": "3.5",
            "table_inventory": ["organizations", "users"],
            "critical_row_counts": {"organizations": 4, "users": 4},
            "source_identity_sha256": "x" * 64,
        },
    ):
        with pytest.raises(backup.BackupToolError, match="Alembic"):
            restore.verify_restore_against_manifest(
                manifest=_base_manifest(),
                restore_database_url="postgresql+psycopg://user:secret@host/db",
            )


def test_missing_postgis_after_restore_produces_fail_verification():
    with patch.object(
        restore,
        "collect_database_metadata",
        return_value={
            "database_name": "restoredb",
            "postgres_server_version": "17.10",
            "alembic_revision": "018_add_batch_evidence_links",
            "postgis_version": None,
            "table_inventory": ["organizations", "users"],
            "critical_row_counts": {"organizations": 4, "users": 4},
            "source_identity_sha256": "x" * 64,
        },
    ):
        with pytest.raises(backup.BackupToolError, match="PostGIS"):
            restore.verify_restore_against_manifest(
                manifest=_base_manifest(),
                restore_database_url="postgresql+psycopg://user:secret@host/db",
            )


def test_table_inventory_mismatch_fails_verification():
    with patch.object(
        restore,
        "collect_database_metadata",
        return_value={
            "database_name": "restoredb",
            "postgres_server_version": "17.10",
            "alembic_revision": "018_add_batch_evidence_links",
            "postgis_version": "3.5",
            "table_inventory": ["organizations"],
            "critical_row_counts": {"organizations": 4, "users": 4},
            "source_identity_sha256": "x" * 64,
        },
    ):
        with pytest.raises(backup.BackupToolError, match="table inventory"):
            restore.verify_restore_against_manifest(
                manifest=_base_manifest(),
                restore_database_url="postgresql+psycopg://user:secret@host/db",
            )


def test_critical_row_count_mismatch_fails_verification():
    with patch.object(
        restore,
        "collect_database_metadata",
        return_value={
            "database_name": "restoredb",
            "postgres_server_version": "17.10",
            "alembic_revision": "018_add_batch_evidence_links",
            "postgis_version": "3.5",
            "table_inventory": ["organizations", "users"],
            "critical_row_counts": {"organizations": 999, "users": 4},
            "source_identity_sha256": "x" * 64,
        },
    ):
        with pytest.raises(backup.BackupToolError, match="critical row counts"):
            restore.verify_restore_against_manifest(
                manifest=_base_manifest(),
                restore_database_url="postgresql+psycopg://user:secret@host/db",
            )


def test_successful_verification_produces_pass(tmp_path):
    report_path = tmp_path / "restore-report.json"
    report = restore.create_restore_report(
        started_at_utc="2026-08-17T00:00:00Z",
        completed_at_utc="2026-08-17T00:10:00Z",
        elapsed_seconds=600,
        dump_filename="archive.dump",
        dump_sha256="b" * 64,
        source_label="production",
        release_commit="894f5d3",
        source_alembic_revision="018_add_batch_evidence_links",
        verification={
            "database_name": "restoredb",
            "postgres_version": "17.10",
            "postgis_version": "3.5",
            "table_inventory_match": True,
            "critical_row_counts_match": True,
        },
        result="PASS",
    )
    backup.atomic_write_json(report_path, report)
    saved = json.loads(
        report_path.read_text(encoding="utf-8")
    )

    assert saved["result"] == "PASS"


def test_gitignore_ignores_backups_postgres():
    gitignore = GITIGNORE_PATH.read_text(encoding="utf-8")

    assert "/backups/postgres/" in gitignore


def test_dr_runbook_preserves_historical_prepass_and_records_p27a3_closure():
    runbook = DR_RUNBOOK_PATH.read_text(encoding="utf-8")

    for token in (
        "tooling implemented/tested locally",
        "REAL pg_dump / pg_restore DRILL STILL REQUIRED",
        "P2.7A3 status:",
        "CLOSED on 2026-08-17 after scheduled off-platform backup acceptance.",
        "GitHub Actions workflow run 32086976028: PASS",
        "direct/unpooled connections",
        "client/server major versions must match",
        "Backup artifacts must never be committed",
        "SHA-256 verified before restore",
        "This closes P2.7A3 overall.",
    ):
        assert token in runbook

    assert "P2.7A3 is NOT CLOSED" not in runbook
