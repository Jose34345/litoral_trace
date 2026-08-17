from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
from typing import Any

import psycopg

from scripts.postgres_logical_backup import (
    BackupToolError,
    FORMAT_VERSION,
    atomic_write_json,
    build_identity_fingerprint,
    build_libpq_environment,
    collect_database_metadata,
    extract_major_version,
    get_binary_version,
    normalize_database_url,
    parse_database_url,
    reject_pooled_neon_endpoint,
    require_binary_in_path,
    require_environment_url,
    sanitize_cli_error_message,
    sha256_file,
    utc_timestamp_iso,
)


REPORT_FORMAT_VERSION = "p27a3.restore.v1"


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(
        path.read_text(encoding="utf-8")
    )
    required_fields = {
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
    }
    missing = required_fields.difference(
        manifest
    )
    if missing:
        raise BackupToolError(
            f"Manifest is missing required fields: {sorted(missing)!r}."
        )
    if manifest["format_version"] != FORMAT_VERSION:
        raise BackupToolError(
            "Unsupported manifest format version."
        )
    return manifest


def verify_dump_checksum(
    *,
    dump_path: Path,
    expected_sha256: str,
) -> str:
    actual_sha256 = sha256_file(dump_path)
    if actual_sha256 != expected_sha256:
        raise BackupToolError(
            "Dump SHA-256 does not match the manifest."
        )
    return actual_sha256


def query_target_identity(
    restore_database_url: str,
) -> dict[str, Any]:
    normalized_url = normalize_database_url(
        restore_database_url
    )
    parsed_url = parse_database_url(
        normalized_url
    )
    reject_pooled_neon_endpoint(
        parsed_url["hostname"]
    )
    with psycopg.connect(normalized_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            database_name = str(cursor.fetchone()[0])

            cursor.execute("SHOW server_version")
            postgres_server_version = str(
                cursor.fetchone()[0]
            )

            cursor.execute(
                """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
                ORDER BY tablename
                """
            )
            raw_inventory = [
                str(row[0]) for row in cursor.fetchall()
            ]

    table_inventory = [
        table_name
        for table_name in raw_inventory
        if table_name
        not in {"alembic_version", "spatial_ref_sys"}
    ]
    metadata = {
        "database_name": database_name,
        "postgres_server_version": postgres_server_version,
        "table_inventory": table_inventory,
    }
    metadata["target_identity_sha256"] = (
        build_identity_fingerprint(
            hostname=parsed_url["hostname"],
            port=int(parsed_url["port"]),
            database_name=database_name,
        )
    )
    return metadata


def ensure_empty_restore_target(
    *,
    table_inventory: list[str],
) -> None:
    if table_inventory:
        raise BackupToolError(
            "P2.7A3 restore requires an isolated empty target."
        )


def build_pg_restore_command(
    *,
    binary_path: str,
    database_name: str,
    dump_path: Path,
) -> list[str]:
    return [
        binary_path,
        "--exit-on-error",
        "--no-password",
        "--no-owner",
        "--no-privileges",
        "--single-transaction",
        "--dbname",
        database_name,
        str(dump_path),
    ]


def verify_restore_against_manifest(
    *,
    manifest: dict[str, Any],
    restore_database_url: str,
) -> dict[str, Any]:
    metadata = collect_database_metadata(
        restore_database_url
    )

    table_inventory_match = list(
        metadata["table_inventory"]
    ) == list(manifest["table_inventory"])
    critical_row_counts_match = dict(
        metadata["critical_row_counts"]
    ) == dict(manifest["critical_row_counts"])

    if (
        metadata["alembic_revision"]
        != manifest["alembic_revision"]
    ):
        raise BackupToolError(
            "Post-restore Alembic revision does not match the manifest."
        )
    if not table_inventory_match:
        raise BackupToolError(
            "Post-restore table inventory does not match the manifest."
        )
    if not critical_row_counts_match:
        raise BackupToolError(
            "Post-restore critical row counts do not match the manifest."
        )
    if metadata["postgis_version"] is None:
        raise BackupToolError(
            "PostGIS is required for restore verification."
        )

    return {
        "database_name": metadata["database_name"],
        "postgres_version": metadata[
            "postgres_server_version"
        ],
        "postgis_version": metadata[
            "postgis_version"
        ],
        "table_inventory_match": table_inventory_match,
        "critical_row_counts_match": critical_row_counts_match,
        "alembic_revision": metadata[
            "alembic_revision"
        ],
    }


def create_restore_report(
    *,
    started_at_utc: str,
    completed_at_utc: str,
    elapsed_seconds: float,
    dump_filename: str,
    dump_sha256: str,
    source_label: str,
    release_commit: str,
    source_alembic_revision: str,
    verification: dict[str, Any],
    result: str,
) -> dict[str, Any]:
    return {
        "format_version": REPORT_FORMAT_VERSION,
        "started_at_utc": started_at_utc,
        "completed_at_utc": completed_at_utc,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "dump_filename": dump_filename,
        "dump_sha256": dump_sha256,
        "source_label": source_label,
        "release_commit": release_commit,
        "source_alembic_revision": source_alembic_revision,
        "target_database_name": verification[
            "database_name"
        ],
        "postgres_version": verification[
            "postgres_version"
        ],
        "postgis_version": verification[
            "postgis_version"
        ],
        "table_inventory_match": verification[
            "table_inventory_match"
        ],
        "critical_row_counts_match": verification[
            "critical_row_counts_match"
        ],
        "result": result,
    }


def run_restore(
    *,
    dump_path: Path,
    manifest_path: Path,
    confirm_isolated_restore: bool,
) -> Path:
    if not confirm_isolated_restore:
        raise BackupToolError(
            "--confirm-isolated-restore is required."
        )

    restore_database_url = require_environment_url(
        "RESTORE_DATABASE_URL"
    )
    parsed_target = parse_database_url(
        restore_database_url
    )
    reject_pooled_neon_endpoint(
        parsed_target["hostname"]
    )

    manifest = load_manifest(manifest_path)
    if not dump_path.exists():
        raise BackupToolError(
            "Dump file does not exist."
        )

    dump_sha256 = verify_dump_checksum(
        dump_path=dump_path,
        expected_sha256=str(
            manifest["dump_sha256"]
        ),
    )

    pg_restore_path = require_binary_in_path(
        "pg_restore"
    )
    pg_restore_version = get_binary_version(
        pg_restore_path
    )
    target_metadata = query_target_identity(
        restore_database_url
    )

    if extract_major_version(
        pg_restore_version
    ) != extract_major_version(
        target_metadata[
            "postgres_server_version"
        ]
    ):
        raise BackupToolError(
            "pg_restore major version must match the target server major version."
        )

    if (
        target_metadata["target_identity_sha256"]
        == manifest["source_identity_sha256"]
    ):
        raise BackupToolError(
            "Source and target identity fingerprints must differ."
        )

    ensure_empty_restore_target(
        table_inventory=list(
            target_metadata["table_inventory"]
        )
    )

    started_at_utc = utc_timestamp_iso()
    started = time.monotonic()
    subprocess.run(
        build_pg_restore_command(
            binary_path=pg_restore_path,
            database_name=str(
                target_metadata["database_name"]
            ),
            dump_path=dump_path,
        ),
        check=True,
        env=build_libpq_environment(
            parsed_target
        ),
    )
    completed_at_utc = utc_timestamp_iso()
    elapsed_seconds = time.monotonic() - started

    try:
        verification = verify_restore_against_manifest(
            manifest=manifest,
            restore_database_url=restore_database_url,
        )
        result = "PASS"
    except BackupToolError:
        failure_metadata = collect_database_metadata(
            restore_database_url
        )
        verification = {
            "database_name": failure_metadata[
                "database_name"
            ],
            "postgres_version": failure_metadata[
                "postgres_server_version"
            ],
            "postgis_version": failure_metadata[
                "postgis_version"
            ],
            "table_inventory_match": list(
                failure_metadata["table_inventory"]
            )
            == list(manifest["table_inventory"]),
            "critical_row_counts_match": dict(
                failure_metadata["critical_row_counts"]
            )
            == dict(manifest["critical_row_counts"]),
        }
        result = "FAIL"
        report_path = manifest_path.with_suffix(
            ".restore-report.json"
        )
        atomic_write_json(
            report_path,
            create_restore_report(
                started_at_utc=started_at_utc,
                completed_at_utc=completed_at_utc,
                elapsed_seconds=elapsed_seconds,
                dump_filename=dump_path.name,
                dump_sha256=dump_sha256,
                source_label=str(
                    manifest["source_label"]
                ),
                release_commit=str(
                    manifest["release_commit"]
                ),
                source_alembic_revision=str(
                    manifest["alembic_revision"]
                ),
                verification=verification,
                result=result,
            ),
        )
        raise

    report = create_restore_report(
        started_at_utc=started_at_utc,
        completed_at_utc=completed_at_utc,
        elapsed_seconds=elapsed_seconds,
        dump_filename=dump_path.name,
        dump_sha256=dump_sha256,
        source_label=str(manifest["source_label"]),
        release_commit=str(
            manifest["release_commit"]
        ),
        source_alembic_revision=str(
            manifest["alembic_revision"]
        ),
        verification=verification,
        result=result,
    )
    report_path = manifest_path.with_suffix(
        ".restore-report.json"
    )
    atomic_write_json(
        report_path,
        report,
    )
    return report_path


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Restore a PostgreSQL logical backup into an isolated empty target."
    )
    parser.add_argument(
        "--dump-file",
        required=True,
    )
    parser.add_argument(
        "--manifest",
        required=True,
    )
    parser.add_argument(
        "--confirm-isolated-restore",
        action="store_true",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    try:
        report_path = run_restore(
            dump_path=Path(args.dump_file),
            manifest_path=Path(args.manifest),
            confirm_isolated_restore=bool(
                args.confirm_isolated_restore
            ),
        )
    except BackupToolError as exc:
        print(
            sanitize_cli_error_message(str(exc)),
            file=sys.stderr,
        )
        return 1
    except subprocess.CalledProcessError as exc:
        print(
            f"pg_restore failed with exit code {exc.returncode}.",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:
        print(
            sanitize_cli_error_message(str(exc)),
            file=sys.stderr,
        )
        return 1

    print(
        json.dumps(
            {
                "restore_report_filename": report_path.name,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
