from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import psycopg


FORMAT_VERSION = "p27a3.backup.v1"
DEFAULT_OUTPUT_DIR = Path("backups/postgres")
CRITICAL_TABLES = (
    "organizations",
    "users",
    "lotes",
    "audit_logs",
    "vault_documents",
    "batch_imports",
)
INVENTORY_EXCLUDED_TABLES = frozenset(
    {
        "alembic_version",
        "spatial_ref_sys",
    }
)


class BackupToolError(RuntimeError):
    """Fail-closed logical backup error."""


def normalize_database_url(raw_url: str) -> str:
    normalized = str(raw_url or "").strip()
    if normalized.startswith("postgresql+psycopg://"):
        return normalized.replace(
            "postgresql+psycopg://",
            "postgresql://",
            1,
        )
    if normalized.startswith("postgresql://"):
        return normalized
    if normalized.startswith("postgres://"):
        return normalized.replace(
            "postgres://",
            "postgresql://",
            1,
        )
    return normalized


def require_environment_url(variable_name: str) -> str:
    value = str(os.environ.get(variable_name) or "").strip()
    if not value:
        raise BackupToolError(
            f"{variable_name} is required."
        )
    return normalize_database_url(value)


def parse_database_url(database_url: str) -> dict[str, Any]:
    normalized_url = normalize_database_url(database_url)
    parsed = urlsplit(normalized_url)

    if parsed.scheme != "postgresql":
        raise BackupToolError(
            "Only PostgreSQL libpq-compatible URLs are supported."
        )

    hostname = (parsed.hostname or "").strip()
    database_name = parsed.path.lstrip("/").strip()

    if not hostname or not database_name:
        raise BackupToolError(
            "Database URL must include host and database name."
        )

    query = parse_qs(parsed.query, keep_blank_values=True)

    return {
        "scheme": parsed.scheme,
        "hostname": hostname,
        "port": parsed.port or 5432,
        "username": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "database": database_name,
        "query": query,
    }


def reject_pooled_neon_endpoint(hostname: str) -> None:
    if "-pooler" in str(hostname or "").strip().lower():
        raise BackupToolError(
            "P2.7A3 requires direct/unpooled PostgreSQL connections."
        )


def build_libpq_environment(parsed_url: dict[str, Any]) -> dict[str, str]:
    environment = os.environ.copy()
    environment["PGHOST"] = str(parsed_url["hostname"])
    environment["PGPORT"] = str(parsed_url["port"])
    environment["PGUSER"] = str(parsed_url["username"])
    environment["PGPASSWORD"] = str(parsed_url["password"])
    environment["PGDATABASE"] = str(parsed_url["database"])

    query = parsed_url["query"]
    if "sslmode" in query and query["sslmode"]:
        environment["PGSSLMODE"] = query["sslmode"][-1]
    if "channel_binding" in query and query["channel_binding"]:
        environment["PGCHANNELBINDING"] = query["channel_binding"][-1]

    return environment


def require_binary_in_path(binary_name: str) -> str:
    resolved = shutil.which(binary_name)
    if not resolved:
        raise BackupToolError(
            f"{binary_name} is required in PATH."
        )
    return resolved


def extract_major_version(version_text: str) -> int:
    digits: list[str] = []
    saw_digit = False
    for char in str(version_text):
        if char.isdigit():
            digits.append(char)
            saw_digit = True
            continue
        if saw_digit:
            break
    if not digits:
        raise BackupToolError(
            f"Unable to determine major version from {version_text!r}."
        )
    return int("".join(digits))


def get_binary_version(binary_path: str) -> str:
    result = subprocess.run(
        [binary_path, "--version"],
        check=True,
        capture_output=True,
        text=True,
    )
    return str(result.stdout or result.stderr).strip()


def build_identity_fingerprint(
    *,
    hostname: str,
    port: int,
    database_name: str,
) -> str:
    payload = (
        f"{str(hostname).strip().lower()}:{int(port)}:"
        f"{str(database_name).strip().lower()}"
    )
    return hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        "w",
        encoding="utf-8",
        delete=False,
        dir=path.parent,
        suffix=".partial",
    ) as handle:
        json.dump(
            payload,
            handle,
            indent=2,
            sort_keys=True,
        )
        handle.write("\n")
        temp_path = Path(handle.name)
    temp_path.replace(path)


def utc_timestamp_for_filename() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )


def utc_timestamp_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(
        microsecond=0
    ).isoformat().replace("+00:00", "Z")


def sanitize_cli_error_message(message: str) -> str:
    sanitized = re.sub(
        r"postgres(?:ql(?:\+psycopg)?)?://\S+",
        "[REDACTED_DATABASE_URL]",
        str(message or ""),
        flags=re.IGNORECASE,
    )
    sanitized = re.sub(
        r"password\s*=\s*[^,\s]+",
        "password=[REDACTED]",
        sanitized,
        flags=re.IGNORECASE,
    )
    return sanitized.strip() or "Operational failure."


def collect_database_metadata(database_url: str) -> dict[str, Any]:
    normalized_url = normalize_database_url(database_url)
    parsed_url = parse_database_url(normalized_url)
    reject_pooled_neon_endpoint(parsed_url["hostname"])

    with psycopg.connect(normalized_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            database_name = str(cursor.fetchone()[0])

            cursor.execute("SHOW server_version")
            postgres_version = str(cursor.fetchone()[0])

            cursor.execute(
                "SELECT version_num FROM alembic_version"
            )
            alembic_revision = str(cursor.fetchone()[0])

            cursor.execute(
                """
                SELECT extversion
                FROM pg_extension
                WHERE extname = 'postgis'
                """
            )
            postgis_row = cursor.fetchone()
            postgis_version = (
                str(postgis_row[0])
                if postgis_row is not None
                else None
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
                if table_name not in INVENTORY_EXCLUDED_TABLES
            ]

            critical_row_counts: dict[str, int] = {}
            for table_name in CRITICAL_TABLES:
                if table_name not in raw_inventory:
                    continue
                cursor.execute(
                    f'SELECT COUNT(*) FROM "{table_name}"'
                )
                critical_row_counts[table_name] = int(
                    cursor.fetchone()[0]
                )

    return {
        "database_name": database_name,
        "postgres_server_version": postgres_version,
        "alembic_revision": alembic_revision,
        "postgis_version": postgis_version,
        "table_inventory": table_inventory,
        "critical_row_counts": critical_row_counts,
        "source_identity_sha256": build_identity_fingerprint(
            hostname=parsed_url["hostname"],
            port=int(parsed_url["port"]),
            database_name=database_name,
        ),
    }


def build_pg_dump_command(
    *,
    binary_path: str,
    dump_path: Path,
) -> list[str]:
    return [
        binary_path,
        "--format=custom",
        "--file",
        str(dump_path),
        "--no-password",
    ]


def create_backup_manifest(
    *,
    created_at_utc: str,
    source_label: str,
    release_commit: str,
    metadata: dict[str, Any],
    pg_dump_version: str,
    dump_filename: str,
    dump_sha256: str,
    dump_size_bytes: int,
) -> dict[str, Any]:
    return {
        "format_version": FORMAT_VERSION,
        "created_at_utc": created_at_utc,
        "source_label": source_label,
        "release_commit": release_commit,
        "database_name": metadata["database_name"],
        "source_identity_sha256": metadata[
            "source_identity_sha256"
        ],
        "postgres_server_version": metadata[
            "postgres_server_version"
        ],
        "pg_dump_version": pg_dump_version,
        "alembic_revision": metadata[
            "alembic_revision"
        ],
        "postgis_version": metadata["postgis_version"],
        "table_inventory": list(metadata["table_inventory"]),
        "critical_row_counts": dict(
            metadata["critical_row_counts"]
        ),
        "dump_filename": dump_filename,
        "dump_sha256": dump_sha256,
        "dump_size_bytes": int(dump_size_bytes),
    }


def run_backup(
    *,
    output_dir: Path,
    source_label: str,
    release_commit: str,
) -> tuple[Path, Path]:
    backup_database_url = require_environment_url(
        "BACKUP_DATABASE_URL"
    )
    parsed_url = parse_database_url(
        backup_database_url
    )
    reject_pooled_neon_endpoint(
        parsed_url["hostname"]
    )

    pg_dump_path = require_binary_in_path(
        "pg_dump"
    )
    pg_dump_version = get_binary_version(
        pg_dump_path
    )
    metadata = collect_database_metadata(
        backup_database_url
    )

    if extract_major_version(
        pg_dump_version
    ) != extract_major_version(
        metadata["postgres_server_version"]
    ):
        raise BackupToolError(
            "pg_dump major version must match the source server major version."
        )

    created_at_utc = utc_timestamp_iso()
    timestamp = utc_timestamp_for_filename()
    safe_source_label = (
        "".join(
            char
            for char in source_label
            if char.isalnum() or char in {"-", "_"}
        ).strip("_-")
        or "source"
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    final_dump_path = output_dir / (
        f"{timestamp}_{safe_source_label}.dump"
    )
    partial_dump_path = final_dump_path.with_suffix(
        ".dump.partial"
    )
    manifest_path = output_dir / (
        f"{timestamp}_{safe_source_label}.manifest.json"
    )

    command = build_pg_dump_command(
        binary_path=pg_dump_path,
        dump_path=partial_dump_path,
    )

    try:
        subprocess.run(
            command,
            check=True,
            env=build_libpq_environment(parsed_url),
        )
    except Exception:
        if partial_dump_path.exists():
            partial_dump_path.unlink()
        raise

    partial_dump_path.replace(final_dump_path)
    dump_sha256 = sha256_file(final_dump_path)
    dump_size_bytes = final_dump_path.stat().st_size

    manifest = create_backup_manifest(
        created_at_utc=created_at_utc,
        source_label=source_label,
        release_commit=release_commit,
        metadata=metadata,
        pg_dump_version=pg_dump_version,
        dump_filename=final_dump_path.name,
        dump_sha256=dump_sha256,
        dump_size_bytes=dump_size_bytes,
    )
    atomic_write_json(
        manifest_path,
        manifest,
    )
    return final_dump_path, manifest_path


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create an independent PostgreSQL logical backup artifact."
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
    )
    parser.add_argument(
        "--source-label",
        required=True,
    )
    parser.add_argument(
        "--release-commit",
        required=True,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    try:
        dump_path, manifest_path = run_backup(
            output_dir=Path(args.output_dir),
            source_label=str(args.source_label),
            release_commit=str(args.release_commit),
        )
    except BackupToolError as exc:
        print(
            sanitize_cli_error_message(str(exc)),
            file=sys.stderr,
        )
        return 1
    except subprocess.CalledProcessError as exc:
        print(
            f"pg_dump failed with exit code {exc.returncode}.",
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
                "dump_filename": dump_path.name,
                "manifest_filename": manifest_path.name,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
