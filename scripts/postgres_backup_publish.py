from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any

import boto3
from botocore.exceptions import ClientError

from scripts.postgres_logical_backup import (
    BackupToolError,
    FORMAT_VERSION,
    sanitize_cli_error_message,
    sha256_file,
    utc_timestamp_iso,
)


REMOTE_FORMAT_VERSION = "p27a3.remote.v1"
SAFE_BASENAME_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]*$"
)
SAFE_SOURCE_LABEL_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_-]*$"
)


def require_env(name: str) -> str:
    value = str(os.environ.get(name) or "").strip()
    if not value:
        raise BackupToolError(
            f"{name} is required."
        )
    return value


def build_publish_config() -> dict[str, str]:
    bucket = require_env("BACKUP_S3_BUCKET")
    prefix = require_env("BACKUP_S3_PREFIX").strip("/")
    if not prefix:
        raise BackupToolError(
            "BACKUP_S3_PREFIX is required."
        )

    sse = str(
        os.environ.get("BACKUP_S3_SSE") or "AES256"
    ).strip() or "AES256"
    if sse not in {"AES256", "aws:kms"}:
        raise BackupToolError(
            "BACKUP_S3_SSE must be AES256 or aws:kms."
        )

    kms_key_id = str(
        os.environ.get("BACKUP_S3_KMS_KEY_ID") or ""
    ).strip()
    if sse == "aws:kms" and not kms_key_id:
        raise BackupToolError(
            "BACKUP_S3_KMS_KEY_ID is required when BACKUP_S3_SSE=aws:kms."
        )

    return {
        "bucket": bucket,
        "prefix": prefix,
        "region": str(
            os.environ.get("BACKUP_S3_REGION") or ""
        ).strip(),
        "endpoint_url": str(
            os.environ.get("BACKUP_S3_ENDPOINT_URL") or ""
        ).strip(),
        "sse": sse,
        "kms_key_id": kms_key_id,
    }


def ensure_safe_basename(name: str) -> str:
    candidate = str(name or "").strip()
    if not candidate:
        raise BackupToolError(
            "Filename must not be empty."
        )
    if candidate != Path(candidate).name:
        raise BackupToolError(
            "Path traversal is not allowed."
        )
    if "/" in candidate or "\\" in candidate:
        raise BackupToolError(
            "Path traversal is not allowed."
        )
    if not SAFE_BASENAME_PATTERN.fullmatch(candidate):
        raise BackupToolError(
            "Filename contains unsafe characters."
        )
    return candidate


def ensure_safe_source_label(source_label: str) -> str:
    candidate = str(source_label or "").strip()
    if not SAFE_SOURCE_LABEL_PATTERN.fullmatch(
        candidate
    ):
        raise BackupToolError(
            "source_label contains unsafe characters."
        )
    return candidate


def load_manifest(
    manifest_path: Path,
) -> dict[str, Any]:
    try:
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )
    except json.JSONDecodeError as exc:
        raise BackupToolError(
            "Manifest JSON is invalid."
        ) from exc

    required_fields = {
        "format_version",
        "created_at_utc",
        "source_label",
        "release_commit",
        "dump_filename",
        "dump_sha256",
        "dump_size_bytes",
    }
    missing = required_fields.difference(manifest)
    if missing:
        raise BackupToolError(
            f"Manifest is missing required fields: {sorted(missing)!r}."
        )
    if manifest["format_version"] != FORMAT_VERSION:
        raise BackupToolError(
            "Unsupported manifest format version."
        )
    return manifest


def parse_backup_timestamp(
    created_at_utc: str,
) -> tuple[str, str, str, str]:
    match = re.fullmatch(
        r"(\d{4})-(\d{2})-(\d{2})T"
        r"(\d{2}):(\d{2}):(\d{2})Z",
        str(created_at_utc or "").strip(),
    )
    if not match:
        raise BackupToolError(
            "Manifest created_at_utc must be canonical UTC ISO format."
        )
    year, month, day, hour, minute, second = (
        match.groups()
    )
    timestamp = (
        f"{year}{month}{day}T"
        f"{hour}{minute}{second}Z"
    )
    return year, month, day, timestamp


def build_object_keys(
    *,
    prefix: str,
    source_label: str,
    created_at_utc: str,
    dump_filename: str,
    manifest_filename: str,
) -> dict[str, str]:
    safe_source_label = ensure_safe_source_label(
        source_label
    )
    safe_dump_filename = ensure_safe_basename(
        dump_filename
    )
    safe_manifest_filename = ensure_safe_basename(
        manifest_filename
    )
    year, month, day, timestamp = parse_backup_timestamp(
        created_at_utc
    )
    base_key = (
        f"{prefix}/{safe_source_label}/"
        f"{year}/{month}/{day}/{timestamp}"
    )
    return {
        "dump_key": (
            f"{base_key}/{safe_dump_filename}"
        ),
        "manifest_key": (
            f"{base_key}/{safe_manifest_filename}"
        ),
        "complete_key": (
            f"{base_key}/complete.json"
        ),
    }


def compute_sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def build_complete_marker(
    *,
    manifest: dict[str, Any],
    object_keys: dict[str, str],
    manifest_sha256: str,
) -> bytes:
    payload = {
        "format_version": REMOTE_FORMAT_VERSION,
        "source_label": manifest["source_label"],
        "release_commit": manifest["release_commit"],
        "backup_created_at_utc": manifest[
            "created_at_utc"
        ],
        "published_at_utc": utc_timestamp_iso(),
        "dump_key": object_keys["dump_key"],
        "dump_sha256": manifest["dump_sha256"],
        "dump_size_bytes": int(
            manifest["dump_size_bytes"]
        ),
        "manifest_key": object_keys["manifest_key"],
        "manifest_sha256": manifest_sha256,
    }
    return (
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def build_s3_client(config: dict[str, str]) -> Any:
    client_kwargs: dict[str, Any] = {}
    if config["region"]:
        client_kwargs["region_name"] = config[
            "region"
        ]
    if config["endpoint_url"]:
        client_kwargs["endpoint_url"] = config[
            "endpoint_url"
        ]
    return boto3.client("s3", **client_kwargs)


def build_put_object_kwargs(
    *,
    bucket: str,
    key: str,
    body: bytes,
    sha256_hex: str,
    sse: str,
    kms_key_id: str,
    content_type: str,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "Bucket": bucket,
        "Key": key,
        "Body": body,
        "ContentType": content_type,
        "Metadata": {
            "sha256": sha256_hex,
        },
        "ServerSideEncryption": sse,
    }
    if sse == "aws:kms":
        kwargs["SSEKMSKeyId"] = kms_key_id
    return kwargs


def is_not_found_error(exc: ClientError) -> bool:
    code = str(
        exc.response.get("Error", {}).get("Code", "")
    )
    return code in {"404", "NoSuchKey", "NotFound"}


def head_remote_object(
    *,
    s3_client: Any,
    bucket: str,
    key: str,
) -> dict[str, Any] | None:
    try:
        return s3_client.head_object(
            Bucket=bucket,
            Key=key,
        )
    except ClientError as exc:
        if is_not_found_error(exc):
            return None
        raise


def ensure_remote_object_matches(
    *,
    head_response: dict[str, Any],
    expected_size: int,
    expected_sha256: str,
) -> None:
    actual_size = int(
        head_response.get("ContentLength", -1)
    )
    actual_sha256 = str(
        head_response.get("Metadata", {}).get(
            "sha256", ""
        )
    )
    if actual_size != int(expected_size):
        raise BackupToolError(
            "Remote object length does not match expected content."
        )
    if actual_sha256 != expected_sha256:
        raise BackupToolError(
            "Remote object SHA-256 metadata does not match expected content."
        )


def publish_object_if_needed(
    *,
    s3_client: Any,
    bucket: str,
    key: str,
    body: bytes,
    sha256_hex: str,
    sse: str,
    kms_key_id: str,
    content_type: str,
) -> None:
    existing = head_remote_object(
        s3_client=s3_client,
        bucket=bucket,
        key=key,
    )
    if existing is not None:
        ensure_remote_object_matches(
            head_response=existing,
            expected_size=len(body),
            expected_sha256=sha256_hex,
        )
        return

    s3_client.put_object(
        **build_put_object_kwargs(
            bucket=bucket,
            key=key,
            body=body,
            sha256_hex=sha256_hex,
            sse=sse,
            kms_key_id=kms_key_id,
            content_type=content_type,
        )
    )


def verify_remote_object(
    *,
    s3_client: Any,
    bucket: str,
    key: str,
    expected_size: int,
    expected_sha256: str,
) -> None:
    head_response = head_remote_object(
        s3_client=s3_client,
        bucket=bucket,
        key=key,
    )
    if head_response is None:
        raise BackupToolError(
            "Remote object is not reachable after publication."
        )
    ensure_remote_object_matches(
        head_response=head_response,
        expected_size=expected_size,
        expected_sha256=expected_sha256,
    )


def validate_local_artifacts(
    *,
    dump_path: Path,
    manifest_path: Path,
) -> tuple[dict[str, Any], bytes, str]:
    if not dump_path.exists():
        raise BackupToolError(
            "Dump file does not exist."
        )
    if not manifest_path.exists():
        raise BackupToolError(
            "Manifest file does not exist."
        )

    ensure_safe_basename(dump_path.name)
    ensure_safe_basename(manifest_path.name)

    manifest = load_manifest(manifest_path)
    ensure_safe_source_label(
        str(manifest["source_label"])
    )

    if str(manifest["dump_filename"]) != dump_path.name:
        raise BackupToolError(
            "Manifest dump_filename does not match supplied dump file."
        )

    dump_size = int(dump_path.stat().st_size)
    if dump_size != int(manifest["dump_size_bytes"]):
        raise BackupToolError(
            "Dump size does not match the manifest."
        )

    dump_sha256 = sha256_file(dump_path)
    if dump_sha256 != str(manifest["dump_sha256"]):
        raise BackupToolError(
            "Dump SHA-256 does not match the manifest."
        )

    manifest_bytes = manifest_path.read_bytes()
    manifest_sha256 = compute_sha256_bytes(
        manifest_bytes
    )
    return manifest, manifest_bytes, manifest_sha256


def run_publish(
    *,
    dump_path: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    config = build_publish_config()
    manifest, manifest_bytes, manifest_sha256 = (
        validate_local_artifacts(
            dump_path=dump_path,
            manifest_path=manifest_path,
        )
    )
    dump_bytes = dump_path.read_bytes()
    object_keys = build_object_keys(
        prefix=config["prefix"],
        source_label=str(manifest["source_label"]),
        created_at_utc=str(manifest["created_at_utc"]),
        dump_filename=dump_path.name,
        manifest_filename=manifest_path.name,
    )

    s3_client = build_s3_client(config)

    publish_object_if_needed(
        s3_client=s3_client,
        bucket=config["bucket"],
        key=object_keys["dump_key"],
        body=dump_bytes,
        sha256_hex=str(manifest["dump_sha256"]),
        sse=config["sse"],
        kms_key_id=config["kms_key_id"],
        content_type="application/octet-stream",
    )
    verify_remote_object(
        s3_client=s3_client,
        bucket=config["bucket"],
        key=object_keys["dump_key"],
        expected_size=len(dump_bytes),
        expected_sha256=str(manifest["dump_sha256"]),
    )

    publish_object_if_needed(
        s3_client=s3_client,
        bucket=config["bucket"],
        key=object_keys["manifest_key"],
        body=manifest_bytes,
        sha256_hex=manifest_sha256,
        sse=config["sse"],
        kms_key_id=config["kms_key_id"],
        content_type="application/json",
    )
    verify_remote_object(
        s3_client=s3_client,
        bucket=config["bucket"],
        key=object_keys["manifest_key"],
        expected_size=len(manifest_bytes),
        expected_sha256=manifest_sha256,
    )

    complete_body = build_complete_marker(
        manifest=manifest,
        object_keys=object_keys,
        manifest_sha256=manifest_sha256,
    )
    complete_sha256 = compute_sha256_bytes(
        complete_body
    )
    publish_object_if_needed(
        s3_client=s3_client,
        bucket=config["bucket"],
        key=object_keys["complete_key"],
        body=complete_body,
        sha256_hex=complete_sha256,
        sse=config["sse"],
        kms_key_id=config["kms_key_id"],
        content_type="application/json",
    )
    verify_remote_object(
        s3_client=s3_client,
        bucket=config["bucket"],
        key=object_keys["complete_key"],
        expected_size=len(complete_body),
        expected_sha256=complete_sha256,
    )

    return {
        "result": "PASS",
        "format_version": REMOTE_FORMAT_VERSION,
        "complete_key": object_keys["complete_key"],
        "dump_sha256": manifest["dump_sha256"],
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish validated PostgreSQL logical backup artifacts to durable off-platform storage."
    )
    parser.add_argument(
        "--dump-file",
        required=True,
    )
    parser.add_argument(
        "--manifest",
        required=True,
    )
    return parser


def sanitize_publish_error_message(
    message: str,
) -> str:
    sanitized = sanitize_cli_error_message(message)
    sanitized = re.sub(
        r"https?://\S+",
        "[REDACTED_ENDPOINT_URL]",
        sanitized,
        flags=re.IGNORECASE,
    )
    sanitized = re.sub(
        r"(aws_access_key_id|aws_secret_access_key|session_token)"
        r"\s*=\s*[^,\s]+",
        r"\1=[REDACTED]",
        sanitized,
        flags=re.IGNORECASE,
    )
    return sanitized.strip() or "Operational failure."


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    try:
        result = run_publish(
            dump_path=Path(args.dump_file),
            manifest_path=Path(args.manifest),
        )
    except BackupToolError as exc:
        print(
            sanitize_publish_error_message(
                str(exc)
            ),
            file=sys.stderr,
        )
        return 1
    except Exception as exc:
        print(
            sanitize_publish_error_message(
                str(exc)
            ),
            file=sys.stderr,
        )
        return 1

    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
