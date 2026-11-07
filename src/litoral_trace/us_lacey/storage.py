"""Private object-storage configuration for the U.S. Lacey pilot."""
from __future__ import annotations

from collections.abc import Mapping
import os

from litoral_trace.config.settings import StorageSettings
from litoral_trace.storage.s3 import Boto3S3ObjectStorage
from litoral_trace.us_lacey.config import load_us_lacey_runtime_config


def us_lacey_max_upload_bytes(environ: Mapping[str, str] | None = None) -> int:
    env = os.environ if environ is None else environ
    limit = int(env.get("US_LACEY_MAX_UPLOAD_BYTES", 50 * 1024 * 1024))
    if limit < 1:
        raise ValueError("The upload size limit must be positive.")
    return limit


def us_lacey_max_request_bytes(environ: Mapping[str, str] | None = None) -> int:
    """Total multipart budget, including framing, enforced before form parsing."""
    env = os.environ if environ is None else environ
    per_file = us_lacey_max_upload_bytes(env)
    limit = int(env.get("US_LACEY_MAX_REQUEST_BYTES", 4 * per_file + 1024 * 1024))
    if limit < per_file:
        raise ValueError("Total upload request size must cover one permitted file.")
    return limit


def build_us_lacey_storage_settings(
    environ: Mapping[str, str] | None = None,
) -> StorageSettings:
    env = os.environ if environ is None else environ
    runtime = load_us_lacey_runtime_config(env)

    def optional(name: str) -> str | None:
        value = str(env.get(name, "")).strip()
        return value or None

    return StorageSettings(
        backend="s3",
        bucket_name=runtime.storage_bucket,
        region=str(env.get("US_LACEY_STORAGE_REGION", "us-east-1")).strip() or "us-east-1",
        endpoint_url=optional("US_LACEY_STORAGE_ENDPOINT_URL"),
        access_key_id=optional("US_LACEY_STORAGE_ACCESS_KEY_ID"),
        secret_access_key=optional("US_LACEY_STORAGE_SECRET_ACCESS_KEY"),
        session_token=optional("US_LACEY_STORAGE_SESSION_TOKEN"),
        force_path_style=str(env.get("US_LACEY_STORAGE_FORCE_PATH_STYLE", "0")).strip().lower()
        in {"1", "true", "yes", "on"},
        use_tls=str(env.get("US_LACEY_STORAGE_USE_TLS", "1")).strip().lower()
        not in {"0", "false", "no", "off"},
        verify_tls=str(env.get("US_LACEY_STORAGE_VERIFY_TLS", "1")).strip().lower()
        not in {"0", "false", "no", "off"},
        key_prefix=runtime.storage_prefix,
        max_upload_bytes=us_lacey_max_upload_bytes(env),
        allowed_content_types=(
            "application/pdf",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "text/csv",
        ),
    )


def get_us_lacey_storage_client(
    environ: Mapping[str, str] | None = None,
) -> Boto3S3ObjectStorage:
    return Boto3S3ObjectStorage(build_us_lacey_storage_settings(environ))
