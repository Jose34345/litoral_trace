"""Fail-closed runtime configuration for the isolated U.S. Lacey pilot.

The pilot must never silently fall back to the Argentina production database or
object-storage namespace. Generic LT variables are inspected only to detect an
unsafe collision; they are never used as U.S. credentials.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from urllib.parse import urlsplit

from litoral_trace.config.settings import normalize_database_url


class UsLaceyConfigurationError(RuntimeError):
    """Raised when the U.S. pilot runtime is not explicitly isolated."""


@dataclass(frozen=True)
class UsLaceyRuntimeConfig:
    environment: str
    database_url: str
    storage_bucket: str
    storage_prefix: str
    app_hostname: str


def _required(env: Mapping[str, str], name: str) -> str:
    value = str(env.get(name, "")).strip()
    if not value:
        raise UsLaceyConfigurationError(f"{name} is required for the U.S. Lacey pilot.")
    return value


def _normalized_database_identity(url: str) -> tuple[str, str, int | None, str]:
    normalized = normalize_database_url(url)
    parsed = urlsplit(normalized.replace("postgresql+psycopg://", "postgresql://", 1))
    return (
        (parsed.hostname or "").lower(),
        parsed.username or "",
        parsed.port,
        (parsed.path or "").lstrip("/"),
    )


def load_us_lacey_runtime_config(
    environ: Mapping[str, str] | None = None,
) -> UsLaceyRuntimeConfig:
    env = os.environ if environ is None else environ
    environment = _required(env, "US_LACEY_ENVIRONMENT").lower()
    if environment not in {"test", "staging", "pilot", "production"}:
        raise UsLaceyConfigurationError(
            "US_LACEY_ENVIRONMENT must be test, staging, pilot or production."
        )

    raw_database_url = _required(env, "US_LACEY_DATABASE_URL")
    database_url = normalize_database_url(raw_database_url)
    if not database_url.startswith("postgresql+psycopg://"):
        raise UsLaceyConfigurationError(
            "US_LACEY_DATABASE_URL must be an explicit PostgreSQL URL."
        )

    generic_db = str(env.get("DATABASE_URL", "")).strip()
    if generic_db:
        try:
            if _normalized_database_identity(generic_db) == _normalized_database_identity(database_url):
                raise UsLaceyConfigurationError(
                    "US_LACEY_DATABASE_URL must not point to the generic/Argentina DATABASE_URL database."
                )
        except ValueError as exc:
            raise UsLaceyConfigurationError("Invalid DATABASE_URL while checking isolation.") from exc

    storage_bucket = _required(env, "US_LACEY_STORAGE_BUCKET")
    storage_prefix = _required(env, "US_LACEY_STORAGE_PREFIX").strip("/")
    if not storage_prefix or any(part in {"", ".", ".."} for part in storage_prefix.split("/")):
        raise UsLaceyConfigurationError("US_LACEY_STORAGE_PREFIX is invalid.")

    generic_bucket = str(env.get("STORAGE_BUCKET_NAME", "")).strip()
    generic_prefix = str(env.get("STORAGE_KEY_PREFIX", "")).strip().strip("/")
    if generic_bucket and storage_bucket == generic_bucket:
        if not generic_prefix or storage_prefix == generic_prefix:
            raise UsLaceyConfigurationError(
                "U.S. Lacey storage must use a distinct bucket or an explicitly distinct prefix."
            )

    hostname = str(env.get("US_LACEY_APP_HOSTNAME", "app.lacey.litoraltrace.com")).strip().lower()
    if hostname != "app.lacey.litoraltrace.com" and environment in {"pilot", "production"}:
        raise UsLaceyConfigurationError(
            "Pilot/production hostname must be app.lacey.litoraltrace.com."
        )

    return UsLaceyRuntimeConfig(
        environment=environment,
        database_url=database_url,
        storage_bucket=storage_bucket,
        storage_prefix=storage_prefix,
        app_hostname=hostname,
    )
