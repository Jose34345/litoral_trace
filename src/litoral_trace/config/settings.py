"Configuracion tipada y centralizada para Litoral Trace."
from __future__ import annotations

import os
from enum import Enum
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field


DEFAULT_STORAGE_ALLOWED_CONTENT_TYPES = (
    "application/pdf",
    "application/json",
    "application/xml",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel",
    "text/csv",
)


class Environment(str, Enum):
    DEVELOPMENT = "development"
    TEST = "test"
    STAGING = "staging"
    PRODUCTION = "production"


def _read_optional_env(*variable_names: str) -> str | None:
    for variable_name in variable_names:
        value = os.environ.get(variable_name, "").strip()
        if value:
            return value
    return None


def _read_bool_env(variable_name: str, *, default: bool = False) -> bool:
    raw_value = os.environ.get(variable_name)
    if raw_value is None:
        return default

    normalized_value = raw_value.strip().lower()
    if normalized_value in {"1", "true", "yes", "on"}:
        return True
    if normalized_value in {"0", "false", "no", "off"}:
        return False

    raise RuntimeError(
        f"{variable_name} debe ser booleano ('true'/'false' o '1'/'0')."
    )


def _read_int_env(variable_name: str, *, default: int) -> int:
    raw_value = os.environ.get(variable_name)
    if raw_value is None or not raw_value.strip():
        return default

    try:
        return int(raw_value.strip())
    except ValueError as exc:
        raise RuntimeError(
            f"{variable_name} debe ser un entero valido."
        ) from exc


def _read_csv_env(variable_name: str) -> tuple[str, ...]:
    raw_value = os.environ.get(variable_name, "").strip()
    if not raw_value:
        return ()
    return tuple(
        item.strip()
        for item in raw_value.split(",")
        if item.strip()
    )


def normalize_database_url(database_url: str) -> str:
    normalized_url = database_url.strip()
    if not normalized_url:
        return normalized_url
    if normalized_url.startswith("postgres://"):
        return normalized_url.replace("postgres://", "postgresql+psycopg://", 1)
    if normalized_url.startswith("postgresql://"):
        return normalized_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return normalized_url


class DatabaseSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    database_url: str | None = Field(default=None, repr=False)
    migration_database_url: str | None = Field(default=None, repr=False)
    postgres_url: str | None = Field(default=None, repr=False)
    db_url: str | None = Field(default=None, repr=False)
    test_database_url: str | None = Field(default=None, repr=False)
    test_postgres_database_url: str | None = Field(default=None, repr=False)
    enable_postgres_tests: bool = False

    @property
    def runtime_database_url(self) -> str | None:
        for value in (
            self.database_url,
            self.postgres_url,
            self.db_url,
        ):
            if value:
                return normalize_database_url(value)
        return None

    @property
    def application_database_url(self) -> str | None:
        """Compatibilidad con llamadas existentes; usar runtime_database_url nuevo."""
        return self.runtime_database_url

    @property
    def resolved_migration_database_url(self) -> str | None:
        if self.migration_database_url:
            return normalize_database_url(self.migration_database_url)
        return self.runtime_database_url

    @property
    def resolved_test_database_url(self) -> str:
        if self.test_database_url:
            return normalize_database_url(self.test_database_url)

        if self.test_postgres_database_url:
            if not self.enable_postgres_tests:
                raise RuntimeError(
                    "TEST_POSTGRES_DATABASE_URL requiere ENABLE_POSTGRES_TESTS=1 "
                    "para evitar uso accidental durante pytest ordinario."
                )

            application_database_url = self.runtime_database_url
            normalized_postgres_test_url = normalize_database_url(
                self.test_postgres_database_url
            )
            if (
                application_database_url
                and application_database_url == normalized_postgres_test_url
            ):
                raise RuntimeError(
                    "TEST_POSTGRES_DATABASE_URL debe ser distinto de DATABASE_URL "
                    "para evitar que los tests reutilicen la base de la aplicacion."
                )

            return normalized_postgres_test_url

        return "sqlite:///litoral_trace_test.db"


class JwtSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    secret_key: str | None = None
    algorithm: str = "HS256"
    issuer: str | None = None
    audience: str | None = None
    access_token_expire_minutes: int = Field(default=30, ge=1)
    refresh_token_expire_days: int = Field(default=30, ge=1)

    @property
    def access_token_expire_seconds(self) -> int:
        return self.access_token_expire_minutes * 60


class CorsSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    allow_origins: tuple[str, ...] = ()


class CacheSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    redis_url: str = "redis://localhost:6379/0"


class GeeSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    gcp_project_id: str | None = None
    gee_project_id: str | None = None
    gcp_service_account: str | None = None
    gee_service_account: str | None = None
    test_mode: bool = False

    @property
    def project_id(self) -> str:
        return (
            self.gcp_project_id
            or self.gee_project_id
            or "litoral-trace-engine"
        )

    @property
    def service_account_json(self) -> str | None:
        return self.gcp_service_account or self.gee_service_account


class StorageSettings(BaseModel):
    """Private S3-compatible object-storage configuration."""

    model_config = ConfigDict(frozen=True)

    backend: str | None = None
    bucket_name: str | None = None
    region: str = "us-east-1"
    endpoint_url: str | None = None

    access_key_id: str | None = Field(default=None, repr=False)
    secret_access_key: str | None = Field(default=None, repr=False)
    session_token: str | None = Field(default=None, repr=False)

    force_path_style: bool = False
    use_tls: bool = True
    verify_tls: bool = True
    ca_bundle_path: str | None = None

    connect_timeout_seconds: int = Field(default=5, ge=1, le=120)
    read_timeout_seconds: int = Field(default=60, ge=1, le=3600)
    max_retries: int = Field(default=3, ge=0, le=20)

    key_prefix: str = "vault"
    max_upload_bytes: int = Field(
        default=25 * 1024 * 1024,
        ge=1,
        le=5 * 1024 * 1024 * 1024,
    )
    allowed_content_types: tuple[str, ...] = (
        DEFAULT_STORAGE_ALLOWED_CONTENT_TYPES
    )

    def model_post_init(self, __context: object) -> None:
        backend = self.normalized_backend

        explicit_storage_values = (
            self.bucket_name,
            self.endpoint_url,
            self.access_key_id,
            self.secret_access_key,
            self.session_token,
        )
        if backend is None:
            if any(value for value in explicit_storage_values):
                raise ValueError(
                    "STORAGE_BACKEND es obligatorio cuando se configura storage."
                )
            return

        if backend != "s3":
            raise ValueError(
                "STORAGE_BACKEND soportado en P2.3: 's3'."
            )

        if not self.bucket_name or not self.bucket_name.strip():
            raise ValueError(
                "STORAGE_BUCKET_NAME es obligatorio cuando STORAGE_BACKEND=s3."
            )

        if any(ord(character) < 32 for character in self.bucket_name):
            raise ValueError(
                "STORAGE_BUCKET_NAME contiene caracteres de control."
            )

        if not self.access_key_id or not self.access_key_id.strip():
            raise ValueError(
                "STORAGE_ACCESS_KEY_ID es obligatorio cuando STORAGE_BACKEND=s3."
            )
        if not self.secret_access_key or not self.secret_access_key.strip():
            raise ValueError(
                "STORAGE_SECRET_ACCESS_KEY es obligatorio cuando STORAGE_BACKEND=s3."
            )

        allowed_content_types = tuple(
            content_type.strip().lower()
            for content_type in self.allowed_content_types
            if content_type and content_type.strip()
        )
        if not allowed_content_types:
            raise ValueError(
                "STORAGE_ALLOWED_CONTENT_TYPES debe contener al menos un MIME type."
            )
        object.__setattr__(
            self,
            "allowed_content_types",
            allowed_content_types,
        )

    @property
    def normalized_backend(self) -> str | None:
        if not self.backend:
            return None
        return self.backend.strip().lower()

    @property
    def normalized_key_prefix(self) -> str:
        prefix = self.key_prefix.strip().strip("/")
        return prefix or "vault"

    @property
    def is_configured(self) -> bool:
        return self.normalized_backend == "s3" and bool(
            self.bucket_name
            and self.access_key_id
            and self.secret_access_key
        )
