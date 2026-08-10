"""Configuracion tipada y centralizada para Litoral Trace."""
from __future__ import annotations

import os
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


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
    model_config = ConfigDict(frozen=True)

    backend: str | None = None
    bucket_name: str | None = None
    endpoint_url: str | None = None


class ObservabilitySettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    log_level: str = "INFO"
    sentry_dsn: str | None = None


class WorkersSettings(BaseModel):
    model_config = ConfigDict(frozen=True)

    enabled: bool = False
    worker_database_url: str | None = Field(default=None, repr=False)
    broker_url: str | None = None
    result_backend_url: str | None = None
    satellite_worker_id: str | None = None
    satellite_worker_poll_seconds: int = Field(default=5, ge=1, le=300)


class Settings(BaseModel):
    model_config = ConfigDict(frozen=True)

    environment: Environment = Environment.DEVELOPMENT
    database: DatabaseSettings
    jwt: JwtSettings
    cors: CorsSettings
    cache: CacheSettings
    gee: GeeSettings
    storage: StorageSettings
    observability: ObservabilitySettings
    workers: WorkersSettings

    @property
    def is_test(self) -> bool:
        return self.environment == Environment.TEST

    @property
    def is_production(self) -> bool:
        return self.environment == Environment.PRODUCTION

    @property
    def is_staging(self) -> bool:
        return self.environment == Environment.STAGING

    @classmethod
    def from_environment(cls) -> Settings:
        raw_environment = (
            _read_optional_env("ENVIRONMENT")
            or Environment.DEVELOPMENT.value
        )
        environment = Environment(raw_environment.lower())

        return cls(
            environment=environment,
            database=DatabaseSettings(
                database_url=_read_optional_env("DATABASE_URL"),
                migration_database_url=_read_optional_env(
                    "MIGRATION_DATABASE_URL"
                ),
                postgres_url=_read_optional_env("POSTGRES_URL"),
                db_url=_read_optional_env("DB_URL"),
                test_database_url=_read_optional_env("TEST_DATABASE_URL"),
                test_postgres_database_url=_read_optional_env(
                    "TEST_POSTGRES_DATABASE_URL"
                ),
                enable_postgres_tests=_read_bool_env(
                    "ENABLE_POSTGRES_TESTS",
                    default=False,
                ),
            ),
            jwt=JwtSettings(
                secret_key=_read_optional_env("JWT_SECRET_KEY"),
                algorithm=_read_optional_env("JWT_ALGORITHM") or "HS256",
                issuer=_read_optional_env("JWT_ISSUER"),
                audience=_read_optional_env("JWT_AUDIENCE"),
                access_token_expire_minutes=_read_int_env(
                    "ACCESS_TOKEN_EXPIRE_MINUTES",
                    default=30,
                ),
                refresh_token_expire_days=_read_int_env(
                    "REFRESH_TOKEN_EXPIRE_DAYS",
                    default=30,
                ),
            ),
            cors=CorsSettings(
                allow_origins=_read_csv_env("CORS_ALLOW_ORIGINS"),
            ),
            cache=CacheSettings(
                redis_url=_read_optional_env("REDIS_URL")
                or "redis://localhost:6379/0",
            ),
            gee=GeeSettings(
                gcp_project_id=_read_optional_env("GCP_PROJECT_ID"),
                gee_project_id=_read_optional_env("GEE_PROJECT_ID"),
                gcp_service_account=_read_optional_env("GCP_SERVICE_ACCOUNT"),
                gee_service_account=_read_optional_env("GEE_SERVICE_ACCOUNT"),
                test_mode=_read_bool_env("TEST_MODE", default=False),
            ),
            storage=StorageSettings(
                backend=_read_optional_env("STORAGE_BACKEND"),
                bucket_name=_read_optional_env("STORAGE_BUCKET_NAME"),
                endpoint_url=_read_optional_env("STORAGE_ENDPOINT_URL"),
            ),
            observability=ObservabilitySettings(
                log_level=_read_optional_env("LOG_LEVEL") or "INFO",
                sentry_dsn=_read_optional_env("SENTRY_DSN"),
            ),
            workers=WorkersSettings(
                enabled=_read_bool_env("WORKERS_ENABLED", default=False),
                worker_database_url=_read_optional_env("WORKER_DATABASE_URL"),
                broker_url=_read_optional_env("WORKER_BROKER_URL"),
                result_backend_url=_read_optional_env("WORKER_RESULT_BACKEND_URL"),
                satellite_worker_id=_read_optional_env("SATELLITE_WORKER_ID"),
                satellite_worker_poll_seconds=_read_int_env(
                    "SATELLITE_WORKER_POLL_SECONDS",
                    default=5,
                ),
            ),
        )


def get_settings() -> Settings:
    """Construye una vista tipada del entorno actual."""
    return Settings.from_environment()


def resolve_runtime_database_url(settings: Settings | None = None) -> str | None:
    active_settings = settings or get_settings()
    return active_settings.database.runtime_database_url


def resolve_migration_database_url(settings: Settings | None = None) -> str | None:
    active_settings = settings or get_settings()
    return active_settings.database.resolved_migration_database_url


def resolve_worker_database_url(settings: Settings | None = None) -> str | None:
    active_settings = settings or get_settings()
    worker_database_url = active_settings.workers.worker_database_url
    if worker_database_url:
        return normalize_database_url(worker_database_url)
    return None
