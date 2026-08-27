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

        has_access_key = bool(self.access_key_id)
        has_secret_key = bool(self.secret_access_key)
        if has_access_key != has_secret_key:
            raise ValueError(
                "STORAGE_ACCESS_KEY_ID y STORAGE_SECRET_ACCESS_KEY "
                "deben configurarse juntos."
            )

        if self.session_token and not (has_access_key and has_secret_key):
            raise ValueError(
                "STORAGE_SESSION_TOKEN requiere credenciales estaticas completas."
            )

        if self.endpoint_url:
            parsed_endpoint = urlsplit(self.endpoint_url)
            if parsed_endpoint.scheme not in {"http", "https"}:
                raise ValueError(
                    "STORAGE_ENDPOINT_URL debe usar esquema http o https."
                )
            if not parsed_endpoint.netloc:
                raise ValueError(
                    "STORAGE_ENDPOINT_URL debe incluir un host."
                )
            if parsed_endpoint.username or parsed_endpoint.password:
                raise ValueError(
                    "STORAGE_ENDPOINT_URL no debe contener credenciales."
                )

            endpoint_uses_tls = parsed_endpoint.scheme == "https"
            if endpoint_uses_tls != self.use_tls:
                raise ValueError(
                    "STORAGE_USE_TLS debe coincidir con el esquema "
                    "de STORAGE_ENDPOINT_URL."
                )

        if self.ca_bundle_path and not self.use_tls:
            raise ValueError(
                "STORAGE_CA_BUNDLE_PATH requiere STORAGE_USE_TLS=true."
            )
        if self.ca_bundle_path and not self.verify_tls:
            raise ValueError(
                "STORAGE_CA_BUNDLE_PATH requiere STORAGE_VERIFY_TLS=true."
            )

        normalized_prefix = self.normalized_key_prefix
        if not normalized_prefix:
            raise ValueError(
                "STORAGE_KEY_PREFIX no puede ser vacio."
            )
        if "\\" in normalized_prefix:
            raise ValueError(
                "STORAGE_KEY_PREFIX no puede contener backslashes."
            )
        if any(ord(character) < 32 for character in normalized_prefix):
            raise ValueError(
                "STORAGE_KEY_PREFIX contiene caracteres de control."
            )
        if any(
            part in {"", ".", ".."}
            for part in normalized_prefix.split("/")
        ):
            raise ValueError(
                "STORAGE_KEY_PREFIX contiene segmentos no permitidos."
            )

        if not self.allowed_content_types:
            raise ValueError(
                "STORAGE_ALLOWED_CONTENT_TYPES no puede ser vacio."
            )
        if any(
            not content_type
            or "/" not in content_type
            or content_type != content_type.strip()
            for content_type in self.allowed_content_types
        ):
            raise ValueError(
                "STORAGE_ALLOWED_CONTENT_TYPES contiene un MIME invalido."
            )

    @property
    def normalized_backend(self) -> str | None:
        if self.backend is None:
            return None
        normalized = self.backend.strip().lower()
        return normalized or None

    @property
    def normalized_key_prefix(self) -> str:
        return self.key_prefix.strip().strip("/")

    @property
    def is_configured(self) -> bool:
        return (
            self.normalized_backend == "s3"
            and bool(self.bucket_name and self.bucket_name.strip())
        )

    @property
    def tls_verify_value(self) -> bool | str:
        if self.ca_bundle_path:
            return self.ca_bundle_path
        return self.verify_tls

    def require_configured(self) -> None:
        if not self.is_configured:
            raise RuntimeError(
                "Object storage S3 no esta configurado."
            )


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
    satellite_worker_heartbeat_seconds: int = Field(default=15, ge=1, le=300)
    satellite_worker_stale_recovery_interval_seconds: int = Field(
        default=30,
        ge=5,
        le=300,
    )
    satellite_worker_retry_base_seconds: int = Field(
        default=30,
        ge=5,
        le=300,
    )
    satellite_worker_retry_max_seconds: int = Field(
        default=900,
        ge=30,
        le=86400,
    )
    satellite_metrics_enabled: bool = False
    satellite_metrics_host: str = "127.0.0.1"
    satellite_metrics_port: int = Field(default=9108, ge=1, le=65535)
    satellite_queue_metrics_refresh_seconds: int = Field(
        default=30,
        ge=1,
        le=3600,
    )

    def model_post_init(self, __context: object) -> None:
        if self.satellite_metrics_enabled and not self.satellite_metrics_host.strip():
            raise ValueError(
                "SATELLITE_METRICS_HOST no puede ser vacio cuando metrics esta habilitado."
            )


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

    def model_post_init(self, __context: object) -> None:
        if not self.is_production or not self.storage.is_configured:
            return

        if not self.storage.use_tls:
            raise ValueError(
                "Produccion requiere STORAGE_USE_TLS=true."
            )
        if not self.storage.verify_tls:
            raise ValueError(
                "Produccion requiere STORAGE_VERIFY_TLS=true."
            )

        if self.storage.endpoint_url:
            endpoint_scheme = urlsplit(
                self.storage.endpoint_url
            ).scheme
            if endpoint_scheme != "https":
                raise ValueError(
                    "Produccion requiere STORAGE_ENDPOINT_URL con https."
                )

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

        storage_allowed_content_types = (
            _read_csv_env("STORAGE_ALLOWED_CONTENT_TYPES")
            or DEFAULT_STORAGE_ALLOWED_CONTENT_TYPES
        )

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
                region=_read_optional_env("STORAGE_REGION")
                or "us-east-1",
                endpoint_url=_read_optional_env("STORAGE_ENDPOINT_URL"),
                access_key_id=_read_optional_env(
                    "STORAGE_ACCESS_KEY_ID"
                ),
                secret_access_key=_read_optional_env(
                    "STORAGE_SECRET_ACCESS_KEY"
                ),
                session_token=_read_optional_env(
                    "STORAGE_SESSION_TOKEN"
                ),
                force_path_style=_read_bool_env(
                    "STORAGE_FORCE_PATH_STYLE",
                    default=False,
                ),
                use_tls=_read_bool_env(
                    "STORAGE_USE_TLS",
                    default=True,
                ),
                verify_tls=_read_bool_env(
                    "STORAGE_VERIFY_TLS",
                    default=True,
                ),
                ca_bundle_path=_read_optional_env(
                    "STORAGE_CA_BUNDLE_PATH"
                ),
                connect_timeout_seconds=_read_int_env(
                    "STORAGE_CONNECT_TIMEOUT_SECONDS",
                    default=5,
                ),
                read_timeout_seconds=_read_int_env(
                    "STORAGE_READ_TIMEOUT_SECONDS",
                    default=60,
                ),
                max_retries=_read_int_env(
                    "STORAGE_MAX_RETRIES",
                    default=3,
                ),
                key_prefix=_read_optional_env(
                    "STORAGE_KEY_PREFIX"
                )
                or "vault",
                max_upload_bytes=_read_int_env(
                    "STORAGE_MAX_UPLOAD_BYTES",
                    default=25 * 1024 * 1024,
                ),
                allowed_content_types=storage_allowed_content_types,
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
                satellite_worker_heartbeat_seconds=_read_int_env(
                    "SATELLITE_WORKER_HEARTBEAT_SECONDS",
                    default=15,
                ),
                satellite_worker_stale_recovery_interval_seconds=_read_int_env(
                    "SATELLITE_WORKER_STALE_RECOVERY_INTERVAL_SECONDS",
                    default=30,
                ),
                satellite_worker_retry_base_seconds=_read_int_env(
                    "SATELLITE_WORKER_RETRY_BASE_SECONDS",
                    default=30,
                ),
                satellite_worker_retry_max_seconds=_read_int_env(
                    "SATELLITE_WORKER_RETRY_MAX_SECONDS",
                    default=900,
                ),
                satellite_metrics_enabled=_read_bool_env(
                    "SATELLITE_METRICS_ENABLED",
                    default=False,
                ),
                satellite_metrics_host=(
                    _read_optional_env("SATELLITE_METRICS_HOST")
                    or "127.0.0.1"
                ),
                satellite_metrics_port=_read_int_env(
                    "SATELLITE_METRICS_PORT",
                    default=9108,
                ),
                satellite_queue_metrics_refresh_seconds=_read_int_env(
                    "SATELLITE_QUEUE_METRICS_REFRESH_SECONDS",
                    default=30,
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