from __future__ import annotations

import io

import pytest
from botocore.exceptions import ClientError
from pydantic import ValidationError

import litoral_trace.storage.s3 as s3_module
from litoral_trace.config.settings import (
    DEFAULT_STORAGE_ALLOWED_CONTENT_TYPES,
    Settings,
    StorageSettings,
)
from litoral_trace.storage import (
    Boto3S3ObjectStorage,
    ObjectStorageError,
    ObjectStorageNotFoundError,
)


class FakeBody:
    def __init__(self, payload: bytes):
        self._buffer = io.BytesIO(payload)
        self.closed = False

    def read(self, amount: int = -1) -> bytes:
        return self._buffer.read(amount)

    def close(self) -> None:
        self.closed = True
        self._buffer.close()


class FakeS3Client:
    def __init__(self):
        self.put_response = {
            "ETag": '"etag-put"',
            "VersionId": "version-put",
        }
        self.head_response = {
            "ContentLength": 7,
            "ContentType": "application/pdf",
            "ETag": '"etag-head"',
            "VersionId": "version-head",
            "Metadata": {"sha256": "abc"},
        }
        self.get_response = {
            "Body": FakeBody(b"payload"),
            "ContentLength": 7,
            "ContentType": "application/pdf",
            "ETag": '"etag-get"',
            "VersionId": "version-get",
            "Metadata": {"sha256": "abc"},
        }
        self.delete_response = {
            "DeleteMarker": True,
            "VersionId": "version-delete",
        }

        self.put_request = None
        self.head_request = None
        self.get_request = None
        self.delete_request = None
        self.head_bucket_request = None

        self.put_error = None
        self.head_error = None
        self.get_error = None
        self.delete_error = None
        self.health_error = None

    def put_object(self, **kwargs):
        if self.put_error:
            raise self.put_error
        self.put_request = kwargs
        return self.put_response

    def head_object(self, **kwargs):
        if self.head_error:
            raise self.head_error
        self.head_request = kwargs
        return self.head_response

    def get_object(self, **kwargs):
        if self.get_error:
            raise self.get_error
        self.get_request = kwargs
        return self.get_response

    def delete_object(self, **kwargs):
        if self.delete_error:
            raise self.delete_error
        self.delete_request = kwargs
        return self.delete_response

    def head_bucket(self, **kwargs):
        if self.health_error:
            raise self.health_error
        self.head_bucket_request = kwargs
        return {}


def _settings(**overrides) -> StorageSettings:
    values = {
        "backend": "s3",
        "bucket_name": "private-vault",
    }
    values.update(overrides)
    return StorageSettings(**values)


def _client_error(
    code: str,
    *,
    message: str = "provider detail must not leak",
) -> ClientError:
    return ClientError(
        {
            "Error": {
                "Code": code,
                "Message": message,
            }
        },
        "HeadObject",
    )


def test_storage_defaults_are_enterprise_bounded_and_inactive():
    settings = StorageSettings()

    assert settings.is_configured is False
    assert settings.backend is None
    assert settings.region == "us-east-1"
    assert settings.use_tls is True
    assert settings.verify_tls is True
    assert settings.force_path_style is False
    assert settings.connect_timeout_seconds == 5
    assert settings.read_timeout_seconds == 60
    assert settings.max_retries == 3
    assert settings.key_prefix == "vault"
    assert settings.max_upload_bytes == 25 * 1024 * 1024
    assert (
        settings.allowed_content_types
        == DEFAULT_STORAGE_ALLOWED_CONTENT_TYPES
    )


def test_storage_settings_require_backend_and_bucket_as_one_contract():
    with pytest.raises(ValidationError):
        StorageSettings(
            bucket_name="private-vault",
        )

    with pytest.raises(ValidationError):
        StorageSettings(
            backend="s3",
        )

    with pytest.raises(ValidationError):
        StorageSettings(
            backend="filesystem",
            bucket_name="private-vault",
        )


def test_storage_static_credentials_are_pairwise_and_secret_repr_is_redacted():
    with pytest.raises(ValidationError):
        _settings(
            access_key_id="access-only",
        )

    with pytest.raises(ValidationError):
        _settings(
            session_token="token-only",
        )

    settings = _settings(
        access_key_id="P23B_ACCESS_SECRET",
        secret_access_key="P23B_SECRET_SECRET",
        session_token="P23B_SESSION_SECRET",
    )

    rendered = repr(settings)

    assert "P23B_ACCESS_SECRET" not in rendered
    assert "P23B_SECRET_SECRET" not in rendered
    assert "P23B_SESSION_SECRET" not in rendered


def test_storage_endpoint_scheme_and_tls_must_be_coherent():
    with pytest.raises(ValidationError):
        _settings(
            endpoint_url="ftp://minio.internal",
        )

    with pytest.raises(ValidationError):
        _settings(
            endpoint_url="http://minio.internal:9000",
            use_tls=True,
        )

    with pytest.raises(ValidationError):
        _settings(
            endpoint_url="https://minio.internal:9000",
            use_tls=False,
        )

    settings = _settings(
        endpoint_url="http://minio:9000",
        use_tls=False,
    )
    assert settings.is_configured is True


def test_storage_endpoint_rejects_embedded_credentials():
    with pytest.raises(ValidationError):
        _settings(
            endpoint_url=(
                "https://user:password@minio.internal:9000"
            ),
        )


def test_storage_key_prefix_is_restricted_to_safe_provider_neutral_segments():
    with pytest.raises(ValidationError):
        _settings(
            key_prefix="../vault",
        )

    with pytest.raises(ValidationError):
        _settings(
            key_prefix=r"vault\tenant",
        )

    settings = _settings(
        key_prefix="/evidence/vault/",
    )
    assert settings.normalized_key_prefix == "evidence/vault"


def test_production_rejects_insecure_configured_storage(monkeypatch):
    monkeypatch.setenv(
        "ENVIRONMENT",
        "production",
    )
    monkeypatch.setenv(
        "STORAGE_BACKEND",
        "s3",
    )
    monkeypatch.setenv(
        "STORAGE_BUCKET_NAME",
        "private-vault",
    )
    monkeypatch.setenv(
        "STORAGE_ENDPOINT_URL",
        "http://minio:9000",
    )
    monkeypatch.setenv(
        "STORAGE_USE_TLS",
        "0",
    )

    with pytest.raises(ValidationError):
        Settings.from_environment()


def test_environment_reads_storage_contract_without_exposing_secrets(monkeypatch):
    monkeypatch.setenv("STORAGE_BACKEND", "s3")
    monkeypatch.setenv(
        "STORAGE_BUCKET_NAME",
        "private-vault",
    )
    monkeypatch.setenv(
        "STORAGE_ACCESS_KEY_ID",
        "P23B_ENV_ACCESS",
    )
    monkeypatch.setenv(
        "STORAGE_SECRET_ACCESS_KEY",
        "P23B_ENV_SECRET",
    )
    monkeypatch.setenv(
        "STORAGE_ALLOWED_CONTENT_TYPES",
        "application/pdf,application/json",
    )
    monkeypatch.setenv(
        "STORAGE_MAX_UPLOAD_BYTES",
        "1048576",
    )

    settings = Settings.from_environment()
    storage = settings.storage

    assert storage.is_configured is True
    assert storage.bucket_name == "private-vault"
    assert storage.max_upload_bytes == 1048576
    assert storage.allowed_content_types == (
        "application/pdf",
        "application/json",
    )

    rendered = repr(storage)
    assert "P23B_ENV_ACCESS" not in rendered
    assert "P23B_ENV_SECRET" not in rendered


def test_boto3_client_configuration_is_explicit_and_bounded(monkeypatch):
    captured = {}
    fake_client = object()

    class FakeSession:
        def __init__(self, **kwargs):
            captured["session"] = kwargs

        def client(self, service_name, **kwargs):
            captured["service_name"] = service_name
            captured["client"] = kwargs
            return fake_client

    monkeypatch.setattr(
        s3_module.boto3.session,
        "Session",
        FakeSession,
    )

    storage = Boto3S3ObjectStorage(
        _settings(
            endpoint_url="https://minio.internal:9000",
            access_key_id="access",
            secret_access_key="secret",
            session_token="token",
            force_path_style=True,
            ca_bundle_path="/run/secrets/ca.pem",
            connect_timeout_seconds=7,
            read_timeout_seconds=45,
            max_retries=4,
        )
    )

    assert storage._client is fake_client
    assert captured["service_name"] == "s3"

    assert captured["session"] == {
        "region_name": "us-east-1",
        "aws_access_key_id": "access",
        "aws_secret_access_key": "secret",
        "aws_session_token": "token",
    }

    client_kwargs = captured["client"]
    assert client_kwargs["region_name"] == "us-east-1"
    assert (
        client_kwargs["endpoint_url"]
        == "https://minio.internal:9000"
    )
    assert client_kwargs["use_ssl"] is True
    assert (
        client_kwargs["verify"]
        == "/run/secrets/ca.pem"
    )

    config = client_kwargs["config"]
    assert config.connect_timeout == 7
    assert config.read_timeout == 45
    assert config.retries["max_attempts"] == 4
    assert config.retries["mode"] == "standard"
    assert config.s3["addressing_style"] == "path"


def test_boto3_client_can_use_provider_credential_chain(monkeypatch):
    captured = {}

    class FakeSession:
        def __init__(self, **kwargs):
            captured["session"] = kwargs

        def client(self, service_name, **kwargs):
            return object()

    monkeypatch.setattr(
        s3_module.boto3.session,
        "Session",
        FakeSession,
    )

    Boto3S3ObjectStorage(
        _settings()
    )

    assert captured["session"] == {
        "region_name": "us-east-1",
    }


def test_put_object_uses_fixed_bucket_and_returns_provider_metadata():
    fake = FakeS3Client()
    storage = Boto3S3ObjectStorage(
        _settings(),
        client=fake,
    )

    result = storage.put_object(
        key="vault/org/document",
        body=b"payload",
        content_type="application/pdf",
        content_length=7,
        metadata={"sha256": "abc"},
    )

    assert fake.put_request["Bucket"] == "private-vault"
    assert (
        fake.put_request["Key"]
        == "vault/org/document"
    )
    assert (
        fake.put_request["ContentType"]
        == "application/pdf"
    )
    assert fake.put_request["ContentLength"] == 7
    assert fake.put_request["Metadata"] == {
        "sha256": "abc"
    }

    assert result.etag == "etag-put"
    assert result.version_id == "version-put"


def test_put_object_rejects_invalid_key_and_length_before_provider_call():
    fake = FakeS3Client()
    storage = Boto3S3ObjectStorage(
        _settings(),
        client=fake,
    )

    with pytest.raises(ValueError):
        storage.put_object(
            key="../secret",
            body=b"payload",
            content_type="application/pdf",
            content_length=7,
        )

    with pytest.raises(ValueError):
        storage.put_object(
            key="vault/document",
            body=b"payload",
            content_type="application/pdf",
            content_length=99,
        )

    assert fake.put_request is None


def test_head_object_and_object_exists_translate_not_found_safely():
    fake = FakeS3Client()
    storage = Boto3S3ObjectStorage(
        _settings(),
        client=fake,
    )

    head = storage.head_object(
        key="vault/document",
    )
    assert head.size_bytes == 7
    assert head.content_type == "application/pdf"
    assert head.etag == "etag-head"
    assert head.version_id == "version-head"
    assert head.metadata == {"sha256": "abc"}

    fake.head_error = _client_error(
        "NoSuchKey"
    )

    assert (
        storage.object_exists(
            key="vault/missing"
        )
        is False
    )

    with pytest.raises(
        ObjectStorageNotFoundError
    ) as exc_info:
        storage.head_object(
            key="vault/missing",
        )

    rendered = str(exc_info.value)
    assert "vault/missing" not in rendered
    assert "private-vault" not in rendered
    assert "provider detail" not in rendered


def test_get_object_stream_is_provider_neutral_and_closes_body():
    fake = FakeS3Client()
    storage = Boto3S3ObjectStorage(
        _settings(),
        client=fake,
    )

    stream = storage.get_object_stream(
        key="vault/document",
    )

    assert stream.head.size_bytes == 7
    assert stream.head.etag == "etag-get"
    assert list(
        stream.iter_chunks(
            chunk_size=3,
        )
    ) == [
        b"pay",
        b"loa",
        b"d",
    ]

    body = fake.get_response["Body"]
    assert body.closed is False
    stream.close()
    assert body.closed is True


def test_delete_object_uses_fixed_bucket_and_returns_delete_metadata():
    fake = FakeS3Client()
    storage = Boto3S3ObjectStorage(
        _settings(),
        client=fake,
    )

    result = storage.delete_object(
        key="vault/document",
        version_id="v1",
    )

    assert fake.delete_request == {
        "Bucket": "private-vault",
        "Key": "vault/document",
        "VersionId": "v1",
    }
    assert result.delete_marker is True
    assert result.version_id == "version-delete"


def test_provider_failures_are_sanitized_and_do_not_include_object_identity():
    fake = FakeS3Client()
    fake.put_error = _client_error(
        "AccessDenied",
        message=(
            "secret bucket private-vault key vault/secret "
            "credential P23B_SECRET"
        ),
    )

    storage = Boto3S3ObjectStorage(
        _settings(),
        client=fake,
    )

    with pytest.raises(
        ObjectStorageError
    ) as exc_info:
        storage.put_object(
            key="vault/secret",
            body=b"payload",
            content_type="application/pdf",
            content_length=7,
        )

    rendered = str(exc_info.value)

    assert "private-vault" not in rendered
    assert "vault/secret" not in rendered
    assert "P23B_SECRET" not in rendered
    assert "put_object" in rendered


def test_health_check_is_read_only_and_fail_closed():
    fake = FakeS3Client()
    storage = Boto3S3ObjectStorage(
        _settings(),
        client=fake,
    )

    assert storage.health_check() is True
    assert fake.head_bucket_request == {
        "Bucket": "private-vault"
    }

    fake.health_error = _client_error(
        "AccessDenied"
    )

    assert storage.health_check() is False