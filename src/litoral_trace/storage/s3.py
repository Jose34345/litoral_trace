"Private S3-compatible object-storage adapter."
from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any, BinaryIO, Protocol, runtime_checkable

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from litoral_trace.config.settings import StorageSettings


_NOT_FOUND_ERROR_CODES = frozenset(
    {
        "404",
        "NoSuchKey",
        "NoSuchObject",
        "NotFound",
    }
)


class ObjectStorageError(RuntimeError):
    """Generic sanitized object-storage failure."""

    def __init__(self, operation: str):
        self.operation = str(operation).strip() or "object_storage"
        super().__init__(
            f"Object storage operation failed: {self.operation}."
        )


class ObjectStorageNotFoundError(ObjectStorageError):
    """Requested private object does not exist."""

    def __init__(self, operation: str = "get_object"):
        super().__init__(operation)


class ObjectStorageConfigurationError(RuntimeError):
    """Object storage is not configured for runtime use."""


@dataclass(frozen=True)
class ObjectWriteResult:
    etag: str | None
    version_id: str | None


@dataclass(frozen=True)
class ObjectDeleteResult:
    delete_marker: bool
    version_id: str | None


@dataclass(frozen=True)
class ObjectHead:
    size_bytes: int
    content_type: str | None
    etag: str | None
    version_id: str | None
    metadata: Mapping[str, str]


class ObjectStorageStream:
    """Provider-neutral wrapper around a streaming object body."""

    def __init__(
        self,
        *,
        body: Any,
        head: ObjectHead,
    ):
        self._body = body
        self.head = head

    def read(self, amount: int = -1) -> bytes:
        return self._body.read(amount)

    def iter_chunks(
        self,
        *,
        chunk_size: int = 64 * 1024,
    ) -> Iterator[bytes]:
        if chunk_size <= 0:
            raise ValueError(
                "chunk_size debe ser positivo."
            )

        while True:
            chunk = self._body.read(chunk_size)
            if not chunk:
                break
            yield chunk

    def close(self) -> None:
        close_method = getattr(
            self._body,
            "close",
            None,
        )
        if callable(close_method):
            close_method()

    def __enter__(self) -> ObjectStorageStream:
        return self

    def __exit__(
        self,
        exc_type,
        exc,
        traceback,
    ) -> None:
        self.close()


@runtime_checkable
class ObjectStorageClient(Protocol):
    def put_object(
        self,
        *,
        key: str,
        body: bytes | BinaryIO,
        content_type: str,
        content_length: int,
        metadata: Mapping[str, str] | None = None,
    ) -> ObjectWriteResult:
        ...

    def get_object_stream(
        self,
        *,
        key: str,
        version_id: str | None = None,
    ) -> ObjectStorageStream:
        ...

    def head_object(
        self,
        *,
        key: str,
        version_id: str | None = None,
    ) -> ObjectHead:
        ...

    def delete_object(
        self,
        *,
        key: str,
        version_id: str | None = None,
    ) -> ObjectDeleteResult:
        ...

    def object_exists(
        self,
        *,
        key: str,
        version_id: str | None = None,
    ) -> bool:
        ...

    def health_check(self) -> bool:
        ...


def _normalize_etag(value: object) -> str | None:
    if value is None:
        return None

    normalized = str(value).strip()
    if (
        len(normalized) >= 2
        and normalized.startswith('"')
        and normalized.endswith('"')
    ):
        normalized = normalized[1:-1]

    return normalized or None


def _normalize_optional_string(
    value: object,
) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _validate_key(key: str) -> str:
    normalized = str(key or "").strip()

    if not normalized:
        raise ValueError(
            "Object storage key no puede ser vacio."
        )
    if normalized.startswith("/"):
        raise ValueError(
            "Object storage key no puede comenzar con '/'."
        )
    if "\\" in normalized:
        raise ValueError(
            "Object storage key no puede contener backslashes."
        )
    if any(ord(character) < 32 for character in normalized):
        raise ValueError(
            "Object storage key contiene caracteres de control."
        )
    if any(
        part in {"", ".", ".."}
        for part in normalized.split("/")
    ):
        raise ValueError(
            "Object storage key contiene segmentos no permitidos."
        )

    return normalized


def _client_error_code(exc: ClientError) -> str:
    response = getattr(
        exc,
        "response",
        {},
    )
    error = response.get("Error", {})
    return str(error.get("Code", "")).strip()


class Boto3S3ObjectStorage:
    """Thin boto3 S3 adapter with a fixed private bucket."""

    def __init__(
        self,
        settings: StorageSettings,
        *,
        client: Any | None = None,
    ):
        try:
            settings.require_configured()
        except RuntimeError as exc:
            raise ObjectStorageConfigurationError(
                "Object storage S3 no esta configurado."
            ) from exc

        self._settings = settings
        self._bucket_name = str(
            settings.bucket_name
        ).strip()
        self._client = (
            client
            if client is not None
            else self._build_client(settings)
        )

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    @staticmethod
    def _build_client(
        settings: StorageSettings,
    ):
        session_kwargs: dict[str, Any] = {
            "region_name": settings.region,
        }

        if settings.access_key_id:
            session_kwargs.update(
                {
                    "aws_access_key_id": settings.access_key_id,
                    "aws_secret_access_key": (
                        settings.secret_access_key
                    ),
                }
            )
            if settings.session_token:
                session_kwargs[
                    "aws_session_token"
                ] = settings.session_token

        session = boto3.session.Session(
            **session_kwargs
        )

        boto_config = Config(
            connect_timeout=(
                settings.connect_timeout_seconds
            ),
            read_timeout=settings.read_timeout_seconds,
            retries={
                "max_attempts": settings.max_retries,
                "mode": "standard",
            },
            s3={
                "addressing_style": (
                    "path"
                    if settings.force_path_style
                    else "auto"
                ),
            },
        )

        return session.client(
            "s3",
            region_name=settings.region,
            endpoint_url=settings.endpoint_url,
            use_ssl=settings.use_tls,
            verify=settings.tls_verify_value,
            config=boto_config,
        )

    @staticmethod
    def _raise_translated_error(
        operation: str,
        exc: Exception,
    ) -> None:
        if (
            isinstance(exc, ClientError)
            and _client_error_code(exc)
            in _NOT_FOUND_ERROR_CODES
        ):
            raise ObjectStorageNotFoundError(
                operation
            ) from None

        if isinstance(
            exc,
            (
                ClientError,
                BotoCoreError,
            ),
        ):
            raise ObjectStorageError(
                operation
            ) from None

        raise exc

    def put_object(
        self,
        *,
        key: str,
        body: bytes | BinaryIO,
        content_type: str,
        content_length: int,
        metadata: Mapping[str, str] | None = None,
    ) -> ObjectWriteResult:
        normalized_key = _validate_key(key)
        normalized_content_type = str(
            content_type or ""
        ).strip()
        normalized_content_length = int(
            content_length
        )

        if not normalized_content_type:
            raise ValueError(
                "content_type no puede ser vacio."
            )
        if normalized_content_length <= 0:
            raise ValueError(
                "content_length debe ser positivo."
            )
        if isinstance(body, bytes):
            if len(body) != normalized_content_length:
                raise ValueError(
                    "content_length no coincide con el cuerpo."
                )

        request: dict[str, Any] = {
            "Bucket": self._bucket_name,
            "Key": normalized_key,
            "Body": body,
            "ContentType": normalized_content_type,
            "ContentLength": normalized_content_length,
        }

        if metadata:
            request["Metadata"] = {
                str(name): str(value)
                for name, value in metadata.items()
            }

        try:
            response = self._client.put_object(
                **request
            )
        except Exception as exc:
            self._raise_translated_error(
                "put_object",
                exc,
            )
            raise AssertionError(
                "unreachable"
            ) from exc

        return ObjectWriteResult(
            etag=_normalize_etag(
                response.get("ETag")
            ),
            version_id=_normalize_optional_string(
                response.get("VersionId")
            ),
        )

    def head_object(
        self,
        *,
        key: str,
        version_id: str | None = None,
    ) -> ObjectHead:
        normalized_key = _validate_key(key)

        request: dict[str, Any] = {
            "Bucket": self._bucket_name,
            "Key": normalized_key,
        }
        if version_id:
            request["VersionId"] = str(
                version_id
            ).strip()

        try:
            response = self._client.head_object(
                **request
            )
        except Exception as exc:
            self._raise_translated_error(
                "head_object",
                exc,
            )
            raise AssertionError(
                "unreachable"
            ) from exc

        return ObjectHead(
            size_bytes=int(
                response.get(
                    "ContentLength",
                    0,
                )
            ),
            content_type=_normalize_optional_string(
                response.get("ContentType")
            ),
            etag=_normalize_etag(
                response.get("ETag")
            ),
            version_id=_normalize_optional_string(
                response.get("VersionId")
            ),
            metadata={
                str(name): str(value)
                for name, value in (
                    response.get("Metadata") or {}
                ).items()
            },
        )

    def get_object_stream(
        self,
        *,
        key: str,
        version_id: str | None = None,
    ) -> ObjectStorageStream:
        normalized_key = _validate_key(key)

        request: dict[str, Any] = {
            "Bucket": self._bucket_name,
            "Key": normalized_key,
        }
        if version_id:
            request["VersionId"] = str(
                version_id
            ).strip()

        try:
            response = self._client.get_object(
                **request
            )
        except Exception as exc:
            self._raise_translated_error(
                "get_object",
                exc,
            )
            raise AssertionError(
                "unreachable"
            ) from exc

        body = response.get("Body")
        if body is None:
            raise ObjectStorageError(
                "get_object"
            )

        head = ObjectHead(
            size_bytes=int(
                response.get(
                    "ContentLength",
                    0,
                )
            ),
            content_type=_normalize_optional_string(
                response.get("ContentType")
            ),
            etag=_normalize_etag(
                response.get("ETag")
            ),
            version_id=_normalize_optional_string(
                response.get("VersionId")
            ),
            metadata={
                str(name): str(value)
                for name, value in (
                    response.get("Metadata") or {}
                ).items()
            },
        )

        return ObjectStorageStream(
            body=body,
            head=head,
        )

    def delete_object(
        self,
        *,
        key: str,
        version_id: str | None = None,
    ) -> ObjectDeleteResult:
        normalized_key = _validate_key(key)

        request: dict[str, Any] = {
            "Bucket": self._bucket_name,
            "Key": normalized_key,
        }
        if version_id:
            request["VersionId"] = str(
                version_id
            ).strip()

        try:
            response = self._client.delete_object(
                **request
            )
        except Exception as exc:
            self._raise_translated_error(
                "delete_object",
                exc,
            )
            raise AssertionError(
                "unreachable"
            ) from exc

        return ObjectDeleteResult(
            delete_marker=bool(
                response.get(
                    "DeleteMarker",
                    False,
                )
            ),
            version_id=_normalize_optional_string(
                response.get("VersionId")
            ),
        )

    def object_exists(
        self,
        *,
        key: str,
        version_id: str | None = None,
    ) -> bool:
        try:
            self.head_object(
                key=key,
                version_id=version_id,
            )
        except ObjectStorageNotFoundError:
            return False

        return True

    def health_check(self) -> bool:
        try:
            self._client.head_bucket(
                Bucket=self._bucket_name
            )
        except (
            ClientError,
            BotoCoreError,
        ):
            return False

        return True