from __future__ import annotations

import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import StorageSettings, normalize_database_url
from litoral_trace.services.vault import (
    JSON_CONTENT_TYPE,
    VaultNotFoundError,
    VaultService,
)
from litoral_trace.storage import (
    Boto3S3ObjectStorage,
    ObjectStorageNotFoundError,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
POSTGRES_ENV_PATH = ROOT_DIR / ".env.integration"
MINIO_ENV_PATH = ROOT_DIR / ".env.minio.integration"
EXPECTED_REVISION = "016_add_vault_documents"
_ALLOWED_MINIO_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        values[name.strip()] = value.strip()
    return values


POSTGRES_ENV = _read_env_file(POSTGRES_ENV_PATH)
MINIO_ENV = _read_env_file(MINIO_ENV_PATH)

POSTGRES_TESTS_ENABLED = _truthy(
    POSTGRES_ENV.get("ENABLE_POSTGRES_TESTS")
)
MINIO_TESTS_ENABLED = _truthy(
    MINIO_ENV.get("ENABLE_MINIO_TESTS")
)

RUNTIME_DATABASE_URL = POSTGRES_ENV.get(
    "TEST_POSTGRES_DATABASE_URL"
)
OWNER_DATABASE_URL = POSTGRES_ENV.get(
    "TEST_POSTGRES_MIGRATION_DATABASE_URL"
)

MINIO_ENDPOINT_URL = MINIO_ENV.get(
    "TEST_MINIO_ENDPOINT_URL"
)
MINIO_ACCESS_KEY_ID = MINIO_ENV.get(
    "TEST_MINIO_ACCESS_KEY_ID"
)
MINIO_SECRET_ACCESS_KEY = MINIO_ENV.get(
    "TEST_MINIO_SECRET_ACCESS_KEY"
)
MINIO_BUCKET_NAME = MINIO_ENV.get(
    "TEST_MINIO_BUCKET_NAME"
)
MINIO_REGION = (
    MINIO_ENV.get("TEST_MINIO_REGION")
    or "us-east-1"
)

pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_TESTS_ENABLED
        and MINIO_TESTS_ENABLED
        and RUNTIME_DATABASE_URL
        and OWNER_DATABASE_URL
        and MINIO_ENDPOINT_URL
        and MINIO_ACCESS_KEY_ID
        and MINIO_SECRET_ACCESS_KEY
        and MINIO_BUCKET_NAME
    ),
    reason=(
        "P2.3G requires isolated PostgreSQL integration URLs plus "
        ".env.minio.integration with ENABLE_MINIO_TESTS=1 and local MinIO."
    ),
)


def _assert_local_minio_contract() -> None:
    parsed = urlsplit(str(MINIO_ENDPOINT_URL))

    if parsed.scheme != "http":
        raise RuntimeError(
            "P2.3G accepts only an HTTP loopback MinIO endpoint."
        )
    if (parsed.hostname or "").lower() not in _ALLOWED_MINIO_HOSTS:
        raise RuntimeError(
            "P2.3G refuses non-loopback object-storage endpoints."
        )
    if parsed.username or parsed.password:
        raise RuntimeError(
            "P2.3G MinIO endpoint must not embed credentials."
        )

    bucket = str(MINIO_BUCKET_NAME or "").strip().lower()
    if not bucket.startswith("litoral-trace-p23g"):
        raise RuntimeError(
            "P2.3G test bucket must start with 'litoral-trace-p23g'."
        )


def _engine(url: str, *, pool_size: int):
    return create_engine(
        normalize_database_url(url),
        pool_size=pool_size,
        max_overflow=0,
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _admin_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT_URL,
        region_name=MINIO_REGION,
        aws_access_key_id=MINIO_ACCESS_KEY_ID,
        aws_secret_access_key=MINIO_SECRET_ACCESS_KEY,
        use_ssl=False,
        verify=False,
        config=Config(
            connect_timeout=5,
            read_timeout=30,
            retries={
                "mode": "standard",
                "max_attempts": 2,
            },
            s3={
                "addressing_style": "path",
            },
        ),
    )


def _ensure_bucket(client) -> None:
    try:
        client.head_bucket(
            Bucket=MINIO_BUCKET_NAME,
        )
        return
    except ClientError as exc:
        code = str(
            exc.response.get("Error", {}).get("Code", "")
        )
        if code not in {
            "404",
            "NoSuchBucket",
            "NotFound",
        }:
            raise

    request = {
        "Bucket": MINIO_BUCKET_NAME,
    }
    if MINIO_REGION != "us-east-1":
        request["CreateBucketConfiguration"] = {
            "LocationConstraint": MINIO_REGION,
        }

    client.create_bucket(**request)


def _delete_prefix(client, prefix: str) -> None:
    continuation_token: str | None = None

    while True:
        request = {
            "Bucket": MINIO_BUCKET_NAME,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
        if continuation_token:
            request["ContinuationToken"] = continuation_token

        response = client.list_objects_v2(**request)
        objects = response.get("Contents") or []

        if objects:
            client.delete_objects(
                Bucket=MINIO_BUCKET_NAME,
                Delete={
                    "Objects": [
                        {"Key": item["Key"]}
                        for item in objects
                    ],
                    "Quiet": True,
                },
            )

        if not response.get("IsTruncated"):
            return

        continuation_token = response.get(
            "NextContinuationToken"
        )


def _object_count(client, prefix: str) -> int:
    response = client.list_objects_v2(
        Bucket=MINIO_BUCKET_NAME,
        Prefix=prefix,
    )
    return int(response.get("KeyCount", 0))


def _json_payload(reference: str) -> bytes:
    return json.dumps(
        {
            "reference_number": reference,
            "status": "COMPLIANT",
            "source": "P2.3G real MinIO acceptance",
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


@pytest.fixture()
def p23g_runtime():
    _assert_local_minio_contract()

    owner_engine = _engine(
        OWNER_DATABASE_URL,
        pool_size=3,
    )
    runtime_engine = _engine(
        RUNTIME_DATABASE_URL,
        pool_size=4,
    )

    RuntimeSession = sessionmaker(
        bind=runtime_engine,
        autoflush=False,
        expire_on_commit=True,
    )

    admin_client = _admin_client()
    _ensure_bucket(admin_client)

    suffix = uuid4().hex[:12]
    key_prefix = f"p23g/{suffix}"

    with owner_engine.begin() as connection:
        revision = connection.execute(
            text(
                "SELECT version_num "
                "FROM public.alembic_version"
            )
        ).scalar_one()

        if revision != EXPECTED_REVISION:
            raise RuntimeError(
                "P2.3G requires integration database revision 016."
            )

        org_a_id = int(
            connection.execute(
                text(
                    """
                    INSERT INTO public.organizations (
                        name,
                        slug,
                        tax_id,
                        tier,
                        description,
                        is_active
                    )
                    VALUES (
                        :name,
                        :slug,
                        :tax_id,
                        'pro',
                        'P2.3G real MinIO acceptance',
                        true
                    )
                    RETURNING id
                    """
                ),
                {
                    "name": f"P23G Org A {suffix}",
                    "slug": f"p23g-org-a-{suffix}",
                    "tax_id": f"P23G-A-{suffix}",
                },
            ).scalar_one()
        )

        org_b_id = int(
            connection.execute(
                text(
                    """
                    INSERT INTO public.organizations (
                        name,
                        slug,
                        tax_id,
                        tier,
                        description,
                        is_active
                    )
                    VALUES (
                        :name,
                        :slug,
                        :tax_id,
                        'pro',
                        'P2.3G real MinIO acceptance',
                        true
                    )
                    RETURNING id
                    """
                ),
                {
                    "name": f"P23G Org B {suffix}",
                    "slug": f"p23g-org-b-{suffix}",
                    "tax_id": f"P23G-B-{suffix}",
                },
            ).scalar_one()
        )

    storage_settings = StorageSettings(
        backend="s3",
        bucket_name=MINIO_BUCKET_NAME,
        region=MINIO_REGION,
        endpoint_url=MINIO_ENDPOINT_URL,
        access_key_id=MINIO_ACCESS_KEY_ID,
        secret_access_key=MINIO_SECRET_ACCESS_KEY,
        force_path_style=True,
        use_tls=False,
        verify_tls=False,
        connect_timeout_seconds=5,
        read_timeout_seconds=30,
        max_retries=2,
        key_prefix=key_prefix,
    )

    storage = Boto3S3ObjectStorage(
        storage_settings
    )

    service = VaultService(
        storage_settings=storage_settings,
        storage=storage,
        session_factory=RuntimeSession,
    )

    try:
        yield {
            "owner_engine": owner_engine,
            "runtime_engine": runtime_engine,
            "RuntimeSession": RuntimeSession,
            "admin_client": admin_client,
            "storage_settings": storage_settings,
            "storage": storage,
            "service": service,
            "org_a_id": org_a_id,
            "org_b_id": org_b_id,
            "key_prefix": key_prefix,
            "suffix": suffix,
        }
    finally:
        try:
            _delete_prefix(
                admin_client,
                key_prefix,
            )
        finally:
            with owner_engine.begin() as connection:
                connection.execute(
                    text(
                        """
                        DELETE FROM public.vault_documents
                        WHERE organization_id IN (
                            :org_a_id,
                            :org_b_id
                        )
                        """
                    ),
                    {
                        "org_a_id": org_a_id,
                        "org_b_id": org_b_id,
                    },
                )
                connection.execute(
                    text(
                        """
                        DELETE FROM public.organizations
                        WHERE id IN (
                            :org_a_id,
                            :org_b_id
                        )
                        """
                    ),
                    {
                        "org_a_id": org_a_id,
                        "org_b_id": org_b_id,
                    },
                )

            runtime_engine.dispose()
            owner_engine.dispose()


def test_p23g_real_minio_adapter_roundtrip_and_sanitized_not_found(
    p23g_runtime,
):
    storage = p23g_runtime["storage"]
    suffix = p23g_runtime["suffix"]

    assert storage.health_check() is True

    payload = _json_payload(
        f"P23G-ADAPTER-{suffix}"
    )
    sha256 = hashlib.sha256(
        payload
    ).hexdigest()
    key = (
        f"{p23g_runtime['key_prefix']}"
        f"/adapter/{uuid4().hex}.json"
    )

    write_result = storage.put_object(
        key=key,
        body=payload,
        content_type=JSON_CONTENT_TYPE,
        content_length=len(payload),
        metadata={
            "sha256": sha256,
            "purpose": "p23g-adapter",
        },
    )

    assert write_result.etag
    assert storage.object_exists(
        key=key
    ) is True

    head = storage.head_object(
        key=key
    )
    assert head.size_bytes == len(payload)
    assert head.content_type == JSON_CONTENT_TYPE
    assert head.metadata["sha256"] == sha256
    assert (
        head.metadata["purpose"]
        == "p23g-adapter"
    )

    with storage.get_object_stream(
        key=key
    ) as stream:
        received = b"".join(
            stream.iter_chunks(
                chunk_size=7,
            )
        )

    assert received == payload
    assert hashlib.sha256(
        received
    ).hexdigest() == sha256

    storage.delete_object(
        key=key
    )
    assert storage.object_exists(
        key=key
    ) is False

    missing_key = (
        f"{p23g_runtime['key_prefix']}"
        f"/missing/{uuid4().hex}"
    )

    with pytest.raises(
        ObjectStorageNotFoundError
    ) as exc_info:
        storage.head_object(
            key=missing_key
        )

    assert missing_key not in str(
        exc_info.value
    )
    assert MINIO_BUCKET_NAME not in str(
        exc_info.value
    )


def test_p23g_vaultservice_postgres_minio_upload_download_delete_roundtrip(
    p23g_runtime,
):
    service = p23g_runtime["service"]
    storage = p23g_runtime["storage"]
    org_id = p23g_runtime["org_a_id"]
    suffix = p23g_runtime["suffix"]

    payload = _json_payload(
        f"P23G-E2E-{suffix}"
    )

    created = service.upload_document(
        organization_id=org_id,
        created_by_user_id=None,
        filename="eudr-evidence.json",
        document_type="DDS_JSON_TRACES",
        content_type=JSON_CONTENT_TYPE,
        content=payload,
        idempotency_key=(
            f"p23g-e2e-{suffix}"
        ),
    )

    assert created.status == "available"

    with p23g_runtime[
        "owner_engine"
    ].connect() as connection:
        row = connection.execute(
            text(
                """
                SELECT
                    organization_id,
                    status,
                    object_key,
                    storage_backend,
                    storage_bucket,
                    storage_etag,
                    storage_version_id,
                    sha256
                FROM public.vault_documents
                WHERE public_id = :public_id
                """
            ),
            {
                "public_id": (
                    created.public_id
                )
            },
        ).mappings().one()

    object_key = row["object_key"]

    assert (
        row["organization_id"]
        == org_id
    )
    assert row["status"] == "available"
    assert row["storage_backend"] == "s3"
    assert (
        row["storage_bucket"]
        == MINIO_BUCKET_NAME
    )
    assert row["storage_etag"]
    assert (
        row["sha256"]
        == hashlib.sha256(
            payload
        ).hexdigest()
    )

    assert (
        "eudr-evidence.json"
        not in object_key
    )
    assert (
        str(created.public_id)
        not in object_key
    )
    assert object_key.startswith(
        (
            f"{p23g_runtime['key_prefix']}"
            f"/tenants/{org_id}/objects/"
        )
    )

    object_head = storage.head_object(
        key=object_key,
        version_id=row[
            "storage_version_id"
        ],
    )

    assert (
        object_head.size_bytes
        == len(payload)
    )
    assert (
        object_head.content_type
        == JSON_CONTENT_TYPE
    )
    assert (
        object_head.metadata["sha256"]
        == row["sha256"]
    )
    assert (
        object_head.metadata["public-id"]
        == str(created.public_id)
    )
    assert (
        object_head.metadata[
            "document-type"
        ]
        == "DDS_JSON_TRACES"
    )

    with service.materialize_verified_download(
        organization_id=org_id,
        document_id=created.public_id,
    ) as verified:
        downloaded = b"".join(
            verified.iter_chunks(
                chunk_size=11,
            )
        )

    assert downloaded == payload
    assert (
        hashlib.sha256(
            downloaded
        ).hexdigest()
        == created.sha256
    )

    deleted = service.delete_document(
        organization_id=org_id,
        document_id=created.public_id,
    )

    assert deleted.status == "deleted"
    assert deleted.deleted_at is not None
    assert storage.object_exists(
        key=object_key
    ) is False

    with p23g_runtime[
        "owner_engine"
    ].connect() as connection:
        final_row = connection.execute(
            text(
                """
                SELECT
                    status,
                    deleted_at
                FROM public.vault_documents
                WHERE public_id = :public_id
                """
            ),
            {
                "public_id": (
                    created.public_id
                )
            },
        ).mappings().one()

    assert (
        final_row["status"]
        == "deleted"
    )
    assert (
        final_row["deleted_at"]
        is not None
    )


def test_p23g_runtime_rls_hides_real_minio_document_from_other_tenant(
    p23g_runtime,
):
    service = p23g_runtime["service"]
    suffix = p23g_runtime["suffix"]

    payload = _json_payload(
        f"P23G-RLS-{suffix}"
    )

    created = service.upload_document(
        organization_id=(
            p23g_runtime["org_a_id"]
        ),
        created_by_user_id=None,
        filename="tenant-a.json",
        document_type="DDS_JSON_TRACES",
        content_type=JSON_CONTENT_TYPE,
        content=payload,
    )

    assert len(
        service.list_documents(
            organization_id=(
                p23g_runtime["org_a_id"]
            )
        )
    ) == 1

    assert (
        service.list_documents(
            organization_id=(
                p23g_runtime["org_b_id"]
            )
        )
        == []
    )

    with pytest.raises(
        VaultNotFoundError
    ):
        service.get_document(
            organization_id=(
                p23g_runtime["org_b_id"]
            ),
            document_id=created.public_id,
        )

    with pytest.raises(
        VaultNotFoundError
    ):
        service.materialize_verified_download(
            organization_id=(
                p23g_runtime["org_b_id"]
            ),
            document_id=created.public_id,
        )


def test_p23g_idempotency_reuses_one_postgres_row_and_one_real_object(
    p23g_runtime,
):
    service = p23g_runtime["service"]
    suffix = p23g_runtime["suffix"]
    org_id = p23g_runtime["org_a_id"]

    payload = _json_payload(
        f"P23G-IDEMPOTENCY-{suffix}"
    )

    kwargs = {
        "organization_id": org_id,
        "created_by_user_id": None,
        "filename": "idempotent.json",
        "document_type": "DDS_JSON_TRACES",
        "content_type": JSON_CONTENT_TYPE,
        "content": payload,
        "idempotency_key": (
            f"p23g-idempotency-{suffix}"
        ),
    }

    first = service.upload_document(
        **kwargs
    )
    second = service.upload_document(
        **kwargs
    )

    assert (
        first.public_id
        == second.public_id
    )

    with p23g_runtime[
        "owner_engine"
    ].connect() as connection:
        row_count = connection.execute(
            text(
                """
                SELECT count(*)
                FROM public.vault_documents
                WHERE organization_id = :organization_id
                  AND idempotency_key = :idempotency_key
                """
            ),
            {
                "organization_id": org_id,
                "idempotency_key": (
                    kwargs[
                        "idempotency_key"
                    ]
                ),
            },
        ).scalar_one()

    assert row_count == 1
    assert _object_count(
        p23g_runtime["admin_client"],
        p23g_runtime["key_prefix"],
    ) == 1