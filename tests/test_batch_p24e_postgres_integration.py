from __future__ import annotations

import asyncio
import io
import json
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException
from openpyxl import Workbook
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker
from starlette.datastructures import UploadFile
from starlette.requests import Request

import litoral_trace.api.batch as batch_api
from litoral_trace.api.auth import UserTenantContext
from litoral_trace.config.settings import (
    normalize_database_url,
)
from litoral_trace.db.models import BatchImport, Lote
from litoral_trace.db.tenant import (
    set_tenant_db_context,
)
from litoral_trace.services.batch import (
    BATCH_COLUMNAS,
    BATCH_SHEET_NAME,
    BATCH_XLSX_MEDIA_TYPE,
)
from litoral_trace.services.batch_imports import (
    BatchImportService,
)
from litoral_trace.services.batch_queries import (
    BatchImportQueryService,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
INTEGRATION_ENV_PATH = (
    ROOT_DIR / ".env.integration"
)
EXPECTED_REVISION = (
    "018_add_batch_evidence_links"
)


def _truthy(
    value: str | None,
) -> bool:
    return (
        value
        or ""
    ).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _read_env_file(
    path: Path,
) -> dict[str, str]:
    values: dict[str, str] = {}

    if not path.exists():
        return values

    for raw_line in path.read_text(
        encoding="utf-8-sig"
    ).splitlines():
        line = raw_line.strip()

        if (
            not line
            or line.startswith("#")
            or "=" not in line
        ):
            continue

        name, value = line.split(
            "=",
            1,
        )
        values[
            name.strip()
        ] = value.strip()

    return values


INTEGRATION_ENV = _read_env_file(
    INTEGRATION_ENV_PATH
)
POSTGRES_TESTS_ENABLED = _truthy(
    INTEGRATION_ENV.get(
        "ENABLE_POSTGRES_TESTS"
    )
)
RUNTIME_DATABASE_URL = (
    INTEGRATION_ENV.get(
        "TEST_POSTGRES_DATABASE_URL"
    )
)
OWNER_DATABASE_URL = (
    INTEGRATION_ENV.get(
        "TEST_POSTGRES_MIGRATION_DATABASE_URL"
    )
)

pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_TESTS_ENABLED
        and RUNTIME_DATABASE_URL
        and OWNER_DATABASE_URL
    ),
    reason=(
        "P2.4E PostgreSQL tests require "
        "ENABLE_POSTGRES_TESTS=1 plus "
        "isolated runtime and migration-owner "
        "integration URLs."
    ),
)


def _engine(
    url: str,
    *,
    pool_size: int,
):
    return create_engine(
        normalize_database_url(url),
        pool_size=pool_size,
        max_overflow=0,
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _user(
    organization_id: int,
) -> UserTenantContext:
    return UserTenantContext(
        user_id=None,
        username=(
            f"p24e-{organization_id}"
        ),
        organization_id=organization_id,
        organization_name=(
            f"P24E Org {organization_id}"
        ),
        organization_slug=(
            f"p24e-{organization_id}"
        ),
        role="admin",
        email=(
            f"p24e-{organization_id}"
            "@example.com"
        ),
    )


def _request(
    path: str,
    method: str = "POST",
) -> Request:
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": method,
            "scheme": "https",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": [
                (
                    b"x-request-id",
                    b"p24e-pg-request",
                ),
                (
                    b"user-agent",
                    b"p24e-pg/1.0",
                ),
            ],
            "client": (
                "203.0.113.40",
                50000,
            ),
            "server": (
                "test",
                443,
            ),
            "root_path": "",
        }
    )


def _xlsx_bytes(
    identificador: str,
) -> bytes:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = BATCH_SHEET_NAME
    worksheet.append(
        BATCH_COLUMNAS
    )
    worksheet.append(
        [
            identificador,
            "30-12345678-9",
            "Madera Aserrada (Pino)",
            50.0,
            -27.45,
            -58.90,
            100.0,
            45.0,
        ]
    )

    output = io.BytesIO()
    workbook.save(
        output
    )
    workbook.close()
    return output.getvalue()


def _upload(
    payload: bytes,
) -> UploadFile:
    return UploadFile(
        filename="P24E_Import.xlsx",
        file=io.BytesIO(payload),
        headers={
            "content-type": (
                BATCH_XLSX_MEDIA_TYPE
            )
        },
    )


def _body(
    response,
):
    return json.loads(
        response.body.decode()
    )


@pytest.fixture()
def pg_batch_api(
    monkeypatch,
):
    owner_engine = _engine(
        OWNER_DATABASE_URL,
        pool_size=3,
    )
    runtime_engine = _engine(
        RUNTIME_DATABASE_URL,
        pool_size=5,
    )
    RuntimeSession = sessionmaker(
        bind=runtime_engine,
        autoflush=False,
        expire_on_commit=False,
    )

    suffix = uuid4().hex[:10]

    with owner_engine.begin() as connection:
        revision = connection.execute(
            text(
                "SELECT version_num "
                "FROM alembic_version"
            )
        ).scalar_one()

        if revision != EXPECTED_REVISION:
            raise RuntimeError(
                "P2.4E requires integration "
                "database at "
                f"{EXPECTED_REVISION}; "
                f"found {revision!r}."
            )

        org_ids: list[int] = []

        for label in (
            "A",
            "B",
        ):
            org_ids.append(
                int(
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
                                'P2.4E batch API integration',
                                true
                            )
                            RETURNING id
                            """
                        ),
                        {
                            "name": (
                                f"P24E Org "
                                f"{label} {suffix}"
                            ),
                            "slug": (
                                f"p24e-org-"
                                f"{label.lower()}-"
                                f"{suffix}"
                            ),
                            "tax_id": (
                                f"P24E-{label}-"
                                f"{suffix}"
                            ),
                        },
                    ).scalar_one()
                )
            )

    import_service = BatchImportService(
        session_factory=RuntimeSession
    )
    query_service = (
        BatchImportQueryService(
            session_factory=RuntimeSession
        )
    )

    monkeypatch.setattr(
        batch_api,
        "_new_batch_import_service",
        lambda: import_service,
    )
    monkeypatch.setattr(
        batch_api,
        "_new_batch_import_query_service",
        lambda: query_service,
    )

    try:
        yield {
            "owner_engine": owner_engine,
            "runtime_engine": runtime_engine,
            "RuntimeSession": RuntimeSession,
            "org_a_id": org_ids[0],
            "org_b_id": org_ids[1],
        }

    finally:
        with owner_engine.begin() as connection:
            connection.execute(
                text(
                    "DELETE FROM public.audit_logs "
                    "WHERE organization_id "
                    "IN (:a, :b)"
                ),
                {
                    "a": org_ids[0],
                    "b": org_ids[1],
                },
            )
            connection.execute(
                text(
                    "DELETE FROM public.batch_imports "
                    "WHERE organization_id "
                    "IN (:a, :b)"
                ),
                {
                    "a": org_ids[0],
                    "b": org_ids[1],
                },
            )
            connection.execute(
                text(
                    "DELETE FROM public.lotes "
                    "WHERE organization_id "
                    "IN (:a, :b)"
                ),
                {
                    "a": org_ids[0],
                    "b": org_ids[1],
                },
            )
            connection.execute(
                text(
                    "DELETE FROM public.organizations "
                    "WHERE id IN (:a, :b)"
                ),
                {
                    "a": org_ids[0],
                    "b": org_ids[1],
                },
            )

        runtime_engine.dispose()
        owner_engine.dispose()


def _tenant_count(
    RuntimeSession,
    model,
    organization_id: int,
) -> int:
    session = RuntimeSession()

    try:
        set_tenant_db_context(
            session,
            organization_id,
        )

        return int(
            session.execute(
                select(
                    func.count(model.id)
                ).where(
                    model.organization_id
                    == organization_id
                )
            ).scalar_one()
        )

    finally:
        session.rollback()
        session.close()


def test_api_initial_import_201_then_replay_200(
    pg_batch_api,
):
    org_id = pg_batch_api[
        "org_a_id"
    ]
    payload = _xlsx_bytes(
        "P24E-REPLAY-001"
    )

    first = asyncio.run(
        batch_api.importar_batch_excel_endpoint(
            file=_upload(payload),
            request=_request(
                "/api/v1/batch/import"
            ),
            idempotency_key=(
                "p24e-api-replay"
            ),
            user=_user(org_id),
        )
    )
    second = asyncio.run(
        batch_api.importar_batch_excel_endpoint(
            file=_upload(payload),
            request=_request(
                "/api/v1/batch/import"
            ),
            idempotency_key=(
                "p24e-api-replay"
            ),
            user=_user(org_id),
        )
    )

    first_body = _body(
        first
    )
    second_body = _body(
        second
    )

    assert first.status_code == 201
    assert second.status_code == 200
    assert first_body["replayed"] is False
    assert second_body["replayed"] is True
    assert (
        first_body["import_id"]
        == second_body["import_id"]
    )
    assert (
        first_body["lote_ids"]
        == second_body["lote_ids"]
    )

    RuntimeSession = pg_batch_api[
        "RuntimeSession"
    ]
    assert _tenant_count(
        RuntimeSession,
        Lote,
        org_id,
    ) == 1
    assert _tenant_count(
        RuntimeSession,
        BatchImport,
        org_id,
    ) == 1


def test_api_same_key_different_file_maps_409(
    pg_batch_api,
):
    org_id = pg_batch_api[
        "org_a_id"
    ]

    asyncio.run(
        batch_api.importar_batch_excel_endpoint(
            file=_upload(
                _xlsx_bytes(
                    "P24E-SHA-A"
                )
            ),
            request=_request(
                "/api/v1/batch/import"
            ),
            idempotency_key=(
                "p24e-api-sha"
            ),
            user=_user(org_id),
        )
    )

    with pytest.raises(
        HTTPException
    ) as exc_info:
        asyncio.run(
            batch_api.importar_batch_excel_endpoint(
                file=_upload(
                    _xlsx_bytes(
                        "P24E-SHA-B"
                    )
                ),
                request=_request(
                    "/api/v1/batch/import"
                ),
                idempotency_key=(
                    "p24e-api-sha"
                ),
                user=_user(org_id),
            )
        )

    assert exc_info.value.status_code == 409
    assert (
        exc_info.value.detail["code"]
        == "IDEMPOTENCY_CONFLICT"
    )


def test_api_status_is_tenant_scoped_404_cross_tenant(
    pg_batch_api,
):
    org_a = pg_batch_api[
        "org_a_id"
    ]
    org_b = pg_batch_api[
        "org_b_id"
    ]

    imported = asyncio.run(
        batch_api.importar_batch_excel_endpoint(
            file=_upload(
                _xlsx_bytes(
                    "P24E-STATUS-001"
                )
            ),
            request=_request(
                "/api/v1/batch/import"
            ),
            idempotency_key=(
                "p24e-api-status"
            ),
            user=_user(org_a),
        )
    )
    import_id = UUID(
        _body(imported)[
            "import_id"
        ]
    )

    own = asyncio.run(
        batch_api.obtener_batch_import_endpoint(
            public_id=import_id,
            user=_user(org_a),
        )
    )
    assert own.status_code == 200
    assert (
        _body(own)[
            "organization_id"
        ]
        == org_a
    )

    with pytest.raises(
        HTTPException
    ) as exc_info:
        asyncio.run(
            batch_api.obtener_batch_import_endpoint(
                public_id=import_id,
                user=_user(org_b),
            )
        )

    assert exc_info.value.status_code == 404


def test_api_validate_does_not_create_db_records(
    pg_batch_api,
):
    org_id = pg_batch_api[
        "org_a_id"
    ]

    response = asyncio.run(
        batch_api.validar_batch_excel_endpoint(
            file=_upload(
                _xlsx_bytes(
                    "P24E-VALIDATE-ONLY"
                )
            ),
            request=_request(
                "/api/v1/batch/validate"
            ),
            user=_user(org_id),
        )
    )

    assert response.status_code == 200
    assert _body(response)["valid"] is True

    RuntimeSession = pg_batch_api[
        "RuntimeSession"
    ]
    assert _tenant_count(
        RuntimeSession,
        Lote,
        org_id,
    ) == 0
    assert _tenant_count(
        RuntimeSession,
        BatchImport,
        org_id,
    ) == 0
