from __future__ import annotations

from io import BytesIO
import os
from uuid import uuid4

import pytest
from openpyxl import Workbook
from sqlalchemy import create_engine, text

import litoral_trace.us_lacey.ingestion as ingestion_module
import litoral_trace.us_lacey.worker as worker_module
from litoral_trace.storage import (
    ObjectDeleteResult,
    ObjectHead,
    ObjectStorageNotFoundError,
    ObjectStorageStream,
    ObjectWriteResult,
)
from litoral_trace.us_lacey.commercial import UsLaceyCommercialConfig
from litoral_trace.us_lacey.db import reset_us_lacey_engine_state
from litoral_trace.us_lacey.ingestion import UsLaceyIngestionService
from litoral_trace.us_lacey.operations import UsLaceyOperationService
from litoral_trace.us_lacey.self_service import register_us_lacey_company, verify_us_lacey_email
from litoral_trace.us_lacey.worker import process_one_us_lacey_job
from litoral_trace.us_lacey.worker_db import reset_us_lacey_worker_engine_state
from litoral_trace.us_lacey.workflow import (
    create_us_lacey_customer_operation,
    upload_and_enqueue_us_lacey_document,
)


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_DATABASE_URL")
    or not os.environ.get("US_LACEY_WORKER_DATABASE_URL"),
    reason="requires isolated U.S. PostgreSQL runtime and worker credentials",
)


class MemoryObjectStorage:
    """In-memory S3 boundary; database, queue and processing remain real."""

    def __init__(self) -> None:
        self.objects: dict[str, dict[str, object]] = {}
        self.put_calls = 0

    def put_object(self, *, key, body, content_type, content_length, metadata=None):
        payload = body if isinstance(body, bytes) else body.read()
        assert len(payload) == content_length
        self.put_calls += 1
        self.objects[key] = {
            "body": payload,
            "content_type": content_type,
            "metadata": dict(metadata or {}),
            "etag": f"etag-{self.put_calls}",
            "version_id": None,
        }
        return ObjectWriteResult(etag=f"etag-{self.put_calls}", version_id=None)

    def _head(self, key: str, version_id=None) -> ObjectHead:
        item = self.objects.get(key)
        if item is None:
            raise ObjectStorageNotFoundError("head_object")
        payload = item["body"]
        return ObjectHead(
            size_bytes=len(payload),
            content_type=str(item["content_type"]),
            etag=str(item["etag"]),
            version_id=version_id,
            metadata=dict(item["metadata"]),
        )

    def head_object(self, *, key, version_id=None):
        return self._head(key, version_id)

    def get_object_stream(self, *, key, version_id=None):
        item = self.objects.get(key)
        if item is None:
            raise ObjectStorageNotFoundError("get_object")
        return ObjectStorageStream(
            body=BytesIO(item["body"]),
            head=self._head(key, version_id),
        )

    def delete_object(self, *, key, version_id=None):
        self.objects.pop(key, None)
        return ObjectDeleteResult(delete_marker=False, version_id=version_id)

    def object_exists(self, *, key, version_id=None):
        return key in self.objects

    def health_check(self):
        return True


def _commercial_config() -> UsLaceyCommercialConfig:
    return UsLaceyCommercialConfig(
        price_cents=12500,
        monthly_operation_limit=25,
        payment_provider="WISE",
        bank_transfer_instructions="CI-only payment instructions",
        terms_version="terms-worker-v1",
        privacy_version="privacy-worker-v1",
        beta_terms_version="beta-worker-v1",
        support_email="support@litoraltrace.com",
    )


def _activate_account(organization_id: int) -> None:
    """Test-only activation through the normal tenant-scoped runtime RLS role."""
    runtime = create_engine(
        os.environ["US_LACEY_DATABASE_URL"],
        pool_pre_ping=True,
        hide_parameters=True,
    )
    with runtime.begin() as connection:
        connection.execute(
            text("SELECT set_config('app.current_organization_id', :organization_id, true)"),
            {"organization_id": str(organization_id)},
        )
        profile_id = connection.execute(
            text(
                "UPDATE public.us_lacey_organization_profiles "
                "SET account_status='ACTIVE', updated_at=now() "
                "WHERE organization_id=:organization_id RETURNING id"
            ),
            {"organization_id": organization_id},
        ).scalar_one_or_none()
        subscription_id = connection.execute(
            text(
                "UPDATE public.us_lacey_subscriptions "
                "SET status='ACTIVE', started_at=coalesce(started_at, now()), updated_at=now() "
                "WHERE organization_id=:organization_id RETURNING id"
            ),
            {"organization_id": organization_id},
        ).scalar_one_or_none()
        payment_id = connection.execute(
            text(
                "UPDATE public.us_lacey_payments "
                "SET status='VERIFIED', verified_at=now(), paid_at=now(), updated_at=now() "
                "WHERE organization_id=:organization_id RETURNING id"
            ),
            {"organization_id": organization_id},
        ).scalar_one_or_none()
        assert profile_id is not None
        assert subscription_id is not None
        assert payment_id is not None
    runtime.dispose()


def _register_active_customer():
    suffix = uuid4().hex[:12]
    email = f"worker-{suffix}@example.com"
    registered = register_us_lacey_company(
        legal_name=f"Worker Imports {suffix} LLC",
        business_type="IMPORTER",
        admin_name="Worker Admin",
        admin_email=email,
        password="correct-horse-worker-123",
        commercial_config=_commercial_config(),
    )
    verify_us_lacey_email(registered.verification_token)
    _activate_account(registered.organization_id)
    return registered, email, suffix


def _csv_bytes() -> bytes:
    return (
        "HTS Code,Merchandise Description,Species,Country of Harvest,Plant Quantity,Metric Unit,Container Number,Origin\n"
        "4407.11,Pine boards,Pinus taeda,Brazil,1000,KG,MSCU1234567,Canada\n"
    ).encode("utf-8")


def _xlsx_bytes() -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Shipment"
    sheet.append(
        [
            "HTS Code",
            "Merchandise Description",
            "Genus",
            "Species",
            "Country of Harvest",
            "Plant Quantity",
            "Metric Unit",
            "Bill of Lading",
        ]
    )
    sheet.append(
        [
            "4412.31",
            "Plywood panels",
            "Eucalyptus",
            "Eucalyptus grandis",
            "Uruguay",
            250,
            "M3",
            "BOL-TEST-9001",
        ]
    )
    buffer = BytesIO()
    workbook.save(buffer)
    workbook.close()
    return buffer.getvalue()


def _assert_processed_fields(*, organization_id: int, operation_public_id, expected: dict[str, str]):
    detail = UsLaceyOperationService().get_detail(
        organization_id=organization_id,
        operation_public_id=operation_public_id,
    )
    assert detail.status == "REVIEW_REQUIRED"
    assert detail.documents
    assert all(
        document.processing_status in {"EXTRACTED", "NEEDS_REVIEW"}
        for document in detail.documents
    )
    by_name = {field.field_name: field for field in detail.fields}
    for field_name, value in expected.items():
        field = by_name[field_name]
        assert field.proposed_value == value
        assert field.status == "REVIEW"
        assert field.confidence >= 0.89
        assert field.source_assurance_document_id is not None
        assert field.source_locator
    return by_name


def test_worker_processes_csv_and_xlsx_with_real_postgres_and_simulated_storage(monkeypatch):
    reset_us_lacey_engine_state()
    reset_us_lacey_worker_engine_state()
    storage = MemoryObjectStorage()

    # Both upload and worker use the same simulated private-object boundary.
    # Everything else -- tenant RLS, durable queue, extraction persistence,
    # worker claim and U.S. projection -- runs against the real PostgreSQL gate.
    monkeypatch.setattr(ingestion_module, "get_us_lacey_storage_client", lambda: storage)
    monkeypatch.setattr(worker_module, "get_us_lacey_storage_client", lambda: storage)

    registered, _email, suffix = _register_active_customer()
    ingestion = UsLaceyIngestionService()

    csv_operation = create_us_lacey_customer_operation(
        organization_id=registered.organization_id,
        user_id=registered.user_id,
        client_reference=f"CSV-{suffix}",
        line_references=("1",),
    )
    queued_csv = upload_and_enqueue_us_lacey_document(
        organization_id=registered.organization_id,
        user_id=registered.user_id,
        operation_public_id=csv_operation.public_id,
        filename="shipment.csv",
        content_type="text/csv",
        content=_csv_bytes(),
        document_role="SUPPLIER_DECLARATION",
        ingestion=ingestion,
    )
    assert queued_csv.job.status == "QUEUED"

    csv_result = process_one_us_lacey_job(worker_id="ci-worker-1")
    assert csv_result.claimed is True
    assert csv_result.job_id == queued_csv.job.id
    assert csv_result.job_status == "COMPLETED"
    assert csv_result.document_status in {"EXTRACTED", "NEEDS_REVIEW"}
    assert csv_result.operation_status == "REVIEW_REQUIRED"
    assert csv_result.projected_count >= 7
    csv_fields = _assert_processed_fields(
        organization_id=registered.organization_id,
        operation_public_id=csv_operation.public_id,
        expected={
            "hts_code": "440711",
            "merchandise_description": "Pine boards",
            "species": "Pinus taeda",
            "country_of_harvest": "Brazil",
            "plant_quantity": "1000",
            "metric_unit": "kg",
            "container_number": "MSCU1234567",
        },
    )
    assert csv_fields["country_of_harvest"].proposed_value != "Canada"
    assert csv_fields["genus"].proposed_value == "Pinus"
    assert csv_fields["genus"].status == "REVIEW"

    xlsx_operation = create_us_lacey_customer_operation(
        organization_id=registered.organization_id,
        user_id=registered.user_id,
        client_reference=f"XLSX-{suffix}",
        line_references=("1",),
    )
    queued_xlsx = upload_and_enqueue_us_lacey_document(
        organization_id=registered.organization_id,
        user_id=registered.user_id,
        operation_public_id=xlsx_operation.public_id,
        filename="shipment.xlsx",
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        content=_xlsx_bytes(),
        document_role="COMMERCIAL_INVOICE",
        ingestion=ingestion,
    )
    assert queued_xlsx.job.status == "QUEUED"

    xlsx_result = process_one_us_lacey_job(worker_id="ci-worker-1")
    assert xlsx_result.claimed is True
    assert xlsx_result.job_id == queued_xlsx.job.id
    assert xlsx_result.job_status == "COMPLETED"
    assert xlsx_result.document_status in {"EXTRACTED", "NEEDS_REVIEW"}
    assert xlsx_result.operation_status == "REVIEW_REQUIRED"
    assert xlsx_result.projected_count >= 8
    _assert_processed_fields(
        organization_id=registered.organization_id,
        operation_public_id=xlsx_operation.public_id,
        expected={
            "hts_code": "441231",
            "merchandise_description": "Plywood panels",
            "genus": "Eucalyptus",
            "species": "Eucalyptus grandis",
            "country_of_harvest": "Uruguay",
            "plant_quantity": "250",
            "metric_unit": "m3",
            "bill_of_lading": "BOL-TEST-9001",
        },
    )

    assert storage.put_calls == 2
    assert len(storage.objects) == 2

    reset_us_lacey_worker_engine_state()
    reset_us_lacey_engine_state()
