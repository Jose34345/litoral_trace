from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from litoral_trace.db.models import (
    UsLaceyOperation,
    UsLaceyOperationDocument,
    UsLaceyOperationField,
    UsLaceyOrganizationProfile,
)
from litoral_trace.db.tenant import apply_tenant_filter, verify_tenant_access
from litoral_trace.us_lacey.config import (
    UsLaceyConfigurationError,
    load_us_lacey_runtime_config,
)
from litoral_trace.us_lacey.domain import US_LACEY_REVIEW_FIELDS
from litoral_trace.us_lacey.storage import build_us_lacey_storage_settings
from litoral_trace.web.us_lacey_pilot_app import app
from sqlalchemy import select


def _safe_env() -> dict[str, str]:
    return {
        "US_LACEY_ENVIRONMENT": "pilot",
        "US_LACEY_DATABASE_URL": "postgresql://us_user:secret@us-db.example.com/us_lacey",
        "US_LACEY_STORAGE_BUCKET": "litoral-trace-us-lacey-private",
        "US_LACEY_STORAGE_PREFIX": "us-lacey/pilot",
        "US_LACEY_APP_HOSTNAME": "app.lacey.litoraltrace.com",
    }


def test_runtime_requires_explicit_us_database_and_storage():
    with pytest.raises(UsLaceyConfigurationError):
        load_us_lacey_runtime_config({"DATABASE_URL": "postgresql://generic/db"})


def test_runtime_rejects_same_database_as_generic_litoral_trace():
    env = _safe_env()
    env["DATABASE_URL"] = "postgresql://us_user:secret@us-db.example.com/us_lacey"
    with pytest.raises(UsLaceyConfigurationError, match="must not point"):
        load_us_lacey_runtime_config(env)


def test_runtime_rejects_same_storage_namespace_as_generic_litoral_trace():
    env = _safe_env()
    env["STORAGE_BUCKET_NAME"] = env["US_LACEY_STORAGE_BUCKET"]
    env["STORAGE_KEY_PREFIX"] = env["US_LACEY_STORAGE_PREFIX"]
    with pytest.raises(UsLaceyConfigurationError, match="distinct bucket"):
        load_us_lacey_runtime_config(env)


def test_runtime_allows_same_bucket_only_with_distinct_prefix():
    env = _safe_env()
    env["STORAGE_BUCKET_NAME"] = env["US_LACEY_STORAGE_BUCKET"]
    env["STORAGE_KEY_PREFIX"] = "argentina/production"
    cfg = load_us_lacey_runtime_config(env)
    assert cfg.storage_prefix == "us-lacey/pilot"


def test_us_storage_adapter_uses_only_us_namespace():
    settings = build_us_lacey_storage_settings(_safe_env())
    assert settings.bucket_name == "litoral-trace-us-lacey-private"
    assert settings.normalized_key_prefix == "us-lacey/pilot"
    assert "application/pdf" in settings.allowed_content_types
    assert "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in settings.allowed_content_types


def test_review_schema_has_stable_25_fields():
    assert len(US_LACEY_REVIEW_FIELDS) == 25
    keys = {key for key, _label in US_LACEY_REVIEW_FIELDS}
    assert {
        "importer_name",
        "bill_of_lading",
        "hts_code",
        "genus",
        "species",
        "country_of_harvest",
        "plant_quantity",
        "metric_unit",
        "supporting_reference",
    }.issubset(keys)


def test_us_profile_is_tenant_scoped_and_us_only():
    columns = UsLaceyOrganizationProfile.__table__.c
    assert columns.organization_id.nullable is False
    assert columns.legal_name.nullable is False
    assert {"country_code", "business_type", "billing_email", "account_status"}.issubset(columns.keys())


def test_us_operation_covers_customer_shipment_metadata():
    columns = UsLaceyOperation.__table__.c
    assert {
        "organization_id",
        "created_by_user_id",
        "client_reference",
        "importer_name",
        "consignee_name",
        "broker_name",
        "supplier_name",
        "operation_date",
        "status",
        "document_count",
        "merchandise_line_count",
        "review_result",
    }.issubset(columns.keys())


def test_operation_document_link_is_tenant_composite_and_versioned():
    columns = UsLaceyOperationDocument.__table__.c
    assert {"operation_id", "assurance_document_id", "document_role", "version_number", "is_current"}.issubset(columns.keys())
    constraints = {
        constraint.name: {column.name for column in constraint.columns}
        for constraint in UsLaceyOperationDocument.__table__.constraints
        if constraint.name and hasattr(constraint, "columns")
    }
    assert constraints["uq_us_lacey_operation_documents_version"] == {
        "organization_id",
        "operation_id",
        "assurance_document_id",
        "version_number",
    }


def test_operation_field_keeps_evidence_confidence_and_human_review():
    columns = UsLaceyOperationField.__table__.c
    assert {
        "field_name",
        "original_value",
        "normalized_value",
        "field_status",
        "confidence",
        "source_assurance_document_id",
        "source_page",
        "source_locator",
        "extractor",
        "extractor_version",
        "human_value",
        "reviewed_by_user_id",
        "reviewed_at",
    }.issubset(columns.keys())


def test_existing_tenant_helpers_scope_us_operations():
    statement = apply_tenant_filter(select(UsLaceyOperation), UsLaceyOperation, 7)
    rendered = str(statement)
    assert "us_lacey_operations.organization_id" in rendered
    entity = UsLaceyOperation(organization_id=7, client_reference="TEST-1")
    assert verify_tenant_access(entity, 7) is True
    assert verify_tenant_access(entity, 8) is False


def test_private_pilot_health_is_live_but_ready_fails_closed_without_us_config(monkeypatch):
    for name in (
        "US_LACEY_ENVIRONMENT",
        "US_LACEY_DATABASE_URL",
        "US_LACEY_STORAGE_BUCKET",
        "US_LACEY_STORAGE_PREFIX",
        "US_LACEY_APP_HOSTNAME",
    ):
        monkeypatch.delenv(name, raising=False)
    client = TestClient(app)
    assert client.get("/health").status_code == 200
    response = client.get("/ready")
    assert response.status_code == 503
    assert "safely configured" in response.json()["detail"]


def test_private_pilot_ready_with_explicit_isolated_config(monkeypatch):
    for key, value in _safe_env().items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("STORAGE_BUCKET_NAME", raising=False)
    monkeypatch.delenv("STORAGE_KEY_PREFIX", raising=False)
    client = TestClient(app)
    response = client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "service": "us-lacey-pilot",
        "environment": "pilot",
        "hostname": "app.lacey.litoraltrace.com",
    }
