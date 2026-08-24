"""P1-A Integration Core + ERP bridge safety contracts."""
from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from litoral_trace.auth.rbac import Permission, permissions_for_role
from litoral_trace.services.integrations.canonical import GenericErpPayload
from litoral_trace.services.integrations.core import (
    IntegrationValidationError,
    _idempotency_hash,
    _payload_hash,
    _validate_config,
    _validate_secret_ref,
)


def test_generic_erp_contract_is_vendor_neutral_strict_and_normalizes_country() -> None:
    payload = GenericErpPayload.model_validate(
        {
            "source_system": "ERP-CLIENTE",
            "suppliers": [
                {"external_id": "SUP-1", "name": "Proveedor Uno", "country": "ar"}
            ],
            "products": [
                {
                    "external_id": "PROD-1",
                    "code": "PINO",
                    "name": "Pino aserrado",
                    "unit": "M3",
                }
            ],
            "receipts": [],
            "shipments": [],
        }
    )
    assert payload.suppliers[0].country == "AR"
    assert payload.entity_count() == 2
    with pytest.raises(ValidationError):
        GenericErpPayload.model_validate({"source_system": "ERP-X", "unknown": []})


def test_hashes_are_deterministic_and_idempotency_key_is_one_way() -> None:
    assert _payload_hash({"b": 2, "a": 1}) == _payload_hash({"a": 1, "b": 2})
    raw_key = "erp-sync-20260822-0001"
    digest = _idempotency_hash(raw_key)
    assert len(digest) == 64
    assert raw_key not in digest


def test_credentials_are_rejected_from_db_configuration_contract() -> None:
    with pytest.raises(IntegrationValidationError) as exc:
        _validate_config({"password": "never-store-this"})
    assert exc.value.code == "SENSITIVE_CONFIG_REJECTED"

    with pytest.raises(IntegrationValidationError) as exc:
        _validate_config({"auth": {"credentials": [{"api_key": "nested-secret"}]}})
    assert exc.value.code == "SENSITIVE_CONFIG_REJECTED"

    with pytest.raises(IntegrationValidationError) as exc:
        _validate_secret_ref("postgresql://user:password@example/db")
    assert exc.value.code == "INVALID_SECRET_REF"

    assert _validate_secret_ref("render:erp_cliente_1") == "render:erp_cliente_1"


def test_integration_rbac_fails_closed() -> None:
    assert Permission.INTEGRATION_MANAGE in permissions_for_role("admin")
    assert Permission.INTEGRATION_MANAGE in permissions_for_role("manager")
    assert Permission.INTEGRATION_READ in permissions_for_role("auditor")
    assert Permission.INTEGRATION_MANAGE not in permissions_for_role("auditor")
    assert Permission.INTEGRATION_READ not in permissions_for_role("cliente")
    assert not permissions_for_role("unknown-role")


def test_integration_service_cannot_mutate_chain_of_custody_ledger() -> None:
    source = Path("src/litoral_trace/services/integrations/core.py").read_text(encoding="utf-8")
    assert "TraceabilityLedgerService" not in source
    assert ".dispatch_shipment(" not in source
    assert ".post_event(" not in source
    assert "SOURCE_CHANGED_AFTER_RECONCILIATION" in source
    assert "ExternalEntityVersion" in source
    assert 'existing.status in {"RECONCILED", "CONFLICT"} or has_reference' in source
    assert "ConnectionWriteResult" in source
    assert "ReconciliationWriteResult" in source


def test_api_declares_staging_only_result() -> None:
    source = Path("src/litoral_trace/api/integrations.py").read_text(encoding="utf-8")
    assert '"ledger_mutated": False' in source
    assert 'alias="Idempotency-Key"' in source
    assert "actor_user_id=user.user_id" in source
    web_source = Path("src/litoral_trace/web/integrations.py").read_text(encoding="utf-8")
    assert web_source.count("actor_user_id=user.user_id") >= 2


def test_migrations_are_tenant_safe_and_immutable_history_is_append_only() -> None:
    core_migration = Path("alembic/versions/021_add_integration_core.py").read_text(encoding="utf-8")
    history_migration = Path("alembic/versions/022_add_integration_history.py").read_text(encoding="utf-8")

    assert 'down_revision: Union[str, Sequence[str], None] = "020_add_traceability_evidence_links"' in core_migration
    assert "FORCE ROW LEVEL SECURITY" in core_migration
    assert "litoral_trace_worker_executor" in core_migration
    assert "GRANT DELETE" not in core_migration

    assert 'down_revision: Union[str, Sequence[str], None] = "021_add_integration_core"' in history_migration
    assert 'TABLE = "external_entity_versions"' in history_migration
    assert "FORCE ROW LEVEL SECURITY" in history_migration
    assert "GRANT SELECT, INSERT ON TABLE" in history_migration
    assert "GRANT UPDATE" not in history_migration
    assert "actor_user_id" in history_migration
    assert "GRANT DELETE" not in history_migration


def test_integration_workspace_is_registered_with_navigation_and_traceability_parent_router() -> None:
    navigation = Path("src/litoral_trace/web/navigation.py").read_text(encoding="utf-8")
    parent_router = Path("src/litoral_trace/api/traceability.py").read_text(encoding="utf-8")
    template = Path("src/litoral_trace/templates/integrations.html").read_text(encoding="utf-8")

    assert 'key="integrations"' in navigation
    assert 'href="/integrations"' in navigation
    assert "integrations_api_router" in parent_router
    assert "integrations_web_router" in parent_router
    assert "Recepción controlada:" in template
    assert "ningún" in template.lower()