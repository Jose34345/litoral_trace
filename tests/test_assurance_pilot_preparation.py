from __future__ import annotations

from datetime import date
from decimal import Decimal
import json

import pytest

from litoral_trace.api.assurance_preflight import (
    AssurancePreflightRequest,
    build_preflight_input,
)
from litoral_trace.assurance.pilot_preparation import (
    PILOT_ACCEPTED_EXTENSIONS,
    AssurancePilotAccessError,
    AssurancePilotConfiguration,
    AssurancePilotConfigurationError,
    AssurancePilotPreparationError,
    AssurancePilotRuleNotFoundError,
    build_historical_replay_plan,
)
from litoral_trace.assurance.preflight import PreflightSignalState


def _enable_pilot(monkeypatch, tmp_path, *, organization_id: int = 42):
    config_path = tmp_path / "pilot.json"
    config_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "organization_id": organization_id,
                "rules": [
                    {
                        "customer_reference": "CLIENTE UNO SA",
                        "market": "BR",
                        "product": "Madera aserrada de pino",
                        "required_document_types": ["INVOICE", "DELIVERY_NOTE"],
                        "phytosanitary_required": True,
                        "eudr_required": True,
                    },
                    {
                        "customer_reference": "CLIENTE DOS SA",
                        "market": "UY",
                        "product": "Madera aserrada de pino",
                        "required_document_types": ["INVOICE"],
                        "phytosanitary_required": False,
                        "eudr_required": False,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("ENVIRONMENT", "staging")
    monkeypatch.setenv("LT_ASSURANCE_PILOT_MODE", "1")
    monkeypatch.setenv("LT_ASSURANCE_PILOT_ORGANIZATION_ID", str(organization_id))
    monkeypatch.setenv("LT_ASSURANCE_PILOT_CONFIG_PATH", str(config_path))
    return config_path


def test_pilot_mode_is_fail_closed_outside_staging(monkeypatch, tmp_path):
    _enable_pilot(monkeypatch, tmp_path)
    monkeypatch.setenv("ENVIRONMENT", "production")
    with pytest.raises(AssurancePilotConfigurationError, match="staging"):
        AssurancePilotConfiguration.from_environment()


def test_pilot_configuration_is_exact_tenant_scoped_and_not_fuzzy(monkeypatch, tmp_path):
    _enable_pilot(monkeypatch, tmp_path)
    config = AssurancePilotConfiguration.from_environment()
    assert config.enabled is True
    assert config.organization_id == 42
    assert len(config.rules) == 2
    assert config.find_rule(
        organization_id=42,
        customer_reference="  cliente uno sa ",
        market="br",
        product="MADERA ASERRADA DE PINO",
    ) is not None
    assert config.find_rule(
        organization_id=42,
        customer_reference="cliente uno",
        market="BR",
        product="Madera aserrada de pino",
    ) is None
    with pytest.raises(AssurancePilotAccessError):
        config.find_rule(
            organization_id=77,
            customer_reference="CLIENTE UNO SA",
            market="BR",
            product="Madera aserrada de pino",
        )


def test_preflight_merges_deployment_rules_and_request_cannot_downgrade_them(
    monkeypatch, tmp_path
):
    _enable_pilot(monkeypatch, tmp_path)
    payload = AssurancePreflightRequest(
        operation_reference="shipment:PILOT-001",
        customer_reference="CLIENTE UNO SA",
        market="BR",
        product="Madera aserrada de pino",
        quantity=Decimal("10"),
        commitment_date=date(2026, 9, 15),
        stock_available=Decimal("20"),
        required_document_types=["FOREST_GUIDE"],
        origin_state=PreflightSignalState.READY,
        genealogy_state=PreflightSignalState.READY,
        phytosanitary_state=PreflightSignalState.NOT_APPLICABLE,
        eudr_state=PreflightSignalState.NOT_APPLICABLE,
    )
    domain = build_preflight_input(payload, organization_id=42)
    assert domain.required_document_types == (
        "INVOICE",
        "DELIVERY_NOTE",
        "FOREST_GUIDE",
    )
    assert domain.phytosanitary_state == PreflightSignalState.UNASSESSED
    assert domain.eudr_state == PreflightSignalState.UNASSESSED


def test_optional_pilot_rules_remove_unnecessary_manual_assessment(monkeypatch, tmp_path):
    _enable_pilot(monkeypatch, tmp_path)
    payload = AssurancePreflightRequest(
        operation_reference="shipment:PILOT-002",
        customer_reference="CLIENTE DOS SA",
        market="UY",
        product="Madera aserrada de pino",
        quantity=Decimal("10"),
        commitment_date=date(2026, 9, 15),
        stock_available=Decimal("20"),
        origin_state=PreflightSignalState.READY,
        genealogy_state=PreflightSignalState.READY,
    )
    domain = build_preflight_input(payload, organization_id=42)
    assert domain.required_document_types == ("INVOICE",)
    assert domain.phytosanitary_state == PreflightSignalState.NOT_APPLICABLE
    assert domain.eudr_state == PreflightSignalState.NOT_APPLICABLE


def test_complete_unconfigured_combination_fails_closed(monkeypatch, tmp_path):
    _enable_pilot(monkeypatch, tmp_path)
    payload = AssurancePreflightRequest(
        operation_reference="shipment:PILOT-003",
        customer_reference="CLIENTE DESCONOCIDO",
        market="BR",
        product="Madera aserrada de pino",
        quantity=Decimal("10"),
        commitment_date=date(2026, 9, 15),
        stock_available=Decimal("20"),
    )
    with pytest.raises(AssurancePilotRuleNotFoundError):
        build_preflight_input(payload, organization_id=42)


def test_historical_replay_uses_existing_file_contract_without_erp_or_process_change():
    plan = build_historical_replay_plan(
        ["factura.pdf", "remito.xlsx", "stock.xls", "despachos.csv"]
    )
    assert set(plan.extensions) == set(PILOT_ACCEPTED_EXTENSIONS)
    assert plan.requires_erp_integration is False
    assert plan.requires_process_change is False
    assert plan.batches == (
        ("factura.pdf", "remito.xlsx", "stock.xls", "despachos.csv"),
    )


def test_historical_replay_chunks_large_sets_and_rejects_new_formats():
    plan = build_historical_replay_plan([f"doc-{index}.csv" for index in range(21)])
    assert len(plan.batches) == 2
    assert len(plan.batches[0]) == 20
    assert len(plan.batches[1]) == 1
    with pytest.raises(AssurancePilotPreparationError):
        build_historical_replay_plan(["operacion.docx"])
