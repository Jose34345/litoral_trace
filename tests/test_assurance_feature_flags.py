import pytest

from litoral_trace.assurance.feature_flags import AssuranceFeatureFlags


_FLAG_NAMES = (
    "LT_ASSURANCE_V1_ENABLED",
    "LT_ASSURANCE_DOCUMENT_INTELLIGENCE_ENABLED",
    "LT_ASSURANCE_RECONCILIATION_ENABLED",
    "LT_ASSURANCE_PREFLIGHT_V2_ENABLED",
    "LT_ASSURANCE_OPERATIONAL_EXCEPTIONS_ENABLED",
    "LT_ASSURANCE_MARKET_READY_INVENTORY_ENABLED",
)


def _clear(monkeypatch):
    for name in _FLAG_NAMES:
        monkeypatch.delenv(name, raising=False)


def test_assurance_is_fully_disabled_by_default(monkeypatch):
    _clear(monkeypatch)
    flags = AssuranceFeatureFlags.from_environment()

    assert flags.assurance_v1 is False
    assert flags.document_intelligence is False
    assert flags.reconciliation is False
    assert flags.preflight_v2 is False
    assert flags.operational_exceptions is False
    assert flags.market_ready_inventory is False


def test_master_flag_enables_children_unless_overridden(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("LT_ASSURANCE_V1_ENABLED", "true")
    monkeypatch.setenv("LT_ASSURANCE_PREFLIGHT_V2_ENABLED", "false")
    monkeypatch.setenv("LT_ASSURANCE_OPERATIONAL_EXCEPTIONS_ENABLED", "false")

    flags = AssuranceFeatureFlags.from_environment()

    assert flags.assurance_v1 is True
    assert flags.document_intelligence is True
    assert flags.reconciliation is True
    assert flags.preflight_v2 is False
    assert flags.operational_exceptions is False
    assert flags.market_ready_inventory is True


def test_operational_exceptions_can_be_enabled_independently(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("LT_ASSURANCE_V1_ENABLED", "1")
    monkeypatch.setenv("LT_ASSURANCE_OPERATIONAL_EXCEPTIONS_ENABLED", "1")

    flags = AssuranceFeatureFlags.from_environment()
    assert flags.assurance_v1 is True
    assert flags.operational_exceptions is True


def test_invalid_boolean_fails_closed(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("LT_ASSURANCE_V1_ENABLED", "maybe")

    with pytest.raises(RuntimeError):
        AssuranceFeatureFlags.from_environment()


def test_unknown_flag_name_is_rejected():
    with pytest.raises(KeyError):
        AssuranceFeatureFlags().enabled("typo")
