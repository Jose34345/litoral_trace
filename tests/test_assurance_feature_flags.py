from litoral_trace.feature_flags import FeatureFlags


def test_assurance_flags_are_disabled_by_default(monkeypatch):
    for name in (
        "LT_ASSURANCE_V1_ENABLED",
        "LT_DOCUMENT_INGESTION_V1_ENABLED",
        "LT_RECONCILIATION_V1_ENABLED",
        "LT_PREFLIGHT_V2_ENABLED",
        "LT_MARKET_READY_INVENTORY_V1_ENABLED",
    ):
        monkeypatch.delenv(name, raising=False)

    flags = FeatureFlags.from_env()

    assert flags.assurance_v1 is False
    assert flags.document_ingestion_v1 is False
    assert flags.automated_reconciliation_v1 is False
    assert flags.preflight_v2 is False
    assert flags.market_ready_inventory_v1 is False


def test_master_flag_enables_assurance_children(monkeypatch):
    monkeypatch.setenv("LT_ASSURANCE_V1_ENABLED", "true")
    monkeypatch.delenv("LT_DOCUMENT_INGESTION_V1_ENABLED", raising=False)
    monkeypatch.delenv("LT_RECONCILIATION_V1_ENABLED", raising=False)
    monkeypatch.delenv("LT_PREFLIGHT_V2_ENABLED", raising=False)
    monkeypatch.delenv("LT_MARKET_READY_INVENTORY_V1_ENABLED", raising=False)

    flags = FeatureFlags.from_env()

    assert flags.assurance_v1 is True
    assert flags.document_ingestion_v1 is True
    assert flags.automated_reconciliation_v1 is True
    assert flags.preflight_v2 is True
    assert flags.market_ready_inventory_v1 is True


def test_child_flag_can_be_disabled_independently(monkeypatch):
    monkeypatch.setenv("LT_ASSURANCE_V1_ENABLED", "1")
    monkeypatch.setenv("LT_PREFLIGHT_V2_ENABLED", "0")

    flags = FeatureFlags.from_env()

    assert flags.assurance_v1 is True
    assert flags.preflight_v2 is False


def test_unknown_flag_name_is_rejected():
    flags = FeatureFlags()

    try:
        flags.enabled("typo_flag")
    except KeyError as exc:
        assert "typo_flag" in str(exc)
    else:
        raise AssertionError("Unknown feature flags must raise KeyError")
