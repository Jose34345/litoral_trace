from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from litoral_trace.db.models.us_lacey_telemetry import TelemetryFieldActionType
from litoral_trace.us_lacey.telemetry import CORPUS_FIELD_ALIASES, TelemetryService


def test_hcr_ignores_unreviewed_fields():
    rate = TelemetryService.calculate_human_correction_rate(
        [
            TelemetryFieldActionType.CONFIRMED,
            TelemetryFieldActionType.CORRECTED,
            TelemetryFieldActionType.UNREVIEWED,
            TelemetryFieldActionType.UNREVIEWED,
        ]
    )
    assert rate == Decimal("0.50000")


def test_hcr_is_none_when_nothing_was_reviewed():
    assert TelemetryService.calculate_human_correction_rate(
        [TelemetryFieldActionType.UNREVIEWED]
    ) is None


def test_run_and_field_ids_are_deterministic_for_idempotent_retry():
    first = TelemetryService.deterministic_run_id(origin="SANDBOX", source_id=42)
    second = TelemetryService.deterministic_run_id(origin="sandbox", source_id=42)
    different = TelemetryService.deterministic_run_id(origin="SANDBOX", source_id=43)
    assert first == second
    assert first != different
    assert TelemetryService.deterministic_field_instance_id(
        run_id=first, field_id=17
    ) == TelemetryService.deterministic_field_instance_id(
        run_id=second, field_id=17
    )


def test_action_classification_uses_review_state():
    reviewed_at = datetime.now(timezone.utc)
    assert TelemetryService.classify_field_action(
        {
            "reviewed_at": None,
            "field_status": "MATCHED",
            "normalized_value": "Eucalyptus",
            "human_value": "Eucalyptus",
        }
    ) is TelemetryFieldActionType.UNREVIEWED
    assert TelemetryService.classify_field_action(
        {
            "reviewed_at": reviewed_at,
            "field_status": "MATCHED",
            "normalized_value": "Eucalyptus",
            "human_value": "Eucalyptus",
        }
    ) is TelemetryFieldActionType.CONFIRMED
    assert TelemetryService.classify_field_action(
        {
            "reviewed_at": reviewed_at,
            "field_status": "MATCHED",
            "normalized_value": "1850",
            "human_value": "1580",
        }
    ) is TelemetryFieldActionType.CORRECTED


def test_corpus_aliases_match_ppq_contract_names():
    assert CORPUS_FIELD_ALIASES["plant_quantity"] == "quantity"
    assert CORPUS_FIELD_ALIASES["metric_unit"] == "unit"
