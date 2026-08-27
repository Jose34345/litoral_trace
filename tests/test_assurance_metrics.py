from litoral_trace.assurance.metrics import (
    AssurancePilotMetrics,
    review_time_reduction_percentage,
)


def test_automatic_data_percentage_and_target():
    metrics = AssurancePilotMetrics(
        fields_detected=100,
        fields_auto_accepted=76,
        fields_manually_reviewed=24,
    )

    assert metrics.automatic_data_percentage == 76.0
    assert metrics.human_touch_percentage == 24.0
    assert metrics.meets_zero_friction_target() is True


def test_empty_metrics_are_safe():
    metrics = AssurancePilotMetrics()

    assert metrics.automatic_data_percentage == 0.0
    assert metrics.human_touch_percentage == 0.0
    assert metrics.meets_zero_friction_target() is False


def test_review_time_reduction_uses_measured_baseline():
    assert review_time_reduction_percentage(
        baseline_seconds=1200,
        assurance_seconds=420,
    ) == 65.0


def test_metrics_payload_contains_derived_fields():
    metrics = AssurancePilotMetrics(fields_detected=20, fields_auto_accepted=14)
    payload = metrics.as_dict()

    assert payload["automatic_data_percentage"] == 70.0
    assert payload["human_touch_percentage"] == 0.0
