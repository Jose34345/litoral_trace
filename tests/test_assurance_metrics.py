from litoral_trace.services.assurance_metrics import PilotMetricsSnapshot


def test_automatic_data_percentage():
    snapshot = PilotMetricsSnapshot(
        fields_detected=100,
        fields_auto_accepted=76,
        fields_manually_reviewed=24,
    )

    assert snapshot.automatic_data_percentage == 76.0
    assert snapshot.meets_zero_friction_target() is True


def test_automatic_data_percentage_handles_empty_input():
    snapshot = PilotMetricsSnapshot()

    assert snapshot.automatic_data_percentage == 0.0
    assert snapshot.meets_zero_friction_target() is False


def test_metrics_payload_includes_derived_percentage():
    snapshot = PilotMetricsSnapshot(
        processing_seconds=4.2,
        fields_detected=20,
        fields_auto_accepted=14,
        reconciliation_issues=2,
        blocking_issues=1,
    )

    payload = snapshot.as_metrics()

    assert payload["processing_seconds"] == 4.2
    assert payload["automatic_data_percentage"] == 70.0
    assert payload["reconciliation_issues"] == 2
    assert payload["blocking_issues"] == 1
