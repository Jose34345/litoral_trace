from __future__ import annotations

import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_POSTGRES_TEST_DATABASE_URL"),
    reason="requires isolated U.S. PostgreSQL root credentials",
)


def _root_engine():
    return create_engine(
        os.environ["US_LACEY_POSTGRES_TEST_DATABASE_URL"],
        pool_pre_ping=True,
        hide_parameters=True,
    )


def test_privacy_trigger_strips_corrected_payload_without_opt_in():
    root = _root_engine()
    run_id = uuid4()
    field_instance_id = uuid4()
    try:
        with root.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO public.us_lacey_telemetry_runs (
                        run_id, origin, learning_opt_in, document_count,
                        document_type_counts, reviewed_field_count,
                        confirmed_field_count, corrected_field_count,
                        rejected_field_count, unreviewed_field_count,
                        human_correction_rate, telemetry_schema_version,
                        started_at, finalized_at
                    ) VALUES (
                        :run_id, 'SANDBOX', false, 1, '{}'::json,
                        1, 0, 1, 0, 0, 1.0, 1, now(), now()
                    )
                    """
                ),
                {"run_id": run_id},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO public.us_lacey_telemetry_field_actions (
                        telemetry_run_id, field_instance_id, field_name,
                        document_type, prediction_confidence, action_taken,
                        deidentified_fragment, target_value
                    ) VALUES (
                        :run_id, :field_instance_id, 'quantity',
                        'COMMERCIAL_INVOICE', 0.71, 'CORRECTED',
                        'NET WT 1,850 KG', '1580'
                    )
                    """
                ),
                {"run_id": run_id, "field_instance_id": field_instance_id},
            )
            stored = connection.execute(
                text(
                    """
                    SELECT action_taken, deidentified_fragment, target_value
                    FROM public.us_lacey_telemetry_field_actions
                    WHERE telemetry_run_id = :run_id
                      AND field_instance_id = :field_instance_id
                    """
                ),
                {"run_id": run_id, "field_instance_id": field_instance_id},
            ).mappings().one()
            assert stored["action_taken"] == "CORRECTED"
            assert stored["deidentified_fragment"] is None
            assert stored["target_value"] is None
            connection.execute(
                text("DELETE FROM public.us_lacey_telemetry_runs WHERE run_id = :run_id"),
                {"run_id": run_id},
            )
    finally:
        root.dispose()
