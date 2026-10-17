from __future__ import annotations

import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import Session

from litoral_trace.db.models.us_lacey_pilot_reliability import (
    PilotIncidentSeverity,
    PilotQualityTrigger,
    UsLaceyPilotIncident,
    UsLaceyPilotQualitySnapshot,
)
from litoral_trace.us_lacey.pilot_reliability import (
    PilotQualityAnomaly,
    PilotQualitySnapshotData,
    _persist_incident,
)


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


def test_pilot_incidents_are_idempotent_and_hidden_from_runtime():
    engine = _root_engine()
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection, autoflush=False, expire_on_commit=False)
    suffix = uuid4().hex[:12]
    try:
        organization_id = int(
            connection.execute(
                text(
                    """
                    INSERT INTO public.organizations(
                        name, slug, tier, is_active, is_sandbox
                    ) VALUES (
                        :name, :slug, 'pro', true, false
                    )
                    RETURNING id
                    """
                ),
                {
                    "name": f"Pilot Reliability {suffix}",
                    "slug": f"pilot-reliability-{suffix}",
                },
            ).scalar_one()
        )
        operation_public_id = uuid4()
        operation_id = int(
            connection.execute(
                text(
                    """
                    INSERT INTO public.us_lacey_operations(
                        public_id,
                        organization_id,
                        client_reference,
                        status,
                        document_count,
                        merchandise_line_count
                    ) VALUES (
                        :public_id,
                        :organization_id,
                        :client_reference,
                        'REVIEW_REQUIRED',
                        3,
                        10
                    )
                    RETURNING id
                    """
                ),
                {
                    "public_id": operation_public_id,
                    "organization_id": organization_id,
                    "client_reference": f"PILOT-{suffix}",
                },
            ).scalar_one()
        )

        snapshot_row = UsLaceyPilotQualitySnapshot(
            organization_id=organization_id,
            operation_id=operation_id,
            trigger=PilotQualityTrigger.INITIAL_PROCESS.value,
            engine_version="lacey-engine-test",
            canonical_publisher_version="canonical-test",
            document_count=3,
            valid_document_count=3,
            logical_document_count=3,
            document_type_counts={"COMMERCIAL_INVOICE": 1, "PACKING_LIST": 2},
            commercial_line_count=3,
            canonical_line_count=10,
            auto_resolved_count=0,
            action_required_count=72,
            confirmed_count=0,
            conflict_count=0,
            total_field_count=90,
            processing_duration_ms=5000,
            export_ready=False,
        )
        session.add(snapshot_row)
        session.flush()

        snapshot = PilotQualitySnapshotData(
            organization_id=organization_id,
            operation_id=operation_id,
            operation_public_id=operation_public_id,
            attribution_session_id=None,
            trigger=PilotQualityTrigger.INITIAL_PROCESS,
            source_set_fingerprint="f" * 64,
            engine_version="lacey-engine-test",
            canonical_publisher_version="canonical-test",
            document_count=3,
            valid_document_count=3,
            logical_document_count=3,
            document_type_counts={"COMMERCIAL_INVOICE": 1, "PACKING_LIST": 2},
            commercial_line_count=3,
            canonical_line_count=10,
            auto_resolved_count=0,
            action_required_count=72,
            confirmed_count=0,
            conflict_count=0,
            total_field_count=90,
            processing_duration_ms=5000,
            export_ready=False,
        )
        anomaly = PilotQualityAnomaly(
            detector_code="LINE_FRAGMENTATION_SPIKE",
            severity=PilotIncidentSeverity.P0,
            reason="test",
        )

        first = _persist_incident(
            session,
            snapshot_row=snapshot_row,
            snapshot=snapshot,
            anomaly=anomaly,
        )
        second = _persist_incident(
            session,
            snapshot_row=snapshot_row,
            snapshot=snapshot,
            anomaly=anomaly,
        )

        assert first.id == second.id
        assert first.fingerprint == second.fingerprint
        assert (
            session.scalar(
                select(func.count(UsLaceyPilotIncident.id)).where(
                    UsLaceyPilotIncident.operation_id == operation_id,
                    UsLaceyPilotIncident.detector_code == "LINE_FRAGMENTATION_SPIKE",
                )
            )
            == 1
        )

        manifest_text = str(first.diagnostic_manifest).casefold()
        for forbidden in (
            "customer_name",
            "supplier_name",
            "importer_name",
            "consignee_name",
            "filename",
            "source_text",
            "normalized_value",
            "raw_text",
            "price_value",
            "taxon_value",
        ):
            assert forbidden not in manifest_text

        privileges = connection.execute(
            text(
                """
                SELECT
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.us_lacey_pilot_quality_snapshots',
                        'SELECT'
                    ) AS runtime_snapshot_select,
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.us_lacey_pilot_incidents',
                        'SELECT'
                    ) AS runtime_incident_select,
                    has_table_privilege(
                        'litoral_trace_worker_executor',
                        'public.us_lacey_pilot_quality_snapshots',
                        'INSERT'
                    ) AS worker_snapshot_insert,
                    has_table_privilege(
                        'litoral_trace_worker_executor',
                        'public.us_lacey_pilot_incidents',
                        'INSERT'
                    ) AS worker_incident_insert
                """
            )
        ).mappings().one()
        assert privileges == {
            "runtime_snapshot_select": False,
            "runtime_incident_select": False,
            "worker_snapshot_insert": True,
            "worker_incident_insert": True,
        }
    finally:
        session.close()
        transaction.rollback()
        connection.close()
        engine.dispose()
