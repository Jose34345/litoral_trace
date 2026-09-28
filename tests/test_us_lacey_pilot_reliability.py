from __future__ import annotations

import json
from uuid import UUID, uuid4

from litoral_trace.db.models.us_lacey_pilot_reliability import (
    PilotIncidentSeverity,
    PilotQualityTrigger,
)
from litoral_trace.us_lacey.pilot_reliability import (
    PilotQualityAnomaly,
    PilotQualityGuard,
    PilotQualitySnapshotData,
    build_diagnostic_manifest,
    estimate_commercial_line_count,
    incident_fingerprint,
)


def _evidence(
    *,
    document_id: str,
    line_key: str,
    value: str,
    source_text: str,
) -> dict:
    return {
        "document_id": document_id,
        "line_key": line_key,
        "normalized_value": value,
        "candidate": {
            "provenance": {
                "source_text": source_text,
                "source_block": {
                    "table_id": "t1",
                    "row_index": int(line_key.rsplit(":", 1)[-1]),
                },
            }
        },
    }


def _structural_payload_with_total_and_sensitive_text() -> dict:
    documents = [
        {
            "document_id": "doc-invoice",
            "resolution": {
                "document_type": "COMMERCIAL_INVOICE",
                "type_confidence": 0.99,
                "filename": "Gulf Wood secret invoice.pdf",
            },
        },
        {
            "document_id": "doc-entry",
            "resolution": {
                "document_type": "CUSTOMS_ENTRY_SUMMARY",
                "type_confidence": 0.99,
                "filename": "customer-entry.pdf",
            },
        },
        {
            "document_id": "doc-packing",
            "resolution": {
                "document_type": "PACKING_LIST",
                "type_confidence": 0.99,
                "filename": "packing.pdf",
            },
        },
    ]
    entered = [
        _evidence(
            document_id="doc-invoice",
            line_key=f"invoice:p1-t1:row:{row}",
            value=value,
            source_text=text,
        )
        for row, value, text in (
            (1, "100.00", "Customer Gulf Wood / Acacia mangium / $100"),
            (2, "200.00", "Customer Gulf Wood / Hevea brasiliensis / $200"),
            (3, "300.00", "Customer Gulf Wood / Tectona grandis / $300"),
            (4, "600.00", "GRAND TOTAL $600.00 - Gulf Wood"),
        )
    ]
    hts = [
        _evidence(
            document_id="doc-invoice",
            line_key=f"invoice:p1-t1:row:{row}",
            value=value,
            source_text=f"HTS {value}",
        )
        for row, value in (
            (1, "4419909000"),
            (2, "4419908000"),
            (3, "4421999880"),
        )
    ]
    packing = [
        _evidence(
            document_id="doc-packing",
            line_key=f"packing:p1-t1:row:{row}",
            value=value,
            source_text=f"Package row {row}",
        )
        for row, value in ((1, "10"), (2, "20"), (3, "30"))
    ]
    entry = [
        _evidence(
            document_id="doc-entry",
            line_key=f"entry:p1-t1:row:{row}",
            value=value,
            source_text=f"Entry row {row}",
        )
        for row, value in ((1, "100.00"), (2, "200.00"), (3, "300.00"))
    ]
    return {
        "documents": documents,
        "canonical_fields": {
            "entered_value": {
                "supporting_evidence": entered + entry,
            },
            "hts_code": {
                "supporting_evidence": hts,
            },
            "plant_quantity": {
                "supporting_evidence": packing,
            },
        },
    }


def _snapshot(**overrides) -> PilotQualitySnapshotData:
    values = {
        "organization_id": 101,
        "operation_id": 202,
        "operation_public_id": UUID("11111111-2222-3333-4444-555555555555"),
        "attribution_session_id": UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
        "trigger": PilotQualityTrigger.INITIAL_PROCESS,
        "source_set_fingerprint": "f" * 64,
        "engine_version": "lacey-engine-2.6.2",
        "canonical_publisher_version": "lacey_canonical_shipment_truth_v6",
        "document_count": 3,
        "valid_document_count": 3,
        "logical_document_count": 3,
        "document_type_counts": {
            "COMMERCIAL_INVOICE": 1,
            "CUSTOMS_ENTRY_SUMMARY": 1,
            "PACKING_LIST": 1,
        },
        "commercial_line_count": 3,
        "canonical_line_count": 10,
        "auto_resolved_count": 0,
        "action_required_count": 72,
        "confirmed_count": 0,
        "conflict_count": 0,
        "total_field_count": 90,
        "processing_duration_ms": 12_345,
        "export_ready": False,
    }
    values.update(overrides)
    return PilotQualitySnapshotData(**values)


def test_commercial_line_estimator_uses_structure_and_suppresses_totals():
    payload = _structural_payload_with_total_and_sensitive_text()

    assert estimate_commercial_line_count(payload) == 3


def test_quality_guard_detects_fragmentation_action_spike_and_zero_automation():
    anomalies = PilotQualityGuard.evaluate(_snapshot())

    assert [(item.detector_code, item.severity.value) for item in anomalies] == [
        ("LINE_FRAGMENTATION_SPIKE", "P0"),
        ("ACTION_REQUIRED_SPIKE", "P1"),
        ("ZERO_AUTOMATION", "P1"),
    ]


def test_quality_guard_does_not_raise_spikes_for_healthy_packet():
    anomalies = PilotQualityGuard.evaluate(
        _snapshot(
            canonical_line_count=3,
            auto_resolved_count=26,
            action_required_count=4,
            confirmed_count=2,
            total_field_count=32,
        )
    )

    assert anomalies == ()


def test_incident_fingerprint_is_deterministic_and_detector_specific():
    snapshot = _snapshot()

    first = incident_fingerprint(
        operation_public_id=snapshot.operation_public_id,
        detector_code="LINE_FRAGMENTATION_SPIKE",
        engine_version=snapshot.engine_version,
    )
    second = incident_fingerprint(
        operation_public_id=snapshot.operation_public_id,
        detector_code="LINE_FRAGMENTATION_SPIKE",
        engine_version=snapshot.engine_version,
    )
    other = incident_fingerprint(
        operation_public_id=snapshot.operation_public_id,
        detector_code="ZERO_AUTOMATION",
        engine_version=snapshot.engine_version,
    )

    assert first == second
    assert first != other
    assert len(first) == 64


def test_diagnostic_manifest_is_strictly_allowlisted_and_contains_no_customer_data():
    snapshot = _snapshot()
    anomaly = PilotQualityAnomaly(
        detector_code="LINE_FRAGMENTATION_SPIKE",
        severity=PilotIncidentSeverity.P0,
        reason="internal reason",
    )

    manifest = build_diagnostic_manifest(snapshot, anomaly)
    serialized = json.dumps(manifest, sort_keys=True)

    assert set(manifest) == {
        "schema_version",
        "detector",
        "operation_public_id",
        "trigger",
        "versions",
        "documents",
        "lines",
        "fields",
        "processing",
    }
    assert manifest["documents"]["by_type"] == snapshot.document_type_counts
    assert "attribution_session_id" not in serialized
    assert "source_set_fingerprint" not in serialized

    for forbidden in (
        "Gulf Wood",
        "Acacia",
        "mangium",
        "Hevea",
        "brasiliensis",
        "Tectona",
        "grandis",
        "$600",
        "invoice",
        "supplier",
        "importer",
        "consignee",
        "price",
        "species",
        "taxon",
        "source_text",
    ):
        assert forbidden.casefold() not in serialized.casefold()
