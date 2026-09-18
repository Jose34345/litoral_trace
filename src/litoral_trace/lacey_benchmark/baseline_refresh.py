"""Reproducible refresh helpers for Shadow-vs-Canonical benchmark fixtures.

Evaluation-only code. Production routing, extraction, publication and review paths
must never import this module.
"""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import json
from typing import Mapping

from litoral_trace.lacey_benchmark.shadow_canonical_diff import (
    BenchmarkFixture,
    CanonicalFieldSnapshot,
    CanonicalPlantLineSnapshot,
    Engine2DossierSnapshot,
    ExpectedDiff,
    ShipmentTruthSnapshot,
    evaluate_shadow_canonical_diff,
)
from litoral_trace.us_lacey.canonical_shipment_truth import (
    CanonicalFieldTruth,
    CanonicalShipmentTruth,
    CanonicalTruthState,
    build_canonical_shipment_truth,
)


def _benchmark_evidence_row(
    *,
    field_key: str,
    evidence,
    ordinal: int,
) -> dict:
    block_id = (
        f"benchmark:{evidence.source_filename}:p{evidence.page}:"
        f"{field_key}:{ordinal}"
    )
    return {
        "candidate_id": block_id,
        "document_id": evidence.source_filename,
        "field_key": field_key,
        "normalized_value": evidence.normalized_value,
        "candidate_score": 100.0,
        "source_authority": 20.0,
        "scope": evidence.scope,
        "line_key": evidence.line_key,
        "component_key": evidence.component_key,
        "quantity_semantic_type": (
            "PLANT_MATERIAL_QUANTITY"
            if field_key == "plant_quantity"
            else "OTHER"
        ),
        "candidate": {
            "score": 100.0,
            "raw": {
                "field_key": field_key,
                "normalized_value": evidence.normalized_value,
                "evidence_class": "EXPLICIT",
                "source_block": {
                    "block_id": block_id,
                    "table_id": None,
                    "row_index": None,
                    "key_text": field_key,
                    "value_text": evidence.normalized_value,
                    "text": evidence.normalized_value,
                    "page": evidence.page,
                },
            },
            "provenance": {
                "page": evidence.page,
                "block_id": block_id,
                "source_text": evidence.normalized_value,
                "evidence_class": "EXPLICIT",
            },
        },
    }


def shipment_resolution_payload_from_shadow(
    shadow: Engine2DossierSnapshot,
) -> dict:
    """Re-encode benchmark Shadow observations as Canonical's input contract.

    This adapter adds no entity-resolution semantics. It preserves field state,
    line_key, component_key and normalized values already present in the fixture.
    """
    canonical_fields: dict[str, dict] = {}
    for field in shadow.fields:
        rows = [
            _benchmark_evidence_row(
                field_key=field.field_key,
                evidence=evidence,
                ordinal=ordinal,
            )
            for ordinal, evidence in enumerate(field.evidence, start=1)
            if evidence.evidence_verified
        ]
        canonical_fields[field.field_key] = {
            "field_key": field.field_key,
            "state": field.state,
            "values": [
                {"value": value, "evidence_ids": []}
                for value in field.values
            ],
            "supporting_evidence": rows,
        }
    return {
        "schema_version": "lacey_shipment_resolution_v1",
        "engine_version": "p2-01-benchmark-refresh",
        "canonical_fields": canonical_fields,
        "issues": [],
    }


def _publication_status(field: CanonicalFieldTruth) -> str:
    if field.state in {
        CanonicalTruthState.SUPPORTED,
        CanonicalTruthState.SUPPORTED_MULTIPLE,
        CanonicalTruthState.NEAR_MATCH,
    }:
        return "FOUND"
    if field.state in {
        CanonicalTruthState.REVIEW_REQUIRED,
        CanonicalTruthState.CONFLICT,
    }:
        return "REVIEW"
    return "MISSING"


def _field_snapshot(field: CanonicalFieldTruth) -> CanonicalFieldSnapshot:
    return CanonicalFieldSnapshot(
        state=field.state.value,
        values=field.values,
        publication_status=_publication_status(field),
        issue_types=(),
    )


def shipment_truth_snapshot(
    truth: CanonicalShipmentTruth,
) -> ShipmentTruthSnapshot:
    return ShipmentTruthSnapshot(
        shipment_fields={
            key: _field_snapshot(field)
            for key, field in sorted(truth.shipment_fields.items())
        },
        plant_lines=tuple(
            CanonicalPlantLineSnapshot(
                entity_key=line.entity_key,
                fields={
                    key: _field_snapshot(field)
                    for key, field in sorted(line.fields.items())
                },
            )
            for line in truth.plant_lines
        ),
    )


def regenerate_canonical_from_shadow(
    shadow: Engine2DossierSnapshot,
) -> ShipmentTruthSnapshot:
    payload = shipment_resolution_payload_from_shadow(shadow)
    truth = build_canonical_shipment_truth(payload)
    return shipment_truth_snapshot(truth)


def refreshed_fixture_payload(payload: Mapping) -> dict:
    fixture = BenchmarkFixture(**dict(payload))
    canonical = regenerate_canonical_from_shadow(fixture.shadow)
    report = evaluate_shadow_canonical_diff(fixture.shadow, canonical)
    expected = ExpectedDiff(
        comparable_slots=report.comparable_slots,
        agreement_count=report.agreement_count,
        shadow_supported_but_canonical_missing=(
            report.shadow_supported_but_canonical_missing
        ),
        canonical_supported_but_shadow_missing=(
            report.canonical_supported_but_shadow_missing
        ),
        false_conflict_count=report.false_conflict_count,
    )
    refreshed = deepcopy(dict(payload))
    refreshed["canonical"] = canonical.model_dump(mode="json")
    refreshed["expected_diff"] = expected.model_dump(mode="json")
    return refreshed


def refresh_fixture(path: Path) -> dict:
    path = Path(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    refreshed = refreshed_fixture_payload(payload)
    path.write_text(
        json.dumps(refreshed, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return refreshed
