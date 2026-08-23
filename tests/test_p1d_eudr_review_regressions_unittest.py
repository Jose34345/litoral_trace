from __future__ import annotations

from pathlib import Path

from litoral_trace.services.eudr_release_control import (
    apply_eudr_conformance_release_control,
    is_eudr_destination,
)


ROOT = Path(__file__).resolve().parents[1]
SERVICE_PATH = ROOT / "src" / "litoral_trace" / "services" / "eudr_dds_candidate.py"
WEB_RELEASE_PATH = ROOT / "src" / "litoral_trace" / "web" / "traceability_release_control.py"


def _payload(destination: str) -> dict[str, object]:
    return {
        "shipment": {
            "shipment_code": "EXP-REVIEW-01",
            "destination_country": destination,
        }
    }


def _control() -> dict[str, object]:
    return {
        "checks": [],
        "links": {},
        "stages": [{"label": "Salida", "state": "READY", "caption": "ok"}],
        "overall": {},
        "next_actions": [],
    }


def test_p1d_eudr_gate_applies_only_to_eu_destinations() -> None:
    assert is_eudr_destination(_payload("DE")) is True
    assert is_eudr_destination(_payload("fr")) is True
    assert is_eudr_destination(_payload("BR")) is False
    assert is_eudr_destination(_payload("US")) is False
    assert is_eudr_destination(_payload("AR")) is False

    original = _control()
    assert (
        apply_eudr_conformance_release_control(
            original,
            lineage_payload=_payload("US"),
            conformance=None,
        )
        is original
    )

    eu_result = apply_eudr_conformance_release_control(
        _control(),
        lineage_payload=_payload("DE"),
        conformance=None,
    )
    checks = list(eu_result["checks"])
    assert checks[-1]["key"] == "eudr_conformance"
    assert checks[-1]["state"] == "BLOCKED"


def test_p1d_candidate_requires_previous_dds_reference_and_verification() -> None:
    source = SERVICE_PATH.read_text(encoding="utf-8")
    assert '"PREVIOUS_DDS_REFERENCE"' in source
    assert '"PREVIOUS_DDS_VERIFICATION"' in source
    assert "bool(candidate.previous_dds_reference) if candidate.relies_on_previous_dds else True" in source
    assert "bool(candidate.previous_dds_verification) if candidate.relies_on_previous_dds else True" in source


def test_p1d_shipment_lookup_remains_case_insensitive() -> None:
    source = SERVICE_PATH.read_text(encoding="utf-8")
    assert "func.lower(Shipment.shipment_code) == normalized.lower()" in source


def test_release_control_only_queries_eudr_service_for_eu_destination() -> None:
    source = WEB_RELEASE_PATH.read_text(encoding="utf-8")
    assert "if is_eudr_destination(payload):" in source
