"""UX10-E buyer-dossier documentary evidence acceptance."""
from __future__ import annotations

import io
import json
import zipfile

import pytest

from litoral_trace.services.traceability_documentary_dossier import (
    DOCUMENTARY_EXTENSION_NAME,
    DOCUMENTARY_EXTENSION_SCHEMA,
    build_documentary_dossier_bundle,
)
from litoral_trace.services.traceability_dossier import (
    OriginDossierValidationError,
)


PAYLOAD = {
    "organization_id": 77,
    "shipment": {
        "id": 910,
        "public_id": "8d4de42a-2283-48e4-996d-e47b19ae1001",
        "shipment_code": "EXP-UE-2026-001",
        "sale_reference": "FACTURA-E-001",
        "buyer_reference": "BUYER-EU-001",
        "destination_country": "DE",
        "shipped_at": "2026-08-20T20:00:00+00:00",
        "status": "DISPATCHED",
        "lineage_state": "FINAL",
    },
    "allocation_method": "PROPORTIONAL_INPUT_ALLOCATION",
    "complete": True,
    "issues": [],
    "unit_totals": [
        {
            "unit": "M3",
            "shipped_quantity": "60.000000",
            "attributed_quantity": "60.000000",
            "unresolved_quantity": "0.000000",
        }
    ],
    "items": [],
    "events": [],
    "source_lotes": [],
}


def _evidence(sha: str = "a" * 64):
    return (
        {
            "subject_type": "SHIPMENT",
            "subject_reference": "EXP-UE-2026-001",
            "evidence_type": "INVOICE",
            "reference_number": "FACTURA-E-001",
            "issuer": "Exportadora Corrientes SA",
            "document_date": "2026-08-20",
            "valid_from": None,
            "valid_until": None,
            "notes": "nota interna que no debe salir al comprador",
            "document": {
                "public_id": "18c275a9-3d9f-4dc6-bcc5-ec55844f2ad5",
                "filename": "factura-e-001.pdf",
                "content_type": "application/pdf",
                "size_bytes": 2048,
                "sha256": sha,
            },
        },
    )


def test_documentary_evidence_is_canonical_and_changes_manifest_integrity_hash():
    first = build_documentary_dossier_bundle(
        PAYLOAD,
        documentary_evidence=_evidence("a" * 64),
    )
    second = build_documentary_dossier_bundle(
        PAYLOAD,
        documentary_evidence=_evidence("b" * 64),
    )

    manifest = first.canonical_manifest
    extension = manifest["extensions"]["documentary_evidence"]
    evidence = manifest["documentary_evidence"]

    assert extension["schema_version"] == DOCUMENTARY_EXTENSION_SCHEMA
    assert extension["name"] == DOCUMENTARY_EXTENSION_NAME
    assert len(evidence) == 1
    assert evidence[0]["subject_type"] == "SHIPMENT"
    assert evidence[0]["subject_reference"] == "EXP-UE-2026-001"
    assert evidence[0]["document"]["sha256"] == "a" * 64
    assert "notes" not in evidence[0]
    assert first.manifest_sha256 != second.manifest_sha256


def test_documentary_summary_is_factual_not_a_compliance_score():
    bundle = build_documentary_dossier_bundle(
        PAYLOAD,
        documentary_evidence=_evidence(),
    )
    summary = bundle.canonical_manifest["documentary_evidence_summary"]

    assert summary["linked_evidence_count"] == 1
    assert summary["subjects_with_evidence"] == 1
    assert summary["unique_document_hashes"] == 1
    assert summary["by_subject_type"]["SHIPMENT"] == 1
    assert "certificación" in summary["statement"]
    assert "cumplimiento regulatorio" in summary["statement"]
    assert "score" not in summary


def test_private_evidence_binaries_are_not_embedded_in_buyer_zip():
    bundle = build_documentary_dossier_bundle(
        PAYLOAD,
        documentary_evidence=_evidence(),
    )

    assert bundle.pdf_bytes.startswith(b"%PDF")
    with zipfile.ZipFile(io.BytesIO(bundle.zip_bytes)) as archive:
        assert sorted(archive.namelist()) == [
            "README.txt",
            "dossier.pdf",
            "manifest.json",
            "origins.geojson",
        ]
        manifest = json.loads(archive.read("manifest.json"))["manifest"]
        assert (
            manifest["documentary_evidence"][0]["document"]["filename"]
            == "factura-e-001.pdf"
        )
        assert "factura-e-001.pdf" not in archive.namelist()
        assert b"Private evidence binaries are not embedded" in archive.read(
            "README.txt"
        )


def test_invalid_document_hash_fails_closed():
    with pytest.raises(OriginDossierValidationError) as exc_info:
        build_documentary_dossier_bundle(
            PAYLOAD,
            documentary_evidence=_evidence("not-a-hash"),
        )

    assert exc_info.value.code == "DOSSIER_EVIDENCE_INTEGRITY_INVALID"


def test_api_projects_graph_evidence_before_building_documentary_dossier():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    source = (
        root / "src/litoral_trace/api/traceability_dossier.py"
    ).read_text(encoding="utf-8")
    service_source = (
        root / "src/litoral_trace/services/traceability_documentary_dossier.py"
    ).read_text(encoding="utf-8")

    assert "project_documentary_evidence(" in source
    assert "build_documentary_dossier_bundle(" in source
    assert "documentary_evidence=documentary_evidence" in source
    assert "HUELLA DOCUMENTAL LITORAL TRACE" in service_source
    assert "Private evidence binaries are not embedded" in service_source
