"""P1E deterministic buyer-facing origin dossier acceptance."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import io
import json
import zipfile

from litoral_trace.services.traceability_dossier import (
    DISCLAIMER,
    SCHEMA_VERSION,
    build_canonical_manifest,
    build_origin_dossier_bundle,
    manifest_sha256,
    safe_artifact_stem,
)


def _payload(*, complete: bool = True) -> dict:
    issues = [] if complete else [
        {
            "code": "MISSING_PROVENANCE",
            "message": "Un lote no tiene origen resoluble.",
            "batch_id": 999,
        }
    ]
    unresolved = "0.000000" if complete else "5.000000"
    attributed = "60.000000" if complete else "55.000000"

    return {
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
        "complete": complete,
        "issues": issues,
        "unit_totals": [
            {
                "unit": "M3",
                "shipped_quantity": "60.000000",
                "attributed_quantity": attributed,
                "unresolved_quantity": unresolved,
            }
        ],
        "items": [
            {
                "shipment_item_id": 1,
                "batch": {
                    "id": 301,
                    "public_id": "8d4de42a-2283-48e4-996d-e47b19ae2001",
                    "code": "ASERRADO-001",
                    "product_name": "Madera aserrada de pino",
                    "stage": "FINISHED_GOOD",
                    "unit": "M3",
                    "status": "ACTIVE",
                    "source_lote_id": None,
                },
                "shipped_quantity": "60.000000",
                "unit": "M3",
                "attributed_quantity": attributed,
                "unresolved_quantity": unresolved,
                "complete": complete,
                "issues": issues,
                "source_contributions": [
                    {
                        "lote": {
                            "id": 10,
                            "identificador": "RODAL-PINO-A",
                            "productor_id": "PROVEEDOR-A",
                            "producto_forestal": "Pino resinoso",
                            "hectareas": 50.0,
                            "latitud": -28.05,
                            "longitud": -56.03,
                            "polygon_wkt": "POLYGON((-56.04 -28.06,-56.02 -28.06,-56.02 -28.04,-56.04 -28.04,-56.04 -28.06))",
                            "estatus": "Verde",
                        },
                        "attributed_shipment_quantity": "42.000000",
                        "unit": "M3",
                        "share_of_shipment_item": "0.700000",
                    },
                    {
                        "lote": {
                            "id": 11,
                            "identificador": "RODAL-PINO-B",
                            "productor_id": "PROVEEDOR-B",
                            "producto_forestal": "Pino resinoso",
                            "hectareas": 60.0,
                            "latitud": -28.06,
                            "longitud": -56.04,
                            "polygon_wkt": "POLYGON((-56.05 -28.07,-56.03 -28.07,-56.03 -28.05,-56.05 -28.05,-56.05 -28.07))",
                            "estatus": "Verde",
                        },
                        "attributed_shipment_quantity": "18.000000",
                        "unit": "M3",
                        "share_of_shipment_item": "0.300000",
                    },
                ],
            }
        ],
        "events": [
            {
                "id": 100,
                "public_id": "8d4de42a-2283-48e4-996d-e47b19ae3001",
                "event_code": "SAW-001",
                "event_type": "TRANSFORMATION",
                "status": "POSTED",
                "occurred_at": "2026-08-19T13:00:00+00:00",
                "facility_reference": "Planta Virasoro",
                "inputs": [
                    {
                        "batch_id": 201,
                        "batch_code": "REC-A-001",
                        "quantity": "70.000000",
                        "unit": "M3",
                    },
                    {
                        "batch_id": 202,
                        "batch_code": "REC-B-001",
                        "quantity": "30.000000",
                        "unit": "M3",
                    },
                ],
                "outputs": [
                    {
                        "batch_id": 301,
                        "batch_code": "ASERRADO-001",
                        "quantity": "65.000000",
                        "unit": "M3",
                    }
                ],
                "reconciliation": {
                    "unit": "M3",
                    "input_quantity": "100.000000",
                    "output_quantity": "65.000000",
                    "loss_quantity": "35.000000",
                    "yield_ratio": "0.650000",
                },
            }
        ],
        "source_lotes": [
            {
                "lote": {
                    "id": 10,
                    "identificador": "RODAL-PINO-A",
                    "productor_id": "PROVEEDOR-A",
                    "producto_forestal": "Pino resinoso",
                    "hectareas": 50.0,
                    "latitud": -28.05,
                    "longitud": -56.03,
                    "polygon_wkt": "POLYGON((-56.04 -28.06,-56.02 -28.06,-56.02 -28.04,-56.04 -28.04,-56.04 -28.06))",
                    "estatus": "Verde",
                },
                "attributed_shipment_quantity": "42.000000",
                "unit": "M3",
                "share_of_shipped_unit": "0.700000",
            },
            {
                "lote": {
                    "id": 11,
                    "identificador": "RODAL-PINO-B",
                    "productor_id": "PROVEEDOR-B",
                    "producto_forestal": "Pino resinoso",
                    "hectareas": 60.0,
                    "latitud": -28.06,
                    "longitud": -56.04,
                    "polygon_wkt": "POLYGON((-56.05 -28.07,-56.03 -28.07,-56.03 -28.05,-56.05 -28.05,-56.05 -28.07))",
                    "estatus": "Verde",
                },
                "attributed_shipment_quantity": "18.000000",
                "unit": "M3",
                "share_of_shipped_unit": "0.300000",
            },
        ],
    }


def test_manifest_is_buyer_facing_and_excludes_internal_database_ids() -> None:
    manifest = build_canonical_manifest(_payload())
    encoded = json.dumps(manifest, ensure_ascii=False)

    assert manifest["schema_version"] == SCHEMA_VERSION
    assert manifest["shipment"]["shipment_code"] == "EXP-UE-2026-001"
    assert manifest["lineage"]["complete"] is True
    assert manifest["origins"][0]["identifier"] == "RODAL-PINO-A"
    assert manifest["origins"][0]["attributed_shipment_quantity"] == "42.000000"
    assert manifest["origins"][1]["attributed_shipment_quantity"] == "18.000000"
    assert manifest["disclaimer"] == DISCLAIMER
    assert '"organization_id"' not in encoded
    assert '"shipment_item_id"' not in encoded
    assert '"source_lote_id"' not in encoded
    assert '"batch_id"' not in encoded
    assert '"event_id"' not in encoded


def test_manifest_hash_is_deterministic_for_same_traceability_content() -> None:
    first = build_canonical_manifest(_payload())
    second_payload = deepcopy(_payload())
    second_payload["source_lotes"] = list(reversed(second_payload["source_lotes"]))
    second_payload["items"][0]["source_contributions"] = list(
        reversed(second_payload["items"][0]["source_contributions"])
    )
    second_payload["events"][0]["inputs"] = list(
        reversed(second_payload["events"][0]["inputs"])
    )
    second = build_canonical_manifest(second_payload)

    assert first == second
    assert manifest_sha256(first) == manifest_sha256(second)
    assert manifest_sha256(first) == hashlib.sha256(
        json.dumps(
            first,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def test_manifest_hash_changes_when_business_evidence_changes() -> None:
    first = build_canonical_manifest(_payload())
    changed_payload = deepcopy(_payload())
    changed_payload["source_lotes"][0]["attributed_shipment_quantity"] = "41.000000"
    changed = build_canonical_manifest(changed_payload)

    assert manifest_sha256(first) != manifest_sha256(changed)


def test_bundle_contains_pdf_geojson_manifest_and_zip_with_same_digest() -> None:
    bundle = build_origin_dossier_bundle(_payload())

    assert bundle.pdf_bytes.startswith(b"%PDF")
    assert bundle.geojson["type"] == "FeatureCollection"
    assert len(bundle.geojson["features"]) == 2
    assert all(
        feature["geometry"]["type"] == "Polygon"
        for feature in bundle.geojson["features"]
    )
    assert (
        bundle.geojson["litoral_trace_integrity"]["canonical_manifest_sha256"]
        == bundle.manifest_sha256
    )
    assert (
        bundle.manifest_document["integrity"]["canonical_manifest_sha256"]
        == bundle.manifest_sha256
    )

    with zipfile.ZipFile(io.BytesIO(bundle.zip_bytes)) as archive:
        assert sorted(archive.namelist()) == [
            "README.txt",
            "dossier.pdf",
            "manifest.json",
            "origins.geojson",
        ]
        manifest_document = json.loads(archive.read("manifest.json"))
        geojson = json.loads(archive.read("origins.geojson"))
        assert archive.read("dossier.pdf").startswith(b"%PDF")
        assert (
            manifest_document["integrity"]["canonical_manifest_sha256"]
            == bundle.manifest_sha256
        )
        assert (
            geojson["litoral_trace_integrity"]["canonical_manifest_sha256"]
            == bundle.manifest_sha256
        )


def test_geometry_falls_back_to_point_and_never_fabricates_polygon() -> None:
    payload = _payload()
    payload["source_lotes"][0]["lote"]["polygon_wkt"] = "BROKEN POLYGON"
    manifest = build_canonical_manifest(payload)
    first_origin = next(
        origin for origin in manifest["origins"]
        if origin["identifier"] == "RODAL-PINO-A"
    )

    assert first_origin["geometry"] == {
        "type": "Point",
        "coordinates": [-56.03, -28.05],
    }
    assert any(
        warning["code"] == "SOURCE_GEOMETRY_POINT_FALLBACK"
        for warning in manifest["artifact_warnings"]
    )


def test_incomplete_lineage_stays_incomplete_in_dossier() -> None:
    bundle = build_origin_dossier_bundle(_payload(complete=False))

    assert bundle.canonical_manifest["lineage"]["complete"] is False
    assert bundle.canonical_manifest["lineage"]["issues"][0]["code"] == "MISSING_PROVENANCE"
    assert bundle.canonical_manifest["unit_totals"][0]["unresolved_quantity"] == "5.000000"
    assert DISCLAIMER in bundle.canonical_manifest["disclaimer"]


def test_safe_artifact_stem_prevents_content_disposition_injection() -> None:
    assert safe_artifact_stem('EXP 2026/001\r\n"evil"') == "EXP-2026-001-evil"
