"""P1E authenticated dossier endpoint contracts."""
from __future__ import annotations

import asyncio
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import litoral_trace.api.traceability_dossier as dossier_api
from litoral_trace.services.traceability_dossier import OriginDossierBundle


EXPECTED_PATHS = {
    "/api/v1/traceability/shipments/dossier/manifest",
    "/api/v1/traceability/shipments/dossier/geojson",
    "/api/v1/traceability/shipments/dossier/pdf",
    "/api/v1/traceability/shipments/dossier/bundle",
}


def _bundle() -> OriginDossierBundle:
    digest = "a" * 64
    manifest = {
        "shipment": {"shipment_code": "EXP-UE-2026-001"},
    }
    document = {
        "integrity": {
            "algorithm": "SHA-256",
            "canonical_manifest_sha256": digest,
        },
        "manifest": manifest,
    }
    geojson = {
        "type": "FeatureCollection",
        "features": [],
    }
    return OriginDossierBundle(
        shipment_code="EXP-UE-2026-001",
        canonical_manifest=manifest,
        manifest_document=document,
        manifest_sha256=digest,
        manifest_json_bytes=b'{"manifest":{}}\n',
        geojson=geojson,
        geojson_bytes=b'{"type":"FeatureCollection"}\n',
        pdf_bytes=b"%PDF-1.7 dossier",
        zip_bytes=b"PK dossier",
    )


def _user():
    return SimpleNamespace(
        organization_id=7,
        role="auditor",
    )


def test_p1e_download_endpoints_are_registered_on_cold_start() -> None:
    root_dir = Path(__file__).resolve().parents[1]
    probe = f"""
import main
expected = {sorted(EXPECTED_PATHS)!r}
schema = main.app.openapi()
for path in expected:
    assert path in schema['paths'], (path, sorted(schema['paths']))
    operation = schema['paths'][path]['get']
    parameters = operation.get('parameters', [])
    shipment = [item for item in parameters if item.get('name') == 'shipment_code']
    assert len(shipment) == 1, (path, parameters)
    assert shipment[0].get('in') == 'query', shipment[0]
    assert shipment[0].get('required') is True, shipment[0]
"""
    completed = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=root_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, (
        "P1E dossier endpoints were not exposed on isolated ASGI cold start.\n"
        f"STDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
    )


def test_manifest_download_has_integrity_header_and_safe_attachment(monkeypatch) -> None:
    bundle = _bundle()
    captured = {}

    def _fake_load_bundle(**kwargs):
        captured.update(kwargs)
        return bundle

    monkeypatch.setattr(dossier_api, "_load_bundle", _fake_load_bundle)

    requested_code = 'EXP 2026/001\r\n"evil"'
    response = asyncio.run(
        dossier_api.descargar_manifest_dossier_endpoint(
            requested_code,
            user=_user(),
        )
    )

    assert captured["shipment_code"] == requested_code
    assert response.status_code == 200
    assert response.media_type == "application/json"
    assert response.headers["x-litoral-trace-manifest-sha256"] == bundle.manifest_sha256
    assert response.headers["cache-control"] == "private, no-store"
    assert response.headers["content-disposition"] == (
        'attachment; filename="litoral-trace-EXP-UE-2026-001-manifest.json"'
    )
    assert response.body == bundle.manifest_json_bytes


def test_geojson_pdf_and_bundle_downloads_use_expected_media_types(monkeypatch) -> None:
    bundle = _bundle()
    monkeypatch.setattr(dossier_api, "_load_bundle", lambda **kwargs: bundle)

    geojson = asyncio.run(
        dossier_api.descargar_geojson_dossier_endpoint(
            bundle.shipment_code,
            user=_user(),
        )
    )
    pdf = asyncio.run(
        dossier_api.descargar_pdf_dossier_endpoint(
            bundle.shipment_code,
            user=_user(),
        )
    )
    archive = asyncio.run(
        dossier_api.descargar_bundle_dossier_endpoint(
            bundle.shipment_code,
            user=_user(),
        )
    )

    assert geojson.media_type == "application/geo+json"
    assert geojson.body == bundle.geojson_bytes
    assert geojson.headers["content-disposition"].endswith('-origins.geojson"')

    assert pdf.media_type == "application/pdf"
    assert pdf.body.startswith(b"%PDF")
    assert pdf.headers["content-disposition"].endswith('-dossier.pdf"')

    assert archive.media_type == "application/zip"
    assert archive.body.startswith(b"PK")
    assert archive.headers["content-disposition"].endswith('-dossier.zip"')

    for response in (geojson, pdf, archive):
        assert response.headers["x-litoral-trace-manifest-sha256"] == bundle.manifest_sha256
        assert response.headers["cache-control"] == "private, no-store"
