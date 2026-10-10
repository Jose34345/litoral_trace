"""Local-only synthetic Assurance V2 demo.

Run: python -m litoral_trace.web.us_lacey_assurance_v2_demo
Open http://127.0.0.1:8765/assurance-v2/operations/11111111-2222-3333-4444-555555555555
Do not mount this fixture app inside the production application.
"""
from __future__ import annotations

from html import escape
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from litoral_trace.us_lacey.exporters.assurance_v2 import AssuranceCase
from litoral_trace.us_lacey.exporters.assurance_v2_package import MemoryPackageStore
from litoral_trace.web.us_lacey_assurance_v2_views import (
    WorkspacePrincipal, create_assurance_v2_router,
)

_ID = "11111111-2222-3333-4444-555555555555"
_REF = "__shipment__"


def fixture_case() -> AssuranceCase:
    names = {
        _REF: {
            "estimated_arrival_date": "2026-10-25", "filing_entry_reference": "123-4567890-1",
            "manufacturer_id": "USABC1234", "importer_name": "Example Imports LLC",
            "consignee_name": "Example Consignee LLC",
            "importer_address": "10 Cargo Way", "consignee_address": "20 Port Road",
            "merchandise_description": "Solid oak furniture",
        },
        "LINE-1": {
            "hts_code": "4407990000", "entered_value": "18000",
            "article_component": "Oak furniture component",
            "genus": "Quercus", "species": "alba", "country_of_harvest": "United States",
            "plant_quantity": "20", "metric_unit": "kg",
        },
    }
    fields = []
    for line, values in names.items():
        for name, value in values.items():
            authority = ("HUMAN_CONFIRMED" if name == "genus" else
                         "VERIFIED_REUSE" if name == "country_of_harvest" else "SUPPORTED")
            fields.append({
                "line_reference": line, "name": name, "value": value,
                "authority": authority,
                "document_ids": ["invoice-v2"],
                "evidence_ids": ["history-country" if name == "country_of_harvest" else f"ev:{name}"],
                "decision_id": "decision-genus" if name == "genus" else None,
                "reuse_id": "reuse-country" if name == "country_of_harvest" else None,
            })
    return AssuranceCase(
        organization_id=11, operation_id=_ID,
        source_set_fingerprint="synthetic-source-set-v2", source_set_revision=2,
        current_source_set_fingerprint="synthetic-source-set-v2", current_source_set_revision=2,
        ruleset_version="synthetic-lacey-rules-v1",
        documents=({"id": "invoice-v2", "name": "Synthetic Commercial Invoice",
                    "role": "COMMERCIAL_INVOICE", "sha256": "e" * 64, "version": 2},),
        lines=("LINE-1",), fields=tuple(fields),
        identities=({"line_reference": "LINE-1", "supplier_id": "demo-us-supplier",
                     "product_id": "demo-oak", "sku": "OAK-CHAIR", "status": "VERIFIED"},),
        exceptions=({
            "id": "exception-species", "field_name": "species", "line_reference": "LINE-1",
            "reason": "Two source documents disagree on species", "blocking": True,
            "status": "OPEN", "entity_ref": "demo-us-supplier/demo-oak",
            "historical_evidence": "Prior reviewed shipment claims genus Quercus; species still disputed.",
            "candidates": [
                {"id": "candidate-1", "value": "alba", "document_id": "invoice-v2",
                 "document_name": "Synthetic Commercial Invoice", "page": 1,
                 "locator": "page:1:block:8", "evidence_id": "ev:species:1"},
                {"id": "candidate-2", "value": "rubra", "document_id": "invoice-v2",
                 "document_name": "Synthetic Supplier Declaration", "page": 2,
                 "locator": "page:2:block:3", "evidence_id": "ev:species:2"},
            ],
        },),
        decisions=({"id": "decision-genus", "field_name": "genus", "line_reference": "LINE-1",
                    "action": "ACCEPT", "value": "Quercus", "actor_id": "demo-reviewer",
                    "decided_at": "2026-10-09T15:00:00Z", "reason": "Botanical certificate"},),
        reuses=({"id": "reuse-country", "status": "VERIFIED", "evidence_id": "history-country",
                 "source_document_hash": "f" * 64, "valid_until": "2030-10-09T00:00:00Z",
                 "verified_by": "demo-reviewer", "line_reference": "LINE-1",
                 "supplier_id": "demo-us-supplier", "product_id": "demo-oak"},),
        rules=({"id": "HTS_APPLICABILITY", "version": "synthetic-lacey-rules-v1",
                "inputs_fingerprint": "fixture-hts-input", "status": "PASS",
                "blocking": True},),
    )


def create_fixture_demo_app() -> FastAPI:
    app = FastAPI(title="Assurance V2 Synthetic Fixture — Local Only")
    from fastapi.staticfiles import StaticFiles
    from litoral_trace.web.templates import STATIC_DIR
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    case = fixture_case()
    principal = WorkspacePrincipal(organization_id=11, actor_id="fixture-reviewer", csrf_token="demo-csrf")
    app.include_router(create_assurance_v2_router(
        principal_for=lambda request: principal,
        case_for=lambda person, operation: case if operation == _ID else None,
        decision_gateway=lambda person, operation, command: {
            "status": "FIXTURE_ONLY_NOT_PERSISTED", "exception_id": command["exception_id"],
        },
        csrf_verify=lambda person, token: token == person.csrf_token,
        package_store=MemoryPackageStore(),
        document_link_for=lambda person, op, document, page:
            f"/fixture-document/{document}?page={page or 1}",
    ))
    @app.get("/fixture-document/{document_id}", response_class=HTMLResponse)
    def document_stub(document_id: str, page: int = 1):
        return HTMLResponse(f"<h1>Synthetic document {escape(document_id)}</h1><p>Fixture page {page}. This is not an actual source PDF.</p>")
    return app


if __name__ == "__main__":
    import uvicorn
    # Fixture auth is deliberately static; NEVER bind to a public interface.
    uvicorn.run(create_fixture_demo_app(), host="127.0.0.1", port=8765)
