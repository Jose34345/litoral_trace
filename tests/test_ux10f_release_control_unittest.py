from __future__ import annotations

from pathlib import Path
import subprocess
import sys

from litoral_trace.services.traceability_release_control import (
    ATTENTION,
    BLOCKED,
    READY,
    build_release_control_view,
)


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "src" / "litoral_trace" / "templates" / "traceability_release_control.html"


def _payload(*, complete: bool = True, dispatched: bool = True, unresolved: str = "0", polygons: bool = True):
    return {
        "complete": complete,
        "allocation_method": "PROPORTIONAL_INPUT_ALLOCATION",
        "shipment": {
            "id": 50,
            "shipment_code": "EXP-UE-2026-001",
            "sale_reference": "FV-0001",
            "buyer_reference": "EU-BUYER-88",
            "destination_country": "DE",
            "shipped_at": "2026-08-21T10:00:00+00:00",
            "status": "DISPATCHED" if dispatched else "DRAFT",
            "lineage_state": "FINAL" if dispatched else "PREVIEW",
        },
        "unit_totals": [
            {
                "unit": "M3",
                "shipped_quantity": "60",
                "attributed_quantity": str(60 - int(unresolved)),
                "unresolved_quantity": unresolved,
            }
        ],
        "source_lotes": [
            {
                "lote": {
                    "id": 1,
                    "identificador": "RODAL-A",
                    "productor_id": "PROV-A",
                    "producto_forestal": "Pino",
                    "latitud": -28.0,
                    "longitud": -58.0,
                    "polygon_wkt": "POLYGON((-58 -28,-58 -27.9,-57.9 -27.9,-58 -28))" if polygons else None,
                },
                "attributed_shipment_quantity": "42",
                "unit": "M3",
            },
            {
                "lote": {
                    "id": 2,
                    "identificador": "RODAL-B",
                    "productor_id": "PROV-B",
                    "producto_forestal": "Pino",
                    "latitud": -28.1,
                    "longitud": -58.1,
                    "polygon_wkt": "POLYGON((-58.1 -28.1,-58.1 -28,-58 -28,-58.1 -28.1))" if polygons else None,
                },
                "attributed_shipment_quantity": "18",
                "unit": "M3",
            },
        ],
        "events": [
            {
                "id": 20,
                "event_code": "PROC-70-30",
                "event_type": "TRANSFORMATION",
                "status": "POSTED",
                "inputs": [
                    {"batch_id": 10, "batch_code": "RAW-A", "quantity": "70", "unit": "M3"},
                    {"batch_id": 11, "batch_code": "RAW-B", "quantity": "30", "unit": "M3"},
                ],
                "outputs": [
                    {"batch_id": 12, "batch_code": "ASERRADO-001", "quantity": "65", "unit": "M3"},
                ],
                "reconciliation": {
                    "unit": "M3",
                    "input_quantity": "100",
                    "output_quantity": "65",
                    "loss_quantity": "35",
                    "yield_ratio": "0.65",
                },
            }
        ],
        "items": [
            {
                "batch": {"id": 12, "code": "ASERRADO-001"},
                "shipped_quantity": "60",
                "unit": "M3",
                "unresolved_quantity": unresolved,
            }
        ],
        "issues": [] if complete else [{"code": "UNRESOLVED", "message": "Volumen sin resolver"}],
    }


def _evidence_for_full_graph():
    refs = [
        ("SOURCE_LOTE", "RODAL-A"),
        ("SOURCE_LOTE", "RODAL-B"),
        ("TRACEABILITY_EVENT", "PROC-70-30"),
        ("TRACEABILITY_BATCH", "RAW-A"),
        ("TRACEABILITY_BATCH", "RAW-B"),
        ("TRACEABILITY_BATCH", "ASERRADO-001"),
        ("SHIPMENT", "EXP-UE-2026-001"),
    ]
    return tuple(
        {
            "subject_type": subject_type,
            "subject_reference": reference,
            "evidence_type": "REMITO",
            "document": {"sha256": f"{index:064x}", "filename": f"doc-{index}.pdf"},
        }
        for index, (subject_type, reference) in enumerate(refs, start=1)
    )


def _check(view, key: str):
    return next(item for item in view["checks"] if item["key"] == key)


def test_ready_release_control_is_decision_oriented_not_regulatory_score():
    view = build_release_control_view(
        _payload(),
        documentary_evidence=_evidence_for_full_graph(),
        manifest_sha256="a" * 64,
    )

    assert view["overall"]["state"] == READY
    assert view["overall"]["title"] == "Listo para compartir"
    assert view["overall"]["ready"] == 7
    assert view["overall"]["blocked"] == 0
    assert view["overall"]["attention"] == 0
    assert view["documentary_coverage"]["covered_subjects"] == 7
    assert view["documentary_coverage"]["total_subjects"] == 7
    assert view["manifest_sha256"] == "a" * 64
    assert "No constituye" in view["disclaimer"]


def test_attention_state_surfaces_document_geometry_and_commercial_gaps_without_blocking_lineage():
    payload = _payload(polygons=False)
    payload["shipment"]["buyer_reference"] = None
    evidence = _evidence_for_full_graph()[:2]
    view = build_release_control_view(
        payload,
        documentary_evidence=evidence,
        manifest_sha256="b" * 64,
    )

    assert view["overall"]["state"] == ATTENTION
    assert _check(view, "lineage")["state"] == READY
    assert _check(view, "volume")["state"] == READY
    assert _check(view, "geometry")["state"] == ATTENTION
    assert _check(view, "evidence")["state"] == ATTENTION
    assert _check(view, "commercial")["state"] == ATTENTION
    assert view["next_actions"][0]["state"] == ATTENTION


def test_blocked_state_prioritizes_actual_release_blockers():
    view = build_release_control_view(
        _payload(complete=False, dispatched=False, unresolved="25"),
        documentary_evidence=(),
        manifest_sha256=None,
        dossier_available=False,
        dossier_error="No fue posible generar el expediente.",
    )

    assert view["overall"]["state"] == BLOCKED
    assert _check(view, "dispatch")["state"] == BLOCKED
    assert _check(view, "lineage")["state"] == BLOCKED
    assert _check(view, "volume")["state"] == BLOCKED
    assert _check(view, "dossier")["state"] == BLOCKED
    assert view["next_actions"][0]["state"] == BLOCKED


def test_volume_check_never_requires_cross_unit_conversion():
    payload = _payload()
    payload["unit_totals"] = [
        {"unit": "M3", "shipped_quantity": "60", "attributed_quantity": "60", "unresolved_quantity": "0"},
        {"unit": "TON", "shipped_quantity": "10", "attributed_quantity": "10", "unresolved_quantity": "0"},
    ]
    ready = build_release_control_view(
        payload,
        documentary_evidence=_evidence_for_full_graph(),
        manifest_sha256="c" * 64,
    )
    assert _check(ready, "volume")["state"] == READY

    payload["unit_totals"][1]["unresolved_quantity"] = "1"
    blocked = build_release_control_view(
        payload,
        documentary_evidence=_evidence_for_full_graph(),
        manifest_sha256="c" * 64,
    )
    assert _check(blocked, "volume")["state"] == BLOCKED


def test_template_has_interactive_filters_route_and_hash_copy_affordance():
    text = TEMPLATE.read_text(encoding="utf-8")
    assert "Pulso del despacho" in text
    assert "Ruta Litoral Trace" in text
    assert "Ruta de cierre" in text
    assert "Huella verificable" in text
    assert 'data-release-filter="BLOCKED"' in text
    assert 'data-release-filter="ATTENTION"' in text
    assert 'data-release-filter="READY"' in text
    assert 'id="copy-release-hash"' in text
    assert "navigator.clipboard.writeText" in text
    assert "No constituye" in text or "no constituye" in text


def test_release_control_route_is_exposed_on_cold_start():
    probe = """
import main
assert str(main.app.url_path_for('render_release_control')) == '/release-control'
"""
    completed = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
