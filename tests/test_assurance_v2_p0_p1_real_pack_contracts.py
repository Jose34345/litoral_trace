"""Pack 2 regression contracts without fabricating missing customs values."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from litoral_trace.us_lacey.canonical_shipment_truth import (
    CanonicalEvidence, CanonicalTruthState,
    _bind_unique_unassociated_source_row, build_canonical_shipment_truth,
)
from litoral_trace.us_lacey import worker


def _row(field, value, doc="128:logical-001"):
    return CanonicalEvidence(
        candidate_id=field + ":doc", document_id=doc, field_key=field,
        normalized_value=value, source_authority=0.7, candidate_score=0.86,
        source_page=1, source_text=field + " source literal " + value,
        line_key=None, component_key=None, evidence_class="EXPLICIT",
    )


def _fixture():
    return {
        "hts_code": (_row("hts_code", "4419.19.9010"),),
        "article_component": (_row("article_component", "Solid bamboo coasters"),),
        "plant_quantity": (
            _row("plant_quantity", "288.0", "124:logical-001"),
            _row("plant_quantity", "288.0"),
        ),
        "metric_unit": (
            _row("metric_unit", "KG", "124:logical-001"),
            _row("metric_unit", "KG"),
        ),
    }


def _payload(rows):
    return {key: {"state": "SUPPORTED_MULTIPLE" if len(v) > 1 else "SUPPORTED"} for key, v in rows.items()}


def test_pack2_unbound_single_row_is_review_only_and_never_promotes_total():
    rows = _fixture()
    bound = _bind_unique_unassociated_source_row(rows, _payload(rows))
    key = bound["hts_code"][0].line_key
    assert key and key.startswith("review-single-source:")
    assert bound["article_component"][0].component_key == key
    assert bound["plant_quantity"][0].component_key == key
    assert bound["plant_quantity"][0].document_id == "128:logical-001"
    assert bound["metric_unit"][0].component_key == key
    assert len(bound["plant_quantity"]) == 1  # no invented cross-PDF line identity


def test_pack2_full_canonical_truth_preserves_source_link_and_human_review():
    fields = _fixture()
    payload = {"canonical_fields": {}, "documents": [], "issues": []}
    for name, rows in fields.items():
        payload["canonical_fields"][name] = {
            "state": "SUPPORTED_MULTIPLE" if len(rows) > 1 else "SUPPORTED",
            "supporting_evidence": [{
                "candidate_id": row.candidate_id,
                "document_id": row.document_id,
                "field_key": name,
                "normalized_value": row.normalized_value,
                "scope": "MERCHANDISE_LINE" if name == "hts_code" else "PLANT_COMPONENT",
                "source_authority": row.source_authority,
                "candidate_score": row.candidate_score,
                "candidate": {"raw": {
                    "normalized_value": row.normalized_value,
                    "evidence_class": "EXPLICIT",
                }, "provenance": {
                    "page": 1,
                    "source_text": row.source_text,
                    "evidence_class": "EXPLICIT",
                }},
            } for row in rows],
        }
    truth = build_canonical_shipment_truth(payload)
    assert len(truth.plant_lines) == 1
    fields = truth.plant_lines[0].fields
    for name in ("hts_code", "article_component", "plant_quantity", "metric_unit"):
        assert fields[name].state is CanonicalTruthState.REVIEW_REQUIRED
        assert len(fields[name].values) == 1
        assert fields[name].evidence[0].document_id == "128:logical-001"
    assert "entered_value" not in fields
    assert "merchandise_description" not in truth.shipment_fields


def test_pack2_never_binds_unrelated_documents_or_multiple_source_rows():
    rows = _fixture()
    rows["article_component"] = (_row("article_component", "Solid bamboo coasters", "111:logical-001"),)
    assert _bind_unique_unassociated_source_row(rows, _payload(rows)) == rows
    rows = _fixture()
    rows["hts_code"] += (_row("hts_code", "4419.19.9010", "124:logical-001"),)
    assert _bind_unique_unassociated_source_row(rows, _payload(rows)) == rows


def test_pack2_conflicting_quantities_are_not_bound_to_one_line():
    rows = _fixture()
    rows["plant_quantity"] = (
        _row("plant_quantity", "288.0"),
        _row("plant_quantity", "384.0", "124:logical-001"),
    )
    bound = _bind_unique_unassociated_source_row(rows, _payload(rows))
    assert bound["plant_quantity"] == rows["plant_quantity"]
    assert bound["article_component"][0].component_key


def test_reuse_audit_details_pin_exact_source_without_claim_text(monkeypatch):
    fake_session = MagicMock()
    fake_session.scalars.return_value.all.return_value = [
        SimpleNamespace(field_name="genus", merchandise_line_reference="1", source_locator="reused_evidence:825e3b2d-87dd-4d0f-bbd0-143219c04aba"),
        SimpleNamespace(field_name="species", merchandise_line_reference="1", source_locator="reused_evidence:825e3b2d-87dd-4d0f-bbd0-143219c04aba"),
        SimpleNamespace(field_name="country_of_harvest", merchandise_line_reference="1", source_locator="reused_evidence:825e3b2d-87dd-4d0f-bbd0-143219c04aba"),
    ]
    monkeypatch.setattr(worker, "get_us_lacey_db_session", lambda: fake_session)
    tenant = MagicMock()
    monkeypatch.setattr(worker, "set_tenant_db_context", tenant)
    result = worker._reused_evidence_event_details(organization_id=1, operation_id=34, claim_count=3)
    tenant.assert_called_once_with(fake_session, 1)
    assert result["claim_count"] == 3 and result["provenance_complete"] is True
    assert len(result["reused_claim_sources"]) == 3
    assert {x["field_name"] for x in result["reused_claim_sources"]} == {"genus", "species", "country_of_harvest"}
    assert "Phyllostachys" not in str(result)
    fake_session.close.assert_called_once()


def test_reuse_audit_missing_source_is_explicitly_incomplete(monkeypatch):
    session = MagicMock()
    session.scalars.return_value.all.return_value = []
    monkeypatch.setattr(worker, "get_us_lacey_db_session", lambda: session)
    monkeypatch.setattr(worker, "set_tenant_db_context", lambda *args: None)
    result = worker._reused_evidence_event_details(organization_id=1, operation_id=34, claim_count=3)
    assert result == {"claim_count": 3, "reused_claim_sources": [], "provenance_complete": False}
