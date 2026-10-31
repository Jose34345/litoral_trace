"""P0 APHIS applicability triage, source classification and private PDF contract."""
from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path
import pytest
from fastapi import HTTPException

from litoral_trace.us_lacey.regulatory.applicability.triage import (
    preview_applicability, REQUIRED_TYPES, NOT_REQUIRED_TYPES
)
from litoral_trace.us_lacey.document_evidence_class import classify_document
from litoral_trace.web import us_lacey_source_inspector as inspector

ROOT = Path(__file__).resolve().parents[1]


def detail(hts="4419199010", description="solid bamboo coasters"):
    return SimpleNamespace(fields=[
        SimpleNamespace(field_name="hts_code", effective_value=None, proposed_value=hts),
        SimpleNamespace(field_name="article_component", effective_value=None, proposed_value=description),
    ])


def test_missing_hts_fails_closed_even_with_cultivated_selected():
    r = preview_applicability(detail(hts=""), entry_type="01", bamboo_origin="CULTIVATED")
    assert r.status == "NEEDS_INFORMATION"
    assert r.hts_status == "MISSING_OR_INVALID"
    assert "HTS" in r.title
    assert r.catalog_version


def test_entry_type_01_and_unknown_bamboo_remain_review_required():
    r = preview_applicability(detail(), entry_type="01", bamboo_origin="UNKNOWN")
    assert r.hts_status == "LISTED"
    assert r.status == "POTENTIALLY_REQUIRED"
    assert "before filing" in r.explanation


def test_cultivated_bamboo_only_preview_not_verified_waiver():
    r = preview_applicability(detail(), entry_type="01", bamboo_origin="CULTIVATED")
    assert r.status == "PRELIMINARY_EXCEPTION"
    assert "must be verified" in r.explanation
    assert "not" not in r.status.lower()


def test_entry_types_11_and_06_have_distinct_states():
    assert preview_applicability(detail(), entry_type="11").status == "PRELIMINARY_EXCEPTION"
    r = preview_applicability(detail(), entry_type="06")
    assert r.status == "NEEDS_INFORMATION"
    assert "FTZ" in r.title
    assert {"01", "02", "03", "07", "31", "32", "34", "38", "52"} <= REQUIRED_TYPES
    assert {"11", "12", "13", "21", "22", "23", "86"} <= NOT_REQUIRED_TYPES


def test_unknown_query_inputs_are_never_treated_as_authority():
    r = preview_applicability(detail(), entry_type="1000", bamboo_origin="CULTIVATED<script>")
    assert r.entry_type == ""
    assert r.bamboo_origin == ""
    assert r.status == "NEEDS_INFORMATION"


def test_multi_line_different_hts_does_not_fake_one_combined_tariff_code():
    d = detail()
    d.fields.append(SimpleNamespace(field_name="hts_code", effective_value=None,
                                    proposed_value="9403608081"))
    assert preview_applicability(d, entry_type="01").status == "NEEDS_INFORMATION"


def test_source_class_cannot_promote_working_worksheet_to_primary():
    assert classify_document("05_Entry_Preparation_Worksheet_Shipment2.pdf", "COMMERCIAL_INVOICE").code == "DERIVED_PREPARATION"
    assert classify_document("Entry Worksheet.pdf", "ENTRY_WORKSHEET").code == "DERIVED_PREPARATION"
    assert classify_document("Invoice.pdf", "COMMERCIAL_INVOICE").code == "COMMERCIAL_SOURCE"
    assert classify_document("supplier botany.pdf", "SUPPLIER_DECLARATION").code == "SUPPLIER_SOURCE"
    assert classify_document("invoice.pdf", "UNKNOWN").code != "COMMERCIAL_SOURCE"


def test_current_pdf_failure_refuses_missing_tenant_membership(monkeypatch):
    monkeypatch.setattr(inspector, "_resolve_current_source_document", lambda **kw: None)
    with pytest.raises(HTTPException) as exc:
        inspector.stream_current_pdf(
            identity=SimpleNamespace(organization_id=99, user_id=11, email="review@example.com"),
            operation_public_id="7b1f1bb5-a299-4f3c-8e33-57f58fb71381",
            assurance_document_id=23,
        )
    assert exc.value.status_code == 404


def test_current_pdf_integrity_rejects_modified_content_without_leak(monkeypatch):
    monkeypatch.setattr(inspector, "_resolve_current_source_document",
                        lambda **kw: {"vault_public_id": "abc", "sha256": "expected"})
    closed = []
    class Materialized:
        document = SimpleNamespace(sha256="different", content_type="application/pdf")
        def close(self): closed.append(True)
    monkeypatch.setattr(inspector, "_current_lacey_vault", lambda: SimpleNamespace(
        materialize_verified_download=lambda **kw: Materialized()
    ))
    with pytest.raises(HTTPException) as exc:
        inspector.stream_current_pdf(
            identity=SimpleNamespace(organization_id=99, user_id=11, email="review@example.com"),
            operation_public_id="7b1f1bb5-a299-4f3c-8e33-57f58fb71381",
            assurance_document_id=23,
        )
    assert exc.value.status_code == 409
    assert closed


def test_source_inspector_and_applicability_render_contracts():
    op = (ROOT / "src/litoral_trace/templates/us_lacey/operation_detail.html").read_text()
    ws = (ROOT / "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html").read_text()
    app = (ROOT / "src/litoral_trace/templates/us_lacey/fragments/applicability_triage.html").read_text()
    js = (ROOT / "src/litoral_trace/static/js/us_lacey_source_inspector.js").read_text()
    css = (ROOT / "src/litoral_trace/static/css/us_lacey_source_inspector.css").read_text()
    assert 'fragments/applicability_triage.html' in op
    assert 'fragments/source_inspector.html' in op
    assert 'data-source-inspect' in ws
    assert 'data-source-authority' in ws
    assert 'name="bamboo_origin"' in app
    assert 'name="entry_type"' in app
    assert "never" not in app or "not" in app
    assert 'window.location.origin' in js
    assert 'root.addEventListener("cancel"' in js
    assert "root.showModal()" in js
    assert "prefers-reduced-motion:reduce" in css
    assert "180ms" in css
    assert "document.register" not in js



def test_source_pdf_vault_uses_lacey_postgres_and_lacey_storage(monkeypatch):
    """Never use the legacy Vault SQLite session or generic S3 configuration."""
    sentinel_storage = object()
    seen = {}
    monkeypatch.setattr(inspector, "build_us_lacey_storage_settings", lambda: sentinel_storage)

    def fake_vault(**kwargs):
        seen.update(kwargs)
        return object()

    monkeypatch.setattr(inspector, "VaultService", fake_vault)
    result = inspector._current_lacey_vault()

    assert result is not None
    assert seen["session_factory"] is inspector.get_us_lacey_db_session
    assert seen["storage_settings"] is sentinel_storage


def test_source_pdf_vault_metadata_sql_error_returns_503_not_raw_500(monkeypatch):
    """Malformed/unavailable metadata should fail closed with a recoverable error."""
    from sqlalchemy.exc import OperationalError

    monkeypatch.setattr(inspector, "_resolve_current_source_document",
                        lambda **kw: {"vault_public_id": "abc", "sha256": "expected"})

    def raise_metadata_error(**_kw):
        raise OperationalError("SELECT vault_documents", {}, Exception("metadata unavailable"))

    monkeypatch.setattr(inspector, "_current_lacey_vault",
                        lambda: SimpleNamespace(materialize_verified_download=raise_metadata_error))
    with pytest.raises(HTTPException) as exc:
        inspector.stream_current_pdf(
            identity=SimpleNamespace(organization_id=99, user_id=11, email="review@example.com"),
            operation_public_id="7b1f1bb5-a299-4f3c-8e33-57f58fb71381",
            assurance_document_id=23,
        )
    assert exc.value.status_code == 503
    assert "unavailable" in exc.value.detail.lower()
