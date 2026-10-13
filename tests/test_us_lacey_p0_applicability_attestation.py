"""P0 evidence-bound applicability attestation: conservative, auditable, never a waiver."""
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from litoral_trace.us_lacey.regulatory.applicability import attestation as a

ROOT = Path(__file__).resolve().parents[1]


def test_no_confirmed_review_no_event_or_db_connection():
    with pytest.raises(a.ApplicabilityAttestationError, match="explicitly confirm"):
        a.record_applicability_attestation(
            organization_id=1, operation_public_id="nonexistent", reviewer_user_id=10,
            entry_type="01", bamboo_origin="CULTIVATED",
            source_document_id=2, source_page=1, explicitly_confirmed=False,
            session_factory=lambda: pytest.fail("DB must not be touched")
        )


@pytest.mark.parametrize("entry_type,origin,document,page", [
    ("99","CULTIVATED",11,1),
    ("01","CULTIVATED",0,1),
    ("01","CULTIVATED",11,10001),
    ("01","CULTIVATED",11,0),
    ("01","bogus",11,1),
])
def test_invalid_inputs_fail_before_db(entry_type,origin,document,page):
    with pytest.raises(a.ApplicabilityAttestationError):
        a.record_applicability_attestation(
            organization_id=1, operation_public_id="irrelevant", reviewer_user_id=10,
            entry_type=entry_type, bamboo_origin=origin,
            source_document_id=document, source_page=page, explicitly_confirmed=True,
            session_factory=lambda: pytest.fail("No database access for invalid fields")
        )


def _source(doc_id, role, filename, *, hash_value="a"*64, deleted=None):
    return (NS(assurance_document_id=doc_id, version_number=1, document_role=role),
            NS(original_filename=filename, sha256=hash_value, status="available",
               deleted_at=deleted, content_type="application/pdf"))


def test_preparation_worksheet_is_never_primary_applicability_authority():
    link, vault = _source(23,"COMMERCIAL_INVOICE","05_Entry_Preparation_Worksheet.pdf")
    assert not a._usable_source(link, vault)
    assert a._source_class(link,vault).code == "DERIVED_PREPARATION"


def _test_session(rows, events=()):
    class DB:
        def __init__(self):
            self.scalar_calls = 0
            self.committed = False
            self.rollback_called = False
            self.closed = False
        def scalar(self, _):
            self.scalar_calls += 1
            return NS(id=9) if self.scalar_calls == 1 else NS(id=25, source_set_fingerprint="f"*64)
        def execute(self, _): return NS(all=lambda: rows)
        def scalars(self, _): return NS(all=lambda: events)
        def commit(self): self.committed = True
        def rollback(self): self.rollback_called = True
        def close(self): self.closed = True
    db = DB()
    return db


def _record(db, monkeypatch, **kwargs):
    monkeypatch.setattr(a, "set_tenant_db_context", lambda *args: None)
    monkeypatch.setattr(a, "_current_context", lambda *_args: (NS(id=9), NS(id=25,source_set_fingerprint="f"*64)))
    monkeypatch.setattr(a, "_source_rows", lambda *_args,**_kw: db.rows)
    recorded = []
    monkeypatch.setattr(a, "append_operation_event", lambda *args, **kw: recorded.append(kw))
    result = a.record_applicability_attestation(
        organization_id=99, operation_public_id="abc", reviewer_user_id=55,
        entry_type="01", bamboo_origin="CULTIVATED",
        source_document_id=20, source_page=1, botanical_document_id=30,
        botanical_page=2, explicitly_confirmed=True, session_factory=lambda: db,
        **kwargs,
    )
    return result, recorded


def test_cultivated_bamboo_attestation_requires_separate_supplier_evidence(monkeypatch):
    class DB:
        def __init__(self, rows):
            self.rows = rows
            self.committed=False
            self.closed=False
        def scalars(self, _): return NS(all=lambda: [
            NS(field_name="hts_code",field_status="CONFIRMED",human_value="4419199010",normalized_value=None,original_value=None),
            NS(field_name="article_component",field_status="CONFIRMED",human_value="bamboo coasters",normalized_value=None,original_value=None)])
        def commit(self): self.committed=True
        def rollback(self): pass
        def close(self): self.closed=True
    db=DB([_source(20,"COMMERCIAL_INVOICE","invoice.pdf"), _source(30,"SUPPLIER_DECLARATION","supplier-origin.pdf",hash_value="b"*64)])
    result, recorded=_record(db,monkeypatch)
    assert db.committed and db.closed
    assert result["advisory_only"] is True
    assert result["filing_authorization"] is False
    assert result["source_set_fingerprint"] == "f"*64
    assert result["source_document_id"] == 20
    assert result["botanical_document_id"] == 30
    assert recorded[0]["event_type"] == a.OperationEventType.HUMAN_REVIEW
    assert recorded[0]["actor_identity"] == "55"

    db2=DB([_source(20,"COMMERCIAL_INVOICE","invoice.pdf"), _source(30,"ENTRY_WORKSHEET","preparation worksheet.pdf")])
    with pytest.raises(a.ApplicabilityAttestationError,match="supplier"):
        _record(db2,monkeypatch)
    assert not db2.committed and db2.closed


def test_source_changes_invalidate_recorded_review(monkeypatch):
    monkeypatch.setattr(a, "set_tenant_db_context", lambda *args: None)
    rows=[_source(20,"COMMERCIAL_INVOICE","invoice.pdf",hash_value="updated")]
    event=NS(details={
        "kind":a.KIND,"source_document_id":20,"source_sha256":"old",
        "source_version":"1","source_set_fingerprint":"f"*64,
        "entry_type":"01","bamboo_origin":"UNKNOWN","hts10":"4419199010",
        "preview_status":"POTENTIALLY_REQUIRED","source_page":1,
    },actor_identity="55")
    db=_test_session(rows,[event])
    sources,record=a.read_applicability_evidence(
        organization_id=99,operation_public_id="abc",session_factory=lambda:db)
    assert record is not None and not record.current
    assert len(sources) == 1
    assert db.closed


def test_mutation_is_separated_from_evaluation_and_lacey_filing_gate():
    app = (ROOT/"src/litoral_trace/web/us_lacey_pilot_app.py").read_text()
    triage = (ROOT/"src/litoral_trace/templates/us_lacey/fragments/applicability_triage.html").read_text()
    implementation = (ROOT/"src/litoral_trace/us_lacey/regulatory/applicability/attestation.py").read_text()
    assert 'purpose=f"applicability:{operation_public_id}"' in app
    assert '_operational_mutation_context(us_session)' in app
    assert 'name="explicitly_confirmed"' in triage
    assert 'name="botanical_document_id"' in triage
    assert 'source_set_fingerprint' in implementation
    assert '"filing_authorization": False' in implementation
    assert "finalize_us_lacey_review" not in implementation
    assert "issue_locked_assurance_package" not in implementation
