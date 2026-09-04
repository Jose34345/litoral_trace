"""Service-path PostgreSQL acceptance coverage for immutable Engine 2 snapshots."""
from __future__ import annotations
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
from types import SimpleNamespace
import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from litoral_trace.db.models import ReconciliationIssue, UsLaceyEngineDocumentRun, UsLaceyEngineShipmentRun, UsLaceyOperation, UsLaceyOperationDocument, UsLaceyOperationField, UsLaceyFieldCandidate, User
from litoral_trace.lacey_engine.domain import AdmittedCandidate, DocumentResolution, DocumentType, EvidenceClass, FieldStatus, LayoutBlock, ParsedLayout, Provenance, RawCandidate, ResolvedField
from litoral_trace.lacey_engine.serialization import DOCUMENT_RESOLUTION_SCHEMA_VERSION, SHIPMENT_RESOLUTION_SCHEMA_VERSION, deserialize_shipment_resolution, serialize_document_resolution
from litoral_trace.lacey_engine.shipment import LaceyRuleset
from litoral_trace.us_lacey import lacey_engine_service as service_module
from litoral_trace.us_lacey.lacey_engine_service import UsLaceyEngine2Service, source_set_fingerprint
from tests.us_lacey_engine2_postgres import FakeVault, add_test_document, create_test_graph, engine2_postgres_engine, engine2_postgres_session_factory, tenant_session


def _resolution(filename, document_type, facts):
    fields = {}
    for ordinal, (key, value) in enumerate(facts.items()):
        label = {"bill_of_lading": "Master B/L", "container_number": "Container Number", "genus": "Component A Genus", "species": "Component A Species", "country_of_harvest": "Component A Country of Harvest"}.get(key, key)
        block = LayoutBlock(f"b{ordinal}", 1, None, value, "text", key_text=label, value_text=value)
        raw = RawCandidate(key, value, value, block, EvidenceClass.EXPLICIT, "test", "1", label=label)
        candidate = AdmittedCandidate(raw, Provenance(filename, 1, None, block.block_id, value, "test", "1", EvidenceClass.EXPLICIT), 1.0, document_type)
        fields[key] = ResolvedField(key, FieldStatus.MATCHED, value, candidate, (candidate,))
    return DocumentResolution(filename, "test-engine", document_type, 1.0, ParsedLayout((), 1), (), fields)


def _dossier(factory):
    org, operation, bill_link, bill_assurance, _, bill_sha = create_test_graph(factory, content=b"bill")
    supplier_link, supplier_assurance, _, supplier_sha = add_test_document(factory, organization_id=org, operation_id=operation, role="SUPPLIER_DECLARATION", filename="supplier.pdf", content=b"supplier")
    resolutions = {"bill.pdf": _resolution("bill.pdf", DocumentType.BILL_OF_LADING, {"bill_of_lading": "MAEU274342495", "container_number": "MSKU9228574"}), "supplier.pdf": _resolution("supplier.pdf", DocumentType.SUPPLIER_DECLARATION, {"genus": "Pinus", "species": "radiata", "country_of_harvest": "CHILE"})}
    return org, operation, (bill_link, bill_assurance, bill_sha), (supplier_link, supplier_assurance, supplier_sha), resolutions


def _service(factory, resolutions, **kwargs):
    return UsLaceyEngine2Service(session_factory=factory, vault_service=FakeVault(b"unused"), **kwargs), lambda **values: resolutions[values["filename"]]


def _snapshot(factory, organization_id, snapshot_id):
    session = tenant_session(factory, organization_id); row = session.query(UsLaceyEngineShipmentRun).filter_by(id=snapshot_id).one(); session.expunge(row); session.close(); return row


def test_engine2_document_run_persists_resolution(engine2_postgres_session_factory, monkeypatch):
    org, operation, link, assurance, _, sha = create_test_graph(engine2_postgres_session_factory)
    monkeypatch.setattr(service_module, "process_document", lambda **_: _resolution("bill.pdf", DocumentType.BILL_OF_LADING, {"bill_of_lading": "MAEU274342495"}))
    assert UsLaceyEngine2Service(session_factory=engine2_postgres_session_factory, vault_service=FakeVault(b"x")).resolve_operation_with_engine2(organization_id=org, operation_id=operation).status == "SUCCEEDED"
    session = tenant_session(engine2_postgres_session_factory, org); run = session.query(UsLaceyEngineDocumentRun).filter_by(operation_document_id=link).one()
    assert (run.status, run.assurance_document_id, run.source_sha256) == ("SUCCEEDED", assurance, sha) and run.resolution_json
    session.close()


def test_engine2_document_failure_persists_safe_error(engine2_postgres_session_factory, monkeypatch):
    org, operation, link, _, _, _ = create_test_graph(engine2_postgres_session_factory)
    monkeypatch.setattr(service_module, "process_document", lambda **_: (_ for _ in ()).throw(RuntimeError("secret traceback bytes")))
    assert UsLaceyEngine2Service(session_factory=engine2_postgres_session_factory, vault_service=FakeVault(b"x")).resolve_operation_with_engine2(organization_id=org, operation_id=operation).status == "FAILED"
    session = tenant_session(engine2_postgres_session_factory, org); run = session.query(UsLaceyEngineDocumentRun).filter_by(operation_document_id=link).one()
    assert run.status == "FAILED" and run.resolution_json is None and run.safe_error_code and "traceback" not in run.safe_error_message.lower(); session.close()


def test_engine2_document_run_reuses_identical_success(engine2_postgres_session_factory, monkeypatch):
    org, operation, link, _, _, _ = create_test_graph(engine2_postgres_session_factory); calls = []
    monkeypatch.setattr(service_module, "process_document", lambda **_: (calls.append(1), _resolution("bill.pdf", DocumentType.BILL_OF_LADING, {"bill_of_lading": "MAEU274342495"}))[1])
    service = UsLaceyEngine2Service(session_factory=engine2_postgres_session_factory, vault_service=FakeVault(b"x")); service.resolve_operation_with_engine2(organization_id=org, operation_id=operation); service.resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    session = tenant_session(engine2_postgres_session_factory, org); assert session.query(UsLaceyEngineDocumentRun).filter_by(operation_document_id=link, status="SUCCEEDED").count() == 1 and len(calls) == 1; session.close()


def test_engine2_schema_versions_do_not_reuse_document_or_shipment_caches(engine2_postgres_session_factory, monkeypatch):
    org, operation, link, assurance, _, sha = create_test_graph(engine2_postgres_session_factory, content=b"schema")
    resolution = _resolution("bill.pdf", DocumentType.BILL_OF_LADING, {"bill_of_lading": "MAEU274342495"})
    session = tenant_session(engine2_postgres_session_factory, org)
    session.add(UsLaceyEngineDocumentRun(organization_id=org, operation_id=operation, operation_document_id=link, assurance_document_id=assurance, engine_version=service_module.ENGINE_VERSION, schema_version="lacey_document_resolution_v0", source_sha256=sha, role_hint="BILL_OF_LADING", status="SUCCEEDED", resolution_json=serialize_document_resolution(resolution)))
    old_fingerprint = source_set_fingerprint(organization_id=org, operation_id=operation, documents=[(SimpleNamespace(id=link, assurance_document_id=assurance, version_number=1), SimpleNamespace(sha256=sha))], shipment_schema_version="lacey_shipment_resolution_v0")
    session.add(UsLaceyEngineShipmentRun(organization_id=org, operation_id=operation, engine_version=service_module.ENGINE_VERSION, ruleset_version="lacey_ruleset_2026_01", schema_version="lacey_shipment_resolution_v0", source_set_fingerprint=old_fingerprint, document_count=0, readiness="BLOCKED", resolution_json={"historical": True}))
    session.commit(); session.close()
    calls = []; monkeypatch.setattr(service_module, "process_document", lambda **_: (calls.append(1), resolution)[1])
    service = UsLaceyEngine2Service(session_factory=engine2_postgres_session_factory, vault_service=FakeVault(b"schema"))
    first = service.resolve_operation_with_engine2(organization_id=org, operation_id=operation); second = service.resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    session = tenant_session(engine2_postgres_session_factory, org)
    assert first.shipment_run_id == second.shipment_run_id and len(calls) == 1
    assert session.query(UsLaceyEngineDocumentRun).filter_by(operation_document_id=link, status="SUCCEEDED").count() == 2
    assert {row.schema_version for row in session.query(UsLaceyEngineDocumentRun).filter_by(operation_document_id=link)} == {"lacey_document_resolution_v0", DOCUMENT_RESOLUTION_SCHEMA_VERSION}
    assert session.query(UsLaceyEngineShipmentRun).filter_by(organization_id=org, operation_id=operation).count() == 2
    assert _snapshot(engine2_postgres_session_factory, org, first.shipment_run_id).schema_version == SHIPMENT_RESOLUTION_SCHEMA_VERSION
    session.close()


def test_engine2_same_sha_is_never_reused_cross_tenant(engine2_postgres_session_factory, monkeypatch):
    a_org, a_operation, a_link, _, _, _ = create_test_graph(engine2_postgres_session_factory, content=b"same-sha")
    b_org, b_operation, b_link, _, _, _ = create_test_graph(engine2_postgres_session_factory, content=b"same-sha")
    monkeypatch.setattr(service_module, "process_document", lambda **_: _resolution("bill.pdf", DocumentType.BILL_OF_LADING, {"bill_of_lading": "MAEU274342495"}))
    service = UsLaceyEngine2Service(session_factory=engine2_postgres_session_factory, vault_service=FakeVault(b"same-sha"))
    service.resolve_operation_with_engine2(organization_id=a_org, operation_id=a_operation); service.resolve_operation_with_engine2(organization_id=b_org, operation_id=b_operation)
    a_session = tenant_session(engine2_postgres_session_factory, a_org); a_run = a_session.query(UsLaceyEngineDocumentRun).filter_by(operation_document_id=a_link).one(); a_session.close()
    b_session = tenant_session(engine2_postgres_session_factory, b_org); b_run = b_session.query(UsLaceyEngineDocumentRun).filter_by(operation_document_id=b_link).one(); b_session.close()
    assert (a_run.organization_id, b_run.organization_id, a_run.source_sha256) == (a_org, b_org, b_run.source_sha256) and a_run.id != b_run.id


def test_engine2_shadow_preserves_human_review_operation_status_and_legacy_rows(engine2_postgres_session_factory, monkeypatch):
    org, operation, link, assurance, _, _ = create_test_graph(engine2_postgres_session_factory)
    reviewed_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
    session = tenant_session(engine2_postgres_session_factory, org)
    user = User(organization_id=org, email=f"engine2-{org}@example.test", username=f"engine2-{org}", password_hash="test", role="auditor"); session.add(user); session.flush()
    field = UsLaceyOperationField(organization_id=org, operation_id=operation, merchandise_line_reference="1", field_name="genus", field_scope="SHIPMENT", original_value="Pinus", normalized_value="PINUS", field_status="MATCHED", confidence=1.0, source_assurance_document_id=assurance, human_value="Pinus reviewed", reviewed_by_user_id=user.id, reviewed_at=reviewed_at, validation_status="VALID")
    session.add(field); session.flush()
    selected = UsLaceyFieldCandidate(organization_id=org, operation_id=operation, operation_field_id=field.id, source_assurance_document_id=assurance, original_value="Pinus", normalized_value="PINUS", validation_status="VALID", confidence=1.0, fingerprint=hashlib.sha256(b"selected").hexdigest(), decision="SELECTED", decided_by_user_id=user.id, decided_at=reviewed_at)
    rejected = UsLaceyFieldCandidate(organization_id=org, operation_id=operation, operation_field_id=field.id, source_assurance_document_id=assurance, original_value="Picea", normalized_value="PICEA", validation_status="VALID", confidence=1.0, fingerprint=hashlib.sha256(b"rejected").hexdigest(), decision="REJECTED", decided_by_user_id=user.id, decided_at=reviewed_at)
    issue = ReconciliationIssue(organization_id=org, operation_reference=f"engine2-{operation}", fingerprint=hashlib.sha256(b"issue").hexdigest(), rule_code="TEST_REVIEW", severity="WARNING", status="RESOLVED", left_document_id=assurance, left_source="test", explanation="test", resolution_justification="Human resolved", resolved_at=reviewed_at)
    session.add_all((selected, rejected, issue)); session.query(UsLaceyOperation).filter_by(id=operation).update({"status": "READY_FOR_REVIEW", "review_result": "READY_FOR_HUMAN_CONFIRMATION"}); session.commit(); session.close()
    session = tenant_session(engine2_postgres_session_factory, org); before = (session.query(UsLaceyOperation).filter_by(id=operation).one().status, session.query(UsLaceyOperation).filter_by(id=operation).one().review_result, field.human_value, field.reviewed_by_user_id, field.reviewed_at, selected.decision, selected.decided_by_user_id, selected.decided_at, rejected.decision, rejected.decided_by_user_id, rejected.decided_at, issue.status, issue.resolution_justification, issue.resolved_at, session.query(UsLaceyOperationField).filter_by(operation_id=operation).count(), session.query(UsLaceyFieldCandidate).filter_by(operation_id=operation).count(), session.query(ReconciliationIssue).filter_by(organization_id=org).count()); session.close()
    monkeypatch.setattr(service_module, "process_document", lambda **_: _resolution("bill.pdf", DocumentType.BILL_OF_LADING, {"bill_of_lading": "MAEU274342495"}))
    result = UsLaceyEngine2Service(session_factory=engine2_postgres_session_factory, vault_service=FakeVault(b"x")).resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    session = tenant_session(engine2_postgres_session_factory, org); after_operation = session.query(UsLaceyOperation).filter_by(id=operation).one(); after_field = session.query(UsLaceyOperationField).filter_by(id=field.id).one(); after_candidates = {item.decision: item for item in session.query(UsLaceyFieldCandidate).filter_by(operation_id=operation)}; after_issue = session.query(ReconciliationIssue).filter_by(id=issue.id).one()
    after = (after_operation.status, after_operation.review_result, after_field.human_value, after_field.reviewed_by_user_id, after_field.reviewed_at, after_candidates["SELECTED"].decision, after_candidates["SELECTED"].decided_by_user_id, after_candidates["SELECTED"].decided_at, after_candidates["REJECTED"].decision, after_candidates["REJECTED"].decided_by_user_id, after_candidates["REJECTED"].decided_at, after_issue.status, after_issue.resolution_justification, after_issue.resolved_at, session.query(UsLaceyOperationField).filter_by(operation_id=operation).count(), session.query(UsLaceyFieldCandidate).filter_by(operation_id=operation).count(), session.query(ReconciliationIssue).filter_by(organization_id=org).count())
    assert before == after and session.query(UsLaceyEngineShipmentRun).filter_by(id=result.shipment_run_id).count() == 1
    session.close()


def test_engine2_shipment_snapshot_persists_and_round_trips(engine2_postgres_session_factory, monkeypatch):
    org, operation, bill, supplier, resolutions = _dossier(engine2_postgres_session_factory)
    service, processor = _service(engine2_postgres_session_factory, resolutions); monkeypatch.setattr(service_module, "process_document", processor)
    result = service.resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    row = _snapshot(engine2_postgres_session_factory, org, result.shipment_run_id); restored = deserialize_shipment_resolution(row.resolution_json)
    assert (row.organization_id, row.operation_id, row.ruleset_version, row.schema_version) == (org, operation, "lacey_ruleset_2026_01", SHIPMENT_RESOLUTION_SCHEMA_VERSION)
    assert row.engine_version and row.source_set_fingerprint and row.document_count == 2 and row.resolution_json
    assert restored.canonical_fields["master_bill_of_lading"].values[0].value == "MAEU274342495"
    assert restored.canonical_fields["container_number"].values[0].value == "MSKU9228574"
    assert restored.canonical_fields["genus"].values[0].value == "PINUS" and restored.canonical_fields["species"].values[0].value == "RADIATA" and restored.canonical_fields["country_of_harvest"].values[0].value == "CHILE"
    assert restored.issues is not None and restored.metrics["documents_processed"] == 2
    assert {item.document_id for item in restored.documents} == {str(bill[0]), str(supplier[0])}
    evidence = restored.canonical_fields["species"].supporting_evidence[0]
    assert evidence.scope.value == "PLANT_COMPONENT" and evidence.component_key == "a"


def test_engine2_shipment_snapshot_is_idempotent(engine2_postgres_session_factory, monkeypatch):
    org, operation, _, _, resolutions = _dossier(engine2_postgres_session_factory); service, processor = _service(engine2_postgres_session_factory, resolutions); monkeypatch.setattr(service_module, "process_document", processor)
    first = service.resolve_operation_with_engine2(organization_id=org, operation_id=operation); second = service.resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    one, two = _snapshot(engine2_postgres_session_factory, org, first.shipment_run_id), _snapshot(engine2_postgres_session_factory, org, second.shipment_run_id)
    session = tenant_session(engine2_postgres_session_factory, org)
    assert first.shipment_run_id == second.shipment_run_id and one.source_set_fingerprint == two.source_set_fingerprint
    assert session.query(UsLaceyEngineShipmentRun).filter_by(organization_id=org, operation_id=operation, source_set_fingerprint=one.source_set_fingerprint).count() == 1
    session.close()


def test_engine2_aggregates_only_current_document_versions(engine2_postgres_session_factory, monkeypatch):
    org, operation, _, _, resolutions = _dossier(engine2_postgres_session_factory)
    old_link, old_assurance, _, _ = add_test_document(engine2_postgres_session_factory, organization_id=org, operation_id=operation, role="COMMERCIAL_INVOICE", filename="old.pdf", content=b"old", version_number=1, is_current=False)
    new_link, new_assurance, _, new_sha = add_test_document(engine2_postgres_session_factory, organization_id=org, operation_id=operation, role="COMMERCIAL_INVOICE", filename="new.pdf", content=b"new", version_number=2, is_current=True)
    resolutions.update({"old.pdf": _resolution("old.pdf", DocumentType.COMMERCIAL_INVOICE, {"consignee_name": "OLD CONSIGNEE"}), "new.pdf": _resolution("new.pdf", DocumentType.COMMERCIAL_INVOICE, {"consignee_name": "NEW CONSIGNEE"})})
    service, processor = _service(engine2_postgres_session_factory, resolutions); monkeypatch.setattr(service_module, "process_document", processor)
    result = service.resolve_operation_with_engine2(organization_id=org, operation_id=operation); restored = deserialize_shipment_resolution(_snapshot(engine2_postgres_session_factory, org, result.shipment_run_id).resolution_json)
    assert str(new_link) in {item.document_id for item in restored.documents} and str(old_link) not in {item.document_id for item in restored.documents}
    session = tenant_session(engine2_postgres_session_factory, org); current = session.query(UsLaceyOperationDocument).filter_by(id=new_link).one(); run = session.query(UsLaceyEngineDocumentRun).filter_by(assurance_document_id=new_assurance).one()
    assert session.query(UsLaceyEngineDocumentRun).filter_by(assurance_document_id=old_assurance).count() == 0 and current.is_current and current.version_number == 2 and run.source_sha256 == new_sha; session.close()


def test_engine2_historical_snapshot_is_immutable_after_version_change(engine2_postgres_session_factory, monkeypatch):
    org, operation, bill, _, resolutions = _dossier(engine2_postgres_session_factory); service, processor = _service(engine2_postgres_session_factory, resolutions); monkeypatch.setattr(service_module, "process_document", processor)
    first = service.resolve_operation_with_engine2(organization_id=org, operation_id=operation); initial = _snapshot(engine2_postgres_session_factory, org, first.shipment_run_id); captured = (initial.id, initial.source_set_fingerprint, deepcopy(initial.resolution_json), initial.created_at)
    add_test_document(engine2_postgres_session_factory, organization_id=org, operation_id=operation, role="BILL_OF_LADING", filename="bill-v2.pdf", content=b"bill-v2", version_number=2, is_current=True)
    session = tenant_session(engine2_postgres_session_factory, org); session.query(UsLaceyOperationDocument).filter_by(id=bill[0]).update({"is_current": False}); session.commit(); session.close()
    resolutions["bill-v2.pdf"] = _resolution("bill-v2.pdf", DocumentType.BILL_OF_LADING, {"bill_of_lading": "NEW-BL", "container_number": "NEW-CN"})
    second = service.resolve_operation_with_engine2(organization_id=org, operation_id=operation); now = _snapshot(engine2_postgres_session_factory, org, captured[0]); later = _snapshot(engine2_postgres_session_factory, org, second.shipment_run_id)
    assert later.id != captured[0] and later.source_set_fingerprint != captured[1] and (now.id, now.source_set_fingerprint, now.resolution_json, now.created_at) == captured


def test_engine2_current_flag_transition_creates_new_snapshot(engine2_postgres_session_factory, monkeypatch):
    org, operation, bill, _, resolutions = _dossier(engine2_postgres_session_factory)
    alternative, _, _, _ = add_test_document(engine2_postgres_session_factory, organization_id=org, operation_id=operation, role="BILL_OF_LADING", filename="alternate.pdf", content=b"alternate", version_number=1, is_current=False)
    resolutions["alternate.pdf"] = _resolution("alternate.pdf", DocumentType.BILL_OF_LADING, {"bill_of_lading": "ALT-BL", "container_number": "ALT-CN"})
    service, processor = _service(engine2_postgres_session_factory, resolutions); monkeypatch.setattr(service_module, "process_document", processor)
    first = service.resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    session = tenant_session(engine2_postgres_session_factory, org); session.query(UsLaceyOperationDocument).filter_by(id=bill[0]).update({"is_current": False}); session.query(UsLaceyOperationDocument).filter_by(id=alternative).update({"is_current": True}); session.commit(); session.close()
    second = service.resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    assert first.shipment_run_id != second.shipment_run_id and _snapshot(engine2_postgres_session_factory, org, first.shipment_run_id).source_set_fingerprint != _snapshot(engine2_postgres_session_factory, org, second.shipment_run_id).source_set_fingerprint


def test_engine2_engine_and_ruleset_versions_invalidate_independent_caches(engine2_postgres_session_factory, monkeypatch):
    org, operation, _, _, resolutions = _dossier(engine2_postgres_session_factory); monkeypatch.setattr(service_module, "process_document", lambda **values: resolutions[values["filename"]])
    first = UsLaceyEngine2Service(session_factory=engine2_postgres_session_factory, vault_service=FakeVault(b"x"), engine_version="v1").resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    version_two = UsLaceyEngine2Service(session_factory=engine2_postgres_session_factory, vault_service=FakeVault(b"x"), engine_version="v2").resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    ruleset_two = UsLaceyEngine2Service(session_factory=engine2_postgres_session_factory, vault_service=FakeVault(b"x"), engine_version="v2", ruleset=LaceyRuleset(version="rules-v2")).resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    a, b, c = (_snapshot(engine2_postgres_session_factory, org, item.shipment_run_id) for item in (first, version_two, ruleset_two)); session = tenant_session(engine2_postgres_session_factory, org)
    assert len({a.id, b.id, c.id}) == 3 and len({a.source_set_fingerprint, b.source_set_fingerprint, c.source_set_fingerprint}) == 3
    assert session.query(UsLaceyEngineDocumentRun).filter_by(organization_id=org).count() == 4 and session.query(UsLaceyEngineDocumentRun).filter_by(engine_version="v2").count() == 2; session.close()


def test_engine2_failed_current_document_blocks_shipment_and_retry_preserves_history(engine2_postgres_session_factory, monkeypatch):
    org, operation, _, supplier, resolutions = _dossier(engine2_postgres_session_factory); calls = {"supplier.pdf": 0}
    def process(**values):
        if values["filename"] == "supplier.pdf" and calls["supplier.pdf"] == 0: calls["supplier.pdf"] += 1; raise RuntimeError("temporary parser failure")
        return resolutions[values["filename"]]
    monkeypatch.setattr(service_module, "process_document", process); service = UsLaceyEngine2Service(session_factory=engine2_postgres_session_factory, vault_service=FakeVault(b"x"))
    assert service.resolve_operation_with_engine2(organization_id=org, operation_id=operation).status == "FAILED"
    session = tenant_session(engine2_postgres_session_factory, org); assert session.query(UsLaceyEngineShipmentRun).filter_by(organization_id=org, operation_id=operation).count() == 0 and session.query(UsLaceyEngineDocumentRun).filter_by(operation_document_id=supplier[0], status="FAILED").count() == 1; session.close()
    recovered = service.resolve_operation_with_engine2(organization_id=org, operation_id=operation); service.resolve_operation_with_engine2(organization_id=org, operation_id=operation); session = tenant_session(engine2_postgres_session_factory, org)
    assert recovered.status == "SUCCEEDED" and session.query(UsLaceyEngineDocumentRun).filter_by(operation_document_id=supplier[0], status="FAILED").count() == 1 and session.query(UsLaceyEngineDocumentRun).filter_by(operation_document_id=supplier[0], status="SUCCEEDED").count() == 1 and session.query(UsLaceyEngineShipmentRun).filter_by(id=recovered.shipment_run_id).one().document_count == 2; session.close()


def test_engine2_duplicate_snapshot_constraint_is_database_safe(engine2_postgres_session_factory, monkeypatch):
    org, operation, _, _, resolutions = _dossier(engine2_postgres_session_factory); service, processor = _service(engine2_postgres_session_factory, resolutions); monkeypatch.setattr(service_module, "process_document", processor)
    result = service.resolve_operation_with_engine2(organization_id=org, operation_id=operation); row = _snapshot(engine2_postgres_session_factory, org, result.shipment_run_id); session = tenant_session(engine2_postgres_session_factory, org)
    session.add(UsLaceyEngineShipmentRun(organization_id=org, operation_id=operation, engine_version=row.engine_version, ruleset_version=row.ruleset_version, schema_version=row.schema_version, source_set_fingerprint=row.source_set_fingerprint, document_count=row.document_count, readiness=row.readiness, resolution_json=row.resolution_json))
    with pytest.raises(IntegrityError): session.commit()
    session.rollback(); session.close(); session = tenant_session(engine2_postgres_session_factory, org); assert session.query(UsLaceyEngineShipmentRun).filter_by(source_set_fingerprint=row.source_set_fingerprint).count() == 1; session.close()


def test_engine2_snapshot_commit_failure_rolls_back_partial_snapshot(engine2_postgres_session_factory, monkeypatch):
    org, operation, _, _, resolutions = _dossier(engine2_postgres_session_factory); service, processor = _service(engine2_postgres_session_factory, resolutions); monkeypatch.setattr(service_module, "process_document", processor)
    def fail_commit(_self): raise RuntimeError("forced shipment commit failure")
    monkeypatch.setattr(Session, "commit", fail_commit)
    with pytest.raises(RuntimeError): service.resolve_operation_with_engine2(organization_id=org, operation_id=operation)
    session = tenant_session(engine2_postgres_session_factory, org); assert session.query(UsLaceyEngineShipmentRun).filter_by(organization_id=org, operation_id=operation).count() == 0; session.close()
