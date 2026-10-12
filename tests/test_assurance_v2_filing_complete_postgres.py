"""Isolated PDF + portal + worker + human decisions -> authorized final dossier.

Never treats shipment/client references as customs entry numbers. The seventh
original Pack 1 documents cannot supply that required identifier. An eighth,
visibly test-only, fabricated *fixture* document is generated here to prove
the complete mechanism, never to impersonate real customer or CBP evidence.
"""
from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from io import BytesIO
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from openpyxl import load_workbook
from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError
from litoral_trace.db.models import AssuranceDocument, VaultDocument
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.assurance_v2_issuance import issue_locked_assurance_package
import litoral_trace.us_lacey.assurance_v2_issuance as issuance_module

import litoral_trace.us_lacey.ingestion as ingestion_module
import litoral_trace.us_lacey.worker as worker_module
from litoral_trace.us_lacey.assurance_v2_case_reader import read_tenant_assurance_case
from litoral_trace.us_lacey.csrf import us_lacey_csrf_token
from litoral_trace.us_lacey.db import reset_us_lacey_engine_state
from litoral_trace.us_lacey.exporters.assurance_v2 import evaluate_filing_gate
from litoral_trace.us_lacey.worker import process_one_us_lacey_job
from litoral_trace.us_lacey.worker_db import reset_us_lacey_worker_engine_state
from litoral_trace.web import us_lacey_pilot_app as portal_module
from litoral_trace.web.us_lacey_assurance_v2_runtime import configure_assurance_v2_shadow

from test_assurance_v2_golden_postgres import _upload_pack
from test_us_lacey_active_portal_postgres_e2e import (
    MemoryObjectStorage, _activate_account, _configure, _csrf_for,
    _organization_id_for_email,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_DATABASE_URL")
    or not os.environ.get("US_LACEY_WORKER_DATABASE_URL"),
    reason="requires isolated PostgreSQL runtime and worker roles",
)

SYNTHETIC_ENTRY_REFERENCE = "999-9999999-9"


def _pdf_fixture(*lines: str) -> bytes:
    """Small deterministic text-only PDF, no external generator dependency."""
    assert all(line.isascii() and 0 < len(line) < 105 for line in lines)
    drawing = ["BT", "/F1 11 Tf", "45 745 Td", "15 TL"]
    for index, line in enumerate(lines):
        escaped = line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        if index:
            drawing.append("T*")
        drawing.append(f"({escaped}) Tj")
    drawing.append("ET")
    stream = ("\n".join(drawing) + "\n").encode("ascii")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        (b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
         b"/Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>"),
        b"<< /Length " + str(len(stream)).encode("ascii")
        + b" >>\nstream\n" + stream + b"endstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    result = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for number, body in enumerate(objects, 1):
        offsets.append(len(result))
        result.extend(f"{number} 0 obj\n".encode("ascii") + body + b"\nendobj\n")
    index = len(result)
    result.extend(f"xref\n0 {len(offsets)}\n0000000000 65535 f \n".encode("ascii"))
    for offset in offsets[1:]:
        result.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    result.extend(
        f"trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\n"
        f"startxref\n{index}\n%%EOF\n".encode("ascii")
    )
    return bytes(result)


def _test_only_entry_source() -> bytes:
    return _pdf_fixture(
        "LITORAL TRACE - ARTIFICIAL ACCEPTANCE FIXTURE",
        "SYNTHETIC TEST DATA - NOT VALID FOR CUSTOMS OR FILING",
        "THIS IS NOT A CUSTOMS ENTRY OR A CONFIRMATION BY CBP",
        "Test worksheet for the isolated Assurance V2 software acceptance",
        "Entry number 999-9999999-9",
        "Customer shipment reference HHI-IMP-2026-1047",
        "Invoice LSBH-CI-261001",
        "B/L HBL-XMN-LAX-261012-041",
        "One fictional test-only entry number, not a real declaration",
    )


def _csrf(client, op_id):
    return us_lacey_csrf_token(
        session_token=client.cookies.get("us_lacey_session"),
        purpose=f"assurance-v2:{op_id}",
    )


def test_portal_to_final_authorized_assurance_issuance_with_separate_test_only_entry(monkeypatch):
    _configure(monkeypatch)
    monkeypatch.setenv("US_LACEY_ENGINE2_MODE", "SHADOW")
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_MODE", "SHADOW")
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_REVIEW_JOURNAL", "ON")
    reset_us_lacey_engine_state()
    reset_us_lacey_worker_engine_state()
    storage = MemoryObjectStorage()
    monkeypatch.setattr(ingestion_module, "get_us_lacey_storage_client", lambda: storage)
    monkeypatch.setattr(worker_module, "get_us_lacey_storage_client", lambda: storage)
    delivered = {}
    monkeypatch.setattr(portal_module, "send_us_lacey_verification_email",
                        lambda **kwargs: delivered.update(kwargs))
    suffix = uuid4().hex[:12]
    email = f"v2-filing-{suffix}@example.com"
    password = f"Test-{uuid4().hex}-Aa1!"
    configure_assurance_v2_shadow(portal_module.app, mode="SHADOW", enable_commands=True)

    with TestClient(portal_module.app, follow_redirects=False) as client:
        signup = client.post("/signup", data={
            "legal_name": f"Fictional V2 Filer {suffix}", "business_type": "IMPORTER",
            "admin_name": "Synthetic Reviewer", "admin_email": email,
            "password": password, "accept_terms": "yes", "accept_privacy": "yes",
            "accept_beta": "yes",
        })
        assert signup.status_code == 201, signup.text
        assert client.get(f"/verify-email?token={delivered['verification_token']}").status_code == 303
        org_id = _organization_id_for_email(email)
        _activate_account(org_id)
        assert client.post("/login", data={"email": email, "password": password}).status_code == 303
        create = client.get("/operations/new")
        operation = client.post("/operations/new", data={
            "csrf_token": _csrf_for(create.text, "/operations/new"),
            "client_reference": f"SYNTHETIC-FILING-{suffix}",
        })
        assert operation.status_code == 303, operation.text
        operation_path = operation.headers["location"]
        op_id = operation_path.rsplit("/", 1)[-1]
        _upload_pack(client, operation_path, "pack1")
        extra_pdf = _test_only_entry_source()
        assert extra_pdf.startswith(b"%PDF")
        detail = client.get(operation_path)
        added = client.post(
            f"{operation_path}/upload",
            data={
                "csrf_token": _csrf_for(detail.text, f"{operation_path}/upload"),
                "document_role": "OTHER",
            },
            files={"document": (
                "SYNTHETIC_ENTRY_NUMBER_CONFIRMATION_NOT_FOR_FILING.pdf",
                extra_pdf, "application/pdf",
            )},
        )
        assert added.status_code == 303, added.text
        for index in range(8):
            result = process_one_us_lacey_job(worker_id=f"v2-final-{suffix}-{index}")
            assert result.claimed and result.job_status == "COMPLETED", result
        case = read_tenant_assurance_case(organization_id=org_id, operation_public_id=op_id)
        assert case is not None and len(case.documents) == 8
        assert not evaluate_filing_gate(case).ready
        assert any(
            item["field_name"] == "filing_entry_reference"
            and item["normalized_value"] == "99999999999"
            and item["support_status"] == "SUPPORTED"
            and item["authority_status"] == "CANDIDATE"
            for item in case.candidate_observations
        )
        assert any(
            item["field_name"] == "merchandise_description"
            and item["normalized_value"] == "retail boxed 4 piece solid bamboo coaster sets"
            and item["support_status"] == "SUPPORTED"
            for item in case.candidate_observations
        )

        # Regression: Pack 1 has B/L ...047, while the artificial entry
        # document contains ...041. A reviewer choosing the explicitly
        # sourced ...047 candidate through the legacy workspace must be
        # journaled with the candidate's CURRENT document and locator.
        # The runtime session has autoflush=False: without flushing the
        # selected source fields BEFORE V2 journaling this returns a generic
        # review error and leaves the field stuck in REVIEW.
        session = get_us_lacey_db_session()
        try:
            set_tenant_db_context(session, org_id)
            candidate_row = session.execute(text("""
                SELECT c.id AS candidate_id, f.id AS field_id
                FROM public.us_lacey_field_candidates c
                JOIN public.us_lacey_operation_fields f
                  ON f.id = c.operation_field_id
                 AND f.organization_id = c.organization_id
                JOIN public.us_lacey_operations o
                  ON o.id = f.operation_id
                 AND o.organization_id = f.organization_id
                WHERE o.public_id = CAST(:op_id AS uuid)
                  AND o.organization_id = :organization_id
                  AND f.field_name = 'bill_of_lading'
                  AND c.normalized_value = 'HBL-XMN-LAX-261012-047'
                ORDER BY c.id
                LIMIT 1
            """), {"op_id": op_id, "organization_id": org_id}).mappings().one()
            bol_field_id = int(candidate_row["field_id"])
            bol_candidate_id = int(candidate_row["candidate_id"])
        finally:
            session.close()
        original_review = client.get(operation_path)
        post_action = f"{operation_path}/review/fields/{bol_field_id}"
        accepted = client.post(post_action, data={
            "csrf_token": _csrf_for(original_review.text, post_action),
            "action": "accept", "candidate_id": str(bol_candidate_id),
        })
        assert accepted.status_code == 303, accepted.text
        session = get_us_lacey_db_session()
        try:
            set_tenant_db_context(session, org_id)
            reviewed_source = session.execute(text("""
                SELECT human_value, field_status, source_assurance_document_id,
                       source_locator
                FROM public.us_lacey_operation_fields
                WHERE organization_id = :organization_id AND id = :field_id
            """), {"organization_id": org_id, "field_id": bol_field_id}).mappings().one()
            assert reviewed_source["human_value"] == 'HBL-XMN-LAX-261012-047'
            assert reviewed_source["field_status"] == "MATCHED"
            assert reviewed_source["source_assurance_document_id"] is not None
            assert reviewed_source["source_locator"]
        finally:
            session.close()

        if case.identities and case.identities[0]["status"] == "REVIEW_REQUIRED":
            identity = case.identities[0]
            confirmed = client.post(
                f"/assurance-v2/operations/{op_id}/identities",
                json={
                    "csrf_token": _csrf(client, op_id),
                    "line_reference": identity["line_reference"],
                    "supplier_id": identity["supplier_id"],
                    "product_id": identity["product_id"],
                    "reason": "Verified original invoice SKU and exact manufacturer identity",
                },
            )
            assert confirmed.status_code == 202, confirmed.text

        # Every nonempty optional/required field must acquire documented human
        # authority. Merely extracting it never lets the export gate pass.
        reviewed_names = set()
        for index in range(24):
            current = read_tenant_assurance_case(
                organization_id=org_id, operation_public_id=op_id,
            )
            pending_fields = [
                ex for ex in current.exceptions
                if str(ex.get("id", "")).startswith("field:")
            ]
            if not pending_fields:
                break
            issue = pending_fields[0]
            name = issue["field_name"]
            field = next(f for f in current.fields if str(f"field:{f['id']}") == issue["id"])
            supported_sources = [
                candidate for candidate in issue.get("candidates") or ()
                if str(candidate.get("id", "")).startswith("source:")
            ]
            if name in {"filing_entry_reference", "merchandise_description"}:
                assert len(supported_sources) == 1, (name, issue)
                selected = supported_sources[0]["id"]
            elif field.get("value"):
                selected = f"legacy:{field['id']}"
            elif supported_sources:
                selected = supported_sources[0]["id"]
            else:
                pytest.fail(f"Missing original evidence for required field: {name}")
            decision = client.post(
                f"/assurance-v2/operations/{op_id}/decisions",
                json={
                    "csrf_token": _csrf(client, op_id),
                    "exception_id": issue["id"], "action": "ACCEPT",
                    "candidate_id": selected,
                    "reason": "Reviewed exact synthetic source and current shipment context",
                },
            )
            assert decision.status_code == 202, (name, decision.text)
            reviewed_names.add(name)
        else:
            pytest.fail("Human review queue failed to reach completion")

        completed = read_tenant_assurance_case(
            organization_id=org_id, operation_public_id=op_id,
        )
        assert not completed.exceptions, completed.exceptions
        assert {"filing_entry_reference", "merchandise_description"} <= reviewed_names
        assert any(f["name"] == "filing_entry_reference"
                   and f["value"] == SYNTHETIC_ENTRY_REFERENCE
                   and f["authority"] == "HUMAN_CONFIRMED"
                   for f in completed.fields)
        # Conditional percent_recycled was NOT_REQUIRED. Its unverified
        # legacy zero must not silently become an exported claim.
        assert any(
            f["name"] == "percent_recycled"
            and f["status"] == "NOT_REQUIRED"
            and not str(f.get("value") or "").strip()
            for f in completed.fields
        )
        gate = evaluate_filing_gate(completed)
        assert {b.code for b in gate.blockers} == {"EXPORT_NOT_AUTHORIZED"}, gate.blockers
        authorized = client.post(
            f"/assurance-v2/operations/{op_id}/authorize-export",
            json={
                "csrf_token": _csrf(client, op_id),
                "reason": "Reviewed all fictional fixture documents for software acceptance only",
                "explicit_confirmation": True,
            },
        )
        assert authorized.status_code == 200, authorized.text
        approved = read_tenant_assurance_case(
            organization_id=org_id, operation_public_id=op_id,
        )
        assert evaluate_filing_gate(approved).ready

        # A previously authorized dossier MUST NOT issue while any original
        # PDF is unavailable. The state transition is in this isolated test
        # database only; the exact original source is restored before
        # exercising successful emission below.
        test_source_doc_id = int(next(
            doc["id"] for doc in approved.documents
            if doc["name"].startswith("SYNTHETIC_ENTRY_NUMBER_CONFIRMATION")
        ))
        def _set_source_status(value: str) -> None:
            session = get_us_lacey_db_session()
            try:
                set_tenant_db_context(session, org_id)
                source = session.scalar(select(AssuranceDocument).where(
                    AssuranceDocument.organization_id == org_id,
                    AssuranceDocument.id == test_source_doc_id,
                ))
                assert source is not None
                vault = session.scalar(select(VaultDocument).where(
                    VaultDocument.organization_id == org_id,
                    VaultDocument.id == source.vault_document_id,
                ))
                assert vault is not None
                vault.status = value
                session.commit()
            finally:
                session.close()

        _set_source_status("delete_pending")
        try:
            forbidden = client.post(
                f"/assurance-v2/operations/{op_id}/issue",
                json={"csrf_token": _csrf(client, op_id)},
            )
            assert forbidden.status_code == 409, forbidden.text
        finally:
            _set_source_status("available")
        assert evaluate_filing_gate(read_tenant_assurance_case(
            organization_id=org_id, operation_public_id=op_id,
        )).ready

        # A forged/replaced source checksum must invalidate current review
        # authority and forbid issuance, even if the human export approval
        # still exists in the append-only operation audit history.
        original_hash = next(
            doc["sha256"] for doc in approved.documents
            if int(doc["id"]) == test_source_doc_id
        )
        def _set_source_hash(checksum: str) -> None:
            session = get_us_lacey_db_session()
            try:
                set_tenant_db_context(session, org_id)
                source = session.scalar(select(AssuranceDocument).where(
                    AssuranceDocument.organization_id == org_id,
                    AssuranceDocument.id == test_source_doc_id,
                ))
                assert source is not None
                vault = session.scalar(select(VaultDocument).where(
                    VaultDocument.organization_id == org_id,
                    VaultDocument.id == source.vault_document_id,
                ))
                assert vault is not None
                vault.sha256 = checksum
                session.commit()
            finally:
                session.close()

        _set_source_hash("f" * 64 if original_hash != "f" * 64 else "e" * 64)
        try:
            replaced_case = read_tenant_assurance_case(
                organization_id=org_id, operation_public_id=op_id,
            )
            assert replaced_case is not None
            assert not evaluate_filing_gate(replaced_case).ready
            forbidden = client.post(
                f"/assurance-v2/operations/{op_id}/issue",
                json={"csrf_token": _csrf(client, op_id)},
            )
            assert forbidden.status_code == 409, forbidden.text
        finally:
            _set_source_hash(original_hash)
        assert evaluate_filing_gate(read_tenant_assurance_case(
            organization_id=org_id, operation_public_id=op_id,
        )).ready

        # True concurrent source-document mutation: the real issuer owns and
        # locks all eight physical PDF dependencies. Hold that transaction
        # during actual package construction, then attempt to withdraw the
        # eighth PDF from a second tenant-scoped PostgreSQL connection.
        # The mutation must TIME OUT rather than winning against a package
        # whose approved case was reviewed before the source changed.
        entered_builder, release_builder = Event(), Event()
        original_builder = issuance_module.issue_assurance_package
        def paused_builder(case):
            entered_builder.set()
            if not release_builder.wait(timeout=25):
                raise AssertionError("Test failed to release package builder")
            return original_builder(case)

        with monkeypatch.context() as concurrent_patch:
            concurrent_patch.setattr(
                issuance_module, "issue_assurance_package", paused_builder,
            )
            with ThreadPoolExecutor(max_workers=1) as executor:
                reviewer_id = int(approved.export_authorization["actor_id"].split(":")[1])
                issue_future = executor.submit(
                    issue_locked_assurance_package,
                    organization_id=org_id, operation_public_id=op_id,
                    authenticated_user_id=reviewer_id,
                    expected_source_set_fingerprint=approved.source_set_fingerprint,
                    expected_case_fingerprint=approved.authority_fingerprint(),
                )
                mutation_session = None
                try:
                    assert entered_builder.wait(timeout=20), "Issuer did not lock physical documents"
                    mutation_session = get_us_lacey_db_session()
                    set_tenant_db_context(mutation_session, org_id)
                    mutation_session.execute(text("SET LOCAL lock_timeout = '300ms'"))
                    source = mutation_session.scalar(select(AssuranceDocument).where(
                        AssuranceDocument.organization_id == org_id,
                        AssuranceDocument.id == test_source_doc_id,
                    ))
                    assert source is not None
                    with pytest.raises(OperationalError) as timeout_error:
                        mutation_session.execute(
                            text("UPDATE vault_documents SET status = 'delete_pending' "
                                 "WHERE organization_id=:org AND id=:vault"),
                            {"org": org_id, "vault": source.vault_document_id},
                        )
                    assert "lock" in str(timeout_error.value).lower(), timeout_error.value
                finally:
                    if mutation_session is not None:
                        mutation_session.rollback()
                        mutation_session.close()
                    release_builder.set()
                concurrent_receipt = issue_future.result(timeout=30)
        assert concurrent_receipt["status"] == "FILING_READY"
        assert concurrent_receipt["submitted_to_agency"] is False

        issue = client.post(
            f"/assurance-v2/operations/{op_id}/issue",
            json={"csrf_token": _csrf(client, op_id)},
        )
        assert issue.status_code == 200, issue.text
        receipt = issue.json()
        assert receipt["status"] == "FILING_READY"
        assert receipt["submitted_to_agency"] is False
        assert receipt["package_fingerprint"] == concurrent_receipt["package_fingerprint"]
        fingerprint = receipt["package_fingerprint"]
        artifact_url = f"/assurance-v2/operations/{op_id}/packages/{fingerprint}"
        manifest = client.get(f"{artifact_url}/manifest")
        assert manifest.status_code == 200
        assert manifest.json()["source_set_fingerprint"] == approved.source_set_fingerprint
        assert manifest.json()["case"]["export_authorization"]["status"] == "AUTHORIZED"

        xml = client.get(f"{artifact_url}/lawgs_xml")
        excel = client.get(f"{artifact_url}/lacey_excel")
        ppq = client.get(f"{artifact_url}/ppq505_preparation_json")
        assert xml.status_code == excel.status_code == ppq.status_code == 200
        assert b"<ns1:genus>Phyllostachys</ns1:genus>" in xml.content
        assert b"<ns1:quantityMaterial>384</ns1:quantityMaterial>" in xml.content
        assert manifest.json()["authorized_export"]["header"]["entry_number"] == SYNTHETIC_ENTRY_REFERENCE
        workbook = load_workbook(BytesIO(excel.content), read_only=True)
        assert workbook.active is not None
        rows = json.loads(ppq.content)["fields"]
        assert any(r["key"] == "filing_entry_reference" and r["value"] == SYNTHETIC_ENTRY_REFERENCE
                   for r in rows)
        assert any(r["key"] == "merchandise_description" and "bamboo" in r["value"].lower()
                   for r in rows)
        duplicate = client.post(
            f"/assurance-v2/operations/{op_id}/issue",
            json={"csrf_token": _csrf(client, op_id)},
        )
        assert duplicate.status_code == 200, duplicate.text
        assert duplicate.json()["package_fingerprint"] == fingerprint
        assert client.get(f"{artifact_url}/manifest").status_code == 200

        # An immutable archived audit event does not imply current authority.
        # After the original PDF is withdrawn, no historical package may be
        # downloaded with a misleading FILING_READY response.
        _set_source_status("delete_pending")
        try:
            assert client.get(f"{artifact_url}/manifest").status_code == 409
            assert client.get(f"{artifact_url}/lawgs_xml").status_code == 409
            assert client.get(f"{artifact_url}/lacey_excel").status_code == 409
            assert client.get(f"{artifact_url}/ppq505_preparation_json").status_code == 409
        finally:
            _set_source_status("available")
        assert client.get(f"{artifact_url}/manifest").status_code == 200
