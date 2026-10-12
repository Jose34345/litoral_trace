"""Original synthetic PDF packs through portal upload, Vault and real Pg worker."""
from __future__ import annotations

import os
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

import litoral_trace.us_lacey.ingestion as ingestion_module
import litoral_trace.us_lacey.worker as worker_module
from litoral_trace.us_lacey.assurance_v2_case_reader import read_tenant_assurance_case
from litoral_trace.us_lacey.assurance_v2_memory import revoke_verified_memory
from litoral_trace.us_lacey.csrf import us_lacey_csrf_token
from litoral_trace.db.tenant import set_tenant_db_context
from sqlalchemy import select
from litoral_trace.db.models import UsLaceyOperation, UsLaceyOperationField
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.exporters.assurance_v2 import evaluate_filing_gate
from litoral_trace.us_lacey.db import reset_us_lacey_engine_state
from litoral_trace.us_lacey.worker import process_one_us_lacey_job
from litoral_trace.us_lacey.worker_db import reset_us_lacey_worker_engine_state
from litoral_trace.web import us_lacey_pilot_app as portal_module
from litoral_trace.web.us_lacey_assurance_v2_runtime import configure_assurance_v2_shadow
from test_assurance_v2_real_pdf_packs import _MANIFEST, _ROOT
from test_us_lacey_active_portal_postgres_e2e import (
    MemoryObjectStorage, _activate_account, _configure, _csrf_for,
    _organization_id_for_email,
)


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_DATABASE_URL")
    or not os.environ.get("US_LACEY_WORKER_DATABASE_URL"),
    reason="requires disposable PostgreSQL runtime and worker roles",
)


def _upload_pack(client: TestClient, operation_path: str, pack: str) -> None:
    files = sorted((_ROOT / pack).glob("*.pdf"))
    assert len(files) == (7 if pack == "pack1" else 6)
    for path in files:
        content = path.read_bytes()
        from hashlib import sha256
        assert content.startswith(b"%PDF")
        assert sha256(content).hexdigest() == _MANIFEST[pack][path.name]
        action = f"{operation_path}/upload"
        detail = client.get(operation_path)
        assert detail.status_code == 200
        role = (
            "COMMERCIAL_INVOICE" if "Invoice" in path.name else
            "PACKING_LIST" if "Packing" in path.name else
            "BILL_OF_LADING" if "Lading" in path.name else
            "SUPPLIER_DECLARATION" if "Botanical_Declaration" in path.name else "OTHER"
        )
        uploaded = client.post(action, data={
            "csrf_token": _csrf_for(detail.text, action),
            "document_role": role,
        }, files={"document": (path.name, content, "application/pdf")})
        assert uploaded.status_code == 303, uploaded.text


def test_four_original_pdf_shipments_http_worker_review_and_controlled_reuse(monkeypatch):
    _configure(monkeypatch)
    monkeypatch.setenv("US_LACEY_MONTHLY_OPERATION_LIMIT", "10")
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
                        lambda **kw: delivered.update(kw))
    suffix = uuid4().hex[:12]
    email = f"v2-golden-{suffix}@example.com"
    password = f"Test-{uuid4().hex}-Aa1!"
    configure_assurance_v2_shadow(portal_module.app, mode="SHADOW", enable_commands=True)
    with TestClient(portal_module.app, follow_redirects=False) as client:
        signup = client.post("/signup", data={
            "legal_name": f"V2 Golden {suffix} LLC", "business_type": "IMPORTER",
            "admin_name": "Golden Reviewer", "admin_email": email,
            "password": password, "accept_terms": "yes", "accept_privacy": "yes",
            "accept_beta": "yes",
        })
        assert signup.status_code == 201, signup.text
        assert client.get(f"/verify-email?token={delivered['verification_token']}").status_code == 303
        organization_id = _organization_id_for_email(email)
        # Exercise the owner-only lab path under real worker ordering.
        monkeypatch.setenv("US_LACEY_ASSURANCE_V2_LAB_ORG_IDS", str(organization_id))
        _activate_account(organization_id)
        assert client.post("/login", data={"email": email, "password": password}).status_code == 303
        create = client.get("/operations/new")
        operation = client.post("/operations/new", data={
            "csrf_token": _csrf_for(create.text, "/operations/new"),
            "client_reference": f"V2-GOLDEN-{suffix}",
        })
        assert operation.status_code == 303, operation.text
        operation_path = operation.headers["location"]
        operation_id = operation_path.rsplit("/", 1)[-1]
        _upload_pack(client, operation_path, "pack1")
        for index in range(7):
            result = process_one_us_lacey_job(worker_id=f"v2-golden-{suffix}-{index}")
            assert result.claimed and result.job_status == "COMPLETED", result
        case = read_tenant_assurance_case(
            organization_id=organization_id, operation_public_id=operation_id,
        )
        assert case is not None
        assert len(case.documents) == 7
        assert len(case.candidate_observations) >= 30
        values = {(item["field_name"], item["normalized_value"])
                  for item in case.candidate_observations
                  if item["support_status"] == "SUPPORTED"}
        assert ("genus", "phyllostachys") in values
        assert ("species", "edulis") in values
        assert ("country_of_harvest", "china") in values
        response = client.get(f"/assurance-v2/operations/{operation_id}")
        assert response.status_code == 200
        assert "SOURCE SUPPORTED" in response.text
        csrf = us_lacey_csrf_token(
            session_token=client.cookies.get("us_lacey_session"),
            purpose=f"assurance-v2:{operation_id}",
        )
        for name in ("genus", "species", "country_of_harvest"):
            current = read_tenant_assurance_case(
                organization_id=organization_id, operation_public_id=operation_id,
            )
            field = next(item for item in current.fields if item["name"] == name)
            reviewed = client.post(
                f"/assurance-v2/operations/{operation_id}/decisions",
                json={"csrf_token": csrf, "exception_id": f"field:{field['id']}",
                      "action": "ACCEPT", "candidate_id": f"legacy:{field['id']}",
                      "reason": "Reviewed original synthetic PDF and source locator"},
            )
            assert reviewed.status_code == 202, (name, reviewed.text)
        reviewed_case = read_tenant_assurance_case(
            organization_id=organization_id, operation_public_id=operation_id,
        )
        assert {field["name"] for field in reviewed_case.fields
                if field["authority"] == "HUMAN_CONFIRMED"} >= {
                    "genus", "species", "country_of_harvest",
                }
        create = client.get("/operations/new")
        assert create.status_code == 200, create.text
        second = client.post("/operations/new", data={
            "csrf_token": _csrf_for(create.text, "/operations/new"),
            "client_reference": f"V2-GOLDEN-SECOND-{suffix}",
        })
        assert second.status_code == 303, second.text
        second_path = second.headers["location"]
        second_id = second_path.rsplit("/", 1)[-1]
        _upload_pack(client, second_path, "pack2")
        for index in range(6):
            result = process_one_us_lacey_job(worker_id=f"v2-golden-{suffix}-second-{index}")
            assert result.claimed and result.job_status == "COMPLETED", result
        second_case = read_tenant_assurance_case(
            organization_id=organization_id, operation_public_id=second_id,
        )
        assert second_case is not None
        assert len(second_case.documents) == 6
        # Real worker: canonical truth -> identity link -> exact-source REVIEW.
        db = get_us_lacey_db_session()
        try:
            set_tenant_db_context(db, organization_id)
            op = db.scalar(select(UsLaceyOperation).where(
                UsLaceyOperation.organization_id == organization_id,
                UsLaceyOperation.public_id == UUID(second_id),
            ))
            recovered = db.scalars(select(UsLaceyOperationField).where(
                UsLaceyOperationField.organization_id == organization_id,
                UsLaceyOperationField.operation_id == op.id,
                UsLaceyOperationField.merchandise_line_reference == "1",
                UsLaceyOperationField.field_name.in_((
                    "entered_value", "article_component",
                    "plant_quantity", "metric_unit",
                )),
            )).all()
            assert {row.field_name for row in recovered} == {
                "entered_value", "article_component",
                "plant_quantity", "metric_unit",
            }
            assert all(
                row.field_status == "REVIEW"
                and row.validation_status == "REVIEW_REQUIRED"
                and row.normalized_value
                and row.source_assurance_document_id is not None
                and row.human_value is None
                and row.reviewed_at is None
                for row in recovered
            ), [(row.field_name, row.field_status, row.normalized_value)
                for row in recovered]
        finally:
            db.close()
        botanical = {"genus", "species", "country_of_harvest"}
        assert not {(item["field_name"], item["normalized_value"])
                    for item in second_case.candidate_observations
                    if item["field_name"] in botanical
                    and item["support_status"] == "SUPPORTED"}
        assert {item["field_name"] for item in second_case.reuses
                if item["status"] == "ELIGIBLE"} == botanical
        # Article/component has an exact documentary line/SKU context.
        # It is still source-supported candidate data, not human authority.
        assert any(
            item["field_name"] == "article_component"
            and item["association_status"] == "DOCUMENT_EXACT_SINGLE_SKU"
            and item["sku"] == "BAM-COAST-04"
            and item["line_reference"] == "1"
            and item["authority_status"] == "CANDIDATE"
            for item in second_case.candidate_observations
        )
        # Source evidence is shown once on the actionable field, not as a
        # second artificial BLOCKING_EXCEPTION/UNDECIDED_CONFLICT.
        article_items = [
            item for item in second_case.exceptions
            if item.get("field_name") == "article_component"
        ]
        assert len(article_items) == 1
        assert article_items[0]["id"].startswith("field:")
        assert len(article_items[0]["pending_candidates"]) == 1
        assert not article_items[0]["candidates"] or all(
            not str(c["id"]).startswith("source:")
            for c in article_items[0]["candidates"]
        )
        assert len(second_case.identities) == 1
        identity = second_case.identities[0]
        if identity["status"] == "REVIEW_REQUIRED":
            confirmed = client.post(
                f"/assurance-v2/operations/{second_id}/identities",
                json={"csrf_token": us_lacey_csrf_token(
                          session_token=client.cookies.get("us_lacey_session"),
                          purpose=f"assurance-v2:{second_id}"),
                      "line_reference": identity["line_reference"],
                      "supplier_id": identity["supplier_id"],
                      "product_id": identity["product_id"],
                      "reason": "Reviewed exact supplier SKU and shipment line"},
            )
            assert confirmed.status_code == 202, confirmed.text
        # Product Simplification acceptance: one reviewed action for the three
        # botanical claims, each independently source-fenced and audit-linked.
        grouped = client.post(
            f"/assurance-v2/operations/{second_id}/reuse-group",
            json={
                "csrf_token": us_lacey_csrf_token(
                    session_token=client.cookies.get("us_lacey_session"),
                    purpose=f"assurance-v2:{second_id}",
                ),
                "reason": "Reviewed original supplier evidence for all three botanical facts",
                "explicit_confirmation": True,
            },
        )
        assert grouped.status_code == 202, grouped.text
        assert grouped.json()["status"] == "VERIFIED_REUSE_BATCH"
        assert grouped.json()["field_count"] == 3
        applied_case = read_tenant_assurance_case(
            organization_id=organization_id, operation_public_id=second_id,
        )
        assert {field["name"] for field in applied_case.fields
                if field["authority"] == "VERIFIED_REUSE"} == botanical
        assert all(not field["document_ids"] for field in applied_case.fields
                   if field["authority"] == "VERIFIED_REUSE")
        # One final human action accepts every eligible literal PDF claim
        # with an individual V2 decision/provenance record in one transaction.
        from litoral_trace.us_lacey.assurance_v2_review_policy import (
            batch_candidates, simplified_review,
        )
        ready_for_review = read_tenant_assurance_case(
            organization_id=organization_id, operation_public_id=second_id,
        )
        plan = simplified_review(ready_for_review)
        assert not plan["identities"]
        assert not plan["reuse"]
        assert plan["can_batch"], plan
        assert {field.field_name for field in batch_candidates(ready_for_review)} >= {
            "entered_value", "article_component", "plant_quantity", "metric_unit",
        }
        unauthenticated = client.post(
            f"/assurance-v2/operations/{second_id}/review-batch",
            json={"csrf_token": "invalid", "reason": "reviewed",
                  "explicit_confirmation": True,
                  "batch_fingerprint": plan["batch_fingerprint"]},
        )
        assert unauthenticated.status_code == 403
        stale = client.post(
            f"/assurance-v2/operations/{second_id}/review-batch",
            json={"csrf_token": us_lacey_csrf_token(
                session_token=client.cookies.get("us_lacey_session"),
                purpose=f"assurance-v2:{second_id}",
            ), "reason": "Reviewed full original source documents",
                  "explicit_confirmation": True, "batch_fingerprint": "a"*64},
        )
        assert stale.status_code == 409
        approved = client.post(
            f"/assurance-v2/operations/{second_id}/review-batch",
            json={
                "csrf_token": us_lacey_csrf_token(
                    session_token=client.cookies.get("us_lacey_session"),
                    purpose=f"assurance-v2:{second_id}",
                ),
                "reason": "Reviewed the exact source-linked values in the current batch",
                "explicit_confirmation": True,
                "batch_fingerprint": plan["batch_fingerprint"],
            },
        )
        assert approved.status_code == 202, approved.text
        assert approved.json()["status"] == "RECORDED"
        assert approved.json()["reviewed_fields"] >= 4
        second_approval = client.post(
            f"/assurance-v2/operations/{second_id}/review-batch",
            json={
                "csrf_token": us_lacey_csrf_token(
                    session_token=client.cookies.get("us_lacey_session"),
                    purpose=f"assurance-v2:{second_id}",
                ),
                "reason": "Attempt duplicate approval",
                "explicit_confirmation": True,
                "batch_fingerprint": plan["batch_fingerprint"],
            },
        )
        assert second_approval.status_code == 409
        updated = read_tenant_assurance_case(
            organization_id=organization_id, operation_public_id=second_id,
        )
        for name in ("entered_value", "article_component", "plant_quantity", "metric_unit"):
            assert any(
                field["name"] == name and field["authority"] == "HUMAN_CONFIRMED"
                and field["document_ids"] and field["evidence_ids"]
                for field in updated.fields
            ), name
            assert not any(row.get("field_name") == name for row in updated.exceptions)
        assert read_tenant_assurance_case(
            organization_id=organization_id + 1000000, operation_public_id=second_id,
        ) is None
        readiness = evaluate_filing_gate(updated)
        assert not readiness.ready
        assert any(blocker.code == "MISSING_REQUIRED_FIELD" for blocker in readiness.blockers)
        assert client.post(
            f"/assurance-v2/operations/{second_id}/issue",
            json={"csrf_token": us_lacey_csrf_token(
                session_token=client.cookies.get("us_lacey_session"),
                purpose=f"assurance-v2:{second_id}"),
            },
        ).status_code == 409
        assert client.get(f"{second_path}/export.csv").status_code == 409
        assert client.get(f"{second_path}/export.xlsx").status_code == 409
        draft = client.get(f"/assurance-v2/operations/{second_id}/draft.xlsx")
        assert draft.status_code == 200
        for ordinal, pack in ((3, "adv_supplier"), (4, "adv_conflict")):
            create = client.get("/operations/new")
            assert create.status_code == 200, create.text
            created = client.post("/operations/new", data={
                "csrf_token": _csrf_for(create.text, "/operations/new"),
                "client_reference": f"V2-GOLDEN-{ordinal}-{suffix}",
            })
            assert created.status_code == 303, created.text
            path = created.headers["location"]
            public_id = path.rsplit("/", 1)[-1]
            _upload_pack(client, path, pack)
            for index in range(6):
                result = process_one_us_lacey_job(
                    worker_id=f"v2-golden-{suffix}-{ordinal}-{index}",
                )
                assert result.claimed and result.job_status == "COMPLETED", result
            adversarial = read_tenant_assurance_case(
                organization_id=organization_id, operation_public_id=public_id,
            )
            assert adversarial is not None
            assert len(adversarial.documents) == 6
            if ordinal == 3:
                assert len(adversarial.identities) == 1
                assert adversarial.identities[0]["sku"] == identity["sku"]
                assert adversarial.identities[0]["supplier_id"] != identity["supplier_id"]
                assert not any(item["status"] == "ELIGIBLE" for item in adversarial.reuses)
            else:
                contradictory = [item for item in adversarial.candidate_observations
                                 if item["field_name"] == "species"
                                 and item["normalized_value"] == "aurea"
                                 and item["support_status"] == "SUPPORTED"]
                assert contradictory
                assert not any(item["field_name"] == "species" and item["status"] == "ELIGIBLE"
                               for item in adversarial.reuses)
        memory_id = UUID(next(item["id"] for item in applied_case.reuses
                              if item["field_name"] == "species"))
        db = get_us_lacey_db_session()
        try:
            set_tenant_db_context(db, organization_id)
            assert revoke_verified_memory(
                db, organization_id=organization_id,
                authenticated_user_id=int(reviewed_case.decisions[0]["actor_id"]),
                memory_public_id=memory_id, reason="Golden negative revocation",
                idempotency_key=f"v2-golden-revoke:{suffix}",
            )
            db.commit()
        finally:
            db.close()
        revoked_case = read_tenant_assurance_case(
            organization_id=organization_id, operation_public_id=second_id,
        )
        assert not any(item["field_name"] == "species" and item["status"] == "VERIFIED"
                       for item in revoked_case.reuses)
        assert not evaluate_filing_gate(revoked_case).ready
