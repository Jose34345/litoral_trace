"""Real original 13-PDF bytes → source claims → DB review → controlled memory.

Fixtures are intentionally SYNTHETIC (not customer uploads). This test exercises
actual PDF parsing and persistent SQLAlchemy records, not a mocked Engine 2
payload. PostgreSQL/RLS and full regulatory export remain separate release gates.
"""
from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from uuid import uuid4
from copy import deepcopy

from sqlalchemy import select

from litoral_trace.db.models import (
    AssuranceDocument, UsLaceyAssuranceV2ClaimSnapshot,
    UsLaceyEngineShipmentRun, UsLaceyOperationDocument,
    UsLaceyOperationField, UsLaceySourceSetMember, VaultDocument,
)
from litoral_trace.lacey_engine.pipeline import process_bundle
from litoral_trace.lacey_engine.shipment import ShipmentDocumentInput, process_shipment
from litoral_trace.lacey_engine.serialization import (
    SHIPMENT_RESOLUTION_SCHEMA_VERSION, serialize_shipment_resolution,
)
from litoral_trace.us_lacey.assurance_v2_claim_ingestion import collect_source_claims
from litoral_trace.us_lacey.assurance_v2_case_reader import _case
from litoral_trace.us_lacey.assurance_v2_memory import (
    promote_decision_to_memory, record_human_decision,
)
from litoral_trace.us_lacey.lacey_engine_service import source_set_fingerprint
from litoral_trace.us_lacey.exporters.assurance_v2 import evaluate_filing_gate
from test_assurance_v2_core_memory import (
    session, org_and_actor, supplier_product, shipment,
)
from test_assurance_v2_real_pdf_packs import _MANIFEST, _ROOT, _raw_pack

_STABLE = ("genus", "species", "country_of_harvest")


def _ingest_original_documents(db, org_id: int, product_id: int, pack: str):
    files = sorted((_ROOT / pack).glob("*.pdf"))
    assert len(files) == (7 if pack == "pack1" else 6)
    truth, _ = _raw_pack(pack)
    assert truth.plant_lines[0].entity_key == "sku:bam-coast-04"

    operation, original_doc, original_link, revision, species_field = shipment(
        db, org_id, product_id, reference=f"REAL-SYNTHETIC-{pack.upper()}",
    )
    operation.document_count = len(files)
    document_rows = []
    inputs = []
    for i, path in enumerate(files, 1):
        raw = path.read_bytes()
        digest = sha256(raw).hexdigest()
        assert raw.startswith(b"%PDF")
        assert digest == _MANIFEST[pack][path.name]
        if i == 1:
            assurance = original_doc
            link = original_link
            vault = db.get(VaultDocument, assurance.vault_document_id)
            vault.original_filename = path.name
            vault.sha256 = digest
            vault.size_bytes = len(raw)
        else:
            vault = VaultDocument(
                organization_id=org_id, original_filename=path.name,
                content_type="application/pdf", size_bytes=len(raw),
                sha256=digest, object_key=f"synthetic-pdf/{uuid4().hex}",
                storage_backend="s3", storage_bucket="synthetic-test",
                document_type="OTHER_EVIDENCE", status="available",
            )
            db.add(vault)
            db.flush()
            assurance = AssuranceDocument(
                organization_id=org_id, vault_document_id=vault.id,
                semantic_document_type="UNKNOWN", type_confidence=0.0,
                processing_status="UPLOADED",
            )
            db.add(assurance)
            db.flush()
            link = UsLaceyOperationDocument(
                organization_id=org_id, operation_id=operation.id,
                assurance_document_id=assurance.id, version_number=1,
                document_role="SUPPLIER_DECLARATION", is_current=True,
            )
            db.add(link)
            db.flush()
            db.add(UsLaceySourceSetMember(
                organization_id=org_id, source_set_revision_id=revision.id,
                operation_document_id=link.id, assurance_document_id=assurance.id,
            ))
        bundle = process_bundle(filename=path.name, content=raw)
        assert bundle.documents
        document_rows.append((link, vault, assurance, raw, bundle))
        for logical in bundle.documents:
            inputs.append(ShipmentDocumentInput(
                document_id=f"{pack}-{i}-{logical.logical_document_id}",
                filename=path.name, resolution=logical.resolution,
            ))
    db.flush()

    revision.source_set_fingerprint = source_set_fingerprint(
        organization_id=org_id, operation_id=operation.id,
        documents=[(link, vault) for link, vault, *_ in document_rows],
    )
    db.flush()
    evidence_by_field = {}
    snapshots = []
    for link, vault, assurance, raw, bundle in document_rows:
        payload = collect_source_claims(
            organization_id=org_id, operation_id=operation.id,
            source_set_revision_id=revision.id,
            source_set_fingerprint=revision.source_set_fingerprint,
            assurance_document_id=assurance.id,
            operation_document_id=link.id,
            document_version_number=link.version_number,
            original_sha256=vault.sha256, original_bytes=raw,
            bundle=bundle,
        )
        snapshots.append(payload)
        for claim in payload["claims"]:
            if claim["field_name"] in _STABLE and claim["support_status"] == "SUPPORTED":
                evidence_by_field.setdefault(claim["field_name"], claim)

    resolution = process_shipment(documents=inputs)
    serialized = serialize_shipment_resolution(resolution)
    serialized["assurance_v2_claim_snapshot"] = {
        "schema_version": "assurance.claim.snapshot.v1",
        "source_set_revision_id": revision.id,
        "source_set_fingerprint": revision.source_set_fingerprint,
        "status": "CANDIDATE_ONLY",
        "documents": snapshots,
        "claim_count": sum(s["claim_count"] for s in snapshots),
    }
    db.add(UsLaceyEngineShipmentRun(
        organization_id=org_id, operation_id=operation.id,
        engine_version=resolution.engine_version,
        ruleset_version=resolution.ruleset_version,
        schema_version=SHIPMENT_RESOLUTION_SCHEMA_VERSION,
        source_set_fingerprint=revision.source_set_fingerprint,
        document_count=len(inputs), readiness=resolution.readiness.value,
        resolution_json=serialized,
    ))
    db.flush()

    fields = {"species": species_field}
    for name in ("genus", "country_of_harvest"):
        field = UsLaceyOperationField(
            organization_id=org_id, operation_id=operation.id,
            merchandise_line_reference="1", field_name=name,
            field_scope="PLANT_LINE", plant_line_id=species_field.plant_line_id,
            field_status="MISSING", confidence=0.0, validation_status="MISSING",
        )
        db.add(field)
        db.flush()
        fields[name] = field
    if pack == "pack1":
        assert set(evidence_by_field) == set(_STABLE)
        expected = {
            "genus": "Phyllostachys", "species": "edulis",
            "country_of_harvest": "CHINA",
        }
        for name, field in fields.items():
            claim = evidence_by_field[name]
            assert claim["original_value"].upper() == expected[name].upper()
            locator = claim["source_locator"]
            field.original_value = claim["original_value"]
            field.normalized_value = expected[name]
            field.source_assurance_document_id = claim["assurance_document_id"]
            field.source_page = locator["page"]
            field.source_locator = f"page:{locator['page']}:{locator['block_id']}"
            field.field_status = "FOUND"
            field.confidence = 1.0
            field.validation_status = "VALID"
            field.extractor = "assurance.claim.v1"
            field.extractor_version = "1"
    db.flush()
    return operation, revision, fields, evidence_by_field


def test_original_13_pdfs_to_verified_source_memory_and_safe_second_shipment(session):
    org, actor = org_and_actor(session, "OriginalPdfMemory")
    _, product = supplier_product(
        session, org.id, supplier_key="MID:LONGSHENG-ORIGINAL-PDF",
        sku="BAM-COAST-04",
    )
    op1, rev1, fields1, supported_claims = _ingest_original_documents(
        session, org.id, product.id, "pack1",
    )
    assert len(supported_claims) == 3
    for name, field in fields1.items():
        chosen = field.normalized_value
        field.human_value = chosen
        field.field_status = "MATCHED"
        decision = record_human_decision(
            session, organization_id=org.id, operation_id=op1.id,
            source_set_revision_id=rev1.id, authenticated_user_id=actor.id,
            action="ACCEPT", field_name=name, line_reference="1",
            selected_value=chosen, reason="Reviewer confirmed the ORIGINAL synthetic source PDF",
            idempotency_key=f"original13:{op1.id}:{name}",
            assurance_document_ids=(field.source_assurance_document_id,),
        )
        memory = promote_decision_to_memory(
            session, organization_id=org.id, decision_id=decision.id,
        )
        assert memory is not None
        assert memory.assurance_document_id == field.source_assurance_document_id

    db_case1 = _case(session, org.id, op1.public_id)
    assert db_case1 is not None
    assert len(db_case1.documents) == 7
    assert db_case1.source_set_generation == rev1.generation
    assert len(db_case1.candidate_observations) >= 30
    first_observation = db_case1.candidate_observations[0]
    assert first_observation["contract_version"] == "assurance.v2/1.0.0"
    assert first_observation["source_claim_contract_version"] == "assurance.claim.v1"
    assert first_observation["source_set_revision_id"] == rev1.id
    assert first_observation["source_set_fingerprint"] == rev1.source_set_fingerprint
    assert first_observation["generation"] == rev1.generation
    assert first_observation["document_version_number"] == 1
    assert len(first_observation["original_sha256"]) == 64
    assert first_observation["entity_type"]
    assert "subject_id" in first_observation
    assert first_observation["authority"] == "CANDIDATE"
    assert "historical_origin" in first_observation
    assert set(f["name"] for f in db_case1.fields if f["authority"] == "HUMAN_CONFIRMED") == set(_STABLE)
    assert not evaluate_filing_gate(db_case1).ready

    op2, rev2, fields2, pack2_factual = _ingest_original_documents(
        session, org.id, product.id, "pack2",
    )
    assert pack2_factual == {}
    db_case2 = _case(session, org.id, op2.public_id)
    assert db_case2 is not None
    assert len(db_case2.documents) == 6
    assert len(db_case2.candidate_observations) >= 25
    assert all(field["authority"] == "CANDIDATE" for field in db_case2.fields)
    assert all(not field.human_value for field in fields2.values())
    assert {item["field_name"] for item in db_case2.reuses} == set(_STABLE)
    assert all(item["status"] == "ELIGIBLE" for item in db_case2.reuses)
    assert all(item["provenance"] == "REUSE_PROPOSED_NOT_CANONICAL" for item in db_case2.reuses)
    assert not evaluate_filing_gate(db_case2).ready
    # The report may propose reuse, but no proposed historical value can be
    # exported as if it were extracted from Pack 2.


def test_incomplete_persisted_claim_fails_explicitly_before_workspace(session):
    org, _ = org_and_actor(session, "OriginalPdfIncomplete")
    _, product = supplier_product(
        session, org.id, supplier_key="MID:LONGSHENG-INCOMPLETE",
        sku="BAM-COAST-04",
    )
    operation, _, _, _ = _ingest_original_documents(
        session, org.id, product.id, "pack1",
    )
    run = session.scalar(select(UsLaceyEngineShipmentRun).where(
        UsLaceyEngineShipmentRun.organization_id == org.id,
        UsLaceyEngineShipmentRun.operation_id == operation.id,
    ))
    snapshot = deepcopy(run.resolution_json)
    snapshot["assurance_v2_claim_snapshot"]["documents"][0]["claims"][0].pop("entity_type")
    run.resolution_json = snapshot
    session.flush()
    case = _case(session, org.id, operation.public_id)
    assert case is not None
    assert not case.candidate_observations
    assert any(item["id"] == "source:claims-invalid" and item["blocking"]
               for item in case.exceptions)
    assert not evaluate_filing_gate(case).ready

    # A document envelope with the right revision but a different fingerprint
    # must not be accepted merely because the individual claim IDs still match.
    snapshot = deepcopy(run.resolution_json)
    snapshot["assurance_v2_claim_snapshot"]["documents"][0]["claims"][0]["entity_type"] = "SHIPMENT"
    snapshot["assurance_v2_claim_snapshot"]["documents"][0]["source_set_fingerprint"] = "0" * 64
    run.resolution_json = snapshot
    session.flush()
    stale = _case(session, org.id, operation.public_id)
    assert not stale.candidate_observations
    assert any(item["id"] == "source:claims-unavailable" and item["blocking"]
               for item in stale.exceptions)


def test_current_sidecar_precedes_legacy_run_and_invalid_sidecar_blocks_fallback(session):
    org, _ = org_and_actor(session, "OriginalPdfSidecar")
    _, product = supplier_product(session, org.id,
                                  supplier_key="MID:LONGSHENG-SIDECAR", sku="BAM-COAST-04")
    operation, revision, _, _ = _ingest_original_documents(session, org.id, product.id, "pack1")
    run = session.scalar(select(UsLaceyEngineShipmentRun).where(
        UsLaceyEngineShipmentRun.organization_id == org.id,
        UsLaceyEngineShipmentRun.operation_id == operation.id,
    ))
    snapshot = deepcopy(run.resolution_json["assurance_v2_claim_snapshot"])
    row = UsLaceyAssuranceV2ClaimSnapshot(
        organization_id=org.id, operation_id=operation.id,
        source_set_revision_id=revision.id,
        source_set_fingerprint=revision.source_set_fingerprint,
        status="CANDIDATE_ONLY", document_count=len(snapshot["documents"]),
        claim_count=snapshot["claim_count"], snapshot_json=snapshot,
    )
    session.add(row)
    legacy = deepcopy(run.resolution_json)
    legacy["assurance_v2_claim_snapshot"]["documents"][0]["claims"][0].pop("entity_type")
    run.resolution_json = legacy
    session.flush()

    current = _case(session, org.id, operation.public_id)
    assert current and len(current.candidate_observations) >= 30

    broken = deepcopy(row.snapshot_json)
    broken["documents"][0]["claims"][0].pop("entity_type")
    row.snapshot_json = broken
    session.flush()
    invalid = _case(session, org.id, operation.public_id)
    assert invalid and not invalid.candidate_observations
    assert any(item["reason"] == "SOURCE_CLAIMS_INVALID" for item in invalid.exceptions)
