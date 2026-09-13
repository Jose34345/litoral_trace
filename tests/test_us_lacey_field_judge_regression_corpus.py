from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid4, uuid5

from litoral_trace.db.models import (
    AssuranceDocument,
    Organization,
    UsLaceyOperation,
    UsLaceyOperationDocument,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    SpecialistRole,
)
from litoral_trace.lacey_engine.multi_agent.fusion import fuse_candidates
from litoral_trace.us_lacey.operations import UsLaceyOperationService
from litoral_trace.us_lacey.ppq505 import PPQ505_SHIPMENT_REFERENCE
from litoral_trace.us_lacey.specialized_projection import (
    SpecializedProjectionMode,
    plan_line_materialization,
    project_specialized_candidates,
)
from tests.us_lacey_engine2_postgres import (
    engine2_postgres_engine,
    engine2_postgres_session_factory,
    tenant_session,
)


_FIXTURE = Path(__file__).parent / "fixtures" / "us_lacey_field_judge_regression_corpus.json"


def _corpus() -> dict[str, object]:
    return json.loads(_FIXTURE.read_text(encoding="utf-8"))


def _candidate(
    *,
    field_key: str,
    value: str,
    source_text: str,
    seed: str,
    line_item_key: str | None = None,
) -> CandidateEnvelope:
    return CandidateEnvelope(
        candidate=AICandidate(
            field_key=field_key,
            value=value,
            normalized_value=value,
            evidence_class=EvidenceClass.EXPLICIT,
            page=1,
            source_text=source_text,
            confidence=0.97,
            provider="regression-fixture",
            model="regression-fixture",
            evidence_verified=True,
        ),
        document_id=uuid5(NAMESPACE_URL, f"regression-document-{seed}"),
        document_type=(
            DocumentType.BILL_OF_LADING
            if field_key == "bill_of_lading"
            else DocumentType.BOTANICAL_DECLARATION
        ),
        specialist=(
            SpecialistRole.LOGISTICS
            if field_key == "bill_of_lading"
            else SpecialistRole.BOTANICAL
        ),
        agent_run_id=uuid5(NAMESPACE_URL, f"regression-run-{seed}"),
        line_item_key=line_item_key,
        source_span_id=None,
    )


def _target(*, field_name: str, line_reference: str, field_scope: str):
    return SimpleNamespace(
        id=uuid5(NAMESPACE_URL, f"target-{field_name}-{line_reference}").int % 2_000_000_000,
        field_name=field_name,
        merchandise_line_reference=line_reference,
        field_scope=field_scope,
        field_status="MISSING",
        original_value=None,
        normalized_value=None,
        confidence=0.0,
        source_assurance_document_id=None,
        source_page=None,
        source_locator=None,
        extractor=None,
        extractor_version=None,
        human_value=None,
        reviewed_by_user_id=None,
        reviewed_at=None,
        validation_status="MISSING",
        validation_error=None,
    )


def test_committed_bol_traps_have_zero_false_safe_projections() -> None:
    false_safe = 0
    safe_projected = 0
    for index, case in enumerate(_corpus()["bol_cases"]):
        candidate = _candidate(
            field_key="bill_of_lading",
            value=case["value"],
            source_text=case["source_text"],
            seed=f"bol-{index}",
        )
        target = _target(
            field_name="bill_of_lading",
            line_reference=PPQ505_SHIPMENT_REFERENCE,
            field_scope="SHIPMENT",
        )
        result = project_specialized_candidates(
            candidates=(candidate,),
            targets=(target,),
            mode=SpecializedProjectionMode.ENFORCE,
        )
        projected = result.projected_count == 1
        if case["expected_safe"]:
            safe_projected += int(projected)
        else:
            false_safe += int(projected)

    assert false_safe == 0
    assert safe_projected == 1


def test_authoritative_bol_contradiction_stays_conflict_and_review() -> None:
    cases = _corpus()["conflicting_bols"]
    candidates = tuple(
        _candidate(
            field_key="bill_of_lading",
            value=case["value"],
            source_text=case["source_text"],
            seed=f"conflict-{index}",
        )
        for index, case in enumerate(cases)
    )

    fusion = fuse_candidates(candidates)
    assert len(fusion.conflicts) == 1
    assert fusion.conflicts[0].requires_ai_resolution is True
    assert set(fusion.conflicts[0].normalized_values) == {
        "MAEU274342495",
        "OOLU1234567890",
    }

    conflict_keys = frozenset(
        (conflict.key.field_key, conflict.key.line_item_key)
        for conflict in fusion.conflicts
    )
    target = _target(
        field_name="bill_of_lading",
        line_reference=PPQ505_SHIPMENT_REFERENCE,
        field_scope="SHIPMENT",
    )
    result = project_specialized_candidates(
        candidates=fusion.fused_candidates,
        targets=(target,),
        mode=SpecializedProjectionMode.ENFORCE,
        conflict_keys=conflict_keys,
    )

    assert result.projected_count == 0
    assert result.review_count == 1
    assert target.field_status == "MISSING"


def test_pinus_and_eucalyptus_stable_lines_remain_separate() -> None:
    cases = _corpus()["stable_lines"]
    candidates = tuple(
        _candidate(
            field_key="genus",
            value=case["genus"],
            source_text=f"Genus: {case['genus']}",
            seed=f"line-{index}",
            line_item_key=case["line_item_key"],
        )
        for index, case in enumerate(cases)
    )
    plan = plan_line_materialization(candidates, existing_line_references=())
    assert len(plan.generated_lines) == 2
    by_key = {item.line_item_key: item.line_reference for item in plan.generated_lines}
    assert len(set(by_key.values())) == 2

    targets = tuple(
        _target(
            field_name="genus",
            line_reference=by_key[case["line_item_key"]],
            field_scope="PLANT_LINE",
        )
        for case in cases
    )
    result = project_specialized_candidates(
        candidates=candidates,
        targets=targets,
        mode=SpecializedProjectionMode.ENFORCE,
    )

    assert result.projected_count == 2
    actual = {
        target.merchandise_line_reference: target.normalized_value for target in targets
    }
    assert actual[by_key["SKU:PINE-001"]] == "Pinus"
    assert actual[by_key["SKU:EUC-002"]] == "Eucalyptus"


def test_seven_unknown_uploads_remain_seven_current_documents(
    engine2_postgres_session_factory,
) -> None:
    factory = engine2_postgres_session_factory
    suffix = uuid4().hex
    session = factory()
    org = Organization(
        name=f"Seven source regression {suffix}",
        slug=f"seven-source-{suffix}",
        tax_id=f"seven-{suffix}",
        tier="pro",
        is_active=True,
    )
    session.add(org)
    session.flush()
    set_tenant_db_context(session, org.id)
    operation = UsLaceyOperation(
        organization_id=org.id,
        client_reference=f"seven-source-{suffix}",
        status="NEW",
        document_count=0,
        merchandise_line_count=0,
    )
    session.add(operation)
    session.flush()

    assurance_ids: list[int] = []
    for index in range(7):
        content = f"independent-unknown-{index}-{suffix}".encode()
        vault = VaultDocument(
            organization_id=org.id,
            original_filename=f"source-{index + 1}.pdf",
            content_type="application/pdf",
            size_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            object_key=f"tests/{suffix}/{index}",
            storage_backend="s3",
            storage_bucket="tests",
            document_type="OTHER_EVIDENCE",
            status="available",
        )
        session.add(vault)
        session.flush()
        assurance = AssuranceDocument(
            organization_id=org.id,
            vault_document_id=vault.id,
            semantic_document_type="UNKNOWN",
            processing_status="EXTRACTED",
        )
        session.add(assurance)
        session.flush()
        assurance_ids.append(assurance.id)
    operation_public_id = operation.public_id
    org_id = org.id
    operation_id = operation.id
    session.commit()
    session.close()

    service = UsLaceyOperationService(session_factory=factory)
    link_ids = tuple(
        service.attach_document(
            organization_id=org_id,
            operation_public_id=operation_public_id,
            assurance_document_id=assurance_id,
            document_role="UNKNOWN",
        )
        for assurance_id in assurance_ids
    )

    # Exact reattachment is idempotent and cannot turn one of the seven independent
    # UNKNOWN uploads historical.
    repeated = service.attach_document(
        organization_id=org_id,
        operation_public_id=operation_public_id,
        assurance_document_id=assurance_ids[0],
        document_role="UNKNOWN",
    )
    assert repeated == link_ids[0]

    check = tenant_session(factory, org_id)
    current_links = (
        check.query(UsLaceyOperationDocument)
        .filter_by(operation_id=operation_id, is_current=True)
        .order_by(UsLaceyOperationDocument.id)
        .all()
    )
    persisted_operation = check.get(UsLaceyOperation, operation_id)
    assert len(current_links) == 7
    assert {link.assurance_document_id for link in current_links} == set(assurance_ids)
    assert persisted_operation.document_count == 7
    check.close()
