from __future__ import annotations

from types import SimpleNamespace
from uuid import NAMESPACE_URL, uuid5

from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    SpecialistRole,
)
from litoral_trace.lacey_engine.multi_agent.field_judge import (
    FIELD_JUDGE_VERSION,
    FieldJudgeDecision,
    FieldJudgeDecisionRecord,
    FieldJudgeEvaluation,
    FieldJudgeMode,
    FieldJudgeReason,
    candidate_identity as field_judge_candidate_identity,
)
from litoral_trace.us_lacey.ppq505 import PPQ505_SHIPMENT_REFERENCE
from litoral_trace.us_lacey.specialized_projection import (
    SpecializedProjectionMode,
    plan_line_materialization,
    project_specialized_candidates,
    specialized_projection_mode,
)


def _candidate(
    *,
    field_key: str,
    line_item_key: str | None,
    value: str = "fixture",
    seed: str,
    evidence_class: EvidenceClass = EvidenceClass.EXPLICIT,
    evidence_verified: bool = True,
) -> CandidateEnvelope:
    candidate = AICandidate(
        field_key=field_key,
        value=value,
        normalized_value=value,
        evidence_class=evidence_class,
        page=1,
        source_text=f"SKU-{seed} {field_key} {value}",
        confidence=0.97,
        provider="fixture",
        model="fixture",
        evidence_verified=evidence_verified,
    )
    shipment_fields = {
        "estimated_arrival_date",
        "filing_entry_reference",
        "container_number",
        "bill_of_lading",
        "manufacturer_id",
        "importer_name",
        "consignee_name",
        "importer_address",
        "consignee_address",
    }
    return CandidateEnvelope(
        candidate=candidate,
        document_id=uuid5(NAMESPACE_URL, f"projection-doc-{seed}"),
        document_type=(
            DocumentType.BILL_OF_LADING
            if field_key in shipment_fields
            else DocumentType.COMMERCIAL_INVOICE
        ),
        specialist=(
            SpecialistRole.LOGISTICS
            if field_key in {"estimated_arrival_date", "container_number", "bill_of_lading"}
            else SpecialistRole.CUSTOMS_IDENTITY
            if field_key in shipment_fields
            else SpecialistRole.COMMERCIAL_LINES
            if field_key in {"description", "article_component", "hts_code", "entered_value"}
            else SpecialistRole.BOTANICAL
        ),
        agent_run_id=uuid5(NAMESPACE_URL, f"projection-run-{seed}"),
        line_item_key=line_item_key,
        source_span_id=None,
    )


def _target(
    *,
    field_name: str,
    line_reference: str,
    field_scope: str,
    field_status: str = "MISSING",
    original_value: str | None = None,
    normalized_value: str | None = None,
    human_value: str | None = None,
    reviewed_at=None,
):
    return SimpleNamespace(
        id=100,
        field_name=field_name,
        merchandise_line_reference=line_reference,
        field_scope=field_scope,
        field_status=field_status,
        original_value=original_value,
        normalized_value=normalized_value,
        confidence=0.0,
        source_assurance_document_id=None,
        source_page=None,
        source_locator=None,
        extractor=None,
        extractor_version=None,
        human_value=human_value,
        reviewed_by_user_id=None,
        reviewed_at=reviewed_at,
        validation_status="MISSING",
        validation_error=None,
    )


def _judge_evaluation(
    candidate: CandidateEnvelope,
    *,
    decision: FieldJudgeDecision,
) -> FieldJudgeEvaluation:
    return FieldJudgeEvaluation(
        version=FIELD_JUDGE_VERSION,
        mode=FieldJudgeMode.ENFORCE,
        provider="fixture",
        model="fixture",
        latency_ms=1,
        input_tokens=10,
        output_tokens=2,
        total_tokens=12,
        decisions=(
            FieldJudgeDecisionRecord(
                candidate_id=field_judge_candidate_identity(candidate),
                field_key=candidate.candidate.field_key,
                line_item_key=candidate.line_item_key,
                decision=decision,
                reason=FieldJudgeReason.EXACT_FIELD_CONTEXT,
            ),
        ),
        safe_error=None,
    )


def test_distinct_stable_line_keys_produce_distinct_deterministic_references() -> None:
    candidates = (
        _candidate(field_key="hts_code", line_item_key="SKU:CHAIR-001", value="940360", seed="a"),
        _candidate(field_key="species", line_item_key="LINE:2", value="radiata", seed="b"),
    )

    first = plan_line_materialization(candidates, existing_line_references=())
    second = plan_line_materialization(tuple(reversed(candidates)), existing_line_references=())

    assert first.generated_lines == second.generated_lines
    assert len(first.generated_lines) == 2
    assert len({line.line_reference for line in first.generated_lines}) == 2
    assert {line.line_item_key for line in first.generated_lines} == {"SKU:CHAIR-001", "LINE:2"}
    assert first.line_references == tuple(line.line_reference for line in first.generated_lines)


def test_line_materialization_planner_is_idempotent_on_rerun() -> None:
    candidates = (
        _candidate(field_key="genus", line_item_key="SKU:TABLE-100", value="Pinus", seed="pinus"),
        _candidate(field_key="species", line_item_key="SKU:TABLE-100", value="radiata", seed="radiata"),
    )

    first = plan_line_materialization(candidates, existing_line_references=())
    rerun = plan_line_materialization(
        candidates,
        existing_line_references=first.line_references,
    )

    assert len(first.generated_lines) == 1
    assert rerun.generated_lines == ()
    assert rerun.line_references == first.line_references


def test_existing_human_line_references_are_retained_in_original_order() -> None:
    candidates = (
        _candidate(field_key="country_of_harvest", line_item_key="LINE:9", value="Canada", seed="c"),
    )

    plan = plan_line_materialization(
        candidates,
        existing_line_references=("HUMAN-Z", "1", "CUSTOM-REF"),
    )

    assert plan.line_references[:3] == ("HUMAN-Z", "1", "CUSTOM-REF")
    assert len(plan.generated_lines) == 1
    assert plan.line_references[3] == plan.generated_lines[0].line_reference


def test_non_line_candidate_cannot_create_a_plant_line() -> None:
    candidate = _candidate(
        field_key="bill_of_lading",
        line_item_key="SKU:SHOULD-NOT-MATERIALIZE-1",
        value="OOLU1234567890",
        seed="shipment",
    )

    plan = plan_line_materialization((candidate,), existing_line_references=())

    assert plan.generated_lines == ()
    assert plan.line_references == ()
    assert plan.review_only_line_keys == ()


def test_fingerprint_only_line_key_remains_reviewable_not_materialized() -> None:
    candidate = _candidate(
        field_key="genus",
        line_item_key="FP:1234567890ABCDEF12345678",
        value="Eucalyptus",
        seed="fp",
    )

    plan = plan_line_materialization((candidate,), existing_line_references=())

    assert plan.generated_lines == ()
    assert plan.line_references == ()
    assert plan.review_only_line_keys == ("FP:1234567890ABCDEF12345678",)


def test_unambiguous_row_key_is_stable_and_materializable() -> None:
    document_id = uuid5(NAMESPACE_URL, "row-key-document")
    row_key = f"ROW:{document_id}:P3:TCOMMERCIAL:R7"
    candidate = _candidate(
        field_key="plant_quantity",
        line_item_key=row_key,
        value="1250",
        seed="row",
    )

    first = plan_line_materialization((candidate,), existing_line_references=())
    second = plan_line_materialization((candidate,), existing_line_references=())

    assert len(first.generated_lines) == 1
    assert first.generated_lines == second.generated_lines
    assert first.generated_lines[0].line_item_key == row_key


def test_specialized_projection_mode_defaults_off_and_accepts_closed_values() -> None:
    assert specialized_projection_mode({}) is SpecializedProjectionMode.OFF
    assert specialized_projection_mode({"LT_AI_SPECIALIZED_PROJECTION_MODE": "SHADOW"}) is SpecializedProjectionMode.SHADOW
    assert specialized_projection_mode({"LT_AI_SPECIALIZED_PROJECTION_MODE": "enforce"}) is SpecializedProjectionMode.ENFORCE
    assert specialized_projection_mode({"LT_AI_SPECIALIZED_PROJECTION_MODE": "unsafe"}) is SpecializedProjectionMode.OFF


def test_projection_off_performs_no_projection() -> None:
    candidate = _candidate(
        field_key="bill_of_lading",
        line_item_key=None,
        value="OOLU1234567890",
        seed="off",
    )
    target = _target(
        field_name="bill_of_lading",
        line_reference=PPQ505_SHIPMENT_REFERENCE,
        field_scope="SHIPMENT",
    )

    result = project_specialized_candidates(
        candidates=(candidate,),
        targets=(target,),
        mode=SpecializedProjectionMode.OFF,
    )

    assert result.projected_count == 0
    assert result.eligible_count == 0
    assert target.field_status == "MISSING"
    assert target.normalized_value is None


def test_projection_shadow_computes_safe_decision_without_mutating_target() -> None:
    candidate = _candidate(
        field_key="bill_of_lading",
        line_item_key=None,
        value="OOLU1234567890",
        seed="shadow",
    )
    target = _target(
        field_name="bill_of_lading",
        line_reference=PPQ505_SHIPMENT_REFERENCE,
        field_scope="SHIPMENT",
    )

    result = project_specialized_candidates(
        candidates=(candidate,),
        targets=(target,),
        mode=SpecializedProjectionMode.SHADOW,
    )

    assert result.eligible_count == 1
    assert result.projected_count == 0
    assert result.review_count == 0
    assert target.field_status == "MISSING"
    assert target.normalized_value is None


def test_projection_enforce_creates_only_unconfirmed_found_suggestion() -> None:
    candidate = _candidate(
        field_key="hts_code",
        line_item_key="SKU:DESK-001",
        value="9403.60.8081",
        seed="enforce",
    )
    line_plan = plan_line_materialization((candidate,), existing_line_references=())
    line_reference = line_plan.generated_lines[0].line_reference
    target = _target(
        field_name="hts_code",
        line_reference=line_reference,
        field_scope="PLANT_LINE",
    )

    result = project_specialized_candidates(
        candidates=(candidate,),
        targets=(target,),
        mode=SpecializedProjectionMode.ENFORCE,
        source_assurance_by_document={candidate.document_id: 321},
    )

    assert result.eligible_count == 1
    assert result.projected_count == 1
    assert target.field_status == "FOUND"
    assert target.original_value == "9403.60.8081"
    assert target.normalized_value == "9403608081"
    assert target.source_assurance_document_id == 321
    assert target.reviewed_at is None
    assert target.reviewed_by_user_id is None
    assert target.human_value is None
    assert target.validation_status == "VALID"


def test_projection_never_overwrites_human_reviewed_target() -> None:
    candidate = _candidate(
        field_key="bill_of_lading",
        line_item_key=None,
        value="NEW-BOL-123",
        seed="human",
    )
    reviewed_marker = object()
    target = _target(
        field_name="bill_of_lading",
        line_reference=PPQ505_SHIPMENT_REFERENCE,
        field_scope="SHIPMENT",
        field_status="MATCHED",
        original_value="HUMAN-BOL",
        normalized_value="HUMAN-BOL",
        human_value="HUMAN-BOL",
        reviewed_at=reviewed_marker,
    )

    result = project_specialized_candidates(
        candidates=(candidate,),
        targets=(target,),
        mode=SpecializedProjectionMode.ENFORCE,
    )

    assert result.projected_count == 0
    assert target.original_value == "HUMAN-BOL"
    assert target.normalized_value == "HUMAN-BOL"
    assert target.human_value == "HUMAN-BOL"
    assert target.reviewed_at is reviewed_marker


def test_competing_values_for_same_target_remain_review_only() -> None:
    first = _candidate(
        field_key="bill_of_lading",
        line_item_key=None,
        value="BOL-A",
        seed="conflict-a",
    )
    target = _target(
        field_name="bill_of_lading",
        line_reference=PPQ505_SHIPMENT_REFERENCE,
        field_scope="SHIPMENT",
    )

    result = project_specialized_candidates(
        candidates=(first,),
        targets=(target,),
        mode=SpecializedProjectionMode.ENFORCE,
        conflict_keys=frozenset({("bill_of_lading", None)}),
    )

    assert result.projected_count == 0
    assert result.review_count == 1
    assert target.field_status == "MISSING"
    assert target.normalized_value is None


def test_projection_refuses_scope_crossing() -> None:
    candidate = _candidate(
        field_key="bill_of_lading",
        line_item_key=None,
        value="BOL-SCOPE",
        seed="scope",
    )
    wrong_scope_target = _target(
        field_name="bill_of_lading",
        line_reference="1",
        field_scope="PLANT_LINE",
    )

    result = project_specialized_candidates(
        candidates=(candidate,),
        targets=(wrong_scope_target,),
        mode=SpecializedProjectionMode.ENFORCE,
    )

    assert result.projected_count == 0
    assert wrong_scope_target.field_status == "MISSING"


def test_projection_refuses_unverified_inferred_and_invalid_candidates() -> None:
    unverified = _candidate(
        field_key="bill_of_lading",
        line_item_key=None,
        value="BOL-UNVERIFIED",
        seed="unverified",
        evidence_verified=False,
    )
    inferred = _candidate(
        field_key="bill_of_lading",
        line_item_key=None,
        value="BOL-INFERRED",
        seed="inferred",
        evidence_class=EvidenceClass.INFERRED,
    )
    invalid = _candidate(
        field_key="estimated_arrival_date",
        line_item_key=None,
        value="not-a-date",
        seed="invalid",
    )
    targets = (
        _target(
            field_name="bill_of_lading",
            line_reference=PPQ505_SHIPMENT_REFERENCE,
            field_scope="SHIPMENT",
        ),
        _target(
            field_name="estimated_arrival_date",
            line_reference=PPQ505_SHIPMENT_REFERENCE,
            field_scope="SHIPMENT",
        ),
    )

    result = project_specialized_candidates(
        candidates=(unverified, inferred, invalid),
        targets=targets,
        mode=SpecializedProjectionMode.ENFORCE,
    )

    assert result.projected_count == 0
    assert all(target.field_status == "MISSING" for target in targets)


def test_projection_requires_judge_accept_when_judge_is_enforcing() -> None:
    candidate = _candidate(
        field_key="bill_of_lading",
        line_item_key=None,
        value="BOL-JUDGE",
        seed="judge",
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
        judge_evaluation=_judge_evaluation(candidate, decision=FieldJudgeDecision.REJECT),
    )

    assert result.projected_count == 0
    assert result.review_count == 1
    assert target.field_status == "MISSING"
