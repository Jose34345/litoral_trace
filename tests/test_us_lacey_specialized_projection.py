from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    SpecialistRole,
)
from litoral_trace.us_lacey.specialized_projection import plan_line_materialization


def _candidate(
    *,
    field_key: str,
    line_item_key: str | None,
    value: str = "fixture",
    seed: str,
) -> CandidateEnvelope:
    candidate = AICandidate(
        field_key=field_key,
        value=value,
        normalized_value=value,
        evidence_class=EvidenceClass.EXPLICIT,
        page=1,
        source_text=f"SKU-{seed} {field_key} {value}",
        confidence=0.97,
        provider="fixture",
        model="fixture",
        evidence_verified=True,
    )
    return CandidateEnvelope(
        candidate=candidate,
        document_id=uuid5(NAMESPACE_URL, f"projection-doc-{seed}"),
        document_type=DocumentType.COMMERCIAL_INVOICE,
        specialist=(
            SpecialistRole.COMMERCIAL_LINES
            if field_key in {"description", "article_component", "hts_code", "entered_value"}
            else SpecialistRole.BOTANICAL
        ),
        agent_run_id=uuid5(NAMESPACE_URL, f"projection-run-{seed}"),
        line_item_key=line_item_key,
        source_span_id=None,
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
