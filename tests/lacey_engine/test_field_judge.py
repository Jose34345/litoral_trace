from __future__ import annotations

from dataclasses import replace
from uuid import UUID

import pytest

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
    FieldJudgeMode,
    FieldJudgeReason,
    candidate_identity,
    field_judge_mode,
    validate_field_judge_decisions,
)


def _candidate(*, verified: bool = True, line_item_key: str | None = "LINE-1") -> CandidateEnvelope:
    return CandidateEnvelope(
        candidate=AICandidate(
            field_key="bill_of_lading",
            value="OOLU1234567",
            normalized_value="OOLU1234567",
            evidence_class=EvidenceClass.EXPLICIT,
            page=1,
            source_text="Bill of Lading: OOLU1234567",
            confidence=0.97,
            provider="gemini",
            model="test-model",
            evidence_verified=verified,
        ),
        document_id=UUID("11111111-1111-1111-1111-111111111111"),
        document_type=DocumentType.BILL_OF_LADING,
        specialist=SpecialistRole.LOGISTICS,
        agent_run_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        line_item_key=line_item_key,
        source_span_id=None,
    )


def test_field_judge_version_is_explicit() -> None:
    assert FIELD_JUDGE_VERSION == "lacey_field_judge_v1"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, FieldJudgeMode.OFF),
        ("off", FieldJudgeMode.OFF),
        (" SHADOW ", FieldJudgeMode.SHADOW),
        ("EnFoRcE", FieldJudgeMode.ENFORCE),
        ("bogus", FieldJudgeMode.OFF),
        ("", FieldJudgeMode.OFF),
    ],
)
def test_field_judge_mode_is_closed_and_fail_safe(raw: str | None, expected: FieldJudgeMode) -> None:
    environ = {} if raw is None else {"LT_AI_FIELD_JUDGE_MODE": raw}
    assert field_judge_mode(environ) is expected


def test_candidate_identity_is_deterministic_and_ignores_ephemeral_agent_run_id() -> None:
    envelope = _candidate()
    changed_run = replace(
        envelope,
        agent_run_id=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
    )
    assert candidate_identity(envelope) == candidate_identity(changed_run)
    assert candidate_identity(envelope).startswith("fj1_")


def test_valid_decision_can_accept_only_the_exact_existing_candidate() -> None:
    envelope = _candidate()
    decision = FieldJudgeDecisionRecord(
        candidate_id=candidate_identity(envelope),
        field_key="bill_of_lading",
        line_item_key="LINE-1",
        decision=FieldJudgeDecision.ACCEPT,
        reason=FieldJudgeReason.EXACT_FIELD_CONTEXT,
    )

    validated = validate_field_judge_decisions((envelope,), (decision,))

    assert validated == (decision,)


def test_hallucinated_candidate_id_degrades_existing_candidate_to_needs_review() -> None:
    envelope = _candidate()
    decision = FieldJudgeDecisionRecord(
        candidate_id="fj1_not-an-existing-candidate",
        field_key="bill_of_lading",
        line_item_key="LINE-1",
        decision=FieldJudgeDecision.ACCEPT,
        reason=FieldJudgeReason.EXACT_FIELD_CONTEXT,
    )

    validated = validate_field_judge_decisions((envelope,), (decision,))

    assert validated[0].candidate_id == candidate_identity(envelope)
    assert validated[0].decision is FieldJudgeDecision.NEEDS_REVIEW
    assert validated[0].reason is FieldJudgeReason.INSUFFICIENT_CONTEXT


def test_mismatched_field_or_line_key_cannot_be_accepted() -> None:
    envelope = _candidate()
    decision = FieldJudgeDecisionRecord(
        candidate_id=candidate_identity(envelope),
        field_key="container_number",
        line_item_key="LINE-9",
        decision=FieldJudgeDecision.ACCEPT,
        reason=FieldJudgeReason.EXACT_FIELD_CONTEXT,
    )

    validated = validate_field_judge_decisions((envelope,), (decision,))

    assert validated[0].field_key == "bill_of_lading"
    assert validated[0].line_item_key == "LINE-1"
    assert validated[0].decision is FieldJudgeDecision.NEEDS_REVIEW
    assert validated[0].reason is FieldJudgeReason.SCOPE_MISMATCH


def test_unverified_evidence_can_never_be_accepted() -> None:
    envelope = _candidate(verified=False)
    decision = FieldJudgeDecisionRecord(
        candidate_id=candidate_identity(envelope),
        field_key="bill_of_lading",
        line_item_key="LINE-1",
        decision=FieldJudgeDecision.ACCEPT,
        reason=FieldJudgeReason.AUTHORITATIVE_SOURCE,
    )

    validated = validate_field_judge_decisions((envelope,), (decision,))

    assert validated[0].decision is FieldJudgeDecision.NEEDS_REVIEW
    assert validated[0].reason is FieldJudgeReason.INSUFFICIENT_CONTEXT


def test_missing_decision_degrades_to_needs_review() -> None:
    envelope = _candidate()

    validated = validate_field_judge_decisions((envelope,), ())

    assert validated[0].candidate_id == candidate_identity(envelope)
    assert validated[0].decision is FieldJudgeDecision.NEEDS_REVIEW
    assert validated[0].reason is FieldJudgeReason.INSUFFICIENT_CONTEXT
