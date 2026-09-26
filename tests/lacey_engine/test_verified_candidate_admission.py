from __future__ import annotations

from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest

from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.candidate_admission import (
    CANDIDATE_ADMISSION_VERSION,
    CandidateAdmissionDecision,
    CandidateAdmissionReason,
    evaluate_verified_candidate_admission,
)
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    SpecialistRole,
)


def _candidate(
    *,
    field_key: str = "hts_code",
    value: str = "4419.90.9000",
    document_type: DocumentType = DocumentType.COMMERCIAL_INVOICE,
    specialist: SpecialistRole = SpecialistRole.COMMERCIAL_LINES,
    line_item_key: str | None = "SKU:ACT-TRAY-18",
    verified: bool = True,
    evidence_class: EvidenceClass = EvidenceClass.EXPLICIT,
    seed: str = "candidate",
) -> CandidateEnvelope:
    candidate = AICandidate(
        field_key=field_key,
        value=value,
        normalized_value=value,
        evidence_class=evidence_class,
        page=1,
        source_text=f"SKU ACT-TRAY-18 {field_key} {value}",
        confidence=0.97,
        provider="fixture",
        model="fixture",
        evidence_verified=verified,
    )
    return CandidateEnvelope(
        candidate=candidate,
        document_id=uuid5(NAMESPACE_URL, f"admission-{seed}"),
        document_type=document_type,
        specialist=specialist,
        agent_run_id=uuid4(),
        line_item_key=line_item_key,
        source_span_id=None,
    )


def _pass(_candidate: CandidateEnvelope) -> CandidateAdmissionReason | None:
    return None


def _single_reason(candidate: CandidateEnvelope) -> CandidateAdmissionReason:
    evaluation = evaluate_verified_candidate_admission(
        (candidate,),
        extra_validator=_pass,
    )
    assert len(evaluation.records) == 1
    return evaluation.records[0].reason


def test_verified_candidate_admission_accepts_only_fully_supported_candidate() -> None:
    candidate = _candidate()

    evaluation = evaluate_verified_candidate_admission(
        (candidate,),
        extra_validator=_pass,
    )

    assert evaluation.version == CANDIDATE_ADMISSION_VERSION
    assert evaluation.admitted_candidates == (candidate,)
    assert evaluation.admitted_count == 1
    assert evaluation.blocked_count == 0
    record = evaluation.records[0]
    assert record.decision is CandidateAdmissionDecision.ADMITTED
    assert record.reason is CandidateAdmissionReason.VERIFIED_SUPPORTED
    assert record.document_id == candidate.document_id
    assert record.field_key == "hts_code"
    assert record.line_item_key == "SKU:ACT-TRAY-18"


@pytest.mark.parametrize(
    ("candidate", "reason"),
    (
        (
            _candidate(verified=False, seed="unverified"),
            CandidateAdmissionReason.EVIDENCE_UNVERIFIED,
        ),
        (
            _candidate(
                evidence_class=EvidenceClass.DERIVED,
                seed="derived",
            ),
            CandidateAdmissionReason.EVIDENCE_NOT_EXPLICIT,
        ),
        (
            _candidate(
                field_key="unsupported_future_field",
                seed="field",
            ),
            CandidateAdmissionReason.FIELD_NOT_ALLOWED,
        ),
        (
            _candidate(
                specialist=SpecialistRole.BOTANICAL,
                seed="specialist",
            ),
            CandidateAdmissionReason.WRONG_SPECIALIST,
        ),
        (
            _candidate(
                field_key="genus",
                value="Pinus",
                document_type=DocumentType.PACKING_LIST,
                specialist=SpecialistRole.BOTANICAL,
                seed="authority",
            ),
            CandidateAdmissionReason.SOURCE_AUTHORITY_UNSUPPORTED,
        ),
        (
            _candidate(
                line_item_key="FP:1234567890ABCDEF12345678",
                seed="fingerprint",
            ),
            CandidateAdmissionReason.LINE_BINDING_AMBIGUOUS,
        ),
        (
            _candidate(
                line_item_key="SKU:",
                seed="malformed-sku",
            ),
            CandidateAdmissionReason.LINE_BINDING_AMBIGUOUS,
        ),
        (
            _candidate(
                line_item_key="LINE:0",
                seed="zero-line",
            ),
            CandidateAdmissionReason.LINE_BINDING_AMBIGUOUS,
        ),
        (
            _candidate(
                line_item_key=(
                    "ROW:00000000-0000-0000-0000-000000000001:"
                    "P1:TINVOICE:R0"
                ),
                seed="foreign-row",
            ),
            CandidateAdmissionReason.LINE_BINDING_AMBIGUOUS,
        ),

        (
            _candidate(
                field_key="bill_of_lading",
                value="OOLU1234567890",
                document_type=DocumentType.BILL_OF_LADING,
                specialist=SpecialistRole.LOGISTICS,
                line_item_key="LINE:1",
                seed="shipment-line",
            ),
            CandidateAdmissionReason.LINE_BINDING_AMBIGUOUS,
        ),
    ),
)
def test_verified_candidate_admission_fails_closed_on_structural_gates(
    candidate: CandidateEnvelope,
    reason: CandidateAdmissionReason,
) -> None:
    evaluation = evaluate_verified_candidate_admission(
        (candidate,),
        extra_validator=_pass,
    )

    assert evaluation.admitted_candidates == ()
    assert evaluation.blocked_count == 1
    assert evaluation.records[0].decision is CandidateAdmissionDecision.BLOCKED
    assert evaluation.records[0].reason is reason


def test_verified_candidate_admission_applies_deterministic_extra_validator_last() -> None:
    candidate = _candidate(seed="invalid-value")

    evaluation = evaluate_verified_candidate_admission(
        (candidate,),
        extra_validator=lambda _item: CandidateAdmissionReason.INVALID_VALUE,
    )

    assert evaluation.admitted_candidates == ()
    assert evaluation.records[0].reason is CandidateAdmissionReason.INVALID_VALUE
