from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

import litoral_trace.us_lacey.engine2_suggestions as engine2_suggestions
import litoral_trace.us_lacey.operations as operations
from litoral_trace.us_lacey._operations_core import (
    FieldCandidateView,
    OperationDetail,
    OperationFieldView,
)


class _Session:
    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


def test_engine2_compatibility_seam_publishes_only_canonical_truth(monkeypatch):
    session = _Session()
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(engine2_suggestions, "get_us_lacey_db_session", lambda: session)
    monkeypatch.setattr(
        engine2_suggestions,
        "set_tenant_db_context",
        lambda current, organization_id: calls.append(("tenant", (current, organization_id))),
    )

    def publish(current, *, organization_id, operation_id):
        calls.append(("canonical", (current, organization_id, operation_id)))
        return SimpleNamespace(field_count=17)

    monkeypatch.setattr(engine2_suggestions, "publish_canonical_shipment_truth", publish)

    projected = engine2_suggestions.project_engine2_supported_suggestions(
        organization_id=7,
        operation_id=11,
    )

    assert projected == 17
    assert [name for name, _value in calls] == ["tenant", "canonical"]
    assert session.committed is True
    assert session.rolled_back is False
    assert session.closed is True


def test_engine2_compatibility_seam_rolls_back_canonical_failure(monkeypatch):
    session = _Session()
    monkeypatch.setattr(engine2_suggestions, "get_us_lacey_db_session", lambda: session)
    monkeypatch.setattr(engine2_suggestions, "set_tenant_db_context", lambda *_args: None)
    monkeypatch.setattr(
        engine2_suggestions,
        "publish_canonical_shipment_truth",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("canonical failed")),
    )

    with pytest.raises(RuntimeError, match="canonical failed"):
        engine2_suggestions.project_engine2_supported_suggestions(
            organization_id=7,
            operation_id=11,
        )

    assert session.committed is False
    assert session.rolled_back is True
    assert session.closed is True


def _candidate(candidate_id: int, decision: str) -> FieldCandidateView:
    return FieldCandidateView(
        id=candidate_id,
        original_value=f"value-{candidate_id}",
        normalized_value=f"value-{candidate_id}",
        validation_status="VALID",
        validation_error=None,
        confidence=0.9,
        source_document_id=100 + candidate_id,
        source_page=1,
        source_locator=f"candidate:{candidate_id}",
        decision=decision,
    )


def test_customer_detail_hides_rejected_machine_candidates_without_deleting_audit(monkeypatch):
    field = OperationFieldView(
        id=1,
        line_reference="1",
        field_name="hts_code",
        label="HTS Code",
        ppq_number=11,
        scope="PLANT_LINE",
        proposed_value="4407110190",
        effective_value="4407110190",
        status="FOUND",
        confidence=0.9,
        source_assurance_document_id=101,
        source_page=1,
        source_locator="canonical:line-1:candidate",
        extractor="canonical-shipment-truth",
        extractor_version="1",
        reviewed_by_user_id=None,
        reviewed_at=None,
        validation_status="VALID",
        validation_error=None,
        not_required_reason_code=None,
        candidates=(
            _candidate(1, "PENDING"),
            _candidate(2, "REJECTED"),
            _candidate(3, "ACCEPTED"),
        ),
    )
    detail = OperationDetail(
        public_id=uuid4(),
        client_reference="CUTOVER",
        importer_name=None,
        consignee_name=None,
        broker_name=None,
        supplier_name=None,
        operation_date=None,
        status="REVIEW_REQUIRED",
        document_count=1,
        merchandise_line_count=1,
        review_result=None,
        created_at=datetime.now(timezone.utc),
        documents=(),
        fields=(field,),
        plant_declarations=(),
        conflicts=(),
    )
    monkeypatch.setattr(
        operations._CoreUsLaceyOperationService,
        "get_detail",
        lambda *_args, **_kwargs: detail,
    )

    visible = operations.UsLaceyOperationService().get_detail(
        organization_id=7,
        operation_public_id=detail.public_id,
    )

    assert [candidate.decision for candidate in visible.fields[0].candidates] == [
        "PENDING",
        "ACCEPTED",
    ]
    assert [candidate.decision for candidate in detail.fields[0].candidates] == [
        "PENDING",
        "REJECTED",
        "ACCEPTED",
    ]
