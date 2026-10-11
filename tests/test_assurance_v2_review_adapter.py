"""Transactional V2 review journal contracts; no default production activation."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import select

from litoral_trace.db.models import (
    AssuranceV2Decision, AssuranceV2DecisionSource, AssuranceV2MemoryLink,
    UsLaceyOperationEvent,
)
from litoral_trace.us_lacey.assurance_v2_review_adapter import (
    journal_reviewed_field, review_journal_enabled,
)
from litoral_trace.us_lacey.assurance_v2_memory import evaluate_reuse
from test_assurance_v2_core_memory import (
    session, org_and_actor, supplier_product, shipment,
)


def _review(field, value="Quercus alba"):
    field.human_value = value
    field.field_status = "MATCHED"
    field.reviewed_at = datetime.now(timezone.utc)
    field.validation_status = "VALID"


def test_journal_opt_in_defaults_off(monkeypatch):
    monkeypatch.delenv("US_LACEY_ASSURANCE_V2_REVIEW_JOURNAL", raising=False)
    assert not review_journal_enabled()
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_REVIEW_JOURNAL", "ON")
    assert review_journal_enabled()


def test_accept_is_source_bound_audited_idempotent_and_promoted(session):
    org, actor = org_and_actor(session, "Journal")
    _, product = supplier_product(session, org.id, supplier_key="MID:JOURNAL")
    op, doc, _, rev, field = shipment(
        session, org.id, product.id, reference="REVIEW-1", source_value="Quercus alba"
    )
    _review(field)
    decision = journal_reviewed_field(
        session, organization_id=org.id, operation=op, field=field,
        authenticated_user_id=actor.id, action="accept", reason=None,
    )
    again = journal_reviewed_field(
        session, organization_id=org.id, operation=op, field=field,
        authenticated_user_id=actor.id, action="accept", reason=None,
    )
    assert again.id == decision.id
    assert session.scalars(select(AssuranceV2Decision)).all() == [decision]
    sources = session.scalars(select(AssuranceV2DecisionSource)).all()
    assert len(sources) == 1
    assert sources[0].assurance_document_id == doc.id
    assert sources[0].document_version_number == 1
    assert session.scalar(select(AssuranceV2MemoryLink)) is not None
    events = session.scalars(select(UsLaceyOperationEvent)).all()
    assert len([e for e in events if e.event_type == "HUMAN_REVIEW"]) == 1
    assert field.original_value == "Quercus alba"

    op2, _, _, rev2, _ = shipment(
        session, org.id, product.id, reference="REVIEW-2"
    )
    proposal = evaluate_reuse(
        session, organization_id=org.id, operation_id=op2.id,
        source_set_revision_id=rev2.id, line_reference="1",
        field_name="species",
    )
    assert proposal.eligible
    assert proposal.provenance == "REUSED_HISTORICAL"


def test_correct_requires_reason_and_preserves_original(session):
    org, actor = org_and_actor(session, "Correct")
    _, product = supplier_product(session, org.id, supplier_key="MID:CORRECT")
    op, _, _, _, field = shipment(
        session, org.id, product.id, reference="CORRECT-1", source_value="Quercus rubra"
    )
    _review(field, "Quercus alba")
    with pytest.raises(ValueError, match="reason"):
        journal_reviewed_field(
            session, organization_id=org.id, operation=op, field=field,
            authenticated_user_id=actor.id, action="edit", reason=None,
        )
    assert session.scalar(select(AssuranceV2Decision)) is None
    decision = journal_reviewed_field(
        session, organization_id=org.id, operation=op, field=field,
        authenticated_user_id=actor.id, action="edit",
        reason="Supplier declaration verified by reviewer",
    )
    assert decision.action == "CORRECT"
    assert decision.selected_value == "Quercus alba"
    assert field.original_value == "Quercus rubra"


def test_missing_reused_and_stale_source_fail_closed(session):
    org, actor = org_and_actor(session, "Guard")
    _, product = supplier_product(session, org.id, supplier_key="MID:GUARD")
    op, _, _, rev, field = shipment(
        session, org.id, product.id, reference="GUARD-1", source_value="Quercus alba"
    )
    _review(field)
    field.source_locator = "reused_evidence:123"
    with pytest.raises(ValueError, match="Historical"):
        journal_reviewed_field(
            session, organization_id=org.id, operation=op, field=field,
            authenticated_user_id=actor.id, action="accept", reason=None,
        )
    field.source_locator = "page:1:species"
    rev.is_current = False
    with pytest.raises(ValueError, match="finalized current"):
        journal_reviewed_field(
            session, organization_id=org.id, operation=op, field=field,
            authenticated_user_id=actor.id, action="accept", reason=None,
        )
    assert session.scalar(select(AssuranceV2Decision)) is None


def test_decision_and_audit_rollback_with_review_transaction(session):
    org, actor = org_and_actor(session, "Rollback")
    _, product = supplier_product(session, org.id, supplier_key="MID:ROLLBACK")
    op, _, _, _, field = shipment(
        session, org.id, product.id, reference="ROLLBACK-1", source_value="Quercus alba"
    )
    _review(field)
    journal_reviewed_field(
        session, organization_id=org.id, operation=op, field=field,
        authenticated_user_id=actor.id, action="accept", reason=None,
    )
    assert session.scalar(select(AssuranceV2Decision)) is not None
    session.rollback()
    assert session.scalar(select(AssuranceV2Decision)) is None
    assert session.scalar(select(AssuranceV2MemoryLink)) is None
    assert session.scalar(select(UsLaceyOperationEvent)) is None
