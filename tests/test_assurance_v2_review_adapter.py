"""Transactional V2 review journal contracts; no default production activation."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

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



def test_v2_review_rejects_unreasoned_correction_before_opening_database(monkeypatch):
    """An enabled V2 gate cannot record a correction with no human reason."""
    from litoral_trace.us_lacey import review as review_module

    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_REVIEW_JOURNAL", "ON")

    def database_must_not_open():
        pytest.fail("V2 correction validation must precede DB access")

    monkeypatch.setattr(review_module, "get_us_lacey_db_session", database_must_not_open)
    with pytest.raises(review_module.UsLaceyReviewError, match="Explain the correction"):
        review_module.review_us_lacey_field(
            organization_id=1, operation_public_id="not-used",
            field_id=1, user_id=1, user_email="reviewer@example.test",
            action="edit", value="Phyllostachys", reason_code=" ",
        )


def test_v2_review_not_applicable_must_not_be_misrepresented_as_accept(monkeypatch):
    from litoral_trace.us_lacey import review as review_module

    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_REVIEW_JOURNAL", "ON")
    monkeypatch.setattr(
        review_module, "get_us_lacey_db_session",
        lambda: pytest.fail("Unsupported V2 action must be rejected before DB"),
    )
    with pytest.raises(review_module.UsLaceyReviewError, match="NOT_REQUIRED"):
        review_module.review_us_lacey_field(
            organization_id=1, operation_public_id="not-used",
            field_id=1, user_id=1, user_email="reviewer@example.test",
            action="not_required", reason_code="NOT_PAPER_OR_PAPERBOARD",
        )


def test_review_dialog_exposes_explicit_correction_reason():
    from pathlib import Path

    template = (
        Path(__file__).resolve().parents[1]
        / "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html"
    ).read_text(encoding="utf-8")
    assert 'name="reason_code" rows="2"' in template
    assert "Reason for correction" in template
    assert 'name="action" value="edit"' in template



def test_authenticated_review_path_does_not_duplicate_legacy_promotion(session, monkeypatch):
    """Actual review command, not just the helper, uses one V2 authority path."""
    from litoral_trace.us_lacey import review as review_module

    org, actor = org_and_actor(session, "OneAuthority")
    _, product = supplier_product(session, org.id, supplier_key="MID:ONE-AUTHORITY")
    op, _, _, _, field = shipment(
        session, org.id, product.id, reference="AUTHORITY-SHIP-1",
        source_value="Quercus alba",
    )
    session.commit()
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_REVIEW_JOURNAL", "ON")
    monkeypatch.setattr(review_module, "get_us_lacey_db_session", lambda: session)
    monkeypatch.setattr(review_module, "set_tenant_db_context", lambda *_: None)

    def legacy_promotion_must_not_run(*args, **kwargs):
        pytest.fail("V2 human authority must not execute legacy promotion")

    monkeypatch.setattr(review_module, "promote_reviewed_field", legacy_promotion_must_not_run)
    # The standalone fixture focuses on the authority boundary. Readiness
    # recalculation has its own integration tests and is not part of this case.
    monkeypatch.setattr(review_module, "reconcile_entered_value_invariant", lambda *a, **kw: None)
    monkeypatch.setattr(review_module, "refresh_regulatory_assessment_snapshot_for_review", lambda *a, **kw: None)
    monkeypatch.setattr(review_module, "refresh_us_lacey_operation_status", lambda *a, **kw: "REVIEW_REQUIRED")
    monkeypatch.setattr(review_module, "_counts", lambda *a, **kw: (0, 0, 0))
    review_audits = []
    monkeypatch.setattr(
        review_module, "record_audit_event",
        lambda *a, **kw: review_audits.append(kw),
    )

    result = review_module.review_us_lacey_field(
        organization_id=org.id, operation_public_id=op.public_id,
        field_id=field.id, user_id=actor.id, user_email=actor.email,
        action="accept",
    )
    assert result.field_status == "MATCHED"
    assert len(review_audits) == 1
    assert review_audits[0]["metadata"]["reusable_evidence_promoted"] is True
    assert review_audits[0]["metadata"]["reusable_evidence_promotion_reason"] == "v2_verified_memory"
    with Session(session.get_bind()) as verify:
        assert len(verify.scalars(select(AssuranceV2Decision)).all()) == 1
        assert len(verify.scalars(select(AssuranceV2MemoryLink)).all()) == 1
        event_types = [event.event_type for event in verify.scalars(select(UsLaceyOperationEvent))]
        assert event_types.count("HUMAN_REVIEW") == 2  # V2 authority + legacy UI audit, both same transaction


def test_v2_review_rejects_stale_case_before_mutating_field(session, monkeypatch):
    from litoral_trace.us_lacey import review as review_module

    org, actor = org_and_actor(session, "StaleReview")
    _, product = supplier_product(session, org.id, supplier_key="MID:STALE-REVIEW")
    op, _, _, rev, field = shipment(
        session, org.id, product.id, reference="STALE-REVIEW-1",
        source_value="Quercus alba",
    )
    session.commit()
    field_id = field.id
    monkeypatch.setenv("US_LACEY_ASSURANCE_V2_REVIEW_JOURNAL", "ON")
    monkeypatch.setattr(review_module, "get_us_lacey_db_session", lambda: session)
    monkeypatch.setattr(review_module, "set_tenant_db_context", lambda *_: None)

    with pytest.raises(review_module.UsLaceyReviewError, match="stale"):
        review_module.review_us_lacey_field(
            organization_id=org.id, operation_public_id=op.public_id,
            field_id=field.id, user_id=actor.id, user_email=actor.email,
            action="accept", expected_source_set_fingerprint="stale",
            expected_case_fingerprint="stale",
        )
    with Session(session.get_bind()) as verify:
        persisted = verify.get(type(field), field_id)
        assert persisted.human_value is None
        assert verify.scalar(select(AssuranceV2Decision)) is None
