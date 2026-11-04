"""Explicit human filing-authorization contract on an isolated SQLite fixture.

This tests the transaction/authority boundary, NOT a CBP filing or the
13-original-PDF + PostgreSQL end-to-end acceptance gate.
"""
from __future__ import annotations

from dataclasses import replace
from sqlalchemy import func, select

import pytest

from litoral_trace.db.models import UsLaceyOperationEvent
from litoral_trace.us_lacey.assurance_v2_export_authorization import (
    authorization_from_event, explicitly_authorize_assurance_export,
)
from litoral_trace.us_lacey.assurance_v2_issuance import issue_locked_assurance_package
from litoral_trace.us_lacey.exporters.assurance_v2 import evaluate_filing_gate
from test_assurance_v2_issuance import _prepare
from test_assurance_v2_core_memory import session


def _authorize(org, actor, op, revision, factory, case, *,
               explicit_confirmation=True, reason="Reviewed and approve this exact filing-ready case"):
    return explicitly_authorize_assurance_export(
        organization_id=org.id, operation_public_id=str(op.public_id),
        authenticated_user_id=actor.id,
        expected_source_set_fingerprint=revision.source_set_fingerprint,
        expected_case_fingerprint=case.authority_fingerprint(),
        reason=reason, explicit_confirmation=explicit_confirmation,
        session_factory=factory, case_loader=lambda *_: case,
    )


def test_authorization_requires_separate_user_confirmation(session):
    org, actor, op, rev, factory, authorized_case = _prepare(session)
    case = replace(authorized_case, export_authorization={})
    assert [b.code for b in evaluate_filing_gate(case).blockers] == ["EXPORT_NOT_AUTHORIZED"]
    with pytest.raises(ValueError, match="Explicit reviewer confirmation"):
        _authorize(org, actor, op, rev, factory, case, explicit_confirmation=False)
    with pytest.raises(ValueError, match="Explicit reviewer confirmation"):
        _authorize(org, actor, op, rev, factory, case, reason="")
    with factory() as db:
        assert db.scalar(select(func.count(UsLaceyOperationEvent.id))) == 0

    receipt = _authorize(org, actor, op, rev, factory, case)
    assert receipt["status"] == "AUTHORIZED"
    assert receipt["actor_id"] == f"user:{actor.id}"
    assert receipt["case_fingerprint"] == case.authority_fingerprint()
    with factory() as db:
        found = authorization_from_event(
            db, organization_id=org.id, operation_id=op.id, case=case,
        )
        assert found == receipt
        assert db.scalar(select(func.count(UsLaceyOperationEvent.id))) == 1
    # Duplicate requests by the SAME actor and exact digest are idempotent.
    assert _authorize(org, actor, op, rev, factory, case) == receipt
    with factory() as db:
        assert db.scalar(select(func.count(UsLaceyOperationEvent.id))) == 1


def test_explicit_authorization_is_fenced_by_case_digest_and_source_revision(session):
    org, actor, op, rev, factory, authorized_case = _prepare(session)
    case = replace(authorized_case, export_authorization={})
    _authorize(org, actor, op, rev, factory, case)
    with factory() as db:
        new_fields = list(case.fields)
        new_fields[0] = {**new_fields[0], "value": "Different value"}
        changed = replace(case, fields=tuple(new_fields))
        assert not authorization_from_event(
            db, organization_id=org.id, operation_id=op.id, case=changed,
        )
        newer = replace(
            case, source_set_revision=rev.id + 1,
            current_source_set_revision=rev.id + 1,
        )
        assert not authorization_from_event(
            db, organization_id=org.id, operation_id=op.id, case=newer,
        )
        assert not authorization_from_event(
            db, organization_id=org.id + 123456, operation_id=op.id, case=case,
        )


def test_export_authorization_never_overrides_unresolved_fields(session):
    org, actor, op, rev, factory, authorized_case = _prepare(session)
    case = replace(authorized_case, export_authorization={})
    fields = list(case.fields)
    fields[0] = {**fields[0], "authority": "CANDIDATE"}
    invalid_case = replace(case, fields=tuple(fields))
    with pytest.raises(ValueError, match="Filing prerequisites"):
        _authorize(org, actor, op, rev, factory, invalid_case)
    with factory() as db:
        assert db.scalar(select(func.count(UsLaceyOperationEvent.id))) == 0


def test_locked_issuance_requires_separate_persisted_export_authorization(session):
    org, actor, op, rev, factory, authorized_case = _prepare(session)
    case = replace(authorized_case, export_authorization={})

    def read_with_event(db, org_id, _uuid):
        return replace(case, export_authorization=authorization_from_event(
            db, organization_id=org_id, operation_id=op.id, case=case,
        ))

    with pytest.raises(PermissionError, match="explicit authorization"):
        issue_locked_assurance_package(
            organization_id=org.id, operation_public_id=str(op.public_id),
            authenticated_user_id=actor.id,
            expected_source_set_fingerprint=rev.source_set_fingerprint,
            expected_case_fingerprint=case.authority_fingerprint(),
            session_factory=factory, case_loader=read_with_event,
        )
    _authorize(org, actor, op, rev, factory, case)
    result = issue_locked_assurance_package(
        organization_id=org.id, operation_public_id=str(op.public_id),
        authenticated_user_id=actor.id,
        expected_source_set_fingerprint=rev.source_set_fingerprint,
        expected_case_fingerprint=case.authority_fingerprint(),
        session_factory=factory, case_loader=read_with_event,
    )
    assert result["status"] == "FILING_READY"
    assert result["submitted_to_agency"] is False
    with factory() as db:
        assert db.scalar(select(func.count(UsLaceyOperationEvent.id))) == 2
