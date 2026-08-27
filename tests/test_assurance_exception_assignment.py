from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from litoral_trace.assurance.domain import (
    OperationalExceptionPriority,
    OperationalExceptionSource,
    OperationalExceptionStatus,
    ReconciliationSeverity,
)
from litoral_trace.assurance.exception_assignment import (
    AssuranceExceptionAssignmentError,
    AssuranceExceptionAssignmentService,
)
from litoral_trace.db.models import OperationalException, Organization, User


def _factory():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    Organization.__table__.create(engine, checkfirst=True)
    User.__table__.create(engine, checkfirst=True)
    OperationalException.__table__.create(engine, checkfirst=True)
    return sessionmaker(bind=engine, expire_on_commit=False)


def _seed(factory):
    session: Session = factory()
    org_a = Organization(name="Org A", slug="org-a", tax_id="30-11111111-1")
    org_b = Organization(name="Org B", slug="org-b", tax_id="30-22222222-2")
    session.add_all([org_a, org_b])
    session.flush()
    user_a = User(
        organization_id=org_a.id,
        email="a@example.com",
        username="operator-a",
        password_hash="not-used",
        role="manager",
        full_name="Operador A",
        is_active=True,
    )
    user_b = User(
        organization_id=org_b.id,
        email="b@example.com",
        username="operator-b",
        password_hash="not-used",
        role="manager",
        full_name="Operador B",
        is_active=True,
    )
    inactive = User(
        organization_id=org_a.id,
        email="inactive@example.com",
        username="inactive-a",
        password_hash="not-used",
        role="manager",
        full_name="Inactivo A",
        is_active=False,
    )
    session.add_all([user_a, user_b, inactive])
    session.flush()
    exception = OperationalException(
        organization_id=org_a.id,
        fingerprint="e" * 64,
        source_type=OperationalExceptionSource.MANUAL.value,
        operation_reference="shipment:abc",
        cause_code="OWNER_REQUIRED",
        entity_type="OPERATION",
        entity_reference="shipment:abc",
        title="Asignar responsable",
        description="La excepción requiere dueño y plazo.",
        impact=ReconciliationSeverity.WARNING.value,
        priority=OperationalExceptionPriority.HIGH.value,
        status=OperationalExceptionStatus.OPEN.value,
        recommended_action="Asignar responsable y fecha límite.",
    )
    session.add(exception)
    session.commit()
    result = {
        "org_a": org_a.id,
        "org_b": org_b.id,
        "user_a": user_a.id,
        "user_b": user_b.id,
        "inactive": inactive.id,
        "exception": exception.public_id,
    }
    session.close()
    return result


def test_assignment_sets_same_tenant_owner_deadline_and_in_progress_status():
    factory = _factory()
    data = _seed(factory)
    due_at = datetime.now(timezone.utc) + timedelta(days=2)

    result = AssuranceExceptionAssignmentService(session_factory=factory).assign(
        organization_id=data["org_a"],
        exception_public_id=data["exception"],
        assigned_to_user_id=data["user_a"],
        due_at=due_at,
    )

    assert result.assigned_to_user_id == data["user_a"]
    assert result.assigned_to_name == "Operador A"
    assert result.status == OperationalExceptionStatus.IN_PROGRESS.value
    assert result.due_at == due_at

    session: Session = factory()
    stored = session.scalar(select(OperationalException))
    assert stored.assigned_to_user_id == data["user_a"]
    assert stored.assigned_to_name == "Operador A"
    assert stored.status == OperationalExceptionStatus.IN_PROGRESS.value
    assert stored.due_at is not None
    session.close()


def test_assignment_rejects_cross_tenant_assignee():
    factory = _factory()
    data = _seed(factory)

    with pytest.raises(AssuranceExceptionAssignmentError, match="otra organización"):
        AssuranceExceptionAssignmentService(session_factory=factory).assign(
            organization_id=data["org_a"],
            exception_public_id=data["exception"],
            assigned_to_user_id=data["user_b"],
            due_at=datetime.now(timezone.utc) + timedelta(days=1),
        )


def test_assignment_rejects_inactive_assignee():
    factory = _factory()
    data = _seed(factory)

    with pytest.raises(AssuranceExceptionAssignmentError, match="inactivo"):
        AssuranceExceptionAssignmentService(session_factory=factory).assign(
            organization_id=data["org_a"],
            exception_public_id=data["exception"],
            assigned_to_user_id=data["inactive"],
            due_at=datetime.now(timezone.utc) + timedelta(days=1),
        )


def test_assignment_rejects_past_deadline():
    factory = _factory()
    data = _seed(factory)

    with pytest.raises(ValueError, match="posterior"):
        AssuranceExceptionAssignmentService(session_factory=factory).assign(
            organization_id=data["org_a"],
            exception_public_id=data["exception"],
            assigned_to_user_id=data["user_a"],
            due_at=datetime.now(timezone.utc) - timedelta(minutes=1),
        )


def test_resolved_exception_cannot_be_reassigned():
    factory = _factory()
    data = _seed(factory)
    session: Session = factory()
    stored = session.scalar(select(OperationalException))
    stored.status = OperationalExceptionStatus.RESOLVED.value
    session.commit()
    session.close()

    with pytest.raises(AssuranceExceptionAssignmentError, match="abierta o en progreso"):
        AssuranceExceptionAssignmentService(session_factory=factory).assign(
            organization_id=data["org_a"],
            exception_public_id=data["exception"],
            assigned_to_user_id=data["user_a"],
            due_at=datetime.now(timezone.utc) + timedelta(days=1),
        )
