"""Assignment and deadline workflow for Assurance operational exceptions."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from litoral_trace.assurance.domain import OperationalExceptionStatus
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import OperationalException, User
from litoral_trace.db.tenant import set_tenant_db_context


SessionFactory = Callable[[], Session | None]


class AssuranceExceptionAssignmentError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ExceptionAssignmentResult:
    exception_public_id: UUID
    assigned_to_user_id: int
    assigned_to_name: str
    due_at: datetime
    status: str


def _normalized_due_at(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("due_at debe ser una fecha y hora válida.")
    normalized = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    normalized = normalized.astimezone(timezone.utc)
    if normalized <= datetime.now(timezone.utc):
        raise ValueError("due_at debe ser posterior al momento actual.")
    return normalized


class AssuranceExceptionAssignmentService:
    """Assign one open exception to a same-tenant active user with a deadline."""

    def __init__(self, *, session_factory: SessionFactory | None = None) -> None:
        self._session_factory = session_factory or get_db_session

    def assign(
        self,
        *,
        organization_id: int,
        exception_public_id: UUID | str,
        assigned_to_user_id: int,
        due_at: datetime,
    ) -> ExceptionAssignmentResult:
        org_id = int(organization_id)
        user_id = int(assigned_to_user_id)
        if org_id <= 0 or user_id <= 0:
            raise ValueError("organization_id y assigned_to_user_id deben ser positivos.")
        public_id = (
            exception_public_id
            if isinstance(exception_public_id, UUID)
            else UUID(str(exception_public_id))
        )
        deadline = _normalized_due_at(due_at)

        session = self._session_factory()
        if session is None:
            raise AssuranceExceptionAssignmentError(
                "No se pudo abrir una sesión para asignar la excepción."
            )
        set_tenant_db_context(session, org_id)
        try:
            row = session.scalar(
                select(OperationalException).where(
                    OperationalException.organization_id == org_id,
                    OperationalException.public_id == public_id,
                )
            )
            if row is None:
                raise AssuranceExceptionAssignmentError("Excepción operativa no encontrada.")
            if row.status not in {
                OperationalExceptionStatus.OPEN.value,
                OperationalExceptionStatus.IN_PROGRESS.value,
            }:
                raise AssuranceExceptionAssignmentError(
                    "Sólo una excepción abierta o en progreso puede asignarse."
                )

            assignee = session.scalar(
                select(User).where(
                    User.id == user_id,
                    User.organization_id == org_id,
                    User.is_active.is_(True),
                )
            )
            if assignee is None:
                raise AssuranceExceptionAssignmentError(
                    "El responsable no existe, está inactivo o pertenece a otra organización."
                )

            display_name = (
                str(assignee.full_name or "").strip()
                or str(assignee.username or "").strip()
                or str(assignee.email or "").strip()
            )
            row.assigned_to_user_id = assignee.id
            row.assigned_to_name = display_name
            row.due_at = deadline
            row.status = OperationalExceptionStatus.IN_PROGRESS.value
            session.commit()

            return ExceptionAssignmentResult(
                exception_public_id=public_id,
                assigned_to_user_id=assignee.id,
                assigned_to_name=display_name,
                due_at=deadline,
                status=row.status,
            )
        except (ValueError, AssuranceExceptionAssignmentError):
            session.rollback()
            raise
        except Exception as exc:
            session.rollback()
            raise AssuranceExceptionAssignmentError(
                "No se pudo asignar la excepción operativa."
            ) from exc
        finally:
            session.close()
