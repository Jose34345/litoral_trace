"""Product-led five-shipment evaluation lifecycle."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import re

from sqlalchemy import func, select, text

from litoral_trace.db.models import (
    UsLaceyEvaluation,
    UsLaceyOperation,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.worker_db import get_us_lacey_worker_db_session


EVALUATION_OPERATION_LIMIT = 5
EVALUATION_INACTIVITY_DAYS = 7
EVALUATION_RAW_RETENTION_HOURS = 4
_FREE_EMAIL_DOMAINS = frozenset(
    {
        "gmail.com",
        "googlemail.com",
        "yahoo.com",
        "yahoo.co.uk",
        "hotmail.com",
        "outlook.com",
        "live.com",
        "msn.com",
        "icloud.com",
        "me.com",
        "mac.com",
        "aol.com",
        "proton.me",
        "protonmail.com",
        "gmx.com",
        "mail.com",
        "yandex.com",
        "zoho.com",
    }
)
_EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


class UsLaceyEvaluationError(RuntimeError):
    """Safe product-led evaluation failure."""


@dataclass(frozen=True, slots=True)
class UsLaceyEvaluationState:
    organization_id: int
    status: str
    work_email: str | None
    operation_limit: int
    successful_operations_used: int
    claimed_at: datetime | None
    last_activity_at: datetime
    inactive_expires_at: datetime | None

    @property
    def remaining_operations(self) -> int:
        return max(0, self.operation_limit - self.successful_operations_used)

    @property
    def claimed(self) -> bool:
        return self.status in {"ACTIVE", "EXHAUSTED"}

    @property
    def read_only(self) -> bool:
        return self.status in {"EXHAUSTED", "EXPIRED"}

    @property
    def can_claim(self) -> bool:
        return self.status == "ANONYMOUS" and self.successful_operations_used >= 1


@dataclass(frozen=True, slots=True)
class UsLaceyEvaluationClaimResult:
    organization_id: int
    user_id: int
    status: str
    successful_operations_used: int
    operation_limit: int
    inactive_expires_at: datetime


@dataclass(frozen=True, slots=True)
class UsLaceyEvaluationUsageResult:
    counted: bool
    successful_operations_used: int
    status: str


def _token_hash(raw_token: str) -> str:
    token = str(raw_token or "").strip()
    if not token:
        raise UsLaceyEvaluationError("Your evaluation session is unavailable.")
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def normalize_work_email(value: str) -> str:
    email = str(value or "").strip().lower()
    if (
        not email
        or len(email) > 255
        or _EMAIL_RE.fullmatch(email) is None
    ):
        raise UsLaceyEvaluationError("Enter a valid work email.")
    domain = email.rsplit("@", 1)[1]
    if domain in _FREE_EMAIL_DOMAINS:
        raise UsLaceyEvaluationError(
            "Use your work email to save this evaluation."
        )
    return email


def _state_from_model(row: UsLaceyEvaluation) -> UsLaceyEvaluationState:
    return UsLaceyEvaluationState(
        organization_id=int(row.organization_id),
        status=str(row.status),
        work_email=(str(row.work_email) if row.work_email else None),
        operation_limit=int(row.operation_limit),
        successful_operations_used=int(row.successful_operations_used),
        claimed_at=row.claimed_at,
        last_activity_at=row.last_activity_at,
        inactive_expires_at=row.inactive_expires_at,
    )


def get_evaluation_state(
    *,
    organization_id: int,
    session_factory=None,
) -> UsLaceyEvaluationState | None:
    org_id = int(organization_id)
    factory = session_factory or get_us_lacey_db_session
    session = factory()
    try:
        set_tenant_db_context(session, org_id)
        row = session.scalar(
            select(UsLaceyEvaluation).where(
                UsLaceyEvaluation.organization_id == org_id
            )
        )
        return None if row is None else _state_from_model(row)
    finally:
        session.close()


def require_evaluation_creation_capacity(
    *,
    organization_id: int,
    session_factory=None,
) -> UsLaceyEvaluationState | None:
    """Prevent parallel operation creation from bypassing the 1/5 shipment cap."""
    org_id = int(organization_id)
    factory = session_factory or get_us_lacey_db_session
    session = factory()
    try:
        set_tenant_db_context(session, org_id)
        evaluation = session.scalar(
            select(UsLaceyEvaluation).where(
                UsLaceyEvaluation.organization_id == org_id
            )
        )
        if evaluation is None:
            return None

        state = _state_from_model(evaluation)
        if state.status == "EXPIRED":
            raise UsLaceyEvaluationError("This evaluation has expired.")
        if state.status == "EXHAUSTED":
            raise UsLaceyEvaluationError(
                "Your 5-shipment evaluation is complete. "
                "Continue with Litoral Trace to create another operation."
            )

        allowed = 1 if state.status == "ANONYMOUS" else state.operation_limit
        non_failed = int(
            session.scalar(
                select(func.count(UsLaceyOperation.id)).where(
                    UsLaceyOperation.organization_id == org_id,
                    UsLaceyOperation.status != "FAILED",
                )
            )
            or 0
        )
        if non_failed >= allowed:
            if state.status == "ANONYMOUS" and state.successful_operations_used:
                raise UsLaceyEvaluationError(
                    "Save this evaluation to test 4 more shipments."
                )
            raise UsLaceyEvaluationError(
                "This evaluation has no available shipment slots right now."
            )
        return state
    finally:
        session.close()


def claim_evaluation(
    *,
    session_token: str,
    work_email: str,
    session_factory=None,
) -> UsLaceyEvaluationClaimResult:
    email = normalize_work_email(work_email)
    factory = session_factory or get_us_lacey_db_session
    session = factory()
    try:
        row = session.execute(
            text(
                "SELECT * FROM public.us_lacey_evaluation_claim("
                ":token_hash, :work_email)"
            ),
            {"token_hash": _token_hash(session_token), "work_email": email},
        ).mappings().one()
        session.commit()
        return UsLaceyEvaluationClaimResult(
            organization_id=int(row["organization_id"]),
            user_id=int(row["user_id"]),
            status=str(row["evaluation_status"]),
            successful_operations_used=int(row["successful_operations_used"]),
            operation_limit=int(row["operation_limit"]),
            inactive_expires_at=row["inactive_expires_at"],
        )
    except UsLaceyEvaluationError:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        message = str(getattr(exc, "orig", exc)).casefold()
        if "work email" in message:
            safe = "Use your work email to save this evaluation."
        elif "process one shipment" in message:
            safe = "Process one shipment before saving the evaluation."
        elif "already exists" in message:
            safe = "An account already exists for this work email."
        elif "already been claimed" in message:
            safe = "This evaluation has already been saved."
        else:
            safe = "Unable to save this evaluation right now."
        raise UsLaceyEvaluationError(safe) from exc
    finally:
        session.close()


def touch_evaluation_activity(
    *,
    session_token: str,
    organization_id: int,
    session_factory=None,
) -> None:
    """Slide claimed evaluation expiry by seven days of inactivity."""
    factory = session_factory or get_us_lacey_db_session
    session = factory()
    try:
        session.execute(
            text(
                "SELECT * FROM public.us_lacey_evaluation_touch("
                ":token_hash, :organization_id)"
            ),
            {
                "token_hash": _token_hash(session_token),
                "organization_id": int(organization_id),
            },
        ).mappings().all()
        session.commit()
    except Exception as exc:
        session.rollback()
        message = str(getattr(exc, "orig", exc)).casefold()
        if "evaluation has expired" in message:
            raise UsLaceyEvaluationError("This evaluation has expired.") from exc
        # Paid and anonymous workspaces legitimately return no row.
        if "invalid or expired" in message:
            raise UsLaceyEvaluationError(
                "Your evaluation session is unavailable."
            ) from exc
        raise UsLaceyEvaluationError(
            "Unable to refresh this evaluation right now."
        ) from exc
    finally:
        session.close()


def mark_evaluation_operation_successful(
    *,
    organization_id: int,
    operation_id: int,
    session_factory=None,
) -> UsLaceyEvaluationUsageResult | None:
    """Count one successful operation exactly once; reprocessing is free."""
    factory = session_factory or get_us_lacey_worker_db_session
    session = factory()
    try:
        row = session.execute(
            text(
                "SELECT * FROM public.us_lacey_evaluation_mark_operation_success("
                ":organization_id, :operation_id)"
            ),
            {
                "organization_id": int(organization_id),
                "operation_id": int(operation_id),
            },
        ).mappings().one_or_none()
        session.commit()
        if row is None:
            return None
        return UsLaceyEvaluationUsageResult(
            counted=bool(row["counted"]),
            successful_operations_used=int(row["successful_operations_used"]),
            status=str(row["evaluation_status"]),
        )
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
