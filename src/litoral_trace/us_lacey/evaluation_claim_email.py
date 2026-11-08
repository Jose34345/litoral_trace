"""Email ownership proof before attaching a work address to an evaluation.

One-use token hashes live in the existing restricted verification table.
No account email changes until the recipient explicitly submits the proof.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import secrets
from typing import Callable

from sqlalchemy import text

from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.evaluation import (
    UsLaceyEvaluationClaimResult,
    UsLaceyEvaluationError,
    normalize_work_email,
)


VerificationDelivery = Callable[[str, str], None]


def _hash_token(raw: str) -> str:
    token = str(raw or "").strip()
    if not token:
        raise UsLaceyEvaluationError("Verification link is invalid or expired.")
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def request_evaluation_claim_email(
    *, session_token: str, work_email: str,
    verification_delivery: VerificationDelivery,
    session_factory=None,
) -> str:
    """Send a 30-minute challenge without claiming an unverified email."""
    email = normalize_work_email(work_email)
    challenge = secrets.token_urlsafe(32)
    factory = session_factory or get_us_lacey_db_session
    session = factory()
    try:
        session.execute(
            text(
                "SELECT * FROM public.us_lacey_evaluation_claim_begin("
                ":session_hash,:email,:challenge_hash)"
            ),
            {
                "session_hash": _hash_token(session_token),
                "email": email,
                "challenge_hash": _hash_token(challenge),
            },
        ).mappings().one()
        # As with normal signup, a failed delivery rolls back the challenge.
        verification_delivery(email, challenge)
        session.commit()
        return email
    except Exception as exc:
        session.rollback()
        if isinstance(exc, UsLaceyEvaluationError):
            raise
        reason = str(getattr(exc, "orig", exc)).casefold()
        if "verification email limit" in reason:
            safe = "Too many verification requests. Try again later."
        elif "evaluation cannot be saved" in reason:
            safe = "Process one shipment before saving the evaluation."
        else:
            safe = "Unable to send a verification link right now."
        raise UsLaceyEvaluationError(safe) from exc
    finally:
        session.close()


def complete_evaluation_claim_email(
    *, session_token: str, verification_token: str, session_factory=None,
) -> UsLaceyEvaluationClaimResult:
    """Consume the proof and apply the existing atomic evaluation claim."""
    factory = session_factory or get_us_lacey_db_session
    session = factory()
    try:
        row = session.execute(
            text(
                "SELECT * FROM public.us_lacey_evaluation_claim_finish("
                ":session_hash,:challenge_hash)"
            ),
            {
                "session_hash": _hash_token(session_token),
                "challenge_hash": _hash_token(verification_token),
            },
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
    except Exception as exc:
        session.rollback()
        raise UsLaceyEvaluationError(
            "Verification link is invalid or expired. Request a new link from the same workspace."
        ) from exc
    finally:
        session.close()
