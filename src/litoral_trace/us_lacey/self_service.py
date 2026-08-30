"""Transactional self-service primitives for U.S. Lacey onboarding and billing."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import secrets
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from litoral_trace.auth.passwords import hash_password
from litoral_trace.us_lacey.commercial import (
    UsLaceyCommercialConfig,
    load_us_lacey_commercial_config,
)
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.domain import UsLaceyBusinessType


class UsLaceySelfServiceError(RuntimeError):
    """Sanitized error safe to surface from the self-service layer."""


@dataclass(frozen=True)
class UsLaceyRegistrationResult:
    organization_id: int
    user_id: int
    payment_public_id: UUID
    payment_reference: str
    amount_cents: int
    account_status: str
    verification_token: str


@dataclass(frozen=True)
class UsLaceyEmailVerificationResult:
    organization_id: int
    user_id: int
    account_status: str


def _verification_token_hash(token: str) -> str:
    normalized = str(token or "").strip()
    if not normalized:
        raise UsLaceySelfServiceError("Verification token is required.")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _normalize_email(value: str) -> str:
    email = str(value or "").strip().lower()
    if not email or len(email) > 255 or "@" not in email:
        raise UsLaceySelfServiceError("Enter a valid business email.")
    local, _, domain = email.partition("@")
    if not local or "." not in domain or domain.startswith(".") or domain.endswith("."):
        raise UsLaceySelfServiceError("Enter a valid business email.")
    return email


def _normalize_legal_name(value: str) -> str:
    legal_name = " ".join(str(value or "").split())
    if not legal_name or len(legal_name) > 255:
        raise UsLaceySelfServiceError("Company legal name is required.")
    return legal_name


def _normalize_business_type(value: str | UsLaceyBusinessType) -> str:
    normalized = str(value).strip().upper()
    allowed = {item.value for item in UsLaceyBusinessType}
    if normalized not in allowed:
        raise UsLaceySelfServiceError("Business type is invalid.")
    return normalized


def register_us_lacey_company(
    *,
    legal_name: str,
    business_type: str | UsLaceyBusinessType,
    admin_name: str,
    admin_email: str,
    password: str,
    commercial_config: UsLaceyCommercialConfig | None = None,
) -> UsLaceyRegistrationResult:
    """Atomically create company, admin, subscription, payment and legal record.

    The raw email-verification token is returned exactly once to the delivery
    layer. PostgreSQL receives and persists only its SHA-256 digest.
    """
    legal_name = _normalize_legal_name(legal_name)
    email = _normalize_email(admin_email)
    business_type = _normalize_business_type(business_type)
    display_name = " ".join(str(admin_name or "").split())
    if not display_name or len(display_name) > 255:
        raise UsLaceySelfServiceError("Administrator name is required.")
    if len(str(password or "")) < 12:
        raise UsLaceySelfServiceError("Password must contain at least 12 characters.")

    config = commercial_config or load_us_lacey_commercial_config()
    verification_token = secrets.token_urlsafe(32)
    verification_hash = _verification_token_hash(verification_token)
    password_hash = hash_password(password)

    session = get_us_lacey_db_session()
    try:
        row = session.execute(
            text(
                """
                SELECT * FROM public.us_lacey_self_register(
                    :legal_name,
                    :business_type,
                    :admin_name,
                    :admin_email,
                    :password_hash,
                    :verification_hash,
                    :price_cents,
                    :monthly_operation_limit,
                    :payment_provider,
                    :terms_version,
                    :privacy_version,
                    :beta_terms_version
                )
                """
            ),
            {
                "legal_name": legal_name,
                "business_type": business_type,
                "admin_name": display_name,
                "admin_email": email,
                "password_hash": password_hash,
                "verification_hash": verification_hash,
                "price_cents": config.price_cents,
                "monthly_operation_limit": config.monthly_operation_limit,
                "payment_provider": config.payment_provider,
                "terms_version": config.terms_version,
                "privacy_version": config.privacy_version,
                "beta_terms_version": config.beta_terms_version,
            },
        ).mappings().one()
        session.commit()
        return UsLaceyRegistrationResult(
            organization_id=int(row["organization_id"]),
            user_id=int(row["user_id"]),
            payment_public_id=UUID(str(row["payment_public_id"])),
            payment_reference=str(row["payment_reference"]),
            amount_cents=int(row["amount_cents"]),
            account_status=str(row["account_status"]),
            verification_token=verification_token,
        )
    except IntegrityError as exc:
        session.rollback()
        raise UsLaceySelfServiceError(
            "An account already exists for this login identity or payment reference."
        ) from exc
    except UsLaceySelfServiceError:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise UsLaceySelfServiceError("Unable to create the U.S. Lacey account.") from exc
    finally:
        session.close()


def verify_us_lacey_email(token: str) -> UsLaceyEmailVerificationResult:
    token_hash = _verification_token_hash(token)
    session = get_us_lacey_db_session()
    try:
        row = session.execute(
            text("SELECT * FROM public.us_lacey_verify_email(:token_hash)"),
            {"token_hash": token_hash},
        ).mappings().one()
        session.commit()
        return UsLaceyEmailVerificationResult(
            organization_id=int(row["organization_id"]),
            user_id=int(row["user_id"]),
            account_status=str(row["account_status"]),
        )
    except Exception as exc:
        session.rollback()
        raise UsLaceySelfServiceError("Verification link is invalid or expired.") from exc
    finally:
        session.close()
