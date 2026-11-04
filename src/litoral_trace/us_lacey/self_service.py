"""Transactional self-service primitives for U.S. Lacey onboarding and billing."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import hashlib
import secrets
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from litoral_trace.auth.passwords import hash_password
from litoral_trace.auth.sessions import hash_refresh_token
from litoral_trace.db.models import UsLaceyPayment, UsLaceySubscription
from litoral_trace.db.tenant import set_tenant_db_context
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


@dataclass(frozen=True)
class UsLaceyBillingSummary:
    plan_code: str
    currency: str
    price_cents: int
    monthly_operation_limit: int
    used_operations: int
    subscription_status: str
    payment_public_id: UUID
    payment_reference: str
    payment_provider: str
    payment_status: str


@dataclass(frozen=True)
class UsLaceyPaymentActivationResult:
    payment_status: str
    subscription_status: str
    account_status: str


@dataclass(frozen=True)
class UsLaceyPilotActivationResult:
    organization_id: int
    previous_account_status: str
    account_status: str
    idempotent: bool


VerificationDelivery = Callable[[str, str, str], None]


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
    verification_delivery: VerificationDelivery | None = None,
) -> UsLaceyRegistrationResult:
    """Create the company and optionally deliver verification before commit.

    The raw email-verification token is returned exactly once to the delivery
    layer. PostgreSQL receives and persists only its SHA-256 digest. When a
    verification delivery callback is supplied, a delivery failure rolls back
    the database transaction so the customer can safely retry signup.
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

        if verification_delivery is not None:
            try:
                verification_delivery(email, legal_name, verification_token)
            except Exception as exc:
                session.rollback()
                raise UsLaceySelfServiceError(
                    "Unable to send the verification email. No account was created."
                ) from exc

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


def get_us_lacey_billing_summary(*, organization_id: int) -> UsLaceyBillingSummary:
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, organization_id)
        subscription = session.execute(
            select(UsLaceySubscription).where(
                UsLaceySubscription.organization_id == organization_id
            )
        ).scalar_one()
        payment = session.execute(
            select(UsLaceyPayment)
            .where(UsLaceyPayment.organization_id == organization_id)
            .order_by(UsLaceyPayment.created_at.desc(), UsLaceyPayment.id.desc())
        ).scalars().first()
        if payment is None:
            raise UsLaceySelfServiceError("No payment record is available for this account.")
        return UsLaceyBillingSummary(
            plan_code=subscription.plan_code,
            currency=subscription.currency,
            price_cents=subscription.price_cents,
            monthly_operation_limit=subscription.monthly_operation_limit,
            used_operations=subscription.used_operations,
            subscription_status=subscription.status,
            payment_public_id=payment.public_id,
            payment_reference=payment.payment_reference,
            payment_provider=payment.provider,
            payment_status=payment.status,
        )
    except UsLaceySelfServiceError:
        raise
    except Exception as exc:
        raise UsLaceySelfServiceError("Unable to load billing information.") from exc
    finally:
        session.close()


def activate_us_lacey_payment(
    *,
    platform_refresh_token: str,
    organization_id: int,
    payment_public_id: UUID,
) -> UsLaceyPaymentActivationResult:
    """Activate a manually confirmed payment through the hardened control plane.

    The caller must possess a valid platform-superadmin refresh session; knowing
    a payment UUID alone is insufficient to activate an account.
    """
    if organization_id <= 0:
        raise UsLaceySelfServiceError("Organization is invalid.")
    refresh_token_hash = hash_refresh_token(platform_refresh_token)
    session = get_us_lacey_db_session()
    try:
        row = session.execute(
            text(
                """
                SELECT * FROM public.us_lacey_verify_payment(
                    :refresh_token_hash,
                    :organization_id,
                    :payment_public_id
                )
                """
            ),
            {
                "refresh_token_hash": refresh_token_hash,
                "organization_id": organization_id,
                "payment_public_id": payment_public_id,
            },
        ).mappings().one()
        session.commit()
        return UsLaceyPaymentActivationResult(
            payment_status=str(row["payment_status"]),
            subscription_status=str(row["subscription_status"]),
            account_status=str(row["account_status"]),
        )
    except Exception as exc:
        session.rollback()
        raise UsLaceySelfServiceError("Unable to verify this payment.") from exc
    finally:
        session.close()


def activate_us_lacey_pilot(
    *, platform_refresh_token: str, organization_id: int, reason: str
) -> UsLaceyPilotActivationResult:
    """Activate a non-paid, audited PILOT entitlement through the control plane."""
    if organization_id <= 0:
        raise UsLaceySelfServiceError("Organization is invalid.")
    normalized_reason = " ".join(str(reason or "").split())
    if not normalized_reason or len(normalized_reason) > 500:
        raise UsLaceySelfServiceError("Pilot activation reason is invalid.")
    session = get_us_lacey_db_session()
    try:
        row = session.execute(
            text("SELECT * FROM public.us_lacey_activate_pilot(:token_hash, :organization_id, :reason)"),
            {"token_hash": hash_refresh_token(platform_refresh_token), "organization_id": organization_id, "reason": normalized_reason},
        ).mappings().one()
        session.commit()
        return UsLaceyPilotActivationResult(
            organization_id=int(row["organization_id"]),
            previous_account_status=str(row["previous_account_status"]),
            account_status=str(row["account_status"]), idempotent=bool(row["idempotent"]),
        )
    except Exception as exc:
        session.rollback()
        raise UsLaceySelfServiceError("Unable to activate this organization as PILOT.") from exc
    finally:
        session.close()
