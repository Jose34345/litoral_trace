"""Stateless CSRF tokens for server-rendered and HTMX frontend mutations."""
from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import hmac
import json
import secrets
import time
from typing import Any

from fastapi import HTTPException, Request, status

from litoral_trace.config import get_settings


CSRF_HEADER_NAME = "X-CSRF-Token"
CSRF_FORM_FIELD = "csrf_token"
CSRF_TOKEN_VERSION = 1
CSRF_MAX_AGE_SECONDS = 60 * 60
CSRF_CLOCK_SKEW_SECONDS = 60
_CSRF_KEY_CONTEXT = b"litoral-trace:csrf:v1"


class CsrfConfigurationError(RuntimeError):
    """Raised when CSRF signing cannot be configured safely."""


@dataclass(frozen=True)
class CsrfSubject:
    """Identity tuple bound into an authenticated CSRF token."""

    username: str
    organization_id: int
    session_id: int


def csrf_subject_from_user(user: Any) -> CsrfSubject:
    username = str(getattr(user, "username", "") or "").strip()

    try:
        organization_id = int(getattr(user, "organization_id", 0) or 0)
        session_id = int(getattr(user, "session_id", 0) or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid authenticated CSRF subject.") from exc

    if not username or organization_id <= 0 or session_id <= 0:
        raise ValueError("Authenticated CSRF subject is incomplete.")

    return CsrfSubject(
        username=username,
        organization_id=organization_id,
        session_id=session_id,
    )


def _resolve_secret(secret_key: str | None = None) -> bytes:
    raw_secret = (secret_key or get_settings().jwt.secret_key or "").strip()

    if not raw_secret:
        raise CsrfConfigurationError(
            "JWT_SECRET_KEY is required for CSRF signing."
        )
    if len(raw_secret) < 32:
        raise CsrfConfigurationError(
            "JWT_SECRET_KEY must contain at least 32 characters."
        )

    return hmac.new(
        raw_secret.encode("utf-8"),
        _CSRF_KEY_CONTEXT,
        hashlib.sha256,
    ).digest()


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode("ascii"))


def _subject_payload(subject: CsrfSubject | None) -> dict[str, object]:
    if subject is None:
        return {"sub": "anonymous", "org": 0, "sid": 0}

    return {
        "sub": subject.username,
        "org": subject.organization_id,
        "sid": subject.session_id,
    }


def create_csrf_token(
    *,
    subject: CsrfSubject | None = None,
    now_epoch: int | None = None,
    nonce: str | None = None,
    secret_key: str | None = None,
) -> str:
    """Create a short-lived token bound to the current authenticated session."""

    now = int(time.time()) if now_epoch is None else int(now_epoch)
    resolved_nonce = nonce or secrets.token_urlsafe(24)

    payload = {
        "v": CSRF_TOKEN_VERSION,
        "iat": now,
        "nonce": resolved_nonce,
        **_subject_payload(subject),
    }

    encoded_payload = _b64url_encode(
        json.dumps(
            payload,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )

    signature = hmac.new(
        _resolve_secret(secret_key),
        encoded_payload.encode("ascii"),
        hashlib.sha256,
    ).digest()

    return f"{encoded_payload}.{_b64url_encode(signature)}"


def verify_csrf_token(
    token: str,
    *,
    subject: CsrfSubject | None = None,
    now_epoch: int | None = None,
    max_age_seconds: int = CSRF_MAX_AGE_SECONDS,
    secret_key: str | None = None,
) -> bool:
    """Verify signature, expiry and exact session/tenant binding."""

    try:
        if max_age_seconds <= 0:
            return False

        encoded_payload, encoded_signature = token.split(".", 1)

        expected_signature = hmac.new(
            _resolve_secret(secret_key),
            encoded_payload.encode("ascii"),
            hashlib.sha256,
        ).digest()
        actual_signature = _b64url_decode(encoded_signature)

        if not hmac.compare_digest(expected_signature, actual_signature):
            return False

        payload = json.loads(_b64url_decode(encoded_payload).decode("utf-8"))
        if not isinstance(payload, dict):
            return False
        if payload.get("v") != CSRF_TOKEN_VERSION:
            return False

        issued_at = int(payload.get("iat"))
        now = int(time.time()) if now_epoch is None else int(now_epoch)

        if issued_at > now + CSRF_CLOCK_SKEW_SECONDS:
            return False
        if now - issued_at > max_age_seconds:
            return False

        nonce = str(payload.get("nonce", "") or "")
        if len(nonce) < 16:
            return False

        expected_subject = _subject_payload(subject)
        return (
            payload.get("sub") == expected_subject["sub"]
            and int(payload.get("org", -1)) == expected_subject["org"]
            and int(payload.get("sid", -1)) == expected_subject["sid"]
        )
    except Exception:
        return False


async def extract_csrf_token(request: Request) -> str | None:
    """Read CSRF from the HTMX header, then from a regular HTML form."""

    header_token = (request.headers.get(CSRF_HEADER_NAME) or "").strip()
    if header_token:
        return header_token

    content_type = (request.headers.get("content-type") or "").lower()
    if (
        "application/x-www-form-urlencoded" not in content_type
        and "multipart/form-data" not in content_type
    ):
        return None

    form = await request.form()
    form_token = str(form.get(CSRF_FORM_FIELD, "") or "").strip()
    return form_token or None


async def enforce_csrf(
    request: Request,
    *,
    user: Any | None = None,
    secret_key: str | None = None,
) -> None:
    """Fail closed for missing, invalid, expired or cross-session CSRF."""

    token = await extract_csrf_token(request)

    try:
        subject = csrf_subject_from_user(user) if user is not None else None
    except ValueError:
        subject = None
        token = None

    if token is None or not verify_csrf_token(
        token,
        subject=subject,
        secret_key=secret_key,
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="CSRF validation failed.",
        )
