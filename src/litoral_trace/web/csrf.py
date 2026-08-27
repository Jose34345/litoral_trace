"""CSRF primitives for server-rendered, HTMX and cookie-authenticated requests."""
from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import hmac
import json
import re
import secrets
import time
from typing import Any

from fastapi import HTTPException, Request, Response, status

from litoral_trace.config import get_settings


CSRF_HEADER_NAME = "X-CSRF-Token"
CSRF_FORM_FIELD = "csrf_token"
CSRF_BROWSER_COOKIE_KEY = "lt_csrf_browser"
CSRF_TOKEN_VERSION = 2
CSRF_MAX_AGE_SECONDS = 60 * 60
CSRF_BROWSER_COOKIE_MAX_AGE_SECONDS = 8 * 60 * 60
CSRF_CLOCK_SKEW_SECONDS = 60

_CSRF_KEY_CONTEXT = b"litoral-trace:csrf:v2"
_BROWSER_NONCE_RE = re.compile(r"^[A-Za-z0-9_-]{32,128}$")


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
        organization_id = int(
            getattr(user, "organization_id", 0) or 0
        )
        session_id = int(
            getattr(user, "session_id", 0) or 0
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Invalid authenticated CSRF subject."
        ) from exc

    if (
        not username
        or organization_id <= 0
        or session_id <= 0
    ):
        raise ValueError(
            "Authenticated CSRF subject is incomplete."
        )

    return CsrfSubject(
        username=username,
        organization_id=organization_id,
        session_id=session_id,
    )


def csrf_subject_from_access_payload(
    payload: dict[str, Any],
) -> CsrfSubject:
    """Build the CSRF subject from a previously verified access-token payload."""

    username = str(payload.get("sub", "") or "").strip()

    try:
        organization_id = int(payload.get("org_id"))
        session_id = int(payload.get("sid"))
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Access-token CSRF subject is incomplete."
        ) from exc

    if (
        not username
        or organization_id <= 0
        or session_id <= 0
    ):
        raise ValueError(
            "Access-token CSRF subject is incomplete."
        )

    return CsrfSubject(
        username=username,
        organization_id=organization_id,
        session_id=session_id,
    )


def _resolve_secret(secret_key: str | None = None) -> bytes:
    raw_secret = (
        secret_key
        or get_settings().jwt.secret_key
        or ""
    ).strip()

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
    return (
        base64.urlsafe_b64encode(data)
        .rstrip(b"=")
        .decode("ascii")
    )


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(
        (data + padding).encode("ascii")
    )


def _subject_payload(
    subject: CsrfSubject | None,
) -> dict[str, object]:
    if subject is None:
        return {
            "sub": "anonymous",
            "org": 0,
            "sid": 0,
        }

    return {
        "sub": subject.username,
        "org": subject.organization_id,
        "sid": subject.session_id,
    }


def create_csrf_browser_nonce() -> str:
    """Create the host-only browser secret used to bind rendered CSRF tokens."""

    return secrets.token_urlsafe(32)


def is_valid_csrf_browser_nonce(
    browser_nonce: str | None,
) -> bool:
    if not isinstance(browser_nonce, str):
        return False

    return bool(
        _BROWSER_NONCE_RE.fullmatch(
            browser_nonce.strip()
        )
    )


def get_csrf_browser_nonce(
    request: Request,
) -> str | None:
    value = request.cookies.get(
        CSRF_BROWSER_COOKIE_KEY
    )

    if not is_valid_csrf_browser_nonce(value):
        return None

    return str(value).strip()


def get_or_create_csrf_browser_nonce(
    request: Request,
) -> tuple[str, bool]:
    existing = get_csrf_browser_nonce(request)
    if existing is not None:
        return existing, False

    return create_csrf_browser_nonce(), True


def refresh_csrf_max_age_seconds(
    settings: Any | None = None,
) -> int:
    """Return the browser/refresh CSRF lifetime aligned to refresh-session TTL."""

    resolved_settings = settings or get_settings()
    try:
        refresh_days = int(
            resolved_settings.jwt.refresh_token_expire_days
        )
    except (AttributeError, TypeError, ValueError):
        refresh_days = 0

    refresh_seconds = (
        max(0, refresh_days) * 24 * 60 * 60
    )
    return max(
        CSRF_BROWSER_COOKIE_MAX_AGE_SECONDS,
        refresh_seconds,
    )


def set_csrf_browser_cookie(
    response: Response,
    browser_nonce: str,
) -> None:
    """Set a host-only HttpOnly browser-binding cookie."""

    if not is_valid_csrf_browser_nonce(browser_nonce):
        raise ValueError(
            "Invalid CSRF browser nonce."
        )

    settings = get_settings()

    response.set_cookie(
        key=CSRF_BROWSER_COOKIE_KEY,
        value=browser_nonce,
        httponly=True,
        samesite="lax",
        secure=settings.is_production,
        max_age=refresh_csrf_max_age_seconds(settings),
        path="/",
    )


def clear_csrf_browser_cookie(
    response: Response,
) -> None:
    settings = get_settings()

    response.delete_cookie(
        CSRF_BROWSER_COOKIE_KEY,
        httponly=True,
        samesite="lax",
        secure=settings.is_production,
        path="/",
    )


def _browser_binding(
    browser_nonce: str | None,
) -> str | None:
    """Return a one-way browser binding without exposing the HttpOnly nonce."""

    if browser_nonce is None:
        return None

    if not is_valid_csrf_browser_nonce(browser_nonce):
        raise ValueError(
            "Invalid CSRF browser nonce."
        )

    digest = hashlib.sha256(
        browser_nonce.encode("utf-8")
    ).digest()

    return _b64url_encode(digest)


def create_csrf_token(
    *,
    subject: CsrfSubject | None = None,
    browser_nonce: str | None = None,
    now_epoch: int | None = None,
    nonce: str | None = None,
    secret_key: str | None = None,
) -> str:
    """Create a signed token bound to session identity and, at runtime, browser."""

    now = (
        int(time.time())
        if now_epoch is None
        else int(now_epoch)
    )

    resolved_nonce = (
        nonce
        or secrets.token_urlsafe(24)
    )

    payload = {
        "v": CSRF_TOKEN_VERSION,
        "iat": now,
        "nonce": resolved_nonce,
        "browser": _browser_binding(browser_nonce),
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

    return (
        f"{encoded_payload}."
        f"{_b64url_encode(signature)}"
    )


def _decode_verified_csrf_payload(
    token: str,
    *,
    now_epoch: int | None = None,
    max_age_seconds: int = CSRF_MAX_AGE_SECONDS,
    secret_key: str | None = None,
) -> dict[str, Any] | None:
    try:
        if max_age_seconds <= 0:
            return None

        encoded_payload, encoded_signature = token.split(
            ".",
            1,
        )

        expected_signature = hmac.new(
            _resolve_secret(secret_key),
            encoded_payload.encode("ascii"),
            hashlib.sha256,
        ).digest()

        actual_signature = _b64url_decode(
            encoded_signature
        )

        if not hmac.compare_digest(
            expected_signature,
            actual_signature,
        ):
            return None

        payload = json.loads(
            _b64url_decode(encoded_payload)
            .decode("utf-8")
        )

        if not isinstance(payload, dict):
            return None

        if payload.get("v") != CSRF_TOKEN_VERSION:
            return None

        issued_at = int(payload.get("iat"))

        now = (
            int(time.time())
            if now_epoch is None
            else int(now_epoch)
        )

        if issued_at > now + CSRF_CLOCK_SKEW_SECONDS:
            return None

        if now - issued_at > max_age_seconds:
            return None

        token_nonce = str(
            payload.get("nonce", "")
            or ""
        )

        if len(token_nonce) < 16:
            return None

        return payload
    except Exception:
        return None


def _payload_matches_browser(
    payload: dict[str, Any],
    browser_nonce: str | None,
) -> bool:
    expected_binding = _browser_binding(
        browser_nonce
    )
    actual_binding = payload.get("browser")

    if (
        expected_binding is None
        or actual_binding is None
    ):
        return (
            expected_binding is None
            and actual_binding is None
        )

    return hmac.compare_digest(
        str(actual_binding),
        expected_binding,
    )


def verify_csrf_browser_binding(
    token: str,
    *,
    browser_nonce: str,
    now_epoch: int | None = None,
    max_age_seconds: int = CSRF_MAX_AGE_SECONDS,
    secret_key: str | None = None,
) -> bool:
    """Verify signature, expiry and browser binding without requiring access JWT."""

    if not is_valid_csrf_browser_nonce(
        browser_nonce
    ):
        return False

    payload = _decode_verified_csrf_payload(
        token,
        now_epoch=now_epoch,
        max_age_seconds=max_age_seconds,
        secret_key=secret_key,
    )

    if payload is None:
        return False

    try:
        return _payload_matches_browser(
            payload,
            browser_nonce,
        )
    except ValueError:
        return False


def verify_csrf_token(
    token: str,
    *,
    subject: CsrfSubject | None = None,
    browser_nonce: str | None = None,
    now_epoch: int | None = None,
    max_age_seconds: int = CSRF_MAX_AGE_SECONDS,
    secret_key: str | None = None,
) -> bool:
    """Verify signature, expiry, exact subject and optional browser binding."""

    payload = _decode_verified_csrf_payload(
        token,
        now_epoch=now_epoch,
        max_age_seconds=max_age_seconds,
        secret_key=secret_key,
    )

    if payload is None:
        return False

    try:
        if not _payload_matches_browser(
            payload,
            browser_nonce,
        ):
            return False

        expected_subject = _subject_payload(
            subject
        )

        return (
            payload.get("sub")
            == expected_subject["sub"]
            and int(payload.get("org", -1))
            == expected_subject["org"]
            and int(payload.get("sid", -1))
            == expected_subject["sid"]
        )
    except Exception:
        return False


async def extract_csrf_token(
    request: Request,
) -> str | None:
    """Read CSRF from request header, then regular HTML form."""

    header_token = (
        request.headers.get(CSRF_HEADER_NAME)
        or ""
    ).strip()

    if header_token:
        return header_token

    content_type = (
        request.headers.get("content-type")
        or ""
    ).lower()

    if (
        "application/x-www-form-urlencoded"
        not in content_type
        and "multipart/form-data"
        not in content_type
    ):
        return None

    form = await request.form()

    form_token = str(
        form.get(CSRF_FORM_FIELD, "")
        or ""
    ).strip()

    return form_token or None


async def enforce_csrf(
    request: Request,
    *,
    user: Any | None = None,
    browser_nonce: str | None = None,
    require_browser_binding: bool = True,
    secret_key: str | None = None,
) -> None:
    """Fail closed for missing, invalid, expired or replayed CSRF tokens."""

    token = await extract_csrf_token(request)

    if (
        require_browser_binding
        and not is_valid_csrf_browser_nonce(
            browser_nonce
        )
    ):
        token = None

    try:
        subject = (
            csrf_subject_from_user(user)
            if user is not None
            else None
        )
    except ValueError:
        subject = None
        token = None

    if (
        token is None
        or not verify_csrf_token(
            token,
            subject=subject,
            browser_nonce=browser_nonce,
            secret_key=secret_key,
        )
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="CSRF validation failed.",
        )