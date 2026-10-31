"""Session-bound CSRF tokens for authenticated U.S. portal mutations."""
from __future__ import annotations

import hashlib
import hmac


class UsLaceyCsrfError(RuntimeError):
    pass


def us_lacey_csrf_token(*, session_token: str, purpose: str) -> str:
    """Derive a purpose-bound token without exposing the opaque session cookie."""
    session = str(session_token or "").strip()
    action = str(purpose or "").strip().lower()
    if not session or not action:
        raise UsLaceyCsrfError("CSRF context is invalid.")
    return hmac.new(
        session.encode("utf-8"),
        f"us-lacey-csrf-v1:{action}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def verify_us_lacey_csrf(
    *,
    session_token: str,
    purpose: str,
    submitted_token: str,
) -> None:
    expected = us_lacey_csrf_token(session_token=session_token, purpose=purpose)
    observed = str(submitted_token or "").strip()
    if not observed or not hmac.compare_digest(expected, observed):
        raise UsLaceyCsrfError("This form expired or could not be verified. Refresh and try again.")
