"""Conservative log redaction for Assurance pilot staging.

This filter is intentionally independent from business persistence/audit data.
It protects operational stdout/stderr logs and never attempts to rewrite the
source documents stored in Evidence Vault.
"""
from __future__ import annotations

import logging
import re
from typing import Any


_REDACTED = "[REDACTED]"
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+")
_JWT_RE = re.compile(
    r"\beyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\b"
)
_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
_CUIT_RE = re.compile(r"\b\d{2}-?\d{8}-?\d\b")
_SENSITIVE_ASSIGNMENT_RE = re.compile(
    r"(?i)\b("
    r"password|passwd|secret(?:_key)?|token|authorization|cookie|set-cookie|"
    r"api[_-]?key|authentication[_-]?key|cuit|tax[_-]?id|email|phone|address|"
    r"raw(?:\.|_)?document(?:\.|_)?text|original_value|normalized_value"
    r")\b\s*[:=]\s*(?:\"[^\"]*\"|'[^']*'|[^,;\s]+)"
)
_SENSITIVE_EXTRA_KEYS = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "secret_key",
        "token",
        "authorization",
        "cookie",
        "set_cookie",
        "api_key",
        "authentication_key",
        "cuit",
        "tax_id",
        "email",
        "phone",
        "address",
        "raw_document_text",
        "original_value",
        "normalized_value",
    }
)
_STANDARD_LOG_RECORD_KEYS = frozenset(
    logging.LogRecord(
        name="x",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="x",
        args=(),
        exc_info=None,
    ).__dict__.keys()
)


def redact_log_text(value: object) -> str:
    text = str(value)
    text = _BEARER_RE.sub("Bearer [REDACTED]", text)
    text = _JWT_RE.sub("[REDACTED_TOKEN]", text)
    text = _EMAIL_RE.sub("[REDACTED_EMAIL]", text)
    text = _CUIT_RE.sub("[REDACTED_CUIT]", text)
    text = _SENSITIVE_ASSIGNMENT_RE.sub(lambda match: f"{match.group(1)}={_REDACTED}", text)
    return text


def _normalized_extra_key(key: object) -> str:
    return str(key or "").strip().lower().replace("-", "_").replace(".", "_")


class SensitiveDataLogFilter(logging.Filter):
    """Render then scrub one LogRecord before any staging handler emits it."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            rendered = record.getMessage()
        except Exception:
            rendered = str(record.msg)
        record.msg = redact_log_text(rendered)
        record.args = ()

        for key, value in list(record.__dict__.items()):
            if key in _STANDARD_LOG_RECORD_KEYS:
                continue
            normalized_key = _normalized_extra_key(key)
            if normalized_key in _SENSITIVE_EXTRA_KEYS:
                record.__dict__[key] = _REDACTED
            elif isinstance(value, str):
                record.__dict__[key] = redact_log_text(value)
        return True


def sanitize_log_value(value: Any) -> str:
    """Public helper for explicit safe logging of external error text."""
    return redact_log_text(value)
