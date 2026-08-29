from __future__ import annotations

import json
import logging
from pathlib import Path

from litoral_trace.observability.redaction import SensitiveDataLogFilter, redact_log_text


ROOT = Path(__file__).resolve().parents[1]


def test_redaction_scrubs_credentials_identity_and_document_values():
    jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwaWxvdCJ9.abcdefghijklmno"
    text = (
        "Authorization=Bearer abc.def.ghi "
        f"token={jwt} email=persona@example.com CUIT=30-70832310-8 "
        "original_value='dato reservado' password=supersecreto"
    )
    redacted = redact_log_text(text)
    for forbidden in (
        "abc.def.ghi",
        jwt,
        "persona@example.com",
        "30-70832310-8",
        "dato reservado",
        "supersecreto",
    ):
        assert forbidden not in redacted
    assert "[REDACTED" in redacted


def test_filter_scrubs_formatted_arguments_and_sensitive_extra_fields():
    record = logging.LogRecord(
        name="litoral_trace.assurance",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="email=%s cuit=%s",
        args=("real@example.com", "30708323108"),
        exc_info=None,
    )
    record.customer_note = "authorization=Bearer secret-token"
    record.email = "real@example.com"
    assert SensitiveDataLogFilter().filter(record) is True
    rendered = record.getMessage()
    assert "real@example.com" not in rendered
    assert "30708323108" not in rendered
    assert "secret-token" not in record.customer_note
    assert record.email == "[REDACTED]"


def test_pilot_logging_config_applies_redaction_to_every_emitting_handler():
    config = json.loads((ROOT / "pilot" / "logging.json").read_text(encoding="utf-8"))
    assert config["filters"]["sensitive"]["()"] == (
        "litoral_trace.observability.redaction.SensitiveDataLogFilter"
    )
    for handler in config["handlers"].values():
        assert "sensitive" in handler.get("filters", [])
