"""Minimal transactional email delivery for U.S. Lacey account verification.

Provider credentials stay in environment variables. The implementation is SMTP
portable so launch is not coupled to a specific email vendor.
"""
from __future__ import annotations

from dataclasses import dataclass
from email.message import EmailMessage
import os
import smtplib
import ssl


class UsLaceyEmailConfigurationError(RuntimeError):
    pass


class UsLaceyEmailDeliveryError(RuntimeError):
    pass


@dataclass(frozen=True)
class UsLaceyEmailConfig:
    host: str
    port: int
    username: str
    password: str
    sender: str
    use_starttls: bool


def load_us_lacey_email_config() -> UsLaceyEmailConfig:
    host = str(os.environ.get("US_LACEY_SMTP_HOST", "")).strip()
    username = str(os.environ.get("US_LACEY_SMTP_USERNAME", "")).strip()
    password = str(os.environ.get("US_LACEY_SMTP_PASSWORD", "")).strip()
    sender = str(os.environ.get("US_LACEY_EMAIL_FROM", "")).strip()
    raw_port = str(os.environ.get("US_LACEY_SMTP_PORT", "587")).strip()
    raw_starttls = str(os.environ.get("US_LACEY_SMTP_STARTTLS", "1")).strip().lower()
    if not host or not username or not password or not sender:
        raise UsLaceyEmailConfigurationError(
            "U.S. transactional email is not fully configured."
        )
    try:
        port = int(raw_port)
    except ValueError as exc:
        raise UsLaceyEmailConfigurationError("US_LACEY_SMTP_PORT is invalid.") from exc
    if port <= 0 or port > 65535:
        raise UsLaceyEmailConfigurationError("US_LACEY_SMTP_PORT is invalid.")
    if "@" not in sender:
        raise UsLaceyEmailConfigurationError("US_LACEY_EMAIL_FROM is invalid.")
    return UsLaceyEmailConfig(
        host=host,
        port=port,
        username=username,
        password=password,
        sender=sender,
        use_starttls=raw_starttls not in {"0", "false", "no"},
    )


def send_us_lacey_verification_email(
    *,
    recipient: str,
    company_name: str,
    verification_token: str,
    config: UsLaceyEmailConfig | None = None,
) -> None:
    recipient = str(recipient or "").strip().lower()
    if not recipient or "@" not in recipient:
        raise UsLaceyEmailDeliveryError("Verification recipient is invalid.")
    token = str(verification_token or "").strip()
    if not token:
        raise UsLaceyEmailDeliveryError("Verification token is missing.")
    settings = config or load_us_lacey_email_config()
    verification_url = (
        "https://app.lacey.litoraltrace.com/verify-email?token=" + token
    )

    message = EmailMessage()
    message["Subject"] = "Verify your Litoral Trace account"
    message["From"] = settings.sender
    message["To"] = recipient
    message.set_content(
        "Welcome to Litoral Trace.\n\n"
        f"Verify the account for {company_name} using this link:\n"
        f"{verification_url}\n\n"
        "This link expires in 24 hours. If you did not create this account, ignore this email."
    )

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(settings.host, settings.port, timeout=20) as client:
            client.ehlo()
            if settings.use_starttls:
                client.starttls(context=context)
                client.ehlo()
            client.login(settings.username, settings.password)
            client.send_message(message)
    except Exception as exc:
        raise UsLaceyEmailDeliveryError("Unable to send the verification email.") from exc
