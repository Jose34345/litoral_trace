"""Transactional email delivery for U.S. Lacey account verification.

Provider credentials stay in environment variables. SMTP remains available for
always-on deployments, while HTTPS API delivery supports platforms whose free
tiers block outbound SMTP ports.
"""
from __future__ import annotations

from dataclasses import dataclass
from email.message import EmailMessage
import json
import logging
import os
import smtplib
import ssl
from urllib import error as urllib_error
from urllib import request as urllib_request

from litoral_trace.us_lacey.config import load_us_lacey_runtime_config


_LOG = logging.getLogger("litoral_trace.us_lacey.email_delivery")
_BREVO_TRANSACTIONAL_EMAIL_URL = "https://api.brevo.com/v3/smtp/email"


class UsLaceyEmailConfigurationError(RuntimeError):
    pass


class UsLaceyEmailDeliveryError(RuntimeError):
    pass


@dataclass(frozen=True)
class UsLaceyEmailConfig:
    # Keep the original SMTP fields first for backwards compatibility with
    # callers/tests that construct the dataclass directly.
    host: str
    port: int
    username: str
    password: str
    sender: str
    use_starttls: bool
    provider: str = "smtp"
    brevo_api_key: str = ""


def load_us_lacey_email_config() -> UsLaceyEmailConfig:
    provider = str(os.environ.get("US_LACEY_EMAIL_PROVIDER", "smtp")).strip().lower()
    sender = str(os.environ.get("US_LACEY_EMAIL_FROM", "")).strip()
    if not sender or "@" not in sender:
        raise UsLaceyEmailConfigurationError("US_LACEY_EMAIL_FROM is invalid.")

    if provider == "brevo_api":
        api_key = str(os.environ.get("US_LACEY_BREVO_API_KEY", "")).strip()
        if not api_key:
            raise UsLaceyEmailConfigurationError(
                "US_LACEY_BREVO_API_KEY is required when US_LACEY_EMAIL_PROVIDER=brevo_api."
            )
        return UsLaceyEmailConfig(
            host="",
            port=443,
            username="",
            password="",
            sender=sender,
            use_starttls=False,
            provider=provider,
            brevo_api_key=api_key,
        )

    if provider != "smtp":
        raise UsLaceyEmailConfigurationError(
            "US_LACEY_EMAIL_PROVIDER must be smtp or brevo_api."
        )

    host = str(os.environ.get("US_LACEY_SMTP_HOST", "")).strip()
    username = str(os.environ.get("US_LACEY_SMTP_USERNAME", "")).strip()
    password = str(os.environ.get("US_LACEY_SMTP_PASSWORD", "")).strip()
    raw_port = str(os.environ.get("US_LACEY_SMTP_PORT", "587")).strip()
    raw_starttls = str(os.environ.get("US_LACEY_SMTP_STARTTLS", "1")).strip().lower()
    if not host or not username or not password:
        raise UsLaceyEmailConfigurationError(
            "U.S. transactional SMTP email is not fully configured."
        )
    try:
        port = int(raw_port)
    except ValueError as exc:
        raise UsLaceyEmailConfigurationError("US_LACEY_SMTP_PORT is invalid.") from exc
    if port <= 0 or port > 65535:
        raise UsLaceyEmailConfigurationError("US_LACEY_SMTP_PORT is invalid.")
    return UsLaceyEmailConfig(
        host=host,
        port=port,
        username=username,
        password=password,
        sender=sender,
        use_starttls=raw_starttls not in {"0", "false", "no"},
        provider=provider,
    )


def _verification_message(*, company_name: str, verification_url: str) -> tuple[str, str]:
    subject = "Verify your Litoral Trace account"
    body = (
        "Welcome to Litoral Trace.\n\n"
        f"Verify the account for {company_name} using this link:\n"
        f"{verification_url}\n\n"
        "This link expires in 24 hours. If you did not create this account, ignore this email."
    )
    return subject, body


def _send_via_smtp(
    *,
    settings: UsLaceyEmailConfig,
    recipient: str,
    subject: str,
    body: str,
) -> None:
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.sender
    message["To"] = recipient
    message.set_content(body)

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
        # Log only protocol/error metadata. Never emit credentials, recipient,
        # verification token, SMTP response text, or message contents.
        _LOG.error(
            "us_lacey_smtp_delivery_failed error_type=%s smtp_code=%s errno=%s host=%s port=%s starttls=%s",
            type(exc).__name__,
            getattr(exc, "smtp_code", None),
            getattr(exc, "errno", None),
            settings.host,
            settings.port,
            settings.use_starttls,
        )
        raise UsLaceyEmailDeliveryError("Unable to send the verification email.") from exc


def _send_via_brevo_api(
    *,
    settings: UsLaceyEmailConfig,
    recipient: str,
    subject: str,
    body: str,
) -> None:
    payload = json.dumps(
        {
            "sender": {"name": "Litoral Trace", "email": settings.sender},
            "to": [{"email": recipient}],
            "subject": subject,
            "textContent": body,
        }
    ).encode("utf-8")
    request = urllib_request.Request(
        _BREVO_TRANSACTIONAL_EMAIL_URL,
        data=payload,
        headers={
            "accept": "application/json",
            "content-type": "application/json",
            "api-key": settings.brevo_api_key,
        },
        method="POST",
    )

    try:
        with urllib_request.urlopen(request, timeout=20) as response:
            status_code = int(getattr(response, "status", 0) or response.getcode())
            if status_code < 200 or status_code >= 300:
                raise UsLaceyEmailDeliveryError("Unable to send the verification email.")
    except urllib_error.HTTPError as exc:
        _LOG.error(
            "us_lacey_email_api_delivery_failed provider=brevo_api error_type=%s http_status=%s",
            type(exc).__name__,
            getattr(exc, "code", None),
        )
        raise UsLaceyEmailDeliveryError("Unable to send the verification email.") from exc
    except urllib_error.URLError as exc:
        _LOG.error(
            "us_lacey_email_api_delivery_failed provider=brevo_api error_type=%s reason_type=%s",
            type(exc).__name__,
            type(getattr(exc, "reason", None)).__name__,
        )
        raise UsLaceyEmailDeliveryError("Unable to send the verification email.") from exc
    except UsLaceyEmailDeliveryError:
        raise
    except Exception as exc:
        _LOG.error(
            "us_lacey_email_api_delivery_failed provider=brevo_api error_type=%s",
            type(exc).__name__,
        )
        raise UsLaceyEmailDeliveryError("Unable to send the verification email.") from exc


def send_us_lacey_verification_email(
    *,
    recipient: str,
    company_name: str,
    verification_token: str,
    config: UsLaceyEmailConfig | None = None,
    public_origin: str | None = None,
) -> None:
    recipient = str(recipient or "").strip().lower()
    if not recipient or "@" not in recipient:
        raise UsLaceyEmailDeliveryError("Verification recipient is invalid.")
    token = str(verification_token or "").strip()
    if not token:
        raise UsLaceyEmailDeliveryError("Verification token is missing.")
    settings = config or load_us_lacey_email_config()
    origin = str(public_origin or "").strip().rstrip("/")
    if not origin:
        runtime = load_us_lacey_runtime_config()
        origin = f"https://{runtime.app_hostname.strip().lower()}"
    if not origin.lower().startswith("https://"):
        raise UsLaceyEmailDeliveryError("Verification origin must use HTTPS.")
    verification_url = f"{origin}/verify-email?token={token}"
    subject, body = _verification_message(
        company_name=company_name,
        verification_url=verification_url,
    )

    if settings.provider == "brevo_api":
        _send_via_brevo_api(
            settings=settings,
            recipient=recipient,
            subject=subject,
            body=body,
        )
        return

    if settings.provider == "smtp":
        _send_via_smtp(
            settings=settings,
            recipient=recipient,
            subject=subject,
            body=body,
        )
        return

    # Config instances may be injected directly by tests/callers, so keep this
    # final defensive check even though the environment loader validates it.
    raise UsLaceyEmailConfigurationError(
        "US_LACEY_EMAIL_PROVIDER must be smtp or brevo_api."
    )
