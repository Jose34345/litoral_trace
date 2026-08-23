"""ACCEPTANCE-only SOAP/WS-Security transport for EUDR DDS V3.

Security values are generated in memory for each request and are never returned
by public APIs or persisted in PostgreSQL.
"""
from __future__ import annotations

import base64
import hashlib
import secrets
import ssl
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4
import xml.etree.ElementTree as ET

from litoral_trace.config.eudr_acceptance import EudrAcceptanceSettings
from litoral_trace.services.eudr_acceptance_contract import (
    DDS_V3_NAMESPACE,
    EUDR_COMMON_V3_NAMESPACE,
    SOAP_ACTION_SUBMIT_DDS,
)


SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
WSSE_NS = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
WSU_NS = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd"
BASE_V4_NS = "http://ec.europa.eu/sanco/tracesnt/base/v4"
PASSWORD_DIGEST_TYPE = (
    "http://docs.oasis-open.org/wss/2004/01/"
    "oasis-200401-wss-username-token-profile-1.0#PasswordDigest"
)
NONCE_ENCODING_TYPE = (
    "http://docs.oasis-open.org/wss/2004/01/"
    "oasis-200401-wss-soap-message-security-1.0#Base64Binary"
)
MAX_RESPONSE_BYTES = 4 * 1024 * 1024

for prefix, namespace in (
    ("soapenv", SOAP_NS),
    ("wsse", WSSE_NS),
    ("wsu", WSU_NS),
    ("v4", BASE_V4_NS),
    ("dds", DDS_V3_NAMESPACE),
    ("eudrCommon", EUDR_COMMON_V3_NAMESPACE),
):
    ET.register_namespace(prefix, namespace)


class EudrAcceptanceTransportError(RuntimeError):
    def __init__(self, code: str, detail: str, *, http_status: int | None = None) -> None:
        self.code = code
        self.detail = detail
        self.http_status = http_status
        super().__init__(detail)


@dataclass(frozen=True)
class EudrAcceptanceResponse:
    http_status: int
    body: bytes


@dataclass(frozen=True)
class EudrSubmitParsedResponse:
    accepted: bool
    remote_uuid: str | None
    remote_status: str | None
    error_code: str | None
    error_summary: str | None


class AcceptanceTransport(Protocol):
    def send(self, *, envelope: bytes, settings: EudrAcceptanceSettings) -> EudrAcceptanceResponse: ...


def _tag(namespace: str, local: str) -> str:
    return f"{{{namespace}}}{local}"


def _utc_timestamp(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc).replace(microsecond=0)
    return normalized.isoformat().replace("+00:00", "Z")


def _read_bounded_response(stream, *, http_status: int) -> bytes:
    body = stream.read(MAX_RESPONSE_BYTES + 1)
    if len(body) > MAX_RESPONSE_BYTES:
        raise EudrAcceptanceTransportError(
            "ACCEPTANCE_RESPONSE_TOO_LARGE",
            "La respuesta ACCEPTANCE excede el límite de seguridad.",
            http_status=http_status,
        )
    return body


def password_digest(*, nonce: bytes, created: str, authentication_key: str) -> str:
    digest = hashlib.sha1()  # noqa: S324 - mandated by WS-Security PasswordDigest contract.
    digest.update(nonce)
    digest.update(created.encode("utf-8"))
    digest.update(authentication_key.encode("utf-8"))
    return base64.b64encode(digest.digest()).decode("ascii")


def build_ws_security_envelope(
    body_xml: bytes,
    *,
    username: str,
    authentication_key: str,
    web_service_client_id: str,
    now: datetime | None = None,
    nonce: bytes | None = None,
    validity_seconds: int = 60,
) -> bytes:
    """Wrap deterministic V3 body XML in one ephemeral WS-Security envelope."""

    if not username or not authentication_key or not web_service_client_id:
        raise EudrAcceptanceTransportError(
            "ACCEPTANCE_CREDENTIALS_REQUIRED",
            "Faltan credenciales WS-Security ACCEPTANCE.",
        )
    if not (1 <= int(validity_seconds) <= 60):
        raise EudrAcceptanceTransportError(
            "INVALID_TIMESTAMP_VALIDITY",
            "La validez WS-Security debe estar entre 1 y 60 segundos.",
        )
    if b"<!DOCTYPE" in body_xml.upper() or b"<!ENTITY" in body_xml.upper():
        raise EudrAcceptanceTransportError(
            "UNSAFE_REQUEST_XML",
            "El body XML contiene declaraciones no permitidas.",
        )
    try:
        body_element = ET.fromstring(body_xml)
    except ET.ParseError as exc:
        raise EudrAcceptanceTransportError(
            "INVALID_REQUEST_XML",
            "El body V3 no es XML válido.",
        ) from exc
    if body_element.tag != _tag(DDS_V3_NAMESPACE, "SubmitDdsRequest"):
        raise EudrAcceptanceTransportError(
            "INVALID_V3_ROOT",
            "El body no corresponde a SubmitDdsRequest V3.",
        )

    instant = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    nonce_bytes = nonce or secrets.token_bytes(16)
    created = _utc_timestamp(instant)
    expires = _utc_timestamp(instant + timedelta(seconds=int(validity_seconds)))
    digest = password_digest(
        nonce=nonce_bytes,
        created=created,
        authentication_key=authentication_key,
    )

    envelope = ET.Element(_tag(SOAP_NS, "Envelope"))
    header = ET.SubElement(envelope, _tag(SOAP_NS, "Header"))
    security = ET.SubElement(
        header,
        _tag(WSSE_NS, "Security"),
        {_tag(SOAP_NS, "mustUnderstand"): "1"},
    )
    timestamp = ET.SubElement(
        security,
        _tag(WSU_NS, "Timestamp"),
        {_tag(WSU_NS, "Id"): f"TS-{uuid4()}"},
    )
    ET.SubElement(timestamp, _tag(WSU_NS, "Created")).text = created
    ET.SubElement(timestamp, _tag(WSU_NS, "Expires")).text = expires

    token = ET.SubElement(
        security,
        _tag(WSSE_NS, "UsernameToken"),
        {_tag(WSU_NS, "Id"): f"UsernameToken-{uuid4()}"},
    )
    ET.SubElement(token, _tag(WSSE_NS, "Username")).text = username
    password = ET.SubElement(token, _tag(WSSE_NS, "Password"), {"Type": PASSWORD_DIGEST_TYPE})
    password.text = digest
    nonce_element = ET.SubElement(
        token,
        _tag(WSSE_NS, "Nonce"),
        {"EncodingType": NONCE_ENCODING_TYPE},
    )
    nonce_element.text = base64.b64encode(nonce_bytes).decode("ascii")
    ET.SubElement(token, _tag(WSU_NS, "Created")).text = created
    ET.SubElement(header, _tag(BASE_V4_NS, "WebServiceClientId")).text = web_service_client_id

    body = ET.SubElement(envelope, _tag(SOAP_NS, "Body"))
    body.append(body_element)
    return ET.tostring(envelope, encoding="utf-8", xml_declaration=True)


class UrllibAcceptanceTransport:
    """Minimal HTTPS transport. TLS verification cannot be disabled."""

    def send(self, *, envelope: bytes, settings: EudrAcceptanceSettings) -> EudrAcceptanceResponse:
        settings.require_network_ready()
        assert settings.endpoint_url is not None
        request = Request(
            settings.endpoint_url,
            data=envelope,
            method="POST",
            headers={
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": SOAP_ACTION_SUBMIT_DDS,
                "User-Agent": "Litoral-Trace/EUDR-Acceptance-V3",
            },
        )
        timeout = max(settings.connect_timeout_seconds, settings.read_timeout_seconds)
        context = ssl.create_default_context()
        try:
            with urlopen(request, timeout=timeout, context=context) as response:  # noqa: S310 - URL is allow-listed by settings.
                status = int(response.status)
                return EudrAcceptanceResponse(
                    http_status=status,
                    body=_read_bounded_response(response, http_status=status),
                )
        except HTTPError as exc:
            status = int(exc.code)
            return EudrAcceptanceResponse(
                http_status=status,
                body=_read_bounded_response(exc, http_status=status),
            )
        except EudrAcceptanceTransportError:
            raise
        except (URLError, TimeoutError, OSError) as exc:
            raise EudrAcceptanceTransportError(
                "ACCEPTANCE_TRANSPORT_ERROR",
                "No fue posible completar la conexión HTTPS con EUDR ACCEPTANCE.",
            ) from exc


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].split(":")[-1]


def parse_submit_response(*, http_status: int, body: bytes) -> EudrSubmitParsedResponse:
    """Parse UUID success or SOAP Fault without exposing arbitrary remote XML."""

    if len(body) > MAX_RESPONSE_BYTES:
        raise EudrAcceptanceTransportError(
            "ACCEPTANCE_RESPONSE_TOO_LARGE",
            "La respuesta ACCEPTANCE excede el límite de seguridad.",
            http_status=http_status,
        )
    upper = body.upper()
    if b"<!DOCTYPE" in upper or b"<!ENTITY" in upper:
        raise EudrAcceptanceTransportError(
            "UNSAFE_ACCEPTANCE_RESPONSE",
            "La respuesta ACCEPTANCE contiene XML no permitido.",
            http_status=http_status,
        )
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return EudrSubmitParsedResponse(
            accepted=False,
            remote_uuid=None,
            remote_status=None,
            error_code="INVALID_ACCEPTANCE_XML",
            error_summary="ACCEPTANCE devolvió una respuesta XML no interpretable.",
        )

    fault = next((node for node in root.iter() if _local_name(node.tag) == "Fault"), None)
    if fault is not None:
        values: dict[str, str] = {}
        for node in fault.iter():
            text = str(node.text or "").strip()
            if text:
                values[_local_name(node.tag)] = text
        summary = values.get("faultstring") or values.get("Text") or "SOAP Fault ACCEPTANCE"
        code = values.get("faultcode") or values.get("Value") or "ACCEPTANCE_SOAP_FAULT"
        return EudrSubmitParsedResponse(
            accepted=False,
            remote_uuid=None,
            remote_status="REJECTED",
            error_code=str(code)[:120],
            error_summary=str(summary)[:1000],
        )

    uuid_value = None
    for node in root.iter():
        if _local_name(node.tag) == "uuid" and str(node.text or "").strip():
            uuid_value = str(node.text).strip()
            break

    if 200 <= http_status < 300 and uuid_value:
        return EudrSubmitParsedResponse(
            accepted=True,
            remote_uuid=uuid_value,
            remote_status="SUBMITTED",
            error_code=None,
            error_summary=None,
        )

    return EudrSubmitParsedResponse(
        accepted=False,
        remote_uuid=None,
        remote_status="REJECTED",
        error_code="ACCEPTANCE_SUBMIT_REJECTED",
        error_summary=f"ACCEPTANCE no devolvió UUID de alta (HTTP {http_status}).",
    )
