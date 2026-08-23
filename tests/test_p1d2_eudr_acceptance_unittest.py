from __future__ import annotations

from datetime import datetime, timezone
import base64
import json

import pytest

from litoral_trace.config.eudr_acceptance import (
    ACCEPTANCE_HOST,
    DDS_V3_SERVICE_PATH,
    EudrAcceptanceSettings,
)
from litoral_trace.db.models.eudr_acceptance_attempt import EudrAcceptanceAttempt
from litoral_trace.services.eudr_acceptance_contract import (
    DDS_V3_NAMESPACE,
    EudrAcceptanceContractError,
    WIRE_CONTRACT_PROFILE,
    WIRE_CONTRACT_SHA256,
    build_submit_dds_body,
)
from litoral_trace.services.eudr_acceptance_transport import (
    build_ws_security_envelope,
    parse_submit_response,
)


def _payload(*, previous: bool = False) -> dict:
    return {
        "target": {
            "environment": "ACCEPTANCE",
            "api_family": "V3",
        },
        "shipment": {
            "shipment_code": "SHIP-EU-001",
        },
        "activity_type": "IMPORT",
        "product": {
            "commodity_profile": "WOOD",
            "hs_code": "4407",
            "description": "Madera aserrada de pino",
            "common_species_name": "Pino",
            "scientific_species_name": "Pinus taeda",
            "net_mass_kg": "12500.000",
        },
        "production": {
            "country_code": "AR",
            "plots": [
                {
                    "parcel_identifier": "LT-001",
                    "area_ha": 8.5,
                    "geojson": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [-58.80, -27.20],
                                [-58.79, -27.20],
                                [-58.79, -27.19],
                                [-58.80, -27.20],
                            ]
                        ],
                    },
                }
            ],
        },
        "previous_dds": {
            "relied_upon": previous,
            "reference": "DDS-PREV" if previous else None,
            "verification": "VERIFY-PREV" if previous else None,
        },
    }


def test_acceptance_settings_fail_closed_and_reject_live_host() -> None:
    disabled = EudrAcceptanceSettings()
    assert disabled.network_ready is False
    with pytest.raises(RuntimeError, match="deshabilitado"):
        disabled.require_network_ready()

    endpoint = f"https://{ACCEPTANCE_HOST}{DDS_V3_SERVICE_PATH}"
    ready = EudrAcceptanceSettings(
        enabled=True,
        endpoint_url=endpoint,
        username="ws-user",
        authentication_key="private-key",
        web_service_client_id="client-id",
    )
    assert ready.network_ready is True
    ready.require_network_ready()

    with pytest.raises(ValueError, match="ACCEPTANCE oficial"):
        EudrAcceptanceSettings(
            enabled=True,
            endpoint_url="https://eudr.webcloud.ec.europa.eu/tracesnt/ws/EUDRDueDiligenceStatementServiceV3",
        )
    with pytest.raises(ValueError, match="HTTPS"):
        EudrAcceptanceSettings(
            enabled=True,
            endpoint_url=f"http://{ACCEPTANCE_HOST}{DDS_V3_SERVICE_PATH}",
        )
    with pytest.raises(ValueError, match="exactamente"):
        EudrAcceptanceSettings(
            enabled=True,
            endpoint_url=f"https://{ACCEPTANCE_HOST}/tracesnt/ws/OtherService",
        )


def test_v3_body_is_deterministic_and_contains_reviewed_fields() -> None:
    first = build_submit_dds_body(
        _payload(),
        operator_role="OPERATOR",
        country_of_activity="DE",
        border_cross_country="DE",
    )
    second = build_submit_dds_body(
        _payload(),
        operator_role="OPERATOR",
        country_of_activity="DE",
        border_cross_country="DE",
    )
    assert first.xml == second.xml
    assert first.sha256 == second.sha256
    assert first.wire_contract_profile == WIRE_CONTRACT_PROFILE
    assert first.wire_contract_sha256 == WIRE_CONTRACT_SHA256

    xml = first.xml.decode("utf-8")
    assert "SubmitDdsRequest" in xml
    assert DDS_V3_NAMESPACE in xml
    assert ">IMPORT<" in xml
    assert ">4407<" in xml
    assert ">12500.000<" in xml
    assert "Pinus taeda" in xml
    assert ">Pino<" in xml
    assert "geoLocationConfidential" in xml
    assert ">false<" in xml

    # geometryGeojson is base64-encoded GeoJSON, not an opaque made-up token.
    import xml.etree.ElementTree as ET

    root = ET.fromstring(first.xml)
    geometry = next(
        node.text
        for node in root.iter()
        if node.tag.rsplit("}", 1)[-1] == "geometryGeojson"
    )
    decoded = json.loads(base64.b64decode(geometry).decode("utf-8"))
    assert decoded["type"] == "FeatureCollection"
    assert decoded["features"][0]["properties"]["ProducerCountry"] == "AR"
    assert decoded["features"][0]["geometry"]["type"] == "Polygon"


def test_v3_body_refuses_previous_dds_semantic_guess() -> None:
    with pytest.raises(EudrAcceptanceContractError) as captured:
        build_submit_dds_body(
            _payload(previous=True),
            operator_role="OPERATOR",
            country_of_activity="DE",
            border_cross_country="DE",
        )
    assert captured.value.code == "PREVIOUS_DDS_V3_MAPPING_REQUIRES_REVIEW"


def test_ws_security_password_is_digest_and_envelope_is_ephemeral() -> None:
    body = build_submit_dds_body(
        _payload(),
        operator_role="OPERATOR",
        country_of_activity="DE",
        border_cross_country="DE",
    )
    now = datetime(2026, 8, 23, 5, 30, tzinfo=timezone.utc)
    envelope_a = build_ws_security_envelope(
        body.xml,
        username="ws-user",
        authentication_key="SUPER-PRIVATE-AUTH-KEY",
        web_service_client_id="lt-client",
        now=now,
        nonce=b"0123456789abcdef",
    )
    envelope_b = build_ws_security_envelope(
        body.xml,
        username="ws-user",
        authentication_key="SUPER-PRIVATE-AUTH-KEY",
        web_service_client_id="lt-client",
        now=now,
        nonce=b"fedcba9876543210",
    )
    rendered = envelope_a.decode("utf-8")
    assert "PasswordDigest" in rendered
    assert "UsernameToken" in rendered
    assert "Timestamp" in rendered
    assert "WebServiceClientId" in rendered
    assert "ws-user" in rendered
    assert "lt-client" in rendered
    assert "SUPER-PRIVATE-AUTH-KEY" not in rendered
    assert envelope_a != envelope_b
    assert body.sha256 == build_submit_dds_body(
        _payload(),
        operator_role="OPERATOR",
        country_of_activity="DE",
        border_cross_country="DE",
    ).sha256


def test_submit_response_parser_accepts_uuid_and_sanitizes_fault() -> None:
    success = b'''<?xml version="1.0"?>
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
      xmlns:dds="http://ec.europa.eu/tracesnt/certificate/eudr/due-diligence-statement/v3">
      <soapenv:Body><dds:SubmitDdsResponse><dds:uuid>REMOTE-UUID-123</dds:uuid></dds:SubmitDdsResponse></soapenv:Body>
    </soapenv:Envelope>'''
    parsed = parse_submit_response(http_status=200, body=success)
    assert parsed.accepted is True
    assert parsed.remote_uuid == "REMOTE-UUID-123"
    assert parsed.remote_status == "SUBMITTED"

    fault = b'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
      <soapenv:Body><soapenv:Fault><faultcode>CLIENT_VALIDATION</faultcode><faultstring>Invalid hsHeading</faultstring></soapenv:Fault></soapenv:Body>
    </soapenv:Envelope>'''
    rejected = parse_submit_response(http_status=500, body=fault)
    assert rejected.accepted is False
    assert rejected.remote_status == "REJECTED"
    assert rejected.error_code == "CLIENT_VALIDATION"
    assert rejected.error_summary == "Invalid hsHeading"


def test_attempt_model_contains_no_eudr_credentials_or_raw_xml_columns() -> None:
    columns = set(EudrAcceptanceAttempt.__table__.columns.keys())
    forbidden = {
        "username",
        "password",
        "authentication_key",
        "web_service_client_id",
        "nonce",
        "request_xml",
        "response_xml",
        "soap_envelope",
    }
    assert columns.isdisjoint(forbidden)
    assert {"request_body_sha256", "envelope_sha256", "response_sha256"} <= columns
