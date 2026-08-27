from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from fastapi import Response
from starlette.requests import Request

from litoral_trace.api.auth import _set_auth_cookies
from litoral_trace.auth.sessions import (
    ACCESS_TOKEN_COOKIE_KEY,
    REFRESH_TOKEN_COOKIE_KEY,
)
from litoral_trace.services.traceability_evidence import EvidenceSubjectChoice
from litoral_trace.web import context as context_web
from litoral_trace.web import eudr_dds_candidate as eudr_web
from litoral_trace.web import shipment_phytosanitary_case as phytosanitary_web
from litoral_trace.web import traceability_evidence as evidence_web
from litoral_trace.web.csrf import (
    CSRF_BROWSER_COOKIE_KEY,
    CSRF_HEADER_NAME,
    create_csrf_browser_nonce,
    create_csrf_token,
    refresh_csrf_max_age_seconds,
    verify_csrf_browser_binding,
)
from litoral_trace.web.middleware import validate_cookie_csrf_request


ROOT = Path(__file__).resolve().parents[1]
STATIC_JS = ROOT / "src" / "litoral_trace" / "static" / "src" / "js"
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
WEB = ROOT / "src" / "litoral_trace" / "web"
SECRET = "pre-pilot-test-secret-key-with-at-least-32-chars"


def _request(
    *,
    browser_nonce: str,
    csrf_token: str,
    access_token: str = "expired",
    path: str = "/api/v1/auth/refresh",
) -> Request:
    cookies = "; ".join(
        (
            f"{ACCESS_TOKEN_COOKIE_KEY}={access_token}",
            f"{REFRESH_TOKEN_COOKIE_KEY}=refresh-cookie-value",
            f"{CSRF_BROWSER_COOKIE_KEY}={browser_nonce}",
        )
    )
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "scheme": "https",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": [
            (b"cookie", cookies.encode("utf-8")),
            (CSRF_HEADER_NAME.lower().encode("ascii"), csrf_token.encode("utf-8")),
        ],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 443),
        "root_path": "",
    }
    return Request(scope)


def _template_request(access_token: str) -> Request:
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "GET",
        "scheme": "https",
        "path": "/dashboard",
        "raw_path": b"/dashboard",
        "query_string": b"",
        "headers": [
            (
                b"cookie",
                f"{ACCESS_TOKEN_COOKIE_KEY}={access_token}".encode("utf-8"),
            )
        ],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 443),
        "root_path": "",
    }
    return Request(scope)


def test_refresh_accepts_browser_bound_csrf_after_access_token_is_unusable() -> None:
    browser_nonce = create_csrf_browser_nonce()
    refresh_csrf = create_csrf_token(
        subject=None,
        browser_nonce=browser_nonce,
        secret_key=SECRET,
    )

    assert (
        validate_cookie_csrf_request(
            _request(browser_nonce=browser_nonce, csrf_token=refresh_csrf),
            secret_key=SECRET,
        )
        is None
    )


def test_refresh_rejects_browser_token_bound_to_another_browser() -> None:
    browser_nonce = create_csrf_browser_nonce()
    other_nonce = create_csrf_browser_nonce()
    refresh_csrf = create_csrf_token(
        subject=None,
        browser_nonce=other_nonce,
        secret_key=SECRET,
    )

    assert validate_cookie_csrf_request(
        _request(browser_nonce=browser_nonce, csrf_token=refresh_csrf),
        secret_key=SECRET,
    ) == "csrf_invalid"


def test_browser_only_refresh_csrf_is_rejected_on_other_unsafe_api_paths() -> None:
    browser_nonce = create_csrf_browser_nonce()
    refresh_csrf = create_csrf_token(
        subject=None,
        browser_nonce=browser_nonce,
        secret_key=SECRET,
    )

    assert validate_cookie_csrf_request(
        _request(
            browser_nonce=browser_nonce,
            csrf_token=refresh_csrf,
            path="/api/v1/satellite/jobs",
        ),
        secret_key=SECRET,
    ) == "csrf_invalid"


def test_refresh_csrf_window_matches_refresh_session_ttl() -> None:
    settings = SimpleNamespace(
        jwt=SimpleNamespace(refresh_token_expire_days=30)
    )
    max_age = refresh_csrf_max_age_seconds(settings)
    assert max_age == 30 * 24 * 60 * 60

    browser_nonce = create_csrf_browser_nonce()
    issued_at = 1_000_000
    refresh_csrf = create_csrf_token(
        subject=None,
        browser_nonce=browser_nonce,
        now_epoch=issued_at,
        secret_key=SECRET,
    )

    # The regular one-hour form-CSRF policy remains short-lived.
    assert not verify_csrf_browser_binding(
        refresh_csrf,
        browser_nonce=browser_nonce,
        now_epoch=issued_at + (2 * 60 * 60),
        secret_key=SECRET,
    )
    # Only refresh explicitly opts into the refresh-session-sized window.
    assert verify_csrf_browser_binding(
        refresh_csrf,
        browser_nonce=browser_nonce,
        now_epoch=issued_at + (2 * 60 * 60),
        max_age_seconds=max_age,
        secret_key=SECRET,
    )

    csrf_source = (WEB / "csrf.py").read_text(encoding="utf-8")
    middleware_source = (WEB / "middleware.py").read_text(encoding="utf-8")
    assert "max_age=refresh_csrf_max_age_seconds(settings)" in csrf_source
    assert "max_age_seconds=refresh_csrf_max_age_seconds()" in middleware_source


def test_production_auth_cookies_are_http_only_secure_lax_and_ttl_bounded() -> None:
    response = Response()
    settings = SimpleNamespace(
        is_production=True,
        jwt=SimpleNamespace(
            access_token_expire_seconds=30 * 60,
            refresh_token_expire_days=30,
        ),
    )

    _set_auth_cookies(
        response=response,
        access_token="access-token",
        refresh_token="refresh-token",
        settings=settings,
    )

    headers = response.headers.getlist("set-cookie")
    access_header = next(
        header for header in headers
        if header.startswith(f"{ACCESS_TOKEN_COOKIE_KEY}=")
    )
    refresh_header = next(
        header for header in headers
        if header.startswith(f"{REFRESH_TOKEN_COOKIE_KEY}=")
    )

    for header in (access_header, refresh_header):
        lowered = header.lower()
        assert "httponly" in lowered
        assert "secure" in lowered
        assert "samesite=lax" in lowered
        assert "path=/" in lowered

    assert "max-age=1800" in access_header.lower()
    assert "max-age=2592000" in refresh_header.lower()


def test_session_refresh_cadence_precedes_short_and_default_access_expiry(monkeypatch) -> None:
    short_settings = SimpleNamespace(
        jwt=SimpleNamespace(access_token_expire_seconds=4 * 60)
    )
    monkeypatch.setattr(context_web, "get_settings", lambda: short_settings)
    assert context_web._session_refresh_after_seconds() == 2 * 60
    assert context_web._session_refresh_after_seconds() < 4 * 60

    default_settings = SimpleNamespace(
        jwt=SimpleNamespace(access_token_expire_seconds=30 * 60)
    )
    monkeypatch.setattr(context_web, "get_settings", lambda: default_settings)
    assert context_web._session_refresh_after_seconds() == 10 * 60
    assert context_web._session_refresh_after_seconds() < 30 * 60


def test_access_expiry_metadata_is_bound_to_exact_hydrated_session(monkeypatch) -> None:
    user = SimpleNamespace(
        username="operator",
        organization_id=17,
        session_id=42,
    )
    request = _template_request("opaque-access-token")

    valid_payload = {
        "sub": "operator",
        "org_id": 17,
        "sid": 42,
        "exp": 2_000_000_000,
    }
    monkeypatch.setattr(
        context_web,
        "verify_jwt_token",
        lambda *_args, **_kwargs: valid_payload,
    )
    assert context_web._session_access_expires_at_epoch(
        request,
        user=user,
    ) == 2_000_000_000

    for mismatched_payload in (
        {**valid_payload, "sub": "other"},
        {**valid_payload, "org_id": 18},
        {**valid_payload, "sid": 43},
        {**valid_payload, "exp": 0},
    ):
        monkeypatch.setattr(
            context_web,
            "verify_jwt_token",
            lambda *_args, payload=mismatched_payload, **_kwargs: payload,
        )
        assert context_web._session_access_expires_at_epoch(
            request,
            user=user,
        ) is None


def test_base_loads_pre_pilot_hardening_layers() -> None:
    base = (TEMPLATES / "base.html").read_text(encoding="utf-8")
    assert "lt-refresh-csrf-token" in base
    assert "lt-session-refresh-after" in base
    assert "lt-session-access-expires-at" in base
    assert "lt-session-server-now" in base
    assert "/src/js/session-renewal.js" in base
    assert "/src/js/evidence-context.js" in base
    assert "/src/js/datetime-local.js" in base
    assert "/src/js/business-language.js" in base


def test_session_renewal_preserves_forms_and_rotates_rendered_csrf() -> None:
    source = (STATIC_JS / "session-renewal.js").read_text(encoding="utf-8")
    assert 'input[name="csrf_token"]' in source
    assert "window.location.href" in source
    assert "DOMParser" in source
    assert 'credentials: "same-origin"' in source
    assert "window.localStorage" not in source
    assert "window.sessionStorage" not in source


def test_session_renewal_uses_verified_server_relative_expiry_not_client_clock() -> None:
    source = (STATIC_JS / "session-renewal.js").read_text(encoding="utf-8")
    assert 'ACCESS_EXPIRES_AT_META_NAME = "lt-session-access-expires-at"' in source
    assert 'SERVER_NOW_META_NAME = "lt-session-server-now"' in source
    assert "readEpochMilliseconds(ACCESS_EXPIRES_AT_META_NAME)" in source
    assert "readEpochMilliseconds(SERVER_NOW_META_NAME)" in source
    assert "return Math.max(0, expiryMs - serverNowMs - intervalMs)" in source
    assert 'SESSION_CLOCK_URL = "/health"' in source
    assert 'response.headers.get("Date")' in source
    assert "Date.now()" not in source
    assert "window.setTimeout" in source
    assert "window.setInterval" not in source


def test_session_renewal_revalidates_server_time_after_suspend() -> None:
    source = (STATIC_JS / "session-renewal.js").read_text(encoding="utf-8")
    assert "revalidateAfterForeground" in source
    assert "refreshServerClock" in source
    assert 'document.visibilityState === "visible"' in source
    assert "foregroundDueProbe = true" in source
    assert 'ACCESS_PROBE_URL = "/api/v1/auth/me"' in source
    assert "accessSessionIsUsable" in source


def test_session_rotation_survives_navigation_unload() -> None:
    source = (STATIC_JS / "session-renewal.js").read_text(encoding="utf-8")
    assert 'REFRESH_URL = "/api/v1/auth/refresh"' in source
    assert "keepalive: true" in source
    assert 'body: "{}"' in source
    assert 'credentials: "same-origin"' in source


def test_session_renewal_serializes_tabs_and_rehydrates_peer_csrf() -> None:
    source = (STATIC_JS / "session-renewal.js").read_text(encoding="utf-8")
    assert "navigator.locks.request" in source
    assert "BroadcastChannel" in source
    assert 'type: "refreshed"' in source
    assert "synchronizeFromPeer" in source
    assert "synchronizeSecurityMeta" in source
    assert "announceRefresh(completedAt)" in source
    assert "performance.timeOrigin + performance.now()" in source


def test_live_csrf_bridge_overrides_stale_page_headers_after_rotation() -> None:
    source = (STATIC_JS / "session-renewal.js").read_text(encoding="utf-8")
    assert "installLiveCsrfFetchBridge" in source
    assert "headers.set(CSRF_HEADER_NAME, token)" in source
    assert "requestUrl.pathname === REFRESH_URL" in source
    assert "readMeta(CSRF_META_NAME)" in source


def test_evidence_context_never_silently_enables_default_subject_mutations() -> None:
    source = (STATIC_JS / "evidence-context.js").read_text(encoding="utf-8")
    assert "SHIPMENT|" in source
    assert "Confirmá el eslabón antes de vincular" in source
    assert 'form[action="/evidence/link"]' in source
    assert 'form[action="/evidence/upload-link"]' in source
    assert "button.disabled = true" in source
    assert "target.searchParams.set(SHIPMENT_PARAM, code)" in source
    assert "return option?.value" in source
    assert "selectorContainsSubject" in source
    assert "params.delete(SUBJECT_PARAM)" in source
    assert "data-evidence-placeholder" in source


def test_evidence_workspace_fails_closed_for_missing_or_invalid_subject(monkeypatch) -> None:
    subject = EvidenceSubjectChoice(
        subject_type="SOURCE_LOTE",
        reference="RODAL-DEMO-001",
        label="RODAL-DEMO-001",
        secondary="PROV-001 · Pino",
        status="ACTIVE",
    )

    class FakeService:
        def list_subjects(self, *, organization_id: int):
            assert organization_id == 1
            return (subject,)

        def list_evidence(self, **kwargs):
            raise AssertionError("No debe consultar evidencia sin un subject confirmado")

        def coverage(self, *, organization_id: int):
            assert organization_id == 1
            return SimpleNamespace(
                subjects_with_evidence=0,
                total_subjects=1,
                percentage=0,
                by_subject_type={},
            )

    monkeypatch.setattr(evidence_web, "_service", lambda: FakeService())
    monkeypatch.setattr(evidence_web, "has_permission", lambda *_args, **_kwargs: False)
    user = SimpleNamespace(organization_id=1)

    for requested in (None, "", "SHIPMENT|stale-or-cross-context"):
        view = evidence_web._present_workspace(user=user, selected_key=requested)
        assert view["selected_key"] is None
        assert view["selected_subject"] is None
        assert view["evidence"] == ()


def test_datetime_local_guard_handles_timestamptz_rendering_without_timezone_shift() -> None:
    source = (STATIC_JS / "datetime-local.js").read_text(encoding="utf-8")
    assert 'input[type="datetime-local"]' in source
    assert "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}" in source
    assert 'getAttribute("value")' in source
    assert "getTimezoneOffset" not in source


def test_phytosanitary_datetime_round_trip_renders_valid_datetime_local() -> None:
    persisted = datetime(2026, 8, 26, 17, 42, tzinfo=timezone.utc)
    case = SimpleNamespace(
        certification_mode="PAPER",
        requirements_reference="SENASA-REF",
        requirements_checked_at=persisted,
        cert_pov_reference="CERT-POV-1",
        certificate_number="PHYTO-1",
        ephyto_reference=None,
        notes=None,
    )
    readiness = SimpleNamespace(
        shipment_code="EXP-001",
        shipment_public_id=uuid4(),
        state="READY",
        ready=True,
        certification_mode="PAPER",
        requirements=(),
        missing=(),
        evidence_types=(),
        phytosanitary_case=case,
    )

    rendered = phytosanitary_web._view_payload(readiness)["case"]["requirements_checked_at"]
    assert rendered == "2026-08-26T17:42"
    assert "+00:00" not in rendered
    assert not rendered.endswith("Z")


def test_eudr_risk_datetime_round_trip_renders_valid_datetime_local() -> None:
    persisted = datetime(2026, 8, 26, 18, 7, tzinfo=timezone.utc)
    candidate = SimpleNamespace(
        activity_type="EXPORT",
        commodity_profile="WOOD",
        operator_name="Operador UE",
        operator_address="Dirección",
        operator_country_code="DE",
        operator_eori="EORI1",
        hs_code="4407",
        trade_name="Madera aserrada",
        product_description="Producto",
        common_species_name="Pino",
        scientific_species_name="Pinus elliottii",
        net_mass_kg="1000",
        production_country_code="AR",
        production_date_from=None,
        production_date_to=None,
        relies_on_previous_dds=False,
        previous_dds_reference=None,
        previous_dds_verification=None,
        risk_conclusion="NO_OR_NEGLIGIBLE_RISK",
        risk_assessment_reference="RISK-1",
        risk_assessed_at=persisted,
        spec_profile="EUDR_V3",
        spec_fingerprint_sha256="abc",
        notes=None,
    )
    conformance = SimpleNamespace(
        shipment_code="EXP-001",
        shipment_public_id=uuid4(),
        state="CONFORMANCE_READY",
        ready=True,
        missing=(),
        lineage_complete=True,
        requirements=(),
        plots=(),
        payload_sha256="payload",
        target_environment="ACCEPTANCE",
        legal_effect="NON_LEGAL_ACCEPTANCE",
        candidate=candidate,
    )

    rendered = eudr_web._view_payload(conformance)["candidate"]["risk_assessed_at"]
    assert rendered == "2026-08-26T18:07"
    assert "+00:00" not in rendered
    assert not rendered.endswith("Z")


def test_business_language_layer_translates_internal_codes_without_mutating_values() -> None:
    source = (STATIC_JS / "business-language.js").read_text(encoding="utf-8")
    required_pairs = (
        '"CONFORMANCE_READY", "Preparado para conformidad"',
        '"DDS_CANDIDATE", "Candidato DDS configurado"',
        '"POSTED", "Contabilizado"',
        '"DISPATCHED", "Despachado"',
        '"RISK_CONCLUSION", "Conclusión de riesgo"',
        '"RISK_ASSESSED_AT", "Fecha de evaluación de riesgo"',
    )
    for pair in required_pairs:
        assert pair in source
    assert "node.nodeValue = translated" in source
    assert ".value =" not in source
    assert "replaceAll" not in source
    assert "trimmed.endsWith(suffix)" in source
    assert "PRESENTATION_LABELS.get(trimmed)" in source
