"""Public zero-touch sandbox provisioning for the U.S. Lacey product."""
from __future__ import annotations

from datetime import datetime, timezone
import logging

from fastapi import APIRouter, Cookie, Form, Request, Response, status
from fastapi.responses import PlainTextResponse, RedirectResponse

from litoral_trace.us_lacey.growth_attribution import (
    OUTREACH_ATTRIBUTION_COOKIE,
    OUTREACH_ATTRIBUTION_COOKIE_MAX_AGE,
    UsLaceyOutreachError,
    bind_outreach_to_sandbox,
    open_outreach_link,
    safe_record_outreach_event,
    safe_record_pre_sandbox_outreach_event,
)
from litoral_trace.us_lacey.portal_auth import (
    US_LACEY_SESSION_COOKIE,
    UsLaceyPortalAuthError,
    resolve_us_lacey_session,
)
from litoral_trace.us_lacey.portal_config import (
    UsLaceyPortalConfigurationError,
    load_us_lacey_portal_config,
)
from litoral_trace.us_lacey.sandbox import (
    UsLaceySandboxError,
    provision_us_lacey_sandbox,
)
from litoral_trace.web.templates import render_template


router = APIRouter(tags=["U.S. Lacey Sandbox"])
LOGGER = logging.getLogger(__name__)


_SAMPLE_PERSONAS = {
    "importer": {
        "label": "Importer",
        "title": "See a wood import reconciled before uploading anything",
        "description": (
            "A synthetic shipment shows how Litoral Trace reconciles normal supplier "
            "documents, isolates exceptions and reuses verified supplier evidence."
        ),
        "first": {
            "reference": "LT-SAMPLE-IMP-001",
            "documents": 7,
            "fields": 22,
            "exceptions": 4,
            "reused": 0,
            "species": "Quercus alba",
            "harvest": "United States",
            "payoff": "Supplier evidence verified for future shipments",
        },
        "second": {
            "reference": "LT-SAMPLE-IMP-002",
            "documents": 5,
            "fields": 20,
            "exceptions": 1,
            "reused": 4,
            "payoff": "4 fields resolved from previously verified supplier evidence",
        },
    },
    "broker": {
        "label": "Customs Broker",
        "title": "See the exception-first broker workflow",
        "description": (
            "A synthetic client file shows how Litoral Trace reconciles import "
            "documents, surfaces only unresolved Lacey items and prepares LAWGS-ready output."
        ),
        "first": {
            "reference": "LT-SAMPLE-BROKER-001",
            "documents": 8,
            "fields": 24,
            "exceptions": 5,
            "reused": 0,
            "species": "Swietenia macrophylla",
            "harvest": "Brazil",
            "payoff": "Missing client evidence isolated before filing",
        },
        "second": {
            "reference": "LT-SAMPLE-BROKER-002",
            "documents": 6,
            "fields": 23,
            "exceptions": 1,
            "reused": 3,
            "payoff": "3 supplier claims reused; only one client exception remains",
        },
    },
}


def _harden_public_response(response):
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["X-Robots-Tag"] = "noindex, nofollow, noarchive"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Content-Security-Policy"] = (
        "default-src 'none'; "
        "img-src 'self' data:; "
        "style-src 'self'; "
        "script-src 'self'; "
        "connect-src 'self'; "
        "form-action 'self'; "
        "base-uri 'none'; "
        "frame-ancestors 'none'"
    )
    return response


@router.get("/sandbox/ref/{slug}", include_in_schema=False)
def sandbox_outreach_referral(slug: str, demo: str | None = None):
    """Track an outreach visit; opt-in persona links bypass landing-page friction.

    Existing links without a demo query keep their original destination.
    Only known synthetic personas may become redirect targets.
    """

    try:
        portal = load_us_lacey_portal_config()
        attribution = open_outreach_link(slug)
    except UsLaceyPortalConfigurationError:
        return _harden_public_response(
            PlainTextResponse(
                "Sandbox is temporarily unavailable.",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        )
    except UsLaceyOutreachError:
        return _harden_public_response(
            PlainTextResponse(
                "Sandbox link is unavailable.",
                status_code=status.HTTP_404_NOT_FOUND,
            )
        )

    normalized_demo = str(demo or "").strip().lower()
    destination = (
        f"/try/{normalized_demo}"
        if normalized_demo in _SAMPLE_PERSONAS
        else "/sandbox/start"
    )
    response = RedirectResponse(
        destination,
        status_code=status.HTTP_303_SEE_OTHER,
    )
    response.set_cookie(
        key=OUTREACH_ATTRIBUTION_COOKIE,
        value=str(attribution.attribution_session_id),
        max_age=OUTREACH_ATTRIBUTION_COOKIE_MAX_AGE,
        httponly=True,
        secure=portal.session_cookie_secure,
        samesite="lax",
        path="/",
    )
    return _harden_public_response(response)


@router.post("/sandbox/engagement/human-visit", include_in_schema=False)
def sandbox_outreach_human_visit(
    sample_persona: str | None = Form(default=None),
    sample_step: int | None = Form(default=None),
    outreach_attribution: str | None = Cookie(
        None,
        alias=OUTREACH_ATTRIBUTION_COOKIE,
    ),
):
    """Count likely-human engagement after visible-browser dwell/interaction.

    GET visits and link-preview robots must not count as product engagement.
    Do not accept arbitrary event types, customer data or redirect destinations.
    """

    if outreach_attribution:
        safe_record_pre_sandbox_outreach_event(
            attribution_session_id=outreach_attribution,
            event_name="HUMAN_VISIT",
            event_key="browser-visible",
        )
        persona = str(sample_persona or "").strip().lower()
        if persona in _SAMPLE_PERSONAS and sample_step in (1, 2):
            sample_event = (
                "SAMPLE_STARTED" if sample_step == 1 else "SAMPLE_REUSE_REACHED"
            )
            safe_record_pre_sandbox_outreach_event(
                attribution_session_id=outreach_attribution,
                event_name=sample_event,
                event_key=persona,
                metadata={"persona": persona, "step": sample_step},
            )
            if sample_step == 2:
                safe_record_pre_sandbox_outreach_event(
                    attribution_session_id=outreach_attribution,
                    event_name="SAMPLE_COMPLETED",
                    event_key=persona,
                    metadata={"persona": persona},
                )

    return _harden_public_response(Response(status_code=status.HTTP_204_NO_CONTENT))


@router.get("/try/{persona}", include_in_schema=False)
def evaluation_sample_view(
    persona: str,
    request: Request,
    step: int = 1,
    outreach_attribution: str | None = Cookie(
        None,
        alias=OUTREACH_ATTRIBUTION_COOKIE,
    ),
):
    normalized = str(persona or "").strip().lower()
    sample = _SAMPLE_PERSONAS.get(normalized)
    if sample is None:
        return _harden_public_response(
            PlainTextResponse(
                "Sample is unavailable.",
                status_code=status.HTTP_404_NOT_FOUND,
            )
        )
    current_step = 2 if int(step) >= 2 else 1
    # Only a browser-interaction POST can establish a likely-human sample view.
    # Automated GET previews still load the page without advancing the funnel.
    response = render_template(
        request,
        "us_lacey/evaluation_sample.html",
        {
            "persona": normalized,
            "sample": sample,
            "step": current_step,
            "track_outreach_human_visit": bool(outreach_attribution),
        },
        status_code=status.HTTP_200_OK,
    )
    return _harden_public_response(response)


@router.get("/sandbox/start", include_in_schema=False)
def sandbox_start_view(
    request: Request,
    outreach_attribution: str | None = Cookie(
        None,
        alias=OUTREACH_ATTRIBUTION_COOKIE,
    ),
):
    """Render the consent screen without creating any tenant or browser state."""

    response = render_template(
        request,
        "us_lacey/sandbox_start.html",
        {"track_outreach_human_visit": bool(outreach_attribution)},
        status_code=status.HTTP_200_OK,
    )
    return _harden_public_response(response)


@router.post("/sandbox/start", include_in_schema=False)
def sandbox_start_provision(
    request: Request,
    consent: str | None = Form(default=None),
    support_debug_consent: str | None = Form(default=None),
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
    outreach_attribution: str | None = Cookie(
        None,
        alias=OUTREACH_ATTRIBUTION_COOKIE,
    ),
):
    """Provision one isolated four-hour tenant after explicit browser consent."""

    if consent != "accepted":
        return _harden_public_response(
            PlainTextResponse(
                "Sandbox consent is required.",
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        )

    # Existing authenticated visitors keep their current tenant instead of
    # silently replacing it with an anonymous sandbox.
    if us_session:
        try:
            resolve_us_lacey_session(us_session)
            response = RedirectResponse(
                "/operations/new",
                status_code=status.HTTP_303_SEE_OTHER,
            )
            if outreach_attribution:
                response.delete_cookie(
                    OUTREACH_ATTRIBUTION_COOKIE,
                    path="/",
                )
            return _harden_public_response(response)
        except UsLaceyPortalAuthError:
            # Invalid/expired browser state is replaced by a fresh sandbox token.
            pass

    try:
        portal = load_us_lacey_portal_config()
    except UsLaceyPortalConfigurationError:
        return _harden_public_response(
            PlainTextResponse(
                "Sandbox is temporarily unavailable.",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        )

    client_ip = request.client.host if request.client is not None else None
    user_agent = request.headers.get("user-agent")

    try:
        sandbox = provision_us_lacey_sandbox(
            client_ip=client_ip,
            user_agent=user_agent,
            learning_opt_in=False,
            support_debug_opt_in=False,
        )
    except UsLaceySandboxError as exc:
        response_status = (
            status.HTTP_429_TOO_MANY_REQUESTS
            if exc.code == "rate_limited"
            else status.HTTP_503_SERVICE_UNAVAILABLE
        )
        response = PlainTextResponse(str(exc), status_code=response_status)
        if response_status == status.HTTP_429_TOO_MANY_REQUESTS:
            response.headers["Retry-After"] = "3600"
        return _harden_public_response(response)

    if outreach_attribution:
        try:
            bind_outreach_to_sandbox(
                attribution_session_id=outreach_attribution,
                session_token=sandbox.session_token,
                organization_id=sandbox.organization_id,
            )
        except UsLaceyOutreachError:
            LOGGER.exception(
                "us_lacey_outreach_bind_failed",
                extra={"organization_id": sandbox.organization_id},
            )
        else:
            safe_record_outreach_event(
                session_token=sandbox.session_token,
                organization_id=sandbox.organization_id,
                event_name="OWN_SHIPMENT_STARTED",
                event_key="first-own-shipment",
            )

    response = RedirectResponse(
        "/operations/new",
        status_code=status.HTTP_303_SEE_OTHER,
    )
    max_age = max(
        1,
        int(
            (
                sandbox.expires_at
                - datetime.now(timezone.utc)
            ).total_seconds()
        ),
    )
    response.set_cookie(
        key=US_LACEY_SESSION_COOKIE,
        value=sandbox.session_token,
        max_age=max_age,
        httponly=True,
        secure=portal.session_cookie_secure,
        samesite="lax",
        path="/",
    )
    if outreach_attribution:
        response.delete_cookie(
            OUTREACH_ATTRIBUTION_COOKIE,
            path="/",
        )
    return _harden_public_response(response)
