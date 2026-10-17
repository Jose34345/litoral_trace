"""Hosted Lemon Squeezy checkout and signed webhook routes for U.S. Lacey."""
from __future__ import annotations

from fastapi import APIRouter, Cookie, HTTPException, Request, status
from fastapi.responses import RedirectResponse, Response

from litoral_trace.us_lacey.commercial import (
    UsLaceyCommercialConfigurationError,
    load_us_lacey_commercial_config,
)
from litoral_trace.us_lacey.lemon_billing import (
    UsLaceyLemonBillingError,
    apply_us_lacey_lemon_order,
)
from litoral_trace.us_lacey.lemon_squeezy import (
    UsLaceyLemonConfigurationError,
    UsLaceyLemonWebhookError,
    build_us_lacey_lemon_checkout_url,
    load_us_lacey_lemon_config,
    parse_us_lacey_lemon_paid_order,
)
from litoral_trace.us_lacey.portal_auth import (
    US_LACEY_SESSION_COOKIE,
    UsLaceyPortalAuthError,
    resolve_us_lacey_session,
)
from litoral_trace.us_lacey.self_service import (
    UsLaceySelfServiceError,
    get_us_lacey_billing_summary,
)


router = APIRouter()
_MAX_WEBHOOK_BYTES = 1_000_000


def _login_redirect() -> RedirectResponse:
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(US_LACEY_SESSION_COOKIE, path="/")
    return response


@router.get("/billing/lemon-checkout", include_in_schema=False)
def lemon_checkout(
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
):
    if not us_session:
        return RedirectResponse("/login", status_code=303)
    try:
        identity = resolve_us_lacey_session(us_session)
        billing = get_us_lacey_billing_summary(organization_id=identity.organization_id)
        commercial = load_us_lacey_commercial_config()
        lemon = load_us_lacey_lemon_config()
    except UsLaceyPortalAuthError:
        return _login_redirect()
    except (
        UsLaceySelfServiceError,
        UsLaceyCommercialConfigurationError,
        UsLaceyLemonConfigurationError,
    ) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Online checkout is temporarily unavailable.",
        ) from exc

    if (
        identity.account_status != "PAYMENT_PENDING"
        or commercial.payment_provider != "LEMON_SQUEEZY"
        or billing.payment_provider != "LEMON_SQUEEZY"
        or billing.payment_status != "PENDING"
    ):
        return RedirectResponse("/billing", status_code=303)

    checkout_url = build_us_lacey_lemon_checkout_url(
        config=lemon,
        organization_id=identity.organization_id,
        payment_public_id=billing.payment_public_id,
    )
    response = RedirectResponse(checkout_url, status_code=303)
    response.headers["Cache-Control"] = "no-store, max-age=0"
    return response


@router.post("/webhooks/lemon-squeezy", include_in_schema=False)
async def lemon_webhook(request: Request) -> Response:
    signature = request.headers.get("X-Signature", "")
    event_header = request.headers.get("X-Event-Name", "")
    content_length = request.headers.get("content-length")
    try:
        if content_length is not None and int(content_length) > _MAX_WEBHOOK_BYTES:
            raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST) from exc

    raw_body = await request.body()
    if not raw_body or len(raw_body) > _MAX_WEBHOOK_BYTES:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE)

    try:
        commercial = load_us_lacey_commercial_config()
        if commercial.payment_provider != "LEMON_SQUEEZY":
            raise UsLaceyLemonConfigurationError("Lemon Squeezy is not enabled.")
        lemon = load_us_lacey_lemon_config()
    except (UsLaceyCommercialConfigurationError, UsLaceyLemonConfigurationError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Payment webhook is not configured.",
        ) from exc

    if event_header != "order_created":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported event.")

    try:
        order = parse_us_lacey_lemon_paid_order(
            raw_body=raw_body,
            signature=signature,
            config=lemon,
            expected_price_cents=commercial.price_cents,
        )
        apply_us_lacey_lemon_order(order)
    except UsLaceyLemonWebhookError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid payment event.",
        ) from exc
    except UsLaceyLemonBillingError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Payment event could not be applied.",
        ) from exc

    response = Response(status_code=200)
    response.headers["Cache-Control"] = "no-store, max-age=0"
    return response
