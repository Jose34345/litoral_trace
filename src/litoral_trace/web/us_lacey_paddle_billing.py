"""Paddle Billing checkout pages and signed webhook routes for U.S. Lacey."""
from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import HTMLResponse, Response

from litoral_trace.us_lacey.commercial import (
    UsLaceyCommercialConfigurationError,
    load_us_lacey_commercial_config,
)
from litoral_trace.us_lacey.paddle import (
    UsLaceyPaddleConfigurationError,
    UsLaceyPaddleWebhookError,
    load_us_lacey_paddle_config,
    parse_us_lacey_paddle_subscription_event,
    parse_us_lacey_paddle_transaction,
    verify_us_lacey_paddle_signature,
)
from litoral_trace.us_lacey.paddle_billing import (
    UsLaceyPaddleBillingError,
    apply_us_lacey_paddle_subscription_event,
    apply_us_lacey_paddle_transaction,
)
from litoral_trace.web.templates import templates


router = APIRouter()
_MAX_WEBHOOK_BYTES = 1_000_000
_SUPPORTED_EVENTS = {
    "transaction.completed",
    "subscription.created",
    "subscription.updated",
    "subscription.canceled",
}


@router.get("/pay", response_class=HTMLResponse, include_in_schema=False)
def paddle_default_payment_page(request: Request) -> HTMLResponse:
    """Public Paddle default-payment-link page.

    Paddle appends a _ptxn query parameter for provider-generated payment links.
    Paddle.js detects that transaction after initialization and opens checkout.
    """
    try:
        commercial = load_us_lacey_commercial_config()
        if commercial.payment_provider != "PADDLE":
            raise UsLaceyPaddleConfigurationError("Paddle is not enabled.")
        paddle = load_us_lacey_paddle_config()
    except (UsLaceyCommercialConfigurationError, UsLaceyPaddleConfigurationError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Online checkout is temporarily unavailable.",
        ) from exc

    html = templates.get_template("us_lacey/paddle_pay.html").render(
        request=request,
        paddle=paddle,
    )
    return HTMLResponse(
        html,
        headers={
            "Cache-Control": "no-store, max-age=0",
            "Pragma": "no-cache",
            "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "same-origin",
        },
    )


@router.post("/webhooks/paddle", include_in_schema=False)
async def paddle_webhook(request: Request) -> Response:
    signature = request.headers.get("Paddle-Signature", "")
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
        if commercial.payment_provider != "PADDLE":
            raise UsLaceyPaddleConfigurationError("Paddle is not enabled.")
        paddle = load_us_lacey_paddle_config()
    except (
        UsLaceyCommercialConfigurationError,
        UsLaceyPaddleConfigurationError,
    ) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Payment webhook is not configured.",
        ) from exc

    if not verify_us_lacey_paddle_signature(
        raw_body=raw_body,
        signature=signature,
        secret=paddle.webhook_secret,
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid payment event.",
        )

    try:
        envelope = json.loads(raw_body.decode("utf-8"))
        event_type = str(envelope.get("event_type", ""))
    except (UnicodeDecodeError, json.JSONDecodeError, AttributeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid payment event.",
        ) from exc
    if event_type not in _SUPPORTED_EVENTS:
        # Paddle destinations should subscribe only to events this runtime owns.
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported payment event.",
        )

    try:
        if event_type == "transaction.completed":
            event = parse_us_lacey_paddle_transaction(
                raw_body=raw_body,
                signature=signature,
                config=paddle,
                expected_price_cents=commercial.price_cents,
            )
            apply_us_lacey_paddle_transaction(event)
        else:
            event = parse_us_lacey_paddle_subscription_event(
                raw_body=raw_body,
                signature=signature,
                config=paddle,
            )
            apply_us_lacey_paddle_subscription_event(event)
    except UsLaceyPaddleWebhookError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid payment event.",
        ) from exc
    except UsLaceyPaddleBillingError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Payment event could not be applied.",
        ) from exc

    return Response(
        status_code=200,
        headers={"Cache-Control": "no-store, max-age=0"},
    )
