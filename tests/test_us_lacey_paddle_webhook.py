"""HTTP boundary tests for the U.S. Lacey Paddle webhook."""
from __future__ import annotations

import hashlib
import hmac
import json
import time

from fastapi import FastAPI
from fastapi.testclient import TestClient

from litoral_trace.us_lacey.commercial import UsLaceyCommercialConfig
from litoral_trace.us_lacey.paddle import UsLaceyPaddleConfig
import litoral_trace.web.us_lacey_paddle_billing as module


SECRET = "pdl_webhook_secret_http_test_2026"


def _app(monkeypatch) -> TestClient:
    commercial = UsLaceyCommercialConfig(
        price_cents=14900,
        monthly_operation_limit=100,
        payment_provider="PADDLE",
        bank_transfer_instructions="",
        terms_version="terms-test",
        privacy_version="privacy-test",
        beta_terms_version="beta-test",
        support_email="support@litoraltrace.com",
    )
    paddle = UsLaceyPaddleConfig(
        environment="SANDBOX",
        client_token="test_" + "x" * 40,
        price_id="pri_" + "a" * 26,
        webhook_secret=SECRET,
    )
    monkeypatch.setattr(module, "load_us_lacey_commercial_config", lambda: commercial)
    monkeypatch.setattr(module, "load_us_lacey_paddle_config", lambda: paddle)
    app = FastAPI()
    app.include_router(module.router)
    return TestClient(app)


def _signature(body: bytes, timestamp: int | None = None) -> str:
    ts = int(time.time()) if timestamp is None else timestamp
    digest = hmac.new(
        SECRET.encode("utf-8"),
        str(ts).encode("ascii") + b":" + body,
        hashlib.sha256,
    ).hexdigest()
    return f"ts={ts};h1={digest}"


def test_invalid_signature_is_rejected_before_json_parse(monkeypatch) -> None:
    client = _app(monkeypatch)

    def forbidden_json_loads(*_args, **_kwargs):
        raise AssertionError("JSON parsing must not run before signature verification")

    monkeypatch.setattr(module.json, "loads", forbidden_json_loads)
    response = client.post(
        "/webhooks/paddle",
        content=b'{"event_type":"transaction.completed"}',
        headers={"Paddle-Signature": "ts=1;h1=" + "0" * 64},
    )
    assert response.status_code == 400
    assert '"detail":"Invalid payment event."' in response.text


def test_unsupported_signed_event_is_rejected(monkeypatch) -> None:
    client = _app(monkeypatch)
    body = json.dumps(
        {
            "event_id": "evt_" + "c" * 26,
            "event_type": "customer.created",
            "occurred_at": "2026-10-01T12:00:00Z",
            "data": {},
        },
        separators=(",", ":"),
    ).encode()
    response = client.post(
        "/webhooks/paddle",
        content=body,
        headers={"Paddle-Signature": _signature(body)},
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "Unsupported payment event."

