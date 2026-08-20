from __future__ import annotations

import asyncio

from render_entrypoint import RenderPublicIngressGuard


async def _receive():
    return {"type": "http.request", "body": b"", "more_body": False}


def _run_request(path: str):
    downstream_calls = []
    sent = []

    async def downstream(scope, receive, send):
        del receive
        downstream_calls.append(scope["path"])
        await send(
            {
                "type": "http.response.start",
                "status": 204,
                "headers": [],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": b"",
            }
        )

    async def send(message):
        sent.append(message)

    guard = RenderPublicIngressGuard(downstream)
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "https",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": b"",
        "headers": [],
        "client": ("203.0.113.10", 54321),
        "server": ("litoraltrace.com", 443),
    }

    asyncio.run(guard(scope, _receive, send))
    return downstream_calls, sent


def test_render_public_ingress_returns_404_for_internal_metrics():
    downstream_calls, sent = _run_request("/internal/metrics")

    assert downstream_calls == []
    assert sent[0]["type"] == "http.response.start"
    assert sent[0]["status"] == 404


def test_render_public_ingress_blocks_entire_internal_namespace():
    downstream_calls, sent = _run_request("/internal/future-private-endpoint")

    assert downstream_calls == []
    assert sent[0]["status"] == 404


def test_render_public_ingress_delegates_public_routes_unchanged():
    downstream_calls, sent = _run_request("/ready")

    assert downstream_calls == ["/ready"]
    assert sent[0]["status"] == 204
