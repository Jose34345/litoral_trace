"""Bound U.S. Lacey upload request bodies before Starlette parses multipart.

This pure ASGI middleware avoids buffering the request a second time and
enforces the limit even if a client omits or lies about Content-Length.
"""
from __future__ import annotations

import re

from starlette.responses import PlainTextResponse

from litoral_trace.us_lacey.storage import us_lacey_max_request_bytes


_UPLOAD_PATH = re.compile(r"^/operations/[^/]+/upload$")
_OVERSIZE_MESSAGE = "The uploaded request exceeds the maximum total upload size."


class _RequestTooLarge(Exception):
    pass


class LaceyMultipartBudgetMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        path = str(scope.get("path") or "")
        if (
            scope.get("type") != "http"
            or scope.get("method") != "POST"
            or (path != "/operations/new" and not _UPLOAD_PATH.fullmatch(path))
        ):
            await self.app(scope, receive, send)
            return

        limit = us_lacey_max_request_bytes()
        headers = dict(scope.get("headers") or ())
        content_length = headers.get(b"content-length", b"")
        if content_length:
            try:
                advertised_size = int(content_length)
            except (ValueError, TypeError):
                advertised_size = -1
            if advertised_size < 0:
                await PlainTextResponse("Invalid request size.", status_code=400)(
                    scope, receive, send
                )
                return
            if advertised_size > limit:
                await PlainTextResponse(_OVERSIZE_MESSAGE, status_code=413)(
                    scope, receive, send
                )
                return

        received = 0
        response_started = False

        async def bounded_receive():
            nonlocal received
            message = await receive()
            if message.get("type") == "http.request":
                received += len(message.get("body") or b"")
                if received > limit:
                    raise _RequestTooLarge()
            return message

        async def tracked_send(message):
            nonlocal response_started
            if message.get("type") == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, bounded_receive, tracked_send)
        except _RequestTooLarge:
            if response_started:
                raise
            await PlainTextResponse(_OVERSIZE_MESSAGE, status_code=413)(
                scope, receive, send
            )
