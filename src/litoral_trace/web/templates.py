"""Central Jinja2 and static-asset paths for the server-rendered frontend."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates


PACKAGE_DIR = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = PACKAGE_DIR / "templates"
STATIC_DIR = PACKAGE_DIR / "static"

templates = Jinja2Templates(
    directory=str(TEMPLATES_DIR)
)


def render_template(
    request: Request,
    name: str,
    context: dict[str, Any],
    *,
    status_code: int = 200,
) -> HTMLResponse:
    """Render a Jinja2 response using the supported request-aware API."""

    return templates.TemplateResponse(
        request=request,
        name=name,
        context=context,
        status_code=status_code,
    )