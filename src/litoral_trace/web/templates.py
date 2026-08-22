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


# Jinja resolves ``foo.bar`` through Python attributes before mapping keys.
# The traceability presentation model deliberately contains an ``items`` key,
# so ``result.items`` / ``graph.items`` would otherwise resolve to the native
# ``dict.items`` method and fail when the template attempts to iterate it.
# Keep the override deliberately narrow so every other Jinja lookup preserves
# the framework default behavior.
_default_jinja_getattr = templates.env.getattr


def _prefer_items_mapping_key(
    obj: Any,
    attribute: str,
) -> Any:
    if (
        attribute == "items"
        and isinstance(obj, dict)
        and "items" in obj
    ):
        return obj["items"]
    return _default_jinja_getattr(obj, attribute)


templates.env.getattr = _prefer_items_mapping_key


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