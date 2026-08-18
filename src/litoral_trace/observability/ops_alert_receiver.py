"""Internal Alertmanager webhook receiver that opens/closes GitHub incidents."""
from __future__ import annotations

import json
import os
import re
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel


_REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_ALLOWED_LABEL_KEYS = ("alertname", "severity", "service", "component")
_ALLOWED_ANNOTATION_KEYS = ("summary", "description")
_MARKER_PREFIX = "<!-- litoral-trace-alert:"


class AlertmanagerAlert(BaseModel):
    status: str
    labels: dict[str, str] = {}
    annotations: dict[str, str] = {}
    startsAt: str | None = None
    endsAt: str | None = None
    fingerprint: str | None = None


class AlertmanagerPayload(BaseModel):
    status: str
    alerts: list[AlertmanagerAlert]


def _clean_text(value: Any, *, limit: int = 500) -> str:
    normalized = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    return normalized[:limit]


def _config() -> tuple[str, str, str | None]:
    token = os.environ.get("OPS_ALERT_GITHUB_TOKEN", "").strip()
    repository = os.environ.get("OPS_ALERT_GITHUB_REPOSITORY", "").strip()
    assignee = os.environ.get("OPS_ALERT_GITHUB_ASSIGNEE", "").strip() or None
    if not token:
        raise RuntimeError("OPS_ALERT_GITHUB_TOKEN is required.")
    if not _REPOSITORY_RE.fullmatch(repository):
        raise RuntimeError("OPS_ALERT_GITHUB_REPOSITORY must be owner/repository.")
    return token, repository, assignee


def _github_request(
    method: str,
    path: str,
    *,
    token: str,
    payload: dict[str, Any] | None = None,
) -> Any:
    url = f"https://api.github.com{path}"
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urlrequest.Request(
        url,
        data=data,
        method=method,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "litoral-trace-ops-alert-receiver",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urlrequest.urlopen(request, timeout=10) as response:
            raw = response.read()
    except (urlerror.URLError, TimeoutError, OSError) as exc:
        raise RuntimeError("GitHub incident delivery failed.") from exc
    if not raw:
        return None
    return json.loads(raw.decode("utf-8"))


def _marker(alert: AlertmanagerAlert) -> str:
    fingerprint = _clean_text(alert.fingerprint or "missing", limit=128)
    return f"{_MARKER_PREFIX}{fingerprint} -->"


def _find_open_issue(
    *,
    token: str,
    repository: str,
    marker: str,
) -> dict[str, Any] | None:
    issues = _github_request(
        "GET",
        f"/repos/{repository}/issues?state=open&per_page=100",
        token=token,
    )
    for issue in issues or []:
        if "pull_request" in issue:
            continue
        if marker in str(issue.get("body") or ""):
            return issue
    return None


def _issue_title(alert: AlertmanagerAlert) -> str:
    alertname = _clean_text(alert.labels.get("alertname", "unknown"), limit=80)
    severity = _clean_text(alert.labels.get("severity", "warning"), limit=20).upper()
    return f"[OPS] {severity} {alertname}"[:120]


def _issue_body(alert: AlertmanagerAlert) -> str:
    labels = {
        key: _clean_text(alert.labels.get(key, ""), limit=120)
        for key in _ALLOWED_LABEL_KEYS
        if alert.labels.get(key)
    }
    annotations = {
        key: _clean_text(alert.annotations.get(key, ""), limit=500)
        for key in _ALLOWED_ANNOTATION_KEYS
        if alert.annotations.get(key)
    }
    lines = [
        _marker(alert),
        "Automated Litoral Trace operational alert.",
        "",
        f"Status: {_clean_text(alert.status, limit=20)}",
    ]
    if alert.startsAt:
        lines.append(f"Started: {_clean_text(alert.startsAt, limit=80)}")
    for key, value in labels.items():
        lines.append(f"{key}: {value}")
    for key, value in annotations.items():
        lines.append(f"{key}: {value}")
    lines.append("")
    lines.append("No credentials, customer identifiers, URLs, or raw payloads are included.")
    return "\n".join(lines)


def reconcile_alert(alert: AlertmanagerAlert) -> str:
    token, repository, assignee = _config()
    marker = _marker(alert)
    existing = _find_open_issue(token=token, repository=repository, marker=marker)
    is_firing = alert.status.strip().lower() == "firing"

    if is_firing and existing is None:
        payload: dict[str, Any] = {
            "title": _issue_title(alert),
            "body": _issue_body(alert),
        }
        if assignee:
            payload["assignees"] = [assignee]
        _github_request(
            "POST",
            f"/repos/{repository}/issues",
            token=token,
            payload=payload,
        )
        return "opened"

    if not is_firing and existing is not None:
        issue_number = int(existing["number"])
        _github_request(
            "PATCH",
            f"/repos/{repository}/issues/{issue_number}",
            token=token,
            payload={"state": "closed", "state_reason": "completed"},
        )
        return "closed"

    return "unchanged"


app = FastAPI(
    title="Litoral Trace Ops Alert Receiver",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


@app.get("/health", include_in_schema=False)
async def health() -> dict[str, str]:
    try:
        _config()
    except RuntimeError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="unavailable",
        ) from None
    return {"status": "ready"}


@app.post("/alertmanager", include_in_schema=False)
async def receive_alertmanager(payload: AlertmanagerPayload) -> dict[str, int]:
    counters = {"opened": 0, "closed": 0, "unchanged": 0}
    try:
        for alert in payload.alerts:
            result = reconcile_alert(alert)
            counters[result] += 1
    except RuntimeError:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="incident delivery failed",
        ) from None
    return counters
