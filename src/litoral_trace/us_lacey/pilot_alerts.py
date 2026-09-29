"""Privacy-bounded incident transports for the Pilot Reliability Loop."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from hashlib import sha256
import json
import logging
import os
import re
from typing import Any, Mapping
from urllib import parse, request
from uuid import UUID

from litoral_trace.db.models.us_lacey_pilot_reliability import (
    UsLaceyPilotIncident,
    UsLaceyPilotQualitySnapshot,
)
from litoral_trace.lacey_engine.domain import DocumentType


LOGGER = logging.getLogger("litoral_trace.us_lacey.pilot_alerts")
_SAFE_TOKEN = re.compile(r"^[A-Z0-9_]{1,64}$")
_SAFE_VERSION = re.compile(r"^[A-Za-z0-9._:+/-]{1,100}$")
_ALLOWED_TRIGGERS = frozenset({"INITIAL_PROCESS", "REPROCESS", "WATCHDOG"})
_ALLOWED_SEVERITIES = frozenset({"P0", "P1"})
_ALLOWED_DETECTORS = frozenset(
    {
        "LINE_FRAGMENTATION_SPIKE",
        "ACTION_REQUIRED_SPIKE",
        "ZERO_AUTOMATION",
        "PROCESSING_STALLED",
    }
)
_ALLOWED_DOCUMENT_TYPES = frozenset(item.value for item in DocumentType)


@dataclass(frozen=True, slots=True)
class GitHubIssueSpec:
    title: str
    body: str
    labels: tuple[str, ...]


class PilotIncidentNotifier(ABC):
    @abstractmethod
    def notify(
        self,
        incident: UsLaceyPilotIncident,
        snapshot: UsLaceyPilotQualitySnapshot,
    ) -> None:
        raise NotImplementedError


def _safe_nonnegative_int(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


def _safe_bool(value: Any) -> bool:
    return value if isinstance(value, bool) else False


def _safe_uuid(value: Any) -> str:
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        return "unknown"


def _safe_token(value: Any, *, fallback: str = "UNKNOWN") -> str:
    normalized = str(value or "").strip().upper()
    return normalized if _SAFE_TOKEN.fullmatch(normalized) else fallback


def _safe_version(value: Any) -> str | None:
    normalized = str(value or "").strip()
    return normalized if _SAFE_VERSION.fullmatch(normalized) else None


def opaque_prospect_slug(organization_id: int) -> str:
    digest = sha256(f"pilot-org:{int(organization_id)}".encode("utf-8")).hexdigest()
    return f"prospect-{digest[:12]}"


def sanitize_diagnostic_manifest(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    """Rebuild a transport manifest from a strict technical allowlist."""
    source = raw if isinstance(raw, Mapping) else {}
    detector = source.get("detector") if isinstance(source.get("detector"), Mapping) else {}
    versions = source.get("versions") if isinstance(source.get("versions"), Mapping) else {}
    documents = source.get("documents") if isinstance(source.get("documents"), Mapping) else {}
    lines = source.get("lines") if isinstance(source.get("lines"), Mapping) else {}
    fields = source.get("fields") if isinstance(source.get("fields"), Mapping) else {}
    processing = source.get("processing") if isinstance(source.get("processing"), Mapping) else {}

    raw_by_type = documents.get("by_type")
    safe_by_type: dict[str, int] = {}
    if isinstance(raw_by_type, Mapping):
        for key, value in raw_by_type.items():
            normalized = str(key or "").strip().upper()
            if normalized in _ALLOWED_DOCUMENT_TYPES:
                safe_by_type[normalized] = _safe_nonnegative_int(value)

    severity = _safe_token(detector.get("severity"))
    if severity not in _ALLOWED_SEVERITIES:
        severity = "P1"
    trigger = _safe_token(source.get("trigger"))
    if trigger not in _ALLOWED_TRIGGERS:
        trigger = "UNKNOWN"

    detector_code = _safe_token(detector.get("code"))
    if detector_code not in _ALLOWED_DETECTORS:
        detector_code = "UNKNOWN"

    return {
        "schema_version": _safe_nonnegative_int(source.get("schema_version")),
        "detector": {
            "code": detector_code,
            "severity": severity,
        },
        "operation_public_id": _safe_uuid(source.get("operation_public_id")),
        "trigger": trigger,
        "versions": {
            "engine": _safe_version(versions.get("engine")),
            "canonical_publisher": _safe_version(versions.get("canonical_publisher")),
        },
        "documents": {
            "physical_count": _safe_nonnegative_int(documents.get("physical_count")),
            "valid_count": _safe_nonnegative_int(documents.get("valid_count")),
            "logical_count": _safe_nonnegative_int(documents.get("logical_count")),
            "by_type": dict(sorted(safe_by_type.items())),
        },
        "lines": {
            "commercial_structural": _safe_nonnegative_int(lines.get("commercial_structural")),
            "canonical": _safe_nonnegative_int(lines.get("canonical")),
        },
        "fields": {
            "total": _safe_nonnegative_int(fields.get("total")),
            "auto_resolved": _safe_nonnegative_int(fields.get("auto_resolved")),
            "action_required": _safe_nonnegative_int(fields.get("action_required")),
            "confirmed": _safe_nonnegative_int(fields.get("confirmed")),
            "conflicts": _safe_nonnegative_int(fields.get("conflicts")),
        },
        "processing": {
            "duration_ms": _safe_nonnegative_int(processing.get("duration_ms")),
            "export_ready": _safe_bool(processing.get("export_ready")),
        },
    }


def _summary(manifest: Mapping[str, Any]) -> str:
    detector = str(manifest["detector"]["code"])
    lines = manifest["lines"]
    fields = manifest["fields"]
    documents = manifest["documents"]
    if detector == "LINE_FRAGMENTATION_SPIKE":
        return (
            f"Canonical lines ({lines['canonical']}) exceed structural lines "
            f"({lines['commercial_structural']})."
        )
    if detector == "ACTION_REQUIRED_SPIKE":
        return (
            f"Action Required fields ({fields['action_required']}) are elevated "
            f"across {fields['total']} tracked fields."
        )
    if detector == "ZERO_AUTOMATION":
        return (
            f"Auto-resolved fields are 0 across {documents['valid_count']} valid documents."
        )
    if detector == "PROCESSING_STALLED":
        return "Pilot operation remained PROCESSING beyond the watchdog threshold."
    return f"Detector {detector} opened a pilot reliability incident."


def _incident_context(
    incident: UsLaceyPilotIncident,
    snapshot: UsLaceyPilotQualitySnapshot,
) -> dict[str, Any]:
    manifest = sanitize_diagnostic_manifest(incident.diagnostic_manifest)
    severity = _safe_token(incident.severity)
    if severity not in _ALLOWED_SEVERITIES:
        severity = str(manifest["detector"]["severity"])
    incident_detector = _safe_token(incident.detector_code)
    if incident_detector not in _ALLOWED_DETECTORS:
        incident_detector = "UNKNOWN"
    return {
        "incident_public_id": _safe_uuid(incident.incident_public_id),
        "prospect_slug": opaque_prospect_slug(incident.organization_id),
        "operation_public_id": manifest["operation_public_id"],
        "detector_code": incident_detector,
        "severity": severity,
        "summary": _summary(manifest),
        "engine_version": _safe_version(snapshot.engine_version)
        or manifest["versions"]["engine"],
        "canonical_publisher_version": _safe_version(snapshot.canonical_publisher_version)
        or manifest["versions"]["canonical_publisher"],
        "diagnostic_manifest": manifest,
    }


class StructuredLogNotifier(PilotIncidentNotifier):
    def notify(
        self,
        incident: UsLaceyPilotIncident,
        snapshot: UsLaceyPilotQualitySnapshot,
    ) -> None:
        payload = {"event": "pilot_incident", **_incident_context(incident, snapshot)}
        message = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        if payload["severity"] == "P0":
            LOGGER.error(message)
        else:
            LOGGER.warning(message)


class GitHubIssueNotifier(PilotIncidentNotifier):
    def __init__(
        self,
        *,
        token: str,
        repository: str,
        api_base: str = "https://api.github.com",
        timeout_seconds: float = 10.0,
    ) -> None:
        token = str(token or "").strip()
        repository = str(repository or "").strip()
        if not token:
            raise ValueError("GitHub issue notifier requires a token.")
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise ValueError("GitHub issue notifier repository must be owner/name.")
        self._token = token
        self._repository = repository
        self._api_base = api_base.rstrip("/")
        self._timeout_seconds = float(timeout_seconds)

    def build_issue(
        self,
        incident: UsLaceyPilotIncident,
        snapshot: UsLaceyPilotQualitySnapshot,
    ) -> GitHubIssueSpec:
        context = _incident_context(incident, snapshot)
        title = (
            f"[PILOT-{context['severity']}] {context['detector_code']} — "
            f"{context['incident_public_id']}"
        )
        manifest_json = json.dumps(
            context["diagnostic_manifest"],
            indent=2,
            sort_keys=True,
        )
        body = "\n".join(
            [
                "## Pilot reliability incident",
                "",
                f"- **Prospect:** `{context['prospect_slug']}`",
                f"- **Operation UUID:** `{context['operation_public_id']}`",
                f"- **Incident public ID:** `{context['incident_public_id']}`",
                f"- **Severity:** `{context['severity']}`",
                f"- **Detector:** `{context['detector_code']}`",
                "",
                "### Summary",
                context["summary"],
                "",
                "### Versions",
                f"- Engine: `{context['engine_version'] or 'unknown'}`",
                (
                    "- Canonical publisher: "
                    f"`{context['canonical_publisher_version'] or 'unknown'}`"
                ),
                "",
                "### Diagnostic manifest",
                "```json",
                manifest_json,
                "```",
            ]
        )
        return GitHubIssueSpec(
            title=title,
            body=body,
            labels=("pilot", "production", str(context["severity"]).lower()),
        )

    def _request_json(
        self,
        *,
        method: str,
        path: str,
        payload: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        data = None
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self._token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "litoral-trace-pilot-reliability",
        }
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = request.Request(
            f"{self._api_base}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        with request.urlopen(req, timeout=self._timeout_seconds) as response:
            decoded = response.read().decode("utf-8")
        parsed = json.loads(decoded) if decoded else {}
        return parsed if isinstance(parsed, Mapping) else {}

    def _existing_issue(self, incident_public_id: str) -> bool:
        query = (
            f'repo:{self._repository} is:issue in:title '
            f'"{incident_public_id}"'
        )
        encoded = parse.urlencode({"q": query, "per_page": 10})
        result = self._request_json(method="GET", path=f"/search/issues?{encoded}")
        items = result.get("items")
        if not isinstance(items, list):
            return False
        return any(
            incident_public_id in str(item.get("title") or "")
            for item in items
            if isinstance(item, Mapping)
        )

    def notify(
        self,
        incident: UsLaceyPilotIncident,
        snapshot: UsLaceyPilotQualitySnapshot,
    ) -> None:
        spec = self.build_issue(incident, snapshot)
        incident_public_id = _safe_uuid(incident.incident_public_id)
        if self._existing_issue(incident_public_id):
            return
        owner_repo = "/".join(
            parse.quote(part, safe="") for part in self._repository.split("/", 1)
        )
        self._request_json(
            method="POST",
            path=f"/repos/{owner_repo}/issues",
            payload={
                "title": spec.title,
                "body": spec.body,
                "labels": list(spec.labels),
            },
        )


class WebhookNotifier(PilotIncidentNotifier):
    def __init__(self, *, url: str, timeout_seconds: float = 10.0) -> None:
        self._url = str(url or "").strip()
        self._timeout_seconds = float(timeout_seconds)
        if not self._url.startswith(("https://", "http://")):
            raise ValueError("Pilot alert webhook URL must be HTTP(S).")

    def notify(
        self,
        incident: UsLaceyPilotIncident,
        snapshot: UsLaceyPilotQualitySnapshot,
    ) -> None:
        payload = json.dumps(_incident_context(incident, snapshot)).encode("utf-8")
        req = request.Request(
            self._url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(req, timeout=self._timeout_seconds):
            return


def configured_pilot_incident_notifiers() -> tuple[PilotIncidentNotifier, ...]:
    notifiers: list[PilotIncidentNotifier] = [StructuredLogNotifier()]

    webhook_url = str(os.environ.get("LT_PILOT_ALERT_WEBHOOK_URL") or "").strip()
    if webhook_url:
        try:
            notifiers.append(WebhookNotifier(url=webhook_url))
        except Exception:
            LOGGER.exception("pilot_webhook_notifier_disabled invalid_configuration=true")

    token = str(os.environ.get("LT_PILOT_GITHUB_TOKEN") or "").strip()
    repository = str(os.environ.get("LT_PILOT_GITHUB_REPOSITORY") or "").strip()
    if token and repository:
        try:
            notifiers.append(
                GitHubIssueNotifier(token=token, repository=repository)
            )
        except Exception:
            LOGGER.exception("pilot_github_notifier_disabled invalid_configuration=true")
    elif token or repository:
        LOGGER.warning("pilot_github_notifier_disabled incomplete_configuration=true")

    return tuple(notifiers)


def notify_pilot_incidents_best_effort(
    *,
    incidents: tuple[UsLaceyPilotIncident, ...],
    snapshot: UsLaceyPilotQualitySnapshot,
) -> None:
    if not incidents:
        return
    for notifier in configured_pilot_incident_notifiers():
        for incident in incidents:
            try:
                notifier.notify(incident, snapshot)
            except Exception:
                LOGGER.exception(
                    "pilot_incident_notification_failed incident_public_id=%s detector=%s transport=%s",
                    _safe_uuid(incident.incident_public_id),
                    _safe_token(incident.detector_code),
                    notifier.__class__.__name__,
                )
