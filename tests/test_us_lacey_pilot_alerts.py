from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from unittest.mock import patch
from urllib.error import HTTPError
from uuid import UUID

import pytest

from litoral_trace.us_lacey.pilot_alerts import (
    GitHubIssueNotifier,
    StructuredLogNotifier,
    configured_pilot_incident_notifiers,
    notify_pilot_incidents_best_effort,
    sanitize_diagnostic_manifest,
)


INCIDENT_ID = UUID("11111111-2222-3333-4444-555555555555")
OPERATION_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def read(self) -> bytes:
        return self._body


def _incident():
    return SimpleNamespace(
        incident_public_id=INCIDENT_ID,
        organization_id=42,
        detector_code="LINE_FRAGMENTATION_SPIKE",
        severity="P0",
        diagnostic_manifest={
            "schema_version": 1,
            "detector": {
                "code": "LINE_FRAGMENTATION_SPIKE",
                "severity": "P0",
                "customer_name": "Private Customer",
            },
            "operation_public_id": str(OPERATION_ID),
            "trigger": "INITIAL_PROCESS",
            "versions": {
                "engine": "engine-2.4.0",
                "canonical_publisher": "canonical-v3",
                "filename": "private-invoice.pdf",
            },
            "documents": {
                "physical_count": 7,
                "valid_count": 7,
                "logical_count": 7,
                "by_type": {
                    "COMMERCIAL_INVOICE": 1,
                    "PACKING_LIST": 1,
                    "private-invoice.pdf": 99,
                },
                "filenames": ["private-invoice.pdf"],
            },
            "lines": {
                "commercial_structural": 3,
                "canonical": 10,
                "invoice_total": "currency-value",
            },
            "fields": {
                "total": 40,
                "auto_resolved": 6,
                "action_required": 34,
                "confirmed": 0,
                "conflicts": 2,
                "supplier_name": "Private Supplier",
            },
            "processing": {
                "duration_ms": 12500,
                "export_ready": False,
            },
            "admin_email": "private-user-at-example",
            "company_name": "Private Customer",
            "monetary_value": 87654321,
            "filename": "private-invoice.pdf",
        },
    )


def _snapshot():
    return SimpleNamespace(
        engine_version="engine-2.4.0",
        canonical_publisher_version="canonical-v3",
    )


def test_github_issue_notifier_formats_required_ticket() -> None:
    notifier = GitHubIssueNotifier(
        token="test-token",
        repository="Jose34345/litoral_trace",
    )

    spec = notifier.build_issue(_incident(), _snapshot())

    assert spec.title == (
        "[PILOT-P0] LINE_FRAGMENTATION_SPIKE — "
        "11111111-2222-3333-4444-555555555555"
    )
    assert spec.labels == ("pilot", "production", "p0")
    assert "**Prospect:** `prospect-" in spec.body
    assert f"**Operation UUID:** `{OPERATION_ID}`" in spec.body
    assert "Canonical lines (10) exceed structural lines (3)." in spec.body
    assert "Engine: `engine-2.4.0`" in spec.body
    assert "Canonical publisher: `canonical-v3`" in spec.body
    assert '"commercial_structural": 3' in spec.body
    assert '"canonical": 10' in spec.body


def test_github_issue_notifier_excludes_sensitive_data_by_allowlist() -> None:
    notifier = GitHubIssueNotifier(
        token="test-token",
        repository="Jose34345/litoral_trace",
    )

    spec = notifier.build_issue(_incident(), _snapshot())
    serialized = json.dumps(
        {"title": spec.title, "body": spec.body, "labels": spec.labels}
    ).lower()

    for forbidden in (
        "private customer",
        "private supplier",
        "private-user-at-example",
        "private-invoice.pdf",
        "currency-value",
        "87654321",
        "customer_name",
        "supplier_name",
        "admin_email",
        "filename",
        "monetary_value",
        "invoice_total",
    ):
        assert forbidden not in serialized


def test_sanitize_diagnostic_manifest_drops_unknown_nested_keys() -> None:
    safe = sanitize_diagnostic_manifest(_incident().diagnostic_manifest)

    assert set(safe) == {
        "schema_version",
        "detector",
        "operation_public_id",
        "trigger",
        "versions",
        "documents",
        "lines",
        "fields",
        "processing",
    }
    assert safe["documents"]["by_type"] == {
        "COMMERCIAL_INVOICE": 1,
        "PACKING_LIST": 1,
    }
    assert safe["lines"] == {"commercial_structural": 3, "canonical": 10}


def test_invalid_optional_transport_config_keeps_structured_log_active(
    monkeypatch,
) -> None:
    monkeypatch.setenv("LT_PILOT_ALERT_WEBHOOK_URL", "not-a-url")
    monkeypatch.delenv("LT_PILOT_GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("LT_PILOT_GITHUB_REPOSITORY", raising=False)

    notifiers = configured_pilot_incident_notifiers()

    assert len(notifiers) == 1
    assert isinstance(notifiers[0], StructuredLogNotifier)

def test_github_issue_notifier_skips_create_when_issue_already_exists() -> None:
    notifier = GitHubIssueNotifier(
        token="test-token",
        repository="Jose34345/litoral_trace",
    )
    existing_title = (
        "[PILOT-P0] LINE_FRAGMENTATION_SPIKE — "
        f"{INCIDENT_ID}"
    )

    with patch(
        "litoral_trace.us_lacey.pilot_alerts.request.urlopen",
        return_value=_FakeResponse(
            {
                "items": [
                    {
                        "title": existing_title,
                        "state": "open",
                    }
                ]
            }
        ),
    ) as urlopen:
        notifier.notify(_incident(), _snapshot())

    assert urlopen.call_count == 1
    search_request = urlopen.call_args.args[0]
    assert search_request.get_method() == "GET"
    assert search_request.full_url.startswith(
        "https://api.github.com/search/issues?"
    )
    assert str(INCIDENT_ID) in search_request.full_url
    assert all(
        call.args[0].get_method() != "POST"
        for call in urlopen.call_args_list
    )


def test_github_issue_notifier_creates_issue_with_expected_request() -> None:
    notifier = GitHubIssueNotifier(
        token="test-token",
        repository="Jose34345/litoral_trace",
    )

    with patch(
        "litoral_trace.us_lacey.pilot_alerts.request.urlopen",
        side_effect=[
            _FakeResponse({"items": []}),
            _FakeResponse({"number": 123, "state": "open"}),
        ],
    ) as urlopen:
        notifier.notify(_incident(), _snapshot())

    assert urlopen.call_count == 2
    search_call, create_call = urlopen.call_args_list
    search_request = search_call.args[0]
    create_request = create_call.args[0]

    assert search_request.get_method() == "GET"
    assert create_request.get_method() == "POST"
    assert create_request.full_url == (
        "https://api.github.com/repos/Jose34345/litoral_trace/issues"
    )
    assert search_call.kwargs["timeout"] == 10.0
    assert create_call.kwargs["timeout"] == 10.0

    headers = {
        key.lower(): value
        for key, value in create_request.header_items()
    }
    assert headers["authorization"] == "Bearer test-token"
    assert headers["accept"] == "application/vnd.github+json"
    assert headers["x-github-api-version"] == "2022-11-28"
    assert headers["user-agent"] == "litoral-trace-pilot-reliability"
    assert headers["content-type"] == "application/json"

    payload = json.loads(create_request.data.decode("utf-8"))
    assert payload["title"] == (
        "[PILOT-P0] LINE_FRAGMENTATION_SPIKE — "
        "11111111-2222-3333-4444-555555555555"
    )
    assert payload["labels"] == ["pilot", "production", "p0"]
    assert f"**Operation UUID:** `{OPERATION_ID}`" in payload["body"]


@pytest.mark.parametrize(
    "transport_error",
    [
        TimeoutError("github request timed out"),
        HTTPError(
            "https://api.github.com/search/issues",
            500,
            "Internal Server Error",
            hdrs=None,
            fp=None,
        ),
    ],
    ids=("timeout", "http-500"),
)
def test_github_issue_notifier_failure_isolated_by_best_effort_dispatcher(
    transport_error: Exception,
    caplog,
) -> None:
    notifier = GitHubIssueNotifier(
        token="test-token",
        repository="Jose34345/litoral_trace",
    )

    with (
        patch(
            "litoral_trace.us_lacey.pilot_alerts.configured_pilot_incident_notifiers",
            return_value=(notifier,),
        ),
        patch(
            "litoral_trace.us_lacey.pilot_alerts.request.urlopen",
            side_effect=transport_error,
        ) as urlopen,
        caplog.at_level(
            logging.ERROR,
            logger="litoral_trace.us_lacey.pilot_alerts",
        ),
    ):
        notify_pilot_incidents_best_effort(
            incidents=(_incident(),),
            snapshot=_snapshot(),
        )

    assert urlopen.call_count == 1
    assert "pilot_incident_notification_failed" in caplog.text
    assert str(INCIDENT_ID) in caplog.text
    assert "GitHubIssueNotifier" in caplog.text
