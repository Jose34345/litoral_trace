from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import UUID

from litoral_trace.us_lacey.pilot_alerts import (
    GitHubIssueNotifier,
    sanitize_diagnostic_manifest,
)


INCIDENT_ID = UUID("11111111-2222-3333-4444-555555555555")
OPERATION_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


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
