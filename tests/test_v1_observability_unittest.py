from __future__ import annotations

import asyncio
from pathlib import Path

from prometheus_client import CollectorRegistry, generate_latest

import main as main_module
from litoral_trace.observability.api_metrics import (
    ApiMetrics,
    classify_http_surface,
)
from litoral_trace.observability.ops_alert_receiver import (
    AlertmanagerAlert,
    reconcile_alert,
)


ROOT = Path(__file__).resolve().parents[1]


class _Session:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.rollbacks = 0
        self.closes = 0

    def execute(self, _statement):
        if self.fail:
            raise RuntimeError("private-postgres-detail")
        return object()

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closes += 1


def _sample(registry, name, labels=None):
    return registry.get_sample_value(name, labels or {})


def test_http_metrics_use_bounded_surfaces_and_record_5xx():
    registry = CollectorRegistry()
    metrics = ApiMetrics(registry)

    metrics.record_http_response(
        method="GET",
        path="/api/v1/lotes/998877?token=never-label-this",
        status_code=500,
        duration_seconds=0.25,
    )

    assert classify_http_surface("/api/v1/lotes/998877") == "api"
    assert _sample(
        registry,
        "litoral_trace_http_requests_total",
        {"method": "GET", "surface": "api", "status_class": "5xx"},
    ) == 1
    rendered = generate_latest(registry)
    assert b"998877" not in rendered
    assert b"never-label-this" not in rendered


def test_auth_anomaly_metric_is_bounded_by_flow_and_outcome():
    registry = CollectorRegistry()
    metrics = ApiMetrics(registry)

    metrics.record_http_response(
        method="POST",
        path="/api/v1/auth/login",
        status_code=401,
        duration_seconds=0.01,
    )
    metrics.record_http_response(
        method="POST",
        path="/api/v1/auth/refresh",
        status_code=403,
        duration_seconds=0.01,
    )

    assert _sample(
        registry,
        "litoral_trace_auth_anomalies_total",
        {"flow": "login", "outcome": "unauthorized"},
    ) == 1
    assert _sample(
        registry,
        "litoral_trace_auth_anomalies_total",
        {"flow": "refresh", "outcome": "forbidden"},
    ) == 1


def test_dependency_metrics_report_database_and_vault_independently():
    registry = CollectorRegistry()
    metrics = ApiMetrics(registry)
    metrics.set_dependency_readiness(database=True, vault=False)

    assert _sample(
        registry,
        "litoral_trace_dependency_ready",
        {"dependency": "database"},
    ) == 1
    assert _sample(
        registry,
        "litoral_trace_dependency_ready",
        {"dependency": "vault"},
    ) == 0


def test_internal_metrics_refreshes_dependency_status(monkeypatch):
    session = _Session()
    monkeypatch.setattr(main_module, "get_db_session", lambda: session)
    monkeypatch.setattr(main_module, "is_vault_storage_ready", lambda: True)

    response = asyncio.run(main_module.internal_metrics())

    assert response.status_code == 200
    assert b'litoral_trace_dependency_ready{dependency="database"} 1.0' in response.body
    assert b'litoral_trace_dependency_ready{dependency="vault"} 1.0' in response.body
    assert session.closes == 1


def test_ready_fails_when_vault_is_unavailable_even_if_database_is_ready(monkeypatch):
    session = _Session()
    monkeypatch.setattr(main_module, "get_db_session", lambda: session)
    monkeypatch.setattr(main_module, "is_vault_storage_ready", lambda: False)

    response = asyncio.run(main_module.readiness_check())

    assert response.status_code == 503
    assert session.closes == 1


def test_alert_receiver_opens_sanitized_github_incident(monkeypatch):
    monkeypatch.setenv("OPS_ALERT_GITHUB_TOKEN", "private-token")
    monkeypatch.setenv("OPS_ALERT_GITHUB_REPOSITORY", "Jose34345/litoral_trace")
    monkeypatch.setenv("OPS_ALERT_GITHUB_ASSIGNEE", "Jose34345")
    monkeypatch.setattr(
        "litoral_trace.observability.ops_alert_receiver._find_open_issue",
        lambda **_kwargs: None,
    )
    calls = []

    def _request(method, path, *, token, payload=None):
        calls.append((method, path, token, payload))
        return {"number": 77}

    monkeypatch.setattr(
        "litoral_trace.observability.ops_alert_receiver._github_request",
        _request,
    )

    result = reconcile_alert(
        AlertmanagerAlert(
            status="firing",
            fingerprint="abc123",
            labels={
                "alertname": "LitoralTraceVaultUnavailable",
                "severity": "critical",
                "service": "litoral-trace",
                "component": "vault",
            },
            annotations={
                "summary": "Vault unavailable",
                "description": "Private storage readiness failed.",
            },
        )
    )

    assert result == "opened"
    assert calls[0][0] == "POST"
    assert calls[0][1] == "/repos/Jose34345/litoral_trace/issues"
    rendered_payload = str(calls[0][3])
    assert "private-token" not in rendered_payload
    assert "abc123" in rendered_payload


def test_alert_receiver_closes_matching_incident(monkeypatch):
    monkeypatch.setenv("OPS_ALERT_GITHUB_TOKEN", "private-token")
    monkeypatch.setenv("OPS_ALERT_GITHUB_REPOSITORY", "Jose34345/litoral_trace")
    monkeypatch.setattr(
        "litoral_trace.observability.ops_alert_receiver._find_open_issue",
        lambda **_kwargs: {"number": 88, "body": "marker"},
    )
    calls = []

    def _request(method, path, *, token, payload=None):
        calls.append((method, path, token, payload))
        return {}

    monkeypatch.setattr(
        "litoral_trace.observability.ops_alert_receiver._github_request",
        _request,
    )

    result = reconcile_alert(
        AlertmanagerAlert(
            status="resolved",
            fingerprint="abc123",
            labels={"alertname": "LitoralTraceApi5xxSpike"},
        )
    )

    assert result == "closed"
    assert calls[0][0] == "PATCH"
    assert calls[0][1].endswith("/issues/88")
    assert calls[0][3]["state"] == "closed"


def test_monitoring_contract_contains_all_v1_alert_families():
    rules = (ROOT / "monitoring" / "alert_rules.yml").read_text(encoding="utf-8")
    required_alerts = {
        "LitoralTraceReadinessUnavailable",
        "LitoralTraceVaultUnavailable",
        "LitoralTraceSatelliteQueueBacklog",
        "LitoralTraceSatelliteStaleJobs",
        "LitoralTraceSatelliteHeartbeatFailure",
        "LitoralTraceSatelliteLeaseLost",
        "LitoralTraceApi5xxSpike",
        "LitoralTraceAuthAnomalySpike",
    }
    for alert_name in required_alerts:
        assert f"alert: {alert_name}" in rules


def test_public_nginx_ingress_blocks_internal_metrics():
    nginx = (ROOT / "nginx" / "nginx.conf").read_text(encoding="utf-8")
    assert "location ^~ /internal/" in nginx
    assert "return 404;" in nginx

    prometheus = (ROOT / "monitoring" / "prometheus.yml").read_text(encoding="utf-8")
    assert "app:8000" in prometheus
    assert "metrics_path: /internal/metrics" in prometheus
