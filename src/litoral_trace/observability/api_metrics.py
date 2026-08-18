"""Low-cardinality Prometheus metrics for the FastAPI/web runtime."""
from __future__ import annotations

import time
from typing import Any

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)


_AUTH_STATUS_OUTCOMES = {
    400: "bad_request",
    401: "unauthorized",
    403: "forbidden",
    429: "rate_limited",
}


def classify_http_surface(path: str) -> str:
    """Map request paths into a bounded label set."""
    normalized = (path or "/").split("?", 1)[0]
    if normalized in {"/login", "/api/v1/auth/login"}:
        return "auth_login"
    if normalized == "/api/v1/auth/refresh":
        return "auth_refresh"
    if normalized.startswith("/api/v1/auth/"):
        return "auth"
    if normalized in {"/health", "/ready"}:
        return "infra"
    if normalized.startswith("/internal/"):
        return "internal"
    if normalized.startswith("/api/v1/"):
        return "api"
    if normalized.startswith("/static/"):
        return "static"
    return "web"


def status_class(status_code: int) -> str:
    normalized = max(100, min(int(status_code), 599))
    return f"{normalized // 100}xx"


class ApiMetrics:
    """Process-local API metrics with bounded, non-business labels."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.http_requests = Counter(
            "litoral_trace_http_requests_total",
            "HTTP responses emitted by the Litoral Trace runtime.",
            ("method", "surface", "status_class"),
            registry=self.registry,
        )
        self.http_request_duration = Histogram(
            "litoral_trace_http_request_duration_seconds",
            "HTTP request duration by bounded surface.",
            ("method", "surface"),
            buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
            registry=self.registry,
        )
        self.auth_anomalies = Counter(
            "litoral_trace_auth_anomalies_total",
            "Authentication responses requiring operator attention.",
            ("flow", "outcome"),
            registry=self.registry,
        )
        self.dependency_ready = Gauge(
            "litoral_trace_dependency_ready",
            "Required runtime dependency readiness (1 ready, 0 unavailable).",
            ("dependency",),
            registry=self.registry,
        )
        self.set_dependency_readiness(database=False, vault=False)

    def record_http_response(
        self,
        *,
        method: str,
        path: str,
        status_code: int,
        duration_seconds: float,
    ) -> None:
        normalized_method = (method or "UNKNOWN").upper()
        surface = classify_http_surface(path)
        normalized_status_class = status_class(status_code)
        self.http_requests.labels(
            method=normalized_method,
            surface=surface,
            status_class=normalized_status_class,
        ).inc()
        self.http_request_duration.labels(
            method=normalized_method,
            surface=surface,
        ).observe(max(float(duration_seconds), 0.0))
        outcome = _AUTH_STATUS_OUTCOMES.get(int(status_code))
        if outcome and surface in {"auth_login", "auth_refresh"}:
            flow = "login" if surface == "auth_login" else "refresh"
            self.auth_anomalies.labels(flow=flow, outcome=outcome).inc()

    def set_dependency_readiness(self, *, database: bool, vault: bool) -> None:
        self.dependency_ready.labels(dependency="database").set(
            1.0 if database else 0.0
        )
        self.dependency_ready.labels(dependency="vault").set(
            1.0 if vault else 0.0
        )

    def render(self) -> bytes:
        return generate_latest(self.registry)


api_metrics = ApiMetrics()


class ApiMetricsMiddleware:
    """ASGI middleware that records responses without raw URL labels."""

    def __init__(self, app: Any, metrics: ApiMetrics | None = None) -> None:
        self.app = app
        self.metrics = metrics or api_metrics

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        started = time.perf_counter()
        response_status = 500
        response_started = False

        async def _send(message) -> None:
            nonlocal response_status, response_started
            if message.get("type") == "http.response.start":
                response_status = int(message.get("status", 500))
                response_started = True
            await send(message)

        try:
            await self.app(scope, receive, _send)
        except Exception:
            if not response_started:
                response_status = 500
            raise
        finally:
            self.metrics.record_http_response(
                method=str(scope.get("method", "UNKNOWN")),
                path=str(scope.get("path", "/")),
                status_code=response_status,
                duration_seconds=time.perf_counter() - started,
            )
