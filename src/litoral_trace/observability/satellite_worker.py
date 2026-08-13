"""Prometheus and structured logging support for the satellite worker."""
from __future__ import annotations

import json
import logging
import math
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
from prometheus_client.exposition import start_wsgi_server
from sqlalchemy import text
from sqlalchemy.orm import Session

from litoral_trace.db.worker import get_worker_db_session


WORKER_QUEUE_METRICS_FUNCTION = (
    "public.worker_get_satellite_queue_metrics"
)

_SAFE_LOG_FIELDS = (
    "job_id",
    "organization_id",
    "job_type",
    "worker_id",
    "attempt_count",
    "max_attempts",
    "retry_delay_seconds",
    "error_code",
    "error_type",
    "error_message",
    "elapsed_ms",
    "requeued_count",
    "failed_count",
    "signal",
)

_ERROR_CODE_ALLOWLIST = frozenset(
    {
        "gee_execution_failed",
        "gee_rate_limited",
        "gee_temporary_network",
        "gee_temporary_service",
        "gee_timeout",
        "geometry_hash_mismatch",
        "invalid_job_payload",
        "lease_lost",
        "stale_recovery_exhausted",
        "unsupported_algorithm_version",
        "unsupported_job_type",
        "worker_execution_failed",
    }
)

_COUNT_FIELDS = (
    "queued_ready_count",
    "queued_delayed_count",
    "running_count",
    "running_stale_count",
    "running_invalid_count",
)

_DURATION_FIELDS = (
    "oldest_ready_age_seconds",
    "oldest_active_lease_age_seconds",
    "oldest_heartbeat_age_seconds",
    "next_delayed_ready_in_seconds",
)


@dataclass(frozen=True)
class SatelliteQueueSnapshot:
    snapshot_time: datetime | None
    queued_ready_count: int
    queued_delayed_count: int
    running_count: int
    running_stale_count: int
    running_invalid_count: int
    oldest_ready_age_seconds: float
    oldest_active_lease_age_seconds: float
    oldest_heartbeat_age_seconds: float
    next_delayed_ready_in_seconds: float

    @classmethod
    def empty(cls) -> "SatelliteQueueSnapshot":
        return cls(
            snapshot_time=None,
            queued_ready_count=0,
            queued_delayed_count=0,
            running_count=0,
            running_stale_count=0,
            running_invalid_count=0,
            oldest_ready_age_seconds=0.0,
            oldest_active_lease_age_seconds=0.0,
            oldest_heartbeat_age_seconds=0.0,
            next_delayed_ready_in_seconds=0.0,
        )


def _nonnegative_count(value: Any, field_name: str) -> int:
    normalized = int(value)
    if normalized < 0:
        raise ValueError(f"{field_name} no puede ser negativo.")
    return normalized


def _nonnegative_duration(value: Any, field_name: str) -> float:
    normalized = 0.0 if value is None else float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} debe ser un numero no negativo.")
    return normalized


def _as_utc(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("snapshot_time debe ser un datetime.")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _map_queue_snapshot(row: Any) -> SatelliteQueueSnapshot:
    mapped = dict(row)
    counts = {
        field: _nonnegative_count(mapped[field], field)
        for field in _COUNT_FIELDS
    }
    durations = {
        field: _nonnegative_duration(mapped.get(field), field)
        for field in _DURATION_FIELDS
    }
    return SatelliteQueueSnapshot(
        snapshot_time=_as_utc(mapped["snapshot_time"]),
        **counts,
        **durations,
    )


def get_satellite_queue_snapshot(
    db_session: Session | None = None,
) -> SatelliteQueueSnapshot:
    """Read the global aggregate through the narrow worker capability."""

    owns_session = db_session is None
    session = db_session or get_worker_db_session()
    if session is None:
        raise RuntimeError("Worker metrics database session is unavailable.")

    try:
        row = session.execute(
            text(
                """
                SELECT
                    snapshot_time,
                    queued_ready_count,
                    queued_delayed_count,
                    running_count,
                    running_stale_count,
                    running_invalid_count,
                    oldest_ready_age_seconds,
                    oldest_active_lease_age_seconds,
                    oldest_heartbeat_age_seconds,
                    next_delayed_ready_in_seconds
                FROM public.worker_get_satellite_queue_metrics()
                """
            )
        ).mappings().one()
        return _map_queue_snapshot(row)
    except Exception:
        if owns_session:
            session.rollback()
        raise
    finally:
        if owns_session:
            session.close()


class SatelliteQueueSnapshotCache:
    """Bound database refreshes while retaining the last valid snapshot."""

    def __init__(
        self,
        *,
        refresh_seconds: float = 30.0,
        loader: Callable[[], SatelliteQueueSnapshot] = (
            get_satellite_queue_snapshot
        ),
        monotonic_func: Callable[[], float] = time.monotonic,
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self.refresh_seconds = float(refresh_seconds)
        if self.refresh_seconds <= 0:
            raise ValueError("refresh_seconds debe ser positivo.")
        self._loader = loader
        self._monotonic = monotonic_func
        self._on_error = on_error
        self._last_attempt_monotonic: float | None = None
        self._last_snapshot: SatelliteQueueSnapshot | None = None
        self._lock = threading.RLock()

    @property
    def has_success(self) -> bool:
        with self._lock:
            return self._last_snapshot is not None

    @property
    def last_success_timestamp_seconds(self) -> float:
        with self._lock:
            if (
                self._last_snapshot is None
                or self._last_snapshot.snapshot_time is None
            ):
                return 0.0
            return self._last_snapshot.snapshot_time.timestamp()

    def get(self) -> SatelliteQueueSnapshot:
        with self._lock:
            now = self._monotonic()
            if (
                self._last_attempt_monotonic is not None
                and now - self._last_attempt_monotonic < self.refresh_seconds
            ):
                return self._last_snapshot or SatelliteQueueSnapshot.empty()

            self._last_attempt_monotonic = now
            try:
                snapshot = self._loader()
            except Exception as exc:
                if self._on_error is not None:
                    self._on_error(exc)
                return self._last_snapshot or SatelliteQueueSnapshot.empty()

            self._last_snapshot = snapshot
            return snapshot


def normalize_error_code(error_code: str | None) -> str:
    normalized = (error_code or "").strip().lower()
    if normalized in _ERROR_CODE_ALLOWLIST:
        return normalized
    return "other"


class SatelliteWorkerMetrics:
    """Explicit per-process registry for worker operational metrics.

    Counters are process-local and reset on worker restart. Prometheus
    rate/increase functions are expected to account for those resets.
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()

        self.jobs_queued_ready = self._gauge(
            "litoral_trace_satellite_jobs_queued_ready"
        )
        self.jobs_queued_delayed = self._gauge(
            "litoral_trace_satellite_jobs_queued_delayed"
        )
        self.jobs_running = self._gauge(
            "litoral_trace_satellite_jobs_running"
        )
        self.jobs_running_stale = self._gauge(
            "litoral_trace_satellite_jobs_running_stale"
        )
        self.jobs_running_invalid = self._gauge(
            "litoral_trace_satellite_jobs_running_invalid"
        )
        self.jobs_oldest_ready_age_seconds = self._gauge(
            "litoral_trace_satellite_jobs_oldest_ready_age_seconds"
        )
        self.jobs_oldest_active_lease_age_seconds = self._gauge(
            "litoral_trace_satellite_jobs_oldest_active_lease_age_seconds"
        )
        self.jobs_oldest_heartbeat_age_seconds = self._gauge(
            "litoral_trace_satellite_jobs_oldest_heartbeat_age_seconds"
        )
        self.jobs_next_delayed_ready_in_seconds = self._gauge(
            "litoral_trace_satellite_jobs_next_delayed_ready_in_seconds"
        )
        self.queue_snapshot_last_success_timestamp_seconds = self._gauge(
            "litoral_trace_satellite_queue_snapshot_last_success_timestamp_seconds"
        )

        self.jobs_claimed = self._counter(
            "litoral_trace_satellite_jobs_claimed_total", ("job_type",)
        )
        self.jobs_succeeded = self._counter(
            "litoral_trace_satellite_jobs_succeeded_total", ("job_type",)
        )
        self.jobs_failed = self._counter(
            "litoral_trace_satellite_jobs_failed_total",
            ("job_type", "error_code"),
        )
        self.jobs_retry_scheduled = self._counter(
            "litoral_trace_satellite_jobs_retry_scheduled_total",
            ("job_type", "error_code"),
        )
        self.jobs_lease_lost = self._counter(
            "litoral_trace_satellite_jobs_lease_lost_total", ("job_type",)
        )
        self.stale_recoveries = self._counter(
            "litoral_trace_satellite_stale_recoveries_total", ("outcome",)
        )
        self.heartbeat_failures = self._counter(
            "litoral_trace_satellite_heartbeat_failures_total"
        )
        self.queue_snapshot_errors = self._counter(
            "litoral_trace_satellite_queue_snapshot_errors_total"
        )

        buckets = (0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900)
        self.claim_duration = Histogram(
            "litoral_trace_satellite_claim_duration_seconds",
            "Duration of a satellite claim transaction.",
            registry=self.registry,
            buckets=buckets,
        )
        self.gee_execution_duration = Histogram(
            "litoral_trace_satellite_gee_execution_duration_seconds",
            "Duration of a satellite GEE adapter call.",
            ("job_type",),
            registry=self.registry,
            buckets=buckets,
        )
        self.execution_duration = Histogram(
            "litoral_trace_satellite_execution_duration_seconds",
            "Duration of a committed satellite worker attempt.",
            ("job_type",),
            registry=self.registry,
            buckets=buckets,
        )
        self.retry_delay = Histogram(
            "litoral_trace_satellite_retry_delay_seconds",
            "Scheduled satellite retry delay.",
            ("job_type",),
            registry=self.registry,
            buckets=buckets,
        )

    def _gauge(self, name: str) -> Gauge:
        return Gauge(name, name, registry=self.registry)

    def _counter(self, name: str, labels: tuple[str, ...] = ()) -> Counter:
        return Counter(name, name, labels, registry=self.registry)

    def bind_queue_snapshot_cache(
        self,
        cache: SatelliteQueueSnapshotCache,
    ) -> None:
        gauge_fields = (
            (self.jobs_queued_ready, "queued_ready_count"),
            (self.jobs_queued_delayed, "queued_delayed_count"),
            (self.jobs_running, "running_count"),
            (self.jobs_running_stale, "running_stale_count"),
            (self.jobs_running_invalid, "running_invalid_count"),
            (
                self.jobs_oldest_ready_age_seconds,
                "oldest_ready_age_seconds",
            ),
            (
                self.jobs_oldest_active_lease_age_seconds,
                "oldest_active_lease_age_seconds",
            ),
            (
                self.jobs_oldest_heartbeat_age_seconds,
                "oldest_heartbeat_age_seconds",
            ),
            (
                self.jobs_next_delayed_ready_in_seconds,
                "next_delayed_ready_in_seconds",
            ),
        )
        for gauge, field_name in gauge_fields:
            gauge.set_function(
                lambda field_name=field_name: float(
                    getattr(cache.get(), field_name)
                )
            )
        self.queue_snapshot_last_success_timestamp_seconds.set_function(
            lambda: (
                cache.get()
                and cache.last_success_timestamp_seconds
            )
        )

    def record_claimed(self, job_type: str) -> None:
        self.jobs_claimed.labels(job_type=str(job_type)).inc()

    def record_succeeded(self, job_type: str) -> None:
        self.jobs_succeeded.labels(job_type=str(job_type)).inc()

    def record_failed(self, job_type: str, error_code: str) -> None:
        self.jobs_failed.labels(
            job_type=str(job_type),
            error_code=normalize_error_code(error_code),
        ).inc()

    def record_retry_scheduled(
        self,
        job_type: str,
        error_code: str,
        delay_seconds: float,
    ) -> None:
        self.jobs_retry_scheduled.labels(
            job_type=str(job_type),
            error_code=normalize_error_code(error_code),
        ).inc()
        self.retry_delay.labels(job_type=str(job_type)).observe(
            max(float(delay_seconds), 0.0)
        )

    def record_lease_lost(self, job_type: str) -> None:
        self.jobs_lease_lost.labels(job_type=str(job_type)).inc()

    def record_stale_recoveries(self, requeued: int, failed: int) -> None:
        if requeued:
            self.stale_recoveries.labels(outcome="requeued").inc(requeued)
        if failed:
            self.stale_recoveries.labels(outcome="failed").inc(failed)

    def record_heartbeat_failure(self) -> None:
        self.heartbeat_failures.inc()

    def record_snapshot_error(self, _exc: Exception) -> None:
        self.queue_snapshot_errors.inc()
        logging.getLogger(__name__).warning(
            "satellite_queue_snapshot_error",
            extra={"error_type": "QueueSnapshotRefreshError"},
        )

    def observe_claim_duration(self, seconds: float) -> None:
        self.claim_duration.observe(max(float(seconds), 0.0))

    def observe_gee_duration(self, job_type: str, seconds: float) -> None:
        self.gee_execution_duration.labels(job_type=str(job_type)).observe(
            max(float(seconds), 0.0)
        )

    def observe_execution_duration(self, job_type: str, seconds: float) -> None:
        self.execution_duration.labels(job_type=str(job_type)).observe(
            max(float(seconds), 0.0)
        )

    def update_queue_snapshot(
        self,
        snapshot: SatelliteQueueSnapshot,
        *,
        last_success_timestamp_seconds: float,
    ) -> None:
        self.jobs_queued_ready.set(snapshot.queued_ready_count)
        self.jobs_queued_delayed.set(snapshot.queued_delayed_count)
        self.jobs_running.set(snapshot.running_count)
        self.jobs_running_stale.set(snapshot.running_stale_count)
        self.jobs_running_invalid.set(snapshot.running_invalid_count)
        self.jobs_oldest_ready_age_seconds.set(
            snapshot.oldest_ready_age_seconds
        )
        self.jobs_oldest_active_lease_age_seconds.set(
            snapshot.oldest_active_lease_age_seconds
        )
        self.jobs_oldest_heartbeat_age_seconds.set(
            snapshot.oldest_heartbeat_age_seconds
        )
        self.jobs_next_delayed_ready_in_seconds.set(
            snapshot.next_delayed_ready_in_seconds
        )
        self.queue_snapshot_last_success_timestamp_seconds.set(
            last_success_timestamp_seconds
        )


class SatelliteWorkerJsonFormatter(logging.Formatter):
    """Render only the worker's explicitly approved structured fields."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created,
                tz=timezone.utc,
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        for field_name in _SAFE_LOG_FIELDS:
            if hasattr(record, field_name):
                payload[field_name] = getattr(record, field_name)
        return json.dumps(payload, separators=(",", ":"), default=str)


def configure_satellite_worker_json_logging(level: int) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(SatelliteWorkerJsonFormatter())
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level)


def start_satellite_metrics_server(
    *,
    host: str,
    port: int,
    registry: CollectorRegistry,
) -> tuple[Any, Any]:
    """Start the optional worker-only HTTP endpoint or fail sanitized."""

    try:
        return start_wsgi_server(port=port, addr=host, registry=registry)
    except Exception as exc:
        raise RuntimeError(
            "No se pudo iniciar el servidor de metricas del worker."
        ) from None
