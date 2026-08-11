"""Generic durable satellite worker runtime."""
from __future__ import annotations

import argparse
import logging
import os
import random
import re
import signal
import socket
import threading
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Callable, Protocol

from sqlalchemy.orm import Session

from litoral_trace.config import get_settings
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.worker import get_worker_db_session
from litoral_trace.services.gee import (
    ALGORITHM_VERSION,
    consultar_serie_temporal_ndvi_gee,
    generate_geometry_hash,
)
from litoral_trace.services.satellite_jobs import (
    ClaimedSatelliteJob,
    SatelliteJobType,
    claim_next_satellite_job,
    recover_stale_satellite_jobs,
    StaleRecoveryResult,
)
from litoral_trace.services.satellite_ndvi_processing import (
    NormalizedNdviExecutionResult,
    SatelliteJobLeaseLostError,
    mark_satellite_job_failed,
    mark_satellite_job_succeeded,
    normalize_ndvi_execution_result,
    persist_ndvi_execution_result,
    update_satellite_job_heartbeat,
)


logger = logging.getLogger(__name__)

_DEFAULT_POLL_SECONDS = 5
_DEFAULT_HEARTBEAT_SECONDS = 15
_DEFAULT_STALE_RECOVERY_INTERVAL_SECONDS = 30
_DEFAULT_STALE_RECOVERY_BATCH_SIZE = 10

_SENSITIVE_TEXT_PATTERNS = (
    re.compile(
        r"postgres(?:ql\+psycopg)?://\S+",
        re.IGNORECASE,
    ),
    re.compile(
        r"Bearer\s+[A-Za-z0-9._\-]+",
        re.IGNORECASE,
    ),
    re.compile(
        (
            r"(access_token|refresh_token|authorization|authorization_code|"
            r"cookie|client_secret|api_key)\s*[:=]\s*\S+"
        ),
        re.IGNORECASE,
    ),
    re.compile(
        r'"private_key"\s*:\s*".+?"',
        re.IGNORECASE,
    ),
)


class WorkerRunStatus(StrEnum):
    IDLE = "idle"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    LEASE_LOST = "lease_lost"
    STOPPED = "stopped"


@dataclass(frozen=True)
class WorkerRunResult:
    status: WorkerRunStatus
    job_id: int | None = None
    organization_id: int | None = None
    job_type: str | None = None
    error_code: str | None = None


@dataclass(frozen=True)
class NdviExecutionRequest:
    polygon_wkt_snapshot: str
    start_date: str
    end_date: str
    max_cloud_pct: float
    geometry_hash: str
    algorithm_version: str


@dataclass(frozen=True)
class WorkerExecutionContext:
    job_id: int
    organization_id: int
    job_type: str
    worker_id: str
    lease_token: str
    claimed_job: ClaimedSatelliteJob


class SatelliteWorkerExecutionError(RuntimeError):
    """Expected, sanitized worker execution failure."""

    def __init__(self, error_code: str, message: str):
        super().__init__(message)
        self.error_code = (
            error_code.strip()[:100]
            or "worker_execution_failed"
        )
        self.safe_message = sanitize_worker_error_message(message)


class GeeNdviAdapter(Protocol):
    """Contract for an NDVI execution backend."""

    def execute(
        self,
        request: NdviExecutionRequest,
    ) -> NormalizedNdviExecutionResult:
        """Run NDVI extraction without SQLAlchemy or tenant persistence."""


class EarthEngineGeeNdviAdapter:
    """Production adapter that requires a real Google Earth Engine execution."""

    def execute(
        self,
        request: NdviExecutionRequest,
    ) -> NormalizedNdviExecutionResult:
        result = consultar_serie_temporal_ndvi_gee(
            polygon_wkt=request.polygon_wkt_snapshot,
            start_date=request.start_date,
            end_date=request.end_date,
            max_cloud_pct=request.max_cloud_pct,
        )

        if result.get("status") != "success":
            raise SatelliteWorkerExecutionError(
                "gee_execution_failed",
                str(
                    result.get("error_detail")
                    or (
                        "La ejecucion Earth Engine no produjo "
                        "un resultado exitoso."
                    )
                ),
            )

        # Critical production invariant:
        # consultar_serie_temporal_ndvi_gee() may return simulated data in
        # explicitly configured test mode when GEE authentication is
        # unavailable. The real Earth Engine adapter must never persist that
        # fallback as a successful durable worker execution.
        #
        # Tests that need deterministic simulated execution must inject their
        # own GeeNdviAdapter implementation into SatelliteWorker.
        if result.get("gee_connected") is not True:
            raise SatelliteWorkerExecutionError(
                "gee_execution_failed",
                (
                    "El worker requiere una ejecucion real de "
                    "Google Earth Engine."
                ),
            )

        normalized = normalize_ndvi_execution_result(result)

        return NormalizedNdviExecutionResult(
            geometry_hash=(
                normalized.geometry_hash
                or request.geometry_hash
            ),
            algorithm_version=(
                normalized.algorithm_version
                or request.algorithm_version
            ),
            observations=normalized.observations,
        )


def sanitize_worker_error_message(
    message: str,
    *,
    max_length: int = 1024,
) -> str:
    """Remove common secret-bearing values from persisted/logged errors."""

    sanitized_message = (message or "").strip()

    for pattern in _SENSITIVE_TEXT_PATTERNS:
        sanitized_message = pattern.sub(
            "[REDACTED]",
            sanitized_message,
        )

    if not sanitized_message:
        sanitized_message = "worker execution failed"

    return sanitized_message[:max_length]


def _normalize_worker_id(worker_id: str) -> str:
    normalized_worker_id = (worker_id or "").strip()

    if not normalized_worker_id:
        raise RuntimeError(
            "SATELLITE_WORKER_ID no puede ser vacio."
        )

    if len(normalized_worker_id) > 255:
        raise RuntimeError(
            "SATELLITE_WORKER_ID no puede superar 255 caracteres."
        )

    return normalized_worker_id


def build_default_worker_id() -> str:
    """Build a process-specific worker identity for operational tracing."""

    hostname = socket.gethostname() or "worker-host"
    pid = os.getpid()
    suffix = f"{random.randint(0, 0xFFFF):04x}"

    return _normalize_worker_id(
        f"{hostname}:{pid}:{suffix}"
    )


def resolve_satellite_worker_id() -> str:
    configured_worker_id = (
        get_settings().workers.satellite_worker_id
    )

    if configured_worker_id:
        return _normalize_worker_id(configured_worker_id)

    return build_default_worker_id()


def resolve_satellite_worker_poll_seconds() -> int:
    configured_poll_seconds = int(
        get_settings().workers.satellite_worker_poll_seconds
        or _DEFAULT_POLL_SECONDS
    )

    if configured_poll_seconds <= 0:
        raise RuntimeError(
            "SATELLITE_WORKER_POLL_SECONDS debe ser positivo."
        )

    return configured_poll_seconds


def resolve_satellite_worker_heartbeat_seconds() -> int:
    configured_heartbeat_seconds = int(
        get_settings().workers.satellite_worker_heartbeat_seconds
        or _DEFAULT_HEARTBEAT_SECONDS
    )

    if configured_heartbeat_seconds <= 0:
        raise RuntimeError(
            "SATELLITE_WORKER_HEARTBEAT_SECONDS debe ser positivo."
        )

    return configured_heartbeat_seconds


def resolve_satellite_worker_stale_recovery_interval_seconds() -> int:
    configured_interval_seconds = int(
        get_settings().workers.satellite_worker_stale_recovery_interval_seconds
        or _DEFAULT_STALE_RECOVERY_INTERVAL_SECONDS
    )

    if configured_interval_seconds <= 0:
        raise RuntimeError(
            (
                "SATELLITE_WORKER_STALE_RECOVERY_INTERVAL_SECONDS "
                "debe ser positivo."
            )
        )

    return configured_interval_seconds


def _format_job_log_fields(
    context: WorkerExecutionContext,
) -> dict[str, Any]:
    """Return non-secret structured fields safe for operational logging."""

    return {
        "job_id": context.job_id,
        "organization_id": context.organization_id,
        "job_type": context.job_type,
        "worker_id": context.worker_id,
    }


class _SatelliteJobHeartbeatController:
    """Periodic tenant-runtime heartbeat loop for one active durable job."""

    def __init__(
        self,
        *,
        organization_id: int,
        job_id: int,
        job_type: str,
        worker_id: str,
        lease_token: str,
        heartbeat_seconds: int,
        tenant_session_factory: Callable[[], Session],
    ):
        self.organization_id = int(organization_id)
        self.job_id = int(job_id)
        self.job_type = str(job_type)
        self.worker_id = str(worker_id)
        self._lease_token = str(lease_token)
        self._heartbeat_seconds = int(heartbeat_seconds)
        self._tenant_session_factory = tenant_session_factory
        self._stop_event = threading.Event()
        self._lease_lost_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"satellite-heartbeat-{self.job_id}",
            daemon=True,
        )
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self._thread.start()
        self._started = True

    def stop(self) -> None:
        self._stop_event.set()

    def join(self) -> None:
        if self._started:
            self._thread.join()

    def is_alive(self) -> bool:
        return self._thread.is_alive() if self._started else False

    def has_lease_lost(self) -> bool:
        return self._lease_lost_event.is_set()

    def _run(self) -> None:
        while not self._stop_event.wait(self._heartbeat_seconds):
            tenant_session = self._tenant_session_factory()

            if tenant_session is None:
                self._log_generic_error(
                    RuntimeError(
                        "Servicio de base de datos tenant no disponible."
                    )
                )
                continue

            try:
                update_satellite_job_heartbeat(
                    tenant_session,
                    organization_id=self.organization_id,
                    job_id=self.job_id,
                    worker_id=self.worker_id,
                    lease_token=self._lease_token,
                )
                tenant_session.commit()

            except SatelliteJobLeaseLostError:
                tenant_session.rollback()
                self._lease_lost_event.set()
                self._stop_event.set()
                break

            except Exception as exc:
                tenant_session.rollback()
                self._log_generic_error(exc)

            finally:
                tenant_session.close()

    def _log_generic_error(self, exc: Exception) -> None:
        logger.warning(
            "satellite_worker_heartbeat_error",
            extra={
                "job_id": self.job_id,
                "organization_id": self.organization_id,
                "job_type": self.job_type,
                "worker_id": self.worker_id,
                "error_type": type(exc).__name__,
                "error_message": sanitize_worker_error_message(
                    str(exc)
                ),
            },
        )


class SatelliteWorker:
    """Durable satellite queue worker.

    P2.2C responsibilities:
    - atomically claim one durable job;
    - commit the claim before external execution;
    - dispatch only code-defined handlers;
    - execute NDVI against the stored geometry snapshot;
    - persist tenant data through the ordinary runtime principal;
    - commit observations and terminal job state atomically;
    - fail deterministically without implementing retries/recovery.

    Lease fencing, periodic heartbeat and stale-job recovery belong to P2.2D.
    Retry/backoff policy remains future work.
    """

    def __init__(
        self,
        *,
        worker_id: str,
        poll_seconds: int = _DEFAULT_POLL_SECONDS,
        heartbeat_seconds: int = _DEFAULT_HEARTBEAT_SECONDS,
        stale_recovery_interval_seconds: int | None = None,
        claim_session_factory: Callable[[], Session] = (
            get_worker_db_session
        ),
        tenant_session_factory: Callable[[], Session] = (
            get_db_session
        ),
        claim_job_func: Callable[
            ...,
            ClaimedSatelliteJob | None,
        ] = claim_next_satellite_job,
        recover_stale_jobs_func: Callable[
            ...,
            StaleRecoveryResult,
        ] = recover_stale_satellite_jobs,
        gee_ndvi_adapter: GeeNdviAdapter | None = None,
        sleep_func: Callable[[float], None] = time.sleep,
        monotonic_func: Callable[[], float] = time.monotonic,
    ):
        self.worker_id = _normalize_worker_id(worker_id)
        self.poll_seconds = int(poll_seconds)

        if self.poll_seconds <= 0:
            raise RuntimeError(
                "poll_seconds debe ser positivo."
            )

        self.heartbeat_seconds = int(heartbeat_seconds)
        if self.heartbeat_seconds <= 0:
            raise RuntimeError(
                "heartbeat_seconds debe ser positivo."
            )

        self.stale_recovery_interval_seconds = (
            None
            if stale_recovery_interval_seconds is None
            else int(stale_recovery_interval_seconds)
        )
        if (
            self.stale_recovery_interval_seconds is not None
            and self.stale_recovery_interval_seconds <= 0
        ):
            raise RuntimeError(
                "stale_recovery_interval_seconds debe ser positivo."
            )

        self.stale_recovery_batch_size = _DEFAULT_STALE_RECOVERY_BATCH_SIZE
        self._claim_session_factory = claim_session_factory
        self._tenant_session_factory = tenant_session_factory
        self._claim_job_func = claim_job_func
        self._recover_stale_jobs_func = recover_stale_jobs_func
        self._gee_ndvi_adapter = (
            gee_ndvi_adapter
            or EarthEngineGeeNdviAdapter()
        )
        self._sleep = sleep_func
        self._monotonic = monotonic_func
        self._stop_requested = False
        self._last_stale_recovery_attempt_monotonic: float | None = None

        # Explicit code-defined dispatch only.
        # No dynamic imports, eval or arbitrary handler names from DB data.
        self._handlers: dict[
            str,
            Callable[
                [WorkerExecutionContext],
                NormalizedNdviExecutionResult,
            ],
        ] = {
            SatelliteJobType.NDVI_TIMESERIES.value:
                self._handle_ndvi_timeseries
        }

    def request_shutdown(self) -> None:
        """Prevent new claims after the current execution finishes."""

        self._stop_requested = True

    def _build_context(
        self,
        claimed_job: ClaimedSatelliteJob,
    ) -> WorkerExecutionContext:
        return WorkerExecutionContext(
            job_id=claimed_job.id,
            organization_id=claimed_job.organization_id,
            job_type=claimed_job.job_type,
            worker_id=self.worker_id,
            lease_token=str(claimed_job.lease_token),
            claimed_job=claimed_job,
        )

    def _claim_next_job(
        self,
    ) -> ClaimedSatelliteJob | None:
        """Claim and commit before any external GEE execution."""

        claim_session = self._claim_session_factory()

        if claim_session is None:
            raise RuntimeError(
                "No se pudo abrir la sesion de claim del worker."
            )

        try:
            claimed_job = self._claim_job_func(
                worker_id=self.worker_id,
                db_session=claim_session,
            )

            # Critical boundary:
            # external GEE work must happen only after the durable claim
            # transaction has committed.
            claim_session.commit()

            return claimed_job

        except Exception:
            claim_session.rollback()
            raise

        finally:
            claim_session.close()

    def _should_attempt_stale_recovery(
        self,
        current_monotonic: float,
    ) -> bool:
        if self.stale_recovery_interval_seconds is None:
            return False

        if self._last_stale_recovery_attempt_monotonic is None:
            return True

        return (
            current_monotonic
            - self._last_stale_recovery_attempt_monotonic
        ) >= self.stale_recovery_interval_seconds

    def _maybe_recover_stale_jobs(self) -> None:
        current_monotonic = self._monotonic()
        if not self._should_attempt_stale_recovery(current_monotonic):
            return

        self._last_stale_recovery_attempt_monotonic = current_monotonic
        recovery_session = self._claim_session_factory()
        if recovery_session is None:
            logger.warning(
                "satellite_worker_stale_recovery_error",
                extra={
                    "worker_id": self.worker_id,
                    "error_type": "RuntimeError",
                    "error_message": sanitize_worker_error_message(
                        "Servicio de recovery satelital no disponible."
                    ),
                },
            )
            return

        start_monotonic = self._monotonic()

        try:
            result = self._recover_stale_jobs_func(
                requested_batch_size=self.stale_recovery_batch_size,
                db_session=recovery_session,
            )
            recovery_session.commit()
            logger.info(
                "satellite_worker_stale_recovery",
                extra={
                    "worker_id": self.worker_id,
                    "requeued_count": result.requeued_count,
                    "failed_count": result.failed_count,
                    "elapsed_ms": int(
                        (
                            self._monotonic()
                            - start_monotonic
                        )
                        * 1000
                    ),
                },
            )
        except Exception as exc:
            recovery_session.rollback()
            logger.warning(
                "satellite_worker_stale_recovery_error",
                extra={
                    "worker_id": self.worker_id,
                    "error_type": type(exc).__name__,
                    "error_message": sanitize_worker_error_message(
                        str(exc)
                    ),
                },
            )
        finally:
            recovery_session.close()

    def _create_heartbeat_controller(
        self,
        context: WorkerExecutionContext,
    ) -> _SatelliteJobHeartbeatController:
        return _SatelliteJobHeartbeatController(
            organization_id=context.organization_id,
            job_id=context.job_id,
            job_type=context.job_type,
            worker_id=context.worker_id,
            lease_token=context.lease_token,
            heartbeat_seconds=self.heartbeat_seconds,
            tenant_session_factory=self._tenant_session_factory,
        )

    def _stop_heartbeat_controller(
        self,
        heartbeat_controller: _SatelliteJobHeartbeatController,
    ) -> bool:
        heartbeat_controller.stop()
        heartbeat_controller.join()
        return heartbeat_controller.has_lease_lost()

    def _build_ndvi_request(
        self,
        context: WorkerExecutionContext,
    ) -> NdviExecutionRequest:
        claimed_job = context.claimed_job

        polygon_wkt_snapshot = (
            claimed_job.polygon_wkt_snapshot
            or ""
        )

        if not polygon_wkt_snapshot.strip():
            raise SatelliteWorkerExecutionError(
                "invalid_job_payload",
                (
                    "El job NDVI no contiene "
                    "polygon_wkt_snapshot."
                ),
            )

        if claimed_job.algorithm_version != ALGORITHM_VERSION:
            raise SatelliteWorkerExecutionError(
                "unsupported_algorithm_version",
                (
                    "El worker no soporta la version "
                    "de algoritmo solicitada."
                ),
            )

        # Recompute the hash from the immutable execution snapshot.
        # The live Lote geometry must not influence execution once queued.
        snapshot_hash = generate_geometry_hash(
            polygon_wkt_snapshot
        )

        if snapshot_hash != claimed_job.geometry_hash:
            raise SatelliteWorkerExecutionError(
                "geometry_hash_mismatch",
                (
                    "La geometria snapshot no coincide con "
                    "el geometry_hash almacenado."
                ),
            )

        if (
            claimed_job.request_start_date is None
            or claimed_job.request_end_date is None
        ):
            raise SatelliteWorkerExecutionError(
                "invalid_job_payload",
                (
                    "El job NDVI no contiene rango "
                    "de fechas valido."
                ),
            )

        if claimed_job.max_cloud_pct is None:
            raise SatelliteWorkerExecutionError(
                "invalid_job_payload",
                (
                    "El job NDVI no contiene "
                    "max_cloud_pct."
                ),
            )

        return NdviExecutionRequest(
            polygon_wkt_snapshot=polygon_wkt_snapshot,
            start_date=(
                claimed_job.request_start_date.isoformat()
            ),
            end_date=(
                claimed_job.request_end_date.isoformat()
            ),
            max_cloud_pct=float(
                claimed_job.max_cloud_pct
            ),
            geometry_hash=str(
                claimed_job.geometry_hash
                or snapshot_hash
            ),
            algorithm_version=str(
                claimed_job.algorithm_version
            ),
        )

    def _handle_ndvi_timeseries(
        self,
        context: WorkerExecutionContext,
    ) -> NormalizedNdviExecutionResult:
        request = self._build_ndvi_request(context)

        result = self._gee_ndvi_adapter.execute(request)

        if result.geometry_hash != request.geometry_hash:
            raise SatelliteWorkerExecutionError(
                "geometry_hash_mismatch",
                (
                    "El adaptador GEE devolvio un "
                    "geometry_hash inconsistente."
                ),
            )

        if (
            result.algorithm_version
            != request.algorithm_version
        ):
            raise SatelliteWorkerExecutionError(
                "unsupported_algorithm_version",
                (
                    "El adaptador GEE devolvio una "
                    "version de algoritmo inconsistente."
                ),
            )

        return result

    def _persist_success(
        self,
        context: WorkerExecutionContext,
        result: NormalizedNdviExecutionResult,
    ) -> None:
        """Persist observations and SUCCEEDED in one tenant transaction."""

        tenant_session = self._tenant_session_factory()

        if tenant_session is None:
            raise RuntimeError(
                "Servicio de base de datos tenant no disponible."
            )

        try:
            persist_ndvi_execution_result(
                tenant_session,
                organization_id=context.organization_id,
                lote_id=int(
                    context.claimed_job.lote_id
                ),
                satellite_job_id=context.job_id,
                result=result,
            )

            mark_satellite_job_succeeded(
                tenant_session,
                organization_id=context.organization_id,
                job_id=context.job_id,
                worker_id=context.worker_id,
                lease_token=context.lease_token,
            )

            tenant_session.commit()

        except Exception:
            tenant_session.rollback()
            raise

        finally:
            tenant_session.close()

    def _persist_failure(
        self,
        context: WorkerExecutionContext,
        *,
        error_code: str,
        error_message: str,
    ) -> None:
        """Persist a deterministic terminal failure through tenant RLS."""

        tenant_session = self._tenant_session_factory()

        if tenant_session is None:
            raise RuntimeError(
                "Servicio de base de datos tenant no disponible."
            )

        safe_error_message = sanitize_worker_error_message(
            error_message
        )

        try:
            mark_satellite_job_failed(
                tenant_session,
                organization_id=context.organization_id,
                job_id=context.job_id,
                worker_id=context.worker_id,
                lease_token=context.lease_token,
                error_code=error_code,
                error_message=safe_error_message,
            )

            tenant_session.commit()

        except Exception:
            tenant_session.rollback()
            raise

        finally:
            tenant_session.close()

    def run_once(self) -> WorkerRunResult:
        """Claim and execute at most one durable satellite job."""

        if self._stop_requested:
            return WorkerRunResult(
                status=WorkerRunStatus.STOPPED
            )

        self._maybe_recover_stale_jobs()
        claimed_job = self._claim_next_job()

        if claimed_job is None:
            return WorkerRunResult(
                status=WorkerRunStatus.IDLE
            )

        context = self._build_context(claimed_job)
        heartbeat_controller = self._create_heartbeat_controller(context)
        start_monotonic = self._monotonic()

        try:
            heartbeat_controller.start()

            handler = self._handlers.get(
                context.job_type
            )

            if handler is None:
                raise SatelliteWorkerExecutionError(
                    "unsupported_job_type",
                    (
                        "Job type no soportado: "
                        f"{context.job_type}"
                    ),
                )

            result = handler(context)

            if self._stop_heartbeat_controller(heartbeat_controller):
                return self._build_lease_lost_result(
                    context,
                    start_monotonic=start_monotonic,
                )

            self._persist_success(
                context,
                result,
            )

            logger.info(
                "satellite_worker_job_succeeded",
                extra={
                    **_format_job_log_fields(context),
                    "elapsed_ms": int(
                        (
                            self._monotonic()
                            - start_monotonic
                        )
                        * 1000
                    ),
                },
            )

            return WorkerRunResult(
                status=WorkerRunStatus.SUCCEEDED,
                job_id=context.job_id,
                organization_id=context.organization_id,
                job_type=context.job_type,
            )

        except (KeyboardInterrupt, SystemExit):
            self._stop_heartbeat_controller(heartbeat_controller)
            raise

        except SatelliteJobLeaseLostError:
            self._stop_heartbeat_controller(heartbeat_controller)
            return self._build_lease_lost_result(
                context,
                start_monotonic=start_monotonic,
            )

        except SatelliteWorkerExecutionError as exc:
            if self._stop_heartbeat_controller(heartbeat_controller):
                return self._build_lease_lost_result(
                    context,
                    start_monotonic=start_monotonic,
                )

            try:
                self._persist_failure(
                    context,
                    error_code=exc.error_code,
                    error_message=exc.safe_message,
                )
            except SatelliteJobLeaseLostError:
                return self._build_lease_lost_result(
                    context,
                    start_monotonic=start_monotonic,
                )

            logger.warning(
                "satellite_worker_job_failed",
                extra={
                    **_format_job_log_fields(context),
                    "error_code": exc.error_code,
                    "elapsed_ms": int(
                        (
                            self._monotonic()
                            - start_monotonic
                        )
                        * 1000
                    ),
                },
            )

            return WorkerRunResult(
                status=WorkerRunStatus.FAILED,
                job_id=context.job_id,
                organization_id=context.organization_id,
                job_type=context.job_type,
                error_code=exc.error_code,
            )

        except Exception as exc:
            sanitized_message = (
                sanitize_worker_error_message(
                    str(exc)
                )
            )

            if self._stop_heartbeat_controller(heartbeat_controller):
                return self._build_lease_lost_result(
                    context,
                    start_monotonic=start_monotonic,
                )

            try:
                self._persist_failure(
                    context,
                    error_code="worker_execution_failed",
                    error_message=sanitized_message,
                )
            except SatelliteJobLeaseLostError:
                return self._build_lease_lost_result(
                    context,
                    start_monotonic=start_monotonic,
                )

            logger.warning(
                "satellite_worker_job_failed",
                extra={
                    **_format_job_log_fields(context),
                    "error_code":
                        "worker_execution_failed",
                    "error_type":
                        type(exc).__name__,
                    "elapsed_ms": int(
                        (
                            self._monotonic()
                            - start_monotonic
                        )
                        * 1000
                    ),
                },
            )

            return WorkerRunResult(
                status=WorkerRunStatus.FAILED,
                job_id=context.job_id,
                organization_id=context.organization_id,
                job_type=context.job_type,
                error_code="worker_execution_failed",
            )

    def _build_lease_lost_result(
        self,
        context: WorkerExecutionContext,
        *,
        start_monotonic: float,
    ) -> WorkerRunResult:
        logger.warning(
            "satellite_worker_job_lease_lost",
            extra={
                **_format_job_log_fields(context),
                "error_code": "lease_lost",
                "elapsed_ms": int(
                    (
                        self._monotonic()
                        - start_monotonic
                    )
                    * 1000
                ),
            },
        )

        return WorkerRunResult(
        status=WorkerRunStatus.LEASE_LOST,
            job_id=context.job_id,
            organization_id=context.organization_id,
            job_type=context.job_type,
            error_code="lease_lost",
        )

    def run_forever(self) -> None:
        """Poll until shutdown without leaking raw exception tracebacks."""

        while not self._stop_requested:
            try:
                result = self.run_once()

            except (KeyboardInterrupt, SystemExit):
                raise

            except Exception as exc:
                # Deliberately avoid logger.exception() here.
                # Driver/cloud exceptions may include connection or
                # authentication material in their traceback/repr.
                logger.warning(
                    "satellite_worker_loop_error",
                    extra={
                        "worker_id": self.worker_id,
                        "error_type": type(exc).__name__,
                        "error_message":
                            sanitize_worker_error_message(
                                str(exc)
                            ),
                    },
                )

                if self._stop_requested:
                    break

                self._sleep(
                    self.poll_seconds
                )
                continue

            if result.status in {
                WorkerRunStatus.IDLE,
                WorkerRunStatus.STOPPED,
            }:
                if (
                    self._stop_requested
                    or result.status
                    is WorkerRunStatus.STOPPED
                ):
                    break

                self._sleep(
                    self.poll_seconds
                )


def build_satellite_worker() -> SatelliteWorker:
    """Build the default production satellite worker."""

    return SatelliteWorker(
        worker_id=resolve_satellite_worker_id(),
        poll_seconds=resolve_satellite_worker_poll_seconds(),
        heartbeat_seconds=resolve_satellite_worker_heartbeat_seconds(),
        stale_recovery_interval_seconds=(
            resolve_satellite_worker_stale_recovery_interval_seconds()
        ),
    )


def _install_signal_handlers(
    worker: SatelliteWorker,
) -> None:
    """Request graceful shutdown on SIGINT/SIGTERM."""

    def _handle_signal(signum, _frame):
        logger.info(
            "satellite_worker_shutdown_requested",
            extra={
                "signal": signum,
                "worker_id": worker.worker_id,
            },
        )

        worker.request_shutdown()

    for signum in (
        signal.SIGINT,
        signal.SIGTERM,
    ):
        try:
            signal.signal(
                signum,
                _handle_signal,
            )
        except Exception:
            # Some runtimes/platforms may not support all signals.
            continue


def main(
    argv: list[str] | None = None,
) -> int:
    """CLI entry point for the satellite worker."""

    parser = argparse.ArgumentParser(
        description=(
            "Litoral Trace satellite worker"
        )
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help=(
            "Claim and execute at most one "
            "durable satellite job."
        ),
    )

    args = parser.parse_args(argv)

    configured_log_level = (
        get_settings()
        .observability
        .log_level
        .upper()
    )

    logging.basicConfig(
        level=getattr(
            logging,
            configured_log_level,
            logging.INFO,
        )
    )

    worker = build_satellite_worker()
    _install_signal_handlers(worker)

    if args.once:
        result = worker.run_once()

        return (
            0
            if result.status
            in {
                WorkerRunStatus.IDLE,
                WorkerRunStatus.SUCCEEDED,
            }
            else 1
        )

    worker.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
