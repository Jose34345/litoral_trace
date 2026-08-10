from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, select

from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import (
    Lote,
    Organization,
    SatelliteJob,
    SatelliteNdviObservation,
)
from litoral_trace.db.worker import (
    get_worker_database_url,
    reset_worker_engine_state,
)
from litoral_trace.services.gee import (
    ALGORITHM_VERSION,
    generate_geometry_hash,
)
from litoral_trace.services.satellite_jobs import (
    ClaimedSatelliteJob,
    SatelliteJobType,
)
from litoral_trace.services.satellite_ndvi_processing import (
    NormalizedNdviExecutionResult,
    NdviObservationRecord,
    mark_satellite_job_succeeded,
    persist_ndvi_execution_result,
)
from litoral_trace.workers.satellite_worker import (
    EarthEngineGeeNdviAdapter,
    NdviExecutionRequest,
    SatelliteWorker,
    SatelliteWorkerExecutionError,
    WorkerRunStatus,
    sanitize_worker_error_message,
)


@pytest.fixture(autouse=True)
def cleanup_satellite_worker_state(monkeypatch):
    reset_worker_engine_state()
    _cleanup_worker_entities()
    yield
    reset_worker_engine_state()
    _cleanup_worker_entities()


@dataclass
class _RecordingClaimSession:
    committed: bool = False
    rolled_back: bool = False
    closed: bool = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class _ExplodingTenantSession:
    def __init__(self):
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def commit(self):
        raise RuntimeError("tenant commit blocked")

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class _RecordingTenantSession:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class _FakeGeeAdapter:
    def __init__(
        self,
        *,
        result=None,
        error: Exception | None = None,
        on_execute=None,
    ):
        self.result = result
        self.error = error
        self.on_execute = on_execute
        self.calls: list = []

    def execute(self, request):
        self.calls.append(request)

        if self.on_execute is not None:
            self.on_execute(request)

        if self.error is not None:
            raise self.error

        return self.result


def _cleanup_worker_entities() -> None:
    session = get_db_session()

    worker_org_ids = session.execute(
        select(Organization.id).where(
            Organization.slug.like("worker-org-%")
        )
    ).scalars().all()

    if worker_org_ids:
        session.execute(
            delete(SatelliteNdviObservation).where(
                SatelliteNdviObservation.organization_id.in_(
                    worker_org_ids
                )
            )
        )

        session.execute(
            delete(SatelliteJob).where(
                SatelliteJob.organization_id.in_(
                    worker_org_ids
                )
            )
        )

        session.execute(
            delete(Lote).where(
                Lote.organization_id.in_(
                    worker_org_ids
                )
            )
        )

        session.execute(
            delete(Organization).where(
                Organization.id.in_(
                    worker_org_ids
                )
            )
        )

        session.commit()

    session.close()


def _create_running_claimed_job(
    *,
    polygon_wkt_snapshot: str | None = None,
    algorithm_version: str = ALGORITHM_VERSION,
    worker_id: str = "worker-test",
) -> ClaimedSatelliteJob:
    session = get_db_session()
    suffix = uuid4().hex[:8]

    organization = Organization(
        name=f"Worker Org {suffix}",
        slug=f"worker-org-{suffix}",
        tax_id=f"80-{suffix}",
        tier="pro",
        is_active=True,
    )
    session.add(organization)
    session.flush()

    lote = Lote(
        organization_id=organization.id,
        identificador=f"WORKER-LOTE-{suffix}",
        productor_id=f"30-{suffix}",
        producto_forestal="Madera Aserrada (Pino)",
        hectareas=10.0,
        latitud=-27.45,
        longitud=-58.90,
        polygon_wkt=(
            polygon_wkt_snapshot
            or (
                "POLYGON(("
                "-58.91 -27.46, "
                "-58.89 -27.46, "
                "-58.89 -27.44, "
                "-58.91 -27.44, "
                "-58.91 -27.46"
                "))"
            )
        ),
        estatus="Pendiente",
        volumen_ingresado_ton=20.0,
        volumen_exportar_ton=5.0,
    )
    session.add(lote)
    session.flush()

    snapshot = (
        polygon_wkt_snapshot
        or lote.polygon_wkt
    )
    geometry_hash = generate_geometry_hash(
        snapshot
    )

    job = SatelliteJob(
        organization_id=organization.id,
        lote_id=lote.id,
        job_type=SatelliteJobType.NDVI_TIMESERIES.value,
        status="running",
        attempt_count=1,
        max_attempts=3,
        request_start_date=date(2020, 12, 31),
        request_end_date=date(2026, 8, 9),
        max_cloud_pct=20.0,
        geometry_hash=geometry_hash,
        algorithm_version=algorithm_version,
        polygon_wkt_snapshot=snapshot,
        locked_at=datetime.now(timezone.utc),
        locked_by=worker_id,
        heartbeat_at=datetime.now(timezone.utc),
        lease_token=str(uuid4()),
        started_at=datetime.now(timezone.utc),
    )
    session.add(job)
    session.commit()
    session.refresh(job)

    claimed_job = ClaimedSatelliteJob(
        id=job.id,
        organization_id=job.organization_id,
        lote_id=job.lote_id,
        job_type=job.job_type,
        status=job.status,
        attempt_count=job.attempt_count,
        max_attempts=job.max_attempts,
        next_attempt_at=job.next_attempt_at,
        locked_by=job.locked_by or worker_id,
        locked_at=job.locked_at,
        heartbeat_at=job.heartbeat_at,
        lease_token=(
            UUID(job.lease_token)
            if job.lease_token is not None
            else uuid4()
        ),
        started_at=job.started_at,
        request_start_date=job.request_start_date,
        request_end_date=job.request_end_date,
        max_cloud_pct=job.max_cloud_pct,
        geometry_hash=job.geometry_hash,
        algorithm_version=job.algorithm_version,
        polygon_wkt_snapshot=job.polygon_wkt_snapshot,
    )

    session.close()
    return claimed_job


def _normalized_result_from_claimed_job(
    claimed_job: ClaimedSatelliteJob,
) -> NormalizedNdviExecutionResult:
    return NormalizedNdviExecutionResult(
        geometry_hash=str(
            claimed_job.geometry_hash
        ),
        algorithm_version=str(
            claimed_job.algorithm_version
        ),
        observations=(
            NdviObservationRecord(
                observation_date=date(2026, 8, 1),
                ndvi_mean=0.61,
                ndvi_min=0.55,
                ndvi_max=0.68,
                ndvi_std=0.03,
                scene_cloud_percentage=5.0,
                valid_pixel_count=10,
                valid_pixel_percentage=98.0,
                satellite="Sentinel-2",
                collection="COPERNICUS/S2_SR_HARMONIZED",
                geometry_hash=str(
                    claimed_job.geometry_hash
                ),
                algorithm_version=str(
                    claimed_job.algorithm_version
                ),
                processing_date=datetime.now(
                    timezone.utc
                ),
            ),
        ),
    )


def test_real_gee_adapter_rejects_simulated_fallback(
    monkeypatch,
):
    """A real worker must never persist GEE test fallback as real data."""

    polygon_wkt = (
        "POLYGON(("
        "-58.00 -27.00, "
        "-57.90 -27.00, "
        "-57.90 -26.90, "
        "-58.00 -26.90, "
        "-58.00 -27.00"
        "))"
    )

    geometry_hash = generate_geometry_hash(
        polygon_wkt
    )

    request = NdviExecutionRequest(
        polygon_wkt_snapshot=polygon_wkt,
        start_date="2025-07-01",
        end_date="2025-08-01",
        max_cloud_pct=20.0,
        geometry_hash=geometry_hash,
        algorithm_version=ALGORITHM_VERSION,
    )

    def _simulated_gee_result(**_kwargs):
        return {
            "status": "success",
            "gee_connected": False,
            "geometry_hash": geometry_hash,
            "total_observations": 1,
            "observations": [
                {
                    "observation_date": "2025-07-04",
                    "ndvi_mean": 0.55,
                    "ndvi_min": 0.55,
                    "ndvi_max": 0.55,
                    "ndvi_std": 0.0,
                    "scene_cloud_percentage": 5.0,
                    "aoi_cloud_percentage": 1.0,
                    "valid_pixel_percentage": 98.0,
                    "satellite": "Sentinel-2_TestMock",
                    "collection": (
                        "COPERNICUS/S2_SR_HARMONIZED"
                    ),
                    "processing_date": (
                        "2025-07-04T12:00:00+00:00"
                    ),
                    "geometry_hash": geometry_hash,
                    "algorithm_version": (
                        ALGORITHM_VERSION
                    ),
                }
            ],
        }

    monkeypatch.setattr(
        (
            "litoral_trace.workers.satellite_worker."
            "consultar_serie_temporal_ndvi_gee"
        ),
        _simulated_gee_result,
    )

    adapter = EarthEngineGeeNdviAdapter()

    with pytest.raises(
        SatelliteWorkerExecutionError
    ) as exc_info:
        adapter.execute(request)

    assert (
        exc_info.value.error_code
        == "gee_execution_failed"
    )

    assert exc_info.value.safe_message == (
        "El worker requiere una ejecucion real "
        "de Google Earth Engine."
    )


def test_run_once_with_empty_queue_returns_idle_and_does_not_call_adapter():
    claim_session = _RecordingClaimSession()
    adapter = _FakeGeeAdapter(
        result=None
    )

    worker = SatelliteWorker(
        worker_id="worker-test",
        claim_session_factory=lambda: claim_session,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: None,
        gee_ndvi_adapter=adapter,
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.IDLE
    assert adapter.calls == []
    assert claim_session.committed is True
    assert claim_session.closed is True


def test_supported_ndvi_job_dispatches_exactly_once_and_uses_snapshot_inputs():
    claim_session = _RecordingClaimSession()

    snapshot = (
        "POLYGON(("
        "-58.00 -27.00, "
        "-57.90 -27.00, "
        "-57.90 -26.90, "
        "-58.00 -26.90, "
        "-58.00 -27.00"
        "))"
    )

    claimed_job = _create_running_claimed_job(
        polygon_wkt_snapshot=snapshot
    )

    adapter = _FakeGeeAdapter(
        result=_normalized_result_from_claimed_job(
            claimed_job
        )
    )

    worker = SatelliteWorker(
        worker_id="worker-test",
        claim_session_factory=lambda: claim_session,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: claimed_job,
        gee_ndvi_adapter=adapter,
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.SUCCEEDED
    assert len(adapter.calls) == 1

    request = adapter.calls[0]

    assert request.polygon_wkt_snapshot == snapshot
    assert request.start_date == "2020-12-31"
    assert request.end_date == "2026-08-09"
    assert request.max_cloud_pct == 20.0
    assert request.algorithm_version == ALGORITHM_VERSION
    assert (
        request.geometry_hash
        == claimed_job.geometry_hash
    )


def test_unknown_job_type_fails_deterministically_without_dynamic_dispatch():
    claim_session = _RecordingClaimSession()

    claimed_job = _create_running_claimed_job()

    claimed_job = ClaimedSatelliteJob(
        **{
            **claimed_job.__dict__,
            "job_type": "unexpected_job_type",
        }
    )

    adapter = _FakeGeeAdapter(
        result=_normalized_result_from_claimed_job(
            claimed_job
        )
    )

    worker = SatelliteWorker(
        worker_id="worker-test",
        claim_session_factory=lambda: claim_session,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: claimed_job,
        gee_ndvi_adapter=adapter,
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.FAILED
    assert (
        result.error_code
        == "unsupported_job_type"
    )
    assert adapter.calls == []


def test_worker_does_not_reload_current_lote_geometry():
    old_snapshot = (
        "POLYGON(("
        "-58.10 -27.10, "
        "-58.00 -27.10, "
        "-58.00 -27.00, "
        "-58.10 -27.00, "
        "-58.10 -27.10"
        "))"
    )

    claimed_job = _create_running_claimed_job(
        polygon_wkt_snapshot=old_snapshot
    )

    session = get_db_session()

    lote = session.execute(
        select(Lote).where(
            Lote.id == claimed_job.lote_id
        )
    ).scalar_one()

    lote.polygon_wkt = (
        "POLYGON(("
        "-59.10 -28.10, "
        "-59.00 -28.10, "
        "-59.00 -28.00, "
        "-59.10 -28.00, "
        "-59.10 -28.10"
        "))"
    )

    session.commit()
    session.close()

    adapter = _FakeGeeAdapter(
        result=_normalized_result_from_claimed_job(
            claimed_job
        )
    )

    worker = SatelliteWorker(
        worker_id="worker-test",
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: claimed_job,
        gee_ndvi_adapter=adapter,
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.SUCCEEDED
    assert (
        adapter.calls[0].polygon_wkt_snapshot
        == old_snapshot
    )


def test_geometry_hash_mismatch_fails_before_calling_adapter():
    claimed_job = _create_running_claimed_job()

    tampered_job = ClaimedSatelliteJob(
        **{
            **claimed_job.__dict__,
            "geometry_hash": "f" * 64,
        }
    )

    adapter = _FakeGeeAdapter(
        result=_normalized_result_from_claimed_job(
            claimed_job
        )
    )

    worker = SatelliteWorker(
        worker_id="worker-test",
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: tampered_job,
        gee_ndvi_adapter=adapter,
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.FAILED
    assert (
        result.error_code
        == "geometry_hash_mismatch"
    )
    assert adapter.calls == []


def test_unsupported_algorithm_version_fails_before_calling_adapter():
    claimed_job = _create_running_claimed_job(
        algorithm_version="0.0.1-legacy"
    )

    adapter = _FakeGeeAdapter(
        result=_normalized_result_from_claimed_job(
            claimed_job
        )
    )

    worker = SatelliteWorker(
        worker_id="worker-test",
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: claimed_job,
        gee_ndvi_adapter=adapter,
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.FAILED
    assert (
        result.error_code
        == "unsupported_algorithm_version"
    )
    assert adapter.calls == []


def test_successful_adapter_output_persists_observations_linked_to_satellite_job():
    claimed_job = _create_running_claimed_job()

    adapter = _FakeGeeAdapter(
        result=_normalized_result_from_claimed_job(
            claimed_job
        )
    )

    worker = SatelliteWorker(
        worker_id="worker-test",
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: claimed_job,
        gee_ndvi_adapter=adapter,
    )

    result = worker.run_once()

    session = get_db_session()

    observations = session.execute(
        select(SatelliteNdviObservation).where(
            SatelliteNdviObservation.satellite_job_id
            == claimed_job.id
        )
    ).scalars().all()

    job = session.execute(
        select(SatelliteJob).where(
            SatelliteJob.id == claimed_job.id
        )
    ).scalar_one()

    session.close()

    assert result.status is WorkerRunStatus.SUCCEEDED
    assert len(observations) == 1

    assert (
        observations[0].organization_id
        == claimed_job.organization_id
    )

    assert (
        observations[0].lote_id
        == claimed_job.lote_id
    )

    assert job.status == "succeeded"
    assert job.finished_at is not None


def test_persistence_reentry_for_same_job_does_not_create_duplicate_rows():
    claimed_job = _create_running_claimed_job()

    session = get_db_session()

    result = _normalized_result_from_claimed_job(
        claimed_job
    )

    persist_ndvi_execution_result(
        session,
        organization_id=claimed_job.organization_id,
        lote_id=int(claimed_job.lote_id),
        satellite_job_id=claimed_job.id,
        result=result,
    )

    mark_satellite_job_succeeded(
        session,
        organization_id=claimed_job.organization_id,
        job_id=claimed_job.id,
        worker_id=claimed_job.locked_by,
        lease_token=claimed_job.lease_token,
    )

    session.commit()
    session.close()

    session = get_db_session()

    persist_ndvi_execution_result(
        session,
        organization_id=claimed_job.organization_id,
        lote_id=int(claimed_job.lote_id),
        satellite_job_id=claimed_job.id,
        result=result,
    )

    session.commit()

    rows = session.execute(
        select(SatelliteNdviObservation).where(
            SatelliteNdviObservation.satellite_job_id
            == claimed_job.id
        )
    ).scalars().all()

    session.close()

    assert len(rows) == 1


def test_sync_style_persistence_does_not_clear_existing_satellite_job_link():
    claimed_job = _create_running_claimed_job()

    result = _normalized_result_from_claimed_job(
        claimed_job
    )

    session = get_db_session()

    persist_ndvi_execution_result(
        session,
        organization_id=claimed_job.organization_id,
        lote_id=int(claimed_job.lote_id),
        satellite_job_id=claimed_job.id,
        result=result,
    )

    session.commit()
    session.close()

    session = get_db_session()

    persist_ndvi_execution_result(
        session,
        organization_id=claimed_job.organization_id,
        lote_id=int(claimed_job.lote_id),
        satellite_job_id=None,
        result=result,
    )

    session.commit()

    row = session.execute(
        select(SatelliteNdviObservation).where(
            SatelliteNdviObservation.organization_id
            == claimed_job.organization_id,
            SatelliteNdviObservation.lote_id
            == claimed_job.lote_id,
        )
    ).scalar_one()

    session.close()

    assert row.satellite_job_id == claimed_job.id


def test_successful_persistence_uses_one_tenant_transaction_for_result_and_status(
    monkeypatch,
):
    claimed_job = _create_running_claimed_job()

    recording_tenant_session = (
        _RecordingTenantSession()
    )

    adapter = _FakeGeeAdapter(
        result=_normalized_result_from_claimed_job(
            claimed_job
        )
    )

    calls: list[tuple[str, object]] = []

    def _record_persist(session, **kwargs):
        calls.append(
            ("persist", session)
        )

    def _record_success(session, **kwargs):
        calls.append(
            ("succeed", session)
        )

    monkeypatch.setattr(
        (
            "litoral_trace.workers.satellite_worker."
            "persist_ndvi_execution_result"
        ),
        _record_persist,
    )

    monkeypatch.setattr(
        (
            "litoral_trace.workers.satellite_worker."
            "mark_satellite_job_succeeded"
        ),
        _record_success,
    )

    worker = SatelliteWorker(
        worker_id="worker-test",
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=(
            lambda: recording_tenant_session
        ),
        claim_job_func=lambda **_: claimed_job,
        gee_ndvi_adapter=adapter,
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.SUCCEEDED

    assert calls == [
        (
            "persist",
            recording_tenant_session,
        ),
        (
            "succeed",
            recording_tenant_session,
        ),
    ]

    assert (
        recording_tenant_session.commits
        == 1
    )

    assert (
        recording_tenant_session.rollbacks
        == 0
    )

    assert (
        recording_tenant_session.closed
        is True
    )


def test_worker_failure_path_marks_job_failed_and_sanitizes_sensitive_material():
    claimed_job = _create_running_claimed_job()

    adapter = _FakeGeeAdapter(
        error=RuntimeError(
            "postgresql+psycopg://user:secret@host/db "
            "Authorization=Bearer super-secret-token"
        )
    )

    worker = SatelliteWorker(
        worker_id="worker-test",
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: claimed_job,
        gee_ndvi_adapter=adapter,
    )

    result = worker.run_once()

    session = get_db_session()

    job = session.execute(
        select(SatelliteJob).where(
            SatelliteJob.id == claimed_job.id
        )
    ).scalar_one()

    session.close()

    assert result.status is WorkerRunStatus.FAILED
    assert job.status == "failed"
    assert job.finished_at is not None

    assert (
        job.error_code
        == "worker_execution_failed"
    )

    assert (
        "postgresql+psycopg://"
        not in (job.error_message or "")
    )

    assert (
        "Bearer"
        not in (job.error_message or "")
    )


def test_worker_keeps_lease_token_internally_and_never_exposes_it_in_public_serializer():
    claimed_job = _create_running_claimed_job()

    captured_context: dict[str, str] = {}

    class _CapturingWorker(SatelliteWorker):
        def _handle_ndvi_timeseries(
            self,
            context,
        ):
            captured_context["lease_token"] = (
                context.lease_token
            )

            return _normalized_result_from_claimed_job(
                context.claimed_job
            )

    worker = _CapturingWorker(
        worker_id="worker-test",
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: claimed_job,
    )

    result = worker.run_once()

    session = get_db_session()

    job = session.execute(
        select(SatelliteJob).where(
            SatelliteJob.id == claimed_job.id
        )
    ).scalar_one()

    session.close()

    from litoral_trace.services.satellite_jobs import (
        serialize_satellite_job,
    )

    serialized = serialize_satellite_job(
        job
    )

    assert result.status is WorkerRunStatus.SUCCEEDED

    assert (
        captured_context["lease_token"]
        == str(claimed_job.lease_token)
    )

    assert "lease_token" not in serialized


def test_worker_claim_transaction_is_completed_before_adapter_executes():
    claim_session = _RecordingClaimSession()
    claimed_job = _create_running_claimed_job()

    def _assert_claim_session_closed(
        _request,
    ):
        assert claim_session.committed is True
        assert claim_session.closed is True

    adapter = _FakeGeeAdapter(
        result=_normalized_result_from_claimed_job(
            claimed_job
        ),
        on_execute=_assert_claim_session_closed,
    )

    worker = SatelliteWorker(
        worker_id="worker-test",
        claim_session_factory=lambda: claim_session,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: claimed_job,
        gee_ndvi_adapter=adapter,
    )

    result = worker.run_once()

    assert result.status is WorkerRunStatus.SUCCEEDED


def test_graceful_stop_prevents_claiming_new_work():
    called = {
        "claim": 0
    }

    worker = SatelliteWorker(
        worker_id="worker-test",
        claim_session_factory=_RecordingClaimSession,
        tenant_session_factory=get_db_session,
        claim_job_func=lambda **_: called.__setitem__(
            "claim",
            called["claim"] + 1,
        ),
    )

    worker.request_shutdown()

    result = worker.run_once()

    assert result.status is WorkerRunStatus.STOPPED
    assert called["claim"] == 0


def test_worker_database_url_absent_fails_closed_without_migration_fallback(
    monkeypatch,
):
    monkeypatch.delenv(
        "WORKER_DATABASE_URL",
        raising=False,
    )

    monkeypatch.setenv(
        "MIGRATION_DATABASE_URL",
        (
            "postgresql://"
            "migration-owner:secret@host:5432/appdb"
        ),
    )

    monkeypatch.setenv(
        "ENVIRONMENT",
        "development",
    )

    with pytest.raises(
        RuntimeError,
        match="WORKER_DATABASE_URL es obligatorio",
    ):
        get_worker_database_url()


def test_worker_database_url_rejects_sqlite(
    monkeypatch,
):
    monkeypatch.setenv(
        "WORKER_DATABASE_URL",
        "sqlite:///worker.db",
    )

    monkeypatch.setenv(
        "ENVIRONMENT",
        "development",
    )

    with pytest.raises(
        RuntimeError,
        match="debe ser una URL PostgreSQL",
    ):
        get_worker_database_url()


def test_sanitize_worker_error_message_redacts_known_sensitive_patterns():
    sanitized = sanitize_worker_error_message(
        (
            "postgresql+psycopg://user:secret@host/db "
            '"private_key":"abc" '
            "Bearer token"
        )
    )

    assert "secret@host" not in sanitized
    assert "private_key" not in sanitized
    assert "Bearer" not in sanitized
