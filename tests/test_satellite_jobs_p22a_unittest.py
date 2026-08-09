from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.exc import IntegrityError

from litoral_trace.db.models import Lote, Organization, SatelliteJob
from litoral_trace.db.engine import get_db_session
from litoral_trace.services.satellite_jobs import (
    SatelliteJobStatus,
    claim_next_satellite_job,
    enqueue_satellite_ndvi_job,
    get_satellite_job,
    serialize_satellite_job,
    update_satellite_job_status,
)


@pytest.fixture(autouse=True)
def cleanup_satellite_jobs():
    session = get_db_session()
    session.execute(delete(SatelliteJob))
    session.commit()
    session.close()

    yield

    session = get_db_session()
    session.execute(delete(SatelliteJob))
    session.commit()
    session.close()


def _create_tenant_lote(*, slug_prefix: str) -> tuple[int, int]:
    suffix = uuid4().hex[:8]
    session = get_db_session()
    organization = Organization(
        name=f"Satellite Jobs Tenant {suffix}",
        slug=f"{slug_prefix}-{suffix}",
        tax_id=f"50-{suffix}",
        tier="pro",
        is_active=True,
    )
    session.add(organization)
    session.flush()

    lote = Lote(
        organization_id=organization.id,
        identificador=f"SAT-JOB-{suffix}",
        productor_id=f"20-{suffix}",
        producto_forestal="Madera Aserrada (Pino)",
        hectareas=10.0,
        latitud=-27.45,
        longitud=-58.90,
        polygon_wkt=(
            "POLYGON(("
            "-58.91 -27.46, -58.89 -27.46, "
            "-58.89 -27.44, -58.91 -27.44, "
            "-58.91 -27.46"
            "))"
        ),
        estatus="Pendiente",
        volumen_ingresado_ton=20.0,
        volumen_exportar_ton=5.0,
    )
    session.add(lote)
    session.commit()
    organization_id = organization.id
    lote_id = lote.id
    session.close()
    return organization_id, lote_id


class _FakeMappingsResult:
    def __init__(self, row):
        self._row = row

    def one_or_none(self):
        return self._row


class _FakeExecuteResult:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return _FakeMappingsResult(self._row)


class _FakeDialect:
    def __init__(self, name: str):
        self.name = name


class _FakeBind:
    def __init__(self, dialect_name: str):
        self.dialect = _FakeDialect(dialect_name)


class _RecordingClaimSession:
    def __init__(self, row=None, *, dialect_name: str = "postgresql"):
        self._row = row
        self._bind = _FakeBind(dialect_name)
        self.executed: list[tuple[str, dict[str, str] | None]] = []
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def get_bind(self):
        return self._bind

    def execute(self, statement, params=None):
        self.executed.append((str(statement), params))
        row = self._row
        if callable(row):
            row = row(params)
        return _FakeExecuteResult(row)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def _build_claimed_row(
    *,
    lease_token: UUID | str,
    worker_id: str = "worker-1",
    attempt_count: int = 1,
) -> dict[str, object]:
    now = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)
    return {
        "id": 55,
        "organization_id": 1,
        "lote_id": 101,
        "job_type": "ndvi_timeseries",
        "status": "running",
        "attempt_count": attempt_count,
        "max_attempts": 3,
        "next_attempt_at": now,
        "locked_by": worker_id,
        "locked_at": now,
        "heartbeat_at": now,
        "lease_token": str(lease_token),
        "started_at": now,
        "request_start_date": date(2020, 12, 31),
        "request_end_date": date(2026, 8, 9),
        "max_cloud_pct": 20.0,
        "geometry_hash": "a" * 64,
        "algorithm_version": "algo-v1",
        "polygon_wkt_snapshot": "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))",
    }


def test_enqueue_satellite_job_persists_valid_ndvi_job():
    job, created = enqueue_satellite_ndvi_job(
        organization_id=1,
        lote_id=101,
        start_date="2020-12-31",
        end_date="2026-08-09",
        max_cloud_pct=20.0,
        idempotency_key=f"ndvi-job-{uuid4().hex}",
    )

    serialized = serialize_satellite_job(job)

    assert created is True
    assert job.status == SatelliteJobStatus.QUEUED.value
    assert job.job_type == "ndvi_timeseries"
    assert job.organization_id == 1
    assert job.lote_id == 101
    assert job.attempt_count == 0
    assert serialized["geometry_hash"]
    assert serialized["request_start_date"] == "2020-12-31"
    assert serialized["request_end_date"] == "2026-08-09"
    assert "polygon_wkt_snapshot" not in serialized
    assert "lease_token" not in serialized
    assert "refresh_token" not in serialized
    assert "authorization" not in serialized


def test_enqueue_satellite_job_reuses_matching_idempotency_key():
    idempotency_key = f"dedupe-{uuid4().hex}"

    first_job, first_created = enqueue_satellite_ndvi_job(
        organization_id=1,
        lote_id=101,
        start_date="2020-12-31",
        end_date="2026-08-09",
        idempotency_key=idempotency_key,
    )
    second_job, second_created = enqueue_satellite_ndvi_job(
        organization_id=1,
        lote_id=101,
        start_date="2020-12-31",
        end_date="2026-08-09",
        idempotency_key=idempotency_key,
    )

    assert first_created is True
    assert second_created is False
    assert second_job.id == first_job.id


def test_enqueue_satellite_job_rejects_idempotency_key_payload_drift():
    idempotency_key = f"dedupe-mismatch-{uuid4().hex}"

    enqueue_satellite_ndvi_job(
        organization_id=1,
        lote_id=101,
        start_date="2020-12-31",
        end_date="2026-08-09",
        idempotency_key=idempotency_key,
    )

    with pytest.raises(ValueError):
        enqueue_satellite_ndvi_job(
            organization_id=1,
            lote_id=101,
            start_date="2021-01-01",
            end_date="2026-08-09",
            idempotency_key=idempotency_key,
        )


def test_enqueue_satellite_job_rejects_invalid_retry_values():
    with pytest.raises(ValueError):
        enqueue_satellite_ndvi_job(
            organization_id=1,
            lote_id=101,
            start_date="2020-12-31",
            end_date="2026-08-09",
            max_attempts=0,
        )

    with pytest.raises(ValueError):
        enqueue_satellite_ndvi_job(
            organization_id=1,
            lote_id=101,
            start_date="2020-12-31",
            end_date="2026-08-09",
            max_cloud_pct=101.0,
        )


def test_update_satellite_job_status_supports_running_and_succeeded():
    job, _ = enqueue_satellite_ndvi_job(
        organization_id=1,
        lote_id=101,
        start_date="2020-12-31",
        end_date="2026-08-09",
    )

    running = update_satellite_job_status(
        organization_id=1,
        job_id=job.id,
        status=SatelliteJobStatus.RUNNING,
        locked_by="worker-1",
    )
    succeeded = update_satellite_job_status(
        organization_id=1,
        job_id=job.id,
        status=SatelliteJobStatus.SUCCEEDED,
    )

    assert running.status == SatelliteJobStatus.RUNNING.value
    assert running.attempt_count == 1
    assert running.locked_by == "worker-1"
    assert running.started_at is not None
    assert succeeded.status == SatelliteJobStatus.SUCCEEDED.value
    assert succeeded.finished_at is not None
    assert succeeded.locked_by is None


def test_update_satellite_job_status_rejects_invalid_status():
    job, _ = enqueue_satellite_ndvi_job(
        organization_id=1,
        lote_id=101,
        start_date="2020-12-31",
        end_date="2026-08-09",
    )

    with pytest.raises(ValueError):
        update_satellite_job_status(
            organization_id=1,
            job_id=job.id,
            status="cancelled",
        )


def test_satellite_job_lookup_and_enqueue_respect_tenant_ownership():
    organization_b_id, lote_b_id = _create_tenant_lote(slug_prefix="sat-job-org-b")

    job, _ = enqueue_satellite_ndvi_job(
        organization_id=1,
        lote_id=101,
        start_date="2020-12-31",
        end_date="2026-08-09",
    )

    assert get_satellite_job(organization_id=1, job_id=job.id) is not None
    assert get_satellite_job(organization_id=organization_b_id, job_id=job.id) is None

    with pytest.raises(ValueError):
        enqueue_satellite_ndvi_job(
            organization_id=1,
            lote_id=lote_b_id,
            start_date="2020-12-31",
            end_date="2026-08-09",
        )


def test_satellite_job_model_rejects_invalid_attempt_constraint():
    session = get_db_session()
    invalid_job = SatelliteJob(
        organization_id=1,
        lote_id=101,
        job_type="ndvi_timeseries",
        status="queued",
        attempt_count=4,
        max_attempts=3,
        request_start_date=date(2020, 12, 31),
        request_end_date=date(2026, 8, 9),
        max_cloud_pct=20.0,
        geometry_hash="a" * 64,
        algorithm_version="test-v1",
        polygon_wkt_snapshot="POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, -58.91 -27.44, -58.91 -27.46))",
    )
    session.add(invalid_job)

    with pytest.raises(IntegrityError):
        session.commit()

    session.rollback()
    session.close()


def test_claim_next_satellite_job_validates_worker_id():
    session = _RecordingClaimSession(row=None)

    with pytest.raises(ValueError):
        claim_next_satellite_job(worker_id="   ", db_session=session)

    with pytest.raises(ValueError):
        claim_next_satellite_job(worker_id="w" * 256, db_session=session)


def test_claim_next_satellite_job_rejects_non_postgres_without_fallback():
    session = _RecordingClaimSession(row=None, dialect_name="sqlite")

    with pytest.raises(RuntimeError, match="requires PostgreSQL"):
        claim_next_satellite_job(worker_id="worker-1", db_session=session)

    assert session.executed == []


def test_claim_next_satellite_job_returns_none_for_empty_queue_using_single_sql_entrypoint():
    session = _RecordingClaimSession(row=None)

    claimed_job = claim_next_satellite_job(worker_id="worker-1", db_session=session)

    assert claimed_job is None
    assert len(session.executed) == 1
    sql_text, params = session.executed[0]
    assert "public.worker_claim_next_satellite_job" in sql_text
    assert "UPDATE public.satellite_jobs" not in sql_text
    assert params is not None
    assert params["requested_worker_id"] == "worker-1"
    assert "requested_lease_token" not in params
    assert session.committed is False
    assert session.rolled_back is False


def test_claim_next_satellite_job_receives_database_generated_lease_token():
    generated_lease = uuid4()
    session = _RecordingClaimSession(
        row=lambda params: _build_claimed_row(
            lease_token=generated_lease,
            worker_id=params["requested_worker_id"],
        )
    )

    claimed_job = claim_next_satellite_job(worker_id="worker-1", db_session=session)

    assert isinstance(claimed_job.lease_token, UUID)
    assert claimed_job.lease_token == generated_lease
    assert len(session.executed) == 1
    _, params = session.executed[0]
    assert params is not None
    assert params == {"requested_worker_id": "worker-1"}


def test_claim_next_satellite_job_maps_returned_row_without_accepting_caller_lease_token():
    generated_lease = uuid4()
    session = _RecordingClaimSession(
        row=_build_claimed_row(
            lease_token=generated_lease,
            worker_id="worker-db",
            attempt_count=2,
        )
    )

    claimed_job = claim_next_satellite_job(
        worker_id="worker-db",
        db_session=session,
    )

    assert claimed_job.id == 55
    assert claimed_job.organization_id == 1
    assert claimed_job.status == "running"
    assert claimed_job.attempt_count == 2
    assert claimed_job.locked_by == "worker-db"
    assert claimed_job.lease_token == generated_lease
    assert claimed_job.polygon_wkt_snapshot is not None
    _, params = session.executed[0]
    assert params == {"requested_worker_id": "worker-db"}


def test_claim_next_satellite_job_does_not_accept_caller_supplied_lease_token_argument():
    session = _RecordingClaimSession(row=None)

    with pytest.raises(TypeError):
        claim_next_satellite_job(  # type: ignore[call-arg]
            worker_id="worker-1",
            lease_token=str(uuid4()),
            db_session=session,
        )
