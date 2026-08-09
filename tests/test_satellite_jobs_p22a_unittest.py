from __future__ import annotations

from datetime import date
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.exc import IntegrityError

from litoral_trace.db.models import Lote, Organization, SatelliteJob
from litoral_trace.db.engine import get_db_session
from litoral_trace.services.satellite_jobs import (
    SatelliteJobStatus,
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
