from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker

from litoral_trace.config.settings import normalize_database_url
from litoral_trace.services.gee import ALGORITHM_VERSION, generate_geometry_hash
from litoral_trace.services.satellite_ndvi_processing import (
    NdviObservationRecord,
    NormalizedNdviExecutionResult,
    SatelliteJobLeaseLostError,
)
from litoral_trace.workers.satellite_worker import (
    SatelliteWorker,
    WorkerExecutionContext,
)


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


POSTGRES_TESTS_ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_DATABASE_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
OWNER_DATABASE_URL = os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")
RUNTIME_ROLE = "litoral_trace_app"
WORKER_ROLE = "litoral_trace_worker_executor"

pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_TESTS_ENABLED
        and RUNTIME_DATABASE_URL
        and OWNER_DATABASE_URL
    ),
    reason=(
        "PostgreSQL P2.2F1 tests require ENABLE_POSTGRES_TESTS=1 plus "
        "isolated runtime and migration test URLs."
    ),
)


def _runtime_engine():
    return create_engine(
        normalize_database_url(RUNTIME_DATABASE_URL),
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _owner_engine():
    return create_engine(
        normalize_database_url(OWNER_DATABASE_URL),
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _set_tenant_context(connection, organization_id: int) -> None:
    connection.execute(
        text(
            "SELECT set_config("
            "'app.current_organization_id', :organization_id, true)"
        ),
        {"organization_id": str(organization_id)},
    )


def _polygon() -> str:
    return (
        "POLYGON((-58.91 -27.46, -58.89 -27.46, -58.89 -27.44, "
        "-58.91 -27.44, -58.91 -27.46))"
    )


@contextmanager
def _fixture():
    suffix = uuid4().hex[:10]
    owner_engine = _owner_engine()
    runtime_engine = _runtime_engine()
    fixture: dict[str, object] = {
        "owner_engine": owner_engine,
        "runtime_engine": runtime_engine,
    }

    with owner_engine.begin() as conn:
        for label in ("a", "b"):
            organization_id = conn.execute(
                text(
                    """
                    INSERT INTO organizations (
                        name, slug, tax_id, tier, description, is_active
                    )
                    VALUES (
                        :name, :slug, :tax_id, 'pro', 'P2.2F1 audit test', true
                    )
                    RETURNING id
                    """
                ),
                {
                    "name": f"P22F1 Org {label.upper()} {suffix}",
                    "slug": f"p22f1-org-{label}-{suffix}",
                    "tax_id": f"F1-{label}-{suffix}",
                },
            ).scalar_one()
            lote_id = conn.execute(
                text(
                    """
                    INSERT INTO lotes (
                        organization_id, identificador, productor_id,
                        producto_forestal, hectareas, latitud, longitud,
                        polygon_wkt, estatus, volumen_ingresado_ton,
                        volumen_exportar_ton
                    )
                    VALUES (
                        :organization_id, :identificador, :productor_id,
                        'Madera Aserrada (Pino)', 10.0, -27.45, -58.90,
                        :polygon_wkt, 'Pendiente', 20.0, 5.0
                    )
                    RETURNING id
                    """
                ),
                {
                    "organization_id": organization_id,
                    "identificador": f"P22F1-{label}-{suffix}",
                    "productor_id": f"F1-{label}-{suffix}",
                    "polygon_wkt": _polygon(),
                },
            ).scalar_one()
            fixture[f"org_{label}_id"] = int(organization_id)
            fixture[f"lote_{label}_id"] = int(lote_id)

    try:
        yield fixture
    finally:
        with owner_engine.begin() as conn:
            organization_ids = [fixture["org_a_id"], fixture["org_b_id"]]
            conn.execute(
                text("DELETE FROM audit_logs WHERE organization_id = ANY(:ids)"),
                {"ids": organization_ids},
            )
            conn.execute(
                text(
                    "DELETE FROM satellite_ndvi_observations "
                    "WHERE organization_id = ANY(:ids)"
                ),
                {"ids": organization_ids},
            )
            conn.execute(
                text(
                    "DELETE FROM satellite_job_results "
                    "WHERE organization_id = ANY(:ids)"
                ),
                {"ids": organization_ids},
            )
            conn.execute(
                text("DELETE FROM satellite_jobs WHERE organization_id = ANY(:ids)"),
                {"ids": organization_ids},
            )
            conn.execute(
                text("DELETE FROM lotes WHERE organization_id = ANY(:ids)"),
                {"ids": organization_ids},
            )
            conn.execute(
                text("DELETE FROM organizations WHERE id = ANY(:ids)"),
                {"ids": organization_ids},
            )
        runtime_engine.dispose()
        owner_engine.dispose()


def _insert_running_job(fixture, *, label: str = "a") -> WorkerExecutionContext:
    organization_id = int(fixture[f"org_{label}_id"])
    lote_id = int(fixture[f"lote_{label}_id"])
    worker_id = f"p22f1-worker-{uuid4().hex[:8]}"
    lease_token = str(uuid4())
    polygon = _polygon()
    geometry_hash = generate_geometry_hash(polygon)

    with fixture["owner_engine"].begin() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO satellite_jobs (
                    organization_id, lote_id, job_type, status,
                    attempt_count, max_attempts, next_attempt_at,
                    locked_at, locked_by, heartbeat_at, lease_token, started_at,
                    request_start_date, request_end_date, max_cloud_pct,
                    geometry_hash, algorithm_version, polygon_wkt_snapshot
                )
                VALUES (
                    :organization_id, :lote_id, 'ndvi_timeseries', 'running',
                    1, 3, :next_attempt_at,
                    :now, :worker_id, :now, :lease_token, :now,
                    '2026-07-01', '2026-08-01', 20.0,
                    :geometry_hash, :algorithm_version, :polygon
                )
                RETURNING id
                """
            ),
            {
                "organization_id": organization_id,
                "lote_id": lote_id,
                "next_attempt_at": datetime.now(timezone.utc) - timedelta(minutes=1),
                "now": datetime.now(timezone.utc),
                "worker_id": worker_id,
                "lease_token": lease_token,
                "geometry_hash": geometry_hash,
                "algorithm_version": ALGORITHM_VERSION,
                "polygon": polygon,
            },
        ).mappings().one()

    claimed = SimpleNamespace(
        id=int(row["id"]),
        organization_id=organization_id,
        lote_id=lote_id,
        job_type="ndvi_timeseries",
        attempt_count=1,
        max_attempts=3,
        lease_token=lease_token,
    )
    return WorkerExecutionContext(
        job_id=claimed.id,
        organization_id=organization_id,
        job_type=claimed.job_type,
        worker_id=worker_id,
        lease_token=lease_token,
        claimed_job=claimed,
    )


def _result(context: WorkerExecutionContext) -> NormalizedNdviExecutionResult:
    return NormalizedNdviExecutionResult(
        geometry_hash=generate_geometry_hash(_polygon()),
        algorithm_version=ALGORITHM_VERSION,
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
                geometry_hash=generate_geometry_hash(_polygon()),
                algorithm_version=ALGORITHM_VERSION,
                aoi_cloud_percentage=1.0,
                processing_date=datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
            ),
        ),
    )


def _worker(runtime_engine) -> SatelliteWorker:
    session_factory = sessionmaker(
        bind=runtime_engine,
        autoflush=False,
        autocommit=False,
    )
    return SatelliteWorker(
        worker_id="p22f1-worker-runtime",
        tenant_session_factory=session_factory,
        stale_recovery_interval_seconds=None,
    )


def _job_state(owner_engine, job_id: int) -> dict[str, object]:
    with owner_engine.connect() as conn:
        return dict(
            conn.execute(
                text(
                    "SELECT status, error_code, error_message "
                    "FROM satellite_jobs WHERE id = :job_id"
                ),
                {"job_id": job_id},
            ).mappings().one()
        )


def _terminal_audits(owner_engine, job_id: int) -> list[dict[str, object]]:
    with owner_engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT organization_id, username, action, entity_type,
                       entity_id, after_data, detail
                FROM audit_logs
                WHERE entity_type = 'satellite_job'
                  AND entity_id = :job_id
                  AND action IN ('satellite.job.succeeded', 'satellite.job.failed')
                ORDER BY id
                """
            ),
            {"job_id": job_id},
        ).mappings().all()
    return [dict(row) for row in rows]


@contextmanager
def _reject_terminal_audit(owner_engine, *, job_id: int, action: str):
    function_name = f"p22f1_reject_audit_{uuid4().hex[:12]}"
    trigger_name = f"p22f1_reject_audit_{uuid4().hex[:12]}"
    with owner_engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE FUNCTION public.{function_name}()
                RETURNS trigger
                LANGUAGE plpgsql
                SET search_path = public, pg_temp
                AS $$
                BEGIN
                    RAISE EXCEPTION 'mandatory audit insert blocked';
                END;
                $$
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TRIGGER {trigger_name}
                BEFORE INSERT ON public.audit_logs
                FOR EACH ROW
                WHEN (NEW.action = '{action}' AND NEW.entity_id = {int(job_id)})
                EXECUTE FUNCTION public.{function_name}()
                """
            )
        )
    try:
        yield
    finally:
        with owner_engine.begin() as conn:
            conn.execute(
                text(f"DROP TRIGGER IF EXISTS {trigger_name} ON public.audit_logs")
            )
            conn.execute(
                text(f"DROP FUNCTION IF EXISTS public.{function_name}()")
            )


def test_migration_head_privileges_rls_and_maintenance_authority():
    with _fixture() as fixture:
        with fixture["owner_engine"].connect() as conn:
            revision = conn.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            runtime = conn.execute(
                text(
                    """
                    SELECT
                      has_table_privilege(:role, 'public.audit_logs', 'SELECT') can_select,
                      has_table_privilege(:role, 'public.audit_logs', 'INSERT') can_insert,
                      has_table_privilege(:role, 'public.audit_logs', 'UPDATE') can_update,
                      has_table_privilege(:role, 'public.audit_logs', 'DELETE') can_delete,
                      has_table_privilege(:role, 'public.audit_logs', 'TRUNCATE') can_truncate,
                      has_table_privilege(:role, 'public.audit_logs', 'REFERENCES') can_references,
                      has_table_privilege(:role, 'public.audit_logs', 'TRIGGER') can_trigger,
                      has_sequence_privilege(:role, 'public.audit_logs_id_seq', 'USAGE') sequence_usage,
                      has_sequence_privilege(:role, 'public.audit_logs_id_seq', 'SELECT') sequence_select
                    """
                ),
                {"role": RUNTIME_ROLE},
            ).mappings().one()
            worker = conn.execute(
                text(
                    """
                    SELECT
                      has_table_privilege(:role, 'public.audit_logs', 'SELECT') can_select,
                      has_table_privilege(:role, 'public.audit_logs', 'INSERT') can_insert,
                      has_table_privilege(:role, 'public.audit_logs', 'UPDATE') can_update,
                      has_table_privilege(:role, 'public.audit_logs', 'DELETE') can_delete
                    """
                ),
                {"role": WORKER_ROLE},
            ).mappings().one()
            owner = conn.execute(
                text(
                    """
                    SELECT
                      has_table_privilege(current_user, 'public.audit_logs', 'SELECT') can_select,
                      has_table_privilege(current_user, 'public.audit_logs', 'INSERT') can_insert,
                      has_table_privilege(current_user, 'public.audit_logs', 'UPDATE') can_update,
                      has_table_privilege(current_user, 'public.audit_logs', 'DELETE') can_delete
                    """
                )
            ).mappings().one()
            rls = conn.execute(
                text(
                    "SELECT relrowsecurity, relforcerowsecurity "
                    "FROM pg_class WHERE oid = 'public.audit_logs'::regclass"
                )
            ).mappings().one()

    assert revision == "015_add_satellite_queue_metrics"
    assert dict(runtime) == {
        "can_select": True,
        "can_insert": True,
        "can_update": False,
        "can_delete": False,
        "can_truncate": False,
        "can_references": False,
        "can_trigger": False,
        "sequence_usage": True,
        "sequence_select": True,
    }
    assert not any(worker.values())
    assert all(owner.values())
    assert dict(rls) == {"relrowsecurity": True, "relforcerowsecurity": True}


def test_runtime_audit_insert_and_select_remain_tenant_isolated():
    with _fixture() as fixture:
        audit_ids = {}
        for label in ("a", "b"):
            with fixture["runtime_engine"].begin() as conn:
                _set_tenant_context(conn, int(fixture[f"org_{label}_id"]))
                audit_ids[label] = conn.execute(
                    text(
                        """
                        INSERT INTO audit_logs (
                            organization_id, username, action, entity_type, after_data
                        )
                        VALUES (
                            :organization_id, 'satellite-worker',
                            'satellite.job.failed', 'satellite_job',
                            '{"outcome":"failure"}'::json
                        )
                        RETURNING id
                        """
                    ),
                    {"organization_id": fixture[f"org_{label}_id"]},
                ).scalar_one()

        with fixture["runtime_engine"].begin() as conn:
            _set_tenant_context(conn, int(fixture["org_a_id"]))
            visible = conn.execute(
                text("SELECT id FROM audit_logs WHERE id = ANY(:ids)"),
                {"ids": list(audit_ids.values())},
            ).scalars().all()
        assert visible == [audit_ids["a"]]

        with pytest.raises(DBAPIError):
            with fixture["runtime_engine"].begin() as conn:
                _set_tenant_context(conn, int(fixture["org_a_id"]))
                conn.execute(
                    text(
                        """
                        INSERT INTO audit_logs (
                            organization_id, username, action, entity_type
                        )
                        VALUES (
                            :organization_id, 'satellite-worker',
                            'satellite.job.failed', 'satellite_job'
                        )
                        """
                    ),
                    {"organization_id": fixture["org_b_id"]},
                )


def test_success_lifecycle_commits_result_observations_status_and_safe_audit():
    with _fixture() as fixture:
        context = _insert_running_job(fixture)
        result = _result(context)

        _worker(fixture["runtime_engine"])._persist_success(context, result)

        with fixture["owner_engine"].connect() as conn:
            result_count = conn.execute(
                text(
                    "SELECT count(*) FROM satellite_job_results "
                    "WHERE satellite_job_id = :job_id"
                ),
                {"job_id": context.job_id},
            ).scalar_one()
            observation_count = conn.execute(
                text(
                    "SELECT count(*) FROM satellite_ndvi_observations "
                    "WHERE satellite_job_id = :job_id"
                ),
                {"job_id": context.job_id},
            ).scalar_one()
        audits = _terminal_audits(fixture["owner_engine"], context.job_id)

        assert _job_state(fixture["owner_engine"], context.job_id)["status"] == "succeeded"
        assert int(result_count) == 1
        assert int(observation_count) == 1
        assert len(audits) == 1
        audit = audits[0]
        assert audit["organization_id"] == context.organization_id
        assert audit["username"] == "satellite-worker"
        assert audit["action"] == "satellite.job.succeeded"
        assert audit["detail"] == "Satellite job completed successfully."
        assert audit["after_data"]["actor_role"] == "system_worker"
        assert audit["after_data"]["metadata"] == {
            "job_type": "ndvi_timeseries",
            "attempt_count": 1,
            "max_attempts": 3,
            "observation_count": 1,
        }


def test_failure_lifecycle_commits_status_and_safe_terminal_audit():
    with _fixture() as fixture:
        context = _insert_running_job(fixture)

        _worker(fixture["runtime_engine"])._persist_failure(
            context,
            error_code="provider_timeout",
            error_message="safe operational failure",
        )

        state = _job_state(fixture["owner_engine"], context.job_id)
        audits = _terminal_audits(fixture["owner_engine"], context.job_id)
        assert state["status"] == "failed"
        assert len(audits) == 1
        audit = audits[0]
        assert audit["action"] == "satellite.job.failed"
        assert audit["detail"] == "Satellite job failed."
        assert audit["after_data"]["metadata"] == {
            "job_type": "ndvi_timeseries",
            "attempt_count": 1,
            "max_attempts": 3,
            "error_code": "provider_timeout",
        }
        assert "error_message" not in str(audit["after_data"])
        assert "safe operational failure" not in str(audit)


def test_audit_insert_failure_rolls_back_entire_success_transaction():
    with _fixture() as fixture:
        context = _insert_running_job(fixture)
        with _reject_terminal_audit(
            fixture["owner_engine"],
            job_id=context.job_id,
            action="satellite.job.succeeded",
        ):
            with pytest.raises(DBAPIError):
                _worker(fixture["runtime_engine"])._persist_success(
                    context,
                    _result(context),
                )

        with fixture["owner_engine"].connect() as conn:
            result_count = conn.execute(
                text(
                    "SELECT count(*) FROM satellite_job_results "
                    "WHERE satellite_job_id = :job_id"
                ),
                {"job_id": context.job_id},
            ).scalar_one()
            observation_count = conn.execute(
                text(
                    "SELECT count(*) FROM satellite_ndvi_observations "
                    "WHERE satellite_job_id = :job_id"
                ),
                {"job_id": context.job_id},
            ).scalar_one()
        assert _job_state(fixture["owner_engine"], context.job_id)["status"] == "running"
        assert int(result_count) == 0
        assert int(observation_count) == 0
        assert _terminal_audits(fixture["owner_engine"], context.job_id) == []


def test_audit_insert_failure_rolls_back_failed_transition():
    with _fixture() as fixture:
        context = _insert_running_job(fixture)
        with _reject_terminal_audit(
            fixture["owner_engine"],
            job_id=context.job_id,
            action="satellite.job.failed",
        ):
            with pytest.raises(DBAPIError):
                _worker(fixture["runtime_engine"])._persist_failure(
                    context,
                    error_code="provider_timeout",
                    error_message="safe operational failure",
                )

        state = _job_state(fixture["owner_engine"], context.job_id)
        assert state["status"] == "running"
        assert state["error_code"] is None
        assert state["error_message"] is None
        assert _terminal_audits(fixture["owner_engine"], context.job_id) == []


def test_stale_execution_cannot_create_duplicate_terminal_audit():
    with _fixture() as fixture:
        context = _insert_running_job(fixture)
        worker = _worker(fixture["runtime_engine"])
        worker._persist_success(context, _result(context))

        with pytest.raises(SatelliteJobLeaseLostError):
            worker._persist_failure(
                context,
                error_code="stale_worker_failure",
                error_message="stale execution",
            )

        audits = _terminal_audits(fixture["owner_engine"], context.job_id)
        assert len(audits) == 1
        assert audits[0]["action"] == "satellite.job.succeeded"
