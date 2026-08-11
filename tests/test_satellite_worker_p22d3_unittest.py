from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

import litoral_trace.services.satellite_jobs as satellite_jobs_module
import litoral_trace.workers.satellite_worker as satellite_worker_module
from litoral_trace.services.satellite_jobs import (
    StaleRecoveryResult,
    _normalize_requested_batch_size,
    recover_stale_satellite_jobs,
)
from litoral_trace.workers.satellite_worker import (
    SatelliteWorker,
    WorkerRunStatus,
    resolve_satellite_worker_stale_recovery_interval_seconds,
)


class _MappingResult:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def one(self):
        return self._row


class _RecordingWorkerSession:
    def __init__(
        self,
        *,
        row=None,
        dialect_name: str = "postgresql",
        execute_error: Exception | None = None,
    ):
        self.row = row
        self.execute_error = execute_error
        self.executed: list[tuple[str, dict | None]] = []
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self._bind = SimpleNamespace(
            dialect=SimpleNamespace(name=dialect_name)
        )

    def get_bind(self):
        return self._bind

    def execute(self, statement, params=None):
        sql_text = str(statement)
        self.executed.append((sql_text, params))
        if self.execute_error is not None:
            raise self.execute_error

        resolved_row = self.row(params) if callable(self.row) else self.row
        if resolved_row is None:
            resolved_row = {"requeued_count": 0, "failed_count": 0}
        return _MappingResult(resolved_row)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class _RecordingClaimSession:
    def __init__(self):
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def _migration_012_content() -> str:
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "012_add_satellite_job_stale_recovery.py"
    )
    assert migration_path.exists()
    return migration_path.read_text(encoding="utf-8")


def _normalized_migration_012_content() -> str:
    return " ".join(_migration_012_content().split())


def test_normalize_requested_batch_size_bounds_values():
    assert _normalize_requested_batch_size(None) == 10
    assert _normalize_requested_batch_size(-5) == 1
    assert _normalize_requested_batch_size(0) == 1
    assert _normalize_requested_batch_size(7) == 7
    assert _normalize_requested_batch_size(999) == 100


def test_recover_stale_satellite_jobs_executes_single_sql_entrypoint_and_hides_internal_fields():
    session = _RecordingWorkerSession(
        row={"requeued_count": 2, "failed_count": 1}
    )

    result = recover_stale_satellite_jobs(
        requested_batch_size=999,
        db_session=session,
    )

    assert result == StaleRecoveryResult(
        requeued_count=2,
        failed_count=1,
    )
    assert not hasattr(result, "lease_token")
    assert len(session.executed) == 1
    sql_text, params = session.executed[0]
    assert "public.worker_recover_stale_satellite_jobs" in sql_text
    assert params == {"requested_batch_size": 100}
    assert session.committed is False
    assert session.rolled_back is False


def test_recover_stale_satellite_jobs_rejects_non_postgres_without_fallback():
    session = _RecordingWorkerSession(dialect_name="sqlite")

    with pytest.raises(RuntimeError, match="requires PostgreSQL"):
        recover_stale_satellite_jobs(
            requested_batch_size=10,
            db_session=session,
        )

    assert session.executed == []


def test_stale_recovery_throttle_uses_monotonic_and_does_not_run_every_poll():
    recovery_calls: list[int] = []
    claim_calls: list[str] = []
    monotonic_values = iter((0.0, 0.0, 0.0, 10.0, 35.0, 35.0, 35.0))

    def _recover(**kwargs):
        recovery_calls.append(kwargs["requested_batch_size"])
        return StaleRecoveryResult(
            requeued_count=0,
            failed_count=0,
        )

    worker = SatelliteWorker(
        worker_id="worker-test",
        stale_recovery_interval_seconds=30,
        claim_session_factory=_RecordingClaimSession,
        claim_job_func=lambda **_: claim_calls.append("claim") or None,
        recover_stale_jobs_func=_recover,
        monotonic_func=lambda: next(monotonic_values),
    )

    assert worker.run_once().status is WorkerRunStatus.IDLE
    assert worker.run_once().status is WorkerRunStatus.IDLE
    assert worker.run_once().status is WorkerRunStatus.IDLE

    assert recovery_calls == [10, 10]
    assert claim_calls == ["claim", "claim", "claim"]


def test_generic_recovery_error_is_sanitized_and_claim_still_runs(caplog):
    claim_calls: list[str] = []

    def _recover(**_kwargs):
        raise RuntimeError(
            "postgresql+psycopg://user:secret@host/db "
            "Bearer very-secret-token"
        )

    worker = SatelliteWorker(
        worker_id="worker-test",
        stale_recovery_interval_seconds=30,
        claim_session_factory=_RecordingClaimSession,
        claim_job_func=lambda **_: claim_calls.append("claim") or None,
        recover_stale_jobs_func=_recover,
        monotonic_func=lambda: 0.0,
    )

    with caplog.at_level(
        logging.WARNING,
        logger=satellite_worker_module.__name__,
    ):
        result = worker.run_once()

    assert result.status is WorkerRunStatus.IDLE
    assert claim_calls == ["claim"]

    recovery_error_records = [
        record
        for record in caplog.records
        if record.getMessage() == "satellite_worker_stale_recovery_error"
    ]
    assert len(recovery_error_records) == 1
    assert "secret@host" not in recovery_error_records[0].error_message
    assert "Bearer" not in recovery_error_records[0].error_message


def test_request_shutdown_before_run_once_does_not_attempt_recovery_or_claim():
    recovery_calls: list[str] = []
    claim_calls: list[str] = []

    worker = SatelliteWorker(
        worker_id="worker-test",
        stale_recovery_interval_seconds=30,
        claim_session_factory=_RecordingClaimSession,
        claim_job_func=lambda **_: claim_calls.append("claim") or None,
        recover_stale_jobs_func=lambda **_: recovery_calls.append("recover") or StaleRecoveryResult(
            requeued_count=0,
            failed_count=0,
        ),
    )
    worker.request_shutdown()

    result = worker.run_once()

    assert result.status is WorkerRunStatus.STOPPED
    assert recovery_calls == []
    assert claim_calls == []


def test_resolve_stale_recovery_interval_reads_default_and_env(monkeypatch):
    monkeypatch.delenv(
        "SATELLITE_WORKER_STALE_RECOVERY_INTERVAL_SECONDS",
        raising=False,
    )
    assert resolve_satellite_worker_stale_recovery_interval_seconds() == 30

    monkeypatch.setenv(
        "SATELLITE_WORKER_STALE_RECOVERY_INTERVAL_SECONDS",
        "45",
    )
    assert resolve_satellite_worker_stale_recovery_interval_seconds() == 45


def test_migration_012_has_expected_revision_and_index_metadata():
    content = _migration_012_content()

    assert 'revision: str = "012_add_satellite_job_stale_recovery"' in content
    assert 'down_revision: Union[str, Sequence[str], None] = "011_add_satellite_job_claiming"' in content
    assert "ix_satellite_jobs_running_heartbeat_at" in content
    assert '"status = \'running\' AND heartbeat_at IS NOT NULL"' in content


def test_migration_012_uses_security_definer_fixed_search_path_and_restricted_execute():
    content = _normalized_migration_012_content()

    assert "SECURITY DEFINER" in content
    assert "SET search_path = public, pg_temp" in content
    assert (
        '"REVOKE ALL ON FUNCTION public.worker_recover_stale_satellite_jobs(integer) "'
        in content
    )
    assert '"FROM PUBLIC"' in content
    assert "FROM {RUNTIME_ROLE}" in content
    assert "GRANT EXECUTE ON FUNCTION public.worker_recover_stale_satellite_jobs(integer)" in content
    assert "TO {WORKER_EXECUTOR_ROLE}" in content
    assert "GRANT SELECT" not in content
    assert "GRANT UPDATE" not in content
    assert "GRANT DELETE" not in content
    assert "GRANT INSERT" not in content


def test_migration_012_uses_fixed_90_second_threshold_and_bounded_batch():
    content = _normalized_migration_012_content()

    assert "clock_timestamp()" in content
    assert "interval '90 seconds'" in content
    assert "greatest(" in content
    assert "least(coalesce(requested_batch_size, 10), 100)" in content
    assert "LIMIT effective_batch" in content


def test_migration_012_uses_atomic_skip_locked_candidate_and_stale_recheck():
    content = _normalized_migration_012_content()

    assert "FOR UPDATE OF jobs SKIP LOCKED" in content
    assert "WITH candidate AS (" in content
    assert "UPDATE public.satellite_jobs AS jobs" in content
    assert "AND jobs.heartbeat_at < stale_cutoff" in content
    assert "jobs.status = 'running'" in content
    assert "jobs.finished_at IS NULL" in content
    assert "jobs.locked_at IS NOT NULL" in content
    assert "jobs.locked_by IS NOT NULL" in content
    assert "jobs.heartbeat_at IS NOT NULL" in content
    assert "jobs.lease_token IS NOT NULL" in content


def test_migration_012_requeue_branch_clears_active_metadata_and_preserves_attempts_started_at():
    content = _normalized_migration_012_content()

    assert "THEN 'queued'" in content
    assert "locked_at = NULL" in content
    assert "locked_by = NULL" in content
    assert "heartbeat_at = NULL" in content
    assert "THEN NULL" in content
    assert "next_attempt_at = CASE" in content
    assert "THEN recovery_now" in content
    assert "error_code = CASE" in content
    assert "error_message = CASE" in content
    assert "attempt_count = " not in content
    assert "started_at =" not in content


def test_migration_012_exhausted_branch_fails_and_retains_terminal_lease():
    content = _normalized_migration_012_content()

    assert "ELSE 'failed'" in content
    assert "ELSE jobs.lease_token" in content
    assert "ELSE jobs.next_attempt_at" in content
    assert "ELSE recovery_now" in content
    assert "stale_recovery_exhausted" in content
    assert "Satellite job heartbeat expired before completion " in content
    assert "and max attempts were exhausted." in content


def test_migration_012_public_return_contract_is_counts_only():
    content = _normalized_migration_012_content()

    assert "requeued_count integer" in content
    assert "failed_count integer" in content
    assert "organization_id integer" not in content
    assert "polygon_wkt_snapshot" not in content
    assert "geometry_hash" not in content
    assert "requested_stale_after" not in content
