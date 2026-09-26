"""Strict physical cleanup for expired zero-touch U.S. Lacey sandboxes.

Invariant:

    database metadata deleted
        => every Vault object in the verified manifest was confirmed absent

Object storage is always handled before tenant metadata.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
import json
import logging

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from litoral_trace.storage.s3 import ObjectStorageClient, ObjectStorageError
from litoral_trace.us_lacey.storage import get_us_lacey_storage_client
from litoral_trace.us_lacey.telemetry import TelemetryService
from litoral_trace.us_lacey.worker_db import get_us_lacey_worker_db_session


logger = logging.getLogger(__name__)


class SandboxCleanupError(RuntimeError):
    """Sanitized sandbox cleanup failure."""


class SandboxCleanupStorageError(SandboxCleanupError):
    """Physical storage could not be proven deleted."""


class SandboxCleanupDatabaseError(SandboxCleanupError):
    """Logical tenant destruction failed."""


class SandboxCleanupUnsupportedStorage(SandboxCleanupStorageError):
    """Provider semantics cannot prove physical deletion."""


@dataclass(frozen=True, slots=True)
class SandboxPurgeJob:
    id: int
    organization_id: int
    expires_at: datetime
    state: str
    attempt_count: int
    locked_by: str
    learning_opt_in: bool = False


@dataclass(frozen=True, slots=True)
class SandboxObjectRef:
    id: int
    bucket: str
    key: str
    version_id: str | None


def _normalize_worker_id(worker_id: str) -> str:
    value = str(worker_id or "").strip()
    if not value or len(value) > 255:
        raise SandboxCleanupError(
            "A valid sandbox cleanup worker_id is required."
        )
    return value


def _job_from_row(row) -> SandboxPurgeJob:
    return SandboxPurgeJob(
        id=int(row["id"]),
        organization_id=int(row["organization_id"]),
        expires_at=row["expires_at"],
        state=str(row["state"]),
        attempt_count=int(row["attempt_count"]),
        locked_by=str(row["locked_by"]),
        learning_opt_in=bool(row.get("learning_opt_in", False)),
    )


def _manifest_from_json(value) -> tuple[SandboxObjectRef, ...]:
    if isinstance(value, str):
        value = json.loads(value)

    if not isinstance(value, list):
        raise SandboxCleanupError(
            "Sandbox purge manifest is invalid."
        )

    objects: list[SandboxObjectRef] = []

    for raw in value:
        if not isinstance(raw, dict):
            raise SandboxCleanupError(
                "Sandbox purge manifest entry is invalid."
            )

        object_id = int(raw["id"])
        bucket = str(raw["bucket"] or "").strip()
        key = str(raw["key"] or "").strip()
        version = raw.get("version_id")
        version_id = (
            str(version).strip()
            if version is not None and str(version).strip()
            else None
        )

        if object_id <= 0 or not bucket or not key:
            raise SandboxCleanupError(
                "Sandbox purge manifest entry is incomplete."
            )

        objects.append(
            SandboxObjectRef(
                id=object_id,
                bucket=bucket,
                key=key,
                version_id=version_id,
            )
        )

    objects.sort(key=lambda item: item.id)
    return tuple(objects)


def _manifest_json(
    manifest: tuple[SandboxObjectRef, ...],
) -> str:
    return json.dumps(
        [
            {
                "id": item.id,
                "bucket": item.bucket,
                "key": item.key,
                "version_id": item.version_id,
            }
            for item in sorted(manifest, key=lambda item: item.id)
        ],
        separators=(",", ":"),
        sort_keys=True,
    )


def claim_next_sandbox_purge_job(
    *,
    worker_id: str,
) -> SandboxPurgeJob | None:
    """Atomically claim one expired sandbox without blocking peer workers."""

    normalized_worker_id = _normalize_worker_id(worker_id)
    session = get_us_lacey_worker_db_session()

    try:
        row = session.execute(
            text(
                """
                WITH candidate AS (
                    SELECT job.id
                    FROM public.us_lacey_sandbox_purge_jobs AS job
                    WHERE job.state IN ('PENDING', 'RETRY')
                      AND job.available_at <= now()
                      AND job.expires_at <= now()
                    ORDER BY
                        job.expires_at ASC,
                        job.available_at ASC,
                        job.id ASC
                    FOR UPDATE OF job SKIP LOCKED
                    LIMIT 1
                )
                UPDATE public.us_lacey_sandbox_purge_jobs AS job
                SET
                    state = 'STORAGE_DELETING',
                    attempt_count = job.attempt_count + 1,
                    locked_by = :worker_id,
                    locked_at = now(),
                    heartbeat_at = now(),
                    storage_confirmed_at = NULL,
                    last_error = NULL,
                    last_error_at = NULL,
                    updated_at = now()
                FROM candidate
                WHERE job.id = candidate.id
                RETURNING
                    job.id,
                    job.organization_id,
                    job.expires_at,
                    job.state,
                    job.attempt_count,
                    job.locked_by,
                    job.learning_opt_in
                """
            ),
            {"worker_id": normalized_worker_id},
        ).mappings().one_or_none()

        session.commit()

        if row is None:
            return None

        return _job_from_row(row)

    except Exception as exc:
        session.rollback()
        raise SandboxCleanupDatabaseError(
            "Unable to claim sandbox purge job."
        ) from exc
    finally:
        session.close()


def _load_manifest(
    *,
    job: SandboxPurgeJob,
    worker_id: str,
) -> tuple[SandboxObjectRef, ...]:
    normalized_worker_id = _normalize_worker_id(worker_id)
    session = get_us_lacey_worker_db_session()

    try:
        value = session.execute(
            text(
                """
                SELECT public.us_lacey_sandbox_purge_manifest(
                    :job_id,
                    :worker_id
                )
                """
            ),
            {
                "job_id": job.id,
                "worker_id": normalized_worker_id,
            },
        ).scalar_one()
        return _manifest_from_json(value)

    except Exception as exc:
        session.rollback()
        raise SandboxCleanupDatabaseError(
            "Unable to load sandbox purge manifest."
        ) from exc
    finally:
        session.close()


def _heartbeat(*, job_id: int, worker_id: str) -> None:
    session = get_us_lacey_worker_db_session()

    try:
        session.execute(
            text(
                """
                UPDATE public.us_lacey_sandbox_purge_jobs
                SET heartbeat_at = now(), updated_at = now()
                WHERE id = :job_id
                  AND locked_by = :worker_id
                  AND state IN ('STORAGE_DELETING', 'DB_DELETING')
                """
            ),
            {
                "job_id": int(job_id),
                "worker_id": _normalize_worker_id(worker_id),
            },
        )
        session.commit()
    except Exception as exc:
        session.rollback()
        raise SandboxCleanupDatabaseError(
            "Unable to heartbeat sandbox purge job."
        ) from exc
    finally:
        session.close()


def _delete_storage_objects(
    *,
    storage: ObjectStorageClient,
    manifest: tuple[SandboxObjectRef, ...],
    progress: Callable[[], None] | None = None,
) -> None:
    """Delete every object and actively prove that it is physically absent."""

    configured_bucket = getattr(storage, "bucket_name", None)

    for item in manifest:
        if (
            configured_bucket is not None
            and str(configured_bucket) != item.bucket
        ):
            raise SandboxCleanupStorageError(
                "Sandbox object belongs to an unexpected storage bucket."
            )

        try:
            existed_before = storage.object_exists(
                key=item.key,
                version_id=item.version_id,
            )

            if existed_before:
                delete_result = storage.delete_object(
                    key=item.key,
                    version_id=item.version_id,
                )

                if item.version_id is None and delete_result.delete_marker:
                    raise SandboxCleanupUnsupportedStorage(
                        "Storage provider returned a delete marker; "
                        "physical deletion cannot be proven."
                    )

            if storage.object_exists(
                key=item.key,
                version_id=item.version_id,
            ):
                raise SandboxCleanupStorageError(
                    "Sandbox object still exists after deletion."
                )

        except SandboxCleanupStorageError:
            raise
        except ObjectStorageError as exc:
            raise SandboxCleanupStorageError(
                "Object storage deletion failed."
            ) from exc
        except Exception as exc:
            raise SandboxCleanupStorageError(
                "Unable to verify physical object deletion."
            ) from exc

        if progress is not None:
            progress()


def _transition_to_db_deleting(
    *,
    job_id: int,
    worker_id: str,
) -> None:
    session = get_us_lacey_worker_db_session()

    try:
        updated = session.execute(
            text(
                """
                UPDATE public.us_lacey_sandbox_purge_jobs
                SET
                    state = 'DB_DELETING',
                    storage_confirmed_at = now(),
                    heartbeat_at = now(),
                    updated_at = now()
                WHERE id = :job_id
                  AND state = 'STORAGE_DELETING'
                  AND locked_by = :worker_id
                RETURNING id
                """
            ),
            {
                "job_id": int(job_id),
                "worker_id": _normalize_worker_id(worker_id),
            },
        ).scalar_one_or_none()

        if updated is None:
            raise SandboxCleanupDatabaseError(
                "Sandbox purge job ownership was lost."
            )

        session.commit()

    except SandboxCleanupDatabaseError:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise SandboxCleanupDatabaseError(
            "Unable to enter database purge phase."
        ) from exc
    finally:
        session.close()


def _delete_database_metadata(
    *,
    job: SandboxPurgeJob,
    worker_id: str,
    expected_manifest: tuple[SandboxObjectRef, ...],
) -> None:
    session = get_us_lacey_worker_db_session()

    try:
        completed = session.execute(
            text(
                """
                SELECT public.us_lacey_sandbox_purge_database(
                    :job_id,
                    :worker_id,
                    CAST(:expected_manifest AS jsonb)
                )
                """
            ),
            {
                "job_id": job.id,
                "worker_id": _normalize_worker_id(worker_id),
                "expected_manifest": _manifest_json(expected_manifest),
            },
        ).scalar_one()

        if completed is not True:
            raise SandboxCleanupDatabaseError(
                "Sandbox database purge did not complete."
            )

        session.commit()

    except SandboxCleanupDatabaseError:
        session.rollback()
        raise
    except SQLAlchemyError as exc:
        session.rollback()
        raise SandboxCleanupDatabaseError(
            "Sandbox database purge rolled back."
        ) from exc
    except Exception as exc:
        session.rollback()
        raise SandboxCleanupDatabaseError(
            "Sandbox database purge failed."
        ) from exc
    finally:
        session.close()


def _retry_delay_seconds(attempt_count: int) -> int:
    attempt = max(1, int(attempt_count))
    return min(3600, 30 * (2 ** min(attempt - 1, 7)))


def _mark_retry(
    *,
    job_id: int,
    worker_id: str,
    attempt_count: int,
    error_message: str,
) -> None:
    safe_error = str(
        error_message or "Sandbox cleanup failed."
    ).strip()[:2000]
    delay_seconds = _retry_delay_seconds(attempt_count)
    session = get_us_lacey_worker_db_session()

    try:
        updated = session.execute(
            text(
                """
                UPDATE public.us_lacey_sandbox_purge_jobs
                SET
                    state = 'RETRY',
                    available_at =
                        now() + make_interval(secs => :delay_seconds),
                    locked_by = NULL,
                    locked_at = NULL,
                    heartbeat_at = NULL,
                    last_error = :last_error,
                    last_error_at = now(),
                    updated_at = now()
                WHERE id = :job_id
                  AND locked_by = :worker_id
                  AND state IN ('STORAGE_DELETING', 'DB_DELETING')
                RETURNING id
                """
            ),
            {
                "job_id": int(job_id),
                "worker_id": _normalize_worker_id(worker_id),
                "delay_seconds": delay_seconds,
                "last_error": safe_error,
            },
        ).scalar_one_or_none()

        if updated is None:
            raise SandboxCleanupDatabaseError(
                "Sandbox purge job ownership was lost before retry."
            )

        session.commit()

    except SandboxCleanupDatabaseError:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise SandboxCleanupDatabaseError(
            "Unable to persist sandbox cleanup retry."
        ) from exc
    finally:
        session.close()


def _mark_failed(
    *,
    job_id: int,
    worker_id: str,
    error_message: str,
) -> None:
    safe_error = str(
        error_message or "Physical deletion cannot be proven."
    ).strip()[:2000]
    session = get_us_lacey_worker_db_session()

    try:
        updated = session.execute(
            text(
                """
                UPDATE public.us_lacey_sandbox_purge_jobs
                SET
                    state = 'FAILED',
                    locked_by = NULL,
                    locked_at = NULL,
                    heartbeat_at = NULL,
                    last_error = :last_error,
                    last_error_at = now(),
                    updated_at = now()
                WHERE id = :job_id
                  AND locked_by = :worker_id
                  AND state IN ('STORAGE_DELETING', 'DB_DELETING')
                RETURNING id
                """
            ),
            {
                "job_id": int(job_id),
                "worker_id": _normalize_worker_id(worker_id),
                "last_error": safe_error,
            },
        ).scalar_one_or_none()

        if updated is None:
            raise SandboxCleanupDatabaseError(
                "Sandbox purge job ownership was lost before failure persistence."
            )

        session.commit()

    except SandboxCleanupDatabaseError:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise SandboxCleanupDatabaseError(
            "Unable to persist terminal sandbox purge failure."
        ) from exc
    finally:
        session.close()


def recover_stale_sandbox_purge_jobs(
    *,
    stale_after_seconds: int = 900,
) -> int:
    if stale_after_seconds < 60 or stale_after_seconds > 86400:
        raise SandboxCleanupError(
            "stale_after_seconds is out of range."
        )

    session = get_us_lacey_worker_db_session()

    try:
        rows = session.execute(
            text(
                """
                UPDATE public.us_lacey_sandbox_purge_jobs
                SET
                    state = 'RETRY',
                    available_at = now(),
                    locked_by = NULL,
                    locked_at = NULL,
                    heartbeat_at = NULL,
                    last_error =
                        'Cleanup worker stopped before completing purge.',
                    last_error_at = now(),
                    updated_at = now()
                WHERE state IN ('STORAGE_DELETING', 'DB_DELETING')
                  AND coalesce(heartbeat_at, locked_at, updated_at)
                      < now() - make_interval(secs => :stale_after_seconds)
                RETURNING id
                """
            ),
            {"stale_after_seconds": stale_after_seconds},
        ).scalars().all()

        session.commit()
        return len(rows)

    except Exception as exc:
        session.rollback()
        raise SandboxCleanupDatabaseError(
            "Unable to recover stale sandbox purge jobs."
        ) from exc
    finally:
        session.close()


def process_sandbox_purge_job(
    *,
    job: SandboxPurgeJob,
    worker_id: str,
    storage: ObjectStorageClient | None = None,
) -> None:
    normalized_worker_id = _normalize_worker_id(worker_id)
    storage_client = (
        storage if storage is not None else get_us_lacey_storage_client()
    )

    try:
        manifest = _load_manifest(
            job=job,
            worker_id=normalized_worker_id,
        )
    except SandboxCleanupError as exc:
        _mark_retry(
            job_id=job.id,
            worker_id=normalized_worker_id,
            attempt_count=job.attempt_count,
            error_message=str(exc),
        )
        return

    try:
        _delete_storage_objects(
            storage=storage_client,
            manifest=manifest,
            progress=lambda: _heartbeat(
                job_id=job.id,
                worker_id=normalized_worker_id,
            ),
        )
    except SandboxCleanupUnsupportedStorage as exc:
        _mark_failed(
            job_id=job.id,
            worker_id=normalized_worker_id,
            error_message=str(exc),
        )
        return
    except SandboxCleanupStorageError as exc:
        _mark_retry(
            job_id=job.id,
            worker_id=normalized_worker_id,
            attempt_count=job.attempt_count,
            error_message=str(exc),
        )
        return

    try:
        _transition_to_db_deleting(
            job_id=job.id,
            worker_id=normalized_worker_id,
        )
    except SandboxCleanupError as exc:
        _mark_retry(
            job_id=job.id,
            worker_id=normalized_worker_id,
            attempt_count=job.attempt_count,
            error_message=str(exc),
        )
        return

    try:
        TelemetryService.capture_sandbox_before_purge(
            organization_id=job.organization_id,
            purge_job_id=job.id,
            learning_opt_in=job.learning_opt_in,
            sandbox_expires_at=job.expires_at,
        )
    except Exception:
        logger.error(
            "Sandbox Learning Plane capture failed; continuing physical tenant purge.",
            extra={"sandbox_purge_job_id": job.id},
            exc_info=True,
        )

    try:
        _delete_database_metadata(
            job=job,
            worker_id=normalized_worker_id,
            expected_manifest=manifest,
        )
    except SandboxCleanupDatabaseError:
        _mark_retry(
            job_id=job.id,
            worker_id=normalized_worker_id,
            attempt_count=job.attempt_count,
            error_message=(
                "Database purge rolled back; "
                "manifest will be rebuilt on retry."
            ),
        )


def run_sandbox_cleanup_once(
    *,
    worker_id: str,
    storage: ObjectStorageClient | None = None,
) -> bool:
    job = claim_next_sandbox_purge_job(worker_id=worker_id)
    if job is None:
        return False

    process_sandbox_purge_job(
        job=job,
        worker_id=worker_id,
        storage=storage,
    )
    return True
