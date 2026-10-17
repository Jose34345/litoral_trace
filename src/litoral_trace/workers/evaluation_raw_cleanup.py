"""Four-hour raw-document cleanup for claimed U.S. Lacey evaluations.

Structured extraction/review state remains available while the five-shipment
workspace is active; original Vault object bytes do not.
"""
from __future__ import annotations

from dataclasses import dataclass
import logging

from sqlalchemy import text

from litoral_trace.storage.s3 import ObjectStorageClient, ObjectStorageError
from litoral_trace.us_lacey.storage import get_us_lacey_storage_client
from litoral_trace.us_lacey.worker_db import get_us_lacey_worker_db_session


LOGGER = logging.getLogger(__name__)


class EvaluationRawCleanupError(RuntimeError):
    """Sanitized evaluation raw-retention failure."""


@dataclass(frozen=True, slots=True)
class EvaluationRawPurgeJob:
    id: int
    organization_id: int
    vault_document_id: int
    storage_bucket: str
    object_key: str
    storage_version_id: str | None
    attempt_count: int


def _worker_id(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > 255:
        raise EvaluationRawCleanupError("A valid cleanup worker id is required.")
    return normalized


def claim_evaluation_raw_purge_job(
    *,
    worker_id: str,
) -> EvaluationRawPurgeJob | None:
    session = get_us_lacey_worker_db_session()
    try:
        row = session.execute(
            text(
                "SELECT * FROM public.us_lacey_evaluation_raw_purge_claim("
                ":worker_id)"
            ),
            {"worker_id": _worker_id(worker_id)},
        ).mappings().one_or_none()
        session.commit()
        if row is None:
            return None
        return EvaluationRawPurgeJob(
            id=int(row["job_id"]),
            organization_id=int(row["organization_id"]),
            vault_document_id=int(row["vault_document_id"]),
            storage_bucket=str(row["storage_bucket"]),
            object_key=str(row["object_key"]),
            storage_version_id=(
                str(row["storage_version_id"])
                if row["storage_version_id"] is not None
                else None
            ),
            attempt_count=int(row["attempt_count"]),
        )
    except Exception as exc:
        session.rollback()
        raise EvaluationRawCleanupError(
            "Unable to claim evaluation raw-document cleanup."
        ) from exc
    finally:
        session.close()


def _complete(*, job: EvaluationRawPurgeJob, worker_id: str) -> None:
    session = get_us_lacey_worker_db_session()
    try:
        completed = session.execute(
            text(
                "SELECT public.us_lacey_evaluation_raw_purge_complete("
                ":job_id, :worker_id)"
            ),
            {"job_id": job.id, "worker_id": _worker_id(worker_id)},
        ).scalar_one()
        if completed is not True:
            raise EvaluationRawCleanupError(
                "Raw-document cleanup did not complete atomically."
            )
        session.commit()
    except EvaluationRawCleanupError:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise EvaluationRawCleanupError(
            "Unable to finalize evaluation raw-document cleanup."
        ) from exc
    finally:
        session.close()


def _retry(
    *,
    job: EvaluationRawPurgeJob,
    worker_id: str,
    error: str,
) -> None:
    delay = min(3600, 30 * (2 ** min(max(job.attempt_count - 1, 0), 7)))
    session = get_us_lacey_worker_db_session()
    try:
        session.execute(
            text(
                "SELECT public.us_lacey_evaluation_raw_purge_retry("
                ":job_id, :worker_id, :error, :delay)"
            ),
            {
                "job_id": job.id,
                "worker_id": _worker_id(worker_id),
                "error": str(error or "Raw cleanup failed.")[:2000],
                "delay": delay,
            },
        ).scalar_one()
        session.commit()
    except Exception as exc:
        session.rollback()
        raise EvaluationRawCleanupError(
            "Unable to persist raw-document cleanup retry."
        ) from exc
    finally:
        session.close()


def process_evaluation_raw_purge_job(
    *,
    job: EvaluationRawPurgeJob,
    worker_id: str,
    storage: ObjectStorageClient | None = None,
) -> None:
    storage_client = storage or get_us_lacey_storage_client()
    configured_bucket = getattr(storage_client, "bucket_name", None)
    if (
        configured_bucket is not None
        and str(configured_bucket) != job.storage_bucket
    ):
        _retry(
            job=job,
            worker_id=worker_id,
            error="Raw document belongs to an unexpected storage bucket.",
        )
        return

    try:
        exists = storage_client.object_exists(
            key=job.object_key,
            version_id=job.storage_version_id,
        )
        if exists:
            result = storage_client.delete_object(
                key=job.object_key,
                version_id=job.storage_version_id,
            )
            if job.storage_version_id is None and result.delete_marker:
                raise EvaluationRawCleanupError(
                    "Storage returned a delete marker; physical deletion "
                    "cannot be proven."
                )
        if storage_client.object_exists(
            key=job.object_key,
            version_id=job.storage_version_id,
        ):
            raise EvaluationRawCleanupError(
                "Raw document still exists after deletion."
            )
        _complete(job=job, worker_id=worker_id)
    except (ObjectStorageError, EvaluationRawCleanupError) as exc:
        _retry(job=job, worker_id=worker_id, error=str(exc))
    except Exception as exc:
        _retry(
            job=job,
            worker_id=worker_id,
            error="Unable to verify physical raw-document deletion.",
        )
        LOGGER.exception(
            "evaluation_raw_cleanup_failed",
            extra={
                "organization_id": job.organization_id,
                "vault_document_id": job.vault_document_id,
            },
        )


def run_evaluation_raw_cleanup_once(
    *,
    worker_id: str,
    storage: ObjectStorageClient | None = None,
) -> bool:
    job = claim_evaluation_raw_purge_job(worker_id=worker_id)
    if job is None:
        return False
    process_evaluation_raw_purge_job(
        job=job,
        worker_id=worker_id,
        storage=storage,
    )
    return True


def has_overdue_evaluation_raw_purge_backlog(
    *,
    grace_seconds: int = 600,
) -> bool:
    if grace_seconds < 60 or grace_seconds > 86400:
        raise EvaluationRawCleanupError("grace_seconds is out of range.")
    session = get_us_lacey_worker_db_session()
    try:
        return bool(
            session.execute(
                text(
                    "SELECT public.us_lacey_evaluation_raw_purge_overdue("
                    ":grace_seconds)"
                ),
                {"grace_seconds": int(grace_seconds)},
            ).scalar_one()
        )
    except Exception as exc:
        session.rollback()
        raise EvaluationRawCleanupError(
            "Unable to inspect evaluation raw-document retention."
        ) from exc
    finally:
        session.close()
