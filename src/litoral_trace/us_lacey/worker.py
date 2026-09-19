"""One-job U.S. worker execution built on the mature Assurance processor."""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
import logging
import os
from pathlib import PurePath
import threading
import time
from uuid import UUID

from sqlalchemy import select

from litoral_trace.assurance.processing import AssuranceProcessingService
from litoral_trace.db.models import (
    AssuranceDocument,
    UsLaceyEngineDocumentRun,
    UsLaceyOperation,
    UsLaceyOperationDocument,
    UsLaceyProcessingJob,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.vault import VaultService
from litoral_trace.us_lacey import specialized_shadow
from litoral_trace.us_lacey.ai_review import recommend_open_reconciliation_issues
from litoral_trace.us_lacey.ai_suggestions import project_verified_ai_suggestions
from litoral_trace.us_lacey.batch_hardening import (
    ShipmentBatchRejected,
    enforce_shipment_document_budget,
    enforce_streamed_csv_budget,
    shipment_spreadsheet_limits,
)
from litoral_trace.us_lacey.candidate_reconciliation import (
    reconcile_duplicate_field_candidates,
)
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.engine2_suggestions import project_engine2_supported_suggestions
from litoral_trace.us_lacey.jobs import (
    claim_next_us_lacey_job,
    complete_us_lacey_job,
    fail_us_lacey_job,
    heartbeat_us_lacey_job,
    recover_stale_us_lacey_jobs,
)
from litoral_trace.us_lacey.operation_lock import us_lacey_operation_projection_lock
from litoral_trace.us_lacey.product_intelligence_snapshot import build_product_intelligence_snapshot
from litoral_trace.us_lacey.regulatory_assessment_snapshot import build_regulatory_assessment_snapshot
from litoral_trace.us_lacey.source_sets import SourceSetClaim, claim_ready_source_set, finalize_claim
from litoral_trace.us_lacey.projection import (
    project_assurance_document_to_us_lacey,
    refresh_us_lacey_operation_status,
)
from litoral_trace.us_lacey.lacey_engine_service import (
    ENGINE2_SHADOW,
    UsLaceyEngine2Service as _BaseUsLaceyEngine2Service,
    engine2_mode,
)
from litoral_trace.us_lacey.shadow_evidence_snapshot import build_shadow_evidence_snapshot
from litoral_trace.us_lacey.storage import (
    build_us_lacey_storage_settings,
    get_us_lacey_storage_client,
)


class UsLaceyWorkerError(RuntimeError):
    pass


LOGGER = logging.getLogger(__name__)


def _log_stage_timing(
    *,
    job,
    stage: str,
    started_at: float,
    source_set_fingerprint: str | None = None,
    job_status: str | None = None,
) -> None:
    """Emit one structured monotonic duration for worker latency diagnosis."""
    extra = {
        "event": "us_lacey_stage_timing",
        "stage": stage,
        "duration_ms": float(max(0.0, (time.perf_counter() - started_at) * 1000.0)),
        "organization_id": job.organization_id,
        "operation_id": job.operation_id,
        "job_id": job.id,
    }
    if source_set_fingerprint is not None:
        extra["source_set_fingerprint"] = source_set_fingerprint
    if job_status is not None:
        extra["job_status"] = job_status
    LOGGER.info("U.S. Lacey worker stage timing", extra=extra)


@contextmanager
def _timed_worker_stage(
    *,
    job,
    stage: str,
    source_set_fingerprint: str | None = None,
):
    started_at = time.perf_counter()
    try:
        yield
    finally:
        _log_stage_timing(
            job=job,
            stage=stage,
            started_at=started_at,
            source_set_fingerprint=source_set_fingerprint,
        )


class UsLaceyEngine2Service(_BaseUsLaceyEngine2Service):
    """Production worker service with durable specialized-source-set idempotency.

    The specialized architecture is shadow-only. Once every document in the exact
    immutable source set already has a successful specialized run for the configured
    provider/model, there is no value in downloading the files and invoking Gemini
    again. Failed or incomplete sets remain retryable.
    """

    def _specialized_source_set_succeeded(
        self,
        *,
        config,
        organization_id: int,
        operation_id: int,
        documents,
        source_set_fingerprint: str,
    ) -> bool:
        if not documents:
            return False
        engine_version = specialized_shadow.specialized_engine_version(
            provider=config.provider,
            model=config.model,
            source_set_fingerprint=source_set_fingerprint,
        )
        session = self._session_factory()
        try:
            set_tenant_db_context(session, organization_id)
            for document in documents:
                existing = session.scalar(
                    select(UsLaceyEngineDocumentRun).where(
                        UsLaceyEngineDocumentRun.organization_id == organization_id,
                        UsLaceyEngineDocumentRun.operation_id == operation_id,
                        UsLaceyEngineDocumentRun.assurance_document_id
                        == document.assurance.id,
                        UsLaceyEngineDocumentRun.source_sha256 == document.vault.sha256,
                        UsLaceyEngineDocumentRun.engine_version == engine_version,
                        UsLaceyEngineDocumentRun.schema_version
                        == specialized_shadow.SPECIALIZED_SHADOW_SCHEMA_VERSION,
                        UsLaceyEngineDocumentRun.role_hint == document.link.document_role,
                        UsLaceyEngineDocumentRun.status == "SUCCEEDED",
                    )
                )
                if existing is None:
                    return False
            return True
        finally:
            session.close()

    def _run_specialized_ai_operation(
        self,
        *,
        config,
        organization_id: int,
        operation_id: int,
        documents,
        source_set_fingerprint: str,
    ) -> None:
        if documents and self._specialized_source_set_succeeded(
            config=config,
            organization_id=organization_id,
            operation_id=operation_id,
            documents=documents,
            source_set_fingerprint=source_set_fingerprint,
        ):
            LOGGER.info(
                "Lacey specialized AI shadow source set already persisted",
                extra={
                    "organization_id": organization_id,
                    "operation_id": operation_id,
                    "document_count": len(documents),
                    "source_set_fingerprint": source_set_fingerprint,
                },
            )
            return
        super()._run_specialized_ai_operation(
            config=config,
            organization_id=organization_id,
            operation_id=operation_id,
            documents=documents,
            source_set_fingerprint=source_set_fingerprint,
        )


class _UsLaceyJobHeartbeat:
    """Refresh a RUNNING queue lease while parsers and shadow AI are busy."""

    def __init__(self, *, job_id, worker_id: str, interval_seconds: float) -> None:
        self._job_id = job_id
        self._worker_id = worker_id
        self._interval_seconds = max(float(interval_seconds), 0.001)
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"us-lacey-heartbeat-{job_id}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        while not self._stop_event.wait(self._interval_seconds):
            try:
                owned = heartbeat_us_lacey_job(
                    job_id=self._job_id, worker_id=self._worker_id,
                )
            except Exception:
                LOGGER.exception(
                    "U.S. Lacey job heartbeat failed",
                    extra={"job_id": str(self._job_id), "worker_id": self._worker_id},
                )
                continue
            if not owned:
                LOGGER.warning(
                    "U.S. Lacey job heartbeat lost ownership",
                    extra={"job_id": str(self._job_id), "worker_id": self._worker_id},
                )
                return

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=max(1.0, self._interval_seconds + 1.0))


def _job_heartbeat_interval_seconds() -> float:
    raw = str(os.getenv("US_LACEY_WORKER_HEARTBEAT_SECONDS", "30")).strip()
    try:
        value = float(raw)
    except ValueError:
        LOGGER.warning("Invalid US_LACEY_WORKER_HEARTBEAT_SECONDS=%r; using 30", raw)
        return 30.0
    return max(5.0, min(300.0, value))


@dataclass(frozen=True, slots=True)
class UsLaceyWorkerResult:
    claimed: bool
    job_id: int | None
    job_status: str | None
    document_status: str | None
    operation_status: str | None
    projected_count: int
    conflict_count: int


@dataclass(frozen=True, slots=True)
class _DocumentDescriptor:
    assurance_public_id: UUID
    vault_public_id: UUID
    filename: str
    size_bytes: int


def _processing_service() -> AssuranceProcessingService:
    settings = build_us_lacey_storage_settings()
    storage = get_us_lacey_storage_client()
    vault = VaultService(
        storage_settings=settings,
        storage=storage,
        session_factory=get_us_lacey_db_session,
    )
    return AssuranceProcessingService(
        session_factory=get_us_lacey_db_session,
        vault_service=vault,
        enable_entity_matching=False,
    )


def _shadow_engine2(*, organization_id: int, operation_id: int) -> None:
    """Best-effort only: never changes authoritative job/projection semantics."""
    if engine2_mode() != ENGINE2_SHADOW:
        return
    settings = build_us_lacey_storage_settings()
    vault = VaultService(
        storage_settings=settings,
        storage=get_us_lacey_storage_client(),
        session_factory=get_us_lacey_db_session,
    )
    try:
        UsLaceyEngine2Service(vault_service=vault).resolve_operation_with_engine2(
            organization_id=organization_id,
            operation_id=operation_id,
        )
    except Exception:
        LOGGER.exception(
            "Lacey Engine 2 shadow resolution failed",
            extra={"organization_id": organization_id, "operation_id": operation_id},
        )
        return


def _operation_source_set_ready_for_finalization(
    *, organization_id: int, operation_id: int, completing_job_id: int,
) -> bool:
    """Allow operation-level work only once every current source has reached it.

    The currently owned job is permitted to remain RUNNING. Any sibling queued,
    retrying or running job -- and any newly attached source without a job yet --
    defers AI/shadow publication. This is a state barrier, not a timed debounce.
    """
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, organization_id)
        sources = session.scalars(
            select(UsLaceyOperationDocument).where(
                UsLaceyOperationDocument.organization_id == organization_id,
                UsLaceyOperationDocument.operation_id == operation_id,
                UsLaceyOperationDocument.is_current.is_(True),
            )
        ).all()
        if not sources:
            return False
        jobs = session.scalars(
            select(UsLaceyProcessingJob).where(
                UsLaceyProcessingJob.organization_id == organization_id,
                UsLaceyProcessingJob.operation_id == operation_id,
            )
        ).all()
        status_by_document = {
            int(job.assurance_document_id): (int(job.id), str(job.status)) for job in jobs
        }
        for source in sources:
            current = status_by_document.get(int(source.assurance_document_id))
            if current is None:
                return False
            job_id, status = current
            if job_id == int(completing_job_id):
                if status != "RUNNING":
                    return False
                continue
            if status != "COMPLETED":
                return False
        return True
    finally:
        session.close()


def _claim_source_set_finalization(*, organization_id: int, operation_id: int, completing_job_id: int) -> SourceSetClaim:
    """Claim one sealed generation; never infer completion from a transient read."""
    return claim_ready_source_set(
        organization_id=organization_id,
        operation_id=operation_id,
        completing_job_id=completing_job_id,
    )


def _project_engine2_suggestions(*, organization_id: int, operation_id: int) -> int:
    """Final canonical publication boundary; any failure aborts the owned job."""
    return int(
        project_engine2_supported_suggestions(
            organization_id=organization_id,
            operation_id=operation_id,
        )
        or 0
    )


def _build_product_intelligence_snapshot(*, organization_id: int, operation_id: int, claim: SourceSetClaim):
    """Best-effort non-canonical composition evidence for the exact claimed source set."""
    try:
        return build_product_intelligence_snapshot(
            organization_id=organization_id,
            operation_id=operation_id,
            claim=claim,
        )
    except Exception:
        LOGGER.exception(
            "Lacey Product Intelligence snapshot build failed",
            extra={
                "organization_id": organization_id,
                "operation_id": operation_id,
                "source_set_revision_id": claim.revision_id,
                "source_set_generation": claim.generation,
                "source_set_fingerprint": claim.fingerprint,
            },
        )
        return None


def _build_regulatory_assessment_snapshot(*, organization_id: int, operation_id: int, claim: SourceSetClaim):
    """Best-effort non-canonical rule assessment for the exact claimed source set."""
    try:
        return build_regulatory_assessment_snapshot(
            organization_id=organization_id,
            operation_id=operation_id,
            claim=claim,
        )
    except Exception:
        # Fail closed: a rule-engine failure produces no CURRENT safe assessment,
        # but cannot roll back the already-published canonical extraction result.
        LOGGER.exception(
            "Lacey regulatory assessment snapshot build failed",
            extra={
                "organization_id": organization_id,
                "operation_id": operation_id,
                "source_set_revision_id": claim.revision_id,
                "source_set_generation": claim.generation,
                "source_set_fingerprint": claim.fingerprint,
            },
        )
        return None


def _reconcile_candidate_equivalence(*, organization_id: int, operation_id: int) -> int:
    """Resolve false conflicts fail-closed; failure leaves human review intact."""
    try:
        result = reconcile_duplicate_field_candidates(
            organization_id=organization_id,
            operation_id=operation_id,
        )
        return int(result.resolved_conflict_count)
    except Exception:
        LOGGER.exception(
            "Lacey candidate-equivalence reconciliation failed closed",
            extra={
                "organization_id": organization_id,
                "operation_id": operation_id,
            },
        )
        return 0


def _project_verified_ai_suggestions(*, organization_id: int, operation_id: int) -> int:
    """Best-effort AI bridge; return how many review fields actually changed."""
    try:
        return int(
            project_verified_ai_suggestions(
                organization_id=organization_id,
                operation_id=operation_id,
            )
            or 0
        )
    except Exception:
        LOGGER.exception(
            "Lacey verified AI suggestion projection failed",
            extra={"organization_id": organization_id, "operation_id": operation_id},
        )
        return 0


def _run_ai_review_recommendations(*, organization_id: int, operation_id: int) -> None:
    """Best-effort only: recommendation JSON cannot change declaration authority."""
    try:
        recommend_open_reconciliation_issues(
            organization_id=organization_id,
            operation_id=operation_id,
        )
    except Exception:
        LOGGER.exception(
            "Lacey AI review recommendation failed",
            extra={"organization_id": organization_id, "operation_id": operation_id},
        )


def _shadow_multilingual_evidence_snapshot(*, organization_id: int, operation_id: int) -> None:
    """Best-effort Phase B dual-write; legacy completion is authoritative."""
    try:
        build_shadow_evidence_snapshot(
            organization_id=organization_id,
            operation_id=operation_id,
        )
    except Exception:
        LOGGER.exception(
            "Lacey multilingual evidence shadow snapshot failed",
            extra={"organization_id": organization_id, "operation_id": operation_id},
        )


def _assurance_public_id(*, organization_id: int, document_id: int) -> UUID:
    """Preserve the worker's stable lookup seam used by existing contracts/tests."""
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, organization_id)
        document = session.scalar(
            select(AssuranceDocument).where(
                AssuranceDocument.organization_id == organization_id,
                AssuranceDocument.id == document_id,
            )
        )
        if document is None:
            raise UsLaceyWorkerError("Queued document no longer exists.")
        return document.public_id
    finally:
        session.close()


def _document_descriptor(*, organization_id: int, document_id: int) -> _DocumentDescriptor:
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, organization_id)
        document = session.scalar(
            select(AssuranceDocument).where(
                AssuranceDocument.organization_id == organization_id,
                AssuranceDocument.id == document_id,
            )
        )
        if document is None:
            raise UsLaceyWorkerError("Queued document no longer exists.")
        vault_document = session.scalar(
            select(VaultDocument).where(
                VaultDocument.organization_id == organization_id,
                VaultDocument.id == document.vault_document_id,
                VaultDocument.status == "available",
            )
        )
        if vault_document is None:
            raise UsLaceyWorkerError("Queued document original is not available.")
        return _DocumentDescriptor(
            assurance_public_id=document.public_id,
            vault_public_id=vault_document.public_id,
            filename=vault_document.original_filename,
            size_bytes=int(vault_document.size_bytes),
        )
    finally:
        session.close()


def _mark_document_policy_failure(
    *,
    organization_id: int,
    document_id: int,
    code: str,
    message: str,
) -> None:
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, organization_id)
        document = session.scalar(
            select(AssuranceDocument).where(
                AssuranceDocument.organization_id == organization_id,
                AssuranceDocument.id == document_id,
            )
        )
        if document is not None:
            document.processing_status = "FAILED"
            document.last_error_code = code[:100]
            document.last_error_message = message[:512]
            session.commit()
    except Exception:
        session.rollback()
        LOGGER.exception(
            "Unable to persist batch-policy failure",
            extra={"organization_id": organization_id, "document_id": document_id},
        )
    finally:
        session.close()


def _preflight_existing_document(*, organization_id: int, descriptor: _DocumentDescriptor) -> None:
    """Reject legacy queued bulk spreadsheets before the expensive parser allocates memory."""
    suffix = PurePath(descriptor.filename).suffix.lower()
    if suffix not in {".csv", ".xlsx", ".xls"}:
        return
    limits = shipment_spreadsheet_limits()
    if descriptor.size_bytes > limits.max_bytes:
        raise ShipmentBatchRejected(
            "DATASET_TOO_LARGE_FOR_SHIPMENT_PIPELINE",
            "This spreadsheet is a bulk or multi-shipment dataset and cannot be processed as one Lacey operation.",
        )
    settings = build_us_lacey_storage_settings()
    vault = VaultService(
        storage_settings=settings,
        storage=get_us_lacey_storage_client(),
        session_factory=get_us_lacey_db_session,
    )
    with vault.materialize_verified_download(
        organization_id=organization_id,
        document_id=descriptor.vault_public_id,
    ) as verified:
        if suffix == ".csv":
            enforce_streamed_csv_budget(
                chunks=verified.iter_chunks(chunk_size=256 * 1024),
                size_bytes=descriptor.size_bytes,
                limits=limits,
            )
            return
        content = b"".join(verified.iter_chunks(chunk_size=256 * 1024))
        enforce_shipment_document_budget(
            filename=descriptor.filename,
            content=content,
            limits=limits,
        )


def _refresh_operation(*, organization_id: int, operation_id: int) -> str:
    session = get_us_lacey_db_session()
    try:
        set_tenant_db_context(session, organization_id)
        operation = session.scalar(
            select(UsLaceyOperation).where(
                UsLaceyOperation.organization_id == organization_id,
                UsLaceyOperation.id == operation_id,
            )
        )
        if operation is None:
            raise UsLaceyWorkerError("Queued operation no longer exists.")
        status = refresh_us_lacey_operation_status(
            session,
            organization_id=organization_id,
            operation=operation,
        )
        session.commit()
        return status
    except UsLaceyWorkerError:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        raise UsLaceyWorkerError("Unable to refresh operation state.") from exc
    finally:
        session.close()


def process_one_us_lacey_job(
    *,
    worker_id: str,
    recover_stale: bool = False,
) -> UsLaceyWorkerResult:
    """Claim and process one durable job; safe to call repeatedly from a worker loop."""
    if recover_stale:
        recover_stale_us_lacey_jobs()
    job = claim_next_us_lacey_job(worker_id=worker_id)
    if job is None:
        return UsLaceyWorkerResult(
            claimed=False,
            job_id=None,
            job_status=None,
            document_status=None,
            operation_status=None,
            projected_count=0,
            conflict_count=0,
        )

    total_started_at = time.perf_counter()
    heartbeat = _UsLaceyJobHeartbeat(
        job_id=job.id,
        worker_id=worker_id,
        interval_seconds=_job_heartbeat_interval_seconds(),
    )
    heartbeat.start()
    try:
        assurance_public_id = _assurance_public_id(
            organization_id=job.organization_id,
            document_id=job.assurance_document_id,
        )

        with _timed_worker_stage(job=job, stage="preflight"):
            if isinstance(assurance_public_id, UUID):
                descriptor = _document_descriptor(
                    organization_id=job.organization_id,
                    document_id=job.assurance_document_id,
                )
                try:
                    _preflight_existing_document(
                        organization_id=job.organization_id,
                        descriptor=descriptor,
                    )
                except ShipmentBatchRejected as exc:
                    _mark_document_policy_failure(
                        organization_id=job.organization_id,
                        document_id=job.assurance_document_id,
                        code=exc.code,
                        message=exc.safe_message,
                    )
                    queue_status = fail_us_lacey_job(
                        job_id=job.id,
                        worker_id=worker_id,
                        error_code=exc.code,
                        safe_error_message=exc.safe_message,
                        retryable=False,
                    )
                    operation_status = _refresh_operation(
                        organization_id=job.organization_id,
                        operation_id=job.operation_id,
                    )
                    return UsLaceyWorkerResult(
                        claimed=True,
                        job_id=job.id,
                        job_status=queue_status,
                        document_status="FAILED",
                        operation_status=operation_status,
                        projected_count=0,
                        conflict_count=0,
                    )

        with _timed_worker_stage(job=job, stage="document_processing"):
            document_status = _processing_service().process(
                organization_id=job.organization_id,
                assurance_public_id=assurance_public_id,
            )
        if document_status == "FAILED":
            queue_status = fail_us_lacey_job(
                job_id=job.id,
                worker_id=worker_id,
                error_code="DOCUMENT_PROCESSING_FAILED",
                safe_error_message=(
                    "The document could not be processed. The original file remains preserved."
                ),
            )
            operation_status = _refresh_operation(
                organization_id=job.organization_id,
                operation_id=job.operation_id,
            )
            return UsLaceyWorkerResult(
                claimed=True,
                job_id=job.id,
                job_status=queue_status,
                document_status=document_status,
                operation_status=operation_status,
                projected_count=0,
                conflict_count=0,
            )

        projection_guard = (
            us_lacey_operation_projection_lock(
                organization_id=job.organization_id,
                operation_id=job.operation_id,
            )
            if isinstance(assurance_public_id, UUID)
            else nullcontext()
        )
        with projection_guard:
            with _timed_worker_stage(job=job, stage="authoritative_projection"):
                projection = project_assurance_document_to_us_lacey(
                    organization_id=job.organization_id,
                    operation_id=job.operation_id,
                    assurance_document_id=job.assurance_document_id,
                )

            source_set_claim = (
                _claim_source_set_finalization(
                    organization_id=job.organization_id,
                    operation_id=job.operation_id,
                    completing_job_id=job.id,
                )
                if isinstance(assurance_public_id, UUID)
                else SourceSetClaim(None, None, None, True, "TEST")
            )
            finalize_source_set = source_set_claim.claimed
            source_set_fingerprint = source_set_claim.fingerprint

            if finalize_source_set:
                with _timed_worker_stage(
                    job=job,
                    stage="candidate_equivalence_reconciliation",
                    source_set_fingerprint=source_set_fingerprint,
                ):
                    _reconcile_candidate_equivalence(
                        organization_id=job.organization_id,
                        operation_id=job.operation_id,
                    )
                with _timed_worker_stage(
                    job=job,
                    stage="engine2_shadow",
                    source_set_fingerprint=source_set_fingerprint,
                ):
                    _shadow_engine2(
                        organization_id=job.organization_id,
                        operation_id=job.operation_id,
                    )
                with _timed_worker_stage(
                    job=job,
                    stage="verified_ai_suggestions",
                    source_set_fingerprint=source_set_fingerprint,
                ):
                    _project_verified_ai_suggestions(
                        organization_id=job.organization_id,
                        operation_id=job.operation_id,
                    )
                with _timed_worker_stage(
                    job=job,
                    stage="canonical_publication",
                    source_set_fingerprint=source_set_fingerprint,
                ):
                    _project_engine2_suggestions(
                        organization_id=job.organization_id,
                        operation_id=job.operation_id,
                    )
                if isinstance(assurance_public_id, UUID):
                    with _timed_worker_stage(
                        job=job,
                        stage="product_intelligence",
                        source_set_fingerprint=source_set_fingerprint,
                    ):
                        _build_product_intelligence_snapshot(
                            organization_id=job.organization_id,
                            operation_id=job.operation_id,
                            claim=source_set_claim,
                        )
                    with _timed_worker_stage(
                        job=job,
                        stage="regulatory_assessment",
                        source_set_fingerprint=source_set_fingerprint,
                    ):
                        _build_regulatory_assessment_snapshot(
                            organization_id=job.organization_id,
                            operation_id=job.operation_id,
                            claim=source_set_claim,
                        )
            else:
                LOGGER.info(
                    "Lacey operation source set not ready; deferred operation-level work",
                    extra={
                        "organization_id": job.organization_id,
                        "operation_id": job.operation_id,
                        "job_id": job.id,
                        "stage": "source_set_finalization",
                    },
                )

        if isinstance(assurance_public_id, UUID) and finalize_source_set:
            with _timed_worker_stage(
                job=job,
                stage="multilingual_snapshot",
                source_set_fingerprint=source_set_fingerprint,
            ):
                _shadow_multilingual_evidence_snapshot(
                    organization_id=job.organization_id,
                    operation_id=job.operation_id,
                )
            with us_lacey_operation_projection_lock(
                organization_id=job.organization_id,
                operation_id=job.operation_id,
            ):
                with _timed_worker_stage(
                    job=job,
                    stage="source_set_finalize",
                    source_set_fingerprint=source_set_fingerprint,
                ):
                    finalized = finalize_claim(
                        organization_id=job.organization_id,
                        claim=source_set_claim,
                    )
            if not finalized:
                LOGGER.info(
                    "Lacey source-set finalization superseded before publication",
                    extra={"organization_id": job.organization_id, "operation_id": job.operation_id, "job_id": job.id, "stage": "source_set_finalization", "source_set_fingerprint": source_set_claim.fingerprint},
                )

        with _timed_worker_stage(job=job, stage="queue_complete"):
            completed = complete_us_lacey_job(job_id=job.id, worker_id=worker_id)
        if not completed:
            raise UsLaceyWorkerError("Processing job could not be completed atomically.")

        with _timed_worker_stage(job=job, stage="operation_refresh"):
            operation_status = _refresh_operation(
                organization_id=job.organization_id,
                operation_id=job.operation_id,
            )

        heartbeat.stop()

        if finalize_source_set:
            try:
                with _timed_worker_stage(
                    job=job,
                    stage="ai_review_recommendations",
                    source_set_fingerprint=source_set_fingerprint,
                ):
                    _run_ai_review_recommendations(
                        organization_id=job.organization_id,
                        operation_id=job.operation_id,
                    )
            except Exception:
                LOGGER.exception(
                    "Lacey post-completion AI review failed; completed job remains authoritative",
                    extra={
                        "organization_id": job.organization_id,
                        "operation_id": job.operation_id,
                        "job_id": job.id,
                        "source_set_fingerprint": source_set_fingerprint,
                    },
                )

        _log_stage_timing(
            job=job,
            stage="total",
            started_at=total_started_at,
            source_set_fingerprint=source_set_fingerprint,
            job_status="COMPLETED",
        )
        return UsLaceyWorkerResult(
            claimed=True,
            job_id=job.id,
            job_status="COMPLETED",
            document_status=document_status,
            operation_status=operation_status,
            projected_count=projection.projected_count,
            conflict_count=projection.conflict_count,
        )
    except Exception as exc:
        queue_status = fail_us_lacey_job(
            job_id=job.id,
            worker_id=worker_id,
            error_code="US_LACEY_WORKER_FAILED",
            safe_error_message="Document processing did not complete. A controlled retry may occur.",
        )
        try:
            operation_status = _refresh_operation(
                organization_id=job.organization_id,
                operation_id=job.operation_id,
            )
        except Exception:
            operation_status = None
        if queue_status is None:
            raise UsLaceyWorkerError("Worker lost ownership of the processing job.") from exc
        return UsLaceyWorkerResult(
            claimed=True,
            job_id=job.id,
            job_status=queue_status,
            document_status=None,
            operation_status=operation_status,
            projected_count=0,
            conflict_count=0,
        )
    finally:
        heartbeat.stop()
