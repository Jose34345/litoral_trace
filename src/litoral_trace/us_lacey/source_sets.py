"""Durable lifecycle for explicitly sealed U.S. Lacey operation source sets."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session
from typing import Callable

from litoral_trace.db.models import (
    AssuranceDocument,
    UsLaceyOperationDocument,
    UsLaceyProcessingJob,
    UsLaceySourceSetMember,
    UsLaceySourceSetRevision,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.lacey_engine_service import source_set_fingerprint


@dataclass(frozen=True, slots=True)
class SourceSetClaim:
    revision_id: int | None
    generation: int | None
    fingerprint: str | None
    claimed: bool
    reason: str
    claimed_at: datetime | None = None


SessionFactory = Callable[[], Session]


def seal_current_source_set(*, organization_id: int, operation_id: int, session_factory: SessionFactory = get_us_lacey_db_session) -> UsLaceySourceSetRevision:
    """Snapshot current links and expose them to workers only as one SEALED set."""
    session = session_factory()
    try:
        set_tenant_db_context(session, organization_id)
        rows = session.execute(
            select(UsLaceyOperationDocument, VaultDocument)
            .join(AssuranceDocument, (AssuranceDocument.id == UsLaceyOperationDocument.assurance_document_id))
            .join(VaultDocument, VaultDocument.id == AssuranceDocument.vault_document_id)
            .where(
                UsLaceyOperationDocument.organization_id == organization_id,
                UsLaceyOperationDocument.operation_id == operation_id,
                UsLaceyOperationDocument.is_current.is_(True),
            ).order_by(UsLaceyOperationDocument.id)
        ).all()
        if not rows:
            raise ValueError("A source set requires at least one current document.")
        fingerprint = source_set_fingerprint(
            organization_id=organization_id, operation_id=operation_id,
            documents=[(link, vault) for link, vault in rows],
        )
        existing = session.scalar(select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == organization_id,
            UsLaceySourceSetRevision.operation_id == operation_id,
            UsLaceySourceSetRevision.source_set_fingerprint == fingerprint,
            UsLaceySourceSetRevision.is_current.is_(True),
        ))
        if existing is not None:
            return existing
        session.execute(update(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == organization_id,
            UsLaceySourceSetRevision.operation_id == operation_id,
            UsLaceySourceSetRevision.is_current.is_(True),
        ).values(is_current=False))
        generation = int(session.scalar(select(func.coalesce(func.max(UsLaceySourceSetRevision.generation), 0)).where(
            UsLaceySourceSetRevision.organization_id == organization_id,
            UsLaceySourceSetRevision.operation_id == operation_id,
        )) or 0) + 1
        revision = UsLaceySourceSetRevision(
            organization_id=organization_id, operation_id=operation_id,
            generation=generation, source_set_fingerprint=fingerprint,
            document_count=len(rows), status="SEALED", is_current=True,
        )
        session.add(revision)
        session.flush()
        session.add_all(UsLaceySourceSetMember(
            organization_id=organization_id, source_set_revision_id=revision.id,
            operation_document_id=link.id, assurance_document_id=link.assurance_document_id,
        ) for link, _ in rows)
        session.commit()
        return revision
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def claim_ready_source_set(*, organization_id: int, operation_id: int, completing_job_id: int, session_factory: SessionFactory = get_us_lacey_db_session) -> SourceSetClaim:
    """CAS-claim a ready source set, including a newer retry of an abandoned claim."""
    session = session_factory()
    try:
        set_tenant_db_context(session, organization_id)
        revision = session.scalar(select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == organization_id,
            UsLaceySourceSetRevision.operation_id == operation_id,
            UsLaceySourceSetRevision.is_current.is_(True),
        ))
        if revision is None or revision.status not in {"SEALED", "FINALIZING"}:
            return SourceSetClaim(None, None, None, False, "NOT_SEALED")
        members = session.scalars(select(UsLaceySourceSetMember).where(
            UsLaceySourceSetMember.organization_id == organization_id,
            UsLaceySourceSetMember.source_set_revision_id == revision.id,
        )).all()
        jobs = session.scalars(select(UsLaceyProcessingJob).where(
            UsLaceyProcessingJob.organization_id == organization_id,
            UsLaceyProcessingJob.operation_id == operation_id,
        )).all()
        by_document = {job.assurance_document_id: job for job in jobs}
        if any((job := by_document.get(member.assurance_document_id)) is None or (job.id != completing_job_id and job.status != "COMPLETED") or (job.id == completing_job_id and job.status != "RUNNING") for member in members):
            return SourceSetClaim(revision.id, revision.generation, revision.source_set_fingerprint, False, "MEMBERS_PENDING")

        completing_job = next((job for job in jobs if int(job.id) == int(completing_job_id)), None)
        if completing_job is None:
            return SourceSetClaim(revision.id, revision.generation, revision.source_set_fingerprint, False, "MEMBERS_PENDING")

        if revision.status == "SEALED":
            claimed_at = session.execute(
                update(UsLaceySourceSetRevision).where(
                    UsLaceySourceSetRevision.id == revision.id,
                    UsLaceySourceSetRevision.status == "SEALED",
                    UsLaceySourceSetRevision.is_current.is_(True),
                ).values(
                    status="FINALIZING",
                    claimed_at=func.clock_timestamp(),
                ).returning(UsLaceySourceSetRevision.claimed_at)
            ).scalar_one_or_none()
            session.commit()
            return SourceSetClaim(
                revision.id,
                revision.generation,
                revision.source_set_fingerprint,
                claimed_at is not None,
                "CLAIMED" if claimed_at is not None else "ALREADY_CLAIMED",
                claimed_at,
            )

        # A queue retry is a new execution attempt. Its locked_at is written when
        # claim_next_us_lacey_job() acquires the retry, so it is newer than the
        # source-set token left by the abandoned attempt. Heartbeats do not move
        # locked_at, which prevents the original attempt from self-reclaiming.
        prior_claimed_at = revision.claimed_at
        if (
            prior_claimed_at is None
            or completing_job.locked_at is None
            or completing_job.locked_at <= prior_claimed_at
        ):
            return SourceSetClaim(
                revision.id,
                revision.generation,
                revision.source_set_fingerprint,
                False,
                "ALREADY_CLAIMED",
                prior_claimed_at,
            )

        reclaimed_at = session.execute(
            update(UsLaceySourceSetRevision).where(
                UsLaceySourceSetRevision.id == revision.id,
                UsLaceySourceSetRevision.organization_id == organization_id,
                UsLaceySourceSetRevision.is_current.is_(True),
                UsLaceySourceSetRevision.source_set_fingerprint == revision.source_set_fingerprint,
                UsLaceySourceSetRevision.status == "FINALIZING",
                UsLaceySourceSetRevision.claimed_at == prior_claimed_at,
            ).values(
                claimed_at=func.clock_timestamp(),
            ).returning(UsLaceySourceSetRevision.claimed_at)
        ).scalar_one_or_none()
        session.commit()
        return SourceSetClaim(
            revision.id,
            revision.generation,
            revision.source_set_fingerprint,
            reclaimed_at is not None,
            "RECLAIMED" if reclaimed_at is not None else "ALREADY_CLAIMED",
            reclaimed_at if reclaimed_at is not None else prior_claimed_at,
        )
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def finalize_claim(*, organization_id: int, claim: SourceSetClaim, session_factory: SessionFactory = get_us_lacey_db_session) -> bool:
    """Publish only if this exact claim token remains current for the revision."""
    if not claim.claimed or claim.revision_id is None or claim.claimed_at is None:
        return False
    session = session_factory()
    try:
        set_tenant_db_context(session, organization_id)
        changed = session.execute(update(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.id == claim.revision_id,
            UsLaceySourceSetRevision.organization_id == organization_id,
            UsLaceySourceSetRevision.is_current.is_(True),
            UsLaceySourceSetRevision.source_set_fingerprint == claim.fingerprint,
            UsLaceySourceSetRevision.status == "FINALIZING",
            UsLaceySourceSetRevision.claimed_at == claim.claimed_at,
        ).values(status="FINALIZED", finalized_at=func.clock_timestamp())).rowcount == 1
        session.commit()
        return changed
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
