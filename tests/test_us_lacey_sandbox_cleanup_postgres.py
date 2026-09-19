from __future__ import annotations

import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from litoral_trace.workers.sandbox_cleanup import (
    SandboxCleanupDatabaseError,
    _delete_database_metadata,
    _load_manifest,
    _mark_retry,
    _transition_to_db_deleting,
    claim_next_sandbox_purge_job,
)
from litoral_trace.us_lacey.worker_db import reset_us_lacey_worker_engine_state


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_POSTGRES_TEST_DATABASE_URL")
    or not os.environ.get("US_LACEY_WORKER_DATABASE_URL"),
    reason="requires isolated U.S. PostgreSQL root and worker credentials",
)


def _root_engine():
    return create_engine(
        os.environ["US_LACEY_POSTGRES_TEST_DATABASE_URL"],
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _create_expired_sandbox(connection, *, suffix: str) -> int:
    return connection.execute(
        text(
            """
            INSERT INTO public.organizations (
                name, slug, tier, description, is_active,
                is_sandbox, sandbox_expires_at, created_at, updated_at
            ) VALUES (
                'Sandbox Cleanup Gate',
                :slug,
                'sandbox',
                'cleanup integration gate',
                true,
                true,
                now() - interval '1 minute',
                now(),
                now()
            )
            RETURNING id
            """
        ),
        {"slug": f"sandbox-cleanup-{suffix}"},
    ).scalar_one()


def _insert_vault_document(connection, *, organization_id: int, suffix: str) -> int:
    return connection.execute(
        text(
            """
            INSERT INTO public.vault_documents (
                public_id,
                organization_id,
                created_by_user_id,
                original_filename,
                content_type,
                size_bytes,
                sha256,
                object_key,
                storage_backend,
                storage_bucket,
                storage_etag,
                storage_version_id,
                document_type,
                status,
                created_at,
                updated_at
            ) VALUES (
                gen_random_uuid(),
                :organization_id,
                NULL,
                :filename,
                'application/pdf',
                1,
                :sha256,
                :object_key,
                's3',
                'us-lacey-ci-private',
                NULL,
                NULL,
                'OTHER_EVIDENCE',
                'available',
                now(),
                now()
            )
            RETURNING id
            """
        ),
        {
            "organization_id": organization_id,
            "filename": f"{suffix}.pdf",
            "sha256": ("a" if suffix.endswith("a") else "b") * 64,
            "object_key": f"us-lacey/ci/sandbox-cleanup/{suffix}.pdf",
        },
    ).scalar_one()


def test_purge_tombstone_survives_organization_deletion():
    reset_us_lacey_worker_engine_state()
    root = _root_engine()
    suffix = uuid4().hex[:12]
    worker_id = f"cleanup-tombstone-{suffix}"

    try:
        with root.begin() as connection:
            org_id = _create_expired_sandbox(connection, suffix=suffix)
            job = connection.execute(
                text(
                    """
                    SELECT id, state
                    FROM public.us_lacey_sandbox_purge_jobs
                    WHERE organization_id = :organization_id
                    """
                ),
                {"organization_id": org_id},
            ).mappings().one()
            assert job["state"] == "PENDING"

        claimed = claim_next_sandbox_purge_job(worker_id=worker_id)
        assert claimed is not None
        assert claimed.organization_id == org_id
        manifest = _load_manifest(job=claimed, worker_id=worker_id)
        assert manifest == ()

        _transition_to_db_deleting(job_id=claimed.id, worker_id=worker_id)
        _delete_database_metadata(
            job=claimed,
            worker_id=worker_id,
            expected_manifest=manifest,
        )

        with root.connect() as connection:
            assert connection.execute(
                text("SELECT count(*) FROM public.organizations WHERE id=:id"),
                {"id": org_id},
            ).scalar_one() == 0
            tombstone = connection.execute(
                text(
                    """
                    SELECT organization_id, state, completed_at
                    FROM public.us_lacey_sandbox_purge_jobs
                    WHERE id = :job_id
                    """
                ),
                {"job_id": claimed.id},
            ).mappings().one()

        assert tombstone["organization_id"] == org_id
        assert tombstone["state"] == "COMPLETED"
        assert tombstone["completed_at"] is not None
    finally:
        root.dispose()
        reset_us_lacey_worker_engine_state()


def test_manifest_race_rolls_back_database_purge_and_can_transition_to_retry():
    reset_us_lacey_worker_engine_state()
    root = _root_engine()
    suffix = uuid4().hex[:12]
    worker_id = f"cleanup-race-{suffix}"

    try:
        with root.begin() as connection:
            org_id = _create_expired_sandbox(connection, suffix=suffix)
            first_id = _insert_vault_document(
                connection,
                organization_id=org_id,
                suffix=f"{suffix}-a",
            )

        claimed = claim_next_sandbox_purge_job(worker_id=worker_id)
        assert claimed is not None
        expected_manifest = _load_manifest(job=claimed, worker_id=worker_id)
        assert [item.id for item in expected_manifest] == [first_id]

        _transition_to_db_deleting(job_id=claimed.id, worker_id=worker_id)

        with root.begin() as connection:
            second_id = _insert_vault_document(
                connection,
                organization_id=org_id,
                suffix=f"{suffix}-b",
            )

        with pytest.raises(
            SandboxCleanupDatabaseError,
            match="rolled back",
        ):
            _delete_database_metadata(
                job=claimed,
                worker_id=worker_id,
                expected_manifest=expected_manifest,
            )

        with root.connect() as connection:
            assert connection.execute(
                text("SELECT count(*) FROM public.organizations WHERE id=:id"),
                {"id": org_id},
            ).scalar_one() == 1
            surviving_ids = connection.execute(
                text(
                    """
                    SELECT id
                    FROM public.vault_documents
                    WHERE organization_id=:organization_id
                    ORDER BY id
                    """
                ),
                {"organization_id": org_id},
            ).scalars().all()
            state = connection.execute(
                text(
                    """
                    SELECT state
                    FROM public.us_lacey_sandbox_purge_jobs
                    WHERE id=:job_id
                    """
                ),
                {"job_id": claimed.id},
            ).scalar_one()

        assert surviving_ids == [first_id, second_id]
        assert state == "DB_DELETING"

        _mark_retry(
            job_id=claimed.id,
            worker_id=worker_id,
            attempt_count=claimed.attempt_count,
            error_message="manifest changed during purge",
        )

        with root.connect() as connection:
            retry_state = connection.execute(
                text(
                    """
                    SELECT state, last_error
                    FROM public.us_lacey_sandbox_purge_jobs
                    WHERE id=:job_id
                    """
                ),
                {"job_id": claimed.id},
            ).mappings().one()

        assert retry_state["state"] == "RETRY"
        assert "manifest changed" in retry_state["last_error"]
    finally:
        root.dispose()
        reset_us_lacey_worker_engine_state()
