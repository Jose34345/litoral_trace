from __future__ import annotations

from datetime import datetime, timezone
import os
import secrets
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from litoral_trace.storage import ObjectDeleteResult
from litoral_trace.us_lacey.db import reset_us_lacey_engine_state
from litoral_trace.us_lacey.sandbox import provision_us_lacey_sandbox
from litoral_trace.us_lacey.worker_db import reset_us_lacey_worker_engine_state
from litoral_trace.workers.sandbox_cleanup import (
    _delete_database_metadata,
    _delete_storage_objects,
    _load_manifest,
    _transition_to_db_deleting,
    claim_next_sandbox_purge_job,
)


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_DATABASE_URL")
    or not os.environ.get("US_LACEY_POSTGRES_TEST_DATABASE_URL")
    or not os.environ.get("US_LACEY_WORKER_DATABASE_URL"),
    reason="requires isolated U.S. runtime, root and worker PostgreSQL credentials",
)


class _PhysicalStorage:
    bucket_name = "us-lacey-ci-private"

    def __init__(self, keys: set[str]) -> None:
        self.keys = set(keys)
        self.events: list[tuple[str, str]] = []

    def object_exists(self, *, key, version_id=None):
        del version_id
        self.events.append(("exists", key))
        return key in self.keys

    def delete_object(self, *, key, version_id=None):
        del version_id
        self.events.append(("delete", key))
        self.keys.discard(key)
        return ObjectDeleteResult(delete_marker=False, version_id=None)


def _root_engine():
    return create_engine(
        os.environ["US_LACEY_POSTGRES_TEST_DATABASE_URL"],
        pool_pre_ping=True,
        hide_parameters=True,
    )


def _insert_vault_document(
    connection,
    *,
    organization_id: int,
    object_key: str,
    suffix: str,
) -> int:
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
            "sha256": (suffix[0].lower() if suffix[0].isalpha() else "a") * 64,
            "object_key": object_key,
        },
    ).scalar_one()


def _purge_claimed_job(*, claimed, worker_id: str, object_key: str) -> _PhysicalStorage:
    manifest = _load_manifest(job=claimed, worker_id=worker_id)
    assert [item.key for item in manifest] == [object_key]

    storage = _PhysicalStorage({object_key})
    _delete_storage_objects(storage=storage, manifest=manifest)
    assert object_key not in storage.keys
    assert ("delete", object_key) in storage.events

    _transition_to_db_deleting(job_id=claimed.id, worker_id=worker_id)
    _delete_database_metadata(
        job=claimed,
        worker_id=worker_id,
        expected_manifest=manifest,
    )
    return storage


def test_without_debug_consent_uses_four_hour_deadline_and_hard_purges():
    reset_us_lacey_engine_state()
    reset_us_lacey_worker_engine_state()
    root = _root_engine()
    suffix = uuid4().hex[:12]
    worker_id = f"debug-retention-default-{suffix}"

    created = provision_us_lacey_sandbox(
        client_ip=f"198.51.100.{secrets.randbelow(200) + 1}",
        user_agent="debug-retention-default-postgres",
        support_debug_opt_in=False,
    )

    try:
        with root.begin() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT
                        org.support_debug_consent,
                        org.debug_retention_until,
                        org.sandbox_expires_at,
                        job.expires_at AS purge_expires_at
                    FROM public.organizations AS org
                    JOIN public.us_lacey_sandbox_purge_jobs AS job
                      ON job.organization_id = org.id
                    WHERE org.id = :organization_id
                    """
                ),
                {"organization_id": created.organization_id},
            ).mappings().one()

            assert row["support_debug_consent"] is False
            assert row["debug_retention_until"] is None
            assert row["purge_expires_at"] == row["sandbox_expires_at"]

            ttl_seconds = (
                row["sandbox_expires_at"] - datetime.now(timezone.utc)
            ).total_seconds()
            assert 3 * 60 * 60 < ttl_seconds <= 4 * 60 * 60 + 30

            object_key = (
                f"us-lacey/tenants/{created.organization_id}/objects/{suffix}"
            )
            vault_id = _insert_vault_document(
                connection,
                organization_id=created.organization_id,
                object_key=object_key,
                suffix=f"a{suffix}",
            )

            # Time-travel the canonical four-hour purge deadline while keeping
            # the same invariant the production trigger creates.
            deadline = connection.execute(
                text("SELECT now() - interval '100 years'")
            ).scalar_one()
            connection.execute(
                text(
                    """
                    UPDATE public.organizations
                    SET sandbox_expires_at = :deadline
                    WHERE id = :organization_id
                    """
                ),
                {
                    "deadline": deadline,
                    "organization_id": created.organization_id,
                },
            )
            connection.execute(
                text(
                    """
                    UPDATE public.us_lacey_sandbox_purge_jobs
                    SET expires_at = :deadline, available_at = :deadline
                    WHERE organization_id = :organization_id
                    """
                ),
                {
                    "deadline": deadline,
                    "organization_id": created.organization_id,
                },
            )

        claimed = claim_next_sandbox_purge_job(worker_id=worker_id)
        assert claimed is not None
        assert claimed.organization_id == created.organization_id

        _purge_claimed_job(
            claimed=claimed,
            worker_id=worker_id,
            object_key=object_key,
        )

        with root.connect() as connection:
            assert connection.execute(
                text(
                    "SELECT count(*) FROM public.vault_documents WHERE id = :id"
                ),
                {"id": vault_id},
            ).scalar_one() == 0
            assert connection.execute(
                text(
                    "SELECT count(*) FROM public.organizations WHERE id = :id"
                ),
                {"id": created.organization_id},
            ).scalar_one() == 0
    finally:
        root.dispose()
        reset_us_lacey_engine_state()
        reset_us_lacey_worker_engine_state()


def test_debug_consent_survives_four_hours_quarantines_and_hard_purges_at_72h():
    reset_us_lacey_engine_state()
    reset_us_lacey_worker_engine_state()
    root = _root_engine()
    suffix = uuid4().hex[:12]
    operation_id = uuid4()
    worker_id = f"debug-retention-opt-in-{suffix}"

    created = provision_us_lacey_sandbox(
        client_ip=f"203.0.113.{secrets.randbelow(200) + 1}",
        user_agent="debug-retention-opt-in-postgres",
        support_debug_opt_in=True,
    )

    try:
        with root.begin() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT
                        org.support_debug_consent,
                        org.debug_retention_until,
                        org.sandbox_expires_at,
                        org.sandbox_started_at,
                        job.expires_at AS purge_expires_at,
                        job.available_at
                    FROM public.organizations AS org
                    JOIN public.us_lacey_sandbox_purge_jobs AS job
                      ON job.organization_id = org.id
                    WHERE org.id = :organization_id
                    """
                ),
                {"organization_id": created.organization_id},
            ).mappings().one()

            assert row["support_debug_consent"] is True
            assert row["debug_retention_until"] is not None
            assert row["purge_expires_at"] == row["debug_retention_until"]
            assert row["available_at"] == row["debug_retention_until"]

            access_ttl = (
                row["sandbox_expires_at"] - datetime.now(timezone.utc)
            ).total_seconds()
            retention_ttl = (
                row["debug_retention_until"] - row["sandbox_started_at"]
            ).total_seconds()
            assert 3 * 60 * 60 < access_ttl <= 4 * 60 * 60 + 30
            assert 72 * 60 * 60 - 5 <= retention_ttl <= 72 * 60 * 60 + 5

            object_key = (
                f"us-lacey/support-quarantine/{operation_id}/tenants/"
                f"{created.organization_id}/objects/{suffix}"
            )
            vault_id = _insert_vault_document(
                connection,
                organization_id=created.organization_id,
                object_key=object_key,
                suffix=f"b{suffix}",
            )

            # Simulate the normal four-hour workspace expiry. The debug deadline
            # and purge job remain in the future, so this tenant is not sweepable.
            connection.execute(
                text(
                    """
                    UPDATE public.organizations
                    SET sandbox_expires_at = now() - interval '1 minute'
                    WHERE id = :organization_id
                    """
                ),
                {"organization_id": created.organization_id},
            )
            due_at_four_hours = connection.execute(
                text(
                    """
                    SELECT count(*)
                    FROM public.us_lacey_sandbox_purge_jobs
                    WHERE organization_id = :organization_id
                      AND state IN ('PENDING', 'RETRY')
                      AND available_at <= now()
                      AND expires_at <= now()
                    """
                ),
                {"organization_id": created.organization_id},
            ).scalar_one()
            assert due_at_four_hours == 0
            assert connection.execute(
                text(
                    "SELECT count(*) FROM public.vault_documents WHERE id = :id"
                ),
                {"id": vault_id},
            ).scalar_one() == 1

            # Time-travel the bounded 72-hour deadline. Use an old timestamp so
            # this job wins the global SKIP LOCKED ordering deterministically.
            deadline = connection.execute(
                text("SELECT now() - interval '100 years'")
            ).scalar_one()
            connection.execute(
                text(
                    """
                    UPDATE public.organizations
                    SET debug_retention_until = :deadline
                    WHERE id = :organization_id
                    """
                ),
                {
                    "deadline": deadline,
                    "organization_id": created.organization_id,
                },
            )
            connection.execute(
                text(
                    """
                    UPDATE public.us_lacey_sandbox_purge_jobs
                    SET expires_at = :deadline, available_at = :deadline
                    WHERE organization_id = :organization_id
                    """
                ),
                {
                    "deadline": deadline,
                    "organization_id": created.organization_id,
                },
            )

        claimed = claim_next_sandbox_purge_job(worker_id=worker_id)
        assert claimed is not None
        assert claimed.organization_id == created.organization_id

        _purge_claimed_job(
            claimed=claimed,
            worker_id=worker_id,
            object_key=object_key,
        )

        with root.connect() as connection:
            assert connection.execute(
                text(
                    "SELECT count(*) FROM public.vault_documents WHERE id = :id"
                ),
                {"id": vault_id},
            ).scalar_one() == 0
            assert connection.execute(
                text(
                    "SELECT count(*) FROM public.organizations WHERE id = :id"
                ),
                {"id": created.organization_id},
            ).scalar_one() == 0
            tombstone = connection.execute(
                text(
                    """
                    SELECT state, completed_at
                    FROM public.us_lacey_sandbox_purge_jobs
                    WHERE id = :job_id
                    """
                ),
                {"job_id": claimed.id},
            ).mappings().one()
            assert tombstone["state"] == "COMPLETED"
            assert tombstone["completed_at"] is not None
    finally:
        root.dispose()
        reset_us_lacey_engine_state()
        reset_us_lacey_worker_engine_state()
