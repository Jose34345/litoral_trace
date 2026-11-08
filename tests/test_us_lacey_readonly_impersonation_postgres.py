"""PostgreSQL acceptance for zero-trust read-only impersonation (migration 055)."""
from __future__ import annotations

import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_TEST_AUDIT_DATABASE_URL")
    or not (
        os.environ.get("US_LACEY_DATABASE_URL")
        or os.environ.get("TEST_POSTGRES_DATABASE_URL")
    ),
    reason="requires the isolated PostgreSQL gate",
)


READER_ROLE = "litoral_trace_impersonation_reader"


def test_055_readonly_impersonation_role_rls_sessions_and_audit() -> None:
    audit = create_engine(
        os.environ["US_LACEY_TEST_AUDIT_DATABASE_URL"],
        pool_pre_ping=True,
    )
    runtime = create_engine(
        os.environ.get("US_LACEY_DATABASE_URL")
        or os.environ["TEST_POSTGRES_DATABASE_URL"],
        pool_pre_ping=True,
    )

    suffix = uuid4().hex[:10]
    actor_token_hash = "a" * 64
    impersonation_token_hash = "b" * 64
    ids: list[int] = []

    try:
        with audit.begin() as connection:
            def create_org(label: str) -> int:
                org_id = connection.execute(
                    text(
                        """
                        INSERT INTO public.organizations(
                            name, slug, tax_id, tier, is_active
                        )
                        VALUES(:name, :slug, :tax_id, 'pro', true)
                        RETURNING id
                        """
                    ),
                    {
                        "name": f"055 {label} {suffix}",
                        "slug": f"p055-{label}-{suffix}",
                        "tax_id": f"55{label}{suffix[:6]}",
                    },
                ).scalar_one()
                ids.append(int(org_id))
                return int(org_id)

            platform_org = create_org("platform")
            tenant_a = create_org("a")
            tenant_b = create_org("b")

            actor_user = connection.execute(
                text(
                    """
                    INSERT INTO public.users(
                        organization_id, email, username, password_hash,
                        role, is_active
                    )
                    VALUES(
                        :organization_id, :email, :username,
                        'unchanged-password-hash', 'superadmin', true
                    )
                    RETURNING id
                    """
                ),
                {
                    "organization_id": platform_org,
                    "email": f"055-actor-{suffix}@example.com",
                    "username": f"055-actor-{suffix}",
                },
            ).scalar_one()

            connection.execute(
                text(
                    """
                    INSERT INTO public.user_sessions(
                        user_id, organization_id, family_id, token_hash,
                        issued_at, expires_at
                    )
                    VALUES(
                        :user_id, :organization_id, :family_id, :token_hash,
                        now(), now() + interval '1 hour'
                    )
                    """
                ),
                {
                    "user_id": actor_user,
                    "organization_id": platform_org,
                    "family_id": str(uuid4()),
                    "token_hash": actor_token_hash,
                },
            )

            for org_id, label in ((tenant_a, "A"), (tenant_b, "B")):
                connection.execute(
                    text(
                        """
                        INSERT INTO public.us_lacey_organization_profiles(
                            organization_id, legal_name, country_code,
                            business_type, account_status
                        )
                        VALUES(:organization_id, :legal_name, 'US', 'IMPORTER', 'PILOT')
                        """
                    ),
                    {
                        "organization_id": org_id,
                        "legal_name": f"Tenant {label} {suffix}",
                    },
                )
                connection.execute(
                    text(
                        """
                        INSERT INTO public.us_lacey_operations(
                            public_id, organization_id, client_reference,
                            status, document_count, merchandise_line_count
                        )
                        VALUES(
                            :public_id, :organization_id, :client_reference,
                            'NEW', 0, 0
                        )
                        """
                    ),
                    {
                        "public_id": str(uuid4()),
                        "organization_id": org_id,
                        "client_reference": f"055-{label}-{suffix}",
                    },
                )

            connection.execute(
                text(
                    """
                    INSERT INTO public.vault_documents(
                        public_id, organization_id, original_filename,
                        content_type, size_bytes, sha256, object_key,
                        storage_backend, storage_bucket, document_type,
                        status, created_at, updated_at
                    )
                    VALUES(
                        :public_id, :organization_id, 'tenant-a.pdf',
                        'application/pdf', 100, :sha256, :object_key,
                        's3', 'private-bucket', 'OTHER_EVIDENCE',
                        'available', now(), now()
                    )
                    """
                ),
                {
                    "public_id": str(uuid4()),
                    "organization_id": tenant_a,
                    "sha256": "c" * 64,
                    "object_key": f"tenant/{tenant_a}/{suffix}.pdf",
                },
            )

        with audit.connect() as connection:
            role = connection.execute(
                text(
                    """
                    SELECT rolcanlogin, rolinherit, rolsuper, rolcreatedb,
                           rolcreaterole, rolbypassrls
                    FROM pg_roles
                    WHERE rolname = :role
                    """
                ),
                {"role": READER_ROLE},
            ).mappings().one()
            assert dict(role) == {
                "rolcanlogin": False,
                "rolinherit": False,
                "rolsuper": False,
                "rolcreatedb": False,
                "rolcreaterole": False,
                "rolbypassrls": False,
            }

            assert connection.execute(
                text(
                    """
                    SELECT count(*)
                    FROM pg_constraint
                    WHERE conrelid =
                        'public.us_lacey_admin_impersonation_sessions'::regclass
                      AND contype = 'f'
                    """
                )
            ).scalar_one() == 0

            policies = connection.execute(
                text(
                    """
                    SELECT policyname, permissive, cmd
                    FROM pg_policies
                    WHERE schemaname = 'public'
                      AND tablename = 'us_lacey_operations'
                      AND policyname IN (
                          'us_lacey_operations_impersonation_allow',
                          'us_lacey_operations_impersonation_read'
                      )
                    ORDER BY policyname
                    """
                )
            ).mappings().all()
            assert {
                row["policyname"]: (row["permissive"], row["cmd"])
                for row in policies
            } == {
                "us_lacey_operations_impersonation_allow": (
                    "PERMISSIVE",
                    "SELECT",
                ),
                "us_lacey_operations_impersonation_read": (
                    "RESTRICTIVE",
                    "SELECT",
                ),
            }

            assert connection.execute(
                text(
                    """
                    SELECT has_table_privilege(
                        :role, 'public.users', 'SELECT'
                    )
                    """
                ),
                {"role": READER_ROLE},
            ).scalar_one() is False
            assert connection.execute(
                text(
                    """
                    SELECT has_column_privilege(
                        :role,
                        'public.vault_documents',
                        'object_key',
                        'SELECT'
                    )
                    """
                ),
                {"role": READER_ROLE},
            ).scalar_one() is False

        with audit.begin() as connection:
            connection.exec_driver_sql(f"SET LOCAL ROLE {READER_ROLE}")
            connection.execute(
                text(
                    """
                    SELECT set_config(
                        'app.current_organization_id',
                        :organization_id,
                        true
                    )
                    """
                ),
                {"organization_id": str(tenant_a)},
            )
            visible = connection.execute(
                text(
                    """
                    SELECT organization_id, client_reference
                    FROM public.us_lacey_operations
                    ORDER BY organization_id
                    """
                )
            ).mappings().all()
            assert len(visible) == 1
            assert visible[0]["organization_id"] == tenant_a

            metadata = connection.execute(
                text(
                    """
                    SELECT original_filename, size_bytes
                    FROM public.vault_documents
                    """
                )
            ).mappings().one()
            assert metadata["original_filename"] == "tenant-a.pdf"
            assert metadata["size_bytes"] == 100

        with pytest.raises(DBAPIError):
            with audit.begin() as connection:
                connection.exec_driver_sql(f"SET LOCAL ROLE {READER_ROLE}")
                connection.execute(
                    text(
                        """
                        SELECT set_config(
                            'app.current_organization_id',
                            :organization_id,
                            true
                        )
                        """
                    ),
                    {"organization_id": str(tenant_a)},
                )
                connection.execute(
                    text(
                        """
                        INSERT INTO public.us_lacey_operations(
                            public_id, organization_id, client_reference,
                            status, document_count, merchandise_line_count
                        )
                        VALUES(
                            :public_id, :organization_id, 'forbidden-write',
                            'NEW', 0, 0
                        )
                        """
                    ),
                    {
                        "public_id": str(uuid4()),
                        "organization_id": tenant_a,
                    },
                )

        with pytest.raises(DBAPIError):
            with audit.begin() as connection:
                connection.exec_driver_sql(f"SET LOCAL ROLE {READER_ROLE}")
                connection.execute(
                    text(
                        """
                        SELECT set_config(
                            'app.current_organization_id',
                            :organization_id,
                            true
                        )
                        """
                    ),
                    {"organization_id": str(tenant_a)},
                )
                connection.execute(
                    text(
                        "SELECT object_key FROM public.vault_documents"
                    )
                ).all()

        with runtime.begin() as connection:
            started = connection.execute(
                text(
                    """
                    SELECT *
                    FROM public.platform_admin_start_readonly_impersonation(
                        :actor_token_hash,
                        :target_organization_id,
                        :reason,
                        :impersonation_token_hash
                    )
                    """
                ),
                {
                    "actor_token_hash": actor_token_hash,
                    "target_organization_id": tenant_a,
                    "reason": "Investigating customer operation rendering issue",
                    "impersonation_token_hash": impersonation_token_hash,
                },
            ).mappings().one()

            resolved = connection.execute(
                text(
                    """
                    SELECT *
                    FROM public.platform_admin_resolve_readonly_impersonation(
                        :actor_token_hash,
                        :impersonation_token_hash
                    )
                    """
                ),
                {
                    "actor_token_hash": actor_token_hash,
                    "impersonation_token_hash": impersonation_token_hash,
                },
            ).mappings().one()
            assert resolved["session_id"] == started["session_id"]
            assert resolved["target_organization_id"] == tenant_a

        with audit.connect() as connection:
            session = connection.execute(
                text(
                    """
                    SELECT admin_user_id, admin_organization_id,
                           target_organization_id, reason,
                           expires_at - created_at AS ttl,
                           revoked_at
                    FROM public.us_lacey_admin_impersonation_sessions
                    WHERE id = :session_id
                    """
                ),
                {"session_id": started["session_id"]},
            ).mappings().one()
            assert session["admin_user_id"] == actor_user
            assert session["admin_organization_id"] == platform_org
            assert session["target_organization_id"] == tenant_a
            assert session["ttl"].total_seconds() == 15 * 60
            assert session["revoked_at"] is None

            assert connection.execute(
                text(
                    """
                    SELECT count(*)
                    FROM public.us_lacey_admin_audit_logs
                    WHERE admin_user_id = :actor_user
                      AND target_organization_id = :target_org
                      AND action_type = 'READONLY_IMPERSONATION_STARTED'
                    """
                ),
                {
                    "actor_user": actor_user,
                    "target_org": tenant_a,
                },
            ).scalar_one() == 1

        with runtime.begin() as connection:
            connection.execute(
                text(
                    """
                    SELECT public.platform_admin_end_readonly_impersonation(
                        :actor_token_hash,
                        :impersonation_token_hash
                    )
                    """
                ),
                {
                    "actor_token_hash": actor_token_hash,
                    "impersonation_token_hash": impersonation_token_hash,
                },
            )

        with audit.connect() as connection:
            revoked_at = connection.execute(
                text(
                    """
                    SELECT revoked_at
                    FROM public.us_lacey_admin_impersonation_sessions
                    WHERE id = :session_id
                    """
                ),
                {"session_id": started["session_id"]},
            ).scalar_one()
            assert revoked_at is not None

            assert connection.execute(
                text(
                    """
                    SELECT count(*)
                    FROM public.us_lacey_admin_audit_logs
                    WHERE admin_user_id = :actor_user
                      AND target_organization_id = :target_org
                      AND action_type = 'READONLY_IMPERSONATION_ENDED'
                    """
                ),
                {
                    "actor_user": actor_user,
                    "target_org": tenant_a,
                },
            ).scalar_one() == 1

            assert connection.execute(
                text(
                    """
                    SELECT has_function_privilege(
                        :role,
                        'public.platform_admin_set_us_lacey_operation_limit(text,integer,integer)',
                        'EXECUTE'
                    )
                    """
                ),
                {"role": READER_ROLE},
            ).scalar_one() is False

    finally:
        with audit.begin() as connection:
            if ids:
                connection.execute(
                    text(
                        """
                        DELETE FROM public.us_lacey_admin_impersonation_sessions
                        WHERE admin_organization_id = ANY(:ids)
                           OR target_organization_id = ANY(:ids)
                        """
                    ),
                    {"ids": ids},
                )
                connection.execute(
                    text(
                        """
                        DELETE FROM public.us_lacey_admin_audit_logs
                        WHERE admin_organization_id = ANY(:ids)
                           OR target_organization_id = ANY(:ids)
                        """
                    ),
                    {"ids": ids},
                )
                connection.execute(
                    text(
                        "DELETE FROM public.vault_documents "
                        "WHERE organization_id = ANY(:ids)"
                    ),
                    {"ids": ids},
                )
                connection.execute(
                    text(
                        "DELETE FROM public.us_lacey_operations "
                        "WHERE organization_id = ANY(:ids)"
                    ),
                    {"ids": ids},
                )
                connection.execute(
                    text(
                        "DELETE FROM public.us_lacey_organization_profiles "
                        "WHERE organization_id = ANY(:ids)"
                    ),
                    {"ids": ids},
                )
                connection.execute(
                    text(
                        "DELETE FROM public.user_sessions "
                        "WHERE organization_id = ANY(:ids)"
                    ),
                    {"ids": ids},
                )
                connection.execute(
                    text(
                        "DELETE FROM public.users "
                        "WHERE organization_id = ANY(:ids)"
                    ),
                    {"ids": ids},
                )
                connection.execute(
                    text(
                        "DELETE FROM public.organizations "
                        "WHERE id = ANY(:ids)"
                    ),
                    {"ids": ids},
                )
        audit.dispose()
        runtime.dispose()
