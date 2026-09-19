"""Add ephemeral U.S. Lacey sandbox tenants and hardened provisioning.

Revision ID: 052_us_lacey_ephemeral_sandbox
Revises: 051_lacey_schema_readiness
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "052_us_lacey_ephemeral_sandbox"
down_revision = "051_lacey_schema_readiness"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
PROVISION_SIGNATURE = (
    "public.us_lacey_sandbox_provision(text,text,text,text,text)"
)


def _grant_temp_platform_set() -> None:
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )


def _revoke_temp_platform_set() -> None:
    op.execute(
        f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER"
    )


def upgrade() -> None:
    op.add_column(
        "organizations",
        sa.Column(
            "is_sandbox",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "organizations",
        sa.Column(
            "sandbox_expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.create_check_constraint(
        "ck_organizations_sandbox_expiry",
        "organizations",
        "(is_sandbox AND sandbox_expires_at IS NOT NULL) "
        "OR (NOT is_sandbox AND sandbox_expires_at IS NULL)",
    )
    op.create_index(
        "ix_organizations_sandbox_expiry",
        "organizations",
        ["sandbox_expires_at"],
        unique=False,
        postgresql_where=sa.text("is_sandbox = true"),
    )

    # Preserve the paid-plan invariant. Only the explicit internal SANDBOX plan
    # may have zero price.
    op.drop_constraint(
        "ck_us_lacey_subscriptions_price_positive",
        "us_lacey_subscriptions",
        type_="check",
    )
    op.create_check_constraint(
        "ck_us_lacey_subscriptions_price_positive",
        "us_lacey_subscriptions",
        "(plan_code = 'SANDBOX' AND price_cents = 0) "
        "OR (plan_code <> 'SANDBOX' AND price_cents > 0)",
    )

    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")

    op.execute(
        """
        CREATE FUNCTION public.us_lacey_sandbox_provision(
            requested_token_hash text,
            requested_family_id text,
            requested_ip text,
            requested_user_agent text,
            requested_password_hash text
        )
        RETURNS TABLE (
            organization_id integer,
            user_id integer,
            session_id integer,
            expires_at timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            new_org_id integer;
            new_user_id integer;
            new_session_id integer;
            sandbox_expires timestamptz := now() + interval '4 hours';
            random_identity text := replace(gen_random_uuid()::text, '-', '');
            sandbox_email text;
        BEGIN
            IF requested_token_hash IS NULL
               OR requested_token_hash !~ '^[0-9a-f]{64}$' THEN
                RAISE EXCEPTION 'invalid sandbox session token'
                    USING ERRCODE = '22023';
            END IF;
            IF btrim(coalesce(requested_family_id, '')) = ''
               OR char_length(requested_family_id) > 36 THEN
                RAISE EXCEPTION 'invalid sandbox session family'
                    USING ERRCODE = '22023';
            END IF;
            IF requested_password_hash IS NULL
               OR char_length(btrim(requested_password_hash)) < 40 THEN
                RAISE EXCEPTION 'invalid sandbox password hash'
                    USING ERRCODE = '22023';
            END IF;

            sandbox_email :=
                'sandbox+' || substr(random_identity, 1, 24) || '@sandbox.invalid';

            IF NULLIF(btrim(coalesce(requested_ip, '')), '') IS NOT NULL
               AND (
                    SELECT count(*)
                    FROM public.user_sessions AS sessions
                    JOIN public.organizations AS organizations
                      ON organizations.id = sessions.organization_id
                    WHERE organizations.is_sandbox
                      AND sessions.created_ip = left(btrim(requested_ip), 45)
                      AND sessions.issued_at > now() - interval '1 hour'
               ) >= 3 THEN
                RAISE EXCEPTION 'sandbox start rate limit exceeded'
                    USING ERRCODE = 'P4290';
            END IF;

            INSERT INTO public.organizations (
                name,
                slug,
                tax_id,
                tier,
                description,
                is_active,
                is_sandbox,
                sandbox_expires_at,
                created_at,
                updated_at
            ) VALUES (
                'Litoral Trace Sandbox',
                'sandbox-' || substr(random_identity, 1, 24),
                NULL,
                'sandbox',
                'Ephemeral zero-touch U.S. Lacey sandbox',
                TRUE,
                TRUE,
                sandbox_expires,
                now(),
                now()
            )
            RETURNING id INTO new_org_id;

            INSERT INTO public.users (
                organization_id,
                email,
                username,
                password_hash,
                role,
                full_name,
                is_active,
                created_at,
                updated_at
            ) VALUES (
                new_org_id,
                sandbox_email,
                sandbox_email,
                requested_password_hash,
                'cliente',
                'Sandbox Visitor',
                TRUE,
                now(),
                now()
            )
            RETURNING id INTO new_user_id;

            INSERT INTO public.us_lacey_organization_profiles (
                organization_id,
                legal_name,
                country_code,
                business_type,
                admin_contact_name,
                admin_contact_email,
                billing_email,
                account_status,
                created_at,
                updated_at
            ) VALUES (
                new_org_id,
                'Litoral Trace Sandbox',
                'US',
                'CUSTOMS_BROKER',
                'Sandbox Visitor',
                sandbox_email,
                NULL,
                'PILOT',
                now(),
                now()
            );

            INSERT INTO public.us_lacey_subscriptions (
                public_id,
                organization_id,
                plan_code,
                currency,
                price_cents,
                monthly_operation_limit,
                used_operations,
                status,
                started_at,
                renews_at,
                created_at,
                updated_at
            ) VALUES (
                gen_random_uuid(),
                new_org_id,
                'SANDBOX',
                'USD',
                0,
                1,
                0,
                'ACTIVE',
                now(),
                sandbox_expires,
                now(),
                now()
            );

            INSERT INTO public.user_sessions (
                user_id,
                organization_id,
                family_id,
                token_hash,
                issued_at,
                expires_at,
                created_ip,
                user_agent
            ) VALUES (
                new_user_id,
                new_org_id,
                requested_family_id,
                requested_token_hash,
                now(),
                sandbox_expires,
                NULLIF(left(btrim(coalesce(requested_ip, '')), 45), ''),
                NULLIF(left(btrim(coalesce(requested_user_agent, '')), 512), '')
            )
            RETURNING id INTO new_session_id;

            RETURN QUERY
            SELECT new_org_id, new_user_id, new_session_id, sandbox_expires;
        END;
        $$;
        """
    )

    op.execute(
        f"REVOKE ALL ON FUNCTION {PROVISION_SIGNATURE} FROM PUBLIC"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {PROVISION_SIGNATURE} TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"ALTER FUNCTION {PROVISION_SIGNATURE} OWNER TO {PLATFORM_ROLE}"
    )

    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()


def downgrade() -> None:
    _grant_temp_platform_set()
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")
    op.execute(f"DROP FUNCTION IF EXISTS {PROVISION_SIGNATURE}")
    op.execute("RESET ROLE")
    _revoke_temp_platform_set()

    # Preserve downgrade compatibility for any sandbox subscription rows that
    # survive a manual downgrade.
    op.execute(
        "UPDATE public.us_lacey_subscriptions "
        "SET price_cents = 1 "
        "WHERE plan_code = 'SANDBOX' AND price_cents = 0"
    )
    op.drop_constraint(
        "ck_us_lacey_subscriptions_price_positive",
        "us_lacey_subscriptions",
        type_="check",
    )
    op.create_check_constraint(
        "ck_us_lacey_subscriptions_price_positive",
        "us_lacey_subscriptions",
        "price_cents > 0",
    )

    op.drop_index(
        "ix_organizations_sandbox_expiry",
        table_name="organizations",
    )
    op.drop_constraint(
        "ck_organizations_sandbox_expiry",
        "organizations",
        type_="check",
    )
    op.drop_column("organizations", "sandbox_expires_at")
    op.drop_column("organizations", "is_sandbox")
