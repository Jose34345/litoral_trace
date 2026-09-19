"""Add zero-touch ephemeral U.S. Lacey sandbox tenants.

Revision ID: 052_lacey_zero_touch_sandbox
Revises: 051_lacey_schema_readiness
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "052_lacey_zero_touch_sandbox"
down_revision = "051_lacey_schema_readiness"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "litoral_trace_app"
PLATFORM_ROLE = "litoral_trace_platform_definer"
SANDBOX_FUNCTION = (
    "public.us_lacey_sandbox_provision(text,text,text,integer,text,text)"
)


def _grant_platform_role() -> None:
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )


def _revoke_platform_role() -> None:
    op.execute(f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER")


def _replace_portal_auth_functions() -> None:
    # Paid/pilot login behavior is unchanged except that synthetic sandbox
    # principals are never password-login identities.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.us_lacey_portal_login_lookup(
            requested_email text
        )
        RETURNS TABLE (
            user_id integer,
            organization_id integer,
            password_hash text,
            user_is_active boolean,
            organization_is_active boolean,
            account_status text
        )
        LANGUAGE sql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
            SELECT
                users.id,
                users.organization_id,
                users.password_hash,
                users.is_active,
                organizations.is_active,
                profiles.account_status
            FROM public.users AS users
            JOIN public.organizations AS organizations
              ON organizations.id = users.organization_id
            JOIN public.us_lacey_organization_profiles AS profiles
              ON profiles.organization_id = users.organization_id
            WHERE users.username = lower(btrim(requested_email))
              AND users.email = lower(btrim(requested_email))
              AND NOT organizations.is_sandbox
            LIMIT 1
        $$;
        """
    )

    # Preserve the existing function signature. Sandbox sessions may use this
    # primitive only while both the browser session and tenant TTL are live.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.us_lacey_portal_create_session(
            requested_user_id integer,
            requested_organization_id integer,
            requested_token_hash text,
            requested_family_id text,
            requested_expires_at timestamptz,
            requested_ip text,
            requested_user_agent text
        )
        RETURNS TABLE (session_id integer)
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            new_session_id integer;
        BEGIN
            IF requested_user_id IS NULL OR requested_user_id <= 0
               OR requested_organization_id IS NULL OR requested_organization_id <= 0 THEN
                RAISE EXCEPTION 'invalid portal identity' USING ERRCODE = '22023';
            END IF;
            IF requested_token_hash IS NULL
               OR requested_token_hash !~ '^[0-9a-f]{64}$' THEN
                RAISE EXCEPTION 'invalid portal session token' USING ERRCODE = '22023';
            END IF;
            IF btrim(coalesce(requested_family_id, '')) = ''
               OR char_length(requested_family_id) > 36 THEN
                RAISE EXCEPTION 'invalid portal session family' USING ERRCODE = '22023';
            END IF;
            IF requested_expires_at IS NULL
               OR requested_expires_at <= now()
               OR requested_expires_at > now() + interval '31 days' THEN
                RAISE EXCEPTION 'invalid portal session expiry' USING ERRCODE = '22023';
            END IF;

            IF NOT EXISTS (
                SELECT 1
                FROM public.users AS users
                JOIN public.organizations AS organizations
                  ON organizations.id = users.organization_id
                JOIN public.us_lacey_organization_profiles AS profiles
                  ON profiles.organization_id = users.organization_id
                WHERE users.id = requested_user_id
                  AND users.organization_id = requested_organization_id
                  AND users.is_active
                  AND organizations.is_active
                  AND profiles.account_status IN ('PAYMENT_PENDING','PILOT','ACTIVE')
                  AND (
                      NOT organizations.is_sandbox
                      OR (
                          organizations.expires_at IS NOT NULL
                          AND organizations.expires_at > now()
                          AND requested_expires_at <= organizations.expires_at
                      )
                  )
            ) THEN
                RAISE EXCEPTION 'portal account unavailable' USING ERRCODE = '28000';
            END IF;

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
                requested_user_id,
                requested_organization_id,
                requested_family_id,
                requested_token_hash,
                now(),
                requested_expires_at,
                NULLIF(left(btrim(coalesce(requested_ip, '')), 45), ''),
                NULLIF(left(btrim(coalesce(requested_user_agent, '')), 512), '')
            )
            RETURNING id INTO new_session_id;

            RETURN QUERY SELECT new_session_id;
        END;
        $$;
        """
    )

    # Tenant expiry is checked in the same SECURITY DEFINER lookup that resolves
    # the opaque browser token. No expired sandbox can recover an organization_id.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.us_lacey_portal_session_lookup(
            requested_token_hash text
        )
        RETURNS TABLE (
            session_id integer,
            user_id integer,
            organization_id integer,
            email text,
            full_name text,
            legal_name text,
            business_type text,
            account_status text,
            expires_at timestamptz
        )
        LANGUAGE sql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
            SELECT
                sessions.id,
                users.id,
                users.organization_id,
                users.email,
                coalesce(users.full_name, users.email),
                profiles.legal_name,
                profiles.business_type,
                profiles.account_status,
                sessions.expires_at
            FROM public.user_sessions AS sessions
            JOIN public.users AS users
              ON users.id = sessions.user_id
             AND users.organization_id = sessions.organization_id
            JOIN public.organizations AS organizations
              ON organizations.id = users.organization_id
            JOIN public.us_lacey_organization_profiles AS profiles
              ON profiles.organization_id = users.organization_id
            WHERE sessions.token_hash = requested_token_hash
              AND sessions.revoked_at IS NULL
              AND sessions.expires_at > now()
              AND users.is_active
              AND organizations.is_active
              AND profiles.account_status IN ('PAYMENT_PENDING','PILOT','ACTIVE')
              AND (
                  NOT organizations.is_sandbox
                  OR (
                      organizations.expires_at IS NOT NULL
                      AND organizations.expires_at > now()
                  )
              )
            LIMIT 1
        $$;
        """
    )


def _create_sandbox_provision_function() -> None:
    op.execute(
        """
        CREATE FUNCTION public.us_lacey_sandbox_provision(
            requested_password_hash text,
            requested_token_hash text,
            requested_family_id text,
            requested_ttl_minutes integer,
            requested_ip text,
            requested_user_agent text
        )
        RETURNS TABLE (
            organization_id integer,
            user_id integer,
            expires_at timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            nonce text := replace(gen_random_uuid()::text, '-', '');
            sandbox_email text;
            sandbox_expiry timestamptz;
            new_org_id integer;
            new_user_id integer;
        BEGIN
            IF requested_password_hash IS NULL
               OR char_length(btrim(requested_password_hash)) < 40 THEN
                RAISE EXCEPTION 'invalid sandbox principal hash'
                    USING ERRCODE = '22023';
            END IF;
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
            IF requested_ttl_minutes IS NULL
               OR requested_ttl_minutes < 15
               OR requested_ttl_minutes > 240 THEN
                RAISE EXCEPTION 'sandbox TTL must be between 15 and 240 minutes'
                    USING ERRCODE = '22023';
            END IF;

            sandbox_expiry := now() + make_interval(mins => requested_ttl_minutes);
            sandbox_email := 'sandbox-' || nonce || '@sandbox.invalid';

            INSERT INTO public.organizations (
                name,
                slug,
                tax_id,
                tier,
                description,
                is_active,
                is_sandbox,
                expires_at,
                created_at,
                updated_at
            ) VALUES (
                'Litoral Trace Sandbox ' || upper(substr(nonce, 1, 8)),
                'sandbox-' || substr(nonce, 1, 24),
                NULL,
                'sandbox',
                'Ephemeral zero-touch U.S. Lacey sandbox tenant',
                TRUE,
                TRUE,
                sandbox_expiry,
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
                'Sandbox visitor',
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
                'Anonymous Customs Broker Sandbox',
                'US',
                'CUSTOMS_BROKER',
                'Sandbox visitor',
                NULL,
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
                sandbox_expiry,
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
                sandbox_expiry,
                NULLIF(left(btrim(coalesce(requested_ip, '')), 45), ''),
                NULLIF(left(btrim(coalesce(requested_user_agent, '')), 512), '')
            );

            RETURN QUERY
            SELECT new_org_id, new_user_id, sandbox_expiry;
        END;
        $$;
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {SANDBOX_FUNCTION} FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SANDBOX_FUNCTION} TO {RUNTIME_ROLE}")


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
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        "ck_organizations_sandbox_requires_expiry",
        "organizations",
        "NOT is_sandbox OR expires_at IS NOT NULL",
    )
    op.create_index(
        "ix_organizations_sandbox_expires",
        "organizations",
        ["is_sandbox", "expires_at"],
    )

    op.drop_constraint(
        "ck_us_lacey_subscriptions_price_positive",
        "us_lacey_subscriptions",
        type_="check",
    )
    op.create_check_constraint(
        "ck_us_lacey_subscriptions_price_nonnegative",
        "us_lacey_subscriptions",
        "price_cents >= 0",
    )

    _grant_platform_role()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")
    _replace_portal_auth_functions()
    _create_sandbox_provision_function()
    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_platform_role()


def downgrade() -> None:
    _grant_platform_role()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")

    op.execute(f"DROP FUNCTION IF EXISTS {SANDBOX_FUNCTION}")

    # Restore pre-sandbox lookup semantics.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.us_lacey_portal_login_lookup(
            requested_email text
        )
        RETURNS TABLE (
            user_id integer,
            organization_id integer,
            password_hash text,
            user_is_active boolean,
            organization_is_active boolean,
            account_status text
        )
        LANGUAGE sql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
            SELECT
                users.id,
                users.organization_id,
                users.password_hash,
                users.is_active,
                organizations.is_active,
                profiles.account_status
            FROM public.users AS users
            JOIN public.organizations AS organizations
              ON organizations.id = users.organization_id
            JOIN public.us_lacey_organization_profiles AS profiles
              ON profiles.organization_id = users.organization_id
            WHERE users.username = lower(btrim(requested_email))
              AND users.email = lower(btrim(requested_email))
            LIMIT 1
        $$;
        """
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.us_lacey_portal_session_lookup(
            requested_token_hash text
        )
        RETURNS TABLE (
            session_id integer,
            user_id integer,
            organization_id integer,
            email text,
            full_name text,
            legal_name text,
            business_type text,
            account_status text,
            expires_at timestamptz
        )
        LANGUAGE sql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
            SELECT
                sessions.id,
                users.id,
                users.organization_id,
                users.email,
                coalesce(users.full_name, users.email),
                profiles.legal_name,
                profiles.business_type,
                profiles.account_status,
                sessions.expires_at
            FROM public.user_sessions AS sessions
            JOIN public.users AS users
              ON users.id = sessions.user_id
             AND users.organization_id = sessions.organization_id
            JOIN public.organizations AS organizations
              ON organizations.id = users.organization_id
            JOIN public.us_lacey_organization_profiles AS profiles
              ON profiles.organization_id = users.organization_id
            WHERE sessions.token_hash = requested_token_hash
              AND sessions.revoked_at IS NULL
              AND sessions.expires_at > now()
              AND users.is_active
              AND organizations.is_active
              AND profiles.account_status IN ('PAYMENT_PENDING','PILOT','ACTIVE')
            LIMIT 1
        $$;
        """
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.us_lacey_portal_create_session(
            requested_user_id integer,
            requested_organization_id integer,
            requested_token_hash text,
            requested_family_id text,
            requested_expires_at timestamptz,
            requested_ip text,
            requested_user_agent text
        )
        RETURNS TABLE (session_id integer)
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            new_session_id integer;
        BEGIN
            IF requested_user_id IS NULL OR requested_user_id <= 0
               OR requested_organization_id IS NULL OR requested_organization_id <= 0 THEN
                RAISE EXCEPTION 'invalid portal identity' USING ERRCODE = '22023';
            END IF;
            IF requested_token_hash IS NULL
               OR requested_token_hash !~ '^[0-9a-f]{64}$' THEN
                RAISE EXCEPTION 'invalid portal session token' USING ERRCODE = '22023';
            END IF;
            IF btrim(coalesce(requested_family_id, '')) = ''
               OR char_length(requested_family_id) > 36 THEN
                RAISE EXCEPTION 'invalid portal session family' USING ERRCODE = '22023';
            END IF;
            IF requested_expires_at IS NULL
               OR requested_expires_at <= now()
               OR requested_expires_at > now() + interval '31 days' THEN
                RAISE EXCEPTION 'invalid portal session expiry' USING ERRCODE = '22023';
            END IF;

            IF NOT EXISTS (
                SELECT 1
                FROM public.users AS users
                JOIN public.organizations AS organizations
                  ON organizations.id = users.organization_id
                JOIN public.us_lacey_organization_profiles AS profiles
                  ON profiles.organization_id = users.organization_id
                WHERE users.id = requested_user_id
                  AND users.organization_id = requested_organization_id
                  AND users.is_active
                  AND organizations.is_active
                  AND profiles.account_status IN ('PAYMENT_PENDING','PILOT','ACTIVE')
            ) THEN
                RAISE EXCEPTION 'portal account unavailable' USING ERRCODE = '28000';
            END IF;

            INSERT INTO public.user_sessions (
                user_id, organization_id, family_id, token_hash, issued_at,
                expires_at, created_ip, user_agent
            ) VALUES (
                requested_user_id, requested_organization_id, requested_family_id,
                requested_token_hash, now(), requested_expires_at,
                NULLIF(left(btrim(coalesce(requested_ip, '')), 45), ''),
                NULLIF(left(btrim(coalesce(requested_user_agent, '')), 512), '')
            )
            RETURNING id INTO new_session_id;

            RETURN QUERY SELECT new_session_id;
        END;
        $$;
        """
    )

    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_platform_role()

    op.drop_constraint(
        "ck_us_lacey_subscriptions_price_nonnegative",
        "us_lacey_subscriptions",
        type_="check",
    )
    op.create_check_constraint(
        "ck_us_lacey_subscriptions_price_positive",
        "us_lacey_subscriptions",
        "price_cents > 0",
    )

    op.drop_index("ix_organizations_sandbox_expires", table_name="organizations")
    op.drop_constraint(
        "ck_organizations_sandbox_requires_expiry",
        "organizations",
        type_="check",
    )
    op.drop_column("organizations", "expires_at")
    op.drop_column("organizations", "is_sandbox")
