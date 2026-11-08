"""Fix outreach link audit signature and tenant scope.

Revision ID: 067_us_lacey_outreach_audit_hotfix
Revises: 066_us_lacey_debug_retention
"""
from __future__ import annotations

from alembic import op


revision = "067_us_lacey_outreach_audit_hotfix"
down_revision = "066_us_lacey_debug_retention"
branch_labels = None
depends_on = None

PLATFORM_ROLE = "litoral_trace_platform_definer"


def _grant_temp_platform_set() -> None:
    op.execute(
        f"GRANT {PLATFORM_ROLE} TO CURRENT_USER "
        "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE GRANTED BY CURRENT_USER"
    )


def _revoke_temp_platform_set() -> None:
    op.execute(
        f"REVOKE {PLATFORM_ROLE} FROM CURRENT_USER GRANTED BY CURRENT_USER"
    )


def _replace_create_link_function(*, fixed: bool) -> None:
    target_org = (
        "actor.actor_organization_id"
        if fixed
        else "NULL"
    )
    entity_id = "new_link.id::integer" if fixed else "new_link.id"

    _grant_temp_platform_set()
    op.execute(f"GRANT CREATE ON SCHEMA public TO {PLATFORM_ROLE}")
    op.execute(f"SET LOCAL ROLE {PLATFORM_ROLE}")

    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.platform_admin_create_outreach_link(
            actor_refresh_token_hash text,
            requested_slug text,
            requested_prospect_label text,
            requested_campaign_code text,
            requested_source text
        )
        RETURNS TABLE(
            outreach_link_id bigint,
            slug text,
            prospect_label text,
            campaign_code text,
            source text,
            active boolean,
            created_at timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public, pg_temp
        AS $$
        DECLARE
            actor record;
            new_link record;
        BEGIN
            SELECT * INTO actor
            FROM public._platform_superadmin_session_actor(
                actor_refresh_token_hash
            );

            IF requested_slug IS NULL
               OR requested_slug !~ '^[a-z0-9][a-z0-9-]{{2,95}}$'
               OR btrim(coalesce(requested_prospect_label, '')) = ''
               OR btrim(coalesce(requested_campaign_code, '')) = ''
               OR btrim(coalesce(requested_source, '')) = '' THEN
                RAISE EXCEPTION 'invalid outreach link request'
                    USING ERRCODE = '22023';
            END IF;

            INSERT INTO public.us_lacey_outreach_links(
                slug,
                prospect_label,
                campaign_code,
                source,
                active,
                click_count,
                created_at
            ) VALUES (
                requested_slug,
                left(btrim(requested_prospect_label), 160),
                left(btrim(requested_campaign_code), 96),
                left(lower(btrim(requested_source)), 48),
                true,
                0,
                now()
            )
            RETURNING * INTO new_link;

            PERFORM public._platform_insert_audit_log(
                actor.actor_user_id,
                NULL::text,
                'superadmin'::text,
                actor.actor_organization_id,
                {target_org},
                'OUTREACH_LINK_CREATED'::text,
                'us_lacey_outreach_link'::text,
                {entity_id},
                jsonb_build_object(
                    'slug', new_link.slug,
                    'prospect_label', new_link.prospect_label,
                    'campaign_code', new_link.campaign_code,
                    'source', new_link.source
                )
            );

            RETURN QUERY
            SELECT
                new_link.id,
                new_link.slug::text,
                new_link.prospect_label::text,
                new_link.campaign_code::text,
                new_link.source::text,
                new_link.active,
                new_link.created_at;
        END;
        $$;
        """
    )

    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA public FROM {PLATFORM_ROLE}")
    _revoke_temp_platform_set()


def upgrade() -> None:
    _replace_create_link_function(fixed=True)


def downgrade() -> None:
    _replace_create_link_function(fixed=False)
