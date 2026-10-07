from __future__ import annotations

import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1"
    or not os.environ.get("US_LACEY_POSTGRES_TEST_DATABASE_URL"),
    reason="requires isolated U.S. PostgreSQL root credentials",
)


def _root_engine():
    return create_engine(
        os.environ["US_LACEY_POSTGRES_TEST_DATABASE_URL"],
        pool_pre_ping=True,
        hide_parameters=True,
    )


def test_outreach_attribution_capabilities_are_runtime_safe_and_durable():
    engine = _root_engine()
    connection = engine.connect()
    transaction = connection.begin()
    try:
        slug = f"gate-{uuid4().hex[:12]}"
        connection.execute(
            text(
                """
                INSERT INTO public.us_lacey_outreach_links(
                    slug,
                    prospect_label,
                    campaign_code,
                    source,
                    active,
                    click_count
                ) VALUES (
                    :slug,
                    'Attribution Gate Prospect',
                    'gate-sep26',
                    'direct_outreach',
                    true,
                    0
                )
                """
            ),
            {"slug": slug},
        )

        runtime_privileges = connection.execute(
            text(
                """
                SELECT
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.us_lacey_outreach_links',
                        'SELECT'
                    ) AS links_select,
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.us_lacey_outreach_sessions',
                        'SELECT'
                    ) AS sessions_select,
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.us_lacey_outreach_events',
                        'SELECT'
                    ) AS events_select
                """
            )
        ).mappings().one()
        assert runtime_privileges == {
            "links_select": False,
            "sessions_select": False,
            "events_select": False,
        }

        token_hash = "a" * 64
        family_id = str(uuid4())

        connection.execute(text("SET LOCAL ROLE litoral_trace_app"))
        sandbox = connection.execute(
            text(
                """
                SELECT *
                FROM public.us_lacey_sandbox_provision(
                    :token_hash,
                    :family_id,
                    :client_ip,
                    :user_agent,
                    :password_hash
                )
                """
            ),
            {
                "token_hash": token_hash,
                "family_id": family_id,
                "client_ip": "203.0.113.77",
                "user_agent": "outreach-attribution-postgres-gate",
                "password_hash": "$2b$12$" + ("x" * 53),
            },
        ).mappings().one()

        outreach = connection.execute(
            text("SELECT * FROM public.us_lacey_outreach_open(:slug)"),
            {"slug": slug},
        ).mappings().one()

        human_visit = connection.execute(
            text(
                """
                SELECT public.us_lacey_outreach_record_pre_sandbox_event(
                    :attribution_session_id,
                    'HUMAN_VISIT',
                    'browser-visible',
                    '{}'::jsonb
                )
                """
            ),
            {"attribution_session_id": outreach["attribution_session_id"]},
        ).scalar_one()
        assert human_visit is True

        bound = connection.execute(
            text(
                """
                SELECT public.us_lacey_outreach_bind_sandbox(
                    :token_hash,
                    :organization_id,
                    :attribution_session_id
                )
                """
            ),
            {
                "token_hash": token_hash,
                "organization_id": sandbox["organization_id"],
                "attribution_session_id": outreach["attribution_session_id"],
            },
        ).scalar_one()
        assert bound is True

        first = connection.execute(
            text(
                """
                SELECT public.us_lacey_outreach_record_event(
                    :token_hash,
                    :organization_id,
                    'OPERATION_CREATED',
                    'operation-gate',
                    '{"source":"postgres-gate"}'::jsonb
                )
                """
            ),
            {
                "token_hash": token_hash,
                "organization_id": sandbox["organization_id"],
            },
        ).scalar_one()
        second = connection.execute(
            text(
                """
                SELECT public.us_lacey_outreach_record_event(
                    :token_hash,
                    :organization_id,
                    'OPERATION_CREATED',
                    'operation-gate',
                    '{"source":"postgres-gate"}'::jsonb
                )
                """
            ),
            {
                "token_hash": token_hash,
                "organization_id": sandbox["organization_id"],
            },
        ).scalar_one()
        assert first is True
        assert second is True
        connection.execute(text("RESET ROLE"))

        stored = connection.execute(
            text(
                """
                SELECT
                    link.click_count,
                    session.sandbox_organization_id,
                    org.sandbox_attribution_session_id,
                    count(event.id) FILTER (
                        WHERE event.event_name = 'LINK_SCANNED'
                    ) AS link_scanned,
                    count(event.id) FILTER (
                        WHERE event.event_name = 'HUMAN_VISIT'
                    ) AS human_visit,
                    count(event.id) FILTER (
                        WHERE event.event_name = 'SANDBOX_STARTED'
                    ) AS sandbox_started,
                    count(event.id) FILTER (
                        WHERE event.event_name = 'SANDBOX_ENGAGED'
                    ) AS sandbox_engaged,
                    count(event.id) FILTER (
                        WHERE event.event_name = 'OPERATION_CREATED'
                    ) AS operations_created
                FROM public.us_lacey_outreach_links AS link
                JOIN public.us_lacey_outreach_sessions AS session
                  ON session.outreach_link_id = link.id
                JOIN public.organizations AS org
                  ON org.id = session.sandbox_organization_id
                LEFT JOIN public.us_lacey_outreach_events AS event
                  ON event.session_id = session.id
                WHERE link.slug = :slug
                GROUP BY
                    link.click_count,
                    session.sandbox_organization_id,
                    org.sandbox_attribution_session_id
                """
            ),
            {"slug": slug},
        ).mappings().one()

        assert stored["click_count"] == 1
        assert stored["sandbox_organization_id"] == sandbox["organization_id"]
        assert stored["sandbox_attribution_session_id"] == outreach[
            "attribution_session_id"
        ]
        assert stored["link_scanned"] == 1
        assert stored["human_visit"] == 1
        assert stored["sandbox_started"] == 1
        assert stored["sandbox_engaged"] == 1
        assert stored["operations_created"] == 1
    finally:
        transaction.rollback()
        connection.close()
        engine.dispose()


def test_platform_admin_outreach_link_creation_writes_tenant_scoped_audit_log():
    engine = _root_engine()
    connection = engine.connect()
    transaction = connection.begin()
    try:
        suffix = uuid4().hex[:12]
        token_hash = uuid4().hex + uuid4().hex
        org_id = connection.execute(
            text(
                """
                INSERT INTO public.organizations(
                    name, slug, tax_id, tier, description,
                    is_active, created_at, updated_at
                ) VALUES (
                    :name, :slug, :tax_id, 'enterprise',
                    'outreach audit regression actor',
                    true, now(), now()
                )
                RETURNING id
                """
            ),
            {
                "name": f"Outreach Audit Gate {suffix}",
                "slug": f"outreach-audit-{suffix}",
                "tax_id": f"OUTREACH-AUDIT-{suffix}",
            },
        ).scalar_one()
        user_id = connection.execute(
            text(
                """
                INSERT INTO public.users(
                    organization_id, email, username, password_hash, role,
                    full_name, is_active, created_at, updated_at
                ) VALUES (
                    :org_id, :email, :username,
                    :password_hash, 'superadmin',
                    'Outreach Audit Gate', true, now(), now()
                )
                RETURNING id
                """
            ),
            {
                "org_id": org_id,
                "email": f"outreach-audit-{suffix}@example.com",
                "username": f"outreach_audit_{suffix}",
                "password_hash": "$2b$12$" + ("x" * 53),
            },
        ).scalar_one()
        connection.execute(
            text(
                """
                INSERT INTO public.user_sessions(
                    user_id, organization_id, family_id, token_hash,
                    issued_at, expires_at, revoked_at, replaced_by_session_id,
                    created_ip, user_agent, created_at, updated_at
                ) VALUES (
                    :user_id, :org_id, :family_id, :token_hash,
                    now(), now() + interval '1 hour', null, null,
                    '127.0.0.1', 'outreach-audit-regression', now(), now()
                )
                """
            ),
            {
                "user_id": user_id,
                "org_id": org_id,
                "family_id": str(uuid4()),
                "token_hash": token_hash,
            },
        )

        slug = f"outreach-audit-{suffix}"
        connection.execute(text("SET LOCAL ROLE litoral_trace_app"))
        created = connection.execute(
            text(
                """
                SELECT *
                FROM public.platform_admin_create_outreach_link(
                    :token_hash,
                    :slug,
                    'Outreach Audit Prospect',
                    'outreach-audit-regression',
                    'direct_outreach'
                )
                """
            ),
            {"token_hash": token_hash, "slug": slug},
        ).mappings().one()
        connection.execute(text("RESET ROLE"))

        audit = connection.execute(
            text(
                """
                SELECT organization_id, user_id, action, entity_type, entity_id
                FROM public.audit_logs
                WHERE action = 'OUTREACH_LINK_CREATED'
                  AND entity_type = 'us_lacey_outreach_link'
                  AND entity_id = :entity_id
                ORDER BY id DESC
                LIMIT 1
                """
            ),
            {"entity_id": created["outreach_link_id"]},
        ).mappings().one()

        assert audit["organization_id"] == org_id
        assert audit["user_id"] == user_id
        assert audit["action"] == "OUTREACH_LINK_CREATED"
        assert audit["entity_type"] == "us_lacey_outreach_link"
        assert audit["entity_id"] == created["outreach_link_id"]
    finally:
        transaction.rollback()
        connection.close()
        engine.dispose()
