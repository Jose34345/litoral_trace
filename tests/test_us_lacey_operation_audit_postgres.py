from __future__ import annotations

import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from litoral_trace.config.settings import normalize_database_url


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
OWNER_URL = (
    os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")
    or os.environ.get("MIGRATION_DATABASE_URL")
)
pytestmark = pytest.mark.skipif(
    not (ENABLED and RUNTIME_URL and OWNER_URL),
    reason="Operation audit RLS acceptance requires runtime and owner URLs.",
)


def _engine(url: str):
    return create_engine(normalize_database_url(url), pool_pre_ping=True)


def _tenant(conn, organization_id: int) -> None:
    conn.execute(
        text(
            "SELECT set_config("
            "'app.current_organization_id', :organization_id, true)"
        ),
        {"organization_id": str(organization_id)},
    )


@pytest.fixture()
def fixture():
    suffix = uuid4().hex[:9]
    owner = _engine(OWNER_URL)
    runtime = _engine(RUNTIME_URL)
    values: dict[str, int] = {}
    with owner.begin() as conn:
        for label in ("a", "b"):
            org_id = int(
                conn.execute(
                    text(
                        "SELECT nextval("
                        "pg_get_serial_sequence('public.organizations', 'id'))"
                    )
                ).scalar_one()
            )
            _tenant(conn, org_id)
            conn.execute(
                text(
                    """
                    INSERT INTO organizations (
                        id, name, slug, tax_id, tier, description, is_active
                    ) VALUES (
                        :id, :name, :slug, :tax_id, 'pro',
                        'Operation audit RLS acceptance', true
                    )
                    """
                ),
                {
                    "id": org_id,
                    "name": f"Audit Tenant {label.upper()} {suffix}",
                    "slug": f"audit-tenant-{label}-{suffix}",
                    "tax_id": f"AUDIT-{label.upper()}-{suffix}",
                },
            )
            operation_id = int(
                conn.execute(
                    text(
                        """
                        INSERT INTO us_lacey_operations (
                            organization_id, client_reference, status,
                            document_count, merchandise_line_count
                        ) VALUES (
                            :org, :reference, 'NEW', 0, 0
                        )
                        RETURNING id
                        """
                    ),
                    {"org": org_id, "reference": f"AUDIT-{label}-{suffix}"},
                ).scalar_one()
            )
            values[f"org_{label}"] = org_id
            values[f"operation_{label}"] = operation_id

    with runtime.begin() as conn:
        _tenant(conn, values["org_b"])
        conn.execute(
            text(
                """
                INSERT INTO us_lacey_operation_events (
                    public_id, organization_id, operation_id, actor_type,
                    actor_identity, event_type, event_key, details
                ) VALUES (
                    :public_id, :org, :operation_id, 'SYSTEM',
                    'Litoral Trace', 'CREATED', 'fixture:b', '{}'::jsonb
                )
                """
            ),
            {
                "public_id": uuid4(),
                "org": values["org_b"],
                "operation_id": values["operation_b"],
            },
        )

    yield {**values, "owner": owner, "runtime": runtime}

    with owner.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM us_lacey_operations "
                "WHERE organization_id IN (:org_a, :org_b)"
            ),
            {"org_a": values["org_a"], "org_b": values["org_b"]},
        )
        conn.execute(
            text(
                "DELETE FROM organizations WHERE id IN (:org_a, :org_b)"
            ),
            {"org_a": values["org_a"], "org_b": values["org_b"]},
        )
    runtime.dispose()
    owner.dispose()


def test_operation_audit_is_tenant_scoped_and_append_only(fixture):
    with fixture["runtime"].begin() as conn:
        _tenant(conn, fixture["org_a"])
        event_id = int(
            conn.execute(
                text(
                    """
                    INSERT INTO us_lacey_operation_events (
                        public_id, organization_id, operation_id, actor_type,
                        actor_identity, event_type, event_key, details
                    ) VALUES (
                        :public_id, :org, :operation_id, 'USER',
                        'auditor@example.com', 'HUMAN_REVIEW',
                        'fixture:a', '{"action":"accept"}'::jsonb
                    )
                    RETURNING id
                    """
                ),
                {
                    "public_id": uuid4(),
                    "org": fixture["org_a"],
                    "operation_id": fixture["operation_a"],
                },
            ).scalar_one()
        )
        rows = conn.execute(
            text(
                "SELECT id, organization_id "
                "FROM us_lacey_operation_events ORDER BY id"
            )
        ).fetchall()
        assert rows
        assert {row.organization_id for row in rows} == {fixture["org_a"]}
        assert event_id in {row.id for row in rows}

    with pytest.raises(DBAPIError):
        with fixture["runtime"].begin() as conn:
            _tenant(conn, fixture["org_a"])
            conn.execute(
                text(
                    """
                    INSERT INTO us_lacey_operation_events (
                        public_id, organization_id, operation_id, actor_type,
                        actor_identity, event_type, details
                    ) VALUES (
                        :public_id, :org_b, :operation_b, 'SYSTEM',
                        'Litoral Trace', 'EXTRACTED', '{}'::jsonb
                    )
                    """
                ),
                {
                    "public_id": uuid4(),
                    "org_b": fixture["org_b"],
                    "operation_b": fixture["operation_b"],
                },
            )

    for statement in (
        "UPDATE us_lacey_operation_events "
        "SET actor_identity='tampered' WHERE id=:event_id",
        "DELETE FROM us_lacey_operation_events WHERE id=:event_id",
    ):
        with pytest.raises(DBAPIError):
            with fixture["runtime"].begin() as conn:
                _tenant(conn, fixture["org_a"])
                conn.execute(text(statement), {"event_id": event_id})

    with fixture["owner"].connect() as conn:
        privileges = conn.execute(
            text(
                """
                SELECT
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.us_lacey_operation_events',
                        'UPDATE'
                    ) AS can_update,
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.us_lacey_operation_events',
                        'DELETE'
                    ) AS can_delete
                """
            )
        ).mappings().one()
    assert privileges["can_update"] is False
    assert privileges["can_delete"] is False
