from __future__ import annotations

from datetime import datetime, timedelta, timezone
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
WORKER_URL = os.environ.get("US_LACEY_WORKER_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not (ENABLED and RUNTIME_URL and OWNER_URL and WORKER_URL),
    reason="Five-shipment evaluation acceptance requires runtime/owner/worker PostgreSQL URLs.",
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
def evaluation_fixture():
    suffix = uuid4().hex[:9]
    owner = _engine(OWNER_URL)
    runtime = _engine(RUNTIME_URL)
    worker = _engine(WORKER_URL)
    values: dict[str, object] = {}

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
                        id, name, slug, tax_id, tier, description, is_active,
                        is_sandbox, sandbox_expires_at
                    ) VALUES (
                        :id, :name, :slug, :tax_id, 'pro',
                        'Product-led evaluation acceptance', true,
                        false, null
                    )
                    """
                ),
                {
                    "id": org_id,
                    "name": f"Evaluation Tenant {label.upper()} {suffix}",
                    "slug": f"evaluation-{label}-{suffix}",
                    "tax_id": f"EVAL-{label.upper()}-{suffix}",
                },
            )
            _tenant(conn, org_id)
            conn.execute(
                text(
                    """
                    INSERT INTO us_lacey_subscriptions (
                        public_id, organization_id, plan_code, currency,
                        price_cents, monthly_operation_limit, used_operations,
                        status, started_at, renews_at, billing_provider
                    ) VALUES (
                        :public_id, :org, 'EVALUATION', 'USD',
                        0, 5, 0, 'ACTIVE', now(),
                        now() + interval '7 days', 'NONE'
                    )
                    """
                ),
                {"public_id": uuid4(), "org": org_id},
            )
            _tenant(conn, org_id)
            conn.execute(
                text(
                    """
                    INSERT INTO us_lacey_evaluations (
                        organization_id, status, work_email, operation_limit,
                        successful_operations_used, claimed_at,
                        last_activity_at, inactive_expires_at,
                        raw_retention_hours
                    ) VALUES (
                        :org, 'ACTIVE', :email, 5, 0, now(), now(),
                        now() + interval '7 days', 4
                    )
                    """
                ),
                {
                    "org": org_id,
                    "email": f"buyer-{label}-{suffix}@example-lumber.com",
                },
            )
            values[f"org_{label}"] = org_id

        operation_ids = []
        _tenant(conn, int(values["org_a"]))
        for index in range(1, 6):
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
                    {
                        "org": values["org_a"],
                        "reference": f"EVAL-{suffix}-{index}",
                    },
                ).scalar_one()
            )
            operation_ids.append(operation_id)
        values["operation_ids"] = tuple(operation_ids)

    yield {**values, "owner": owner, "runtime": runtime, "worker": worker}

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
                "DELETE FROM organizations "
                "WHERE id IN (:org_a, :org_b)"
            ),
            {"org_a": values["org_a"], "org_b": values["org_b"]},
        )
    worker.dispose()
    runtime.dispose()
    owner.dispose()


def test_evaluation_rls_and_privileges_are_fail_closed(evaluation_fixture):
    org_a = int(evaluation_fixture["org_a"])
    org_b = int(evaluation_fixture["org_b"])
    runtime = evaluation_fixture["runtime"]
    owner = evaluation_fixture["owner"]

    with runtime.begin() as conn:
        _tenant(conn, org_a)
        visible = conn.execute(
            text(
                """
                SELECT organization_id, status, operation_limit,
                       successful_operations_used, raw_retention_hours
                FROM public.us_lacey_evaluations
                """
            )
        ).mappings().all()
        assert len(visible) == 1
        assert visible[0]["organization_id"] == org_a
        assert visible[0]["operation_limit"] == 5
        assert visible[0]["raw_retention_hours"] == 4

        hidden = conn.execute(
            text(
                "SELECT count(*) FROM public.us_lacey_evaluations "
                "WHERE organization_id=:org_b"
            ),
            {"org_b": org_b},
        ).scalar_one()
        assert hidden == 0

    for statement in (
        "INSERT INTO public.us_lacey_evaluations "
        "(organization_id,status,operation_limit,successful_operations_used,"
        "last_activity_at,raw_retention_hours) "
        "VALUES (:org,'ANONYMOUS',5,0,now(),4)",
        "UPDATE public.us_lacey_evaluations "
        "SET successful_operations_used=5 WHERE organization_id=:org",
        "DELETE FROM public.us_lacey_evaluations WHERE organization_id=:org",
    ):
        with pytest.raises(DBAPIError):
            with runtime.begin() as conn:
                _tenant(conn, org_a)
                conn.execute(text(statement), {"org": org_a})

    with owner.connect() as conn:
        privileges = conn.execute(
            text(
                """
                SELECT
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.us_lacey_evaluations',
                        'SELECT'
                    ) AS runtime_select,
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.us_lacey_evaluations',
                        'INSERT'
                    ) AS runtime_insert,
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.us_lacey_evaluations',
                        'UPDATE'
                    ) AS runtime_update,
                    has_table_privilege(
                        'litoral_trace_app',
                        'public.us_lacey_evaluations',
                        'DELETE'
                    ) AS runtime_delete,
                    has_function_privilege(
                        'litoral_trace_app',
                        'public.us_lacey_evaluation_claim(text,text)',
                        'EXECUTE'
                    ) AS runtime_claim,
                    has_function_privilege(
                        'litoral_trace_app',
                        'public.us_lacey_evaluation_mark_operation_success(integer,integer)',
                        'EXECUTE'
                    ) AS runtime_count,
                    has_function_privilege(
                        'litoral_trace_worker_executor',
                        'public.us_lacey_evaluation_mark_operation_success(integer,integer)',
                        'EXECUTE'
                    ) AS worker_count
                """
            )
        ).mappings().one()

    assert privileges == {
        "runtime_select": True,
        "runtime_insert": False,
        "runtime_update": False,
        "runtime_delete": False,
        "runtime_claim": True,
        "runtime_count": False,
        "worker_count": True,
    }


def test_only_successful_distinct_operations_consume_five_shipment_evaluation(
    evaluation_fixture,
):
    org_id = int(evaluation_fixture["org_a"])
    operation_ids = tuple(int(v) for v in evaluation_fixture["operation_ids"])
    owner = evaluation_fixture["owner"]

    def state(conn):
        return conn.execute(
            text(
                """
                SELECT status, successful_operations_used
                FROM public.us_lacey_evaluations
                WHERE organization_id=:org
                """
            ),
            {"org": org_id},
        ).mappings().one()

    with owner.begin() as conn:
        # A failure is explicitly free.
        conn.execute(
            text(
                "UPDATE us_lacey_operations SET status='FAILED' "
                "WHERE organization_id=:org AND id=:op"
            ),
            {"org": org_id, "op": operation_ids[0]},
        )
        assert state(conn)["successful_operations_used"] == 0

        # The same operation becomes successful and consumes exactly one slot.
        conn.execute(
            text(
                "UPDATE us_lacey_operations SET status='REVIEW_REQUIRED' "
                "WHERE organization_id=:org AND id=:op"
            ),
            {"org": org_id, "op": operation_ids[0]},
        )
        assert state(conn)["successful_operations_used"] == 1

        # Reprocessing and reaching another successful state does not consume again.
        conn.execute(
            text(
                "UPDATE us_lacey_operations SET status='PROCESSING' "
                "WHERE organization_id=:org AND id=:op"
            ),
            {"org": org_id, "op": operation_ids[0]},
        )
        conn.execute(
            text(
                "UPDATE us_lacey_operations SET status='READY_FOR_REVIEW' "
                "WHERE organization_id=:org AND id=:op"
            ),
            {"org": org_id, "op": operation_ids[0]},
        )
        assert state(conn)["successful_operations_used"] == 1

        for op_id in operation_ids[1:]:
            conn.execute(
                text(
                    "UPDATE us_lacey_operations SET status='COMPLETED' "
                    "WHERE organization_id=:org AND id=:op"
                ),
                {"org": org_id, "op": op_id},
            )

        final = state(conn)
        subscription = conn.execute(
            text(
                """
                SELECT plan_code, price_cents, monthly_operation_limit,
                       used_operations
                FROM public.us_lacey_subscriptions
                WHERE organization_id=:org
                """
            ),
            {"org": org_id},
        ).mappings().one()
        counted_rows = conn.execute(
            text(
                "SELECT count(*) FROM public.us_lacey_evaluation_operations "
                "WHERE organization_id=:org"
            ),
            {"org": org_id},
        ).scalar_one()

    assert final["successful_operations_used"] == 5
    assert final["status"] == "EXHAUSTED"
    assert counted_rows == 5
    assert subscription == {
        "plan_code": "EVALUATION",
        "price_cents": 0,
        "monthly_operation_limit": 5,
        "used_operations": 5,
    }


def test_security_definer_ownership_and_function_separation(evaluation_fixture):
    owner = evaluation_fixture["owner"]
    with owner.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT p.proname, p.prosecdef, pg_get_userbyid(p.proowner) AS owner
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid=p.pronamespace
                WHERE n.nspname='public'
                  AND p.proname IN (
                    'us_lacey_evaluation_claim',
                    'us_lacey_evaluation_touch',
                    'us_lacey_evaluation_mark_operation_success',
                    'us_lacey_evaluation_raw_purge_claim',
                    'us_lacey_outreach_record_pre_sandbox_event'
                  )
                ORDER BY p.proname
                """
            )
        ).mappings().all()

    assert len(rows) == 5
    assert all(row["prosecdef"] is True for row in rows)
    assert all(row["owner"] == "litoral_trace_platform_definer" for row in rows)
