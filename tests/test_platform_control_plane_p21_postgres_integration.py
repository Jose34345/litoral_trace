from __future__ import annotations

import asyncio
import json
import os
from contextlib import contextmanager
from http.cookies import SimpleCookie
from uuid import uuid4

import pytest
from fastapi import HTTPException, Response
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from litoral_trace.api.auth import LoginRequest, get_current_tenant_user, login_b2b
from litoral_trace.api.lotes import listar_lotes_tenant
from litoral_trace.api.settings import consultar_licencia_tenant
from litoral_trace.api.admin import require_superadmin_role
from litoral_trace.auth.passwords import hash_password
from litoral_trace.auth.sessions import hash_refresh_token
from litoral_trace.auth.tokens import create_jwt_token
from litoral_trace.config.settings import normalize_database_url
from litoral_trace.db.engine import reset_engine_state
from litoral_trace.services.admin import (
    alternar_estado_empresa,
    crear_nueva_empresa_cliente,
    listar_empresas_superadmin,
    upsert_license_superadmin,
)


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


POSTGRES_RLS_TESTS_ENABLED = _truthy(os.environ.get("ENABLE_POSTGRES_TESTS"))
RUNTIME_TEST_DATABASE_URL = os.environ.get("TEST_POSTGRES_DATABASE_URL")
MIGRATION_TEST_DATABASE_URL = (
    os.environ.get("TEST_POSTGRES_MIGRATION_DATABASE_URL")
    or os.environ.get("MIGRATION_DATABASE_URL")
)

pytestmark = pytest.mark.skipif(
    not (
        POSTGRES_RLS_TESTS_ENABLED
        and RUNTIME_TEST_DATABASE_URL
        and MIGRATION_TEST_DATABASE_URL
    ),
    reason=(
        "PostgreSQL P2.1 tests require ENABLE_POSTGRES_TESTS=1, "
        "TEST_POSTGRES_DATABASE_URL y TEST_POSTGRES_MIGRATION_DATABASE_URL "
        "(o MIGRATION_DATABASE_URL)."
    ),
)


def _runtime_engine():
    return create_engine(
        normalize_database_url(RUNTIME_TEST_DATABASE_URL),
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )


def _owner_engine():
    return create_engine(
        normalize_database_url(MIGRATION_TEST_DATABASE_URL),
        pool_pre_ping=True,
    )


def _extract_cookies(response: Response) -> dict[str, str]:
    parsed_cookie = SimpleCookie()
    for set_cookie_header in response.headers.getlist("set-cookie"):
        parsed_cookie.load(set_cookie_header)
    return {
        cookie_name: morsel.value
        for cookie_name, morsel in parsed_cookie.items()
    }


def _assert_secret_values_absent(serialized_payload: str, *secrets: str) -> None:
    for secret in secrets:
        if secret and secret in serialized_payload:
            raise AssertionError("Platform audit leaked sensitive secret material.")


@contextmanager
def _failing_platform_audit_trigger(owner_engine):
    suffix = uuid4().hex[:8]
    function_name = f"fail_platform_audit_{suffix}"
    trigger_name = f"trg_fail_platform_audit_{suffix}"

    with owner_engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE FUNCTION public.{function_name}()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                BEGIN
                    IF NEW.action LIKE 'platform.%' THEN
                        RAISE EXCEPTION 'forced platform audit failure'
                            USING ERRCODE = 'P0001';
                    END IF;
                    RETURN NEW;
                END;
                $$;
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TRIGGER {trigger_name}
                BEFORE INSERT ON public.audit_logs
                FOR EACH ROW
                EXECUTE FUNCTION public.{function_name}()
                """
            )
        )

    try:
        yield
    finally:
        with owner_engine.begin() as conn:
            conn.execute(
                text(f"DROP TRIGGER IF EXISTS {trigger_name} ON public.audit_logs")
            )
            conn.execute(text(f"DROP FUNCTION IF EXISTS public.{function_name}()"))


@contextmanager
def _postgres_runtime_environment():
    original_values = {
        "ENVIRONMENT": os.environ.get("ENVIRONMENT"),
        "DATABASE_URL": os.environ.get("DATABASE_URL"),
        "MIGRATION_DATABASE_URL": os.environ.get("MIGRATION_DATABASE_URL"),
        "TEST_DATABASE_URL": os.environ.get("TEST_DATABASE_URL"),
    }

    os.environ["ENVIRONMENT"] = "development"
    os.environ["DATABASE_URL"] = RUNTIME_TEST_DATABASE_URL or ""
    os.environ["MIGRATION_DATABASE_URL"] = MIGRATION_TEST_DATABASE_URL or ""
    os.environ.pop("TEST_DATABASE_URL", None)
    reset_engine_state()

    try:
        yield
    finally:
        reset_engine_state()
        for variable_name, original_value in original_values.items():
            if original_value is None:
                os.environ.pop(variable_name, None)
            else:
                os.environ[variable_name] = original_value
        reset_engine_state()


def _login(username: str, password: str) -> tuple[object, dict[str, str]]:
    with _postgres_runtime_environment():
        response = Response()
        token_response = asyncio.run(
            login_b2b(
                LoginRequest(username=username, password=password),
                response,
            )
        )
    return token_response, _extract_cookies(response)


def _provision_control_plane_org(
    control_plane_fixture,
    *,
    refresh_token: str,
    name_prefix: str,
    tax_prefix: str,
    username_prefix: str,
    email_prefix: str,
    password_prefix: str,
    tier: str = "pro",
    monthly_lote_limit: int = 75,
    monthly_ton_limit: float = 5500.0,
    max_batch_rows: int = 550,
) -> tuple[dict[str, object], str]:
    suffix = uuid4().hex[:8]
    admin_password = f"{password_prefix}-{suffix}!"

    with _postgres_runtime_environment():
        created = crear_nueva_empresa_cliente(
            refresh_token=refresh_token,
            name=f"{name_prefix} {suffix}",
            tax_id=f"{tax_prefix}{suffix}",
            admin_email=f"{email_prefix}-{suffix}@example.com",
            admin_username=f"{username_prefix}_{suffix}",
            admin_password=admin_password,
            tier=tier,
            monthly_lote_limit=monthly_lote_limit,
            monthly_ton_limit=monthly_ton_limit,
            max_batch_rows=max_batch_rows,
        )

    control_plane_fixture["org_ids"].add(int(created["organization_id"]))
    return created, admin_password


@pytest.fixture(scope="module")
def control_plane_fixture():
    suffix = uuid4().hex[:10]
    runtime_engine = _runtime_engine()
    owner_engine = _owner_engine()

    fixture_state = {
        "runtime_engine": runtime_engine,
        "owner_engine": owner_engine,
        "org_ids": set(),
    }

    platform_password = f"P21-superadmin-{suffix}!"
    tenant_password = f"P21-tenant-admin-{suffix}!"

    with owner_engine.begin() as conn:
        fixture_state["platform_org_id"] = conn.execute(
            text(
                """
                INSERT INTO organizations (name, slug, tax_id, tier, description, is_active)
                VALUES (:name, :slug, :tax_id, 'enterprise', 'P2.1 platform org', true)
                RETURNING id
                """
            ),
            {
                "name": f"P21 Platform Org {suffix}",
                "slug": f"p21-platform-org-{suffix}",
                "tax_id": f"35-9{suffix[:8]}",
            },
        ).scalar_one()
        fixture_state["tenant_org_id"] = conn.execute(
            text(
                """
                INSERT INTO organizations (name, slug, tax_id, tier, description, is_active)
                VALUES (:name, :slug, :tax_id, 'pro', 'P2.1 tenant org', true)
                RETURNING id
                """
            ),
            {
                "name": f"P21 Tenant Org {suffix}",
                "slug": f"p21-tenant-org-{suffix}",
                "tax_id": f"35-8{suffix[:8]}",
            },
        ).scalar_one()
        fixture_state["org_ids"].update(
            {
                fixture_state["platform_org_id"],
                fixture_state["tenant_org_id"],
            }
        )

        fixture_state["platform_user_id"] = conn.execute(
            text(
                """
                INSERT INTO users (
                    organization_id, email, username, password_hash, role, full_name, is_active
                )
                VALUES (
                    :organization_id, :email, :username, :password_hash, 'superadmin', 'P2.1 Platform Admin', true
                )
                RETURNING id
                """
            ),
            {
                "organization_id": fixture_state["platform_org_id"],
                "email": f"p21-platform-{suffix}@example.com",
                "username": f"p21_platform_{suffix}",
                "password_hash": hash_password(platform_password),
            },
        ).scalar_one()
        fixture_state["tenant_user_id"] = conn.execute(
            text(
                """
                INSERT INTO users (
                    organization_id, email, username, password_hash, role, full_name, is_active
                )
                VALUES (
                    :organization_id, :email, :username, :password_hash, 'admin', 'P2.1 Tenant Admin', true
                )
                RETURNING id
                """
            ),
            {
                "organization_id": fixture_state["tenant_org_id"],
                "email": f"p21-tenant-{suffix}@example.com",
                "username": f"p21_tenant_{suffix}",
                "password_hash": hash_password(tenant_password),
            },
        ).scalar_one()

        conn.execute(
            text(
                """
                INSERT INTO licenses (
                    organization_id, plan_type, max_lotes, max_volume_tons, max_batch_rows, is_active
                )
                VALUES (:organization_id, 'enterprise', 500, 50000.0, 2000, true)
                """
            ),
            {"organization_id": fixture_state["platform_org_id"]},
        )
        conn.execute(
            text(
                """
                INSERT INTO licenses (
                    organization_id, plan_type, max_lotes, max_volume_tons, max_batch_rows, is_active
                )
                VALUES (:organization_id, 'pro', 100, 5000.0, 500, true)
                """
            ),
            {"organization_id": fixture_state["tenant_org_id"]},
        )
        conn.execute(
            text(
                """
                INSERT INTO lotes (
                    organization_id, identificador, productor_id, producto_forestal,
                    hectareas, latitud, longitud, polygon_wkt, estatus,
                    volumen_ingresado_ton, volumen_exportar_ton
                )
                VALUES (
                    :organization_id, :identificador, :productor_id, 'Madera Aserrada (Pino)',
                    12.0, -27.31, -58.71, :polygon_wkt, 'Pendiente', 20.0, 5.0
                )
                """
            ),
            {
                "organization_id": fixture_state["platform_org_id"],
                "identificador": f"P21-PLATFORM-LOTE-{suffix}",
                "productor_id": f"20-PLAT-{suffix[:4]}",
                "polygon_wkt": "POLYGON((-58.72 -27.32, -58.70 -27.32, -58.70 -27.30, -58.72 -27.30, -58.72 -27.32))",
            },
        )
        conn.execute(
            text(
                """
                INSERT INTO lotes (
                    organization_id, identificador, productor_id, producto_forestal,
                    hectareas, latitud, longitud, polygon_wkt, estatus,
                    volumen_ingresado_ton, volumen_exportar_ton
                )
                VALUES (
                    :organization_id, :identificador, :productor_id, 'Madera Aserrada (Pino)',
                    10.0, -27.41, -58.81, :polygon_wkt, 'Pendiente', 10.0, 3.0
                )
                """
            ),
            {
                "organization_id": fixture_state["tenant_org_id"],
                "identificador": f"P21-TENANT-LOTE-{suffix}",
                "productor_id": f"20-TENT-{suffix[:4]}",
                "polygon_wkt": "POLYGON((-58.82 -27.42, -58.80 -27.42, -58.80 -27.40, -58.82 -27.40, -58.82 -27.42))",
            },
        )

    fixture_state["platform_username"] = f"p21_platform_{suffix}"
    fixture_state["platform_password"] = platform_password
    fixture_state["tenant_username"] = f"p21_tenant_{suffix}"
    fixture_state["tenant_password"] = tenant_password

    yield fixture_state

    org_ids = sorted(fixture_state["org_ids"])
    with owner_engine.begin() as conn:
        conn.execute(
            text("DELETE FROM user_sessions WHERE organization_id = ANY(:org_ids)"),
            {"org_ids": org_ids},
        )
        conn.execute(
            text("DELETE FROM lotes WHERE organization_id = ANY(:org_ids)"),
            {"org_ids": org_ids},
        )
        conn.execute(
            text("DELETE FROM licenses WHERE organization_id = ANY(:org_ids)"),
            {"org_ids": org_ids},
        )
        conn.execute(
            text("DELETE FROM users WHERE organization_id = ANY(:org_ids)"),
            {"org_ids": org_ids},
        )
        conn.execute(
            text("DELETE FROM organizations WHERE id = ANY(:org_ids)"),
            {"org_ids": org_ids},
        )

    runtime_engine.dispose()
    owner_engine.dispose()


def test_control_plane_functions_are_security_definer_and_runtime_only(
    control_plane_fixture,
):
    with control_plane_fixture["owner_engine"].connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    p.proname,
                    pg_get_userbyid(p.proowner) AS owner_name,
                    p.prosecdef,
                    coalesce(array_to_string(p.proconfig, ','), '') AS proconfig,
                    coalesce(p.proacl::text, '') AS acl_text,
                    has_function_privilege('litoral_trace_app', p.oid, 'EXECUTE') AS runtime_execute
                FROM pg_proc AS p
                JOIN pg_namespace AS n
                    ON n.oid = p.pronamespace
                WHERE n.nspname = 'public'
                  AND p.proname IN (
                      '_platform_superadmin_session_actor',
                      'platform_create_organization',
                      'platform_list_organizations',
                      'platform_toggle_organization_status',
                      'platform_upsert_license'
                  )
                ORDER BY p.proname
                """
            )
        ).mappings().all()

    assert [row["proname"] for row in rows] == [
        "_platform_superadmin_session_actor",
        "platform_create_organization",
        "platform_list_organizations",
        "platform_toggle_organization_status",
        "platform_upsert_license",
    ]
    for row in rows:
        assert row["owner_name"] != "litoral_trace_app"
        assert row["prosecdef"] is True
        assert "search_path=public, pg_temp" in row["proconfig"]
        assert "{=X/" not in row["acl_text"]
        assert ",=X/" not in row["acl_text"]
        if row["proname"] == "_platform_superadmin_session_actor":
            assert row["runtime_execute"] is False
        else:
            assert row["runtime_execute"] is True


def test_license_organization_id_unique_index_exists(control_plane_fixture):
    with control_plane_fixture["owner_engine"].connect() as conn:
        indexes = conn.execute(
            text(
                """
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND tablename = 'licenses'
                  AND indexname = 'ix_licenses_organization_id'
                """
            )
        ).mappings().all()

    assert len(indexes) == 1
    assert "CREATE UNIQUE INDEX" in indexes[0]["indexdef"]


def test_ordinary_tenant_admin_cannot_invoke_platform_control_plane(
    control_plane_fixture,
):
    _, tenant_cookies = _login(
        control_plane_fixture["tenant_username"],
        control_plane_fixture["tenant_password"],
    )
    tenant_refresh_token = tenant_cookies["refresh_token"]

    with _postgres_runtime_environment():
        with pytest.raises(HTTPException) as list_exc:
            listar_empresas_superadmin(refresh_token=tenant_refresh_token)
        with pytest.raises(HTTPException) as create_exc:
            crear_nueva_empresa_cliente(
                refresh_token=tenant_refresh_token,
                name=f"P21 Forbidden Org {uuid4().hex[:8]}",
                tax_id=f"36-7{uuid4().hex[:8]}",
                admin_email=f"forbidden-{uuid4().hex[:6]}@example.com",
                admin_username=f"p21_forbidden_{uuid4().hex[:6]}",
                admin_password="ForbiddenPassword-P21!",
                tier="pro",
                monthly_lote_limit=10,
                monthly_ton_limit=100.0,
            )
        with pytest.raises(HTTPException) as status_exc:
            alternar_estado_empresa(
                refresh_token=tenant_refresh_token,
                org_id=control_plane_fixture["platform_org_id"],
            )
        with pytest.raises(HTTPException) as license_exc:
            upsert_license_superadmin(
                refresh_token=tenant_refresh_token,
                organization_id=control_plane_fixture["platform_org_id"],
                plan_type="enterprise",
                max_lotes=250,
                max_volume_tons=25000.0,
                max_batch_rows=750,
                is_active=True,
            )

    assert list_exc.value.status_code == 403
    assert create_exc.value.status_code == 403
    assert status_exc.value.status_code == 403
    assert license_exc.value.status_code == 403


def test_forged_superadmin_jwt_still_fails_closed(control_plane_fixture):
    _, tenant_cookies = _login(
        control_plane_fixture["tenant_username"],
        control_plane_fixture["tenant_password"],
    )

    forged_token = create_jwt_token(
        {
            "sub": control_plane_fixture["tenant_username"],
            "org_id": control_plane_fixture["tenant_org_id"],
            "org_name": f"P21 Tenant Org {uuid4().hex[:4]}",
            "role": "superadmin",
            "email": f"{control_plane_fixture['tenant_username']}@example.com",
        },
        expires_in_seconds=3600,
        token_type="access",
    )

    with _postgres_runtime_environment():
        context = get_current_tenant_user(authorization=f"Bearer {forged_token}")
        with pytest.raises(HTTPException) as permission_exc:
            require_superadmin_role(user=context)
        with pytest.raises(HTTPException) as boundary_exc:
            listar_empresas_superadmin(
                refresh_token=tenant_cookies["refresh_token"],
            )

    assert context.role == "admin"
    assert context.is_platform_superadmin is False
    assert permission_exc.value.status_code == 403
    assert boundary_exc.value.status_code == 403


def test_platform_superadmin_can_list_create_update_and_toggle_persisted_orgs(
    control_plane_fixture,
):
    _, platform_cookies = _login(
        control_plane_fixture["platform_username"],
        control_plane_fixture["platform_password"],
    )

    suffix = uuid4().hex[:8]
    with _postgres_runtime_environment():
        organizations_before = listar_empresas_superadmin(
            refresh_token=platform_cookies["refresh_token"],
        )
        created = crear_nueva_empresa_cliente(
            refresh_token=platform_cookies["refresh_token"],
            name=f"P21 Created Org {suffix}",
            tax_id=f"37-6{suffix}",
            admin_email=f"p21-created-{suffix}@example.com",
            admin_username=f"p21_created_{suffix}",
            admin_password="CreatedPassword-P21!",
            tier="enterprise",
            monthly_lote_limit=220,
            monthly_ton_limit=18000.0,
            max_batch_rows=900,
        )
        updated_license = upsert_license_superadmin(
            refresh_token=platform_cookies["refresh_token"],
            organization_id=int(created["organization_id"]),
            plan_type="custom",
            max_lotes=275,
            max_volume_tons=22000.0,
            max_batch_rows=950,
            is_active=True,
        )
        toggled = alternar_estado_empresa(
            refresh_token=platform_cookies["refresh_token"],
            org_id=int(created["organization_id"]),
        )
        organizations_after = listar_empresas_superadmin(
            refresh_token=platform_cookies["refresh_token"],
        )

    control_plane_fixture["org_ids"].add(int(created["organization_id"]))
    assert len(organizations_after) >= len(organizations_before) + 1
    assert created["status"] == "success"
    assert updated_license["plan_type"] == "custom"
    assert toggled["is_active"] is False

    with control_plane_fixture["owner_engine"].connect() as conn:
        persisted = conn.execute(
            text(
                """
                SELECT
                    o.id,
                    o.is_active,
                    l.plan_type,
                    l.max_lotes,
                    u.username
                FROM organizations AS o
                JOIN licenses AS l
                    ON l.organization_id = o.id
                JOIN users AS u
                    ON u.organization_id = o.id
                WHERE o.id = :organization_id
                  AND u.role = 'admin'
                """
            ),
            {"organization_id": int(created["organization_id"])},
        ).mappings().one()

    assert persisted["id"] == int(created["organization_id"])
    assert persisted["is_active"] is False
    assert persisted["plan_type"] == "custom"
    assert persisted["max_lotes"] == 275
    assert persisted["username"] == f"p21_created_{suffix}"


def test_revoked_platform_session_cannot_invoke_control_plane(control_plane_fixture):
    _, platform_cookies = _login(
        control_plane_fixture["platform_username"],
        control_plane_fixture["platform_password"],
    )
    refresh_token = platform_cookies["refresh_token"]

    with control_plane_fixture["owner_engine"].begin() as conn:
        conn.execute(
            text(
                """
                UPDATE user_sessions
                SET revoked_at = now(),
                    updated_at = now()
                WHERE token_hash = :token_hash
                """
            ),
            {"token_hash": hash_refresh_token(refresh_token)},
        )

    with _postgres_runtime_environment():
        with pytest.raises(HTTPException) as exc_info:
            listar_empresas_superadmin(refresh_token=refresh_token)

    assert exc_info.value.status_code == 401


def test_failed_provisioning_rolls_back_without_half_created_rows(
    control_plane_fixture,
):
    _, platform_cookies = _login(
        control_plane_fixture["platform_username"],
        control_plane_fixture["platform_password"],
    )

    suffix = uuid4().hex[:8]
    failing_tax_id = f"38-5{suffix}"
    failing_name = f"P21 Rollback Org {suffix}"

    with _postgres_runtime_environment():
        with pytest.raises(HTTPException) as exc_info:
            crear_nueva_empresa_cliente(
                refresh_token=platform_cookies["refresh_token"],
                name=failing_name,
                tax_id=failing_tax_id,
                admin_email=f"rollback-{suffix}@example.com",
                admin_username=control_plane_fixture["tenant_username"],
                admin_password="RollbackPassword-P21!",
                tier="pro",
                monthly_lote_limit=50,
                monthly_ton_limit=5000.0,
            )

    assert exc_info.value.status_code == 409

    with control_plane_fixture["owner_engine"].connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, name, tax_id
                FROM organizations
                WHERE name = :organization_name
                   OR tax_id = :tax_id
                """
            ),
            {
                "organization_name": failing_name,
                "tax_id": failing_tax_id,
            },
        ).mappings().all()

    assert rows == []


def test_new_tenant_bootstrap_remains_isolated_after_platform_provisioning(
    control_plane_fixture,
):
    _, platform_cookies = _login(
        control_plane_fixture["platform_username"],
        control_plane_fixture["platform_password"],
    )

    suffix = uuid4().hex[:8]
    created = None
    with _postgres_runtime_environment():
        created = crear_nueva_empresa_cliente(
            refresh_token=platform_cookies["refresh_token"],
            name=f"P21 Isolated Org {suffix}",
            tax_id=f"39-4{suffix}",
            admin_email=f"isolated-{suffix}@example.com",
            admin_username=f"p21_isolated_{suffix}",
            admin_password="IsolatedPassword-P21!",
            tier="pro",
            monthly_lote_limit=90,
            monthly_ton_limit=7000.0,
            max_batch_rows=600,
        )

    control_plane_fixture["org_ids"].add(int(created["organization_id"]))
    token_response, _ = _login(
        created["admin_username"],
        "IsolatedPassword-P21!",
    )

    with _postgres_runtime_environment():
        context = get_current_tenant_user(
            authorization=f"Bearer {token_response.access_token}"
        )
        license_response = asyncio.run(consultar_licencia_tenant(user=context))
        lotes_response = asyncio.run(listar_lotes_tenant(user=context))
        with pytest.raises(HTTPException) as permission_exc:
            require_superadmin_role(user=context)

    license_body = json.loads(license_response.body.decode("utf-8"))
    lotes_body = json.loads(lotes_response.body.decode("utf-8"))

    assert context.organization_id == int(created["organization_id"])
    assert context.is_platform_superadmin is False
    assert license_body["organization_id"] == int(created["organization_id"])
    assert license_body["organization_name"] == created["organization_name"]
    assert lotes_body["lotes"] == []
    assert permission_exc.value.status_code == 403


def test_platform_create_persists_atomic_audit_rows_with_platform_actor(
    control_plane_fixture,
):
    _, platform_cookies = _login(
        control_plane_fixture["platform_username"],
        control_plane_fixture["platform_password"],
    )

    created, admin_password = _provision_control_plane_org(
        control_plane_fixture,
        refresh_token=platform_cookies["refresh_token"],
        name_prefix="P21A Audit Org",
        tax_prefix="40-3",
        username_prefix="p21a_audit",
        email_prefix="p21a-audit",
        password_prefix="P21A-Create",
        tier="enterprise",
        monthly_lote_limit=180,
        monthly_ton_limit=12000.0,
        max_batch_rows=650,
    )
    organization_id = int(created["organization_id"])

    with control_plane_fixture["owner_engine"].connect() as conn:
        persisted_counts = conn.execute(
            text(
                """
                SELECT
                    EXISTS (
                        SELECT 1 FROM organizations WHERE id = :organization_id
                    ) AS organization_exists,
                    EXISTS (
                        SELECT 1 FROM users
                        WHERE organization_id = :organization_id
                          AND role = 'admin'
                    ) AS admin_exists,
                    EXISTS (
                        SELECT 1 FROM licenses
                        WHERE organization_id = :organization_id
                    ) AS license_exists
                """
            ),
            {"organization_id": organization_id},
        ).mappings().one()
        audit_rows = conn.execute(
            text(
                """
                SELECT action, user_id, username, after_data
                FROM audit_logs
                WHERE organization_id = :organization_id
                  AND action LIKE 'platform.%'
                ORDER BY action
                """
            ),
            {"organization_id": organization_id},
        ).mappings().all()

    assert persisted_counts["organization_exists"] is True
    assert persisted_counts["admin_exists"] is True
    assert persisted_counts["license_exists"] is True
    assert [row["action"] for row in audit_rows] == [
        "platform.license.create",
        "platform.organization.create",
        "platform.organization_admin.create",
    ]
    assert {row["user_id"] for row in audit_rows} == {
        control_plane_fixture["platform_user_id"]
    }
    assert {row["username"] for row in audit_rows} == {
        control_plane_fixture["platform_username"]
    }

    serialized_audit = json.dumps(
        [dict(row) for row in audit_rows],
        default=str,
        sort_keys=True,
    )
    _assert_secret_values_absent(
        serialized_audit,
        admin_password,
        platform_cookies["refresh_token"],
        hash_refresh_token(platform_cookies["refresh_token"]),
    )
    assert "password_hash" not in serialized_audit
    assert "refresh_token_hash" not in serialized_audit
    assert "authorization" not in serialized_audit.lower()
    assert "cookie" not in serialized_audit.lower()
    assert "database_url" not in serialized_audit.lower()


def test_platform_create_rolls_back_when_mandatory_audit_fails(
    control_plane_fixture,
):
    _, platform_cookies = _login(
        control_plane_fixture["platform_username"],
        control_plane_fixture["platform_password"],
    )

    suffix = uuid4().hex[:8]
    create_name = f"P21A Rollback Org {suffix}"
    create_tax_id = f"41-2{suffix}"
    create_username = f"p21a_rollback_{suffix}"

    with _failing_platform_audit_trigger(control_plane_fixture["owner_engine"]):
        with _postgres_runtime_environment():
            with pytest.raises(DBAPIError):
                crear_nueva_empresa_cliente(
                    refresh_token=platform_cookies["refresh_token"],
                    name=create_name,
                    tax_id=create_tax_id,
                    admin_email=f"p21a-rollback-{suffix}@example.com",
                    admin_username=create_username,
                    admin_password=f"P21A-Rollback-{suffix}!",
                    tier="pro",
                    monthly_lote_limit=75,
                    monthly_ton_limit=5500.0,
                    max_batch_rows=550,
                )

    with control_plane_fixture["owner_engine"].connect() as conn:
        rollback_state = conn.execute(
            text(
                """
                SELECT
                    EXISTS (
                        SELECT 1 FROM organizations
                        WHERE name = :organization_name OR tax_id = :tax_id
                    ) AS organization_exists,
                    EXISTS (
                        SELECT 1 FROM users WHERE username = :admin_username
                    ) AS admin_exists,
                    EXISTS (
                        SELECT 1
                        FROM audit_logs
                        WHERE action = 'platform.organization.create'
                          AND after_data -> 'metadata' ->> 'organization_name' = :organization_name
                    ) AS organization_audit_exists,
                    EXISTS (
                        SELECT 1
                        FROM audit_logs
                        WHERE action = 'platform.organization_admin.create'
                          AND after_data -> 'metadata' ->> 'admin_username' = :admin_username
                    ) AS admin_audit_exists
                """
            ),
            {
                "organization_name": create_name,
                "tax_id": create_tax_id,
                "admin_username": create_username,
            },
        ).mappings().one()

    assert rollback_state["organization_exists"] is False
    assert rollback_state["admin_exists"] is False
    assert rollback_state["organization_audit_exists"] is False
    assert rollback_state["admin_audit_exists"] is False


def test_platform_status_change_is_atomic_with_audit_rows(
    control_plane_fixture,
):
    _, platform_cookies = _login(
        control_plane_fixture["platform_username"],
        control_plane_fixture["platform_password"],
    )

    created, _ = _provision_control_plane_org(
        control_plane_fixture,
        refresh_token=platform_cookies["refresh_token"],
        name_prefix="P21A Toggle Org",
        tax_prefix="42-1",
        username_prefix="p21a_toggle",
        email_prefix="p21a-toggle",
        password_prefix="P21A-Toggle",
        tier="pro",
        monthly_lote_limit=60,
        monthly_ton_limit=4200.0,
        max_batch_rows=520,
    )
    organization_id = int(created["organization_id"])

    with _postgres_runtime_environment():
        toggled = alternar_estado_empresa(
            refresh_token=platform_cookies["refresh_token"],
            org_id=organization_id,
        )

    assert toggled["is_active"] is False

    with control_plane_fixture["owner_engine"].connect() as conn:
        status_row = conn.execute(
            text(
                """
                SELECT is_active
                FROM organizations
                WHERE id = :organization_id
                """
            ),
            {"organization_id": organization_id},
        ).mappings().one()
        audit_row = conn.execute(
            text(
                """
                SELECT action, user_id, username, after_data
                FROM audit_logs
                WHERE organization_id = :organization_id
                  AND action = 'platform.organization.status_change'
                ORDER BY id DESC
                LIMIT 1
                """
            ),
            {"organization_id": organization_id},
        ).mappings().one()

    assert status_row["is_active"] is False
    assert audit_row["user_id"] == control_plane_fixture["platform_user_id"]
    assert audit_row["username"] == control_plane_fixture["platform_username"]
    assert audit_row["after_data"]["metadata"]["is_active"] is False


def test_platform_status_change_rolls_back_when_mandatory_audit_fails(
    control_plane_fixture,
):
    _, platform_cookies = _login(
        control_plane_fixture["platform_username"],
        control_plane_fixture["platform_password"],
    )

    created, _ = _provision_control_plane_org(
        control_plane_fixture,
        refresh_token=platform_cookies["refresh_token"],
        name_prefix="P21A Toggle Rollback Org",
        tax_prefix="43-0",
        username_prefix="p21a_toggle_rollback",
        email_prefix="p21a-toggle-rollback",
        password_prefix="P21A-ToggleRollback",
        tier="pro",
        monthly_lote_limit=65,
        monthly_ton_limit=4300.0,
        max_batch_rows=525,
    )
    organization_id = int(created["organization_id"])

    with _failing_platform_audit_trigger(control_plane_fixture["owner_engine"]):
        with _postgres_runtime_environment():
            with pytest.raises(DBAPIError):
                alternar_estado_empresa(
                    refresh_token=platform_cookies["refresh_token"],
                    org_id=organization_id,
                )

    with control_plane_fixture["owner_engine"].connect() as conn:
        status_row = conn.execute(
            text(
                """
                SELECT is_active
                FROM organizations
                WHERE id = :organization_id
                """
            ),
            {"organization_id": organization_id},
        ).mappings().one()
        audit_count = conn.execute(
            text(
                """
                SELECT count(*)
                FROM audit_logs
                WHERE organization_id = :organization_id
                  AND action = 'platform.organization.status_change'
                """
            ),
            {"organization_id": organization_id},
        ).scalar_one()

    assert status_row["is_active"] is True
    assert audit_count == 0


def test_platform_license_update_is_atomic_and_sanitized(
    control_plane_fixture,
):
    _, platform_cookies = _login(
        control_plane_fixture["platform_username"],
        control_plane_fixture["platform_password"],
    )

    created, _ = _provision_control_plane_org(
        control_plane_fixture,
        refresh_token=platform_cookies["refresh_token"],
        name_prefix="P21A License Org",
        tax_prefix="44-9",
        username_prefix="p21a_license",
        email_prefix="p21a-license",
        password_prefix="P21A-License",
        tier="pro",
        monthly_lote_limit=70,
        monthly_ton_limit=4500.0,
        max_batch_rows=540,
    )
    organization_id = int(created["organization_id"])

    with _postgres_runtime_environment():
        updated_license = upsert_license_superadmin(
            refresh_token=platform_cookies["refresh_token"],
            organization_id=organization_id,
            plan_type="custom",
            max_lotes=410,
            max_volume_tons=24000.0,
            max_batch_rows=980,
            is_active=True,
        )

    assert updated_license["plan_type"] == "custom"

    with control_plane_fixture["owner_engine"].connect() as conn:
        license_row = conn.execute(
            text(
                """
                SELECT plan_type, max_lotes, max_volume_tons, max_batch_rows
                FROM licenses
                WHERE organization_id = :organization_id
                """
            ),
            {"organization_id": organization_id},
        ).mappings().one()
        audit_row = conn.execute(
            text(
                """
                SELECT action, user_id, username, after_data
                FROM audit_logs
                WHERE organization_id = :organization_id
                  AND action = 'platform.license.update'
                ORDER BY id DESC
                LIMIT 1
                """
            ),
            {"organization_id": organization_id},
        ).mappings().one()

    assert license_row["plan_type"] == "custom"
    assert license_row["max_lotes"] == 410
    assert license_row["max_batch_rows"] == 980
    assert audit_row["user_id"] == control_plane_fixture["platform_user_id"]
    assert audit_row["username"] == control_plane_fixture["platform_username"]

    serialized_audit = json.dumps(dict(audit_row), default=str, sort_keys=True)
    _assert_secret_values_absent(
        serialized_audit,
        platform_cookies["refresh_token"],
        hash_refresh_token(platform_cookies["refresh_token"]),
    )
    assert "refresh_token_hash" not in serialized_audit
    assert "authorization" not in serialized_audit.lower()
    assert "cookie" not in serialized_audit.lower()


def test_platform_license_update_rolls_back_when_mandatory_audit_fails(
    control_plane_fixture,
):
    _, platform_cookies = _login(
        control_plane_fixture["platform_username"],
        control_plane_fixture["platform_password"],
    )

    created, _ = _provision_control_plane_org(
        control_plane_fixture,
        refresh_token=platform_cookies["refresh_token"],
        name_prefix="P21A License Rollback Org",
        tax_prefix="45-8",
        username_prefix="p21a_license_rollback",
        email_prefix="p21a-license-rollback",
        password_prefix="P21A-LicenseRollback",
        tier="pro",
        monthly_lote_limit=75,
        monthly_ton_limit=4700.0,
        max_batch_rows=560,
    )
    organization_id = int(created["organization_id"])

    with control_plane_fixture["owner_engine"].connect() as conn:
        baseline_license = conn.execute(
            text(
                """
                SELECT plan_type, max_lotes, max_volume_tons, max_batch_rows
                FROM licenses
                WHERE organization_id = :organization_id
                """
            ),
            {"organization_id": organization_id},
        ).mappings().one()

    with _failing_platform_audit_trigger(control_plane_fixture["owner_engine"]):
        with _postgres_runtime_environment():
            with pytest.raises(DBAPIError):
                upsert_license_superadmin(
                    refresh_token=platform_cookies["refresh_token"],
                    organization_id=organization_id,
                    plan_type="enterprise",
                    max_lotes=999,
                    max_volume_tons=99999.0,
                    max_batch_rows=1999,
                    is_active=False,
                )

    with control_plane_fixture["owner_engine"].connect() as conn:
        persisted_license = conn.execute(
            text(
                """
                SELECT plan_type, max_lotes, max_volume_tons, max_batch_rows, is_active
                FROM licenses
                WHERE organization_id = :organization_id
                """
            ),
            {"organization_id": organization_id},
        ).mappings().one()
        audit_count = conn.execute(
            text(
                """
                SELECT count(*)
                FROM audit_logs
                WHERE organization_id = :organization_id
                  AND action = 'platform.license.update'
                """
            ),
            {"organization_id": organization_id},
        ).scalar_one()

    assert persisted_license["plan_type"] == baseline_license["plan_type"]
    assert persisted_license["max_lotes"] == baseline_license["max_lotes"]
    assert persisted_license["max_volume_tons"] == baseline_license["max_volume_tons"]
    assert persisted_license["max_batch_rows"] == baseline_license["max_batch_rows"]
    assert persisted_license["is_active"] is True
    assert audit_count == 0
