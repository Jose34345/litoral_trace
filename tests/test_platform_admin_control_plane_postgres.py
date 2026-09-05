"""Real PostgreSQL acceptance for migration 044's privileged capabilities."""
from __future__ import annotations

import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError


pytestmark = pytest.mark.skipif(
    os.environ.get("ENABLE_POSTGRES_TESTS") != "1" or not os.environ.get("US_LACEY_TEST_AUDIT_DATABASE_URL") or not (os.environ.get("US_LACEY_DATABASE_URL") or os.environ.get("TEST_POSTGRES_DATABASE_URL")),
    reason="requires the isolated PostgreSQL gate",
)


def test_044_control_plane_authorization_promotion_reset_and_paid_guard() -> None:
    audit = create_engine(os.environ["US_LACEY_TEST_AUDIT_DATABASE_URL"], pool_pre_ping=True)
    runtime = create_engine(os.environ.get("US_LACEY_DATABASE_URL") or os.environ["TEST_POSTGRES_DATABASE_URL"], pool_pre_ping=True)
    suffix = uuid4().hex[:10]
    ids: list[int] = []
    actor_token, normal_token = ("a" * 54 + suffix[:10], "b" * 54 + suffix[:10])
    actor_token, normal_token = actor_token[:64], normal_token[:64]
    try:
        with audit.begin() as c:
            def org(label: str) -> int:
                value = c.execute(text("INSERT INTO public.organizations(name,slug,tax_id,tier,is_active) VALUES(:n,:s,:t,'pro',true) RETURNING id"), {"n": f"044 {label} {suffix}", "s": f"p044-{label}-{suffix}", "t": f"44{len(ids)}{suffix[:7]}"}).scalar_one()
                ids.append(value); return value
            platform, tenant_a, tenant_b = org("platform"), org("a"), org("b")
            def user(org_id: int, email: str, role: str, token: str | None = None) -> int:
                uid = c.execute(text("INSERT INTO public.users(organization_id,email,username,password_hash,role,is_active) VALUES(:o,:e,:u,:p,:r,true) RETURNING id"), {"o":org_id,"e":email,"u":f"u-{uuid4().hex[:12]}","p":"unchanged-password-hash","r":role}).scalar_one()
                if token:
                    c.execute(text("INSERT INTO public.user_sessions(user_id,organization_id,family_id,token_hash,issued_at,expires_at) VALUES(:u,:o,:f,:t,now(),now()+interval '1 hour')"), {"u":uid,"o":org_id,"f":str(uuid4()),"t":token})
                return uid
            actor = user(platform, f"actor-{suffix}@example.com", "superadmin", actor_token)
            founder = user(tenant_a, f"founder-{suffix}@example.com", "admin", normal_token)
            other = user(tenant_b, f"other-{suffix}@example.com", "admin")
            for oid, email in ((tenant_a, f"founder-{suffix}@example.com"), (tenant_b, f"other-{suffix}@example.com")):
                c.execute(text("INSERT INTO public.us_lacey_organization_profiles(organization_id,legal_name,country_code,business_type,admin_contact_email,account_status) VALUES(:o,:n,'US','IMPORTER',:e,'PAYMENT_PENDING')"), {"o":oid,"n":f"Legal {oid}","e":email})
                c.execute(text("INSERT INTO public.us_lacey_subscriptions(public_id,organization_id,plan_code,currency,price_cents,monthly_operation_limit,used_operations,status) VALUES(:p,:o,'TEST','USD',100,5,0,'PENDING')"), {"p":str(uuid4()),"o":oid})
            c.execute(text("INSERT INTO public.us_lacey_operations(public_id,organization_id,created_by_user_id,client_reference,status,document_count,merchandise_line_count) VALUES(:p,:o,:u,:r,'NEW',0,0)"), {"p":str(uuid4()),"o":tenant_a,"u":founder,"r":f"a-{suffix}"})
            c.execute(text("INSERT INTO public.us_lacey_operations(public_id,organization_id,created_by_user_id,client_reference,status,document_count,merchandise_line_count) VALUES(:p,:o,:u,:r,'NEW',0,0)"), {"p":str(uuid4()),"o":tenant_b,"u":other,"r":f"b-{suffix}"})
        with runtime.begin() as c:
            promoted = c.execute(text("SELECT * FROM public.platform_admin_promote_existing_user(:t,:e)"), {"t":actor_token,"e":f"founder-{suffix}@example.com"}).mappings().one()
            assert promoted["user_id"] == founder
            assert c.execute(text("SELECT * FROM public.platform_admin_promote_existing_user(:t,:e)"), {"t":actor_token,"e":f"founder-{suffix}@example.com"}).mappings().one()["user_id"] == founder
            assert c.execute(text("SELECT * FROM public.platform_admin_set_us_lacey_account_status(:t,:o,'PILOT')"), {"t":actor_token,"o":tenant_a}).mappings().one()["account_status"] == "PILOT"
            assert c.execute(text("SELECT * FROM public.platform_admin_set_us_lacey_operation_limit(:t,:o,9)"), {"t":actor_token,"o":tenant_a}).mappings().one()["monthly_operation_limit"] == 9
            assert c.execute(text("SELECT * FROM public.platform_admin_reset_pilot_account(:t,:o)"), {"t":actor_token,"o":tenant_a}).mappings().one()["operations_deleted"] == 1
        with runtime.begin() as c:
            with pytest.raises(DBAPIError): c.execute(text("SELECT * FROM public.platform_admin_set_us_lacey_operation_limit(:t,:o,0)"), {"t":actor_token,"o":tenant_a})
        with runtime.begin() as c:
            with pytest.raises(DBAPIError): c.execute(text("SELECT * FROM public.platform_admin_set_us_lacey_account_status(:t,:o,'PILOT')"), {"t":normal_token,"o":tenant_b})
        with audit.connect() as c:
            privileges = c.execute(text("SELECT has_table_privilege('litoral_trace_platform_definer','public.us_lacey_operations','DELETE') AS definer_delete, has_table_privilege('litoral_trace_app','public.us_lacey_operations','DELETE') AS runtime_delete, has_table_privilege('litoral_trace_us_lacey_worker','public.us_lacey_operations','DELETE') AS worker_delete, has_table_privilege('public','public.us_lacey_operations','DELETE') AS public_delete")).mappings().one()
            assert dict(privileges) == {"definer_delete": True, "runtime_delete": False, "worker_delete": False, "public_delete": False}
            assert c.execute(text("SELECT count(*) FROM pg_policies WHERE schemaname='public' AND tablename='us_lacey_operations' AND policyname='us_lacey_operations_platform_delete_044'" )).scalar_one() == 1
            assert c.execute(text("SELECT rolcanlogin FROM pg_roles WHERE rolname='litoral_trace_platform_definer'")).scalar_one() is False
            assert c.execute(text("SELECT rolbypassrls FROM pg_roles WHERE rolname='litoral_trace_app'")).scalar_one() is False
            assert c.execute(text("SELECT role FROM public.users WHERE id=:u"), {"u":founder}).scalar_one() == "superadmin"
            assert c.execute(text("SELECT password_hash FROM public.users WHERE id=:u"), {"u":founder}).scalar_one() == "unchanged-password-hash"
            assert c.execute(text("SELECT count(*) FROM public.user_sessions WHERE user_id=:u AND revoked_at IS NOT NULL"), {"u":founder}).scalar_one() == 1
            assert c.execute(text("SELECT count(*) FROM public.us_lacey_operations WHERE organization_id=:o"), {"o":tenant_a}).scalar_one() == 0
            assert c.execute(text("SELECT count(*) FROM public.us_lacey_operations WHERE organization_id=:o"), {"o":tenant_b}).scalar_one() == 1
            assert c.execute(text("SELECT count(*) FROM public.audit_logs WHERE organization_id=:o AND action IN ('FOUNDER_PROMOTED','ACCOUNT_STATUS_CHANGED','OPERATION_LIMIT_CHANGED','PILOT_TEST_RESET')"), {"o":tenant_a}).scalar_one() >= 4
    finally:
        with audit.begin() as c:
            if ids:
                c.execute(text("DELETE FROM public.us_lacey_operations WHERE organization_id = ANY(:ids)"), {"ids": ids})
                c.execute(text("DELETE FROM public.us_lacey_subscriptions WHERE organization_id = ANY(:ids)"), {"ids": ids})
                c.execute(text("DELETE FROM public.us_lacey_organization_profiles WHERE organization_id = ANY(:ids)"), {"ids": ids})
                c.execute(text("DELETE FROM public.user_sessions WHERE organization_id = ANY(:ids)"), {"ids": ids})
                c.execute(text("DELETE FROM public.audit_logs WHERE organization_id = ANY(:ids)"), {"ids": ids})
                c.execute(text("DELETE FROM public.users WHERE organization_id = ANY(:ids)"), {"ids": ids})
                c.execute(text("DELETE FROM public.organizations WHERE id = ANY(:ids)"), {"ids": ids})
        audit.dispose(); runtime.dispose()
