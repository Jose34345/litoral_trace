from __future__ import annotations

import asyncio
from uuid import uuid4

from fastapi import Response
from sqlalchemy import select

from litoral_trace.api.auth import (
    LoginRequest,
    _is_platform_superadmin,
    get_current_tenant_user,
    login_b2b,
)
from litoral_trace.auth.passwords import hash_password
from litoral_trace.auth.tokens import create_jwt_token
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import Organization, User


def _create_organization_and_user(
    *,
    role: str,
    organization_slug: str | None = None,
    organization_id: int | None = None,
    username: str | None = None,
) -> dict[str, str | int]:
    suffix = uuid4().hex[:10]
    password = "PlatformPrivilege2026!"
    db_session = get_db_session()

    try:
        if organization_id is None:
            organization = Organization(
                name=f"Platform Tenant {suffix}",
                slug=organization_slug or f"platform-tenant-{suffix}",
                tax_id=f"30-{suffix[:8]}",
                tier="pro",
                is_active=True,
            )
            db_session.add(organization)
            db_session.commit()
            db_session.refresh(organization)
        else:
            organization = db_session.execute(
                select(Organization).where(Organization.id == organization_id)
            ).scalar_one()

        user = User(
            organization_id=organization.id,
            email=f"{suffix}@example.com",
            username=username or f"user_{suffix}",
            password_hash=hash_password(password),
            role=role,
            full_name="Platform Privilege Test User",
            is_active=True,
        )
        db_session.add(user)
        db_session.commit()
        db_session.refresh(user)
        return {
            "organization_id": organization.id,
            "organization_name": organization.name,
            "organization_slug": organization.slug,
            "user_email": user.email,
            "username": user.username,
            "role": user.role,
            "password": password,
        }
    finally:
        db_session.close()


def test_superadmin_role_is_platform_superadmin():
    token = asyncio.run(
        login_b2b(
            LoginRequest(username="admin", password="admin123"),
            Response(),
        )
    )

    context = get_current_tenant_user(authorization=f"Bearer {token.access_token}")

    assert context.role == "superadmin"
    assert context.is_platform_superadmin is True


def test_organization_admin_is_not_platform_superadmin():
    account = _create_organization_and_user(role="admin")
    token = asyncio.run(
        login_b2b(
            LoginRequest(
                username=str(account["username"]),
                password=str(account["password"]),
            ),
            Response(),
        )
    )

    context = get_current_tenant_user(authorization=f"Bearer {token.access_token}")

    assert context.role == "admin"
    assert context.is_platform_superadmin is False


def test_username_admin_without_platform_role_is_not_superadmin():
    user = User(
        organization_id=1,
        email="nonplatform-admin@example.com",
        username="admin",
        password_hash="placeholder",
        role="admin",
        is_active=True,
    )

    assert _is_platform_superadmin(user=user) is False


def test_exp_chaco_membership_without_platform_role_is_not_superadmin():
    db_session = get_db_session()
    try:
        exp_chaco = db_session.execute(
            select(Organization).where(Organization.slug == "exp-chaco")
        ).scalar_one()
    finally:
        db_session.close()

    account = _create_organization_and_user(
        role="admin",
        organization_id=exp_chaco.id,
    )
    token = asyncio.run(
        login_b2b(
            LoginRequest(
                username=str(account["username"]),
                password=str(account["password"]),
            ),
            Response(),
        )
    )

    context = get_current_tenant_user(authorization=f"Bearer {token.access_token}")

    assert context.organization_slug == "exp-chaco"
    assert context.role == "admin"
    assert context.is_platform_superadmin is False


def test_client_role_claim_cannot_escalate_platform_privilege():
    account = _create_organization_and_user(role="manager")
    forged_token = create_jwt_token(
        {
            "sub": str(account["username"]),
            "org_id": int(account["organization_id"]),
            "org_name": "forged-org",
            "role": "superadmin",
            "email": "forged@example.com",
        },
        expires_in_seconds=3600,
    )

    context = get_current_tenant_user(authorization=f"Bearer {forged_token}")

    assert context.organization_id == account["organization_id"]
    assert context.organization_name == account["organization_name"]
    assert context.role == "manager"
    assert context.is_platform_superadmin is False
