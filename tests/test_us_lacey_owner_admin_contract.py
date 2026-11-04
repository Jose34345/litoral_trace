from __future__ import annotations

from pathlib import Path


MIGRATION = Path("alembic/versions/042_add_us_lacey_owner_admin_overview.py")
SERVICE = Path("src/litoral_trace/services/us_lacey_admin.py")
ADMIN_API = Path("src/litoral_trace/api/admin.py")
ADMIN_PAGE = Path("src/litoral_trace/templates/admin_organizations.html")
ADMIN_FRAGMENT = Path("src/litoral_trace/templates/admin_us_lacey_accounts.html")


def test_042_is_single_chain_after_lemon_and_uses_platform_definer() -> None:
    text = MIGRATION.read_text(encoding="utf-8")

    assert 'revision: str = "042_us_lacey_owner_admin"' in text
    assert '"041_us_lacey_lemon"' in text
    assert "platform_us_lacey_account_overview" in text
    assert "_platform_superadmin_session_actor" in text
    assert "SECURITY DEFINER" in text
    assert "SET search_path = public, pg_temp" in text
    assert "SET ROLE {PLATFORM_ROLE}" in text


def test_042_owner_projection_is_read_only_and_does_not_expose_lemon_ledger_secrets() -> None:
    text = MIGRATION.read_text(encoding="utf-8")

    assert "GRANT EXECUTE" in text
    assert "TO {RUNTIME_ROLE}" in text
    assert "REVOKE ALL ON FUNCTION" in text
    assert "payload_sha256" not in text
    assert "provider_order_id" not in text
    assert "store_id" not in text
    assert "variant_id" not in text
    assert "INSERT INTO public.us_lacey" not in text
    assert "UPDATE public.us_lacey" not in text
    assert "DELETE FROM public.us_lacey" not in text


def test_owner_service_calls_only_the_curated_platform_function() -> None:
    text = SERVICE.read_text(encoding="utf-8")

    assert "platform_us_lacey_account_overview" in text
    assert "_require_platform_refresh_token_hash" in text
    assert 'bind.dialect.name != "postgresql"' in text
    for table_name in (
        "us_lacey_organization_profiles",
        "us_lacey_subscriptions",
        "us_lacey_payments",
        "us_lacey_processing_jobs",
        "us_lacey_payment_events",
    ):
        assert table_name not in text


def test_owner_api_reuses_platform_admin_permission_and_persistent_session() -> None:
    text = ADMIN_API.read_text(encoding="utf-8")

    assert "Permission.PLATFORM_ADMIN" in text
    assert "REFRESH_TOKEN_COOKIE_KEY" in text
    assert 'router.get("/us-lacey/accounts"' in text
    assert '"/us-lacey/accounts/fragment"' in text
    assert "list_us_lacey_accounts_superadmin" in text


def test_existing_admin_page_mounts_control_plane_us_lacey_console() -> None:
    page = ADMIN_PAGE.read_text(encoding="utf-8")
    fragment = ADMIN_FRAGMENT.read_text(encoding="utf-8")

    assert 'hx-get="/api/v1/admin/us-lacey/accounts/fragment"' in page
    assert "Owner / Admin Console" in fragment
    assert "Vista global de solo lectura" in fragment
    assert "no puede activar pagos" in fragment
    assert "hx-post" in fragment
    assert "X-CSRF-Token" in fragment
    assert "reset-pilot" in fragment
    assert "verify-payment" not in fragment.lower()
