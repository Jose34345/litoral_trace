from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "alembic" / "versions" / "028_platform_definer_rls.py"
NON_SUPERUSER_GATE = (
    ROOT / ".github" / "workflows" / "p17k-nonsuperuser-migration-gate.yml"
)


def test_preexisting_cluster_role_collision_fails_without_mutation() -> None:
    source = MIGRATION.read_text(encoding="utf-8")
    ensure_source = source.split("def _ensure_platform_role()", 1)[1].split(
        "def _grant_temporary_platform_set_capability()", 1
    )[0]

    assert "platform definer role already exists outside migration 028 lifecycle" in ensure_source
    assert "USING ERRCODE = '42710'" in ensure_source
    assert "ALTER ROLE {PLATFORM_ROLE}" not in ensure_source
    assert "CREATE ROLE {PLATFORM_ROLE}" in ensure_source
    assert "NOLOGIN" in ensure_source
    assert "NOSUPERUSER" in ensure_source
    assert "NOCREATEDB" in ensure_source
    assert "NOCREATEROLE" in ensure_source
    assert "NOINHERIT" in ensure_source
    assert "NOREPLICATION" in ensure_source
    assert "NOBYPASSRLS" in ensure_source


def test_migrator_privilege_edges_are_self_issued_and_transaction_scoped() -> None:
    source = MIGRATION.read_text(encoding="utf-8")
    set_helper = source.split(
        "def _grant_temporary_platform_set_capability()", 1
    )[1].split("def _grant_temporary_platform_inherit_capability()", 1)[0]
    inherit_helper = source.split(
        "def _grant_temporary_platform_inherit_capability()", 1
    )[1].split("def _revoke_temporary_platform_capability()", 1)[0]
    revoke_helper = source.split(
        "def _revoke_temporary_platform_capability()", 1
    )[1].split("def _grant_platform_capabilities()", 1)[0]
    transfer_helper = source.split(
        "def _transfer_platform_function_ownership()", 1
    )[1].split("def _restore_027_actor()", 1)[0]
    transfer_back_helper = source.split(
        "def _transfer_platform_functions_back_to_migration_role()", 1
    )[1].split("def _drop_platform_rls_policies()", 1)[0]

    assert "WITH ADMIN FALSE, INHERIT FALSE, SET TRUE" in set_helper
    assert "GRANTED BY CURRENT_USER" in set_helper
    assert "WITH ADMIN FALSE, INHERIT TRUE, SET FALSE" in inherit_helper
    assert "GRANTED BY CURRENT_USER" in inherit_helper
    assert "REVOKE {PLATFORM_ROLE} FROM CURRENT_USER" in revoke_helper
    assert "GRANTED BY CURRENT_USER" in revoke_helper

    assert "_grant_temporary_platform_set_capability()" in transfer_helper
    assert "_revoke_temporary_platform_capability()" in transfer_helper
    assert transfer_helper.index("_grant_temporary_platform_set_capability()") < transfer_helper.index(
        "ALTER FUNCTION {function_signature} OWNER TO {PLATFORM_ROLE}"
    )
    assert transfer_helper.index("ALTER FUNCTION {function_signature} OWNER TO {PLATFORM_ROLE}") < transfer_helper.index(
        "_revoke_temporary_platform_capability()"
    )

    assert "_grant_temporary_platform_inherit_capability()" in transfer_back_helper
    assert "REASSIGN OWNED BY {PLATFORM_ROLE} TO CURRENT_USER" in transfer_back_helper
    assert "_revoke_temporary_platform_capability()" in transfer_back_helper
    assert transfer_back_helper.index("_grant_temporary_platform_inherit_capability()") < transfer_back_helper.index(
        "REASSIGN OWNED BY {PLATFORM_ROLE} TO CURRENT_USER"
    )
    assert transfer_back_helper.index("REASSIGN OWNED BY {PLATFORM_ROLE} TO CURRENT_USER") < transfer_back_helper.index(
        "_revoke_temporary_platform_capability()"
    )
    assert "SET ROLE" not in transfer_back_helper
    assert "ALTER FUNCTION" not in transfer_back_helper


def test_downgrade_drops_only_role_owned_by_successful_028_lifecycle() -> None:
    source = MIGRATION.read_text(encoding="utf-8")
    ensure_source = source.split("def _ensure_platform_role()", 1)[1].split(
        "def _grant_temporary_platform_set_capability()", 1
    )[0]
    downgrade_helper = source.split(
        "def _revoke_platform_capabilities_and_drop_role()", 1
    )[1].split("def upgrade()", 1)[0]

    assert "IF EXISTS" in ensure_source
    assert "RAISE EXCEPTION" in ensure_source
    assert "DROP ROLE {PLATFORM_ROLE}" in downgrade_helper
    assert "DROP ROLE IF EXISTS" not in downgrade_helper
    assert "aborts before mutating database state on collision" in downgrade_helper
    assert "automatic creator grant" in downgrade_helper


def test_non_superuser_gate_exercises_collision_then_clean_lifecycle() -> None:
    workflow = NON_SUPERUSER_GATE.read_text(encoding="utf-8")

    assert "Seed colliding pre-existing platform role" in workflow
    assert "Expect migration 028 to reject the cluster role collision" in workflow
    assert "platform definer role already exists outside migration 028 lifecycle" in workflow
    assert "Verify collision attempt left role unchanged and database at 027" in workflow
    assert "Remove collision and migrate to canonical head" in workflow
    assert "Verify 028 downgrade removes only its own role" in workflow
    assert "Re-upgrade to canonical head after rollback proof" in workflow
