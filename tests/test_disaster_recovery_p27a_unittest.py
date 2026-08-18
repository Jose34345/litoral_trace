from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DR_RUNBOOK_PATH = ROOT / "DISASTER_RECOVERY_RUNBOOK.md"
DEPLOYMENT_RUNBOOK_PATH = ROOT / "DEPLOYMENT_RUNBOOK.md"


def _dr_text() -> str:
    return DR_RUNBOOK_PATH.read_text(encoding="utf-8")


def _deployment_text() -> str:
    return DEPLOYMENT_RUNBOOK_PATH.read_text(encoding="utf-8")


def test_p27a_dr_runbook_exists():
    assert DR_RUNBOOK_PATH.exists()


def test_p27a_dr_runbook_defines_rpo_rto_and_history_contract():
    dr = _dr_text()

    for token in (
        "PostgreSQL operational RPO",
        "PostgreSQL operational RTO",
        "<= 15 minutes",
        "<= 4 hours",
        ">= 7 days",
        "6-hour history window",
        "CURRENT GAP - NOT YET GO-LIVE COMPLIANT",
        "independent pg_dump / pg_restore",
        "<= 24 hours",
        "quarterly",
    ):
        assert token in dr


def test_p27a_dr_runbook_distinguishes_database_from_vault_object_recovery():
    dr = _dr_text()

    assert "separate recovery domains" in dr
    assert "Restoring PostgreSQL metadata does not restore S3 / Vault object bytes." in dr
    assert "Vault object recovery remains a separate gate" in dr


def test_p27a_dr_runbook_requires_isolated_restore_before_cutover():
    dr = _dr_text()

    assert "restore into an isolated branch/environment first" in dr
    assert "only then perform controlled cutover/finalization" in dr
    assert "Production must not be overwritten first when an isolated restore path is available." in dr


def test_p27a_dr_runbook_prohibits_blind_downgrade_and_overwrite():
    dr = _dr_text()

    assert "Blind production database overwrite is prohibited" in dr
    assert "Blind Alembic downgrade is prohibited" in dr


def test_p27a_dr_runbook_requires_pre_migration_recovery_point():
    dr = _dr_text()

    assert "Every production schema-changing deployment must have a recovery point created or verified before Alembic upgrade head." in dr
    assert "release commit" in dr
    assert "current Alembic revision" in dr
    assert "verification status" in dr


def test_p27a_dr_runbook_defines_restore_evidence_fields():
    dr = _dr_text()

    for token in (
        "date/time",
        "source environment/branch",
        "recovery mechanism",
        "recovery point",
        "isolated restore target",
        "database reachability",
        "PostgreSQL version",
        "PostGIS availability/version",
        "Alembic revision",
        "critical schema/table verification",
        "selected critical row-count/data verification",
        "elapsed restore/verification time",
        "final result",
        "whether production was modified",
    ):
        assert token in dr


def test_p27a_dr_runbook_records_p27a1_restore_drill():
    dr = _dr_text()

    for token in (
        "2026-08-17",
        "Neon manual snapshot + multi-step isolated restore",
        "production",
        "PASS",
        "PostgreSQL 17.10",
        "PostGIS 3.5",
        "008_add_platform_control_plane_functions",
        "organizations 4",
        "users 4",
        "lotes 1",
        "audit_logs 6",
        "critical table inventory parity: PASS",
        "production replaced: NO",
    ):
        assert token in dr


def test_p27a_dr_runbook_records_p27a3_closed_and_vault_remains_separate():
    dr = _dr_text()

    for token in (
        "P2.7A3 status:",
        "CLOSED on 2026-08-17 after scheduled off-platform backup acceptance.",
        "GitHub Actions workflow run 32086976028: PASS",
        "PostgreSQL client: 17.11",
        "AWS authentication: OIDC assumed role",
        "server-side encryption: SSE-S3",
        "Object Lock: Governance",
        "default retention: 35 days",
        "<= 24 hours independent logical backup RPO target",
        "P2.7A4 Vault object-storage recovery contract",
    ):
        assert token in dr

    assert "P2.7A3 is NOT CLOSED yet." not in dr
    assert "Vault recovery complete" not in dr


def test_p27a_dr_runbook_records_real_logical_recovery_drill_pass():
    dr = _dr_text()

    for token in (
        "Historical evidence: P2.7A3B real logical backup/restore drill",
        "portable atomic pg_dump / pg_restore isolated logical recovery drill",
        "source release 894f5d3",
        "source database neondb",
        "20260817T181632Z_production.dump",
        "20260817T181632Z_production.manifest.json",
        "df4b805a64f3bd8e0b88430a54cbf71e06dfd0a250df344d2dd24817327ca122",
        "database p27a3_restore",
        "enterprise-integration",
        "result: PASS",
        "table_inventory_match: true",
        "critical_row_counts_match: true",
        "organizations 4, users 4, lotes 1, audit_logs 6",
        "api_keys, audit_logs, licenses, lotes, organizations, satellite_ndvi_observations, user_sessions, users",
        "production overwritten/swapped: NO",
    ):
        assert token in dr


def test_p27a_dr_runbook_records_portable_atomic_restore_semantics():
    dr = _dr_text()

    for token in (
        "pg_restore portability/atomicity is required:",
        "direct/unpooled target only",
        "isolated target must be empty before restore",
        "credentials only through libpq environment",
        "portable restore flags must exclude source ownership/ACL replay",
        "restore must run in a single transaction",
        "pg_restore is portable and atomic",
    ):
        assert token in dr


def test_p27a_dr_runbook_records_real_drill_security_evidence():
    dr = _dr_text()

    for token in (
        "manifest contained no database URL",
        "manifest contained no password token",
        "manifest contained no pooler hostname",
        "restore report contained no database URL",
        "restore report contained no password token",
        "restore report contained no pooler hostname",
    ):
        assert token in dr


def test_p27a_deployment_runbook_references_dr_and_pre_migration_recovery():
    deployment = _deployment_text()

    assert "6B. Disaster recovery pre-migration gate" in deployment
    assert "DISASTER_RECOVERY_RUNBOOK.md" in deployment
    assert "Schema-changing production deployments require a verified recovery point before migration." in deployment
    assert "Go-live PITR/history target is >= 7 days" in deployment
    assert "Successful isolated restore testing is required." in deployment
    assert "Independent pg_dump fallback and Vault recovery are covered by the disaster recovery program." in deployment
    assert "Blind production database downgrade/restore is prohibited." in deployment


def test_p27a_dr_runbook_records_p27a5_operational_acceptance():
    dr = _dr_text()

    for token in (
        "CLOSED on 2026-08-18 after real production pre-migration recovery-gate acceptance.",
        "20260818T021501Z_production.manifest.json",
        "stale-evidence result: NO MIGRATION",
        "fresh production logical-backup workflow run 32155184817: PASS",
        "20260818T153402Z_production.manifest.json",
        "0c94022ad7ffdae781b85b8dac38f18e64de0e05",
        "008_add_platform_control_plane_functions",
        "p27a5.gate.v1",
        "109026eaf5c421b5051f26c882f60cc35a18650e0f47f99d30a4be0804bf9190",
        "d0f40f7dbfdb29812fe6aafd8f7e7c8bec7393a91342775fffde5eba6931a383",
        "backup age at verification: 915 seconds",
        "2026-08-18T15:49:17Z",
        "verification status: PASS",
        "production migration executed during acceptance: NO",
        "P2.7A5 operational acceptance is complete.",
    ):
        assert token in dr

    assert (
        "Operational execution against real production recovery evidence "
        "is still required"
        not in dr
    )
