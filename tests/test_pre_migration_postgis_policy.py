"""Fail-closed recovery compatibility for Lacey without PostGIS."""
import pytest

from scripts.pre_migration_recovery_gate import (
    RecoveryGateError,
    _verify_postgis_compatibility,
)


@pytest.mark.parametrize(
    "revision",
    [
        "079_us_lacey_assurance_v2_claim_snapshots",
        "080_us_lacey_evaluation_email_proof",
    ],
)
def test_lacey_without_postgis_is_valid(revision):
    _verify_postgis_compatibility(
        manifest_version=None, live_version=None, alembic_revision=revision,
    )


@pytest.mark.parametrize(
    "manifest,live,revision",
    [
        (None, None, "028_platform_definer_rls"),
        (None, "3.4.1", "079_us_lacey_assurance_v2_claim_snapshots"),
        ("3.4.1", None, "079_us_lacey_assurance_v2_claim_snapshots"),
        ("3.4.1", "3.5.0", "028_platform_definer_rls"),
    ],
)
def test_missing_or_mismatched_postgis_fails_closed(manifest, live, revision):
    with pytest.raises(RecoveryGateError):
        _verify_postgis_compatibility(
            manifest_version=manifest,
            live_version=live,
            alembic_revision=revision,
        )


def test_legacy_postgis_matching_major_minor_passes():
    _verify_postgis_compatibility(
        manifest_version="3.5.2", live_version="3.5.1",
        alembic_revision="028_platform_definer_rls",
    )
