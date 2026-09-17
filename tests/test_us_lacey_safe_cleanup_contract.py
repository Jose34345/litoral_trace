from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_historical_us_lacey_root_docs_are_compatibility_stubs() -> None:
    expected = {
        "US_LACEY_COMPLETION_CHECKLIST.md": "docs/archive/us-lacey/US_LACEY_COMPLETION_CHECKLIST.md",
        "US_LACEY_UI_PARITY_AUDIT.md": "docs/archive/us-lacey/US_LACEY_UI_PARITY_AUDIT.md",
    }
    for root_name, archive_path in expected.items():
        content = (ROOT / root_name).read_text(encoding="utf-8")
        assert "ARCHIVED DOCUMENT" in content
        assert archive_path in content
        assert "docs/us-lacey/README.md" in content


def test_us_lacey_archive_and_cleanup_audit_exist() -> None:
    archive_root = ROOT / "docs" / "archive" / "us-lacey"
    assert (archive_root / "README.md").is_file()

    archived_docs = [
        archive_root / "US_LACEY_COMPLETION_CHECKLIST.md",
        archive_root / "US_LACEY_UI_PARITY_AUDIT.md",
    ]
    for path in archived_docs:
        assert path.is_file()
        content = path.read_text(encoding="utf-8")
        assert "ARCHIVED" in content
        assert "docs/us-lacey/README.md" in content

    audit = (ROOT / "docs" / "us-lacey" / "CLEANUP_AUDIT.md").read_text(encoding="utf-8")
    for required in (
        "us_lacey_unified_app.py",
        "us_lacey_free_app.py",
        "us_lacey_pilot_app.py",
        "us_lacey_worker_app.py",
        "lacey_experiment_app.py",
        "us_lacey_platform_admin.py",
        "KEEP",
    ):
        assert required in audit
