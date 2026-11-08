from pathlib import Path


def test_container_build_excludes_local_secrets_and_database_artifacts():
    root = Path(__file__).resolve().parents[1]
    ignore_file = root / ".dockerignore"
    assert ignore_file.is_file(), "The container build context needs a .dockerignore."

    rules = {
        line.strip()
        for line in ignore_file.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }
    assert {
        "**/.git",
        "**/.venv",
        "**/.env",
        "**/.env.*",
        "backups/postgres",
        "**/*.dump",
        "**/*.backup",
        "**/*.sql.gz",
    } <= rules

    # These checked-in examples may still be used to document runtime config.
    assert {rule for rule in rules if rule.startswith("!")} == {
        "!.env.example",
        "!.env.assurance-pilot-staging.example",
    }
