from __future__ import annotations

from pathlib import Path
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
CAPABILITIES = REPO_ROOT / "docs" / "us-lacey" / "CAPABILITIES.toml"
VALIDATOR = REPO_ROOT / "scripts" / "validate_us_lacey_architecture_docs.py"


def test_us_lacey_control_plane_docs_exist() -> None:
    required = [
        REPO_ROOT / "AGENTS.md",
        REPO_ROOT / "src" / "litoral_trace" / "us_lacey" / "AGENTS.md",
        REPO_ROOT / "src" / "litoral_trace" / "lacey_engine" / "AGENTS.md",
        REPO_ROOT / "docs" / "us-lacey" / "README.md",
        REPO_ROOT / "docs" / "us-lacey" / "ARCHITECTURE.md",
        CAPABILITIES,
        REPO_ROOT / "docs" / "us-lacey" / "INVARIANTS.md",
        REPO_ROOT / "docs" / "us-lacey" / "DATA_MODEL.md",
        REPO_ROOT / "docs" / "us-lacey" / "PIPELINE.md",
        REPO_ROOT / "docs" / "us-lacey" / "TEST_MATRIX.md",
        REPO_ROOT / "docs" / "us-lacey" / "ROADMAP.md",
        REPO_ROOT / "docs" / "us-lacey" / "CLEANUP_CANDIDATES.md",
    ]
    missing = [str(path.relative_to(REPO_ROOT)) for path in required if not path.is_file()]
    assert not missing, f"Missing U.S. Lacey control-plane docs: {missing}"


def test_us_lacey_capability_map_references_existing_paths() -> None:
    assert VALIDATOR.is_file(), (
        "Architecture-doc validator is required at "
        "scripts/validate_us_lacey_architecture_docs.py"
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(VALIDATOR),
            "--repo-root",
            str(REPO_ROOT),
            "--capabilities",
            str(CAPABILITIES),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
