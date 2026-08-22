"""Mapper-level regression for tenant-safe traceability relationships."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def test_traceability_mappers_configure_without_sqlalchemy_warnings() -> None:
    """Composite tenant FKs must be explicit enough for warning-free mapper setup."""

    env = os.environ.copy()
    src = str(ROOT / "src")
    env["PYTHONPATH"] = (
        src
        if not env.get("PYTHONPATH")
        else src + os.pathsep + env["PYTHONPATH"]
    )

    probe = r'''
import warnings
from sqlalchemy.exc import SAWarning
from sqlalchemy.orm import configure_mappers

warnings.simplefilter("error", SAWarning)
import litoral_trace.db.models  # noqa: F401,E402
configure_mappers()
'''

    completed = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == 0, (
        "Traceability ORM mapper configuration emitted an SAWarning or failed:\n"
        f"STDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
    )
