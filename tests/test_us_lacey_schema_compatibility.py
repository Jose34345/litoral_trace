from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from litoral_trace.us_lacey import schema_compatibility


class _Result:
    def __init__(self, value):
        self._value = value

    def scalar_one(self):
        if isinstance(self._value, Exception):
            raise self._value
        return self._value


class _Connection:
    def __init__(self, value, statements):
        self._value = value
        self._statements = statements

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def execute(self, statement):
        self._statements.append(str(statement))
        if isinstance(self._value, Exception):
            raise self._value
        return _Result(self._value)


class _Engine:
    def __init__(self, value):
        self._value = value
        self.statements = []

    def connect(self):
        return _Connection(self._value, self.statements)


def test_required_schema_revision_is_repository_canonical_head() -> None:
    assert schema_compatibility.required_us_lacey_schema_revision() == "058_us_lacey_exception_first_states"


def test_schema_probe_is_ready_only_on_exact_canonical_head(monkeypatch) -> None:
    engine = _Engine("058_us_lacey_exception_first_states")
    monkeypatch.setattr(schema_compatibility, "get_us_lacey_engine", lambda: engine)

    assert schema_compatibility.probe_us_lacey_schema_compatibility() is True
    assert engine.statements == ["SELECT version_num FROM alembic_version"]


def test_schema_probe_fails_closed_when_database_is_behind(monkeypatch) -> None:
    engine = _Engine("050_lacey_regulatory_assessment_snapshots")
    monkeypatch.setattr(schema_compatibility, "get_us_lacey_engine", lambda: engine)

    assert schema_compatibility.probe_us_lacey_schema_compatibility() is False


def test_schema_probe_fails_closed_when_database_is_unavailable(monkeypatch) -> None:
    engine = _Engine(RuntimeError("database unavailable"))
    monkeypatch.setattr(schema_compatibility, "get_us_lacey_engine", lambda: engine)

    assert schema_compatibility.probe_us_lacey_schema_compatibility() is False


def test_render_runtime_declares_schema_compatibility_dependency() -> None:
    root = Path(__file__).resolve().parents[1]
    requirements = (root / "requirements.txt").read_text(encoding="utf-8")

    assert "alembic>=1.13,<2" in requirements


def test_release_contract_migrates_neon_before_render_checks_pass_deploy() -> None:
    root = Path(__file__).resolve().parents[1]
    neon = (root / ".github/workflows/us-lacey-neon-live-gate.yml").read_text(
        encoding="utf-8"
    )
    render = (root / "deploy/render-us-lacey-pilot-free.yaml").read_text(
        encoding="utf-8"
    )

    assert "feature/us-lacey-pilot-platform" in neon
    assert 'MODE="migrate"' in neon
    assert "python -m alembic upgrade head" in neon
    assert "US_LACEY_NEON_MIGRATION_DATABASE_URL" in neon
    assert "autoDeployTrigger: checksPass" in render
    assert "US_LACEY_NEON_MIGRATION_DATABASE_URL" not in render
    assert "MIGRATION_DATABASE_URL" not in render
