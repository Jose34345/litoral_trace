from __future__ import annotations

import argparse
from pathlib import Path
import sys
import tomllib
from typing import Any


PATH_FIELDS = ("implementation", "models", "migrations", "tests", "workflows")


def _resolve_inside_repo(repo_root: Path, raw_path: str) -> tuple[Path | None, str | None]:
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return None, f"absolute paths are not allowed: {raw_path}"

    resolved = (repo_root / candidate).resolve()
    try:
        resolved.relative_to(repo_root)
    except ValueError:
        return None, f"path escapes repository root: {raw_path}"
    return resolved, None


def validate_capabilities(repo_root: Path, capabilities_path: Path) -> list[str]:
    repo_root = repo_root.resolve()
    capabilities_path = capabilities_path.resolve()
    errors: list[str] = []

    if not capabilities_path.is_file():
        return [f"capability map does not exist: {capabilities_path}"]

    try:
        with capabilities_path.open("rb") as handle:
            payload: dict[str, Any] = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        return [f"cannot load capability map: {exc}"]

    capabilities = payload.get("capabilities")
    if not isinstance(capabilities, dict) or not capabilities:
        return ["capability map must contain a non-empty [capabilities] table"]

    for capability_name, capability in sorted(capabilities.items()):
        if not isinstance(capability, dict):
            errors.append(f"capability {capability_name!r} must be a table")
            continue

        for field in PATH_FIELDS:
            values = capability.get(field, [])
            if not isinstance(values, list):
                errors.append(
                    f"capability {capability_name!r} field {field!r} must be a list"
                )
                continue

            for raw_path in values:
                if not isinstance(raw_path, str) or not raw_path.strip():
                    errors.append(
                        f"capability {capability_name!r} field {field!r} contains "
                        "a non-string or empty path"
                    )
                    continue

                resolved, path_error = _resolve_inside_repo(repo_root, raw_path)
                if path_error:
                    errors.append(
                        f"capability {capability_name!r} field {field!r}: {path_error}"
                    )
                    continue

                assert resolved is not None
                if not resolved.exists():
                    errors.append(
                        f"capability {capability_name!r} field {field!r} references "
                        f"missing path: {raw_path}"
                    )

    return errors


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate path references in the U.S. Lacey capability map."
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root (defaults to the parent of scripts/).",
    )
    parser.add_argument(
        "--capabilities",
        type=Path,
        default=None,
        help="Capability TOML path (defaults to docs/us-lacey/CAPABILITIES.toml).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    repo_root = args.repo_root.resolve()
    capabilities_path = args.capabilities
    if capabilities_path is None:
        capabilities_path = repo_root / "docs" / "us-lacey" / "CAPABILITIES.toml"

    errors = validate_capabilities(repo_root, capabilities_path)
    if errors:
        print("U.S. Lacey architecture capability map validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print("U.S. Lacey architecture capability map validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
