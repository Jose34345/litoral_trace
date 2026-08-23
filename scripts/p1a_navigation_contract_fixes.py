"""Update legacy navigation expectations for the P1-A Integraciones module.

Temporary CI helper. It changes tests only and fails closed if an expected
pre-P1-A contract is not present exactly once.
"""
from __future__ import annotations

from pathlib import Path


REPLACEMENTS: dict[str, list[tuple[str, str]]] = {
    "tests/test_batch_p24g_ui_unittest.py": [
        (
            '        ("evidence", "/evidence"),\n        ("settings", "/settings"),',
            '        ("evidence", "/evidence"),\n        ("integrations", "/integrations"),\n        ("settings", "/settings"),',
        ),
    ],
    "tests/test_frontend_p2fea_unittest.py": [
        (
            '        "evidence",\n        "settings",\n    ]\n\n    superadmin_nav =',
            '        "evidence",\n        "integrations",\n        "settings",\n    ]\n\n    superadmin_nav =',
        ),
        (
            '        "evidence",\n        "settings",\n        "platform",',
            '        "evidence",\n        "integrations",\n        "settings",\n        "platform",',
        ),
    ],
    "tests/test_frontend_p2feb4_unittest.py": [
        (
            '        ("evidence", "/evidence"),\n        ("settings", "/settings"),',
            '        ("evidence", "/evidence"),\n        ("integrations", "/integrations"),\n        ("settings", "/settings"),',
        ),
    ],
}


def main() -> None:
    for raw_path, replacements in REPLACEMENTS.items():
        path = Path(raw_path)
        text = path.read_text(encoding="utf-8")
        for old, new in replacements:
            count = text.count(old)
            if count != 1:
                raise SystemExit(
                    f"{raw_path}: expected replacement anchor exactly once; found {count}"
                )
            text = text.replace(old, new, 1)
        path.write_text(text, encoding="utf-8")
        print(f"updated {raw_path}")


if __name__ == "__main__":
    main()
