"""CLI for private/local AI-shadow golden evaluation.

Example:
  python -m litoral_trace.lacey_engine.ai_shadow_cli \
    --file tests/fixtures/import_info_wood_brokerage_real.pdf \
    --expected /path/to/private-golden-expectations.json \
    --role SUPPLIER_SHEET
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from .ai_providers import AIProviderConfig, build_ai_provider
from .ai_shadow import dump_golden_metrics, evaluate_golden, reconcile_engine2_with_ai
from .pipeline import process_document


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate AI Extraction Shadow v1 against an Engine 2 golden document.")
    parser.add_argument("--file", required=True, help="Private source document path. The file is not persisted by this CLI.")
    parser.add_argument("--expected", required=True, help="Private JSON file containing {\"expected\": {field: value|null}}.")
    parser.add_argument("--role", default=None, help="Optional Engine 2 role hint.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    source_path = Path(args.file)
    expected_path = Path(args.expected)
    expected_payload = json.loads(expected_path.read_text(encoding="utf-8"))
    expected = expected_payload.get("expected")
    if not isinstance(expected, dict):
        raise SystemExit("Expected JSON must contain an 'expected' object.")
    content = source_path.read_bytes()
    engine2 = process_document(filename=source_path.name, content=content, role_hint=args.role)
    config = AIProviderConfig.from_env()
    provider = build_ai_provider(config)
    if provider is None:
        raise SystemExit("AI shadow is OFF. Set US_LACEY_AI_SHADOW_MODE=SHADOW.")
    ai = provider.extract(filename=source_path.name, content=content)
    comparison = reconcile_engine2_with_ai(engine2=engine2, ai=ai)
    metrics = evaluate_golden(engine2=engine2, ai=ai, expected=expected)
    print(dump_golden_metrics(metrics))
    print(json.dumps({
        "provider": comparison.provider,
        "model": comparison.model,
        "reconciliation": {row.field_key: row.status.value for row in comparison.fields if row.field_key in expected},
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
