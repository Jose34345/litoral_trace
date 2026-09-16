from __future__ import annotations

from pathlib import Path

TARGET = Path("src/litoral_trace/us_lacey/canonical_shipment_truth.py")
SELF = Path("scripts/_apply_cross_document_line_identity_patch.py")
WORKFLOW = Path(".github/workflows/_cross_document_line_identity_apply.yml")

text = TARGET.read_text(encoding="utf-8")

replacements = (
    (
        "from litoral_trace.us_lacey.ppq505 import (\n",
        "from litoral_trace.us_lacey.cross_document_line_identity import (\n"
        "    reconcile_cross_document_line_identity,\n"
        ")\n"
        "from litoral_trace.us_lacey.ppq505 import (\n",
    ),
    (
        'CANONICAL_PUBLISHER_VERSION = "lacey_canonical_shipment_truth_v1"',
        'CANONICAL_PUBLISHER_VERSION = "lacey_canonical_shipment_truth_v2"',
    ),
    (
        'def build_canonical_shipment_truth(payload: Mapping) -> CanonicalShipmentTruth:\n'
        '    """Build one fail-closed shipment/plant-line truth from Engine 2 JSON."""\n'
        '    fields_payload = payload.get("canonical_fields")',
        'def build_canonical_shipment_truth(payload: Mapping) -> CanonicalShipmentTruth:\n'
        '    """Build one fail-closed shipment/plant-line truth from Engine 2 JSON."""\n'
        '    payload = reconcile_cross_document_line_identity(payload)\n'
        '    fields_payload = payload.get("canonical_fields")',
    ),
)

for old, new in replacements:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected exactly one match, found {count}: {old[:80]!r}")
    text = text.replace(old, new, 1)

TARGET.write_text(text, encoding="utf-8")
SELF.unlink()
WORKFLOW.unlink()
