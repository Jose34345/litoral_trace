from __future__ import annotations

import base64
import hashlib
from pathlib import Path
import zlib

from litoral_trace.assurance.extraction import classify_document
from litoral_trace.assurance.parsers import parse_document
from litoral_trace.us_lacey.projection import _fold


_FIXTURE = Path(__file__).parents[1] / "fixtures" / "us_lacey_golden_packet_01.pdf.zlib.b64"
_GOLDEN_SHA256 = "6061e66f3b80b94ed69a87c3295f30ead300898908f2baea0657875388da4c50"


def _golden_packet_bytes() -> bytes:
    compressed = base64.b64decode("".join(_FIXTURE.read_text(encoding="ascii").split()))
    payload = zlib.decompress(compressed)
    assert hashlib.sha256(payload).hexdigest() == _GOLDEN_SHA256
    return payload


def test_diagnostic_shipment_entered_value_table_headers():
    filename = "LitoralTrace_Lacey_Golden_Test_Packet_01.pdf"
    parsed = parse_document(filename, _golden_packet_bytes())
    classification = classify_document(filename, parsed)
    tables = []
    for table_index, table in enumerate(parsed.tables, start=1):
        headers = frozenset(_fold(header) for header in table.headers)
        values = [str(value) for record in table.rows for value in record.values() if value is not None]
        if "$18,600.00" in values:
            tables.append((table_index, sorted(headers), values))
    assert False, repr((classification.document_type.value, classification.evidence, tables))
