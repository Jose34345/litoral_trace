from __future__ import annotations

from litoral_trace.db.models.traceability_evidence_link import TRACEABILITY_EVIDENCE_TYPES
from litoral_trace.web.traceability_evidence import _EVIDENCE_LABELS


def test_p1b_fruit_guide_is_selectable_evidence() -> None:
    assert "FRUIT_GUIDE" in TRACEABILITY_EVIDENCE_TYPES
    assert _EVIDENCE_LABELS["FRUIT_GUIDE"] == "Guía de Frutos"
    assert set(_EVIDENCE_LABELS).issubset(TRACEABILITY_EVIDENCE_TYPES)
