"""Pydantic-built adversarial Shadow fixtures for fail-closed Lacey QA.

These seeds intentionally contain unsafe or contradictory observations. They are
evaluation-only and must be passed through baseline_refresh before being persisted
as versioned benchmark JSON.
"""
from __future__ import annotations

from litoral_trace.lacey_benchmark.shadow_canonical_diff import (
    BenchmarkFixture,
    Engine2DossierSnapshot,
    Engine2EvidenceSnapshot,
    Engine2FieldSnapshot,
    ExpectedDiff,
    ShipmentTruthSnapshot,
)


ORPHAN_POSITIONAL_FILE = "pack_adversarial_orphan_positional_shadow_canonical.json"
AUTHORITY_CONFLICT_FILE = "pack_adversarial_authority_conflict_shadow_canonical.json"
BROKEN_ARITHMETIC_FILE = "pack_adversarial_broken_arithmetic_shadow_canonical.json"


def _evidence(
    *,
    scope: str,
    value: str,
    source_filename: str,
    line_key: str | None = None,
    component_key: str | None = None,
    language: str = "en",
) -> Engine2EvidenceSnapshot:
    return Engine2EvidenceSnapshot(
        scope=scope,
        line_key=line_key,
        component_key=component_key,
        normalized_value=value,
        source_filename=source_filename,
        page=1,
        evidence_verified=True,
        language=language,
    )


def _field(
    field_key: str,
    *,
    state: str,
    values: tuple[str, ...],
    evidence: tuple[Engine2EvidenceSnapshot, ...],
) -> Engine2FieldSnapshot:
    return Engine2FieldSnapshot(
        field_key=field_key,
        state=state,
        values=values,
        evidence=evidence,
    )


def _seed(
    *,
    pack_id: str,
    description: str,
    fields: tuple[Engine2FieldSnapshot, ...],
) -> BenchmarkFixture:
    return BenchmarkFixture(
        version="p2-shadow-canonical-v2-adversarial",
        pack_id=pack_id,
        description=description,
        shadow=Engine2DossierSnapshot(
            availability="CURRENT",
            fields=fields,
        ),
        canonical=ShipmentTruthSnapshot(),
        expected_diff=ExpectedDiff(
            comparable_slots=0,
            agreement_count=0,
            shadow_supported_but_canonical_missing=0,
            canonical_supported_but_shadow_missing=0,
            false_conflict_count=0,
            safe_review_count=0,
        ),
    )


def orphan_positional_seed() -> BenchmarkFixture:
    """Two same-HTS products plus positional quantities with no binding proof."""
    invoice = "01_Commercial_Invoice_Orphans.pdf"
    botanical = "02_Botanical_Declaration_Orphans.pdf"
    return _seed(
        pack_id="pack-adversarial-orphan-positional",
        description=(
            "Two distinct taxa share HTS 4407990190 while invoice quantities are "
            "position-only observations. Canonical must not guess quantity-to-taxon joins."
        ),
        fields=(
            _field(
                "hts_code",
                state="SUPPORTED_MULTIPLE",
                values=("4407990190",),
                evidence=(
                    _evidence(
                        scope="MERCHANDISE_LINE",
                        value="4407990190",
                        source_filename=invoice,
                        line_key="invoice:items:row:1",
                    ),
                    _evidence(
                        scope="MERCHANDISE_LINE",
                        value="4407990190",
                        source_filename=invoice,
                        line_key="invoice:items:row:2",
                    ),
                ),
            ),
            _field(
                "species",
                state="SUPPORTED_MULTIPLE",
                values=("Pinus taeda", "Eucalyptus grandis"),
                evidence=(
                    _evidence(
                        scope="PLANT_COMPONENT",
                        value="Pinus taeda",
                        source_filename=botanical,
                        component_key="taxon:pinus:taeda",
                    ),
                    _evidence(
                        scope="PLANT_COMPONENT",
                        value="Eucalyptus grandis",
                        source_filename=botanical,
                        component_key="taxon:eucalyptus:grandis",
                    ),
                ),
            ),
            _field(
                "plant_quantity",
                state="SUPPORTED_MULTIPLE",
                values=("20", "15"),
                evidence=(
                    _evidence(
                        scope="PLANT_COMPONENT",
                        value="20",
                        source_filename=invoice,
                    ),
                    _evidence(
                        scope="PLANT_COMPONENT",
                        value="15",
                        source_filename=invoice,
                    ),
                ),
            ),
            _field(
                "metric_unit",
                state="SUPPORTED_MULTIPLE",
                values=("m3",),
                evidence=(
                    _evidence(
                        scope="PLANT_COMPONENT",
                        value="m3",
                        source_filename=invoice,
                    ),
                    _evidence(
                        scope="PLANT_COMPONENT",
                        value="m3",
                        source_filename=invoice,
                    ),
                ),
            ),
        ),
    )


def authority_conflict_seed() -> BenchmarkFixture:
    """One explicit line carries two incompatible botanical identities."""
    invoice = "01_Commercial_Invoice_Conflict.pdf"
    botanical = "02_Botanical_Declaration_Conflict.pdf"
    return _seed(
        pack_id="pack-adversarial-authority-conflict",
        description=(
            "LINE:1 is explicitly associated with incompatible Eucalyptus grandis "
            "and Tectona grandis observations. Canonical must preserve a hard review conflict."
        ),
        fields=(
            _field(
                "hts_code",
                state="SUPPORTED",
                values=("4407990190",),
                evidence=(
                    _evidence(
                        scope="MERCHANDISE_LINE",
                        value="4407990190",
                        source_filename=invoice,
                        line_key="LINE:1",
                    ),
                ),
            ),
            _field(
                "entered_value",
                state="SUPPORTED",
                values=("9800",),
                evidence=(
                    _evidence(
                        scope="MERCHANDISE_LINE",
                        value="9800",
                        source_filename=invoice,
                        line_key="LINE:1",
                    ),
                ),
            ),
            _field(
                "genus",
                state="SUPPORTED_MULTIPLE",
                values=("Eucalyptus", "Tectona"),
                evidence=(
                    _evidence(
                        scope="PLANT_COMPONENT",
                        value="Eucalyptus",
                        source_filename=invoice,
                        line_key="LINE:1",
                        component_key="LINE:1",
                    ),
                    _evidence(
                        scope="PLANT_COMPONENT",
                        value="Tectona",
                        source_filename=botanical,
                        line_key="LINE:1",
                        component_key="LINE:1",
                    ),
                ),
            ),
            _field(
                "species",
                state="SUPPORTED_MULTIPLE",
                values=("Eucalyptus grandis", "Tectona grandis"),
                evidence=(
                    _evidence(
                        scope="PLANT_COMPONENT",
                        value="Eucalyptus grandis",
                        source_filename=invoice,
                        line_key="LINE:1",
                        component_key="LINE:1",
                    ),
                    _evidence(
                        scope="PLANT_COMPONENT",
                        value="Tectona grandis",
                        source_filename=botanical,
                        line_key="LINE:1",
                        component_key="LINE:1",
                    ),
                ),
            ),
        ),
    )


def broken_arithmetic_seed() -> BenchmarkFixture:
    """One declarable line plus two contradictory shipment totals."""
    invoice = "01_Commercial_Invoice_Totals.pdf"
    bol = "02_Bill_of_Lading_Totals.pdf"
    botanical = "03_Botanical_Declaration_Totals.pdf"
    line_key = "SKU:EG-ONE-01"
    taxon = "taxon:eucalyptus:grandis"
    return _seed(
        pack_id="pack-adversarial-broken-arithmetic",
        description=(
            "One declarable Eucalyptus line has two distinct shipment-level entered "
            "value totals. The one-line arithmetic inference must abort."
        ),
        fields=(
            _field(
                "hts_code",
                state="SUPPORTED",
                values=("4407990190",),
                evidence=(
                    _evidence(
                        scope="MERCHANDISE_LINE",
                        value="4407990190",
                        source_filename=invoice,
                        line_key=line_key,
                    ),
                ),
            ),
            _field(
                "species",
                state="SUPPORTED",
                values=("Eucalyptus grandis",),
                evidence=(
                    _evidence(
                        scope="PLANT_COMPONENT",
                        value="Eucalyptus grandis",
                        source_filename=botanical,
                        line_key=line_key,
                        component_key=taxon,
                    ),
                ),
            ),
            _field(
                "plant_quantity",
                state="SUPPORTED",
                values=("12",),
                evidence=(
                    _evidence(
                        scope="PLANT_COMPONENT",
                        value="12",
                        source_filename=botanical,
                        line_key=line_key,
                        component_key=taxon,
                    ),
                ),
            ),
            _field(
                "metric_unit",
                state="SUPPORTED",
                values=("m3",),
                evidence=(
                    _evidence(
                        scope="PLANT_COMPONENT",
                        value="m3",
                        source_filename=botanical,
                        line_key=line_key,
                        component_key=taxon,
                    ),
                ),
            ),
            _field(
                "shipment_total_entered_value",
                state="SUPPORTED_MULTIPLE",
                values=("12000", "12750"),
                evidence=(
                    _evidence(
                        scope="SHIPMENT",
                        value="12000",
                        source_filename=invoice,
                    ),
                    _evidence(
                        scope="SHIPMENT",
                        value="12750",
                        source_filename=bol,
                    ),
                ),
            ),
        ),
    )


def adversarial_fixture_seeds() -> dict[str, BenchmarkFixture]:
    return {
        ORPHAN_POSITIONAL_FILE: orphan_positional_seed(),
        AUTHORITY_CONFLICT_FILE: authority_conflict_seed(),
        BROKEN_ARITHMETIC_FILE: broken_arithmetic_seed(),
    }
