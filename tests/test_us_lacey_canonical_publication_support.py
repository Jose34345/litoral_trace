from __future__ import annotations

from types import SimpleNamespace

from litoral_trace.us_lacey.canonical_publication_support import (
    derive_article_component,
    is_safe_provisional_machine_line,
)


def _field(
    *,
    value: str | None = None,
    extractor: str | None = None,
    source_document_id: int | None = None,
    human_value: str | None = None,
    reviewed_at=None,
    reviewed_by_user_id: int | None = None,
):
    return SimpleNamespace(
        normalized_value=value,
        original_value=value,
        extractor=extractor,
        source_assurance_document_id=source_document_id,
        human_value=human_value,
        reviewed_at=reviewed_at,
        reviewed_by_user_id=reviewed_by_user_id,
    )


def test_article_component_is_derived_only_by_exact_taxon_subtraction() -> None:
    assert (
        derive_article_component(
            "Tablas secas / KD boards - Pinus taeda",
            "taxon:pinus:taeda",
        )
        == "Tablas secas / KD boards"
    )
    assert (
        derive_article_component(
            "Pinus taeda KD sawn boards",
            "taxon:pinus:taeda",
        )
        == "KD sawn boards"
    )
    assert derive_article_component("Pinus boards", "taxon:pinus:taeda") is None
    assert derive_article_component("Pinus taeda", "taxon:pinus:taeda") is None


def test_numeric_surplus_line_with_only_machine_provenance_is_safe_to_compact() -> None:
    fields = [
        _field(),
        _field(
            value="Eucalyptus",
            extractor="assurance-deterministic-parser",
            source_document_id=39,
        ),
        _field(
            value="4407990190",
            extractor="us-lacey-deterministic-projector",
            source_document_id=40,
        ),
    ]

    assert is_safe_provisional_machine_line(line_reference="3", fields=fields) is True


def test_customer_or_reviewed_line_is_never_safe_to_compact() -> None:
    assert (
        is_safe_provisional_machine_line(
            line_reference="CUSTOMER-LINE-3",
            fields=[
                _field(
                    value="Eucalyptus",
                    extractor="assurance-deterministic-parser",
                    source_document_id=39,
                )
            ],
        )
        is False
    )
    assert (
        is_safe_provisional_machine_line(
            line_reference="3",
            fields=[
                _field(
                    value="Eucalyptus",
                    extractor="assurance-deterministic-parser",
                    source_document_id=39,
                    human_value="Eucalyptus",
                )
            ],
        )
        is False
    )


def test_unknown_writer_or_empty_numeric_line_fails_closed() -> None:
    assert (
        is_safe_provisional_machine_line(
            line_reference="3",
            fields=[_field(value="Eucalyptus", extractor="customer-import", source_document_id=39)],
        )
        is False
    )
    assert is_safe_provisional_machine_line(line_reference="3", fields=[_field()]) is False
