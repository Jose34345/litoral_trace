from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from litoral_trace.web.regional_intelligence import (
    DEFAULT_REGIONAL_PROVENANCE,
    DEFAULT_REGIONAL_RISK_CONTEXT,
    REGIONAL_EVIDENCE_DOMAINS,
    REGIONAL_PROFILES,
    EvidenceDomain,
    get_regional_profile,
    get_regional_profile_by_id,
    list_regional_profiles,
)


ROOT = Path(__file__).resolve().parents[1]

WEB_DIR = (
    ROOT
    / "src"
    / "litoral_trace"
    / "web"
)

TEMPLATES = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
    / "public"
)


def test_regional_profiles_have_unique_canonical_identity():
    profiles = list_regional_profiles()

    region_ids = tuple(
        profile.region_id
        for profile in profiles
    )

    slugs = tuple(
        profile.slug
        for profile in profiles
    )

    assert len(region_ids) == len(set(region_ids))
    assert len(slugs) == len(set(slugs))

    assert all(
        profile.region_id
        for profile in profiles
    )

    assert all(
        profile.slug
        for profile in profiles
    )


def test_regional_profiles_keep_web_slug_separate_from_domain_id():
    for profile in REGIONAL_PROFILES:
        assert profile.region_id
        assert profile.slug

        assert (
            profile.region_id.lower()
            != profile.slug.lower()
            or profile.region_id == "ARG"
        )


def test_regional_profiles_have_country_and_jurisdiction_context():
    for profile in REGIONAL_PROFILES:
        assert profile.country_code == "AR"
        assert profile.jurisdiction_code
        assert profile.geographic_scope


def test_canonical_region_lookup_is_normalized():
    profile = get_regional_profile_by_id(
        "  arg-chaco  "
    )

    assert profile is not None
    assert profile.region_id == "ARG-CHACO"
    assert profile.slug == "chaco"

    assert (
        get_regional_profile_by_id(
            "unknown-region"
        )
        is None
    )


def test_slug_lookup_remains_backward_compatible():
    profile = get_regional_profile(
        "  ChAcO  "
    )

    assert profile is not None
    assert profile.region_id == "ARG-CHACO"
    assert profile.slug == "chaco"


def test_evidence_domain_keys_are_unique():
    keys = tuple(
        domain.key
        for domain in REGIONAL_EVIDENCE_DOMAINS
    )

    assert keys == (
        "origin",
        "documentary",
        "geospatial",
        "supply_chain",
        "compliance",
        "auditability",
    )

    assert len(keys) == len(set(keys))


def test_profiles_use_canonical_evidence_domain_contract():
    for profile in REGIONAL_PROFILES:
        assert (
            profile.evidence_domains
            is REGIONAL_EVIDENCE_DOMAINS
        )

        assert len(
            profile.evidence_domains
        ) == 6


def test_evidence_domains_are_complete():
    for domain in REGIONAL_EVIDENCE_DOMAINS:
        assert domain.key
        assert domain.title
        assert domain.description
        assert domain.icon


def test_regional_profile_is_immutable():
    profile = REGIONAL_PROFILES[0]

    with pytest.raises(
        FrozenInstanceError
    ):
        profile.name = "Modified"


def test_evidence_domain_is_immutable():
    domain: EvidenceDomain = (
        REGIONAL_EVIDENCE_DOMAINS[0]
    )

    with pytest.raises(
        FrozenInstanceError
    ):
        domain.title = "Modified"


def test_risk_context_is_immutable():
    with pytest.raises(
        FrozenInstanceError
    ):
        DEFAULT_REGIONAL_RISK_CONTEXT.status = (
            "modified"
        )


def test_provenance_is_immutable():
    with pytest.raises(
        FrozenInstanceError
    ):
        DEFAULT_REGIONAL_PROVENANCE.status = (
            "modified"
        )


def test_no_profile_asserts_transaction_specific_risk():
    forbidden_statuses = {
        "low",
        "medium",
        "high",
        "low_risk",
        "medium_risk",
        "high_risk",
        "compliant",
        "non_compliant",
    }

    for profile in REGIONAL_PROFILES:
        assert (
            profile.risk_context.status
            == "not_assessed"
        )

        assert (
            profile.risk_context.status
            not in forbidden_statuses
        )

        rationale = (
            profile
            .risk_context
            .rationale
            .lower()
        )

        assert (
            "does not constitute"
            in rationale
        )


def test_regional_provenance_is_explicit():
    for profile in REGIONAL_PROFILES:
        provenance = profile.provenance

        assert (
            provenance.status
            == "framework_only"
        )

        assert provenance.label
        assert provenance.freshness_label
        assert provenance.source_scope


def test_regional_index_exposes_canonical_metadata():
    template = (
        TEMPLATES
        / "regional_index.html"
    ).read_text(
        encoding="utf-8"
    )

    assert (
        'data-region-id="{{ profile.region_id }}"'
        in template
    )

    assert (
        "{{ profile.risk_context.label }}"
        in template
    )

    assert (
        "{{ profile.provenance.label }}"
        in template
    )


def test_regional_detail_uses_domain_driven_evidence():
    template = (
        TEMPLATES
        / "regional_detail.html"
    ).read_text(
        encoding="utf-8"
    )

    assert (
        "{% for domain in profile.evidence_domains %}"
        in template
    )

    assert (
        "{{ domain.key }}"
        in template
    )

    assert (
        "{{ domain.title }}"
        in template
    )

    assert (
        "{{ domain.description }}"
        in template
    )

    assert (
        "{{ domain.icon }}"
        in template
    )


def test_regional_detail_does_not_define_evidence_catalog():
    template = (
        TEMPLATES
        / "regional_detail.html"
    ).read_text(
        encoding="utf-8"
    )

    assert (
        "for icon, title, copy"
        not in template
    )

    assert (
        '("fa-location-dot",'
        not in template
    )

    assert (
        '("fa-file-shield",'
        not in template
    )


def test_regional_templates_have_no_region_specific_branching():
    combined = "\n".join(
        (
            (
                TEMPLATES
                / "regional_index.html"
            ).read_text(
                encoding="utf-8"
            ),
            (
                TEMPLATES
                / "regional_detail.html"
            ).read_text(
                encoding="utf-8"
            ),
        )
    )

    forbidden_patterns = (
        "if profile.slug ==",
        "if profile.region_id ==",
        "if profile.name ==",
        "if region_slug ==",
    )

    for pattern in forbidden_patterns:
        assert pattern not in combined


def test_regional_detail_exposes_assessment_boundary():
    template = (
        TEMPLATES
        / "regional_detail.html"
    ).read_text(
        encoding="utf-8"
    )

    assert (
        "{{ profile.risk_context.rationale }}"
        in template
    )

    assert (
        "{{ profile.provenance.source_scope }}"
        in template
    )

    assert (
        "{{ profile.provenance.freshness_label }}"
        in template
    )


def test_regional_business_model_stays_out_of_router():
    router_text = (
        WEB_DIR
        / "router.py"
    ).read_text(
        encoding="utf-8"
    )

    assert (
        "EvidenceDomain("
        not in router_text
    )

    assert (
        "RegionalRiskContext("
        not in router_text
    )

    assert (
        "DataProvenance("
        not in router_text
    )


def test_regional_templates_avoid_unsourced_risk_claims():
    combined = "\n".join(
        (
            (
                TEMPLATES
                / "regional_index.html"
            ).read_text(
                encoding="utf-8"
            ),
            (
                TEMPLATES
                / "regional_detail.html"
            ).read_text(
                encoding="utf-8"
            ),
        )
    ).lower()

    forbidden_claims = (
        "low risk region",
        "medium risk region",
        "high risk region",
        "compliance guaranteed",
        "eudr compliant region",
        "approved region",
    )

    for claim in forbidden_claims:
        assert claim not in combined