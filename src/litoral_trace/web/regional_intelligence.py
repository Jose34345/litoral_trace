"""Public Regional Intelligence catalog.

P2.FE-B5.3A established the public Regional Intelligence
information architecture.

P2.FE-B5.3B evolves that foundation into a presentation-independent
regional compliance model. The model is intentionally reusable by
future exporter workspaces, consultant workflows, supplier and buyer
portals, API schemas, and regulatory adapters.

Regional context must not be interpreted as a transaction-, supplier-,
plot-, shipment-, or due-diligence-specific compliance conclusion.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(
    frozen=True,
    slots=True,
)
class EvidenceDomain:
    """One reusable compliance-evidence domain."""

    key: str
    title: str
    description: str
    icon: str


@dataclass(
    frozen=True,
    slots=True,
)
class RegionalRiskContext:
    """Assessment boundary for a regional profile."""

    status: str
    label: str
    rationale: str


@dataclass(
    frozen=True,
    slots=True,
)
class DataProvenance:
    """Source and freshness metadata for regional intelligence."""

    status: str
    label: str
    freshness_label: str
    source_scope: str


@dataclass(
    frozen=True,
    slots=True,
)
class RegionalProfile:
    """Immutable regional compliance context.

    ``region_id`` is the canonical internal identity.

    ``slug`` is only the public web-navigation identifier and must
    therefore not be used as the durable domain identity by future
    integrations.
    """

    region_id: str
    slug: str

    country_code: str
    jurisdiction_code: str

    name: str
    headline: str
    summary: str
    icon: str

    geographic_scope: str
    focus_areas: tuple[str, ...]

    evidence_domains: tuple[
        EvidenceDomain,
        ...,
    ]

    risk_context: RegionalRiskContext
    provenance: DataProvenance


REGIONAL_EVIDENCE_DOMAINS: tuple[
    EvidenceDomain,
    ...,
] = (
    EvidenceDomain(
        key="origin",
        title="Origin context",
        description=(
            "Structure evidence around the declared "
            "geographic origin of forest material."
        ),
        icon="fa-location-dot",
    ),
    EvidenceDomain(
        key="documentary",
        title="Documentary evidence",
        description=(
            "Connect supporting records to origin, "
            "actors, products, and compliance workflows."
        ),
        icon="fa-file-shield",
    ),
    EvidenceDomain(
        key="geospatial",
        title="Geospatial context",
        description=(
            "Organize geospatial observations alongside "
            "the declared origin and evidence chain."
        ),
        icon="fa-satellite",
    ),
    EvidenceDomain(
        key="supply_chain",
        title="Supply-chain context",
        description=(
            "Preserve relevant evidence context as "
            "material moves through operational workflows."
        ),
        icon="fa-route",
    ),
    EvidenceDomain(
        key="compliance",
        title="Compliance context",
        description=(
            "Keep regulatory and due-diligence context "
            "connected to reviewable evidence."
        ),
        icon="fa-scale-balanced",
    ),
    EvidenceDomain(
        key="auditability",
        title="Auditability",
        description=(
            "Maintain evidence in a structured and "
            "reviewable architecture suitable for audit."
        ),
        icon="fa-box-archive",
    ),
)


DEFAULT_REGIONAL_RISK_CONTEXT = RegionalRiskContext(
    status="not_assessed",
    label="Regional context only",
    rationale=(
        "Regional Intelligence provides contextual information "
        "and does not constitute a transaction-, supplier-, plot-, "
        "shipment-, or due-diligence-specific risk conclusion."
    ),
)


DEFAULT_REGIONAL_PROVENANCE = DataProvenance(
    status="framework_only",
    label="Evidence framework",
    freshness_label="No dated external dataset attached",
    source_scope=(
        "The public profile currently exposes the Litoral Trace "
        "evidence architecture. Jurisdiction-specific sourced "
        "datasets are incorporated separately when validated."
    ),
)


REGIONAL_PROFILES: tuple[
    RegionalProfile,
    ...,
] = (
    RegionalProfile(
        region_id="ARG-CHACO",
        slug="chaco",
        country_code="AR",
        jurisdiction_code="ARG-CHACO",
        name="Chaco",
        headline="Native forest & origin",
        summary=(
            "Origin context for forest supply chains "
            "in the Gran Chaco."
        ),
        icon="fa-tree",
        geographic_scope="Provincial profile",
        focus_areas=(
            "Origin evidence",
            "Geospatial context",
            "Documentary chain",
        ),
        evidence_domains=REGIONAL_EVIDENCE_DOMAINS,
        risk_context=DEFAULT_REGIONAL_RISK_CONTEXT,
        provenance=DEFAULT_REGIONAL_PROVENANCE,
    ),
    RegionalProfile(
        region_id="ARG-CORRIENTES",
        slug="corrientes",
        country_code="AR",
        jurisdiction_code="ARG-CORRIENTES",
        name="Corrientes",
        headline="Plantation forestry",
        summary=(
            "Regional context for plantation-based "
            "forestry supply chains."
        ),
        icon="fa-seedling",
        geographic_scope="Provincial profile",
        focus_areas=(
            "Origin evidence",
            "Plantation context",
            "Documentary chain",
        ),
        evidence_domains=REGIONAL_EVIDENCE_DOMAINS,
        risk_context=DEFAULT_REGIONAL_RISK_CONTEXT,
        provenance=DEFAULT_REGIONAL_PROVENANCE,
    ),
    RegionalProfile(
        region_id="ARG-MISIONES",
        slug="misiones",
        country_code="AR",
        jurisdiction_code="ARG-MISIONES",
        name="Misiones",
        headline="Forestry & industry",
        summary=(
            "Origin and industrial context for "
            "forestry supply chains."
        ),
        icon="fa-industry",
        geographic_scope="Provincial profile",
        focus_areas=(
            "Origin evidence",
            "Industrial context",
            "Documentary chain",
        ),
        evidence_domains=REGIONAL_EVIDENCE_DOMAINS,
        risk_context=DEFAULT_REGIONAL_RISK_CONTEXT,
        provenance=DEFAULT_REGIONAL_PROVENANCE,
    ),
    RegionalProfile(
        region_id="ARG-NEA",
        slug="nea",
        country_code="AR",
        jurisdiction_code="ARG-NEA",
        name="NEA",
        headline="Regional supply chain",
        summary=(
            "Cross-provincial context for "
            "Northeast Argentina."
        ),
        icon="fa-route",
        geographic_scope="Regional profile",
        focus_areas=(
            "Cross-provincial origins",
            "Supply-chain context",
            "Evidence continuity",
        ),
        evidence_domains=REGIONAL_EVIDENCE_DOMAINS,
        risk_context=DEFAULT_REGIONAL_RISK_CONTEXT,
        provenance=DEFAULT_REGIONAL_PROVENANCE,
    ),
    RegionalProfile(
        region_id="ARG",
        slug="argentina",
        country_code="AR",
        jurisdiction_code="ARG",
        name="Argentina",
        headline="National framework",
        summary=(
            "National compliance and export context "
            "for Argentine origins."
        ),
        icon="fa-flag",
        geographic_scope="National profile",
        focus_areas=(
            "National origin context",
            "Compliance framework",
            "Export evidence",
        ),
        evidence_domains=REGIONAL_EVIDENCE_DOMAINS,
        risk_context=DEFAULT_REGIONAL_RISK_CONTEXT,
        provenance=DEFAULT_REGIONAL_PROVENANCE,
    ),
)


_PROFILE_BY_SLUG = {
    profile.slug: profile
    for profile in REGIONAL_PROFILES
}


_PROFILE_BY_REGION_ID = {
    profile.region_id: profile
    for profile in REGIONAL_PROFILES
}


def list_regional_profiles() -> tuple[
    RegionalProfile,
    ...,
]:
    """Return Regional Intelligence profiles in display order."""

    return REGIONAL_PROFILES


def get_regional_profile(
    slug: str,
) -> RegionalProfile | None:
    """Resolve a public regional profile by normalized slug."""

    normalized = slug.strip().lower()

    return _PROFILE_BY_SLUG.get(
        normalized
    )


def get_regional_profile_by_id(
    region_id: str,
) -> RegionalProfile | None:
    """Resolve a profile by canonical regional identity."""

    normalized = region_id.strip().upper()

    return _PROFILE_BY_REGION_ID.get(
        normalized
    )