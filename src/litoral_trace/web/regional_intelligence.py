"""Public Regional Intelligence catalog.

P2.FE-B5.3A establishes the public information architecture.
Detailed source-backed regional content is layered on top in
subsequent B5.3 phases without duplicating route or template logic.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(
    frozen=True,
    slots=True,
)
class RegionalProfile:
    """Immutable public metadata for one regional profile."""

    slug: str
    name: str
    headline: str
    summary: str
    icon: str
    geographic_scope: str
    focus_areas: tuple[str, ...]


REGIONAL_PROFILES: tuple[
    RegionalProfile,
    ...,
] = (
    RegionalProfile(
        slug="chaco",
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
    ),
    RegionalProfile(
        slug="corrientes",
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
    ),
    RegionalProfile(
        slug="misiones",
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
    ),
    RegionalProfile(
        slug="nea",
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
    ),
    RegionalProfile(
        slug="argentina",
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
    ),
)


_PROFILE_BY_SLUG = {
    profile.slug: profile
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
