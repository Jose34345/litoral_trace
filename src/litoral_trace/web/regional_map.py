"""Territorial presentation model for Regional Intelligence.

P2.FE-B5.3C adds a map-oriented presentation layer on top of the
canonical regional compliance model introduced in P2.FE-B5.3B.

This module deliberately contains no compliance-risk assessment.

Territorial selection means geographic/navigation context only.
It must not be interpreted as supplier, plot, shipment, DDS, or
transaction-specific risk.
"""

from __future__ import annotations

from dataclasses import dataclass

from litoral_trace.web.regional_intelligence import (
    get_regional_profile_by_id,
    list_regional_profiles,
)


@dataclass(
    frozen=True,
    slots=True,
)
class TerritorialDataset:
    """Metadata for a locally versioned territorial geometry snapshot."""

    key: str
    source_name: str
    source_url: str
    static_path: str
    snapshot_date: str
    expected_feature_count: int
    feature_name_property: str


@dataclass(
    frozen=True,
    slots=True,
)
class RegionalMapDefinition:
    """Map-only binding between a canonical region and Georef features.

    ``region_id`` belongs to the canonical Litoral Trace regional model.

    ``georef_names`` identifies administrative geometries only and is
    intentionally kept outside the compliance domain.
    """

    region_id: str
    scope_kind: str
    georef_names: tuple[str, ...]
    includes_all_provinces: bool = False


@dataclass(
    frozen=True,
    slots=True,
)
class RegionalMapScope:
    """Resolved map presentation scope.

    Public labels and slugs come from ``RegionalProfile`` so that the
    visualization layer cannot silently create a second regional catalog.
    """

    region_id: str
    slug: str
    name: str
    geographic_scope: str

    scope_kind: str
    georef_names: tuple[str, ...]
    includes_all_provinces: bool


REGIONAL_MAP_DATASET = TerritorialDataset(
    key="argentina-georef-provinces-2026-08-16",
    source_name="Argentina Georef",
    source_url=(
        "https://apis.datos.gob.ar/georef/"
        "api/v2.0/provincias.geojson"
    ),
    static_path=(
        "/static/data/georef/"
        "provincias.geojson"
    ),
    snapshot_date="2026-08-16",
    expected_feature_count=24,
    feature_name_property="nombre",
)


REGIONAL_MAP_DEFINITIONS: tuple[
    RegionalMapDefinition,
    ...,
] = (
    RegionalMapDefinition(
        region_id="ARG-CHACO",
        scope_kind="province",
        georef_names=(
            "Chaco",
        ),
    ),
    RegionalMapDefinition(
        region_id="ARG-CORRIENTES",
        scope_kind="province",
        georef_names=(
            "Corrientes",
        ),
    ),
    RegionalMapDefinition(
        region_id="ARG-MISIONES",
        scope_kind="province",
        georef_names=(
            "Misiones",
        ),
    ),
    RegionalMapDefinition(
        region_id="ARG-NEA",
        scope_kind="aggregate",
        georef_names=(
            "Chaco",
            "Corrientes",
            "Formosa",
            "Misiones",
        ),
    ),
    RegionalMapDefinition(
        region_id="ARG",
        scope_kind="country",
        georef_names=(),
        includes_all_provinces=True,
    ),
)


_DEFINITION_BY_REGION_ID = {
    definition.region_id: definition
    for definition in REGIONAL_MAP_DEFINITIONS
}


def _build_regional_map_scopes() -> tuple[
    RegionalMapScope,
    ...,
]:
    """Resolve map bindings through the canonical regional catalog."""

    scopes: list[
        RegionalMapScope
    ] = []

    for profile in list_regional_profiles():
        definition = (
            _DEFINITION_BY_REGION_ID.get(
                profile.region_id
            )
        )

        if definition is None:
            raise RuntimeError(
                "Regional map configuration missing "
                f"for canonical region "
                f"{profile.region_id!r}."
            )

        scopes.append(
            RegionalMapScope(
                region_id=(
                    profile.region_id
                ),
                slug=profile.slug,
                name=profile.name,
                geographic_scope=(
                    profile.geographic_scope
                ),
                scope_kind=(
                    definition.scope_kind
                ),
                georef_names=(
                    definition.georef_names
                ),
                includes_all_provinces=(
                    definition
                    .includes_all_provinces
                ),
            )
        )

    return tuple(
        scopes
    )


REGIONAL_MAP_SCOPES: tuple[
    RegionalMapScope,
    ...,
] = _build_regional_map_scopes()


_SCOPE_BY_REGION_ID = {
    scope.region_id: scope
    for scope in REGIONAL_MAP_SCOPES
}


def list_regional_map_scopes() -> tuple[
    RegionalMapScope,
    ...,
]:
    """Return map scopes in canonical Regional Intelligence order."""

    return REGIONAL_MAP_SCOPES


def get_regional_map_scope(
    region_id: str,
) -> RegionalMapScope | None:
    """Resolve one territorial map scope by canonical region identity."""

    normalized = (
        region_id.strip().upper()
    )

    return _SCOPE_BY_REGION_ID.get(
        normalized
    )


def get_regional_map_scope_for_slug(
    slug: str,
) -> RegionalMapScope | None:
    """Resolve a territorial scope through the canonical public profile."""

    normalized_slug = (
        slug.strip().lower()
    )

    profile = next(
        (
            candidate
            for candidate
            in list_regional_profiles()
            if (
                candidate.slug
                == normalized_slug
            )
        ),
        None,
    )

    if profile is None:
        return None

    return get_regional_map_scope(
        profile.region_id
    )


def validate_regional_map_contract() -> None:
    """Fail fast when map configuration drifts from the canonical catalog."""

    canonical_region_ids = {
        profile.region_id
        for profile
        in list_regional_profiles()
    }

    map_region_ids = {
        definition.region_id
        for definition
        in REGIONAL_MAP_DEFINITIONS
    }

    if (
        canonical_region_ids
        != map_region_ids
    ):
        missing = (
            canonical_region_ids
            - map_region_ids
        )

        unknown = (
            map_region_ids
            - canonical_region_ids
        )

        raise RuntimeError(
            "Regional map contract does not match "
            "the canonical regional catalog. "
            f"Missing={sorted(missing)!r}; "
            f"unknown={sorted(unknown)!r}."
        )

    for definition in (
        REGIONAL_MAP_DEFINITIONS
    ):
        if (
            definition.scope_kind
            not in {
                "province",
                "aggregate",
                "country",
            }
        ):
            raise RuntimeError(
                "Unsupported regional map "
                f"scope kind "
                f"{definition.scope_kind!r}."
            )

        if (
            definition.scope_kind
            == "country"
            and not (
                definition
                .includes_all_provinces
            )
        ):
            raise RuntimeError(
                "Country territorial scope must "
                "include all provincial features."
            )

        if (
            definition.scope_kind
            != "country"
            and not definition.georef_names
        ):
            raise RuntimeError(
                "Non-country territorial scope "
                "must identify at least one "
                "Georef feature."
            )

        profile = (
            get_regional_profile_by_id(
                definition.region_id
            )
        )

        if profile is None:
            raise RuntimeError(
                "Regional map definition references "
                "an unknown canonical region "
                f"{definition.region_id!r}."
            )


validate_regional_map_contract()