from __future__ import annotations

import json

from dataclasses import (
    FrozenInstanceError,
    fields,
)

from pathlib import Path

import pytest


from litoral_trace.web.regional_intelligence import (
    REGIONAL_PROFILES,
    get_regional_profile_by_id,
)

from litoral_trace.web.regional_map import (
    REGIONAL_MAP_DATASET,
    REGIONAL_MAP_DEFINITIONS,
    REGIONAL_MAP_SCOPES,
    RegionalMapDefinition,
    RegionalMapScope,
    TerritorialDataset,
    get_regional_map_scope,
    get_regional_map_scope_for_slug,
    list_regional_map_scopes,
    validate_regional_map_contract,
)


ROOT = (
    Path(__file__).resolve().parents[1]
)

GEOREF_SNAPSHOT = (
    ROOT
    / "src"
    / "litoral_trace"
    / "static"
    / "data"
    / "georef"
    / "provincias.geojson"
)

PUBLIC_TEMPLATES = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
    / "public"
)

REGIONAL_INDEX_TEMPLATE = (
    PUBLIC_TEMPLATES
    / "regional_index.html"
)

REGIONAL_DETAIL_TEMPLATE = (
    PUBLIC_TEMPLATES
    / "regional_detail.html"
)

REGIONAL_MAP_JS = (
    ROOT
    / "src"
    / "litoral_trace"
    / "static"
    / "src"
    / "js"
    / "regional_intelligence_map.js"
)

WEB_ROUTER = (
    ROOT
    / "src"
    / "litoral_trace"
    / "web"
    / "router.py"
)


def _load_georef_snapshot() -> dict:
    return json.loads(
        GEOREF_SNAPSHOT.read_text(
            encoding="utf-8"
        )
    )


def _features_by_name() -> dict[
    str,
    dict,
]:
    snapshot = (
        _load_georef_snapshot()
    )

    return {
        str(
            feature.get(
                "properties",
                {},
            ).get(
                REGIONAL_MAP_DATASET
                .feature_name_property,
                "",
            )
        ).strip(): feature
        for feature
        in snapshot.get(
            "features",
            [],
        )
    }


def test_regional_map_contract_matches_canonical_catalog():
    validate_regional_map_contract()

    canonical_ids = tuple(
        profile.region_id
        for profile
        in REGIONAL_PROFILES
    )

    map_ids = tuple(
        scope.region_id
        for scope
        in list_regional_map_scopes()
    )

    assert (
        map_ids
        == canonical_ids
    )


def test_map_scope_public_identity_is_derived_from_canonical_profile():
    for scope in REGIONAL_MAP_SCOPES:
        profile = (
            get_regional_profile_by_id(
                scope.region_id
            )
        )

        assert profile is not None

        assert (
            scope.slug
            == profile.slug
        )

        assert (
            scope.name
            == profile.name
        )

        assert (
            scope.geographic_scope
            == profile.geographic_scope
        )


def test_map_definitions_have_unique_region_ids():
    region_ids = tuple(
        definition.region_id
        for definition
        in REGIONAL_MAP_DEFINITIONS
    )

    assert len(
        region_ids
    ) == len(
        set(region_ids)
    )


def test_provincial_map_bindings_are_explicit():
    expected = {
        "ARG-CHACO": (
            "Chaco",
        ),
        "ARG-CORRIENTES": (
            "Corrientes",
        ),
        "ARG-MISIONES": (
            "Misiones",
        ),
    }

    for (
        region_id,
        expected_names,
    ) in expected.items():
        scope = (
            get_regional_map_scope(
                region_id
            )
        )

        assert scope is not None

        assert (
            scope.scope_kind
            == "province"
        )

        assert (
            scope.georef_names
            == expected_names
        )

        assert (
            scope.includes_all_provinces
            is False
        )


def test_nea_scope_includes_the_four_statistical_northeast_provinces():
    scope = get_regional_map_scope(
        "ARG-NEA"
    )

    assert scope is not None

    assert (
        scope.scope_kind
        == "aggregate"
    )

    assert (
        scope.georef_names
        == (
            "Chaco",
            "Corrientes",
            "Formosa",
            "Misiones",
        )
    )

    assert (
        scope.includes_all_provinces
        is False
    )


def test_argentina_scope_uses_all_provincial_features():
    scope = get_regional_map_scope(
        "ARG"
    )

    assert scope is not None

    assert (
        scope.scope_kind
        == "country"
    )

    assert (
        scope.georef_names
        == ()
    )

    assert (
        scope.includes_all_provinces
        is True
    )


def test_map_lookup_is_normalized():
    scope = get_regional_map_scope(
        "  arg-chaco  "
    )

    assert scope is not None

    assert (
        scope.region_id
        == "ARG-CHACO"
    )

    assert (
        get_regional_map_scope(
            "unknown"
        )
        is None
    )


def test_map_slug_lookup_resolves_through_canonical_profile():
    scope = (
        get_regional_map_scope_for_slug(
            "  ChAcO  "
        )
    )

    assert scope is not None

    assert (
        scope.region_id
        == "ARG-CHACO"
    )

    assert (
        scope.slug
        == "chaco"
    )

    assert (
        get_regional_map_scope_for_slug(
            "unknown"
        )
        is None
    )


@pytest.mark.parametrize(
    "instance",
    (
        REGIONAL_MAP_DATASET,
        REGIONAL_MAP_DEFINITIONS[0],
        REGIONAL_MAP_SCOPES[0],
    ),
)
def test_regional_map_models_are_immutable(
    instance,
):
    first_field = (
        fields(
            instance
        )[0].name
    )

    with pytest.raises(
        FrozenInstanceError
    ):
        setattr(
            instance,
            first_field,
            "modified",
        )


def test_map_presentation_contract_contains_no_risk_or_compliance_status():
    forbidden_fields = {
        "risk",
        "risk_score",
        "risk_status",
        "compliance",
        "compliance_status",
        "assessment",
        "assessment_status",
    }

    for model_type in (
        TerritorialDataset,
        RegionalMapDefinition,
        RegionalMapScope,
    ):
        model_fields = {
            field.name
            for field
            in fields(
                model_type
            )
        }

        assert (
            model_fields
            .isdisjoint(
                forbidden_fields
            )
        )


def test_georef_snapshot_is_a_complete_province_feature_collection():
    snapshot = (
        _load_georef_snapshot()
    )

    assert (
        snapshot.get(
            "type"
        )
        == "FeatureCollection"
    )

    features = snapshot.get(
        "features",
        []
    )

    assert (
        len(features)
        == REGIONAL_MAP_DATASET
        .expected_feature_count
    )

    assert (
        len(features)
        == 24
    )


def test_georef_snapshot_contains_all_nea_map_members():
    features = (
        _features_by_name()
    )

    expected = {
        "Chaco",
        "Corrientes",
        "Formosa",
        "Misiones",
    }

    assert expected.issubset(
        features
    )


def test_target_georef_features_have_polygonal_geometry():
    features = (
        _features_by_name()
    )

    for name in (
        "Chaco",
        "Corrientes",
        "Formosa",
        "Misiones",
    ):
        feature = features[
            name
        ]

        geometry = feature.get(
            "geometry",
            {}
        )

        assert (
            geometry.get(
                "type"
            )
            in {
                "Polygon",
                "MultiPolygon",
            }
        )

        assert (
            geometry.get(
                "coordinates"
            )
        )


def test_target_georef_features_expose_source_metadata():
    features = (
        _features_by_name()
    )

    for name in (
        "Chaco",
        "Corrientes",
        "Formosa",
        "Misiones",
    ):
        properties = (
            features[
                name
            ].get(
                "properties",
                {},
            )
        )

        assert (
            properties.get(
                "id"
            )
        )

        assert (
            properties.get(
                "nombre"
            )
            == name
        )

        assert (
            properties.get(
                "fuente"
            )
        )


def test_georef_dataset_metadata_points_to_local_versioned_snapshot():
    assert (
        REGIONAL_MAP_DATASET
        .source_name
        == "Argentina Georef"
    )

    assert (
        REGIONAL_MAP_DATASET
        .snapshot_date
        == "2026-08-16"
    )

    assert (
        REGIONAL_MAP_DATASET
        .static_path
        == (
            "/static/data/georef/"
            "provincias.geojson"
        )
    )

    assert (
        GEOREF_SNAPSHOT.exists()
    )

    assert (
        GEOREF_SNAPSHOT.stat()
        .st_size
        > 0
    )


def test_regional_index_exposes_interactive_map_contract():
    template = (
        REGIONAL_INDEX_TEMPLATE
        .read_text(
            encoding="utf-8"
        )
    )

    assert (
        "data-regional-map"
        in template
    )

    assert (
        "regional_map_dataset.static_path"
        in template
    )

    assert (
        "data-regional-map-control"
        in template
    )

    assert (
        "regional_map_scope_by_region_id"
        in template
    )

    assert (
        "Geography, not risk"
        in template
    )

    assert (
        "regional_intelligence_map.js"
        in template
    )


def test_regional_index_cards_expose_map_scope_without_hardcoded_region_branching():
    template = (
        REGIONAL_INDEX_TEMPLATE
        .read_text(
            encoding="utf-8"
        )
    )

    assert (
        'data-region-id="{{ profile.region_id }}"'
        in template
    )

    assert (
        'data-scope-kind="{{ map_scope.scope_kind }}"'
        in template
    )

    assert (
        "profile.slug == "
        not in template
    )

    assert (
        "profile.region_id == "
        not in template
    )


def test_regional_detail_exposes_contextual_map_contract():
    template = (
        REGIONAL_DETAIL_TEMPLATE
        .read_text(
            encoding="utf-8"
        )
    )

    assert (
        "data-regional-map"
        in template
    )

    assert (
        'data-initial-region-id="{{ regional_map_scope.region_id }}"'
        in template
    )

    assert (
        'data-region-id="{{ regional_map_scope.region_id }}"'
        in template
    )

    assert (
        'data-scope-kind="{{ regional_map_scope.scope_kind }}"'
        in template
    )

    assert (
        "regional_map_dataset.static_path"
        in template
    )

    assert (
        "regional_intelligence_map.js"
        in template
    )


def test_regional_detail_map_explicitly_separates_geography_from_risk():
    template = (
        REGIONAL_DETAIL_TEMPLATE
        .read_text(
            encoding="utf-8"
        )
    )

    assert (
        "Geography, not risk"
        in template
    )

    assert (
        "does not represent supplier"
        in template
    )

    assert (
        "DDS"
        in template
    )

    assert (
        "compliance risk"
        in template
    )


def test_router_passes_map_contract_to_index_and_detail():
    router = (
        WEB_ROUTER.read_text(
            encoding="utf-8"
        )
    )

    assert (
        "REGIONAL_MAP_DATASET"
        in router
    )

    assert (
        "list_regional_map_scopes"
        in router
    )

    assert (
        "get_regional_map_scope"
        in router
    )

    assert (
        '"regional_map_dataset"'
        in router
    )

    assert (
        '"regional_map_scope"'
        in router
    )

    assert (
        "profile.region_id"
        in router
    )


def test_map_javascript_supports_index_and_detail_initial_scope():
    javascript = (
        REGIONAL_MAP_JS.read_text(
            encoding="utf-8"
        )
    )

    assert (
        "data-regional-map"
        in javascript
    )

    assert (
        "initialRegionId"
        in javascript
    )

    assert (
        "requestedInitialRegionId"
        in javascript
    )

    assert (
        "container,"
        in javascript
    )

    assert (
        "scopes.size > 1"
        in javascript
    )


def test_map_javascript_uses_local_geojson_without_external_tile_runtime():
    javascript = (
        REGIONAL_MAP_JS.read_text(
            encoding="utf-8"
        ).lower()
    )

    assert (
        "window.fetch"
        in javascript
    )

    assert (
        "l.geojson"
        in javascript
    )

    assert (
        "tilelayer"
        not in javascript
    )

    assert (
        "openstreetmap"
        not in javascript
    )

    assert (
        "apis.datos.gob.ar"
        not in javascript
    )


def test_map_javascript_styles_are_geographic_not_risk_categories():
    javascript = (
        REGIONAL_MAP_JS.read_text(
            encoding="utf-8"
        ).lower()
    )

    forbidden = (
        "risk_score",
        "riskstatus",
        "risk_status",
        "low_risk",
        "medium_risk",
        "high_risk",
        "compliant",
        "non_compliant",
    )

    for value in forbidden:
        assert (
            value
            not in javascript
        )


def test_map_javascript_has_fail_safe_non_map_fallback():
    javascript = (
        REGIONAL_MAP_JS.read_text(
            encoding="utf-8"
        )
    )

    assert (
        "Regional profile content remains accessible."
        in javascript
    )

    assert (
        "map.remove()"
        in javascript
    )


def test_detail_template_keeps_canonical_risk_and_provenance_contract():
    template = (
        REGIONAL_DETAIL_TEMPLATE
        .read_text(
            encoding="utf-8"
        )
    )

    assert (
        "profile.risk_context.rationale"
        in template
    )

    assert (
        "profile.risk_context.status"
        in template
    )

    assert (
        "profile.provenance.status"
        in template
    )

    assert (
        "profile.provenance.source_scope"
        in template
    )