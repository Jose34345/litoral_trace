"""Catálogo público de contexto regional de origen.

Esta capa aporta contexto territorial reutilizable para Chaco, Corrientes,
Misiones, NEA y Argentina. No constituye una evaluación de riesgo de una
operación, proveedor, parcela, despacho ni una conclusión de debida diligencia.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EvidenceDomain:
    """Un dominio visible de evidencia y trazabilidad."""

    key: str
    title: str
    description: str
    icon: str


@dataclass(frozen=True, slots=True)
class RegionalRiskContext:
    """Límite de interpretación para un perfil regional."""

    status: str
    label: str
    rationale: str


@dataclass(frozen=True, slots=True)
class DataProvenance:
    """Procedencia y vigencia de la información regional."""

    status: str
    label: str
    freshness_label: str
    source_scope: str


@dataclass(frozen=True, slots=True)
class RegionalProfile:
    """Perfil regional inmutable para contexto de origen."""

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
    evidence_domains: tuple[EvidenceDomain, ...]
    risk_context: RegionalRiskContext
    provenance: DataProvenance


REGIONAL_EVIDENCE_DOMAINS: tuple[EvidenceDomain, ...] = (
    EvidenceDomain(
        key="origin",
        title="Origen declarado",
        description=(
            "Organiza la evidencia vinculada con el origen geográfico "
            "de la materia prima y sus parcelas o rodales."
        ),
        icon="fa-location-dot",
    ),
    EvidenceDomain(
        key="documentary",
        title="Evidencia documental",
        description=(
            "Relaciona documentos de respaldo con orígenes, actores, "
            "productos y operaciones trazables."
        ),
        icon="fa-file-shield",
    ),
    EvidenceDomain(
        key="geospatial",
        title="Contexto geoespacial",
        description=(
            "Conserva geometrías y observaciones geoespaciales junto con "
            "el origen declarado y su cadena de evidencia."
        ),
        icon="fa-satellite",
    ),
    EvidenceDomain(
        key="supply_chain",
        title="Cadena de custodia",
        description=(
            "Mantiene el contexto de origen mientras el material avanza "
            "por recepciones, transformaciones, lotes y despachos."
        ),
        icon="fa-route",
    ),
    EvidenceDomain(
        key="compliance",
        title="Debida diligencia",
        description=(
            "Ordena información relevante para procesos de revisión y "
            "debida diligencia sin emitir una certificación automática."
        ),
        icon="fa-scale-balanced",
    ),
    EvidenceDomain(
        key="auditability",
        title="Auditabilidad",
        description=(
            "Conserva evidencia estructurada y revisable para facilitar "
            "controles internos, auditorías y consultas de compradores."
        ),
        icon="fa-box-archive",
    ),
)


DEFAULT_REGIONAL_RISK_CONTEXT = RegionalRiskContext(
    status="not_assessed",
    label="Contexto territorial, no evaluación de riesgo",
    rationale=(
        "Esta sección describe el ámbito territorial y la arquitectura de "
        "evidencia disponible. No determina por sí sola el riesgo de una "
        "operación, proveedor, parcela, despacho ni una conclusión de "
        "debida diligencia."
    ),
)


DEFAULT_REGIONAL_PROVENANCE = DataProvenance(
    status="framework_only",
    label="Marco de evidencia disponible",
    freshness_label="Sin dataset externo de riesgo validado para este perfil",
    source_scope=(
        "El perfil muestra el marco territorial y de evidencia de Litoral "
        "Trace. Los datos externos específicos de cada jurisdicción se "
        "incorporan únicamente cuando su fuente, alcance y vigencia fueron "
        "validados."
    ),
)


REGIONAL_PROFILES: tuple[RegionalProfile, ...] = (
    RegionalProfile(
        region_id="ARG-CHACO",
        slug="chaco",
        country_code="AR",
        jurisdiction_code="ARG-CHACO",
        name="Chaco",
        headline="Bosque nativo y trazabilidad de origen",
        summary=(
            "Contexto territorial para cadenas forestales con origen en "
            "Chaco y el Gran Chaco argentino."
        ),
        icon="fa-tree",
        geographic_scope="Perfil provincial",
        focus_areas=(
            "Origen y parcelas",
            "Contexto geoespacial",
            "Cadena documental",
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
        headline="Forestal implantado y cadena industrial",
        summary=(
            "Contexto territorial para cadenas forestales basadas en "
            "plantaciones, aserrado y transformación industrial."
        ),
        icon="fa-seedling",
        geographic_scope="Perfil provincial",
        focus_areas=(
            "Origen de plantaciones",
            "Transformación industrial",
            "Cadena documental",
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
        headline="Origen forestal e industria",
        summary=(
            "Contexto territorial para cadenas forestales e industriales "
            "con origen en Misiones."
        ),
        icon="fa-industry",
        geographic_scope="Perfil provincial",
        focus_areas=(
            "Origen forestal",
            "Contexto industrial",
            "Cadena documental",
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
        headline="Cadena de suministro regional",
        summary=(
            "Contexto interprovincial para orígenes y flujos de suministro "
            "del Nordeste Argentino."
        ),
        icon="fa-route",
        geographic_scope="Perfil regional",
        focus_areas=(
            "Orígenes interprovinciales",
            "Cadena de suministro",
            "Continuidad de evidencia",
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
        headline="Marco nacional de origen y exportación",
        summary=(
            "Contexto nacional para cadenas argentinas que necesitan "
            "documentar origen, trazabilidad y evidencia de exportación."
        ),
        icon="fa-flag",
        geographic_scope="Perfil nacional",
        focus_areas=(
            "Contexto nacional de origen",
            "Debida diligencia",
            "Evidencia de exportación",
        ),
        evidence_domains=REGIONAL_EVIDENCE_DOMAINS,
        risk_context=DEFAULT_REGIONAL_RISK_CONTEXT,
        provenance=DEFAULT_REGIONAL_PROVENANCE,
    ),
)


_PROFILE_BY_SLUG = {profile.slug: profile for profile in REGIONAL_PROFILES}
_PROFILE_BY_REGION_ID = {profile.region_id: profile for profile in REGIONAL_PROFILES}


def list_regional_profiles() -> tuple[RegionalProfile, ...]:
    """Devuelve los perfiles regionales en orden de presentación."""

    return REGIONAL_PROFILES


def get_regional_profile(slug: str) -> RegionalProfile | None:
    """Resuelve un perfil regional público por su slug normalizado."""

    return _PROFILE_BY_SLUG.get(slug.strip().lower())


def get_regional_profile_by_id(region_id: str) -> RegionalProfile | None:
    """Resuelve un perfil por su identidad regional canónica."""

    return _PROFILE_BY_REGION_ID.get(region_id.strip().upper())
