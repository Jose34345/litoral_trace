"""Legacy non-regulatory lot indicators.

This module predates the shipment-level EUDR conformance workflow. It may be
used only for operational previews such as mass-balance and vegetation-change
indicators. It MUST NOT assert legal harvest, deforestation-free status or EUDR
compliance, and it MUST NOT produce a legal/submit-ready Due Diligence Statement.
"""
from __future__ import annotations

import json
from typing import Any

from litoral_trace.services.mass_balance import evaluar_balance_masas


DISCLAIMER_EUDR = (
    "Las métricas de NDVI representan índices de vegetación y detección "
    "de cambios de biomasa. No constituyen por sí mismas prueba jurídica "
    "de deforestación bajo el Reglamento (UE) 2023/1115."
)


def _extract_ndvi_val(obs: dict[str, Any]) -> float:
    val = obs.get("ndvi_mean")
    if val is None:
        val = obs.get("ndvi", 0.0)
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def evaluar_indicador_variacion_biomasa(
    observaciones_ndvi: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compatibility-only vegetation-change indicator for legacy previews."""
    if not observaciones_ndvi:
        return {
            "status": "INSUFFICIENT_DATA",
            "vegetation_change_percentage": 0.0,
            "vegetation_change_indicator": "SIN_DATOS_SATELITALES",
            "eudr_vegetation_risk_indicator": "INDETERMINADO",
            "ndvi_base_2020": 0.0,
            "ndvi_actual_12m": 0.0,
            "disclaimer": DISCLAIMER_EUDR,
        }

    obs_2020 = [
        _extract_ndvi_val(o)
        for o in observaciones_ndvi
        if str(o.get("observation_date", o.get("fecha", ""))).startswith("2020")
    ]
    obs_post_2020 = [
        _extract_ndvi_val(o)
        for o in observaciones_ndvi
        if not str(o.get("observation_date", o.get("fecha", ""))).startswith("2020")
    ]
    if obs_post_2020:
        obs_recientes = obs_post_2020[-6:]
    elif len(observaciones_ndvi) >= 6:
        obs_recientes = [_extract_ndvi_val(o) for o in observaciones_ndvi[-6:]]
    else:
        obs_recientes = [_extract_ndvi_val(o) for o in observaciones_ndvi]
    if not obs_2020:
        obs_2020 = [_extract_ndvi_val(o) for o in observaciones_ndvi[:6]]

    base_2020 = sum(obs_2020) / len(obs_2020) if obs_2020 else 0.0
    actual_12m = sum(obs_recientes) / len(obs_recientes) if obs_recientes else 0.0
    if base_2020 == 0.0:
        return {
            "status": "ZERO_BASELINE",
            "vegetation_change_percentage": 0.0,
            "vegetation_change_indicator": "LINEA_BASE_NULA",
            "eudr_vegetation_risk_indicator": "INDETERMINADO",
            "ndvi_base_2020": 0.0,
            "ndvi_actual_12m": round(actual_12m, 4),
            "disclaimer": DISCLAIMER_EUDR,
        }

    var_pct = ((actual_12m - base_2020) / base_2020) * 100.0
    if var_pct >= -5.0:
        change_ind = "ESTABLE_SIN_CAMBIOS_SIGNIFICATIVOS"
        risk_ind = "BAJO_RIESGO_VEGETACIONAL"
    elif var_pct >= -15.0:
        change_ind = "VARIACION_MODERADA_ESTACIONAL_O_PRACTICA_SILVICOLA"
        risk_ind = "RIESGO_MODERADO_REVISAR_PRACTICA"
    else:
        change_ind = "CAIDA_SIGNIFICATIVA_DE_BIOMASA"
        risk_ind = "ALTA_VARIACION_REQUIERE_EVALUAR_COBERTURA"

    return {
        "status": "SUCCESS",
        "vegetation_change_percentage": round(var_pct, 2),
        "vegetation_change_indicator": change_ind,
        "eudr_vegetation_risk_indicator": risk_ind,
        "ndvi_base_2020": round(base_2020, 4),
        "ndvi_actual_12m": round(actual_12m, 4),
        "disclaimer": DISCLAIMER_EUDR,
    }


def calcular_ndvi_simulado(
    lat: float,
    lon: float,
    num_puntos: int = 24,
) -> list[dict[str, str | float]]:
    """Legacy deterministic simulation retained only for compatibility."""
    import math

    del lat, lon
    puntos: list[dict[str, str | float]] = []
    for i in range(num_puntos):
        year = 2020 + (i // 12)
        month = (i % 12) + 1
        val = round(0.60 + 0.15 * math.sin(i * 0.5), 4)
        puntos.append(
            {
                "fecha": f"{year}-{month:02d}-15",
                "ndvi": val,
                "ndvi_mean": val,
            }
        )
    return puntos


LEGACY_NON_REGULATORY_PROFILE = "LEGACY_NON_REGULATORY_PREVIEW"


class LegacyComplianceDisabledError(RuntimeError):
    """Compatibility marker for retired legacy regulatory behavior."""


def evaluar_compliance_lote(
    lote_data: dict[str, Any],
    volumen_ingresado_ton: float,
    volumen_exportar_ton: float,
) -> dict[str, Any]:
    """Return legacy operational indicators without a regulatory conclusion.

    The vegetation series is simulated historical/demo logic. A green result
    means only that this legacy preview found no operational alert under local
    thresholds; it is not evidence of EUDR compliance and must never be
    consumed by the P1-D DDS candidate workflow.
    """
    tipo_cultivo = lote_data.get(
        "producto_forestal",
        "Madera Aserrada (Pino)",
    )
    lat = float(lote_data.get("latitud", -27.45))
    lon = float(lote_data.get("longitud", -59.05))

    mb_result = evaluar_balance_masas(
        volumen_ingresado_ton,
        volumen_exportar_ton,
        tipo_cultivo,
    )

    puntos_ndvi = calcular_ndvi_simulado(lat, lon)
    puntos_format = [
        {
            "ndvi_mean": point["ndvi"],
            "observation_date": point["fecha"],
        }
        for point in puntos_ndvi
    ]
    sat_eval = evaluar_indicador_variacion_biomasa(puntos_format)

    sat_dictamen = (
        "Rojo"
        if sat_eval["eudr_vegetation_risk_indicator"]
        == "ALTA_VARIACION_REQUIERE_EVALUAR_COBERTURA"
        else "Verde"
    )
    sat_obs = sat_eval["vegetation_change_indicator"]

    if not mb_result.es_valido:
        dictamen_final = "Rojo"
        observacion_final = (
            "PREVIEW NO REGULATORIO — alerta de balance: "
            f"{mb_result.mensaje_observacion}"
        )
    elif sat_dictamen == "Rojo":
        dictamen_final = "Rojo"
        observacion_final = (
            "PREVIEW NO REGULATORIO — indicador de vegetación requiere "
            f"evaluación adicional: {sat_obs}"
        )
    else:
        dictamen_final = "Verde"
        observacion_final = (
            "PREVIEW NO REGULATORIO — sin alertas en los indicadores legacy. "
            "No constituye una conclusión EUDR ni una verificación de "
            f"deforestación/legalidad. {sat_obs} | {mb_result.mensaje_observacion}"
        )

    return {
        "profile": LEGACY_NON_REGULATORY_PROFILE,
        "regulatory_conclusion": None,
        "dictamen": dictamen_final,
        "observacion": observacion_final,
        "balance_masas": mb_result,
        "satelital": {
            "source": "SIMULATED_LEGACY_SERIES",
            "dictamen": sat_dictamen,
            "base_2020": sat_eval["ndvi_base_2020"],
            "actual": sat_eval["ndvi_actual_12m"],
            "puntos_ndvi": puntos_ndvi,
        },
    }


def generar_dds_json_traces_nt(
    lote_data: dict[str, Any],
    volumen_exportar_ton: float,
    operador_username: str = "comercial@litoraltrace.com",
) -> str:
    """Return only a retired compatibility preview for old callers.

    The function name is retained so disabled Streamlit/legacy batch and API
    callers do not crash during P1-D migration. The returned JSON is explicitly
    *not* a DDS, contains no legal conclusion, no compliance status and no
    automatic deforestation/legal-harvest assertions. P1-D is the sole path for
    a shipment-level API V3 DDS candidate.
    """
    polygon_wkt = lote_data.get("polygon_wkt")
    lat = float(lote_data.get("latitud", 0.0))
    lon = float(lote_data.get("longitud", 0.0))

    preview = {
        "profile": LEGACY_NON_REGULATORY_PROFILE,
        "retired_generator": True,
        "not_a_due_diligence_statement": True,
        "submit_ready": False,
        "regulatory_conclusion": None,
        "warning": (
            "Artefacto legacy no regulatorio. No presentar en EUDR/TRACES. "
            "Use el flujo EUDR API V3 de candidato DDS."
        ),
        "legacy_input_snapshot": {
            "operator_hint": operador_username,
            "commodity": lote_data.get("producto_forestal"),
            "volume_tons": round(max(float(volumen_exportar_ton or 0.0), 0.0), 2),
            "producer_reference": lote_data.get("productor_id"),
            "parcel_identifier": lote_data.get("identificador"),
            "geolocation": {
                "centroid": {
                    "latitude": round(lat, 6),
                    "longitude": round(lon, 6),
                },
                "polygon_wkt": polygon_wkt,
            },
        },
    }
    return json.dumps(preview, indent=2, ensure_ascii=False, sort_keys=True)
