"""Módulo de Análisis de Variación de Biomasa (NDVI) e Indicadores de Riesgo EUDR."""
from __future__ import annotations
from typing import Any

DISCLAIMER_EUDR = (
    "Las métricas de NDVI representan índices de vegetación satelital y detección de cambios de biomasa. "
    "No constituyen por sí mismas prueba jurídica de deforestación bajo el Reglamento (UE) 2023/1115, "
    "las cuales requieren análisis de cambio de cobertura de suelo y verificación de legalidad en origen."
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
    observaciones_ndvi: list[dict[str, Any]]
) -> dict[str, Any]:
    """Calcula la variación de biomasa entre la línea base 2020 y el período reciente, emitiendo un indicador de riesgo honesto."""
    if not observaciones_ndvi:
        return {
            "status": "INSUFFICIENT_DATA",
            "vegetation_change_percentage": 0.0,
            "vegetation_change_indicator": "SIN_DATOS_SATELITALES",
            "eudr_vegetation_risk_indicator": "INDETERMINADO",
            "ndvi_base_2020": 0.0,
            "ndvi_actual_12m": 0.0,
            "disclaimer": DISCLAIMER_EUDR
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
        obs_recientes = [
            _extract_ndvi_val(o)
            for o in observaciones_ndvi[-6:]
        ]
    else:
        obs_recientes = [
            _extract_ndvi_val(o)
            for o in observaciones_ndvi
        ]

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
            "disclaimer": DISCLAIMER_EUDR
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
        "disclaimer": DISCLAIMER_EUDR
    }

evaluar_deforestacion_eudr = evaluar_indicador_variacion_biomasa

def calcular_ndvi_simulado(lat: float, lon: float, num_puntos: int = 24) -> list[dict[str, str | float]]:
    """Función de simulación para pruebas unitarias exclusivamente."""
    import math
    puntos = []
    for i in range(num_puntos):
        year = 2020 + (i // 12)
        month = (i % 12) + 1
        val = round(0.60 + 0.15 * math.sin(i * 0.5), 4)
        puntos.append({"fecha": f"{year}-{month:02d}-15", "ndvi": val, "ndvi_mean": val})
    return puntos
