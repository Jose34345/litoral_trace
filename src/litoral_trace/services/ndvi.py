"""Telemetría Satelital de Biomasa (NDVI Copernicus Sentinel-2) para Detección de Deforestación."""
from __future__ import annotations
import math
from datetime import datetime, date

EUDR_CUTOFF_DATE = "2020-12-31"

def calcular_ndvi_simulado(lat: float, lon: float, num_puntos: int = 24) -> list[dict[str, str | float]]:
    """Genera serie de datos NDVI histórica (2020 a 2026) simulada para operar en modo offline/fallback."""
    puntos = []
    # Generar lecturas mensuales simuladas
    start_year = 2020
    for i in range(num_puntos):
        year = start_year + (i // 12)
        month = (i % 12) + 1
        fecha_str = f"{year}-{month:02d}-15"
        
        # Variación estacional suave alrededor de NDVI ~0.65
        base_val = 0.60 + 0.15 * math.sin(i * 0.5)
        puntos.append({
            "fecha": fecha_str,
            "ndvi": round(base_val, 4),
            "origen": "Satelital_Copernicus_Sentinel2_Simulado"
        })
    return puntos

def evaluar_deforestacion_eudr(puntos_ndvi: list[dict[str, str | float]], umbral_descarte_pct: float = -15.0) -> tuple[str, str, float, float]:
    """Evalúa la variación de biomasa (NDVI) desde la fecha límite EUDR (31 Diciembre 2020) hasta el presente.
    
    Returns:
        tuple[dictamen, observacion, ndvi_base_2020, ndvi_actual]
    """
    if not puntos_ndvi:
        return "Pendiente", "Insuficiencia de telemetría satelital", 0.0, 0.0
        
    puntos_2020 = [p["ndvi"] for p in puntos_ndvi if str(p["fecha"]).startswith("2020")]
    puntos_recientes = [p["ndvi"] for p in puntos_ndvi[-6:]] if len(puntos_ndvi) >= 6 else [p["ndvi"] for p in puntos_ndvi]
    
    if not puntos_2020 or not puntos_recientes:
        return "Pendiente", "Datos históricos insuficientes para calcular la línea base 2020", 0.0, 0.0
        
    base_2020 = sum(puntos_2020) / len(puntos_2020)
    actual = sum(puntos_recientes) / len(puntos_recientes)
    
    if base_2020 == 0:
        return "Pendiente", "Línea base NDVI nula", 0.0, 0.0
        
    variacion_pct = ((actual - base_2020) / base_2020) * 100.0
    
    if variacion_pct < umbral_descarte_pct:
        dictamen = "Rojo"
        obs = f"Alerta EUDR: Caída de biomasa del {variacion_pct:.1f}% respecto a la línea base 2020 (Umbral máximo: {umbral_descarte_pct}%)."
    else:
        dictamen = "Verde"
        obs = f"Cumplimiento EUDR Verificado: Variación de biomasa del {variacion_pct:+.1f}% respecto a la línea base de diciembre 2020."
        
    return dictamen, obs, round(base_2020, 3), round(actual, 3)
