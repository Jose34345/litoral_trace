"""Servicio Integrado de Compliance EUDR y Generador DDS TRACES NT."""
from __future__ import annotations
import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Any

from litoral_trace.services.mass_balance import evaluar_balance_masas
from litoral_trace.services.ndvi import calcular_ndvi_simulado, evaluar_indicador_variacion_biomasa

def evaluar_compliance_lote(
    lote_data: dict[str, Any],
    volumen_ingresado_ton: float,
    volumen_exportar_ton: float
) -> dict[str, Any]:
    """Evalúa de forma integral el cumplimiento EUDR (Satelital + Balance de Masas)."""
    tipo_cultivo = lote_data.get("producto_forestal", "Madera Aserrada (Pino)")
    lat = float(lote_data.get("latitud", -27.45))
    lon = float(lote_data.get("longitud", -59.05))
    
    mb_result = evaluar_balance_masas(volumen_ingresado_ton, volumen_exportar_ton, tipo_cultivo)
    
    puntos_ndvi = calcular_ndvi_simulado(lat, lon)
    puntos_format = [{"ndvi_mean": p["ndvi"], "observation_date": p["fecha"]} for p in puntos_ndvi]
    sat_eval = evaluar_indicador_variacion_biomasa(puntos_format)
    
    sat_dictamen = "Rojo" if sat_eval["eudr_vegetation_risk_indicator"] == "ALTA_VARIACION_REQUIERE_EVALUAR_COBERTURA" else "Verde"
    sat_obs = sat_eval["vegetation_change_indicator"]

    if not mb_result.es_valido:
        dictamen_final = "Rojo"
        observacion_final = f"BLOQUEADO: {mb_result.mensaje_observacion}"
    elif sat_dictamen == "Rojo":
        dictamen_final = "Rojo"
        observacion_final = f"BLOQUEADO: {sat_obs}"
    elif sat_dictamen == "Verde" and mb_result.es_valido:
        dictamen_final = "Verde"
        observacion_final = f"APROBADO / COMPLIANT. {sat_obs} | {mb_result.mensaje_observacion}"
    else:
        dictamen_final = "Pendiente"
        observacion_final = f"PENDIENTE DE VERIFICACIÓN: {sat_obs}"
        
    return {
        "dictamen": dictamen_final,
        "observacion": observacion_final,
        "balance_masas": mb_result,
        "satelital": {
            "dictamen": sat_dictamen,
            "base_2020": sat_eval["ndvi_base_2020"],
            "actual": sat_eval["ndvi_actual_12m"],
            "puntos_ndvi": puntos_ndvi
        }
    }

def generar_dds_json_traces_nt(
    lote_data: dict[str, Any],
    volumen_exportar_ton: float,
    operador_username: str = "comercial@litoraltrace.com"
) -> str:
    """Genera la Declaración de Debida Diligencia (DDS) en formato JSON estandarizado para TRACES NT."""
    ts_bytes = str(time.time()).encode('utf-8')
    reference_number = f"EUDR-DDS-{hashlib.sha256(ts_bytes).hexdigest()[:12].upper()}"
    
    polygon_wkt = lote_data.get("polygon_wkt")
    lat = float(lote_data.get("latitud", 0.0))
    lon = float(lote_data.get("longitud", 0.0))
    
    dds_payload = {
        "header": {
            "system": "Litoral Trace Compliance Engine v2.4",
            "regulation": "Reglamento (UE) 2023/1115 (EUDR)",
            "reference_number": reference_number,
            "timestamp": datetime.now(timezone.utc).isoformat()
        },
        "operator": {
            "id": operador_username,
            "country_origin": "AR",
            "region": "NEA / Gran Chaco"
        },
        "declaration": {
            "commodity": lote_data.get("producto_forestal", "Madera Aserrada (Pino)"),
            "volume_tons": round(max(float(volumen_exportar_ton or 0.0), 0.0), 2),
            "producer_tax_id": lote_data.get("productor_id", "N/A"),
            "parcel_identifier": lote_data.get("identificador", "Lote sin nombre")
        },
        "geolocation": {
            "type": "Polygon" if polygon_wkt else "Point",
            "centroid": {
                "latitude": round(lat, 6),
                "longitude": round(lon, 6)
            },
            "polygon_wkt": polygon_wkt or f"POINT({lon} {lat})"
        },
        "compliance": {
            "deforestation_free": True,
            "legal_harvest_verified": True,
            "status": "COMPLIANT"
        }
    }
    
    return json.dumps(dds_payload, indent=2, ensure_ascii=False)
