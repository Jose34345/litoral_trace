"""Exportación unificada de servicios de Litoral Trace."""
from litoral_trace.services.mass_balance import evaluar_balance_masas, MassBalanceResult
from litoral_trace.services.ndvi import calcular_ndvi_simulado, evaluar_indicador_variacion_biomasa, DISCLAIMER_EUDR
from litoral_trace.services.compliance import evaluar_compliance_lote, generar_dds_json_traces_nt
from litoral_trace.services.reports import generar_pdf_reporte_bytes

# Alias para compatibilidad histórica
evaluar_deforestacion_eudr = evaluar_indicador_variacion_biomasa

__all__ = [
    "evaluar_balance_masas",
    "MassBalanceResult",
    "calcular_ndvi_simulado",
    "evaluar_indicador_variacion_biomasa",
    "evaluar_deforestacion_eudr",
    "DISCLAIMER_EUDR",
    "evaluar_compliance_lote",
    "generar_dds_json_traces_nt",
    "generar_pdf_reporte_bytes",
]
