"""Motor de Matemática de Balance de Masas (Input-Output) para Compliance EUDR."""
from __future__ import annotations
from dataclasses import dataclass

# Coeficientes de Rendimiento Industrial por Producto Forestal
RENDIMIENTO_INDUSTRIAL: dict[str, float] = {
    "Madera Aserrada (Pino)": 0.50,
    "Madera Aserrada (Eucalipto)": 0.45,
    "Extracto de Quebracho (Tanino)": 0.30,
    "Rollizo Triturable": 0.95,
    "Carbón Vegetal": 0.25,
}

DEFAULT_COEFICIENTE: float = 0.50

@dataclass(frozen=True)
class MassBalanceResult:
    tipo_cultivo: str
    coeficiente_rendimiento: float
    volumen_ingresado_ton: float
    volumen_exportar_ton: float
    volumen_maximo_permitido_ton: float
    es_valido: bool
    mensaje_observacion: str

def evaluar_balance_masas(
    volumen_ingresado: float,
    volumen_exportar: float,
    tipo_cultivo: str
) -> MassBalanceResult:
    """Valida si el volumen declarado a exportar respeta el límite físico-matemático según el rendimiento industrial.
    
    Args:
        volumen_ingresado: Toneladas de materia prima ingresadas al establecimiento.
        volumen_exportar: Toneladas del producto derivado que se pretenden exportar.
        tipo_cultivo: Especie / producto forestal.
        
    Returns:
        MassBalanceResult: Resultado con el veredicto y métricas asociadas.
    """
    vol_in = max(float(volumen_ingresado or 0.0), 0.0)
    vol_out = max(float(volumen_exportar or 0.0), 0.0)
    
    coeficiente = RENDIMIENTO_INDUSTRIAL.get(tipo_cultivo.strip(), DEFAULT_COEFICIENTE)
    vol_max = vol_in * coeficiente
    
    es_valido = vol_out <= vol_max
    
    if es_valido:
        obs = f"Balance de Masas Conforme: {vol_out:.2f} ton exportables dentro del límite legal ({vol_max:.2f} ton con rendimiento del {coeficiente*100:.0f}%)."
    else:
        exceso = vol_out - vol_max
        obs = f"Alerta de Sobredeclaración: El volumen a exportar ({vol_out:.2f} ton) supera en {exceso:.2f} ton el máximo físico permitido ({vol_max:.2f} ton)."
        
    return MassBalanceResult(
        tipo_cultivo=tipo_cultivo,
        coeficiente_rendimiento=coeficiente,
        volumen_ingresado_ton=vol_in,
        volumen_exportar_ton=vol_out,
        volumen_maximo_permitido_ton=vol_max,
        es_valido=es_valido,
        mensaje_observacion=obs
    )
