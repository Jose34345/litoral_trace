"""Módulo de Procesamiento Batch Masivo y Generación de Plantillas Excel."""
from __future__ import annotations
import io
import zipfile
import pandas as pd

from litoral_trace.services.compliance import evaluar_compliance_lote, generar_dds_json_traces_nt
from litoral_trace.services.reports import generar_pdf_reporte_bytes

BATCH_COLUMNAS = [
    "Identificador_Lote",
    "ID_Proveedor",
    "Producto_Forestal",
    "Hectareas",
    "Latitud",
    "Longitud",
    "Volumen_Ingresado_Ton",
    "Volumen_Exportar_Ton",
]

BATCH_FILA_EJEMPLO = [
    "Rodal_Norte_01",
    "CUIT-30123456789",
    "Madera Aserrada (Eucalipto)",
    120.0,
    -27.50,
    -58.90,
    500.0,
    200.0,
]

def generar_plantilla_excel() -> bytes:
    """Genera la plantilla Excel oficial para la importación masiva de lotes."""
    df_template = pd.DataFrame(columns=BATCH_COLUMNAS)
    df_template.loc[0] = BATCH_FILA_EJEMPLO
    
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df_template.to_excel(writer, index=False, sheet_name="Plantilla_LitoralTrace")
    return buffer.getvalue()

def procesar_lote_masivo(df_upload: pd.DataFrame) -> tuple[pd.DataFrame, bytes]:
    """Procesa una matriz de datos cargada desde Excel y genera paquete ZIP de auditoría.
    
    Returns:
        tuple[Resumen_DataFrame, ZIP_Bytes]
    """
    resumen_filas = []
    zip_buffer = io.BytesIO()
    
    if df_upload is None or df_upload.empty:
        return pd.DataFrame(resumen_filas), zip_buffer.getvalue()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for idx, row in df_upload.iterrows():
            nombre = str(row.get("Identificador_Lote") or f"Lote_{idx+1}").strip()
            proveedor = str(row.get("ID_Proveedor") or "N/A").strip()
            producto = str(row.get("Producto_Forestal") or "Madera Aserrada (Pino)").strip()
            
            try:
                hectareas = float(row.get("Hectareas") or 0.0)
                lat = float(row.get("Latitud") or -27.45)
                lon = float(row.get("Longitud") or -59.05)
                vol_in = float(row.get("Volumen_Ingresado_Ton") or 0.0)
                vol_out = float(row.get("Volumen_Exportar_Ton") or 0.0)
            except (ValueError, TypeError):
                hectareas, lat, lon, vol_in, vol_out = 0.0, -27.45, -58.90, 0.0, 0.0

            lote_data = {
                "identificador": nombre,
                "productor_id": proveedor,
                "producto_forestal": producto,
                "hectareas": hectareas,
                "latitud": lat,
                "longitud": lon,
                "polygon_wkt": f"POLYGON(({lon-0.01} {lat-0.01}, {lon+0.01} {lat-0.01}, {lon+0.01} {lat+0.01}, {lon-0.01} {lat+0.01}, {lon-0.01} {lat-0.01}))"
            }

            eval_res = evaluar_compliance_lote(lote_data, vol_in, vol_out)
            dictamen = eval_res["dictamen"]
            obs = eval_res["observacion"]
            mb_result = eval_res["balance_masas"]

            resumen_filas.append({
                "Lote": nombre,
                "Proveedor": proveedor,
                "Producto": producto,
                "Vol. Exportar (Ton)": vol_out,
                "Dictamen": dictamen,
                "Observación": obs
            })

            # Generar PDF de Auditoría
            pdf_bytes = generar_pdf_reporte_bytes(
                lote_data, dictamen, obs, vol_in, vol_out, mb_result.coeficiente_rendimiento
            )
            carpeta = f"{dictamen}_{proveedor}_{nombre}/"
            zip_file.writestr(f"{carpeta}AUDITORIA_{proveedor}.pdf", pdf_bytes)

            # Si es Apto (Verde), adjuntar también el JSON para TRACES NT
            if dictamen == "Verde":
                json_data = generar_dds_json_traces_nt(lote_data, vol_out)
                zip_file.writestr(f"{carpeta}DDS_TRACES_NT_{proveedor}.json", json_data.encode("utf-8"))

    return pd.DataFrame(resumen_filas), zip_buffer.getvalue()
