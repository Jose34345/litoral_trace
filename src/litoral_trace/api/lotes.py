"""Router REST de Lotes Geoespaciales, Compliance y Procesamiento Batch."""
from __future__ import annotations
import io
from typing import Any
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Response, status
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field

from litoral_trace.api.auth import get_current_tenant_user, UserTenantContext
from litoral_trace.services.compliance import evaluar_compliance_lote, generar_dds_json_traces_nt
from litoral_trace.services.reports import generar_pdf_reporte_bytes
from litoral_trace.services.batch import generar_plantilla_excel, procesar_lote_masivo
import pandas as pd

router = APIRouter(prefix="/api/v1", tags=["Lotes & Compliance EUDR"])

class LoteEvaluacionRequest(BaseModel):
    identificador: str = Field(..., example="Rodal Norte 01")
    productor_id: str = Field(..., example="30-12345678-9")
    producto_forestal: str = Field(default="Madera Aserrada (Pino)", example="Madera Aserrada (Pino)")
    hectareas: float = Field(default=100.0, ge=0.0)
    latitud: float = Field(default=-27.45, ge=-90.0, le=90.0)
    longitud: float = Field(default=-59.05, ge=-180.0, le=180.0)
    volumen_ingresado_ton: float = Field(..., ge=0.0)
    volumen_exportar_ton: float = Field(..., ge=0.0)

@router.get("/lotes", tags=["Lotes Geoespaciales"])
async def listar_lotes_tenant(
    user: UserTenantContext = Depends(get_current_tenant_user)
) -> JSONResponse:
    """Lista todos los lotes geoespaciales registrados bajo la organización del usuario."""
    lotes_demo = [
        {
            "id": 101,
            "organization_id": user.organization_id,
            "identificador": "Rodal Norte 01",
            "productor_id": "30-11111111-1",
            "producto_forestal": "Madera Aserrada (Pino)",
            "hectareas": 120.0,
            "latitud": -27.45,
            "longitud": -58.90,
            "estatus": "Verde"
        },
        {
            "id": 102,
            "organization_id": user.organization_id,
            "identificador": "Rodal Sur 02",
            "productor_id": "30-22222222-2",
            "producto_forestal": "Carbón Vegetal",
            "hectareas": 85.0,
            "latitud": -26.80,
            "longitud": -60.40,
            "estatus": "Verde"
        }
    ]
    return JSONResponse(status_code=status.HTTP_200_OK, content={"organization": user.organization_name, "total": len(lotes_demo), "lotes": lotes_demo})

@router.post("/compliance/evaluate", tags=["Compliance EUDR"])
async def evaluar_compliance_endpoint(
    payload: LoteEvaluacionRequest,
    user: UserTenantContext = Depends(get_current_tenant_user)
) -> JSONResponse:
    """Ejecuta la evaluación integral de biomasa (NDVI) y balance de masas para un lote."""
    lote_data = {
        "identificador": payload.identificador,
        "productor_id": payload.productor_id,
        "producto_forestal": payload.producto_forestal,
        "hectareas": payload.hectareas,
        "latitud": payload.latitud,
        "longitud": payload.longitud,
        "polygon_wkt": f"POLYGON(({payload.longitud-0.01} {payload.latitud-0.01}, {payload.longitud+0.01} {payload.latitud-0.01}, {payload.longitud+0.01} {payload.latitud+0.01}, {payload.longitud-0.01} {payload.latitud+0.01}, {payload.longitud-0.01} {payload.latitud-0.01}))"
    }

    comp_res = evaluar_compliance_lote(lote_data, payload.volumen_ingresado_ton, payload.volumen_exportar_ton)
    
    dds_json = None
    if comp_res["dictamen"] == "Verde":
        dds_json = generar_dds_json_traces_nt(lote_data, payload.volumen_exportar_ton, operador_username=user.email)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "identificador": payload.identificador,
            "productor_id": payload.productor_id,
            "dictamen": comp_res["dictamen"],
            "observacion": comp_res["observacion"],
            "balance_masas": {
                "coeficiente": comp_res["balance_masas"].coeficiente_rendimiento,
                "vol_max_permitido": comp_res["balance_masas"].volumen_maximo_permitido_ton,
                "es_valido": comp_res["balance_masas"].es_valido
            },
            "satelital": {
                "base_2020": comp_res["satelital"]["base_2020"],
                "actual": comp_res["satelital"]["actual"]
            },
            "dds_traces_nt_json": dds_json
        }
    )

@router.get("/batch/template", tags=["Procesamiento Batch"])
async def descargar_plantilla_excel_endpoint() -> StreamingResponse:
    """Descarga la plantilla Excel oficial para la importación masiva de remitos."""
    template_bytes = generar_plantilla_excel()
    return StreamingResponse(
        io.BytesIO(template_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=LitoralTrace_Plantilla_Ingreso.xlsx"}
    )

@router.post("/batch/upload", tags=["Procesamiento Batch"])
async def procesar_batch_excel_endpoint(
    file: UploadFile = File(...),
    user: UserTenantContext = Depends(get_current_tenant_user)
) -> StreamingResponse:
    """Procesa una matriz Excel subida por el usuario y genera el paquete de auditoría ZIP."""
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El archivo subido debe ser una planilla de Excel (.xlsx)")

    contents = await file.read()
    try:
        df_upload = pd.read_excel(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error al leer la planilla Excel: {e}")

    df_resumen, zip_bytes = procesar_lote_masivo(df_upload)

    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=LitoralTrace_Paquete_Auditoria_{user.username}.zip"}
    )
