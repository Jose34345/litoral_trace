"""Router REST de Lotes Geoespaciales, Compliance y Procesamiento Batch."""

from __future__ import annotations

import io
from typing import Any

import pandas as pd
from fastapi import (
    APIRouter,
    Depends,
    File,
    HTTPException,
    UploadFile,
    status,
)
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from litoral_trace.api.auth import (
    UserTenantContext,
)
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import Lote
from litoral_trace.db.tenant import (
    get_tenant_scoped_db_session,
    set_tenant_db_context,
)
from litoral_trace.services.batch import (
    generar_plantilla_excel,
    procesar_lote_masivo,
)
from litoral_trace.services.compliance import (
    evaluar_compliance_lote,
    generar_dds_json_traces_nt,
)


router = APIRouter(
    prefix="/api/v1",
    tags=["Lotes & Compliance EUDR"],
)


# ============================================================================
# MODELOS PYDANTIC
# ============================================================================


class LoteCreateRequest(BaseModel):
    """Datos necesarios para crear un lote."""

    identificador: str = Field(
        ...,
        min_length=1,
        max_length=100,
        examples=["Rodal Norte 01"],
    )

    productor_id: str = Field(
        ...,
        min_length=1,
        max_length=100,
        examples=["30-12345678-9"],
    )

    producto_forestal: str = Field(
        default="Madera Aserrada (Pino)",
        min_length=1,
        max_length=100,
    )

    hectareas: float = Field(
        default=100.0,
        ge=0.0,
    )

    latitud: float = Field(
        default=-27.45,
        ge=-90.0,
        le=90.0,
    )

    longitud: float = Field(
        default=-59.05,
        ge=-180.0,
        le=180.0,
    )

    polygon_wkt: str | None = Field(
        default=None,
        description="Polígono WKT opcional. Si no se informa, se genera uno alrededor del centroide.",
    )

    volumen_ingresado_ton: float = Field(
        default=0.0,
        ge=0.0,
    )

    volumen_exportar_ton: float = Field(
        default=0.0,
        ge=0.0,
    )


class LoteUpdateRequest(BaseModel):
    """Campos modificables de un lote."""

    identificador: str | None = Field(
        default=None,
        min_length=1,
        max_length=100,
    )

    productor_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=100,
    )

    producto_forestal: str | None = Field(
        default=None,
        min_length=1,
        max_length=100,
    )

    hectareas: float | None = Field(
        default=None,
        ge=0.0,
    )

    latitud: float | None = Field(
        default=None,
        ge=-90.0,
        le=90.0,
    )

    longitud: float | None = Field(
        default=None,
        ge=-180.0,
        le=180.0,
    )

    polygon_wkt: str | None = None

    estatus: str | None = Field(
        default=None,
        max_length=50,
    )

    volumen_ingresado_ton: float | None = Field(
        default=None,
        ge=0.0,
    )

    volumen_exportar_ton: float | None = Field(
        default=None,
        ge=0.0,
    )


class LoteResponse(BaseModel):
    """Representación pública de un lote."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    organization_id: int
    identificador: str
    productor_id: str
    producto_forestal: str
    hectareas: float
    latitud: float
    longitud: float
    polygon_wkt: str | None
    estatus: str
    volumen_ingresado_ton: float | None
    volumen_exportar_ton: float | None


class LoteEvaluacionRequest(BaseModel):
    """Datos utilizados para evaluar compliance EUDR."""

    identificador: str = Field(
        ...,
        min_length=1,
        max_length=100,
        examples=["Rodal Norte 01"],
    )

    productor_id: str = Field(
        ...,
        min_length=1,
        max_length=100,
        examples=["30-12345678-9"],
    )

    producto_forestal: str = Field(
        default="Madera Aserrada (Pino)",
        min_length=1,
        max_length=100,
    )

    hectareas: float = Field(
        default=100.0,
        ge=0.0,
    )

    latitud: float = Field(
        default=-27.45,
        ge=-90.0,
        le=90.0,
    )

    longitud: float = Field(
        default=-59.05,
        ge=-180.0,
        le=180.0,
    )

    volumen_ingresado_ton: float = Field(
        ...,
        ge=0.0,
    )

    volumen_exportar_ton: float = Field(
        ...,
        ge=0.0,
    )


# ============================================================================
# HELPERS
# ============================================================================


def _generate_default_polygon(
    latitud: float,
    longitud: float,
) -> str:
    """Genera un pequeño polígono WKT alrededor del centroide."""

    delta = 0.01

    return (
        f"POLYGON(("
        f"{longitud - delta} {latitud - delta}, "
        f"{longitud + delta} {latitud - delta}, "
        f"{longitud + delta} {latitud + delta}, "
        f"{longitud - delta} {latitud + delta}, "
        f"{longitud - delta} {latitud - delta}"
        f"))"
    )


def _lote_to_dict(lote: Lote) -> dict[str, Any]:
    """Convierte un modelo ORM Lote en un diccionario JSON."""

    return {
        "id": lote.id,
        "organization_id": lote.organization_id,
        "identificador": lote.identificador,
        "productor_id": lote.productor_id,
        "producto_forestal": lote.producto_forestal,
        "hectareas": lote.hectareas,
        "latitud": lote.latitud,
        "longitud": lote.longitud,
        "polygon_wkt": lote.polygon_wkt,
        "estatus": lote.estatus,
        "volumen_ingresado_ton": lote.volumen_ingresado_ton,
        "volumen_exportar_ton": lote.volumen_exportar_ton,
    }


def _get_tenant_lote(
    session: Any,
    lote_id: int,
    organization_id: int,
) -> Lote:
    """
    Obtiene un lote perteneciente exclusivamente al tenant autenticado.

    Es importante que organization_id forme parte de la consulta.
    De esta manera un usuario no puede acceder a un lote de otra empresa
    simplemente modificando el ID de la URL.
    """

    lote = session.execute(
        select(Lote).where(
            Lote.id == lote_id,
            Lote.organization_id == organization_id,
        )
    ).scalar_one_or_none()

    if lote is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Lote no encontrado.",
        )

    return lote


# ============================================================================
# GET /LOTES
# ============================================================================


@router.get(
    "/lotes",
    response_model=dict[str, Any],
    tags=["Lotes Geoespaciales"],
)
async def listar_lotes_tenant(
    user: UserTenantContext = Depends(require_permission(Permission.LOTE_READ)),
) -> JSONResponse:
    """
    Lista únicamente los lotes pertenecientes a la organización autenticada.
    """

    session = get_tenant_scoped_db_session(user.organization_id)

    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    try:
        result = session.execute(
            select(Lote)
            .where(Lote.organization_id == user.organization_id)
            .order_by(Lote.id.desc())
        )

        lotes = result.scalars().all()

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "organization_id": user.organization_id,
                "organization": user.organization_name,
                "total": len(lotes),
                "lotes": [_lote_to_dict(lote) for lote in lotes],
            },
        )

    finally:
        session.close()


# ============================================================================
# GET /LOTES/{ID}
# ============================================================================


@router.get(
    "/lotes/{lote_id}",
    response_model=LoteResponse,
    tags=["Lotes Geoespaciales"],
)
async def obtener_lote(
    lote_id: int,
    user: UserTenantContext = Depends(require_permission(Permission.LOTE_READ)),
) -> LoteResponse:
    """Obtiene un lote perteneciente al tenant autenticado."""

    session = get_tenant_scoped_db_session(user.organization_id)

    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    try:
        lote = _get_tenant_lote(
            session=session,
            lote_id=lote_id,
            organization_id=user.organization_id,
        )

        return LoteResponse.model_validate(lote)

    finally:
        session.close()


# ============================================================================
# POST /LOTES
# ============================================================================


@router.post(
    "/lotes",
    response_model=LoteResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Lotes Geoespaciales"],
)
async def crear_lote(
    payload: LoteCreateRequest,
    user: UserTenantContext = Depends(require_permission(Permission.LOTE_CREATE)),
) -> LoteResponse:
    """
    Crea un lote real en PostgreSQL asociado al tenant autenticado.
    """

    session = get_tenant_scoped_db_session(user.organization_id)

    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    try:
        # Evita duplicar el mismo identificador dentro de una organización.
        existing = session.execute(
            select(Lote).where(
                Lote.organization_id == user.organization_id,
                Lote.identificador == payload.identificador.strip(),
            )
        ).scalar_one_or_none()

        if existing is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Ya existe un lote con el identificador "
                    f"'{payload.identificador}' en esta organización."
                ),
            )

        polygon_wkt = payload.polygon_wkt

        if not polygon_wkt:
            polygon_wkt = _generate_default_polygon(
                latitud=payload.latitud,
                longitud=payload.longitud,
            )

        lote = Lote(
            organization_id=user.organization_id,
            identificador=payload.identificador.strip(),
            productor_id=payload.productor_id.strip(),
            producto_forestal=payload.producto_forestal.strip(),
            hectareas=payload.hectareas,
            latitud=payload.latitud,
            longitud=payload.longitud,
            polygon_wkt=polygon_wkt,
            estatus="Pendiente",
            volumen_ingresado_ton=payload.volumen_ingresado_ton,
            volumen_exportar_ton=payload.volumen_exportar_ton,
        )

        session.add(lote)
        session.commit()
        set_tenant_db_context(session, user.organization_id)
        session.refresh(lote)

        return LoteResponse.model_validate(lote)

    except HTTPException:
        session.rollback()
        raise

    except Exception:
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No fue posible crear el lote.",
        )

    finally:
        session.close()


# ============================================================================
# PUT /LOTES/{ID}
# ============================================================================


@router.put(
    "/lotes/{lote_id}",
    response_model=LoteResponse,
    tags=["Lotes Geoespaciales"],
)
async def actualizar_lote(
    lote_id: int,
    payload: LoteUpdateRequest,
    user: UserTenantContext = Depends(require_permission(Permission.LOTE_UPDATE)),
) -> LoteResponse:
    """Actualiza un lote perteneciente exclusivamente al tenant autenticado."""

    session = get_tenant_scoped_db_session(user.organization_id)

    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    try:
        lote = _get_tenant_lote(
            session=session,
            lote_id=lote_id,
            organization_id=user.organization_id,
        )

        changes = payload.model_dump(exclude_unset=True)

        # Si cambia el identificador, comprobamos duplicados dentro del tenant.
        if "identificador" in changes:
            nuevo_identificador = changes["identificador"].strip()

            duplicate = session.execute(
                select(Lote).where(
                    Lote.organization_id == user.organization_id,
                    Lote.identificador == nuevo_identificador,
                    Lote.id != lote.id,
                )
            ).scalar_one_or_none()

            if duplicate is not None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"Ya existe otro lote con el identificador "
                        f"'{nuevo_identificador}' en esta organización."
                    ),
                )

            changes["identificador"] = nuevo_identificador

        if "productor_id" in changes and changes["productor_id"] is not None:
            changes["productor_id"] = changes["productor_id"].strip()

        if (
            "producto_forestal" in changes
            and changes["producto_forestal"] is not None
        ):
            changes["producto_forestal"] = changes["producto_forestal"].strip()

        for field_name, value in changes.items():
            setattr(lote, field_name, value)

        session.commit()
        set_tenant_db_context(session, user.organization_id)
        session.refresh(lote)

        return LoteResponse.model_validate(lote)

    except HTTPException:
        session.rollback()
        raise

    except Exception:
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No fue posible actualizar el lote.",
        )

    finally:
        session.close()


# ============================================================================
# DELETE /LOTES/{ID}
# ============================================================================


@router.delete(
    "/lotes/{lote_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["Lotes Geoespaciales"],
)
async def eliminar_lote(
    lote_id: int,
    user: UserTenantContext = Depends(require_permission(Permission.LOTE_DELETE)),
) -> None:
    """
    Elimina un lote únicamente si pertenece al tenant autenticado.

    """

    session = get_tenant_scoped_db_session(user.organization_id)

    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    try:
        lote = _get_tenant_lote(
            session=session,
            lote_id=lote_id,
            organization_id=user.organization_id,
        )

        session.delete(lote)
        session.commit()

    except HTTPException:
        session.rollback()
        raise

    except Exception:
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No fue posible eliminar el lote.",
        )

    finally:
        session.close()


# ============================================================================
# COMPLIANCE EUDR
# ============================================================================


@router.post(
    "/compliance/evaluate",
    tags=["Compliance EUDR"],
)
async def evaluar_compliance_endpoint(
    payload: LoteEvaluacionRequest,
    user: UserTenantContext = Depends(require_permission(Permission.LOTE_READ)),
) -> JSONResponse:
    """
    Ejecuta la evaluación integral de biomasa (NDVI)
    y balance de masas para un lote.
    """

    lote_data = {
        "identificador": payload.identificador,
        "productor_id": payload.productor_id,
        "producto_forestal": payload.producto_forestal,
        "hectareas": payload.hectareas,
        "latitud": payload.latitud,
        "longitud": payload.longitud,
        "polygon_wkt": _generate_default_polygon(
            latitud=payload.latitud,
            longitud=payload.longitud,
        ),
    }

    comp_res = evaluar_compliance_lote(
        lote_data,
        payload.volumen_ingresado_ton,
        payload.volumen_exportar_ton,
    )

    dds_json = None

    if comp_res["dictamen"] == "Verde":
        dds_json = generar_dds_json_traces_nt(
            lote_data,
            payload.volumen_exportar_ton,
            operador_username=user.email,
        )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "organization_id": user.organization_id,
            "identificador": payload.identificador,
            "productor_id": payload.productor_id,
            "dictamen": comp_res["dictamen"],
            "observacion": comp_res["observacion"],
            "balance_masas": {
                "coeficiente": comp_res["balance_masas"].coeficiente_rendimiento,
                "vol_max_permitido": (
                    comp_res["balance_masas"].volumen_maximo_permitido_ton
                ),
                "es_valido": comp_res["balance_masas"].es_valido,
            },
            "satelital": {
                "base_2020": comp_res["satelital"]["base_2020"],
                "actual": comp_res["satelital"]["actual"],
            },
            "dds_traces_nt_json": dds_json,
        },
    )


# ============================================================================
# BATCH
# ============================================================================


@router.get(
    "/batch/template",
    tags=["Procesamiento Batch"],
)
async def descargar_plantilla_excel_endpoint() -> StreamingResponse:
    """Descarga la plantilla Excel oficial para importación masiva."""

    template_bytes = generar_plantilla_excel()

    return StreamingResponse(
        io.BytesIO(template_bytes),
        media_type=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
        headers={
            "Content-Disposition": (
                "attachment; "
                "filename=LitoralTrace_Plantilla_Ingreso.xlsx"
            )
        },
    )


@router.post(
    "/batch/upload",
    tags=["Procesamiento Batch"],
)
async def procesar_batch_excel_endpoint(
    file: UploadFile = File(...),
    user: UserTenantContext = Depends(require_permission(Permission.LOTE_CREATE)),
) -> StreamingResponse:
    """Procesa una matriz Excel y genera el paquete de auditoría ZIP."""

    filename = file.filename or ""

    if not filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "El archivo subido debe ser una planilla "
                "de Excel (.xlsx o .xls)."
            ),
        )

    contents = await file.read()

    if not contents:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El archivo Excel está vacío.",
        )

    try:
        df_upload = pd.read_excel(io.BytesIO(contents))

    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error al leer la planilla Excel: {exc}",
        ) from exc

    try:
        _, zip_bytes = procesar_lote_masivo(df_upload)

    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Error al procesar la planilla: {exc}",
        ) from exc

    return StreamingResponse(
        io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={
            "Content-Disposition": (
                "attachment; "
                f"filename=LitoralTrace_Paquete_Auditoria_{user.username}.zip"
            )
        },
    )
