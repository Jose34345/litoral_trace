"""Read-only industrial traceability API for P1C reverse genealogy."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.db.tenant import get_tenant_scoped_db_session
from litoral_trace.services.traceability_lineage import (
    TraceabilityLineageNotFoundError,
    TraceabilityLineageService,
    TraceabilityLineageValidationError,
)


router = APIRouter(
    prefix="/api/v1/traceability",
    tags=["Trazabilidad Industrial"],
)


def _detail(*, code: str, message: str) -> dict[str, str]:
    return {
        "code": code,
        "message": message,
    }


@router.get(
    "/shipments/{shipment_code}/origin",
    response_model=dict[str, Any],
)
async def obtener_origen_despacho_endpoint(
    shipment_code: str,
    user: UserTenantContext = Depends(
        require_permission(Permission.LOTE_READ)
    ),
) -> JSONResponse:
    """Reconstruct one tenant shipment back to source parcels and producers.

    Quantities attributed to source parcels use the explicit
    ``PROPORTIONAL_INPUT_ALLOCATION`` convention when industrial events mix
    multiple homogeneous inputs. ``complete=false`` means the chain contains
    a gap and must not be presented as closed provenance evidence.
    """
    session = get_tenant_scoped_db_session(user.organization_id)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_detail(
                code="TRACEABILITY_SERVICE_UNAVAILABLE",
                message="El servicio de trazabilidad no está disponible temporalmente.",
            ),
        )

    try:
        service = TraceabilityLineageService(
            session=session,
            organization_id=user.organization_id,
        )
        payload = service.trace_shipment(shipment_code)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=payload,
        )
    except TraceabilityLineageValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=_detail(code=exc.code, message=str(exc)),
        ) from None
    except TraceabilityLineageNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=_detail(code=exc.code, message=str(exc)),
        ) from None
    finally:
        session.close()
