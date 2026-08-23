"""Industrial traceability API and browser-domain composition."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.api.integrations import router as integrations_api_router
from litoral_trace.api.shipment_export_case import router as export_case_api_router
from litoral_trace.api.shipment_phytosanitary_case import router as phytosanitary_api_router
from litoral_trace.api.traceability_dossier import router as dossier_router
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.db.tenant import get_tenant_scoped_db_session
from litoral_trace.services.traceability_lineage import (
    TraceabilityLineageNotFoundError,
    TraceabilityLineageService,
    TraceabilityLineageValidationError,
)
from litoral_trace.web.integrations import router as integrations_web_router
from litoral_trace.web.shipment_export_case import router as export_case_web_router
from litoral_trace.web.shipment_phytosanitary_case import router as phytosanitary_web_router
from litoral_trace.web.traceability import router as traceability_web_router
from litoral_trace.web.traceability_release_control import (
    router as release_control_web_router,
)


api_router = APIRouter(
    prefix="/api/v1/traceability",
    tags=["Trazabilidad Industrial"],
)


def _detail(*, code: str, message: str) -> dict[str, str]:
    return {"code": code, "message": message}


@api_router.get(
    "/shipments/{shipment_code}/origin",
    response_model=dict[str, Any],
)
async def obtener_origen_despacho_endpoint(
    shipment_code: str,
    user: UserTenantContext = Depends(require_permission(Permission.LOTE_READ)),
) -> JSONResponse:
    """Reconstruct one tenant shipment back to source parcels and producers."""
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
            session=session, organization_id=user.organization_id
        )
        payload = service.trace_shipment(shipment_code)
        return JSONResponse(status_code=status.HTTP_200_OK, content=payload)
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


# main.py includes one traceability-domain router. Feature namespaces are
# composed here so production bootstrap remains stable across additive blocks.
router = APIRouter()
router.include_router(api_router)
router.include_router(dossier_router)
router.include_router(integrations_api_router)
router.include_router(export_case_api_router)
router.include_router(phytosanitary_api_router)
router.include_router(traceability_web_router)
router.include_router(release_control_web_router)
router.include_router(integrations_web_router)
router.include_router(export_case_web_router)
router.include_router(phytosanitary_web_router)
