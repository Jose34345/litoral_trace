"""Authenticated P1E origin-dossier download endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.db.tenant import get_tenant_scoped_db_session
from litoral_trace.services.traceability_documentary_dossier import (
    build_documentary_dossier_bundle,
)
from litoral_trace.services.traceability_dossier import (
    OriginDossierGenerationError,
    OriginDossierValidationError,
    safe_artifact_stem,
)
from litoral_trace.services.traceability_evidence_dossier import (
    project_documentary_evidence,
)
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
    return {"code": code, "message": message}


def _load_bundle(*, shipment_code: str, user: UserTenantContext):
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
        payload = TraceabilityLineageService(
            session=session,
            organization_id=user.organization_id,
        ).trace_shipment(shipment_code)
        documentary_evidence = project_documentary_evidence(
            session=session,
            organization_id=user.organization_id,
            lineage_payload=payload,
        )
        return build_documentary_dossier_bundle(
            payload,
            documentary_evidence=documentary_evidence,
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
    except OriginDossierValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=_detail(code=exc.code, message=str(exc)),
        ) from None
    except OriginDossierGenerationError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_detail(
                code="ORIGIN_DOSSIER_GENERATION_FAILED",
                message="No fue posible generar el dossier de origen en este momento.",
            ),
        ) from None
    finally:
        session.close()


def _headers(*, bundle, filename: str) -> dict[str, str]:
    return {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Cache-Control": "private, no-store",
        "X-Litoral-Trace-Manifest-SHA256": bundle.manifest_sha256,
    }


ShipmentCodeQuery = Query(
    ...,
    min_length=1,
    max_length=120,
    description="Código comercial del despacho dentro de la organización autenticada.",
)


@router.get(
    "/shipments/dossier/manifest",
    response_class=Response,
)
async def descargar_manifest_dossier_endpoint(
    shipment_code: str = ShipmentCodeQuery,
    user: UserTenantContext = Depends(
        require_permission(Permission.LOTE_READ)
    ),
) -> Response:
    bundle = _load_bundle(shipment_code=shipment_code, user=user)
    stem = safe_artifact_stem(bundle.shipment_code)
    return Response(
        content=bundle.manifest_json_bytes,
        media_type="application/json",
        headers=_headers(
            bundle=bundle,
            filename=f"litoral-trace-{stem}-manifest.json",
        ),
    )


@router.get(
    "/shipments/dossier/geojson",
    response_class=Response,
)
async def descargar_geojson_dossier_endpoint(
    shipment_code: str = ShipmentCodeQuery,
    user: UserTenantContext = Depends(
        require_permission(Permission.LOTE_READ)
    ),
) -> Response:
    bundle = _load_bundle(shipment_code=shipment_code, user=user)
    stem = safe_artifact_stem(bundle.shipment_code)
    return Response(
        content=bundle.geojson_bytes,
        media_type="application/geo+json",
        headers=_headers(
            bundle=bundle,
            filename=f"litoral-trace-{stem}-origins.geojson",
        ),
    )


@router.get(
    "/shipments/dossier/pdf",
    response_class=Response,
)
async def descargar_pdf_dossier_endpoint(
    shipment_code: str = ShipmentCodeQuery,
    user: UserTenantContext = Depends(
        require_permission(Permission.LOTE_READ)
    ),
) -> Response:
    bundle = _load_bundle(shipment_code=shipment_code, user=user)
    stem = safe_artifact_stem(bundle.shipment_code)
    return Response(
        content=bundle.pdf_bytes,
        media_type="application/pdf",
        headers=_headers(
            bundle=bundle,
            filename=f"litoral-trace-{stem}-dossier.pdf",
        ),
    )


@router.get(
    "/shipments/dossier/bundle",
    response_class=Response,
)
async def descargar_bundle_dossier_endpoint(
    shipment_code: str = ShipmentCodeQuery,
    user: UserTenantContext = Depends(
        require_permission(Permission.LOTE_READ)
    ),
) -> Response:
    bundle = _load_bundle(shipment_code=shipment_code, user=user)
    stem = safe_artifact_stem(bundle.shipment_code)
    return Response(
        content=bundle.zip_bytes,
        media_type="application/zip",
        headers=_headers(
            bundle=bundle,
            filename=f"litoral-trace-{stem}-dossier.zip",
        ),
    )
