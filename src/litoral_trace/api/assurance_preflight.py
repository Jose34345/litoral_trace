"""Tenant-scoped HTTP surface for Assurance Preflight 2.0."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.assurance.feature_flags import get_assurance_feature_flags
from litoral_trace.assurance.operational_exceptions import (
    AssuranceOperationalExceptionError,
    AssuranceOperationalExceptionService,
)
from litoral_trace.assurance.preflight import (
    PreflightDocument,
    PreflightInput,
    PreflightSignalState,
    reason_catalog_payload,
)
from litoral_trace.assurance.preflight_service import (
    AssurancePreflightError,
    AssurancePreflightService,
)
from litoral_trace.auth.rbac import Permission, require_permission


class AssurancePreflightDocumentRequest(BaseModel):
    document_type: str = Field(min_length=1, max_length=64)
    reference: str = Field(min_length=1, max_length=512)
    valid_until: date | None = None


class AssurancePreflightRequest(BaseModel):
    """Preflight input.

    Only ``operation_reference`` is transport-required.  The deterministic
    engine remains fail-closed when business inputs are absent, which lets the
    friction-zero workspace launch Preflight immediately after ingestion and
    ask the operator only for facts that were not available from LT-owned data.
    """

    operation_reference: str = Field(min_length=1, max_length=255)
    customer_reference: str | None = Field(default=None, max_length=255)
    market: str | None = Field(default=None, max_length=8)
    product: str | None = Field(default=None, max_length=255)
    quantity: Decimal | None = None
    commitment_date: date | None = None
    stock_available: Decimal | None = None
    documents: list[AssurancePreflightDocumentRequest] = Field(default_factory=list)
    required_document_types: list[str] = Field(default_factory=list)
    origin_state: PreflightSignalState = PreflightSignalState.UNASSESSED
    genealogy_state: PreflightSignalState = PreflightSignalState.UNASSESSED
    phytosanitary_state: PreflightSignalState = PreflightSignalState.UNASSESSED
    eudr_state: PreflightSignalState = PreflightSignalState.UNASSESSED


def _require_preflight_enabled() -> None:
    flags = get_assurance_feature_flags()
    if not flags.assurance_v1 or not flags.preflight_v2:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Assurance Preflight 2.0 no está habilitado para este entorno.",
        )


def build_preflight_input(payload: AssurancePreflightRequest) -> PreflightInput:
    """Map one validated API request into the deterministic domain contract."""
    return PreflightInput(
        customer_reference=payload.customer_reference,  # type: ignore[arg-type]
        market=payload.market,  # type: ignore[arg-type]
        product=payload.product,  # type: ignore[arg-type]
        quantity=payload.quantity,  # type: ignore[arg-type]
        commitment_date=payload.commitment_date,  # type: ignore[arg-type]
        stock_available=payload.stock_available,  # type: ignore[arg-type]
        documents=tuple(
            PreflightDocument(
                document_type=document.document_type,
                reference=document.reference,
                valid_until=document.valid_until,
            )
            for document in payload.documents
        ),
        required_document_types=tuple(payload.required_document_types),
        origin_state=payload.origin_state,
        genealogy_state=payload.genealogy_state,
        phytosanitary_state=payload.phytosanitary_state,
        eudr_state=payload.eudr_state,
    )


def _serialize_result(*, organization_id: int, view) -> dict[str, object]:
    result = view.result
    return {
        "organization_id": organization_id,
        "operation_reference": view.operation_reference,
        "status": result.status.value,
        "reason_codes": list(result.reason_codes),
        "requires_human_action": result.requires_human_action,
        "open_reconciliation_issue_count": view.open_reconciliation_issue_count,
        "reasons": [
            {
                "code": reason.code,
                "category": reason.category,
                "status": reason.status.value,
                "explanation": reason.explanation,
                "action": reason.action,
                "source": reason.source,
            }
            for reason in result.reasons
        ],
        "legal_disclaimer": (
            "Resultado operativo determinístico de preparación; no constituye "
            "una decisión legal ni una presentación regulatoria."
        ),
    }


async def assurance_preflight_reason_catalog(
    user: UserTenantContext = Depends(require_permission(Permission.VAULT_READ)),
) -> JSONResponse:
    _require_preflight_enabled()
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "organization_id": user.organization_id,
            "reasons": list(reason_catalog_payload()),
        },
    )


async def assurance_preflight(
    payload: AssurancePreflightRequest,
    user: UserTenantContext = Depends(require_permission(Permission.VAULT_READ)),
) -> JSONResponse:
    _require_preflight_enabled()
    domain_payload = build_preflight_input(payload)
    try:
        view = AssurancePreflightService().evaluate(
            organization_id=user.organization_id,
            operation_reference=payload.operation_reference,
            payload=domain_payload,
        )
        flags = get_assurance_feature_flags()
        if flags.assurance_v1 and flags.operational_exceptions:
            AssuranceOperationalExceptionService().sync_preflight(
                organization_id=user.organization_id,
                view=view,
            )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"code": "ASSURANCE_PREFLIGHT_INVALID", "message": str(exc)},
        ) from None
    except AssurancePreflightError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "ASSURANCE_PREFLIGHT_UNAVAILABLE", "message": str(exc)},
        ) from None
    except AssuranceOperationalExceptionError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "ASSURANCE_EXCEPTION_SYNC_UNAVAILABLE", "message": str(exc)},
        ) from None
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=_serialize_result(organization_id=user.organization_id, view=view),
    )


def build_assurance_preflight_router() -> APIRouter:
    """Build an isolated Preflight router for each parent application."""
    api_router = APIRouter(prefix="/preflight", tags=["Assurance Preflight 2.0"])
    api_router.add_api_route(
        "/reasons",
        assurance_preflight_reason_catalog,
        methods=["GET"],
        status_code=status.HTTP_200_OK,
    )
    api_router.add_api_route(
        "",
        assurance_preflight,
        methods=["POST"],
        status_code=status.HTTP_200_OK,
    )
    return api_router


# Compatibility export only. Parent routers should build a fresh instance.
router = build_assurance_preflight_router()
