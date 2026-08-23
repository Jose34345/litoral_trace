"""Tenant-safe EUDR API V3 ACCEPTANCE transport API.

Every endpoint is explicitly non-legal and ACCEPTANCE-only. No route exposes
credentials or a force-retry capability for ambiguous transport outcomes.
"""
from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.config.eudr_acceptance import (
    READINESS_WAITING_FOR_CREDENTIALS,
    get_eudr_acceptance_settings,
)
from litoral_trace.db.tenant import get_tenant_scoped_db_session
from litoral_trace.services.eudr_acceptance_submission import (
    EudrAcceptanceAttemptView,
    EudrAcceptancePreparedView,
    EudrAcceptanceSubmissionError,
    EudrAcceptanceSubmissionPersistenceError,
    EudrAcceptanceSubmissionService,
)
from litoral_trace.services.eudr_dds_candidate import (
    EudrDdsCandidateError,
    EudrDdsCandidateNotFoundError,
    EudrDdsCandidatePersistenceError,
    EudrDdsCandidateValidationError,
)


router = APIRouter(prefix="/api/v1/eudr-acceptance", tags=["EUDR ACCEPTANCE V3"])


class EudrAcceptancePrepareRequest(BaseModel):
    operator_role: Literal["OPERATOR"] = "OPERATOR"
    country_of_activity: str = Field(min_length=2, max_length=2)
    border_cross_country: str | None = Field(default=None, min_length=2, max_length=2)
    internal_reference_number: str | None = Field(default=None, max_length=120)


def _service(user: UserTenantContext):
    session = get_tenant_scoped_db_session(user.organization_id)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "EUDR_ACCEPTANCE_UNAVAILABLE",
                "message": "La base de datos no está disponible temporalmente.",
            },
        )
    return session, EudrAcceptanceSubmissionService(
        session=session,
        organization_id=user.organization_id,
    )


def _attempt_payload(row: EudrAcceptanceAttemptView) -> dict[str, Any]:
    return {
        "public_id": str(row.public_id),
        "candidate_public_id": str(row.candidate_public_id),
        "shipment_code": row.shipment_code,
        "state": row.state,
        "environment": row.environment,
        "operation": row.operation,
        "operator_role": row.operator_role,
        "country_of_activity": row.country_of_activity,
        "border_cross_country": row.border_cross_country,
        "internal_reference_number": row.internal_reference_number,
        "candidate_payload_sha256": row.candidate_payload_sha256,
        "wire_contract_profile": row.wire_contract_profile,
        "wire_contract_sha256": row.wire_contract_sha256,
        "request_body_sha256": row.request_body_sha256,
        "envelope_sha256": row.envelope_sha256,
        "response_sha256": row.response_sha256,
        "request_body_bytes": row.request_body_bytes,
        "remote_uuid": row.remote_uuid,
        "remote_status": row.remote_status,
        "http_status": row.http_status,
        "error_code": row.error_code,
        "error_summary": row.error_summary,
        "sent_at": row.sent_at.isoformat() if row.sent_at else None,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
        "target_environment": "ACCEPTANCE",
        "legal_effect": "NONE_NON_LEGAL_ACCEPTANCE",
        "live_submission_performed": False,
        "ledger_mutated": False,
    }


def _prepared_payload(row: EudrAcceptancePreparedView) -> dict[str, Any]:
    return {
        "attempt": _attempt_payload(row.attempt),
        "created": row.created,
        "network_ready": row.network_ready,
        "acceptance_submission_performed": False,
        "live_submission_performed": False,
        "target_environment": "ACCEPTANCE",
        "legal_effect": "NONE_NON_LEGAL_ACCEPTANCE",
        "ledger_mutated": False,
    }


def _raise_error(exc: Exception) -> None:
    if isinstance(exc, EudrAcceptanceSubmissionPersistenceError) or isinstance(
        exc, EudrDdsCandidatePersistenceError
    ):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": getattr(exc, "code", "EUDR_ACCEPTANCE_UNAVAILABLE"), "message": str(exc)},
        ) from None

    if isinstance(exc, EudrDdsCandidateNotFoundError):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "SHIPMENT_NOT_FOUND", "message": str(exc)},
        ) from None

    if isinstance(exc, EudrAcceptanceSubmissionError):
        if exc.code in {"ACCEPTANCE_ATTEMPT_NOT_FOUND", "EUDR_CANDIDATE_NOT_FOUND"}:
            http_status = status.HTTP_404_NOT_FOUND
        elif exc.code in {
            "ACCEPTANCE_NETWORK_NOT_READY",
            "ACCEPTANCE_DELIVERY_UNCERTAIN",
            "ACCEPTANCE_RETRY_REQUIRES_EXPLICIT_OVERRIDE",
            "ACCEPTANCE_ATTEMPT_NOT_SENDABLE",
            "ACCEPTANCE_ATTEMPT_SHIPMENT_MISMATCH",
            "ACCEPTANCE_CANDIDATE_MISMATCH",
            "ACCEPTANCE_PREPARED_PAYLOAD_STALE",
        }:
            http_status = status.HTTP_409_CONFLICT
        else:
            http_status = status.HTTP_422_UNPROCESSABLE_CONTENT
        raise HTTPException(
            status_code=http_status,
            detail={"code": exc.code, "message": exc.detail},
        ) from None

    if isinstance(exc, EudrDdsCandidateValidationError):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"code": exc.code, "message": exc.detail},
        ) from None

    if isinstance(exc, EudrDdsCandidateError):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "EUDR_CANDIDATE_ERROR", "message": str(exc)},
        ) from None
    raise exc


@router.get("/readiness")
def get_eudr_acceptance_readiness(
    user: UserTenantContext = Depends(require_permission(Permission.LOTE_READ)),
) -> dict[str, Any]:
    """Return deployment readiness flags without returning credential material."""
    del user
    try:
        payload = get_eudr_acceptance_settings().sanitized_readiness()
        payload["configuration_valid"] = True
        payload["acceptance_smoke_performed"] = False
        return payload
    except (RuntimeError, ValueError):
        return {
            "state": READINESS_WAITING_FOR_CREDENTIALS,
            "enabled": False,
            "network_ready": False,
            "configuration_valid": False,
            "endpoint_configured": False,
            "username_configured": False,
            "authentication_key_configured": False,
            "web_service_client_id_configured": False,
            "missing_configuration": ["INVALID_RUNTIME_CONFIGURATION"],
            "target_environment": "ACCEPTANCE",
            "api_family": "V3",
            "legal_effect": "NONE_NON_LEGAL_ACCEPTANCE",
            "live_enabled": False,
            "acceptance_smoke_performed": False,
        }


@router.post("/{shipment_code}/prepare")
def prepare_eudr_acceptance(
    shipment_code: str,
    body: EudrAcceptancePrepareRequest,
    user: UserTenantContext = Depends(
        require_permission(Permission.TRACEABILITY_EVIDENCE)
    ),
) -> dict[str, Any]:
    session, service = _service(user)
    try:
        try:
            result = service.prepare(
                shipment_code=shipment_code,
                operator_role=body.operator_role,
                country_of_activity=body.country_of_activity,
                border_cross_country=body.border_cross_country,
                internal_reference_number=body.internal_reference_number,
                geo_location_confidential=False,
                actor_user_id=user.user_id,
            )
            return _prepared_payload(result)
        except Exception as exc:
            _raise_error(exc)
            raise
    finally:
        session.close()


@router.get("/attempts/{attempt_public_id}")
def get_eudr_acceptance_attempt(
    attempt_public_id: UUID,
    user: UserTenantContext = Depends(require_permission(Permission.LOTE_READ)),
) -> dict[str, Any]:
    session, service = _service(user)
    try:
        try:
            return _attempt_payload(service.get_attempt(attempt_public_id))
        except Exception as exc:
            _raise_error(exc)
            raise
    finally:
        session.close()


@router.post("/{shipment_code}/attempts/{attempt_public_id}/submit")
def submit_eudr_acceptance_attempt(
    shipment_code: str,
    attempt_public_id: UUID,
    user: UserTenantContext = Depends(
        require_permission(Permission.TRACEABILITY_EVIDENCE)
    ),
) -> dict[str, Any]:
    session, service = _service(user)
    try:
        try:
            row = service.submit(
                attempt_public_id=attempt_public_id,
                shipment_code=shipment_code,
                actor_user_id=user.user_id,
                allow_retry_after_transport_error=False,
            )
            payload = _attempt_payload(row)
            payload["acceptance_submission_performed"] = row.state in {
                "SENT",
                "REMOTE_ACCEPTED",
                "REMOTE_REJECTED",
                "TRANSPORT_ERROR",
            }
            return payload
        except Exception as exc:
            _raise_error(exc)
            raise
    finally:
        session.close()
