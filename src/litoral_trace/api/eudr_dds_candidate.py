"""Tenant-scoped local EUDR API V3 DDS candidate API.

No endpoint in this module performs a LIVE or ACCEPTANCE network submission.
It exposes only local configuration, fail-closed conformance and a deterministic
non-legal candidate payload/hash.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.db.tenant import get_tenant_scoped_db_session, set_tenant_db_context
from litoral_trace.services.eudr_dds_candidate import (
    EudrDdsCandidateNotFoundError,
    EudrDdsCandidatePersistenceError,
    EudrDdsCandidateService,
    EudrDdsCandidateValidationError,
)


router = APIRouter(prefix="/api/v1/eudr-candidates", tags=["EUDR DDS Conformance"])


class EudrDdsCandidateRequest(BaseModel):
    activity_type: str
    commodity_profile: str
    operator_name: str | None = Field(default=None, max_length=240)
    operator_address: str | None = Field(default=None, max_length=2000)
    operator_country_code: str | None = Field(default=None, max_length=2)
    operator_eori: str | None = Field(default=None, max_length=32)
    hs_code: str | None = Field(default=None, max_length=16)
    trade_name: str | None = Field(default=None, max_length=240)
    product_description: str | None = Field(default=None, max_length=4000)
    common_species_name: str | None = Field(default=None, max_length=240)
    scientific_species_name: str | None = Field(default=None, max_length=240)
    net_mass_kg: str | float | None = None
    production_country_code: str | None = Field(default=None, max_length=2)
    production_date_from: date | None = None
    production_date_to: date | None = None
    relies_on_previous_dds: bool = False
    previous_dds_reference: str | None = Field(default=None, max_length=160)
    previous_dds_verification: str | None = Field(default=None, max_length=160)
    risk_conclusion: str = "UNASSESSED"
    risk_assessment_reference: str | None = Field(default=None, max_length=240)
    risk_assessed_at: datetime | None = None
    notes: str | None = Field(default=None, max_length=4000)


def _service(user: UserTenantContext):
    session = get_tenant_scoped_db_session(user.organization_id)
    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "EUDR_CANDIDATE_UNAVAILABLE",
                "message": "La base de datos no está disponible temporalmente.",
            },
        )
    return session, EudrDdsCandidateService(
        session=session,
        organization_id=user.organization_id,
    )


def _raise_service_error(exc: Exception) -> None:
    if isinstance(exc, EudrDdsCandidateValidationError):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"code": exc.code, "message": exc.detail},
        ) from None
    if isinstance(exc, EudrDdsCandidateNotFoundError):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "SHIPMENT_NOT_FOUND", "message": str(exc)},
        ) from None
    if isinstance(exc, EudrDdsCandidatePersistenceError):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"code": "EUDR_CANDIDATE_UNAVAILABLE", "message": str(exc)},
        ) from None
    raise exc


def _candidate_payload(row) -> dict[str, Any]:
    return {
        "public_id": str(row.public_id),
        "shipment_public_id": str(row.shipment_public_id),
        "shipment_code": row.shipment_code,
        "activity_type": row.activity_type,
        "commodity_profile": row.commodity_profile,
        "operator_name": row.operator_name,
        "operator_address": row.operator_address,
        "operator_country_code": row.operator_country_code,
        "operator_eori": row.operator_eori,
        "hs_code": row.hs_code,
        "trade_name": row.trade_name,
        "product_description": row.product_description,
        "common_species_name": row.common_species_name,
        "scientific_species_name": row.scientific_species_name,
        "net_mass_kg": row.net_mass_kg,
        "production_country_code": row.production_country_code,
        "production_date_from": (
            row.production_date_from.isoformat() if row.production_date_from else None
        ),
        "production_date_to": (
            row.production_date_to.isoformat() if row.production_date_to else None
        ),
        "relies_on_previous_dds": row.relies_on_previous_dds,
        "previous_dds_reference": row.previous_dds_reference,
        "previous_dds_verification": row.previous_dds_verification,
        "risk_conclusion": row.risk_conclusion,
        "risk_assessment_reference": row.risk_assessment_reference,
        "risk_assessed_at": row.risk_assessed_at.isoformat() if row.risk_assessed_at else None,
        "spec_profile": row.spec_profile,
        "spec_fingerprint_sha256": row.spec_fingerprint_sha256,
        "notes": row.notes,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }


def _conformance_payload(row) -> dict[str, Any]:
    return {
        "shipment_public_id": str(row.shipment_public_id),
        "shipment_code": row.shipment_code,
        "state": row.state,
        "ready": row.ready,
        "missing": list(row.missing),
        "lineage_complete": row.lineage_complete,
        "requirements": [
            {
                "key": item.key,
                "label": item.label,
                "satisfied": item.satisfied,
                "source": item.source,
                "detail": item.detail,
            }
            for item in row.requirements
        ],
        "candidate": _candidate_payload(row.candidate) if row.candidate else None,
        "plots": list(row.plots),
        "payload": row.payload,
        "payload_sha256": row.payload_sha256,
        "target_environment": row.target_environment,
        "legal_effect": row.legal_effect,
        "acceptance_submission_performed": False,
        "live_submission_performed": False,
        "ledger_mutated": False,
    }


@router.get("/{shipment_code}")
def get_eudr_candidate_conformance(
    shipment_code: str,
    user: UserTenantContext = Depends(require_permission(Permission.LOTE_READ)),
) -> dict[str, Any]:
    session, service = _service(user)
    try:
        try:
            return _conformance_payload(service.conformance(shipment_code))
        except Exception as exc:
            _raise_service_error(exc)
            raise
    finally:
        session.close()


@router.put("/{shipment_code}")
def upsert_eudr_candidate(
    shipment_code: str,
    body: EudrDdsCandidateRequest,
    user: UserTenantContext = Depends(
        require_permission(Permission.TRACEABILITY_EVIDENCE)
    ),
) -> dict[str, Any]:
    session, service = _service(user)
    try:
        try:
            row = service.upsert_candidate(
                shipment_code=shipment_code,
                activity_type=body.activity_type,
                commodity_profile=body.commodity_profile,
                operator_name=body.operator_name,
                operator_address=body.operator_address,
                operator_country_code=body.operator_country_code,
                operator_eori=body.operator_eori,
                hs_code=body.hs_code,
                trade_name=body.trade_name,
                product_description=body.product_description,
                common_species_name=body.common_species_name,
                scientific_species_name=body.scientific_species_name,
                net_mass_kg=body.net_mass_kg,
                production_country_code=body.production_country_code,
                production_date_from=body.production_date_from,
                production_date_to=body.production_date_to,
                relies_on_previous_dds=body.relies_on_previous_dds,
                previous_dds_reference=body.previous_dds_reference,
                previous_dds_verification=body.previous_dds_verification,
                risk_conclusion=body.risk_conclusion,
                risk_assessment_reference=body.risk_assessment_reference,
                risk_assessed_at=body.risk_assessed_at,
                notes=body.notes,
                actor_user_id=user.user_id,
            )
            set_tenant_db_context(session, user.organization_id)
            conformance = service.conformance(shipment_code)
            return {
                "candidate": _candidate_payload(row),
                "conformance": _conformance_payload(conformance),
                "acceptance_submission_performed": False,
                "live_submission_performed": False,
                "ledger_mutated": False,
            }
        except Exception as exc:
            _raise_service_error(exc)
            raise
    finally:
        session.close()
