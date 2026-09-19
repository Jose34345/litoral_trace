"""Authenticated download endpoints for U.S. Lacey declaration exports."""
from __future__ import annotations

from io import BytesIO

from fastapi import APIRouter, Cookie, HTTPException, status
from fastapi.responses import Response, StreamingResponse

from litoral_trace.us_lacey.access import (
    UsLaceyOperationalAccessError,
    require_us_lacey_operational_access,
)
from litoral_trace.us_lacey.exporters import (
    build_lacey_excel,
    build_lawgs_xml,
    consolidate_export_snapshot,
)
from litoral_trace.us_lacey.operations import (
    UsLaceyOperationNotFound,
    UsLaceyOperationService,
)
from litoral_trace.us_lacey.portal_auth import (
    US_LACEY_SESSION_COOKIE,
    UsLaceyPortalAuthError,
    resolve_us_lacey_session,
)
from litoral_trace.us_lacey.semantic_evidence_read import SemanticEvidenceReadService


router = APIRouter()


def _identity_for_export(us_session: str | None):
    if not us_session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Sign in to export this operation.",
        )
    try:
        identity = resolve_us_lacey_session(us_session)
        require_us_lacey_operational_access(organization_id=identity.organization_id)
        return identity
    except UsLaceyPortalAuthError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Your session is invalid or expired.",
        ) from exc
    except UsLaceyOperationalAccessError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This account cannot export operational data.",
        ) from exc


def _export_snapshot(*, operation_id: str, us_session: str | None):
    identity = _identity_for_export(us_session)
    try:
        detail = UsLaceyOperationService().get_detail(
            organization_id=identity.organization_id,
            operation_public_id=operation_id,
        )
    except UsLaceyOperationNotFound as exc:
        raise HTTPException(status_code=404, detail="Operation not found.") from exc

    if detail.status != "COMPLETED":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Complete human review before exporting the declaration package.",
        )

    evidence_snapshot = SemanticEvidenceReadService().get_operation_evidence(
        organization_id=identity.organization_id,
        operation_public_id=detail.public_id,
    )
    return consolidate_export_snapshot(
        detail=detail,
        evidence_snapshot=evidence_snapshot,
    )


@router.get("/operations/{operation_id}/export/lawgs-xml")
def export_lawgs_xml(
    operation_id: str,
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
) -> Response:
    snapshot = _export_snapshot(operation_id=operation_id, us_session=us_session)
    xml_data = build_lawgs_xml(snapshot)
    response = Response(content=xml_data, media_type="application/xml")
    response.headers["Content-Disposition"] = (
        f'attachment; filename="lawgs_declaration_{snapshot.operation_id}.xml"'
    )
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


@router.get("/operations/{operation_id}/export/excel")
def export_lacey_excel(
    operation_id: str,
    us_session: str | None = Cookie(None, alias=US_LACEY_SESSION_COOKIE),
) -> StreamingResponse:
    snapshot = _export_snapshot(operation_id=operation_id, us_session=us_session)
    excel_io: BytesIO = build_lacey_excel(snapshot)
    response = StreamingResponse(
        excel_io,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response.headers["Content-Disposition"] = (
        f'attachment; filename="lacey_summary_{snapshot.operation_id}.xlsx"'
    )
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response
