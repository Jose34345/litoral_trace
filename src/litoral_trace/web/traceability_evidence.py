"""Server-rendered UX10-E contextual evidence workspace."""
from __future__ import annotations

from datetime import date
from urllib.parse import urlencode
from uuid import UUID

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import HTMLResponse, RedirectResponse

from litoral_trace.auth.rbac import Permission, has_permission
from litoral_trace.config import get_settings
from litoral_trace.services.audit import build_audit_actor_from_user
from litoral_trace.services.traceability_evidence import (
    EvidenceSubjectChoice,
    TraceabilityEvidenceConflictError,
    TraceabilityEvidenceError,
    TraceabilityEvidenceNotFoundError,
    TraceabilityEvidencePersistenceError,
    TraceabilityEvidenceService,
    TraceabilityEvidenceValidationError,
)
from litoral_trace.services.vault import (
    VaultError,
    VaultService,
    VaultValidationError,
)
from litoral_trace.web.csrf import enforce_csrf, get_csrf_browser_nonce
from litoral_trace.web.runtime import (
    get_html_route_user,
    render_access_denied,
    render_csrf_failure,
    render_web_template,
)


router = APIRouter(tags=["Frontend B2B"])

_SUBJECT_LABELS = {
    "SOURCE_LOTE": "Origen",
    "TRACEABILITY_EVENT": "Movimiento",
    "TRACEABILITY_BATCH": "Lote industrial",
    "SHIPMENT": "Despacho",
}
_EVIDENCE_LABELS = {
    "ORIGIN_AUTHORIZATION": "Autorización de origen",
    "FOREST_GUIDE": "Guía forestal",
    "FRUIT_GUIDE": "Guía de Frutos",
    "REMITO": "Remito",
    "INVOICE": "Factura / documento comercial",
    "CERTIFICATE": "Certificado",
    "TRANSPORT": "Documento de transporte",
    "GEOSPATIAL": "Evidencia geoespacial",
    "SUPPLIER_DECLARATION": "Declaración de proveedor",
    "OTHER": "Otra evidencia",
}
_DOCUMENT_TYPE_LABELS = {
    "PDF_CERTIFICADO": "PDF / certificado",
    "REMITO_EXCEL": "Planilla XLSX",
    "OTHER_EVIDENCE": "Otra evidencia",
    "DDS_JSON_TRACES": "JSON técnico",
}
_RESULT_MESSAGES = {
    "linked": ("success", "Evidencia vinculada", "El documento quedó asociado al eslabón seleccionado."),
    "replayed": ("info", "Vínculo ya existente", "Ese documento ya respaldaba el eslabón seleccionado."),
    "uploaded-linked": ("success", "Documento cargado y vinculado", "El archivo quedó guardado en Documentos y evidencias y asociado al eslabón."),
    "unlinked": ("success", "Evidencia desvinculada", "Se quitó la asociación sin borrar el documento de la bóveda."),
}


def _service() -> TraceabilityEvidenceService:
    return TraceabilityEvidenceService()


def _vault() -> VaultService:
    return VaultService()


def _subject_key(subject_type: str, reference: str) -> str:
    return f"{subject_type}|{reference}"


def _parse_subject_key(value: str | None) -> tuple[str | None, str | None]:
    normalized = str(value or "").strip()
    if not normalized or "|" not in normalized:
        return None, None
    subject_type, reference = normalized.split("|", 1)
    subject_type = subject_type.strip().upper()
    reference = reference.strip()
    if not subject_type or not reference:
        return None, None
    return subject_type, reference


def _date(value: str | None) -> date | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        return date.fromisoformat(normalized)
    except ValueError as exc:
        raise TraceabilityEvidenceValidationError(
            "INVALID_DATE",
            "Una de las fechas documentales no tiene un formato válido.",
        ) from exc


def _safe_error(exc: Exception) -> dict[str, str]:
    if isinstance(exc, TraceabilityEvidenceValidationError):
        return {"title": "Datos documentales no válidos", "message": exc.detail}
    if isinstance(exc, TraceabilityEvidenceNotFoundError):
        return {"title": "Referencia no encontrada", "message": str(exc)}
    if isinstance(exc, TraceabilityEvidenceConflictError):
        return {"title": "Vínculo en conflicto", "message": str(exc)}
    if isinstance(exc, TraceabilityEvidencePersistenceError):
        return {"title": "Evidencia no disponible", "message": "No fue posible completar la operación documental en este momento."}
    if isinstance(exc, VaultValidationError):
        return {"title": "Archivo no válido", "message": str(exc)}
    if isinstance(exc, VaultError):
        return {"title": "Documentos no disponibles", "message": "No fue posible completar la operación con la bóveda documental."}
    return {"title": "No se pudo completar la operación", "message": "La operación documental fue rechazada de forma segura."}


def _status_for_error(exc: Exception) -> int:
    if isinstance(exc, (TraceabilityEvidenceValidationError, VaultValidationError)):
        return status.HTTP_422_UNPROCESSABLE_CONTENT
    if isinstance(exc, TraceabilityEvidenceNotFoundError):
        return status.HTTP_404_NOT_FOUND
    if isinstance(exc, TraceabilityEvidenceConflictError):
        return status.HTTP_409_CONFLICT
    if isinstance(exc, (TraceabilityEvidencePersistenceError, VaultError)):
        return status.HTTP_503_SERVICE_UNAVAILABLE
    return status.HTTP_400_BAD_REQUEST


def _present_subject(subject: EvidenceSubjectChoice) -> dict[str, str]:
    return {
        "key": _subject_key(subject.subject_type, subject.reference),
        "subject_type": subject.subject_type,
        "type_label": _SUBJECT_LABELS.get(subject.subject_type, subject.subject_type),
        "reference": subject.reference,
        "label": subject.label,
        "secondary": subject.secondary,
        "status": subject.status,
    }


def _present_evidence(item) -> dict:
    return {
        "link_public_id": str(item.link_public_id),
        "subject_key": _subject_key(item.subject_type, item.subject_reference),
        "subject_type": item.subject_type,
        "subject_type_label": _SUBJECT_LABELS.get(item.subject_type, item.subject_type),
        "subject_label": item.subject_label,
        "evidence_type": item.evidence_type,
        "evidence_type_label": _EVIDENCE_LABELS.get(item.evidence_type, item.evidence_type),
        "reference_number": item.reference_number or "—",
        "issuer": item.issuer or "—",
        "document_date": item.document_date.strftime("%d/%m/%Y") if item.document_date else "—",
        "validity": (
            f"{item.valid_from.strftime('%d/%m/%Y') if item.valid_from else '—'} → "
            f"{item.valid_until.strftime('%d/%m/%Y') if item.valid_until else '—'}"
            if item.valid_from or item.valid_until
            else "—"
        ),
        "notes": item.notes or "",
        "filename": item.document_filename,
        "document_type_label": _DOCUMENT_TYPE_LABELS.get(item.document_type, item.document_type),
        "status": item.document_status,
        "sha256": item.document_sha256,
        "size_kb": round(item.document_size_bytes / 1024, 1),
        "download_href": f"/api/v1/vault/documents/{item.vault_document_public_id}/download",
    }


def _present_workspace(
    *,
    user,
    selected_key: str | None,
    result_code: str | None = None,
    error: dict[str, str] | None = None,
) -> dict:
    service = _service()
    subjects = service.list_subjects(organization_id=user.organization_id)
    subject_views = tuple(_present_subject(subject) for subject in subjects)
    valid_keys = {subject["key"] for subject in subject_views}
    selected_key = selected_key if selected_key in valid_keys else (subject_views[0]["key"] if subject_views else None)
    selected_type, selected_reference = _parse_subject_key(selected_key)

    evidence = service.list_evidence(
        organization_id=user.organization_id,
        subject_type=selected_type,
        subject_reference=selected_reference,
    ) if selected_type and selected_reference else ()
    coverage = service.coverage(organization_id=user.organization_id)

    available_documents = ()
    vault_error = None
    if has_permission(user, Permission.VAULT_READ):
        try:
            available_documents = tuple(
                document
                for document in _vault().list_documents(organization_id=user.organization_id)
                if document.status == "available"
            )
        except VaultError:
            vault_error = {
                "title": "Bóveda no disponible",
                "message": "Podés revisar la cobertura registrada, pero no fue posible cargar la lista de documentos disponibles.",
            }

    coverage_by_type = {
        key: {
            "label": _SUBJECT_LABELS[key],
            "covered": coverage.by_subject_type.get(key, (0, 0))[0],
            "total": coverage.by_subject_type.get(key, (0, 0))[1],
        }
        for key in _SUBJECT_LABELS
    }
    message = None
    if result_code in _RESULT_MESSAGES:
        level, title, text = _RESULT_MESSAGES[result_code]
        message = {"level": level, "title": title, "message": text}

    return {
        "subjects": subject_views,
        "selected_key": selected_key,
        "selected_subject": next((item for item in subject_views if item["key"] == selected_key), None),
        "evidence": tuple(_present_evidence(item) for item in evidence),
        "available_documents": tuple(
            {
                "public_id": str(document.public_id),
                "filename": document.filename,
                "document_type": document.document_type,
                "document_type_label": _DOCUMENT_TYPE_LABELS.get(document.document_type, document.document_type),
                "sha256": document.sha256,
            }
            for document in available_documents
        ),
        "coverage": {
            "covered": coverage.subjects_with_evidence,
            "total": coverage.total_subjects,
            "percentage": coverage.percentage,
            "by_type": coverage_by_type,
        },
        "evidence_types": _EVIDENCE_LABELS,
        "document_types": {
            key: label
            for key, label in _DOCUMENT_TYPE_LABELS.items()
            if key != "DDS_JSON_TRACES"
        },
        "can_manage": has_permission(user, Permission.TRACEABILITY_EVIDENCE),
        "can_upload": has_permission(user, Permission.TRACEABILITY_EVIDENCE) and has_permission(user, Permission.VAULT_UPLOAD),
        "message": message,
        "error": error or vault_error,
    }


def _render(request: Request, *, user, selected_key: str | None, result_code: str | None = None, error: dict[str, str] | None = None, status_code: int = 200) -> HTMLResponse:
    try:
        view = _present_workspace(
            user=user,
            selected_key=selected_key,
            result_code=result_code,
            error=error,
        )
    except TraceabilityEvidenceError as exc:
        view = {
            "subjects": (), "selected_key": None, "selected_subject": None,
            "evidence": (), "available_documents": (),
            "coverage": {"covered": 0, "total": 0, "percentage": 0, "by_type": {}},
            "evidence_types": _EVIDENCE_LABELS, "document_types": {},
            "can_manage": False, "can_upload": False,
            "message": None, "error": _safe_error(exc),
        }
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return render_web_template(
        request,
        "traceability_evidence.html",
        user=user,
        context={"evidence_view": view},
        status_code=status_code,
    )


async def _csrf_or_response(request: Request, user):
    try:
        await enforce_csrf(
            request,
            user=user,
            browser_nonce=get_csrf_browser_nonce(request),
            require_browser_binding=True,
        )
    except HTTPException:
        return render_csrf_failure()
    return None


def _redirect(selected_key: str | None, result: str) -> RedirectResponse:
    query = {"result": result}
    if selected_key:
        query["subject"] = selected_key
    return RedirectResponse(
        url=f"/evidence?{urlencode(query)}",
        status_code=status.HTTP_303_SEE_OTHER,
    )


@router.get("/evidence", response_class=HTMLResponse, include_in_schema=False, name="render_traceability_evidence")
async def render_traceability_evidence(request: Request) -> HTMLResponse:
    user, denied = get_html_route_user(request, required_permission=Permission.LOTE_READ)
    if denied is not None:
        return denied
    if not has_permission(user, Permission.VAULT_READ):
        return render_access_denied()
    return _render(
        request,
        user=user,
        selected_key=request.query_params.get("subject"),
        result_code=request.query_params.get("result"),
    )


@router.post("/evidence/link", include_in_schema=False, name="link_traceability_evidence")
async def link_traceability_evidence(request: Request):
    user, denied = get_html_route_user(request, required_permission=Permission.TRACEABILITY_EVIDENCE)
    if denied is not None:
        return denied
    if not has_permission(user, Permission.VAULT_READ):
        return render_access_denied()
    csrf_response = await _csrf_or_response(request, user)
    if csrf_response is not None:
        return csrf_response
    form = await request.form()
    selected_key = str(form.get("subject", ""))
    subject_type, subject_reference = _parse_subject_key(selected_key)
    if not subject_type or not subject_reference:
        exc = TraceabilityEvidenceValidationError("INVALID_SUBJECT", "Seleccioná un eslabón válido de la cadena de custodia.")
        return _render(request, user=user, selected_key=None, error=_safe_error(exc), status_code=422)
    try:
        result = _service().link_evidence(
            organization_id=user.organization_id,
            actor=build_audit_actor_from_user(user),
            subject_type=subject_type,
            subject_reference=subject_reference,
            vault_document_id=str(form.get("document_id", "")),
            evidence_type=str(form.get("evidence_type", "")),
            reference_number=form.get("reference_number"),
            issuer=form.get("issuer"),
            document_date=_date(form.get("document_date")),
            valid_from=_date(form.get("valid_from")),
            valid_until=_date(form.get("valid_until")),
            notes=form.get("notes"),
        )
        return _redirect(selected_key, "replayed" if result.replayed else "linked")
    except TraceabilityEvidenceError as exc:
        return _render(request, user=user, selected_key=selected_key, error=_safe_error(exc), status_code=_status_for_error(exc))


@router.post("/evidence/upload-link", include_in_schema=False, name="upload_and_link_traceability_evidence")
async def upload_and_link_traceability_evidence(
    request: Request,
    file: UploadFile = File(...),
    subject: str = Form(...),
    document_type: str = Form(...),
    evidence_type: str = Form(...),
    reference_number: str = Form(default=""),
    issuer: str = Form(default=""),
    document_date: str = Form(default=""),
    valid_from: str = Form(default=""),
    valid_until: str = Form(default=""),
    notes: str = Form(default=""),
):
    user, denied = get_html_route_user(request, required_permission=Permission.TRACEABILITY_EVIDENCE)
    if denied is not None:
        return denied
    if not has_permission(user, Permission.VAULT_UPLOAD):
        return render_access_denied()
    csrf_response = await _csrf_or_response(request, user)
    if csrf_response is not None:
        return csrf_response
    subject_type, subject_reference = _parse_subject_key(subject)
    if not subject_type or not subject_reference:
        exc = TraceabilityEvidenceValidationError("INVALID_SUBJECT", "Seleccioná un eslabón válido de la cadena de custodia.")
        return _render(request, user=user, selected_key=None, error=_safe_error(exc), status_code=422)

    max_bytes = get_settings().storage.max_upload_bytes
    payload = await file.read(max_bytes + 1)
    if len(payload) > max_bytes:
        exc = VaultValidationError("El archivo excede el tamaño máximo permitido.")
        return _render(request, user=user, selected_key=subject, error=_safe_error(exc), status_code=422)

    try:
        document = _vault().upload_document(
            organization_id=user.organization_id,
            created_by_user_id=user.user_id,
            filename=file.filename or "documento",
            document_type=document_type,
            content_type=file.content_type or "",
            content=payload,
        )
    except VaultError as exc:
        return _render(request, user=user, selected_key=subject, error=_safe_error(exc), status_code=_status_for_error(exc))

    try:
        _service().link_evidence(
            organization_id=user.organization_id,
            actor=build_audit_actor_from_user(user),
            subject_type=subject_type,
            subject_reference=subject_reference,
            vault_document_id=document.public_id,
            evidence_type=evidence_type,
            reference_number=reference_number,
            issuer=issuer,
            document_date=_date(document_date),
            valid_from=_date(valid_from),
            valid_until=_date(valid_until),
            notes=notes,
        )
        return _redirect(subject, "uploaded-linked")
    except TraceabilityEvidenceError as exc:
        error = _safe_error(exc)
        error["message"] = (
            "El archivo quedó guardado en Documentos y evidencias, pero no pudo vincularse al eslabón. "
            + error["message"]
        )
        return _render(request, user=user, selected_key=subject, error=error, status_code=_status_for_error(exc))


@router.post("/evidence/{link_public_id}/unlink", include_in_schema=False, name="unlink_traceability_evidence")
async def unlink_traceability_evidence(request: Request, link_public_id: UUID):
    user, denied = get_html_route_user(request, required_permission=Permission.TRACEABILITY_EVIDENCE)
    if denied is not None:
        return denied
    csrf_response = await _csrf_or_response(request, user)
    if csrf_response is not None:
        return csrf_response
    form = await request.form()
    selected_key = str(form.get("subject", "")) or None
    try:
        _service().unlink_evidence(
            organization_id=user.organization_id,
            actor=build_audit_actor_from_user(user),
            link_public_id=link_public_id,
        )
        return _redirect(selected_key, "unlinked")
    except TraceabilityEvidenceError as exc:
        return _render(request, user=user, selected_key=selected_key, error=_safe_error(exc), status_code=_status_for_error(exc))