"""Router REST de la Bóveda Documental Privada (Document Vault)."""
from __future__ import annotations
import io
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse, StreamingResponse

from litoral_trace.api.auth import UserTenantContext
from litoral_trace.auth.rbac import Permission, require_permission
from litoral_trace.services.audit import (
    AuditAction,
    AuditOutcome,
    build_audit_actor_from_user,
    build_request_audit_context,
    record_audit_event_now,
)
from litoral_trace.services.vault import listar_documentos_boveda_tenant

router = APIRouter(prefix="/api/v1/vault", tags=["Bóveda Documental B2B"])

@router.get("/documents", tags=["Bóveda Documental B2B"])
async def consultar_documentos_boveda(
    q: str | None = Query(None, description="Buscador por nombre de archivo, rodal o CUIT"),
    type: str | None = Query(None, description="Filtro por tipo de documento"),
    user: UserTenantContext = Depends(require_permission(Permission.VAULT_READ))
) -> JSONResponse:
    """Consulta los documentos almacenados en la bóveda privada del cliente."""
    docs = listar_documentos_boveda_tenant(
        organization_id=user.organization_id,
        query_search=q,
        doc_type_filter=type
    )
    total_kb = sum(d["file_size_kb"] for d in docs)
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "organization_id": user.organization_id,
            "organization_name": user.organization_name,
            "total_documents": len(docs),
            "total_storage_kb": round(total_kb, 1),
            "documents": docs
        }
    )

@router.get("/download/{doc_id}", tags=["Bóveda Documental B2B"])
async def descargar_documento_boveda(
    doc_id: str,
    request: Request = None,
    user: UserTenantContext = Depends(require_permission(Permission.VAULT_READ))
) -> StreamingResponse:
    """Descarga segura de un documento desde la bóveda privada con verificación de tenant."""
    docs = listar_documentos_boveda_tenant(organization_id=user.organization_id)
    doc_match = next((d for d in docs if d["id"] == doc_id), None)
    
    if not doc_match:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Documento '{doc_id}' no encontrado o no pertenece a su organización."
        )
        
    # Contenido sintético según tipo de documento
    filename = doc_match["filename"]
    if doc_match["doc_type"] == "DDS_JSON_TRACES":
        content_bytes = f'{{\n  "reference_number": "{doc_id}",\n  "status": "COMPLIANT",\n  "organization": "{user.organization_name}"\n}}'.encode("utf-8")
        media_type = "application/json"
    elif doc_match["doc_type"] == "PDF_CERTIFICADO":
        content_bytes = f"%PDF-1.4 Litoral Trace Certificate {doc_id} Organization {user.organization_name}".encode("latin-1")
        media_type = "application/pdf"
    else:
        content_bytes = b"Litoral Trace Remito Excel Data"
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    record_audit_event_now(
        actor=build_audit_actor_from_user(user),
        action=AuditAction.VAULT_DOWNLOAD,
        entity_type="vault_document",
        entity_id=None,
        outcome=AuditOutcome.SUCCESS,
        request_context=build_request_audit_context(request),
        metadata={
            "document_id": doc_id,
            "filename": filename,
            "doc_type": doc_match["doc_type"],
        },
        best_effort=True,
    )

    return StreamingResponse(
        io.BytesIO(content_bytes),
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
