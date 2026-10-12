"""Explicitly opt-in Assurance V2 portal adapter.

Never mount an independent authentication surface. SHADOW mode exposes a
tenant-authenticated evidence case. Commands require a separate opt-in switch
and the V2 review journal; ENABLED remains blocked pending release acceptance.
"""
from __future__ import annotations

import hmac
import os
from uuid import UUID

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from sqlalchemy import select

from litoral_trace.db.models import (
    AssuranceDocument, UsLaceyOperation, UsLaceyOperationDocument,
    UsLaceySourceSetMember, UsLaceySourceSetRevision, VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.services.audit import (
    AuditAction, AuditActor, AuditOutcome, record_audit_event,
)
from litoral_trace.services.vault import (
    VaultConflictError, VaultIntegrityError, VaultNotFoundError,
    VaultPersistenceError, VaultService, VaultStorageOperationError,
    VaultValidationError,
)

from litoral_trace.us_lacey.access import (
    UsLaceyOperationalAccessError, require_us_lacey_operational_access,
)
from litoral_trace.us_lacey.assurance_v2_case_reader import read_tenant_assurance_case
from litoral_trace.us_lacey.assurance_v2_botanical_batch import approve_botanical_evidence_group
from litoral_trace.us_lacey.assurance_v2_historical_source import verified_historical_original
from litoral_trace.us_lacey.assurance_v2_batch_review import accept_assurance_v2_batch
from litoral_trace.us_lacey.assurance_v2_review_delta import read_p1_review_insights
from litoral_trace.us_lacey.assurance_v2_export_authorization import (
    explicitly_authorize_assurance_export,
)
from litoral_trace.us_lacey.assurance_v2_issuance import (
    StaleAssuranceIssue, issue_locked_assurance_package,
)
from litoral_trace.us_lacey.assurance_v2_review_adapter import review_journal_enabled
from litoral_trace.us_lacey.assurance_v2_reuse_application import (
    apply_authorized_assurance_v2_reuse,
)
from litoral_trace.us_lacey.assurance_v2_identity_confirmation import (
    confirm_assurance_v2_identity,
)
from litoral_trace.us_lacey.csrf import us_lacey_csrf_token
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.exporters.assurance_v2_package import (
    PostgresOperationEventPackageStore,
)
from litoral_trace.us_lacey.portal_auth import (
    US_LACEY_SESSION_COOKIE, UsLaceyPortalAuthError,
    resolve_us_lacey_session,
)
from litoral_trace.us_lacey.review import UsLaceyReviewError, review_us_lacey_field
from litoral_trace.web.us_lacey_assurance_v2_views import (
    WorkspacePrincipal, create_assurance_v2_router, templates,
)


def _resolve_current_source_document(
    *, organization_id: int, operation_public_id: str,
    assurance_document_id: str, session_factory=get_us_lacey_db_session,
) -> dict[str, str] | None:
    """Resolve a source only through this tenant's finalized current set."""
    try:
        operation_uuid = UUID(str(operation_public_id))
        document_id = int(assurance_document_id)
        if document_id <= 0:
            return None
    except (TypeError, ValueError, AttributeError):
        return None
    org_id = int(organization_id)
    db = session_factory()
    try:
        set_tenant_db_context(db, org_id)
        operation = db.scalar(select(UsLaceyOperation).where(
            UsLaceyOperation.organization_id == org_id,
            UsLaceyOperation.public_id == operation_uuid,
        ))
        if operation is None:
            return None
        revision = db.scalar(select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == org_id,
            UsLaceySourceSetRevision.operation_id == operation.id,
            UsLaceySourceSetRevision.is_current.is_(True),
            UsLaceySourceSetRevision.status == "FINALIZED",
        ))
        if revision is None:
            return None
        found = db.execute(
            select(UsLaceyOperationDocument, AssuranceDocument, VaultDocument)
            .join(UsLaceySourceSetMember,
                  UsLaceySourceSetMember.operation_document_id == UsLaceyOperationDocument.id)
            .join(AssuranceDocument,
                  AssuranceDocument.id == UsLaceyOperationDocument.assurance_document_id)
            .join(VaultDocument, VaultDocument.id == AssuranceDocument.vault_document_id)
            .where(
                UsLaceyOperationDocument.organization_id == org_id,
                UsLaceyOperationDocument.operation_id == operation.id,
                UsLaceyOperationDocument.assurance_document_id == document_id,
                UsLaceyOperationDocument.is_current.is_(True),
                UsLaceySourceSetMember.organization_id == org_id,
                UsLaceySourceSetMember.source_set_revision_id == revision.id,
                UsLaceySourceSetMember.assurance_document_id == document_id,
                AssuranceDocument.organization_id == org_id,
                VaultDocument.organization_id == org_id,
                VaultDocument.status == "available",
            )
        ).one_or_none()
        if found is None:
            return None
        link, _, vault = found
        return {
            "vault_public_id": str(vault.public_id),
            "sha256": str(vault.sha256),
            "version": str(link.version_number),
        }
    finally:
        db.close()


def _audit_source_access(
    *, principal: WorkspacePrincipal, operation_id: str,
    assurance_document_id: str, session_factory=get_us_lacey_db_session,
) -> None:
    db = session_factory()
    try:
        set_tenant_db_context(db, principal.organization_id)
        record_audit_event(
            db,
            actor=AuditActor(
                organization_id=principal.organization_id,
                user_id=int(principal.actor_id),
                username=principal.actor_email or None,
                role="us_lacey_customer",
            ),
            action=AuditAction.VAULT_DOWNLOAD,
            entity_type="assurance_document",
            entity_id=int(assurance_document_id),
            outcome=AuditOutcome.SUCCESS,
            metadata={"operation_public_id": operation_id, "source": "assurance_v2"},
            detail="Original source viewed from Assurance V2 case.",
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def configure_assurance_v2_shadow(
    app: FastAPI, *, mode: str | None = None, enable_commands: bool | None = None,
) -> bool:
    """Install tenant-scoped projection; commands default OFF until RC gates."""
    setting = (mode if mode is not None
               else os.environ.get("US_LACEY_ASSURANCE_V2_MODE", "OFF"))
    normalized = str(setting).strip().upper()
    environment = os.environ.get("US_LACEY_ENVIRONMENT", "").strip().lower()
    lab_org_ids = frozenset(
        int(item.strip()) for item in
        os.environ.get("US_LACEY_ASSURANCE_V2_LAB_ORG_IDS", "").split(",")
        if item.strip().isdigit() and int(item.strip()) > 0
    )
    app.state.assurance_v2_lab_org_ids = lab_org_ids if normalized == "SHADOW" else frozenset()
    if normalized in {"", "OFF", "DISABLED"}:
        return False
    if environment in {"production", "pilot"} and not lab_org_ids:
        raise ValueError("Production Assurance V2 shadow requires an explicit organization allowlist")
    if normalized != "SHADOW":
        raise ValueError("Assurance V2 final release is blocked until full E2E acceptance")
    commands = (
        os.environ.get("US_LACEY_ASSURANCE_V2_COMMANDS", "OFF").strip().upper() == "ON"
        if enable_commands is None else bool(enable_commands)
    )

    def principal_for(request: Request) -> WorkspacePrincipal:
        cookie = request.cookies.get(US_LACEY_SESSION_COOKIE)
        if not cookie:
            raise HTTPException(401, "Authentication required")
        try:
            identity = resolve_us_lacey_session(cookie)
            require_us_lacey_operational_access(
                organization_id=int(identity.organization_id),
                require_mutation_access=request.method not in {"GET", "HEAD"},
            )
        except (UsLaceyPortalAuthError, UsLaceyOperationalAccessError):
            raise HTTPException(401, "Session unavailable") from None
        if environment in {"production", "pilot"} and int(identity.organization_id) not in lab_org_ids:
            raise HTTPException(404, "Assurance V2 private lab is unavailable")
        operation_id = str(request.path_params.get("operation_id") or "")
        if environment in {"production", "pilot"} and operation_id:
            try:
                op_uuid = UUID(operation_id)
            except ValueError:
                raise HTTPException(404, "Test operation not found") from None
            db = get_us_lacey_db_session()
            try:
                set_tenant_db_context(db, int(identity.organization_id))
                test_operation = db.scalar(select(UsLaceyOperation).where(
                    UsLaceyOperation.organization_id == int(identity.organization_id),
                    UsLaceyOperation.public_id == op_uuid,
                    UsLaceyOperation.client_reference.startswith("V2LAB-"),
                ))
                if test_operation is None:
                    raise HTTPException(404, "Assurance V2 is limited to V2LAB- test shipments")
            finally:
                db.close()
        csrf = us_lacey_csrf_token(
            session_token=cookie, purpose=f"assurance-v2:{operation_id}",
        )
        return WorkspacePrincipal(
            organization_id=int(identity.organization_id),
            actor_id=str(identity.user_id),
            csrf_token=csrf,
            actor_email=str(identity.email),
        )

    def require_commands(principal: WorkspacePrincipal) -> None:
        # In production the journal is scoped to the authenticated owner and
        # specifically-marked V2LAB shipments; never global to existing customers.
        if environment in {"production", "pilot"}:
            journal_ok = review_journal_enabled(
                principal.organization_id, "V2LAB-PRIVATE-REVIEW",
            )
        else:
            journal_ok = review_journal_enabled()
        if not commands or not journal_ok:
            raise HTTPException(503, "Assurance V2 commands are disabled pending release acceptance")

    def decision_gateway(principal, operation_id, command):
        require_commands(principal)
        action = command["action"]
        exception_id = command["exception_id"]
        if action not in {"ACCEPT", "CORRECT"} or not exception_id.startswith("field:"):
            raise HTTPException(422, "This review action has no V2 authority adapter")
        try:
            field_id = int(exception_id[6:])
            if field_id <= 0:
                raise ValueError
        except ValueError:
            raise HTTPException(422, "Invalid field reference") from None
        candidate_id = command["candidate_id"]
        source_claim_id = None
        if candidate_id.startswith("source:") and action == "ACCEPT":
            source_claim_id = candidate_id.removeprefix("source:")
        elif candidate_id and candidate_id != f"legacy:{field_id}":
            raise HTTPException(422, "Candidate reference is not bound to this review field")
        try:
            result = review_us_lacey_field(
                organization_id=principal.organization_id,
                operation_public_id=operation_id,
                field_id=field_id,
                user_id=int(principal.actor_id),
                user_email=principal.actor_email,
                action="accept" if action == "ACCEPT" else "edit",
                value=command["value"] if action == "CORRECT" else None,
                reason_code=command["reason"] or None,
                candidate_id=None,
                source_claim_id=source_claim_id,
                expected_source_set_fingerprint=command["expected_source_set_fingerprint"],
                expected_case_fingerprint=command["expected_case_fingerprint"],
            )
        except UsLaceyReviewError as exc:
            status = 409 if "stale" in str(exc).lower() else 422
            raise HTTPException(status, str(exc)) from None
        return {"status": "RECORDED", "field_id": result.field_id,
                "field_status": result.field_status}

    def batch_review_gateway(principal, operation_id, selections, reason,
                             source_fingerprint, case_fingerprint,
                             active_review_seconds=None):
        require_commands(principal)
        try:
            return accept_assurance_v2_batch(
                organization_id=principal.organization_id,
                operation_public_id=operation_id,
                authenticated_user_id=int(principal.actor_id),
                selected=selections, reason=reason, explicit_confirmation=True,
                expected_source_set_fingerprint=source_fingerprint,
                expected_case_fingerprint=case_fingerprint,
                active_review_seconds=active_review_seconds,
            )
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None
        except PermissionError:
            raise HTTPException(403, "Batch review denied") from None

    def botanical_batch_gateway(principal, operation_id, selections, reason,
                                source_fingerprint, case_fingerprint,
                                active_review_seconds=None):
        require_commands(principal)
        try:
            return approve_botanical_evidence_group(
                organization_id=principal.organization_id,
                operation_public_id=operation_id,
                authenticated_user_id=int(principal.actor_id),
                selected=selections, reason=reason, explicit_confirmation=True,
                expected_source_set_fingerprint=source_fingerprint,
                expected_case_fingerprint=case_fingerprint,
                active_review_seconds=active_review_seconds,
            )
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None
        except PermissionError:
            raise HTTPException(403, "Botanical reuse denied") from None

    def authorization_gateway(principal, operation_id, source_fingerprint,
                              case_fingerprint, reason, confirmed):
        require_commands(principal)
        try:
            return explicitly_authorize_assurance_export(
                organization_id=principal.organization_id,
                operation_public_id=operation_id,
                authenticated_user_id=int(principal.actor_id),
                expected_source_set_fingerprint=source_fingerprint,
                expected_case_fingerprint=case_fingerprint,
                reason=reason, explicit_confirmation=confirmed,
            )
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None
        except PermissionError:
            raise HTTPException(403, "Export authorization denied") from None

    def reuse_gateway(principal, operation_id, command):
        require_commands(principal)
        try:
            return apply_authorized_assurance_v2_reuse(
                organization_id=principal.organization_id,
                operation_public_id=operation_id,
                authenticated_user_id=int(principal.actor_id),
                memory_public_id=command["memory_id"],
                line_reference=command["line_reference"],
                field_name=command["field_name"],
                reason=command["reason"],
                expected_source_set_fingerprint=command["expected_source_set_fingerprint"],
                expected_case_fingerprint=command["expected_case_fingerprint"],
            )
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None
        except PermissionError:
            raise HTTPException(403, "Historical reuse denied") from None

    def identity_gateway(principal, operation_id, command):
        require_commands(principal)
        try:
            return confirm_assurance_v2_identity(
                organization_id=principal.organization_id,
                operation_public_id=operation_id,
                authenticated_user_id=int(principal.actor_id),
                line_reference=command["line_reference"],
                supplier_id=command["supplier_id"],
                product_id=command["product_id"],
                reason=command["reason"],
                expected_source_set_fingerprint=command["expected_source_set_fingerprint"],
                expected_case_fingerprint=command["expected_case_fingerprint"],
            )
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None
        except PermissionError:
            raise HTTPException(403, "Identity confirmation denied") from None

    def issue_gateway(principal, operation_id, source_fingerprint, case_fingerprint):
        if environment in {"production", "pilot"}:
            raise HTTPException(403, "Private V2 laboratory: regulatory package issuance is disabled; not for filing")
        require_commands(principal)
        try:
            return issue_locked_assurance_package(
                organization_id=principal.organization_id,
                operation_public_id=operation_id,
                authenticated_user_id=int(principal.actor_id),
                expected_source_set_fingerprint=source_fingerprint,
                expected_case_fingerprint=case_fingerprint,
            )
        except (StaleAssuranceIssue, ValueError) as exc:
            raise HTTPException(409, str(exc)) from None
        except PermissionError:
            raise HTTPException(403, "Package issuance denied") from None

    router = create_assurance_v2_router(
        principal_for=principal_for,
        case_for=lambda principal, operation_id: read_tenant_assurance_case(
            organization_id=principal.organization_id,
            operation_public_id=operation_id,
        ),
        decision_gateway=decision_gateway,
        csrf_verify=lambda principal, token: hmac.compare_digest(
            principal.csrf_token, str(token),
        ),
        package_store=PostgresOperationEventPackageStore(),
        authorization_gateway=authorization_gateway,
        issue_gateway=issue_gateway,
        reuse_gateway=reuse_gateway,
        identity_gateway=identity_gateway,
        batch_review_gateway=batch_review_gateway,
        botanical_batch_gateway=botanical_batch_gateway,
        review_insights_for=lambda principal, operation_id, case: (
            read_p1_review_insights(
                organization_id=principal.organization_id,
                operation_public_id=operation_id,
                current_case=case,
            )
            if (
                os.getenv("US_LACEY_ASSURANCE_V2_REVIEW_JOURNAL", "OFF").upper() == "ON"
                and str(principal.organization_id) in {
                    org.strip() for org in os.getenv(
                        "US_LACEY_ASSURANCE_V2_LAB_ORG_IDS", ""
                    ).split(",")
                }
            )
            else {}
        ),
        document_link_for=lambda principal, operation_id, doc, page: (
            f"/assurance-v2/operations/{operation_id}/documents/{doc}"
            + (f"#page={page}" if page is not None and 0 < page <= 10000 else "")
        ),
    )

    @app.get("/assurance-v2", include_in_schema=False, response_class=HTMLResponse)
    def assurance_v2_lab_home(request: Request):
        principal = principal_for(request)
        cookie = request.cookies.get(US_LACEY_SESSION_COOKIE) or ""
        identity = resolve_us_lacey_session(cookie)
        entitlement = require_us_lacey_operational_access(
            organization_id=principal.organization_id,
        )
        db = get_us_lacey_db_session()
        try:
            set_tenant_db_context(db, principal.organization_id)
            rows = db.scalars(select(UsLaceyOperation).where(
                UsLaceyOperation.organization_id == principal.organization_id,
                UsLaceyOperation.client_reference.startswith("V2LAB-"),
            ).order_by(UsLaceyOperation.id.desc()).limit(30)).all()
            operations = [{
                "public_id": str(op.public_id),
                "client_reference": str(op.client_reference),
                "status": str(op.status),
            } for op in rows]
        finally:
            db.close()
        return templates.TemplateResponse(
            request=request, name="us_lacey/assurance_v2_lab_home.html",
            context={
                "identity": identity, "entitlement": entitlement,
                "authenticated": True, "operations": operations,
            },
            headers={"Cache-Control": "private, no-store"},
        )

    @router.get("/{operation_id}/documents/{document_id}")
    def original_source(request: Request, operation_id: str, document_id: str):
        principal = principal_for(request)
        case = read_tenant_assurance_case(
            organization_id=principal.organization_id,
            operation_public_id=operation_id,
        )
        if case is None or case.operation_id != operation_id:
            raise HTTPException(404, "Document not found")
        manifest = next((doc for doc in case.documents
                         if str(doc.get("id")) == document_id), None)
        if manifest is None or manifest.get("raw_available") is not True:
            raise HTTPException(404, "Document not found")
        current = _resolve_current_source_document(
            organization_id=principal.organization_id,
            operation_public_id=operation_id,
            assurance_document_id=document_id,
        )
        if (current is None or current["sha256"] != manifest.get("sha256")
                or current["version"] != str(manifest.get("version"))):
            raise HTTPException(404, "Document not found")
        try:
            verified = VaultService().materialize_verified_download(
                organization_id=principal.organization_id,
                document_id=current["vault_public_id"],
            )
        except (VaultNotFoundError, VaultValidationError):
            raise HTTPException(404, "Document not found") from None
        except (VaultIntegrityError, VaultConflictError):
            raise HTTPException(409, "Source document integrity changed") from None
        except (VaultPersistenceError, VaultStorageOperationError):
            raise HTTPException(503, "Source document unavailable") from None
        if verified.document.sha256 != current["sha256"]:
            verified.close()
            raise HTTPException(409, "Source document integrity changed")
        try:
            _audit_source_access(
                principal=principal, operation_id=operation_id,
                assurance_document_id=document_id,
            )
        except Exception:
            verified.close()
            raise HTTPException(503, "Source access audit unavailable") from None

        def chunks():
            try:
                yield from verified.iter_chunks(chunk_size=64 * 1024)
            finally:
                verified.close()

        return StreamingResponse(chunks(), media_type=verified.document.content_type,
                                 headers={
                                     "Content-Disposition": 'inline; filename="source.pdf"',
                                     "Content-Length": str(verified.document.size_bytes),
                                     "Cache-Control": "private, no-store",
                                     "X-Content-Type-Options": "nosniff",
                                 })


    @router.get("/{operation_id}/historical-documents/{document_id}/{memory_id}")
    def historical_source(request: Request, operation_id: str,
                          document_id: str, memory_id: str):
        principal = principal_for(request)
        current = read_tenant_assurance_case(
            organization_id=principal.organization_id,
            operation_public_id=operation_id,
        )
        if current is None or current.operation_id != operation_id:
            raise HTTPException(404, "Historical source not found")
        allowed = any(
            str(item.get("id")) == memory_id
            and str(item.get("origin_document_id")) == document_id
            and item.get("status") in {"ELIGIBLE", "VERIFIED"}
            for item in current.reuses
        )
        if not allowed:
            raise HTTPException(404, "Historical source not eligible for this shipment")
        db = get_us_lacey_db_session()
        try:
            set_tenant_db_context(db, principal.organization_id)
            source = verified_historical_original(
                db, organization_id=principal.organization_id,
                memory_public_id=memory_id, origin_document_id=document_id,
            )
        finally:
            db.close()
        if source is None:
            raise HTTPException(404, "Original historical evidence no longer available")
        try:
            verified = VaultService().materialize_verified_download(
                organization_id=principal.organization_id,
                document_id=source["vault_public_id"],
            )
        except (VaultNotFoundError, VaultValidationError):
            raise HTTPException(404, "Original historical evidence unavailable") from None
        except (VaultIntegrityError, VaultConflictError):
            raise HTTPException(409, "Historical document integrity changed") from None
        except (VaultPersistenceError, VaultStorageOperationError):
            raise HTTPException(503, "Historical original cannot be inspected") from None
        if verified.document.sha256 != source["sha256"]:
            verified.close()
            raise HTTPException(409, "Historical document integrity changed")
        try:
            _audit_source_access(
                principal=principal, operation_id=operation_id,
                assurance_document_id=document_id,
            )
        except Exception:
            verified.close()
            raise HTTPException(503, "Historical source access audit unavailable") from None

        def chunks():
            try:
                yield from verified.iter_chunks(chunk_size=64 * 1024)
            finally:
                verified.close()
        return StreamingResponse(
            chunks(), media_type=verified.document.content_type,
            headers={
                "Content-Disposition": 'inline; filename="verified-historical-source.pdf"',
                "Content-Length": str(verified.document.size_bytes),
                "Cache-Control": "private, no-store",
                "X-Content-Type-Options": "nosniff",
            },
        )

    app.include_router(router)
    return True
