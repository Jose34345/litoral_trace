"""Standalone Assurance V2 UI router; deliberately NOT mounted in main.py.

Integration must provide an authenticated tenant principal, a source-authority
case reader, a verified CSRF hook and the Agent 2 decision command gateway.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol, Any
import re

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from litoral_trace.us_lacey.exporters.assurance_v2 import AssuranceCase, evaluate_filing_gate
from litoral_trace.us_lacey.assurance_v2_evidence_checklist import required_evidence_summary
from litoral_trace.us_lacey.assurance_v2_review_policy import simplified_review
from litoral_trace.us_lacey.exporters.assurance_v2_package import (
    ExportBlocked, PackageStore, draft_assurance_excel,
)
from litoral_trace.web.templates import templates


_ACTIONS = frozenset({"ACCEPT", "CORRECT"})
_FP = re.compile(r"^[a-f0-9]{64}$")


@dataclass(frozen=True)
class WorkspacePrincipal:
    organization_id: int
    actor_id: str
    csrf_token: str
    actor_email: str = ""


class CaseReader(Protocol):
    def __call__(self, principal: WorkspacePrincipal, operation_id: str) -> AssuranceCase | None: ...


class DecisionGateway(Protocol):
    def __call__(self, principal: WorkspacePrincipal, operation_id: str, command: dict[str, str]) -> dict[str, Any]: ...


class ReuseGateway(Protocol):
    def __call__(self, principal: WorkspacePrincipal, operation_id: str,
                 command: dict[str, str]) -> dict[str, Any]: ...


class IdentityGateway(Protocol):
    def __call__(self, principal: WorkspacePrincipal, operation_id: str,
                 command: dict[str, str]) -> dict[str, Any]: ...


class BatchGateway(Protocol):
    def __call__(self, principal: WorkspacePrincipal, operation_id: str,
                 command: dict[str, Any]) -> dict[str, Any]: ...


class AuthorizationGateway(Protocol):
    """Only a tenant-authenticated, current-source-set-locked writer."""
    def __call__(self, principal: WorkspacePrincipal, operation_id: str,
                 expected_source_set_fingerprint: str,
                 expected_case_fingerprint: str, reason: str,
                 explicit_confirmation: bool) -> dict[str, Any]: ...


class IssueGateway(Protocol):
    """Must acquire source-set/operation locks and persist within one transaction.

    The UI may pass expected state, but must never mint a filing-ready package
    from an earlier request's detached read-model snapshot.
    """
    def __call__(self, principal: WorkspacePrincipal, operation_id: str,
                 expected_source_set_fingerprint: str,
                 expected_case_fingerprint: str) -> dict[str, Any]: ...


def create_assurance_v2_router(
    *,
    principal_for: Callable[[Request], WorkspacePrincipal],
    case_for: CaseReader,
    decision_gateway: DecisionGateway,
    csrf_verify: Callable[[WorkspacePrincipal, str], bool],
    package_store: PackageStore,
    document_link_for: Callable[[WorkspacePrincipal, str, str, int | None], str | None],
    issue_gateway: IssueGateway | None = None,
    authorization_gateway: AuthorizationGateway | None = None,
    reuse_gateway: ReuseGateway | None = None,
    identity_gateway: IdentityGateway | None = None,
    batch_gateway: BatchGateway | None = None,
) -> APIRouter:
    """The web adapter cannot select/promote candidates or write canonical truth."""
    router = APIRouter(prefix="/assurance-v2/operations", tags=["Assurance V2"])

    def context(request: Request, operation_id: str):
        principal = principal_for(request)  # Must reject unauthenticated callers.
        if not principal.actor_id or principal.organization_id <= 0:
            raise HTTPException(401, "Authentication required")
        case = case_for(principal, operation_id)
        if case is None or case.organization_id != principal.organization_id or case.operation_id != operation_id:
            raise HTTPException(404, "Case not found")
        return principal, case

    def source_link(principal: WorkspacePrincipal, operation_id: str, doc: str, page: int | None) -> str | None:
        url = document_link_for(principal, operation_id, doc, page)
        if not url or not url.startswith("/") or url.startswith("//") or "\\" in url or any(x in url for x in ("\r", "\n")):
            return None
        return url

    @router.get("/{operation_id}", response_class=HTMLResponse)
    def case_view(request: Request, operation_id: str):
        principal, case = context(request, operation_id)
        gate = evaluate_filing_gate(case)
        snapshot = case.snapshot()
        documents = {str(doc.get("id")): doc for doc in snapshot["documents"]}
        # Source URL is a server-authenticated document resolver; the browser
        # cannot choose tenant or cross-scope document ownership.
        for doc in snapshot["documents"]:
            doc["url"] = source_link(principal, operation_id, str(doc.get("id")), None)
        for item in snapshot["exceptions"]:
            for candidate in item.get("candidates", []):
                candidate["url"] = source_link(
                    principal, operation_id,
                    str(candidate.get("document_id") or ""),
                    int(candidate["page"]) if str(candidate.get("page") or "").isdigit() else None,
                ) if str(candidate.get("document_id")) in documents else None
        for claim in snapshot["candidate_observations"]:
            doc_id = str(claim.get("assurance_document_id") or "")
            locator = claim.get("source_locator") or {}
            page = locator.get("page") if isinstance(locator, dict) else None
            claim["url"] = source_link(
                principal, operation_id, doc_id,
                int(page) if str(page or "").isdigit() else None,
            ) if doc_id in documents else None
        return templates.TemplateResponse(
            request=request, name="us_lacey/assurance_v2_case.html",
            context={"case": snapshot, "gate": gate,
                     "simple": simplified_review(case),
                     "required_evidence": required_evidence_summary(case),
                     "csrf_token": principal.csrf_token,
                     "actions": sorted(_ACTIONS), "authenticated": True},
        )

    @router.post("/{operation_id}/decisions")
    async def submit_decision(request: Request, operation_id: str):
        principal, case = context(request, operation_id)
        body = await request.json()
        if not isinstance(body, dict):
            raise HTTPException(422, "Decision body must be an object")
        if not csrf_verify(principal, str(body.get("csrf_token") or "")):
            raise HTTPException(403, "Invalid CSRF token")
        action = str(body.get("action") or "").upper()
        exception_id = str(body.get("exception_id") or "")
        if action not in _ACTIONS or not any(str(ex.get("id")) == exception_id for ex in case.exceptions):
            raise HTTPException(422, "Invalid exception/action")
        if action == "CORRECT" and not str(body.get("value") or "").strip():
            raise HTTPException(422, "Correction must contain a value")
        if action in {"REJECT", "CORRECT", "REQUEST_EVIDENCE"} and not str(body.get("reason") or "").strip():
            raise HTTPException(422, "Review reason is required")
        # No frontend decision state. Agent 2 owns permission checks,
        # evidence binding, optimistic concurrency, audit and promotion.
        command = {
            "operation_id": operation_id,
            "exception_id": exception_id,
            "action": action,
            "candidate_id": str(body.get("candidate_id") or ""),
            "value": str(body.get("value") or ""),
            "reason": str(body.get("reason") or ""),
            "expected_source_set_fingerprint": case.source_set_fingerprint,
            "expected_case_fingerprint": case.authority_fingerprint(),
        }
        receipt = decision_gateway(principal, operation_id, command)
        return JSONResponse(receipt, status_code=202)

    @router.post("/{operation_id}/review-batch")
    async def submit_review_batch(request: Request, operation_id: str):
        principal, case = context(request, operation_id)
        body = await request.json()
        if not isinstance(body, dict) or not csrf_verify(
            principal, str(body.get("csrf_token") or "")
        ):
            raise HTTPException(403, "Invalid CSRF token")
        reason = str(body.get("reason") or "").strip()
        if body.get("explicit_confirmation") is not True or not reason or len(reason) > 1000:
            raise HTTPException(422, "Explicit review and a reason are required")
        preview = simplified_review(case)
        if (not preview["can_batch"]
                or not preview["batch_fingerprint"]
                or body.get("batch_fingerprint") != preview["batch_fingerprint"]):
            raise HTTPException(409, "Review the latest shipment summary before approving")
        if batch_gateway is None:
            raise HTTPException(503, "Authenticated batch review is not enabled")
        return JSONResponse(batch_gateway(principal, operation_id, {
            "reason": reason, "explicit_confirmation": True,
            "expected_source_set_fingerprint": case.source_set_fingerprint,
            "expected_case_fingerprint": case.authority_fingerprint(),
            "expected_batch_fingerprint": preview["batch_fingerprint"],
        }), status_code=202)

    @router.post("/{operation_id}/authorize-export")
    async def authorize_export(request: Request, operation_id: str):
        principal, case = context(request, operation_id)
        body = await request.json()
        if not isinstance(body, dict) or not csrf_verify(
            principal, str(body.get("csrf_token") or "")
        ):
            raise HTTPException(403, "Invalid CSRF token")
        reason = str(body.get("reason") or "").strip()
        if body.get("explicit_confirmation") is not True or not reason or len(reason) > 1000:
            raise HTTPException(422, "Explicit confirmation and reviewer reason are required")
        gate = evaluate_filing_gate(case)
        other_blockers = [
            {"code": b.code, "subject": b.subject}
            for b in gate.blockers if b.code != "EXPORT_NOT_AUTHORIZED"
        ]
        if other_blockers:
            return JSONResponse({"status": "BLOCKED", "blockers": other_blockers}, status_code=409)
        if authorization_gateway is None:
            raise HTTPException(503, "Authenticated export authorization is not enabled")
        return authorization_gateway(
            principal, operation_id, case.source_set_fingerprint,
            gate.case_fingerprint, reason, True,
        )

    @router.post("/{operation_id}/reuses")
    async def apply_reuse(request: Request, operation_id: str):
        principal, case = context(request, operation_id)
        body = await request.json()
        if not isinstance(body, dict) or not csrf_verify(
            principal, str(body.get("csrf_token") or "")
        ):
            raise HTTPException(403, "Invalid CSRF token")
        memory_id = str(body.get("memory_id") or "").strip()
        field_name = str(body.get("field_name") or "").strip()
        line_reference = str(body.get("line_reference") or "").strip()
        reason = str(body.get("reason") or "").strip()
        if not reason or len(reason) > 180:
            raise HTTPException(422, "Reviewer reason is required")
        eligible = [item for item in case.reuses if (
            item.get("status") == "ELIGIBLE"
            and str(item.get("id")) == memory_id
            and item.get("field_name") == field_name
            and item.get("line_reference") == line_reference
        )]
        if len(eligible) != 1:
            raise HTTPException(422, "Historical evidence is not eligible for this line")
        if reuse_gateway is None:
            raise HTTPException(503, "Authenticated reuse application is disabled")
        command = {
            "memory_id": memory_id,
            "field_name": field_name,
            "line_reference": line_reference,
            "reason": reason,
            "expected_source_set_fingerprint": case.source_set_fingerprint,
            "expected_case_fingerprint": case.authority_fingerprint(),
        }
        return JSONResponse(reuse_gateway(principal, operation_id, command), status_code=202)

    @router.post("/{operation_id}/identities")
    async def confirm_identity(request: Request, operation_id: str):
        principal, case = context(request, operation_id)
        body = await request.json()
        if not isinstance(body, dict) or not csrf_verify(
            principal, str(body.get("csrf_token") or "")
        ):
            raise HTTPException(403, "Invalid CSRF token")
        line_reference = str(body.get("line_reference") or "").strip()
        reason = str(body.get("reason") or "").strip()
        try:
            supplier_id = int(body.get("supplier_id"))
            product_id = int(body.get("product_id"))
        except (TypeError, ValueError):
            raise HTTPException(422, "Invalid supplier or product") from None
        if not line_reference or not reason or len(reason) > 180:
            raise HTTPException(422, "Exact line and reviewer reason are required")
        matches = [item for item in case.identities if (
            item.get("status") == "REVIEW_REQUIRED"
            and item.get("line_reference") == line_reference
            and item.get("supplier_id") == supplier_id
            and item.get("product_id") == product_id
        )]
        if len(matches) != 1:
            raise HTTPException(422, "Supplier product is not this line's current candidate")
        if identity_gateway is None:
            raise HTTPException(503, "Authenticated identity confirmation is disabled")
        command = {
            "line_reference": line_reference,
            "supplier_id": supplier_id,
            "product_id": product_id,
            "reason": reason,
            "expected_source_set_fingerprint": case.source_set_fingerprint,
            "expected_case_fingerprint": case.authority_fingerprint(),
        }
        return JSONResponse(identity_gateway(principal, operation_id, command), status_code=202)

    @router.post("/{operation_id}/issue")
    async def issue(request: Request, operation_id: str):
        principal, case = context(request, operation_id)
        body = await request.json()
        if not isinstance(body, dict) or not csrf_verify(principal, str(body.get("csrf_token") or "")):
            raise HTTPException(403, "Invalid CSRF token")
        # The request's read model is only a preflight preview. It cannot
        # authorize or persist final artifacts: a separate trusted gateway must
        # lock the operation/source set, re-read authority, and save atomically.
        gate = evaluate_filing_gate(case)
        if not gate.ready:
            return JSONResponse({"status": "BLOCKED", "blockers": [
                {"code": b.code, "subject": b.subject} for b in gate.blockers
            ]}, status_code=409)
        if issue_gateway is None:
            raise HTTPException(503, "Atomic Assurance V2 issuance is not enabled")
        try:
            return issue_gateway(
                principal, operation_id, case.source_set_fingerprint,
                gate.case_fingerprint,
            )
        except ExportBlocked as exc:
            return JSONResponse({"status": "BLOCKED", "blockers": [
                {"code": b.code, "subject": b.subject} for b in exc.gate.blockers
            ]}, status_code=409)

    @router.get("/{operation_id}/draft.xlsx")
    def draft(request: Request, operation_id: str):
        _, case = context(request, operation_id)
        if evaluate_filing_gate(case).ready:
            raise HTTPException(409, "Ready cases must use an authorized package")
        return Response(content=draft_assurance_excel(case),
                        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        headers={"Content-Disposition": 'attachment; filename="ASSURANCE_INCOMPLETE_DRAFT.xlsx"',
                                 "X-Assurance-Status": "DRAFT"})

    @router.get("/{operation_id}/packages/{fingerprint}/{kind}")
    def artifact(request: Request, operation_id: str, fingerprint: str, kind: str):
        principal, current_case = context(request, operation_id)
        if not _FP.fullmatch(fingerprint) or kind not in {"lawgs_xml", "lacey_excel", "ppq505_preparation_json", "manifest"}:
            raise HTTPException(404, "Package not found")
        package = package_store.load(organization_id=principal.organization_id,
                                     operation_id=operation_id, fingerprint=fingerprint)
        if package is None:
            raise HTTPException(404, "Package not found")
        # The immutable event remains available in the audit system, but an
        # obsolete package must never be served by a route explicitly marked
        # FILING_READY. This can happen after source replacement, document
        # withdrawal, reviewer supersession, or historical evidence revocation.
        gate = evaluate_filing_gate(current_case)
        if (not gate.ready
                or package.snapshot.get("case_fingerprint") != gate.case_fingerprint
                or package.snapshot.get("source_set_fingerprint") != current_case.source_set_fingerprint
                or package.snapshot.get("source_set_revision") != current_case.source_set_revision):
            raise HTTPException(409, "Issued package is no longer current or authorized")
        if kind == "manifest":
            import json
            safe = {key: value for key, value in package.snapshot.items() if key != "artifacts"}
            return JSONResponse(safe, headers={"X-Assurance-Status": "FILING_READY"})
        mime = ("application/xml" if kind == "lawgs_xml" else
                "application/json" if kind == "ppq505_preparation_json" else
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        ext = "xml" if kind == "lawgs_xml" else "json" if kind == "ppq505_preparation_json" else "xlsx"
        return Response(package.artifact(kind), media_type=mime, headers={
            "Content-Disposition": f'attachment; filename="assurance_{fingerprint[:12]}.{ext}"',
            "X-Assurance-Status": "FILING_READY",
            "X-Assurance-Fingerprint": fingerprint,
        })

    return router
