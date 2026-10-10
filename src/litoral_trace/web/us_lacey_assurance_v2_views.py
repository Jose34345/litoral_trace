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
from litoral_trace.us_lacey.exporters.assurance_v2_package import (
    ExportBlocked, PackageStore, draft_assurance_excel, issue_assurance_package,
)
from litoral_trace.web.templates import templates


_ACTIONS = frozenset({"ACCEPT", "CORRECT", "REJECT", "REQUEST_EVIDENCE", "PENDING"})
_FP = re.compile(r"^[a-f0-9]{64}$")


@dataclass(frozen=True)
class WorkspacePrincipal:
    organization_id: int
    actor_id: str
    csrf_token: str


class CaseReader(Protocol):
    def __call__(self, principal: WorkspacePrincipal, operation_id: str) -> AssuranceCase | None: ...


class DecisionGateway(Protocol):
    def __call__(self, principal: WorkspacePrincipal, operation_id: str, command: dict[str, str]) -> dict[str, Any]: ...


def create_assurance_v2_router(
    *,
    principal_for: Callable[[Request], WorkspacePrincipal],
    case_for: CaseReader,
    decision_gateway: DecisionGateway,
    csrf_verify: Callable[[WorkspacePrincipal, str], bool],
    package_store: PackageStore,
    document_link_for: Callable[[WorkspacePrincipal, str, str, int | None], str | None],
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
        return templates.TemplateResponse(
            request=request, name="us_lacey/assurance_v2_case.html",
            context={"case": snapshot, "gate": gate, "csrf_token": principal.csrf_token,
                     "actions": sorted(_ACTIONS)},
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

    @router.post("/{operation_id}/issue")
    async def issue(request: Request, operation_id: str):
        principal, case = context(request, operation_id)
        body = await request.json()
        if not isinstance(body, dict) or not csrf_verify(principal, str(body.get("csrf_token") or "")):
            raise HTTPException(403, "Invalid CSRF token")
        # Even if the UI previously showed ready, re-read current source set
        # and rerun authority checks at the issuance boundary.
        try:
            package = issue_assurance_package(case)
        except ExportBlocked as exc:
            return JSONResponse({"status": "BLOCKED", "blockers": [
                {"code": b.code, "subject": b.subject} for b in exc.gate.blockers
            ]}, status_code=409)
        package_store.save(organization_id=principal.organization_id,
                           operation_id=operation_id, package=package)
        return {"status": "FILING_READY", "package_fingerprint": package.fingerprint,
                "submitted_to_agency": False}

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
        principal, _ = context(request, operation_id)
        if not _FP.fullmatch(fingerprint) or kind not in {"lawgs_xml", "lacey_excel", "ppq505_preparation_json", "manifest"}:
            raise HTTPException(404, "Package not found")
        package = package_store.load(organization_id=principal.organization_id,
                                     operation_id=operation_id, fingerprint=fingerprint)
        if package is None:
            raise HTTPException(404, "Package not found")
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
