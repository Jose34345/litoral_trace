"""Public and authenticated HTML routes for Litoral Trace."""
from __future__ import annotations

from fastapi import (
    APIRouter,
    File,
    Form,
    HTTPException,
    Request,
    Response,
    UploadFile,
    status,
)
from fastapi.responses import (
    HTMLResponse,
    RedirectResponse,
)
from uuid import UUID

from litoral_trace.api.auth import (
    LoginRequest,
    login_b2b,
    logout_b2b_session,
)
from litoral_trace.auth.rbac import (
    Permission,
    has_permission,
)
from litoral_trace.auth.sessions import (
    ACCESS_TOKEN_COOKIE_KEY,
    REFRESH_TOKEN_COOKIE_KEY,
)
from litoral_trace.config import get_settings
from litoral_trace.services.admin import (
    listar_empresas_superadmin,
)
from litoral_trace.services.audit import (
    build_audit_actor_from_user,
    build_request_audit_context,
)
from litoral_trace.services.batch import (
    BatchSemanticValidationError,
    validar_filas_lotes,
)
from litoral_trace.services.batch_evidence import (
    BatchEvidenceError,
    BatchEvidenceConflictError,
    BatchEvidenceNotFoundError,
    BatchEvidencePersistenceError,
    BatchEvidenceService,
    BatchEvidenceValidationError,
)
from litoral_trace.services.batch_imports import (
    BatchImportConflictError,
    BatchImportIdempotencyConflictError,
    BatchImportPersistenceError,
    BatchImportService,
)
from litoral_trace.services.batch_queries import (
    BatchImportQueryError,
    BatchImportQueryService,
)
from litoral_trace.services.vault import (
    VaultError,
    VaultService,
)
from litoral_trace.web.batch_import import (
    BatchImportAlertView,
    BatchImportDetailPageView,
    BatchImportHtmlError,
    build_workspace_view,
    issue_browser_import_idempotency_key,
    normalize_browser_import_idempotency_key,
    parse_browser_upload,
    present_evidence_mutation_error,
    present_import_detail_result,
    present_import_detail_page,
    present_import_error,
    present_import_success,
    present_validation,
    workspace_context,
)
from litoral_trace.web.csrf import (
    enforce_csrf,
    get_csrf_browser_nonce,
)
from litoral_trace.web.runtime import (
    clear_browser_security_cookies,
    copy_response_cookies,
    get_authenticated_html_user,
    get_html_route_user,
    redirect_to_login,
    render_access_denied,
    render_csrf_failure,
    render_web_template,
    rotate_csrf_browser_cookie,
)
from litoral_trace.web.regional_intelligence import (
    get_regional_profile,
    list_regional_profiles,
)
from litoral_trace.web.regional_map import (
    REGIONAL_MAP_DATASET,
    get_regional_map_scope,
    list_regional_map_scopes,
)


router = APIRouter(
    tags=["Frontend B2B"],
)


def _new_batch_import_service() -> BatchImportService:
    return BatchImportService()


def _new_batch_import_query_service() -> BatchImportQueryService:
    return BatchImportQueryService()


def _new_batch_evidence_service() -> BatchEvidenceService:
    return BatchEvidenceService()


def _new_vault_service() -> VaultService:
    return VaultService()


def _render_batch_import_workspace(
    request: Request,
    *,
    user,
    view,
    status_code: int = 200,
    ) -> HTMLResponse:
    return render_web_template(
        request,
        "batch_import.html",
        user=user,
        context=workspace_context(view),
        status_code=status_code,
    )


def _render_batch_import_detail(
    request: Request,
    *,
    user,
    view: BatchImportDetailPageView,
    status_code: int = 200,
) -> HTMLResponse:
    return render_web_template(
        request,
        "batch_import_detail.html",
        user=user,
        context={
            "batch_import_detail_view": view,
        },
        status_code=status_code,
    )


def _render_login_error(
    request: Request,
    *,
    message: str,
    status_code: int,
) -> HTMLResponse:
    return render_web_template(
        request,
        "login.html",
        user=None,
        context={
            "error": message,
        },
        status_code=status_code,
    )


def _detail_redirect(
    public_id: UUID,
    *,
    result_code: str | None = None,
) -> RedirectResponse:
    url = f"/imports/{public_id}"
    if result_code is not None:
        url = f"{url}?evidence_result={result_code}"
    return RedirectResponse(
        url=url,
        status_code=status.HTTP_303_SEE_OTHER,
    )


def _evidence_operation_status(
    exc: BatchEvidenceError,
) -> int:
    if isinstance(exc, BatchEvidenceValidationError):
        return status.HTTP_422_UNPROCESSABLE_CONTENT
    if isinstance(exc, BatchEvidenceNotFoundError):
        return status.HTTP_404_NOT_FOUND
    if isinstance(exc, BatchEvidenceConflictError):
        return status.HTTP_409_CONFLICT
    if isinstance(exc, BatchEvidencePersistenceError):
        return status.HTTP_503_SERVICE_UNAVAILABLE
    return status.HTTP_400_BAD_REQUEST


def _safe_evidence_page_message(
    exc: BatchEvidenceError,
):
    code = getattr(
        exc,
        "code",
        "BATCH_EVIDENCE_UNAVAILABLE",
    )
    if isinstance(exc, BatchEvidencePersistenceError):
        code = "BATCH_EVIDENCE_UNAVAILABLE"
    return present_evidence_mutation_error(
        code=code,
    )


def _load_batch_import_detail_page(
    request: Request,
    *,
    user,
    public_id: UUID,
    page_message=None,
) -> tuple[BatchImportDetailPageView, int]:
    try:
        snapshot = _new_batch_import_query_service().get_by_public_id(
            organization_id=user.organization_id,
            public_id=public_id,
        )
    except BatchImportQueryError:
        return (
            present_import_detail_page(
                snapshot=None,
                can_view_evidence=False,
                evidence_error=BatchImportAlertView(
                    code="BATCH_QUERY_UNAVAILABLE",
                    title="Detalle no disponible",
                    message=(
                        "No fue posible consultar la importacion en este "
                        "momento."
                    ),
                ),
                page_message=page_message,
            ),
            status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    if snapshot is None:
        return (
            present_import_detail_page(
                snapshot=None,
                can_view_evidence=False,
                not_found=True,
                page_message=page_message,
            ),
            status.HTTP_404_NOT_FOUND,
        )

    can_view_evidence = has_permission(
        user,
        Permission.VAULT_READ,
    )
    can_manage_evidence = can_view_evidence and has_permission(
        user,
        Permission.LOTE_UPDATE,
    )

    evidence: tuple = ()
    evidence_error = None

    if can_view_evidence:
        try:
            evidence = _new_batch_evidence_service().list_evidence(
                organization_id=user.organization_id,
                batch_import_id=public_id,
            )
        except BatchEvidenceError:
            evidence_error = BatchImportAlertView(
                code="BATCH_EVIDENCE_UNAVAILABLE",
                title="Evidencia no disponible",
                message=(
                    "No fue posible consultar la evidencia vinculada "
                    "a esta importacion."
                ),
            )
            can_manage_evidence = False
            return (
                present_import_detail_page(
                    snapshot=snapshot,
                    can_view_evidence=True,
                    can_manage_evidence=False,
                    evidence_error=evidence_error,
                    page_message=page_message,
                ),
                status.HTTP_503_SERVICE_UNAVAILABLE,
            )

    available_documents = ()
    if can_manage_evidence:
        try:
            documents = _new_vault_service().list_documents(
                organization_id=user.organization_id,
            )
            available_documents = tuple(
                document
                for document in documents
                if document.status == "available"
            )
        except VaultError:
            evidence_error = BatchImportAlertView(
                code="VAULT_DOCUMENTS_UNAVAILABLE",
                title="Documentos Vault no disponibles",
                message=(
                    "No fue posible consultar documentos Vault "
                    "disponibles para vincular."
                ),
            )
            can_manage_evidence = False
            return (
                present_import_detail_page(
                    snapshot=snapshot,
                    can_view_evidence=can_view_evidence,
                    can_manage_evidence=False,
                    evidence=evidence,
                    evidence_error=evidence_error,
                    page_message=page_message,
                ),
                status.HTTP_503_SERVICE_UNAVAILABLE,
            )

    return (
        present_import_detail_page(
            snapshot=snapshot,
            can_view_evidence=can_view_evidence,
            can_manage_evidence=can_manage_evidence,
            evidence=evidence,
            available_documents=available_documents,
            evidence_error=evidence_error,
            page_message=page_message,
        ),
        status.HTTP_200_OK,
    )


@router.get(
    "/",
    response_class=HTMLResponse,
)
async def render_home_view(
    request: Request,
):
    """Render the public Litoral Trace corporate homepage."""

    return render_web_template(
        request,
        "public/home.html",
        user=None,
    )


@router.get(
    "/regional-intelligence",
    response_class=HTMLResponse,
)
async def render_regional_intelligence_index_view(
    request: Request,
):
    """Render the public Regional Intelligence catalog."""

    map_scopes = (
        list_regional_map_scopes()
    )

    return render_web_template(
        request,
        "public/regional_index.html",
        user=None,
        context={
            "regional_profiles": (
                list_regional_profiles()
            ),
            "regional_map_dataset": (
                REGIONAL_MAP_DATASET
            ),
            "regional_map_scopes": (
                map_scopes
            ),
            "regional_map_scope_by_region_id": {
                scope.region_id: scope
                for scope in map_scopes
            },
        },
    )


@router.get(
    "/regional-intelligence/{region_slug}",
    response_class=HTMLResponse,
)
async def render_regional_intelligence_detail_view(
    request: Request,
    region_slug: str,
):
    """Render one reusable public regional profile."""

    profile = get_regional_profile(
        region_slug
    )

    if profile is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Regional profile not found.",
        )

    map_scope = get_regional_map_scope(
        profile.region_id
    )

    if map_scope is None:
        raise RuntimeError(
            "Regional map configuration missing "
            f"for canonical region "
            f"{profile.region_id!r}."
        )

    return render_web_template(
        request,
        "public/regional_detail.html",
        user=None,
        context={
            "profile": profile,
            "regional_map_dataset": (
                REGIONAL_MAP_DATASET
            ),
            "regional_map_scope": (
                map_scope
            ),
        },
    )


@router.get(
    "/login",
    response_class=HTMLResponse,
)
async def render_login_view(
    request: Request,
):
    """Render the browser login page with browser-bound CSRF."""

    return render_web_template(
        request,
        "login.html",
        user=None,
    )


@router.post(
    "/login",
    response_class=HTMLResponse,
)
async def submit_login_view(
    request: Request,
):
    """Authenticate HTML credentials through the existing auth service."""

    browser_nonce = get_csrf_browser_nonce(
        request
    )

    try:
        await enforce_csrf(
            request,
            browser_nonce=browser_nonce,
            require_browser_binding=True,
        )
    except HTTPException:
        return _render_login_error(
            request,
            message=(
                "El formulario de acceso expiro. "
                "Volvé a ingresar tus credenciales."
            ),
            status_code=status.HTTP_403_FORBIDDEN,
        )

    form = await request.form()

    username = str(
        form.get("username", "")
    ).strip()

    password = str(
        form.get("password", "")
    )

    temp_response = Response()

    try:
        await login_b2b(
            LoginRequest(
                username=username,
                password=password,
            ),
            temp_response,
            request,
        )
    except HTTPException as exc:
        error_message = (
            exc.detail
            if (
                exc.status_code
                == status.HTTP_400_BAD_REQUEST
            )
            else "Usuario o contrasena incorrectos."
        )

        return _render_login_error(
            request,
            message=error_message,
            status_code=(
                exc.status_code
                if (
                    exc.status_code
                    == status.HTTP_400_BAD_REQUEST
                )
                else status.HTTP_401_UNAUTHORIZED
            ),
        )

    response = RedirectResponse(
        url="/dashboard",
        status_code=status.HTTP_303_SEE_OTHER,
    )

    copy_response_cookies(
        source=temp_response,
        target=response,
    )

    rotate_csrf_browser_cookie(response)

    return response


@router.get(
    "/dashboard",
    response_class=HTMLResponse,
)
async def render_dashboard_view(
    request: Request,
):
    user, denied_response = get_html_route_user(
        request,
        required_permission=Permission.LOTE_READ,
    )

    if denied_response is not None:
        return denied_response

    return render_web_template(
        request,
        "dashboard.html",
        user=user,
    )


@router.get(
    "/imports",
    response_class=HTMLResponse,
)
async def render_batch_import_view(
    request: Request,
):
    user, denied_response = get_html_route_user(
        request,
        required_permission=Permission.LOTE_CREATE,
    )

    if denied_response is not None:
        return denied_response

    return _render_batch_import_workspace(
        request,
        user=user,
        view=build_workspace_view(
            idempotency_key=issue_browser_import_idempotency_key()
        ),
    )


@router.get(
    "/imports/{public_id}",
    response_class=HTMLResponse,
)
async def render_batch_import_detail_view(
    request: Request,
    public_id: UUID,
):
    user, denied_response = get_html_route_user(
        request,
        required_permission=Permission.LOTE_READ,
    )

    if denied_response is not None:
        return denied_response

    view, status_code = _load_batch_import_detail_page(
        request,
        user=user,
        public_id=public_id,
        page_message=present_import_detail_result(
            request.query_params.get(
                "evidence_result"
            )
        ),
    )
    return _render_batch_import_detail(
        request,
        user=user,
        view=view,
        status_code=status_code,
    )


@router.post(
    "/imports/{public_id}/evidence",
)
async def link_batch_import_evidence_view(
    request: Request,
    public_id: UUID,
    document_id: str = Form(...),
    evidence_type: str = Form(...),
):
    user, denied_response = get_html_route_user(
        request,
        required_permission=Permission.LOTE_UPDATE,
    )

    if denied_response is not None:
        return denied_response

    if not has_permission(
        user,
        Permission.VAULT_READ,
    ):
        return render_access_denied()

    browser_nonce = get_csrf_browser_nonce(
        request
    )

    try:
        await enforce_csrf(
            request,
            user=user,
            browser_nonce=browser_nonce,
            require_browser_binding=True,
        )
    except HTTPException:
        return render_csrf_failure()

    try:
        result = _new_batch_evidence_service().link_evidence(
            organization_id=user.organization_id,
            batch_import_id=public_id,
            vault_document_id=document_id,
            evidence_type=evidence_type,
            actor=build_audit_actor_from_user(
                user
            ),
            request_context=build_request_audit_context(
                request
            ),
        )
    except BatchEvidenceError as exc:
        view, status_code = _load_batch_import_detail_page(
            request,
            user=user,
            public_id=public_id,
            page_message=_safe_evidence_page_message(
                exc
            ),
        )
        return _render_batch_import_detail(
            request,
            user=user,
            view=view,
            status_code=max(
                status_code,
                _evidence_operation_status(
                    exc
                ),
            ),
        )

    return _detail_redirect(
        public_id,
        result_code=(
            "replayed"
            if result.replayed
            else "linked"
        ),
    )


@router.post(
    "/imports/{public_id}/evidence/{document_id}/unlink",
)
async def unlink_batch_import_evidence_view(
    request: Request,
    public_id: UUID,
    document_id: UUID,
):
    user, denied_response = get_html_route_user(
        request,
        required_permission=Permission.LOTE_UPDATE,
    )

    if denied_response is not None:
        return denied_response

    if not has_permission(
        user,
        Permission.VAULT_READ,
    ):
        return render_access_denied()

    browser_nonce = get_csrf_browser_nonce(
        request
    )

    try:
        await enforce_csrf(
            request,
            user=user,
            browser_nonce=browser_nonce,
            require_browser_binding=True,
        )
    except HTTPException:
        return render_csrf_failure()

    try:
        _new_batch_evidence_service().unlink_evidence(
            organization_id=user.organization_id,
            batch_import_id=public_id,
            vault_document_id=document_id,
            actor=build_audit_actor_from_user(
                user
            ),
            request_context=build_request_audit_context(
                request
            ),
        )
    except BatchEvidenceError as exc:
        view, status_code = _load_batch_import_detail_page(
            request,
            user=user,
            public_id=public_id,
            page_message=_safe_evidence_page_message(
                exc
            ),
        )
        return _render_batch_import_detail(
            request,
            user=user,
            view=view,
            status_code=max(
                status_code,
                _evidence_operation_status(
                    exc
                ),
            ),
        )

    return _detail_redirect(
        public_id,
        result_code="unlinked",
    )


@router.post(
    "/imports/validate",
    response_class=HTMLResponse,
)
async def validate_batch_import_view(
    request: Request,
    file: UploadFile = File(...),
    idempotency_key: str = Form(...),
):
    user, denied_response = get_html_route_user(
        request,
        required_permission=Permission.LOTE_CREATE,
    )

    if denied_response is not None:
        return denied_response

    browser_nonce = get_csrf_browser_nonce(
        request
    )

    try:
        await enforce_csrf(
            request,
            user=user,
            browser_nonce=browser_nonce,
            require_browser_binding=True,
        )
    except HTTPException:
        return render_csrf_failure()

    current_key = issue_browser_import_idempotency_key()

    try:
        current_key = normalize_browser_import_idempotency_key(
            idempotency_key
        )
        workbook = await parse_browser_upload(
            file,
            request=request,
        )
        validation = validar_filas_lotes(
            workbook
        )
    except BatchImportHtmlError as exc:
        return _render_batch_import_workspace(
            request,
            user=user,
            view=build_workspace_view(
                idempotency_key=current_key,
                alert=BatchImportAlertView(
                    code=exc.code,
                    title=exc.title,
                    message=exc.message,
                ),
                requires_reupload=True,
            ),
            status_code=exc.status_code,
        )

    return _render_batch_import_workspace(
        request,
        user=user,
        view=build_workspace_view(
            idempotency_key=current_key,
            validation=present_validation(
                workbook,
                validation,
            ),
            requires_reupload=True,
        ),
    )


@router.post(
    "/imports",
    response_class=HTMLResponse,
)
async def submit_batch_import_view(
    request: Request,
    file: UploadFile = File(...),
    idempotency_key: str = Form(...),
):
    user, denied_response = get_html_route_user(
        request,
        required_permission=Permission.LOTE_CREATE,
    )

    if denied_response is not None:
        return denied_response

    browser_nonce = get_csrf_browser_nonce(
        request
    )

    try:
        await enforce_csrf(
            request,
            user=user,
            browser_nonce=browser_nonce,
            require_browser_binding=True,
        )
    except HTTPException:
        return render_csrf_failure()

    current_key = issue_browser_import_idempotency_key()
    workbook = None

    try:
        current_key = normalize_browser_import_idempotency_key(
            idempotency_key
        )
        workbook = await parse_browser_upload(
            file,
            request=request,
        )
        validation = validar_filas_lotes(
            workbook
        )

        if not validation.valid:
            return _render_batch_import_workspace(
                request,
                user=user,
                view=build_workspace_view(
                    idempotency_key=current_key,
                    validation=present_validation(
                        workbook,
                        validation,
                    ),
                    result=present_import_error(
                        code="ROW_VALIDATION_FAILED",
                        title="Importacion cancelada",
                        message=(
                            "La planilla contiene filas con errores "
                            "de validacion. No se persistio ningun lote."
                        ),
                        source_filename=workbook.filename,
                    ),
                    requires_reupload=True,
                ),
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            )

        result = _new_batch_import_service().import_validated(
            validation,
            organization_id=user.organization_id,
            actor=build_audit_actor_from_user(
                user
            ),
            request_context=build_request_audit_context(
                request
            ),
            source_filename=workbook.filename,
            source_sha256=workbook.sha256,
            idempotency_key=current_key,
        )
    except BatchImportHtmlError as exc:
        return _render_batch_import_workspace(
            request,
            user=user,
            view=build_workspace_view(
                idempotency_key=current_key,
                alert=BatchImportAlertView(
                    code=exc.code,
                    title=exc.title,
                    message=exc.message,
                ),
                requires_reupload=True,
            ),
            status_code=exc.status_code,
        )
    except BatchSemanticValidationError as exc:
        if workbook is None:
            raise

        return _render_batch_import_workspace(
            request,
            user=user,
            view=build_workspace_view(
                idempotency_key=current_key,
                validation=present_validation(
                    workbook,
                    exc.result,
                ),
                result=present_import_error(
                    code="ROW_VALIDATION_FAILED",
                    title="Importacion cancelada",
                    message=(
                        "La planilla contiene filas con errores de "
                        "validacion. No se persistio ningun lote."
                    ),
                    source_filename=workbook.filename,
                ),
                requires_reupload=True,
            ),
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        )
    except BatchImportConflictError as exc:
        return _render_batch_import_workspace(
            request,
            user=user,
            view=build_workspace_view(
                idempotency_key=current_key,
                result=present_import_error(
                    code="DUPLICATE_LOTE_IDENTIFIERS",
                    title="Importacion rechazada",
                    message=(
                        "Ya existen identificadores de lote para esta "
                        "organizacion. Corregi la planilla antes de "
                        "reintentar."
                    ),
                    duplicate_identifiers=exc.identifiers,
                    source_filename=(
                        workbook.filename
                        if workbook is not None
                        else None
                    ),
                ),
                requires_reupload=True,
            ),
            status_code=status.HTTP_409_CONFLICT,
        )
    except BatchImportIdempotencyConflictError as exc:
        return _render_batch_import_workspace(
            request,
            user=user,
            view=build_workspace_view(
                idempotency_key=issue_browser_import_idempotency_key(),
                result=present_import_error(
                    code="IDEMPOTENCY_CONFLICT",
                    title="Clave de importacion en conflicto",
                    message=(
                        "La misma clave de idempotencia ya fue usada "
                        "con una planilla diferente."
                    ),
                    import_id=(
                        str(exc.import_public_id)
                        if exc.import_public_id is not None
                        else None
                    ),
                    source_filename=(
                        workbook.filename
                        if workbook is not None
                        else None
                    ),
                ),
                requires_reupload=True,
            ),
            status_code=status.HTTP_409_CONFLICT,
        )
    except BatchImportPersistenceError:
        return _render_batch_import_workspace(
            request,
            user=user,
            view=build_workspace_view(
                idempotency_key=current_key,
                result=present_import_error(
                    code="SERVICE_UNAVAILABLE",
                    title="Importacion no disponible",
                    message=(
                        "No fue posible completar la importacion en este "
                        "momento. Reintenta con la misma planilla cuando "
                        "el servicio vuelva a estar disponible."
                    ),
                    source_filename=(
                        workbook.filename
                        if workbook is not None
                        else None
                    ),
                ),
                requires_reupload=True,
            ),
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    result_view = present_import_success(
        workbook,
        result,
    )

    return _render_batch_import_workspace(
        request,
        user=user,
        view=build_workspace_view(
            idempotency_key=issue_browser_import_idempotency_key(),
            result=result_view,
        ),
        status_code=(
            status.HTTP_200_OK
            if result.replayed
            else status.HTTP_201_CREATED
        ),
    )


@router.get(
    "/vault",
    response_class=HTMLResponse,
)
async def render_vault_view(
    request: Request,
):
    user, denied_response = get_html_route_user(
        request,
        required_permission=Permission.VAULT_READ,
    )

    if denied_response is not None:
        return denied_response

    storage_settings = get_settings().storage

    return render_web_template(
        request,
        "vault.html",
        user=user,
        context={
            "vault_can_upload": has_permission(
                user,
                Permission.VAULT_UPLOAD,
            ),
            "vault_can_delete": has_permission(
                user,
                Permission.VAULT_DELETE,
            ),
            "vault_max_upload_bytes": (
                storage_settings.max_upload_bytes
            ),
            "vault_max_upload_mb": round(
                storage_settings.max_upload_bytes
                / (1024 * 1024),
                1,
            ),
        },
    )


@router.get(
    "/settings",
    response_class=HTMLResponse,
)
async def render_settings_view(
    request: Request,
):
    user, denied_response = get_html_route_user(
        request,
        required_permission=Permission.SETTINGS_WRITE,
    )

    if denied_response is not None:
        return denied_response

    return render_web_template(
        request,
        "settings.html",
        user=user,
    )


@router.get(
    "/admin",
    response_class=HTMLResponse,
)
async def render_admin_view(
    request: Request,
):
    user, denied_response = get_html_route_user(
        request,
        required_permission=Permission.PLATFORM_ADMIN,
    )

    if denied_response is not None:
        return denied_response

    try:
        organizations = listar_empresas_superadmin(
            refresh_token=request.cookies.get(
                REFRESH_TOKEN_COOKIE_KEY
            ),
        )
    except HTTPException as exc:
        if (
            exc.status_code
            == status.HTTP_401_UNAUTHORIZED
        ):
            return redirect_to_login(
                clear_cookies=True
            )

        if (
            exc.status_code
            == status.HTTP_403_FORBIDDEN
        ):
            return render_access_denied()

        raise

    return render_web_template(
        request,
        "admin_organizations.html",
        user=user,
        context={
            "organizations": organizations,
            "organization_count": len(
                organizations
            ),
        },
    )


@router.get(
    "/logout",
    response_class=HTMLResponse,
)
async def logout_view(
    request: Request,
):
    user, denied_response = (
        get_authenticated_html_user(
            request
        )
    )

    if denied_response is not None:
        return denied_response

    return render_web_template(
        request,
        "logout.html",
        user=user,
    )


@router.post(
    "/logout",
)
async def logout_submit_view(
    request: Request,
):
    user, denied_response = (
        get_authenticated_html_user(
            request
        )
    )

    if denied_response is not None:
        return denied_response

    browser_nonce = get_csrf_browser_nonce(
        request
    )

    try:
        await enforce_csrf(
            request,
            user=user,
            browser_nonce=browser_nonce,
            require_browser_binding=True,
        )
    except HTTPException:
        return render_csrf_failure()

    temp_response = Response()

    await logout_b2b_session(
        temp_response,
        request=request,
        refresh_token_cookie=request.cookies.get(
            REFRESH_TOKEN_COOKIE_KEY
        ),
        session_jwt=request.cookies.get(
            ACCESS_TOKEN_COOKIE_KEY
        ),
    )

    response = RedirectResponse(
        url="/login",
        status_code=status.HTTP_303_SEE_OTHER,
    )

    copy_response_cookies(
        source=temp_response,
        target=response,
    )

    clear_browser_security_cookies(response)

    return response
