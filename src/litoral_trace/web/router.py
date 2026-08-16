"""Public and authenticated HTML routes for Litoral Trace."""
from __future__ import annotations

from fastapi import (
    APIRouter,
    HTTPException,
    Request,
    Response,
    status,
)
from fastapi.responses import (
    HTMLResponse,
    RedirectResponse,
)

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