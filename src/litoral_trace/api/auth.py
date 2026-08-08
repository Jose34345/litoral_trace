"""Router de autenticacion REST B2B, emision JWT y contexto Tenant."""
from __future__ import annotations

from typing import Any

from fastapi import (
    APIRouter,
    Cookie,
    Depends,
    Header,
    HTTPException,
    Request,
    Response,
    status,
)
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from sqlalchemy import select

from litoral_trace.auth.sessions import (
    ACCESS_TOKEN_COOKIE_KEY,
    REFRESH_TOKEN_COOKIE_KEY,
    SessionSecurityError,
    create_user_session,
    revoke_session,
    rotate_refresh_session,
    sanitize_ip_address,
    sanitize_user_agent,
)
from litoral_trace.auth.passwords import verify_password
from litoral_trace.auth.tokens import create_jwt_token, verify_jwt_token
from litoral_trace.config import get_settings
from litoral_trace.db.engine import get_db_session
from litoral_trace.db.models import Organization, User, UserSession


router = APIRouter(
    prefix="/api/v1/auth",
    tags=["Autenticacion B2B"],
)

oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="/api/v1/auth/login",
    auto_error=False,
)


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in_seconds: int
    user_info: dict[str, Any]


class UserTenantContext(BaseModel):
    user_id: int | None = None
    username: str
    organization_id: int
    organization_name: str
    organization_slug: str | None = None
    role: str
    email: str
    session_id: int | None = None
    is_platform_superadmin: bool = False


class RefreshRequest(BaseModel):
    refresh_token: str | None = None


class LogoutRequest(BaseModel):
    refresh_token: str | None = None


class LogoutResponse(BaseModel):
    detail: str


def _build_user_tenant_context(payload: dict[str, Any]) -> UserTenantContext:
    subject = str(payload.get("sub", "")).strip()
    role = str(payload.get("role", "")).strip()
    organization_name = str(payload.get("org_name", "")).strip()
    email = str(payload.get("email", "")).strip()

    try:
        organization_id = int(payload.get("org_id"))
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token JWT invalido o incompleto.",
            headers={"WWW-Authenticate": "Bearer"},
        ) from None

    if not subject or organization_id <= 0 or not role:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token JWT invalido o incompleto.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    session_id = None
    if payload.get("sid") is not None:
        try:
            session_id = int(payload.get("sid"))
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token JWT invalido o incompleto.",
                headers={"WWW-Authenticate": "Bearer"},
            ) from None

    return UserTenantContext(
        username=subject,
        organization_id=organization_id,
        organization_name=organization_name,
        role=role,
        email=email,
        session_id=session_id,
    )


def _is_platform_superadmin(
    *,
    user: User,
    organization: Organization,
) -> bool:
    return (
        user.role.strip().lower() == "admin"
        and user.username.strip().lower() == "admin"
        and organization.slug.strip().lower() == "exp-chaco"
    )


def _hydrate_user_tenant_context(
    raw_context: UserTenantContext,
) -> UserTenantContext:
    session = get_db_session()

    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    try:
        user = session.execute(
            select(User).where(
                User.username == raw_context.username,
                User.organization_id == raw_context.organization_id,
            )
        ).scalar_one_or_none()

        if user is None or not user.is_active:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token JWT invalido o expirado.",
                headers={"WWW-Authenticate": "Bearer"},
            )

        organization = session.execute(
            select(Organization).where(
                Organization.id == user.organization_id,
            )
        ).scalar_one_or_none()

        if organization is None or not organization.is_active:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token JWT invalido o expirado.",
                headers={"WWW-Authenticate": "Bearer"},
            )

        if raw_context.session_id is not None:
            user_session = session.execute(
                select(UserSession).where(
                    UserSession.id == raw_context.session_id,
                    UserSession.user_id == user.id,
                    UserSession.organization_id == organization.id,
                )
            ).scalar_one_or_none()

            if user_session is None or user_session.revoked_at is not None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Token JWT invalido o expirado.",
                    headers={"WWW-Authenticate": "Bearer"},
                )

        return UserTenantContext(
            user_id=user.id,
            username=user.username,
            organization_id=organization.id,
            organization_name=organization.name,
            organization_slug=organization.slug,
            role=user.role,
            email=user.email,
            session_id=raw_context.session_id,
            is_platform_superadmin=_is_platform_superadmin(
                user=user,
                organization=organization,
            ),
        )
    finally:
        session.close()


def _build_user_info(
    *,
    user: User,
    organization: Organization,
) -> dict[str, Any]:
    return {
        "username": user.username,
        "organization_id": user.organization_id,
        "organization_name": organization.name,
        "role": user.role,
        "email": user.email,
    }


def _build_access_token_payload(
    *,
    user: User,
    organization: Organization,
    session_id: int,
) -> dict[str, Any]:
    return {
        "sub": user.username,
        "org_id": user.organization_id,
        "org_name": organization.name,
        "role": user.role,
        "email": user.email,
        "sid": session_id,
    }


def _set_auth_cookies(
    *,
    response: Response,
    access_token: str,
    refresh_token: str,
    settings,
) -> None:
    access_token_expire_seconds = settings.jwt.access_token_expire_seconds
    refresh_token_expire_seconds = settings.jwt.refresh_token_expire_days * 24 * 60 * 60
    secure_cookie = settings.is_production

    response.set_cookie(
        key=ACCESS_TOKEN_COOKIE_KEY,
        value=access_token,
        httponly=True,
        samesite="lax",
        secure=secure_cookie,
        max_age=access_token_expire_seconds,
        path="/",
    )
    response.set_cookie(
        key=REFRESH_TOKEN_COOKIE_KEY,
        value=refresh_token,
        httponly=True,
        samesite="lax",
        secure=secure_cookie,
        max_age=refresh_token_expire_seconds,
        path="/",
    )


def clear_auth_cookies(response: Response) -> None:
    secure_cookie = get_settings().is_production
    response.delete_cookie(
        ACCESS_TOKEN_COOKIE_KEY,
        httponly=True,
        samesite="lax",
        secure=secure_cookie,
        path="/",
    )
    response.delete_cookie(
        REFRESH_TOKEN_COOKIE_KEY,
        httponly=True,
        samesite="lax",
        secure=secure_cookie,
        path="/",
    )


def _extract_request_metadata(request: Request | None) -> tuple[str | None, str | None]:
    if request is None:
        return None, None

    client_host = None
    if request.client is not None:
        client_host = sanitize_ip_address(request.client.host)

    return client_host, sanitize_user_agent(request.headers.get("user-agent"))


def _extract_session_id_from_access_token(token: str | None) -> int | None:
    if not token:
        return None

    payload = verify_jwt_token(
        token,
        expected_token_type="access",
    )

    if not payload:
        return None

    try:
        session_id = int(payload.get("sid"))
    except (TypeError, ValueError):
        return None

    return session_id if session_id > 0 else None


def revoke_logout_target(
    session,
    *,
    refresh_token: str | None = None,
    access_token: str | None = None,
) -> None:
    session_id = _extract_session_id_from_access_token(access_token)
    revoke_session(
        session,
        refresh_token=refresh_token,
        session_id=session_id if refresh_token is None else None,
    )


def get_current_tenant_user(
    authorization: str | None = Header(None),
    bearer_token: str | None = Depends(oauth2_scheme),
    session_jwt: str | None = Cookie(None, alias=ACCESS_TOKEN_COOKIE_KEY),
) -> UserTenantContext:
    """Extrae y valida el contexto Tenant desde un access token JWT."""

    token = None

    if isinstance(bearer_token, str) and bearer_token:
        token = bearer_token
    elif isinstance(authorization, str) and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
    elif isinstance(session_jwt, str) and session_jwt:
        token = session_jwt

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Se requiere un token JWT.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    payload = verify_jwt_token(
        token,
        expected_token_type="access",
    )

    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token JWT invalido o expirado.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return _hydrate_user_tenant_context(
        _build_user_tenant_context(payload)
    )


@router.post("/login", response_model=TokenResponse)
async def login_b2b(
    payload: LoginRequest,
    response: Response,
    request: Request = None,
) -> TokenResponse:
    """Autentica un usuario real contra PostgreSQL y emite un access token JWT."""

    username = payload.username.strip()
    password = payload.password

    if not username or not password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Debe ingresar usuario y contrasena.",
        )

    session = get_db_session()

    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    try:
        settings = get_settings()
        access_token_expire_seconds = settings.jwt.access_token_expire_seconds
        client_ip, user_agent = _extract_request_metadata(request)

        user = session.execute(
            select(User).where(User.username == username)
        ).scalar_one_or_none()

        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Usuario o contrasena incorrectos.",
                headers={"WWW-Authenticate": "Bearer"},
            )

        if not user.is_active:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="El usuario esta inactivo.",
            )

        if not verify_password(password, user.password_hash):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Usuario o contrasena incorrectos.",
                headers={"WWW-Authenticate": "Bearer"},
            )

        organization = session.execute(
            select(Organization).where(
                Organization.id == user.organization_id
            )
        ).scalar_one_or_none()

        if organization is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="La organizacion del usuario no existe.",
            )

        if not organization.is_active:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="La organizacion esta inactiva.",
            )

        issued_session = create_user_session(
            session,
            user=user,
            organization=organization,
            created_ip=client_ip,
            user_agent=user_agent,
        )
        user.last_login_at = issued_session.session.issued_at

        jwt_token = create_jwt_token(
            _build_access_token_payload(
                user=user,
                organization=organization,
                session_id=issued_session.session.id,
            ),
            expires_in_seconds=access_token_expire_seconds,
            token_type="access",
        )
        session.commit()
        _set_auth_cookies(
            response=response,
            access_token=jwt_token,
            refresh_token=issued_session.refresh_token,
            settings=settings,
        )

        return TokenResponse(
            access_token=jwt_token,
            expires_in_seconds=access_token_expire_seconds,
            user_info=_build_user_info(
                user=user,
                organization=organization,
            ),
        )
    except SessionSecurityError as exc:
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc
    except HTTPException:
        session.rollback()
        raise
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@router.post("/refresh", response_model=TokenResponse)
async def refresh_b2b_session(
    response: Response,
    payload: RefreshRequest | None = None,
    request: Request = None,
    refresh_token_cookie: str | None = Cookie(None, alias=REFRESH_TOKEN_COOKIE_KEY),
) -> TokenResponse:
    session = get_db_session()

    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    refresh_token = None
    if payload is not None and isinstance(payload.refresh_token, str):
        refresh_token = payload.refresh_token.strip() or None
    if refresh_token is None and isinstance(refresh_token_cookie, str):
        refresh_token = refresh_token_cookie.strip() or None

    if not refresh_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token invalido o expirado.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        settings = get_settings()
        access_token_expire_seconds = settings.jwt.access_token_expire_seconds
        client_ip, user_agent = _extract_request_metadata(request)
        rotated_session = rotate_refresh_session(
            session,
            refresh_token=refresh_token,
            created_ip=client_ip,
            user_agent=user_agent,
        )

        jwt_token = create_jwt_token(
            _build_access_token_payload(
                user=rotated_session.user,
                organization=rotated_session.organization,
                session_id=rotated_session.new_session.id,
            ),
            expires_in_seconds=access_token_expire_seconds,
            token_type="access",
        )
        _set_auth_cookies(
            response=response,
            access_token=jwt_token,
            refresh_token=rotated_session.refresh_token,
            settings=settings,
        )
        session.commit()

        return TokenResponse(
            access_token=jwt_token,
            expires_in_seconds=access_token_expire_seconds,
            user_info=_build_user_info(
                user=rotated_session.user,
                organization=rotated_session.organization,
            ),
        )
    except SessionSecurityError as exc:
        try:
            session.commit()
        except Exception:
            session.rollback()
        clear_auth_cookies(response)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(exc),
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@router.post("/logout", response_model=LogoutResponse)
async def logout_b2b_session(
    response: Response,
    payload: LogoutRequest | None = None,
    authorization: str | None = Header(None),
    bearer_token: str | None = Depends(oauth2_scheme),
    refresh_token_cookie: str | None = Cookie(None, alias=REFRESH_TOKEN_COOKIE_KEY),
    session_jwt: str | None = Cookie(None, alias=ACCESS_TOKEN_COOKIE_KEY),
) -> LogoutResponse:
    session = get_db_session()

    if session is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos no disponible.",
        )

    refresh_token = None
    if payload is not None and isinstance(payload.refresh_token, str):
        refresh_token = payload.refresh_token.strip() or None
    if refresh_token is None and isinstance(refresh_token_cookie, str):
        refresh_token = refresh_token_cookie.strip() or None

    access_token = None
    if isinstance(bearer_token, str) and bearer_token:
        access_token = bearer_token
    elif isinstance(authorization, str) and authorization.lower().startswith("bearer "):
        access_token = authorization.split(" ", 1)[1].strip()
    elif isinstance(session_jwt, str) and session_jwt:
        access_token = session_jwt

    try:
        revoke_logout_target(
            session,
            refresh_token=refresh_token,
            access_token=access_token,
        )
        session.commit()
        clear_auth_cookies(response)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    return LogoutResponse(detail="Sesion finalizada.")


@router.get("/me", response_model=UserTenantContext)
async def get_current_user_profile(
    user: UserTenantContext = Depends(get_current_tenant_user),
) -> UserTenantContext:
    """Obtiene el contexto Tenant del usuario autenticado."""

    return user
